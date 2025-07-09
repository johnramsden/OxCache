use crate::cache::Cache;
use crate::eviction::{EvictionPolicyWrapper, Evictor};
use crate::readerpool::{ReadRequest, ReaderPool};
use crate::writerpool::{WriteRequest, WriterPool};
use std::error::Error;
// use tokio::spawn;
use tokio::io::{split, AsyncReadExt, AsyncWriteExt};
use tokio::net::{UnixListener, UnixStream};
use tokio::sync::{Mutex, Notify};

use crate::remote::{EmulatedBackend, RemoteBackend};
use crate::{device, remote, request};
use std::path::Path;
use std::sync::Arc;

use bincode;
use bincode::error::DecodeError;
use bytes::Bytes;
use futures::{SinkExt, StreamExt};
use tokio_util::codec::{FramedRead, FramedWrite, LengthDelimitedCodec};
use crate::cache::bucket::Chunk;

#[derive(Debug)]
pub struct ServerRemoteConfig {
    pub remote_type: String,
    pub bucket: Option<String>,
}

#[derive(Debug, Clone)]
pub struct ServerEvictionConfig {
    pub eviction_type: String,
    pub high_water_evict: usize,
    pub low_water_evict: usize,
}

#[derive(Debug)]
pub struct ServerConfig {
    pub socket: String,
    pub disk: String,
    pub writer_threads: usize,
    pub reader_threads: usize,
    pub remote: ServerRemoteConfig,
    pub eviction: ServerEvictionConfig,
    pub chunk_size: usize,
}

pub struct Server<T: RemoteBackend + Send + Sync> {
    cache: Arc<Cache>,
    config: ServerConfig,
    remote: Arc<T>,
}

impl<T: RemoteBackend + Send + Sync + 'static> Server<T> {
    pub fn new(config: ServerConfig, remote: Arc<T>) -> Result<Self, Box<dyn Error>> {
        Ok(Self {
            cache: Arc::new(Cache::new()),
            remote,
            config,
        })
    }

    pub async fn run(&self) -> tokio::io::Result<()> {
        let socket_path = Path::new(&self.config.socket);

        // Clean up old socket if present
        if socket_path.exists() {
            tokio::fs::remove_file(socket_path).await?;
        }

        let listener = UnixListener::bind(socket_path)?;
        println!("Listening on socket: {}", self.config.socket);
        
        let device = device::get_device(
            self.config.disk.as_str(),
            self.config.chunk_size,
            self.config.eviction.clone()
        )?;

        let evictor = Evictor::start(Arc::clone(&device));
        let writerpool = Arc::new(WriterPool::start(self.config.writer_threads, Arc::clone(&device)));
        let readerpool = Arc::new(ReaderPool::start(self.config.reader_threads, Arc::clone(&device)));

        // Shutdown signal
        let shutdown = Arc::new(Notify::new());
        let shutdown_signal = shutdown.clone();

        // Spawn a task to listen for Ctrl+C
        let shutdown_task = tokio::spawn({
            async move {
                tokio::signal::ctrl_c()
                    .await
                    .expect("Failed to listen for ctrl_c");
                println!("Ctrl+C received, shutting down...");
                shutdown_signal.notify_waiters();
            }
        });

        // Request handling (green threads)
        loop {
            tokio::select! {
                _ = shutdown.notified() => {
                    println!("Shutting down accept loop.");
                    break;
                }

                accept_result = listener.accept() => {
                    match accept_result {
                        Ok((stream, addr)) => {
                            println!("Accepted connection: {:?}", addr);

                            tokio::spawn({
                                let writer_pool = Arc::clone(&self.cache);
                                let remote = Arc::clone(&self.remote);
                                let writerpool = Arc::clone(&writerpool);
                                let readerpool = Arc::clone(&readerpool);
                                let cache = Arc::clone(&self.cache);
                                async move {
                                    
                                if let Err(e) = handle_connection(stream, writerpool, readerpool, remote, cache).await {
                                    eprintln!("Connection error: {}", e);
                                }
                            }});
                        },
                        Err(e) => {
                            eprintln!("Accept failed: {}", e);
                        }
                    }
                }
            }
        }

        // Cleanup (last)
        evictor.stop();
        // writerpool.stop(); // Cannot move
        // readerpool.stop(); // Cannot move
        Ok(())
    }
}

async fn handle_connection<T: RemoteBackend + Send + Sync + 'static>(
    stream: UnixStream,
    writer_pool: Arc<WriterPool>,
    reader_pool: Arc<ReaderPool>,
    remote: Arc<T>,
    cache: Arc<Cache>,
) -> tokio::io::Result<()> {
    let (read_half, write_half) = split(stream);
    let mut reader = FramedRead::new(read_half, LengthDelimitedCodec::new());
    let writer = Arc::new(Mutex::new(FramedWrite::new(write_half, LengthDelimitedCodec::new())));

    while let Some(frame) = reader.next().await {
        let f = frame?;
        let bytes = f.as_ref();
        let msg: Result<(request::Request, usize), DecodeError> =
            bincode::serde::decode_from_slice(bytes, bincode::config::standard());
        println!("Received: {:?}", msg);

        match msg {
            Ok((request, _)) => {
                println!("Received: {:?}", request);
                match request {
                    request::Request::Get(req) => {
                        println!("Received get request: {:?}", req);
                        let chunk: Chunk = req.into();
                        cache.get_or_insert_with(
                            chunk.clone(),
                            // Data was in map, read and return it
                            {
                                let writer = Arc::clone(&writer);
                                let reader_pool = Arc::clone(&reader_pool);
                                |location| async move {
                                    let location = location.as_ref().clone(); // Owned copy
                                    // Read from disk via channel
                                    // Proceed with writing to disk
                                    let (tx, rx) = flume::bounded(1);
                                    let read_req = ReadRequest {
                                        location,
                                        responder: tx,
                                    };
                                    reader_pool.send(read_req).await?;

                                    // Recieve read val
                                    let recv_err = rx.recv_async().await;
                                    let read_response = match recv_err {
                                        Ok(wr) => {
                                            match wr.data {
                                                Ok(loc) => loc,
                                                Err(e) => {
                                                    return Err(e);
                                                }
                                            }
                                        },
                                        Err(e) => {
                                            eprintln!("Failed to get read response {:?}", e);
                                            return Err(std::io::Error::new(std::io::ErrorKind::Other, recv_err.unwrap_err()));
                                        },
                                    };

                                    // Send to user
                                    let encoded = bincode::serde::encode_to_vec(
                                        request::GetResponse::Response(read_response),
                                        bincode::config::standard()
                                    ).unwrap();
                                    {
                                        let mut w = writer.lock().await;
                                        w.send(Bytes::from(encoded)).await?;
                                    }
                                    
                                    Ok(())
                                }
                            },
                            // Data wasn't in map, request it and write it
                            {
                                let chunk = chunk.clone();
                                let writer = Arc::clone(&writer);
                                let remote = Arc::clone(&remote);
                                let writer_pool = Arc::clone(&writer_pool);
                                move || async move {
                                    // Call remote (S3, ...)
                                    let resp = match remote.get(chunk.uuid.as_str(), chunk.offset, chunk.size).await {
                                        Ok(resp) => resp,
                                        Err(e) => {
                                            let encoded = bincode::serde::encode_to_vec(
                                                request::GetResponse::Error(e.to_string()),
                                                bincode::config::standard()
                                            ).unwrap();
                                            {
                                                let mut w = writer.lock().await;
                                                w.send(Bytes::from(encoded)).await?;
                                            }
                                            // Fatal error, or keep accepting? Currently fatal, closes connection.
                                            return Err(e);
                                        }
                                    };
                                    
                                    // Send to user
                                    let encoded = bincode::serde::encode_to_vec(
                                        request::GetResponse::Response(resp.clone()),
                                        bincode::config::standard()
                                    ).unwrap();
                                    {
                                        let mut w = writer.lock().await;
                                        w.send(Bytes::from(encoded)).await?;
                                    }

                                    // Proceed with writing to disk
                                    let (tx, rx) = flume::bounded(1);
                                    let write_req = WriteRequest {
                                        data: resp,
                                        responder: tx,
                                    };
                                    writer_pool.send(write_req).await?;

                                    // Recieve written val
                                    let recv_err = rx.recv_async().await;
                                    let write_response = match recv_err {
                                        Ok(wr) => {
                                            match wr.location {
                                                Ok(loc) => loc,
                                                Err(e) => {
                                                    return Err(e);
                                                }
                                            }
                                        },
                                        Err(e) => {
                                            eprintln!("Failed to get write response {:?}", e);
                                            return Err(std::io::Error::new(std::io::ErrorKind::Other, recv_err.unwrap_err()));
                                        },
                                    };
                                    Ok(write_response)
                                }
                            },
                        ).await?;                        
                    }
                    request::Request::Close => {
                        println!("Received close request");
                        break;
                    }
                    _ => {
                        println!("Unknown request: {:?}", request);
                    }
                }
            }
            Err(e) => {
                println!("Error receiving data: {:?}", e);
                break;
            }
        }
    }
    Ok(())
}
