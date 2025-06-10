use crate::cache::Cache;
use crate::eviction::Evictor;
use crate::readerpool::ReaderPool;
use crate::writerpool::{WriteRequest, WriterPool};
use std::error::Error;

// use tokio::spawn;
use tokio::io::{split, AsyncReadExt, AsyncWriteExt};
use tokio::net::{UnixListener, UnixStream};
use tokio::sync::Notify;

use crate::remote::{EmulatedBackend, RemoteBackend};
use crate::{device, remote, request};
use std::path::Path;
use std::sync::Arc;

use bincode;
use bincode::error::DecodeError;
use bytes::Bytes;
use futures::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use tokio_util::codec::{FramedRead, FramedWrite, LengthDelimitedCodec};

#[derive(Debug)]
pub struct ServerRemoteConfig {
    pub remote_type: String,
    pub bucket: Option<String>,
}

#[derive(Debug)]
pub struct ServerConfig {
    pub socket: String,
    pub disk: String,
    pub writer_threads: usize,
    pub reader_threads: usize,
    pub remote: ServerRemoteConfig,
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
        
        let device = device::get_device(self.config.disk.as_str())?;

        let evictor = Evictor::start();
        let writerpool = Arc::new(WriterPool::start(self.config.writer_threads, device));
        let readerpool = Arc::new(ReaderPool::start(self.config.reader_threads));

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
                                async move {
                                    
                                if let Err(e) = handle_connection(stream, writerpool, readerpool, remote).await {
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

async fn handle_connection<T: RemoteBackend + Send + Sync>(
    stream: UnixStream,
    writer_pool: Arc<WriterPool>,
    reader_pool: Arc<ReaderPool>,
    remote: Arc<T>,
) -> tokio::io::Result<()> {
    let (read_half, write_half) = split(stream);
    let mut reader = FramedRead::new(read_half, LengthDelimitedCodec::new());
    let mut writer = FramedWrite::new(write_half, LengthDelimitedCodec::new());
    
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
                        
                        // Grab from remote
                        let resp = match remote.get(req.key.as_str(), req.offset, req.size).await {
                            Ok(resp) => resp,
                            Err(e) => {
                                let encoded = bincode::serde::encode_to_vec(
                                    request::GetResponse::Error(e.to_string()),
                                    bincode::config::standard()
                                ).unwrap();
                                writer.send(Bytes::from(encoded)).await?;
                                // Fatal error, or keep accepting? Currently fatal, closes connection.
                                return Err(e);
                            }
                        };
                        let encoded = bincode::serde::encode_to_vec(
                            request::GetResponse::Response(resp.clone()),
                            bincode::config::standard()
                        ).unwrap();
                        writer.send(Bytes::from(encoded)).await?;
                        
                        // Proceed with writing/reading to/from disk
                        let (tx, rx) = flume::bounded(1);
                        let write_req = WriteRequest {
                            data: resp,
                            responder: tx,
                        };
                        writer_pool.send(write_req).await?;
                        let recv_err = rx.recv_async().await;
                        if recv_err.is_err() {
                            eprintln!("Failed to get response {:?}", recv_err);
                        }
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
