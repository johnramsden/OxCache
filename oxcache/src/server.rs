use crate::cache::Cache;
use crate::device::Device;
use crate::eviction::{EvictionPolicyWrapper, Evictor, EvictorMessage};
use crate::readerpool::{ReadRequest, ReaderPool};
use crate::writerpool::{WriteRequest, WriterPool};
use std::error::Error;
use tokio::net::{UnixListener, UnixStream};
use tokio::sync::{Mutex, Notify};

use crate::cache::bucket::Chunk;
use crate::remote::RemoteBackend;
use crate::{device, request};
use bincode;
use bincode::error::DecodeError;
use bytes::Bytes;
use flume::{Receiver, Sender};
use futures::{SinkExt, StreamExt};
use once_cell::sync::Lazy;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;
use tokio::runtime::{Builder, Runtime};
use tokio_util::codec::{FramedRead, FramedWrite, LengthDelimitedCodec};

// Global tokio runtime
// pub static RUNTIME: Lazy<Runtime> =
// Lazy::new(|| Runtime::new().expect("Failed to create Tokio runtime"));

pub static RUNTIME: Lazy<Runtime> = Lazy::new(|| {
    Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("Failed to create Tokio runtime")
});

#[derive(Debug)]
pub struct ServerRemoteConfig {
    pub remote_type: String,
    pub bucket: Option<String>,
    pub remote_artificial_delay_microsec: Option<usize>,
}

#[derive(Debug, Clone)]
pub struct ServerEvictionConfig {
    pub eviction_type: String,
    pub high_water_evict: usize,
    pub low_water_evict: usize,
    pub eviction_interval: u64,
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
    pub block_zone_capacity: usize,
}

pub struct Server<T: RemoteBackend + Send + Sync> {
    cache: Arc<Cache>,
    config: ServerConfig,
    remote: Arc<T>,
    device: Arc<dyn Device>,
    evict_rx: Receiver<EvictorMessage>,
}

impl<T: RemoteBackend + Send + Sync + 'static> Server<T> {
    pub fn new(config: ServerConfig, mut remote: T) -> Result<Self, Box<dyn Error>> {
        let (evict_tx, evict_rx): (Sender<EvictorMessage>, Receiver<EvictorMessage>) = flume::unbounded();
        let device = device::get_device(
            config.disk.as_str(),
            config.chunk_size,
            config.block_zone_capacity,
            evict_tx,
        )?;

        device.reset_all()?;

        remote.set_blocksize(device.get_block_size());

        Ok(Self {
            cache: Arc::new(Cache::new(
                device.get_num_zones(),
                device.get_chunks_per_zone(),
            )),
            remote: Arc::new(remote),
            config,
            device,
            evict_rx
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

        let eviction_policy = Arc::new(std::sync::Mutex::new(EvictionPolicyWrapper::new(
            self.config.eviction.eviction_type.as_str(),
            self.config.eviction.high_water_evict,
            self.config.eviction.low_water_evict,
            self.device.get_num_zones(),
            self.device.get_chunks_per_zone(),
        )?));

        let evictor = Evictor::start(
            Arc::clone(&self.device),
            Arc::clone(&eviction_policy),
            Arc::clone(&self.cache),
            Duration::from_secs(self.config.eviction.eviction_interval),
            self.evict_rx.clone(),
        )?;
        let writerpool = Arc::new(WriterPool::start(
            self.config.writer_threads,
            Arc::clone(&self.device),
            &eviction_policy,
        ));
        let readerpool = Arc::new(ReaderPool::start(
            self.config.reader_threads,
            Arc::clone(&self.device),
            &eviction_policy,
        ));

        // Shutdown signal
        let shutdown = Arc::new(Notify::new());
        let shutdown_signal = shutdown.clone();

        // Spawn a task to listen for Ctrl+C
        let _shutdown_task = tokio::spawn({
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
                                let remote = Arc::clone(&self.remote);
                                let writerpool = Arc::clone(&writerpool);
                                let readerpool = Arc::clone(&readerpool);
                                let cache = Arc::clone(&self.cache);
                                let chunk_size = self.config.chunk_size;
                                async move {
                                if let Err(e) = handle_connection(stream, writerpool, readerpool, remote, cache, chunk_size).await {
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

const MAX_FRAME_LENGTH: usize = 2 * 1024 * 1024 * 1024; // 2 GB

async fn handle_connection<T: RemoteBackend + Send + Sync + 'static>(
    stream: UnixStream,
    writer_pool: Arc<WriterPool>,
    reader_pool: Arc<ReaderPool>,
    remote: Arc<T>,
    cache: Arc<Cache>,
    chunk_size: usize,
) -> tokio::io::Result<()> {
    let (read_half, write_half) = tokio::io::split(stream);

    let codec = LengthDelimitedCodec::builder()
        .max_frame_length(MAX_FRAME_LENGTH)
        .new_codec();

    let mut reader = FramedRead::new(read_half, codec.clone());
    let writer = Arc::new(Mutex::new(FramedWrite::new(write_half, codec)));

    while let Some(frame) = reader.next().await {
        let f = frame.map_err(|e| {
            std::io::Error::new(
                std::io::ErrorKind::Other,
                format!("frame read failed: {}", e),
            )
        })?;
        let bytes = f.as_ref();
        let msg: Result<(request::Request, usize), DecodeError> =
            bincode::serde::decode_from_slice(bytes, bincode::config::standard());
        // println!("Received: {:?}", msg);

        match msg {
            Ok((request, _)) => {
                // println!("Received req");
                match request {
                    request::Request::Get(req) => {
                        if let Err(e) = req.validate(chunk_size) {
                            let encoded = bincode::serde::encode_to_vec(
                                request::GetResponse::Error(e.to_string()),
                                bincode::config::standard(),
                            )
                            .unwrap();
                            {
                                let mut w = writer.lock().await;
                                w.send(Bytes::from(encoded)).await.map_err(|e| {
                                    std::io::Error::new(
                                        std::io::ErrorKind::Other,
                                        format!("failed to send validation error: {}", e),
                                    )
                                })?;
                            }

                            continue;
                        }

                        // println!("Received get request: {:?}", req);
                        let chunk: Chunk = req.into();

                        cache.get_or_insert_with(
                            chunk.clone(),
                            {
                                let writer = Arc::clone(&writer);
                                let reader_pool = Arc::clone(&reader_pool);
                                // let chunk = chunk.clone();
                                |location| async move {
                                    // println!("HIT {:?}", chunk);
                                    let location = location.as_ref().clone();

                                    let (tx, rx) = flume::bounded(1);
                                    let read_req = ReadRequest {
                                        location,
                                        responder: tx,
                                    };
                                    reader_pool.send(read_req).await.map_err(|e| {
                                        std::io::Error::new(std::io::ErrorKind::Other, format!("failed to send read request: {}", e))
                                    })?;

                                    let recv_err = rx.recv_async().await;
                                    let read_response = match recv_err {
                                        Ok(wr) => match wr.data {
                                            Ok(loc) => loc,
                                            Err(e) => {
                                                return Err(std::io::Error::new(std::io::ErrorKind::Other, format!("read data error: {}", e)));
                                            }
                                        },
                                        Err(e) => {
                                            return Err(std::io::Error::new(std::io::ErrorKind::Other, format!("recv_async failed for read: {}", e)));
                                        }
                                    };

                                    let encoded = bincode::serde::encode_to_vec(
                                        request::GetResponse::Response(read_response.clone()),
                                        bincode::config::standard()
                                    ).unwrap();
                                    {
                                        let mut w = writer.lock().await;
                                        w.send(Bytes::from(encoded)).await.map_err(|e| {
                                            std::io::Error::new(std::io::ErrorKind::Other, format!("failed to send read response: {}", e))
                                        })?;
                                    }

                                    Ok(())
                                }
                            },
                            {
                                let chunk = chunk.clone();
                                let writer = Arc::clone(&writer);
                                let remote = Arc::clone(&remote);
                                let writer_pool = Arc::clone(&writer_pool);
                                move || async move {
                                    // println!("MISS {:?}", chunk);
                                    let resp = match remote.get(chunk.uuid.as_str(), chunk.offset, chunk.size).await {
                                        Ok(resp) => resp,
                                        Err(e) => {
                                            let encoded = bincode::serde::encode_to_vec(
                                                request::GetResponse::Error(e.to_string()),
                                                bincode::config::standard()
                                            ).unwrap();
                                            {
                                                let mut w = writer.lock().await;
                                                w.send(Bytes::from(encoded)).await.map_err(|e| {
                                                    std::io::Error::new(std::io::ErrorKind::Other, format!("failed to send remote error response: {}", e))
                                                })?;
                                            }
                                            return Err(std::io::Error::new(std::io::ErrorKind::Other, format!("remote.get failed: {}", e)));
                                        }
                                    };

                                    let encoded = bincode::serde::encode_to_vec(
                                        request::GetResponse::Response(resp.clone()),
                                        bincode::config::standard()
                                    ).map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, format!("serialization failed: {}", e)))?;
                                    {
                                        let mut w = writer.lock().await;
                                        w.send(Bytes::from(encoded)).await.map_err(|e| {
                                            std::io::Error::new(std::io::ErrorKind::Other, format!("failed to send remote get response: {}", e))
                                        })?;
                                    }

                                    let (tx, rx) = flume::bounded(1);
                                    let write_req = WriteRequest {
                                        data: resp.clone(),
                                        responder: tx,
                                    };
                                    writer_pool.send(write_req).await.map_err(|e| {
                                        std::io::Error::new(std::io::ErrorKind::Other, format!("failed to send write request: {}", e))
                                    })?;

                                    let recv_err = rx.recv_async().await;
                                    let write_response = match recv_err {
                                        Ok(wr) => match wr.location {
                                            Ok(loc) => loc,
                                            Err(e) => {
                                                return Err(std::io::Error::new(std::io::ErrorKind::Other, format!("write location error: {}", e)));
                                            }
                                        },
                                        Err(e) => {
                                            return Err(std::io::Error::new(std::io::ErrorKind::Other, format!("recv_async failed for write: {}", e)));
                                        }
                                    };
                                    Ok(write_response)
                                }
                            },
                        ).await.map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, format!("cache.get_or_insert_with failed: {}", e)))?;
                    }
                    request::Request::Close => {
                        println!("Received close request");
                        break;
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
