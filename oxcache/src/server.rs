use crate::cache::Cache;
use crate::device::Device;
use crate::eviction::{EvictionPolicyWrapper, Evictor, EvictorMessage};
use crate::readerpool::{ReadRequest, ReaderPool};
use crate::writerpool::{WriteRequest, WriterPool};
use tracing::debug;
use nvme::types::Byte;
use std::error::Error;
use std::hash::{DefaultHasher, Hash, Hasher};
use std::net::SocketAddr;
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
use crate::metrics::{init_metrics_exporter, HitType, MetricType, METRICS};
use std::sync::atomic::{AtomicU64, Ordering};
// Global tokio runtime
// pub static RUNTIME: Lazy<Runtime> =
// Lazy::new(|| Runtime::new().expect("Failed to create Tokio runtime"));

pub static RUNTIME: Lazy<Runtime> = Lazy::new(|| {
    Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("Failed to create Tokio runtime")
});

// Global request ID counter for tracking
static REQUEST_ID_COUNTER: AtomicU64 = AtomicU64::new(1);

#[derive(Debug)]
pub struct ServerRemoteConfig {
    pub remote_type: String,
    pub bucket: Option<String>,
    pub remote_artificial_delay_microsec: Option<usize>,
}

#[derive(Debug, Clone)]
pub struct ServerEvictionConfig {
    pub eviction_type: String,
    pub high_water_evict: u64,
    pub low_water_evict: u64,
    pub low_water_clean: Option<u64>,
    pub eviction_interval: u64,
}

#[derive(Debug, Clone)]
pub struct ServerMetricsConfig {
    pub metrics_exporter_addr: Option<SocketAddr>,
}

#[derive(Debug)]
pub struct ServerConfig {
    pub socket: String,
    pub disk: String,
    pub writer_threads: usize,
    pub reader_threads: usize,
    pub remote: ServerRemoteConfig,
    pub eviction: ServerEvictionConfig,
    pub chunk_size: Byte,
    pub block_zone_capacity: Byte,
    pub max_write_size: Byte,
    pub max_zones: Option<u64>,
    pub metrics: ServerMetricsConfig,
}

pub struct Server<T: RemoteBackend + Send + Sync> {
    cache: Arc<Cache>,
    config: ServerConfig,
    remote: Arc<T>,
    device: Arc<dyn Device>,
    evict_rx: Receiver<EvictorMessage>,
    pub shutdown: Arc<Notify>,
}

pub fn validate_read_response(buf: &[u8], key: &str, offset: Byte, size: Byte) {
    // Hash using DefaultHasher (64-bit hash)
    let mut hasher = DefaultHasher::new();
    key.hash(&mut hasher);
    let key_hash = hasher.finish(); // 8bytes

    // Check the buffer
    let mut buffer = [0u8; 8];
    buffer.copy_from_slice(&buf[0..8]);
    let read_key_hash = u64::from_be_bytes(buffer);
    if read_key_hash != key_hash {
        tracing::error!("Buffer validation failed - key hash mismatch: expected {}, got {}, buf_len: {}", key_hash, read_key_hash, buf.len());
        tracing::error!("Buffer prefix: {:02x?}", &buf[0..std::cmp::min(32, buf.len())]);
    }
    debug_assert_eq!(read_key_hash, key_hash);

    buffer.copy_from_slice(&buf[8..16]);
    debug_assert_eq!(u64::from_be_bytes(buffer), offset);

    buffer.copy_from_slice(&buf[16..24]);
    debug_assert_eq!(u64::from_be_bytes(buffer), size);
}

impl<T: RemoteBackend + Send + Sync + 'static> Server<T> {
    pub fn new(config: ServerConfig, mut remote: T) -> Result<Self, Box<dyn Error>> {
        let (evict_tx, evict_rx): (Sender<EvictorMessage>, Receiver<EvictorMessage>) =
            flume::unbounded();
        let device = device::get_device(
            config.disk.as_str(),
            config.chunk_size,
            config.block_zone_capacity,
            evict_tx,
            config.max_write_size,
            config.max_zones,
        )?;

        device.reset()?;

        remote.set_blocksize(device.get_block_size());

        let shutdown = Arc::new(Notify::new());

        Ok(Self {
            cache: Arc::new(Cache::new(
                device.get_num_zones(),
                device.get_chunks_per_zone(),
            )),
            remote: Arc::new(remote),
            config,
            device,
            evict_rx,
            shutdown,
        })
    }

    pub async fn run(&self) -> tokio::io::Result<()> {
        let socket_path = Path::new(&self.config.socket);

        // Clean up old socket if present
        if socket_path.exists() {
            tokio::fs::remove_file(socket_path).await?;
        }

        let listener = UnixListener::bind(socket_path)?;
        tracing::info!("Listening on socket: {}", self.config.socket);

        if let Some(addr) = self.config.metrics.metrics_exporter_addr {
            init_metrics_exporter(addr);
        }

        let eviction_policy = Arc::new(std::sync::Mutex::new(EvictionPolicyWrapper::new(
            self.config.eviction.eviction_type.as_str(),
            self.config.eviction.high_water_evict,
            self.config.eviction.low_water_evict,
            self.device.get_num_zones(),
            self.device.get_chunks_per_zone(),
            self.config.eviction.low_water_clean,
        )?));

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

        let evictor = Evictor::start(
            Arc::clone(&self.device),
            Arc::clone(&eviction_policy),
            Arc::clone(&self.cache),
            Duration::from_millis(self.config.eviction.eviction_interval),
            self.evict_rx.clone(),
            writerpool.clone()
        )?;

        // Shutdown signal
        let shutdown = self.shutdown.clone();
        let shutdown_signal = self.shutdown.clone();

        // Spawn a task to listen for Ctrl+C
        let _shutdown_task = tokio::spawn({
            async move {
                tokio::signal::ctrl_c()
                    .await
                    .expect("Failed to listen for ctrl_c");
                tracing::info!("Ctrl+C received, shutting down...");
                shutdown_signal.notify_waiters();
            }
        });

        // Reset device
        self.device.reset()?;

        // Request handling (green threads)
        loop {
            tokio::select! {
                _ = shutdown.notified() => {
                    tracing::debug!("Shutting down accept loop.");
                    break;
                }

                accept_result = listener.accept() => {
                    match accept_result {
                        Ok((stream, addr)) => {
                            tracing::debug!("Accepted connection: {:?}", addr);

                            tokio::spawn({
                                let remote = Arc::clone(&self.remote);
                                let writerpool = Arc::clone(&writerpool);
                                let readerpool = Arc::clone(&readerpool);
                                let cache = Arc::clone(&self.cache);
                                let chunk_size = self.config.chunk_size;
                                async move {
                                    if let Err(e) = handle_connection(stream, writerpool, readerpool, remote, cache, chunk_size).await {
                                        tracing::error!("Connection error: {}", e);
                                    }
                            }});
                        },
                        Err(e) => {
                            tracing::error!("Accept failed: {}", e);
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
    chunk_size: Byte,
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

        let start = Arc::new(std::time::Instant::now());
        METRICS.update_metric_counter("requests", 1);

        match msg {
            Ok((request, _)) => {
                let request_id = REQUEST_ID_COUNTER.fetch_add(1, Ordering::SeqCst);
                tracing::debug!("REQ[{}] Started processing request", request_id);
                
                match request {
                    request::Request::Get(req) => {
                        tracing::debug!("REQ[{}] Processing GET request for key={}, offset={}, size={}",
                                     request_id, req.key, req.offset, req.size);
                        if let Err(e) = req.validate(chunk_size) {
                            tracing::warn!("REQ[{}] Validation failed: {}", request_id, e);
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
                            tracing::debug!("REQ[{}] Completed with validation error", request_id);
                            continue;
                        }

                        // println!("Received get request: {:?}", req);
                        let chunk: Chunk = req.into();
                        let chunk_clone = chunk.clone();

                        tracing::debug!("REQ[{}] Calling cache.get_or_insert_with", request_id);
                        cache.get_or_insert_with(
                            chunk.clone(),
                            {
                                let writer = Arc::clone(&writer);
                                let reader_pool = Arc::clone(&reader_pool);
                                let start = Arc::clone(&start);
                                // let chunk = chunk.clone();
                                move |pin_guard| async move {
                                    let chunk = chunk.clone();
                                    tracing::debug!("REQ[{}] CACHE HIT - entering read path", request_id);
                                    let location = pin_guard.location().clone();

                                    let (tx, rx) = flume::bounded(1);
                                    let read_req = ReadRequest {
                                        location,
                                        responder: tx,
                                        _pin_guard: pin_guard,
                                        read_offset: chunk.offset,
                                        read_size: chunk.size,
                                    };
                                    tracing::debug!("REQ[{}] Sending read request to reader pool", request_id);
                                    reader_pool.send(read_req).await.map_err(|e| {
                                        tracing::error!("REQ[{}] Failed to send read request: {}", request_id, e);
                                        std::io::Error::new(std::io::ErrorKind::Other, format!("failed to send read request: {}", e))
                                    })?;

                                    tracing::debug!("REQ[{}] Waiting for read response", request_id);
                                    let recv_err = rx.recv_async().await;
                                    let (header, data) = match recv_err {
                                        Ok(wr) => {
                                            tracing::debug!("REQ[{}] Received read response from reader pool", request_id);
                                            match wr.data {
                                                Ok((header, data)) => (header, data),
                                                Err(e) => {
                                                    tracing::error!("REQ[{}] Read data error: {}", request_id, e);
                                                    return Err(std::io::Error::new(std::io::ErrorKind::Other, format!("read data error: {}", e)));
                                                }
                                            }
                                        },
                                        Err(e) => {
                                            tracing::error!("REQ[{}] recv_async failed for read: {}", request_id, e);
                                            return Err(std::io::Error::new(std::io::ErrorKind::Other, format!("recv_async failed for read: {}", e)));
                                        }
                                    };

                                    // Validate read response - pin_guard must stay alive until here!
                                    // Note: We validate against the original chunk that was stored (0, chunk_size),
                                    // not the subset request (chunk.offset, chunk.size)
                                    #[cfg(debug_assertions)]
                                    validate_read_response(&header, &chunk.uuid, 0, chunk_size);

                                    // Return only the data portion (header is just for validation)
                                    let chunked_resp = data;

                                    let encoded = bincode::serde::encode_to_vec(
                                        request::GetResponse::Response(chunked_resp.clone()),
                                        bincode::config::standard()
                                    ).unwrap();
                                    {
                                        tracing::debug!("REQ[{}] Sending response to client", request_id);
                                        let mut w = writer.lock().await;
                                        w.send(Bytes::from(encoded)).await.map_err(|e| {
                                            tracing::error!("REQ[{}] Failed to send read response: {}", request_id, e);
                                            std::io::Error::new(std::io::ErrorKind::Other, format!("failed to send read response: {}", e))
                                        })?;
                                    }

                                    METRICS.update_metric_histogram_latency("get_hit_latency_ms", start.elapsed(), MetricType::MsLatency);
                                    METRICS.update_metric_counter("hit", 1);
                                    METRICS.update_hitratio(HitType::Hit);
                                    METRICS.update_metric_counter("written_bytes_total", chunk.size);
                                    METRICS.update_metric_counter("bytes_total", chunk.size);
                                    tracing::debug!("REQ[{}] CACHE HIT completed successfully", request_id);
                                    Ok(())
                                }
                            },
                            {
                                let chunk = chunk_clone.clone();
                                let writer = Arc::clone(&writer);
                                let remote = Arc::clone(&remote);
                                let writer_pool = Arc::clone(&writer_pool);
                                let start = Arc::clone(&start);
                                move || async move {
                                    tracing::debug!("REQ[{}] CACHE MISS - entering remote fetch path", request_id);
                                    let resp = match remote.get(chunk.uuid.as_str(), 0, chunk_size).await {
                                        Ok(resp) => {
                                            tracing::debug!("REQ[{}] Remote fetch completed successfully", request_id);
                                            resp
                                        },
                                        Err(e) => {
                                            tracing::error!("REQ[{}] Remote fetch failed: {}", request_id, e);
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
                                            tracing::debug!("REQ[{}] Completed with remote fetch error", request_id);
                                            return Err(std::io::Error::new(std::io::ErrorKind::Other, format!("remote.get failed: {}", e)));
                                        }
                                    };

                                    // Will implicitly fail if size larger than chunk
                                    let chunked_resp = resp.slice(chunk.offset as usize..(chunk.offset + chunk.size) as usize);
                                    let encoded = bincode::serde::encode_to_vec(
                                        request::GetResponse::Response(chunked_resp),
                                        bincode::config::standard()
                                    ).map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, format!("serialization failed: {}", e)))?;
                                    {
                                        tracing::debug!("REQ[{}] Sending remote response to client", request_id);
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
                                    tracing::debug!("REQ[{}] Sending write request to writer pool", request_id);
                                    writer_pool.send(write_req).await.map_err(|e| {
                                        tracing::error!("REQ[{}] Failed to send write request: {}", request_id, e);
                                        std::io::Error::new(std::io::ErrorKind::Other, format!("failed to send write request: {}", e))
                                    })?;

                                    tracing::debug!("REQ[{}] Waiting for write response", request_id);
                                    let recv_err = rx.recv_async().await;
                                    let write_response = match recv_err {
                                        Ok(wr) => {
                                            tracing::debug!("REQ[{}] Received write response from writer pool", request_id);
                                            match wr.location {
                                                Ok(loc) => loc,
                                                Err(e) => {
                                                    tracing::error!("REQ[{}] Write location error: {}", request_id, e);
                                                    return Err(std::io::Error::new(std::io::ErrorKind::Other, format!("write location error: {}", e)));
                                                }
                                            }
                                        },
                                        Err(e) => {
                                            tracing::error!("REQ[{}] recv_async failed for write: {}", request_id, e);
                                            return Err(std::io::Error::new(std::io::ErrorKind::Other, format!("recv_async failed for write: {}", e)));
                                        }
                                    };

                                    METRICS.update_metric_counter("miss", 1);
                                    METRICS.update_metric_histogram_latency("get_miss_latency_ms", start.elapsed(), MetricType::MsLatency);
                                    METRICS.update_hitratio(HitType::Miss);
                                    METRICS.update_metric_counter("read_bytes_total", chunk_size);
                                    METRICS.update_metric_counter("bytes_total", chunk_size);
                                    tracing::debug!("REQ[{}] CACHE MISS completed successfully", request_id);
                                    Ok(write_response)
                                }
                            },
                        ).await.map_err(|e| {
                            tracing::error!("REQ[{}] cache.get_or_insert_with failed: {}", request_id, e);
                            std::io::Error::new(std::io::ErrorKind::Other, format!("cache.get_or_insert_with failed: {}", e))
                        })?;
                        tracing::debug!("REQ[{}] Request completed successfully", request_id);
                    }
                    request::Request::Close => {
                        tracing::debug!("Received close request");
                        break;
                    }
                }
            }
            Err(e) => {
                tracing::error!("Error receiving data: {:?}", e);
                break;
            }
        }
        METRICS.update_metric_histogram_latency("get_total_latency_ms", start.elapsed(), MetricType::MsLatency);
    }
    Ok(())
}
