use crate::cache::bucket::PinGuard;
use crate::eviction::EvictionPolicyWrapper;
use crate::metrics::{METRICS, MetricType};
use crate::{cache, device};
use bytes::Bytes;
use flume::{Receiver, Sender, unbounded};
use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};

#[derive(Debug)]
pub struct ReadResponse {
    pub data: std::io::Result<(Bytes, Bytes)>, // (header, data)
}

#[derive(Debug)]
pub struct ReadRequest {
    pub location: cache::bucket::ChunkLocation,
    pub responder: Sender<ReadResponse>,
    pub _pin_guard: PinGuard,           // Keep pin alive during disk I/O
    pub read_offset: nvme::types::Byte, // Offset within the chunk to start reading
    pub read_size: nvme::types::Byte,   // Number of bytes to read from the chunk
}

/// Represents an individual reader thread
struct Reader {
    device: Arc<dyn device::Device>,
    id: usize,
    receiver: Receiver<ReadRequest>,
    eviction_policy: Arc<Mutex<EvictionPolicyWrapper>>,
}

impl Reader {
    fn new(
        id: usize,
        receiver: Receiver<ReadRequest>,
        device: Arc<dyn device::Device>,
        eviction_policy: &Arc<Mutex<EvictionPolicyWrapper>>,
    ) -> Self {
        Self {
            device,
            id,
            receiver,
            eviction_policy: eviction_policy.clone(),
        }
    }

    fn run(self) {
        tracing::debug!("Reader {} started", self.id);
        while let Ok(msg) = self.receiver.recv() {
            // println!("Reader {} processing: {:?}", self.id, msg);
            let start = std::time::Instant::now();
            let result =
                self.device
                    .read_subset(msg.location.clone(), msg.read_offset, msg.read_size);
            METRICS.update_metric_histogram_latency(
                "device_read_latency_ms",
                start.elapsed(),
                MetricType::MsLatency,
            );
            if result.is_ok() {
                let mtx = Arc::clone(&self.eviction_policy);
                let policy = mtx.lock().unwrap();

                match &*policy {
                    EvictionPolicyWrapper::Promotional(p) => {
                        // Clone Arc references before dropping lock
                        let zone_idx = msg.location.zone as usize;
                        let nr_chunks = p.nr_chunks_per_zone;
                        let counters = Arc::clone(&p.zone_chunk_counts);
                        drop(policy);

                        // Check atomically if zone is full (outside lock)
                        if zone_idx < counters.len() {
                            let count = counters[zone_idx].load(Ordering::Relaxed);

                            // Only acquire lock if zone is full
                            if count >= nr_chunks {
                                let mut policy = mtx.lock().unwrap();
                                policy.read_update(msg.location);
                            }
                        }
                    }
                    EvictionPolicyWrapper::Chunk(_) => {
                        drop(policy);
                        let mut policy = mtx.lock().unwrap();
                        policy.read_update(msg.location);
                    }
                }
            }
            let resp = ReadResponse { data: result };
            let snd = msg.responder.send(resp);
            if snd.is_err() {
                tracing::error!(
                    "Failed to send response from writer: {}",
                    snd.err().unwrap()
                );
            }
        }
        tracing::debug!("Reader {} exiting", self.id);
    }
}

/// Pool of writer threads sharing a single receiver
#[derive(Debug)]
pub struct ReaderPool {
    sender: Sender<ReadRequest>,
    handles: Vec<JoinHandle<()>>,
}

impl ReaderPool {
    /// Creates and starts the reader pool with a given number of threads
    pub fn start(
        num_readers: usize,
        device: Arc<dyn device::Device>,
        eviction_policy: &Arc<Mutex<EvictionPolicyWrapper>>,
    ) -> Self {
        let (sender, receiver): (Sender<ReadRequest>, Receiver<ReadRequest>) = unbounded();
        let mut handles = Vec::with_capacity(num_readers);

        for id in 0..num_readers {
            let rx_clone = receiver.clone();
            let reader = Reader::new(id, rx_clone, device.clone(), eviction_policy);
            let handle = thread::spawn(move || reader.run());
            handles.push(handle);
        }

        Self { sender, handles }
    }

    /// Send a message to the reader pool
    pub async fn send(&self, message: ReadRequest) -> std::io::Result<()> {
        self.sender.send_async(message).await.map_err(|e| {
            std::io::Error::new(
                std::io::ErrorKind::Other,
                format!("WriterPool::send_async failed: {}", e),
            )
        })
    }

    /// Stop the pool and wait for all reader threads to finish.
    pub fn stop(self) {
        drop(self.sender); // Close the channel
        for handle in self.handles {
            if let Err(e) = handle.join() {
                // A panic occurred — e is a Box<dyn Any + Send + 'static>
                if let Some(msg) = e.downcast_ref::<&str>() {
                    tracing::error!("Reader thread panicked with message: {}", msg);
                } else if let Some(msg) = e.downcast_ref::<String>() {
                    tracing::error!("Reader thread panicked with message: {}", msg);
                } else {
                    tracing::error!("Reader thread panicked with unknown payload.");
                }
            }
        }
    }
}
