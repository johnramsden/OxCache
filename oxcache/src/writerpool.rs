use crate::metrics::{METRICS, MetricType};
use crate::{
    cache::{self},
    device,
    eviction::EvictionPolicyWrapper,
};
use bytes::Bytes;
use flume::{Receiver, Sender, unbounded};
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};

#[derive(Debug)]
pub struct WriteResponse {
    pub location: std::io::Result<cache::bucket::ChunkLocation>,
}

#[derive(Debug)]
pub struct WriteRequest {
    pub data: Bytes,
    pub responder: Sender<WriteResponse>,
}

#[derive(Debug)]
pub struct WriteRequestInternal {
    pub data: Bytes,
    pub responder: Sender<WriteResponse>,
    pub update_lru: bool,
}

fn request_update_lru(req: WriteRequest) -> WriteRequestInternal {
    WriteRequestInternal { data: req.data, responder: req.responder, update_lru: true }
}

fn request_no_update_lru(req: WriteRequest) -> WriteRequestInternal {
    WriteRequestInternal { data: req.data, responder: req.responder, update_lru: false }
}

/// Represents an individual writer thread
struct Writer {
    device: Arc<dyn device::Device>,
    id: usize,
    receiver: Receiver<WriteRequestInternal>,
    eviction: Arc<Mutex<EvictionPolicyWrapper>>,
}

impl Writer {
    fn new(
        id: usize,
        receiver: Receiver<WriteRequestInternal>,
        device: Arc<dyn device::Device>,
        eviction: &Arc<Mutex<EvictionPolicyWrapper>>,
    ) -> Self {
        Self {
            id,
            receiver,
            device,
            eviction: eviction.clone(),
        }
    }

    fn run(self) {
        tracing::debug!("Writer {} started", self.id);
        while let Ok(msg) = self.receiver.recv() {
            // println!("Writer {} processing: {:?}", self.id, msg);

            let start = std::time::Instant::now();

            let result = self.device.append(msg.data).inspect(|loc| {
                let mtx = Arc::clone(&self.eviction);

                if msg.update_lru {
                    let mut policy = mtx.lock().unwrap();
                    policy.write_update(loc.clone());
                }
            });
            METRICS.update_metric_histogram_latency(
                "device_write_latency_ms",
                start.elapsed(),
                MetricType::MsLatency,
            );

            let resp = WriteResponse { location: result };
            let snd = msg.responder.send(resp);
            if snd.is_err() {
                tracing::error!(
                    "Failed to send response from writer: {}",
                    snd.err().unwrap()
                );
            }
        }
        tracing::info!("Writer {} exiting", self.id);
    }
}

/// Pool of writer threads sharing a single receiver
#[derive(Debug)]
pub struct WriterPool {
    sender: Sender<WriteRequestInternal>,
    handles: Vec<JoinHandle<()>>,
}

impl WriterPool {
    /// Creates and starts the writer pool with a given number of threads
    pub fn start(
        num_writers: usize,
        device: Arc<dyn device::Device>,
        eviction_policy: &Arc<Mutex<EvictionPolicyWrapper>>,
    ) -> Self {
        let (sender, receiver): (Sender<WriteRequestInternal>, Receiver<WriteRequestInternal>) = unbounded();
        let mut handles = Vec::with_capacity(num_writers);

        for id in 0..num_writers {
            let rx_clone = receiver.clone();
            let writer = Writer::new(id, rx_clone, device.clone(), eviction_policy);
            let handle = thread::spawn(move || writer.run());
            handles.push(handle);
        }

        Self { sender, handles }
    }

    /// Send a message to the writer pool
    pub async fn send(&self, message: WriteRequest) -> std::io::Result<()> {
        self.sender.send_async(request_update_lru(message)).await.map_err(|e| {
            std::io::Error::new(
                std::io::ErrorKind::Other,
                format!("WriterPool::send_async failed: {}", e),
            )
        })
    }

    pub async fn send_no_update_lru(&self, message: WriteRequest) -> std::io::Result<()> {
        self.sender.send_async(request_no_update_lru(message)).await.map_err(|e| {
            std::io::Error::new(
                std::io::ErrorKind::Other,
                format!("WriterPool::send_async failed: {}", e),
            )
        })
    }

    /// Stop the pool and wait for all writer threads to finish.
    pub fn stop(self) {
        drop(self.sender); // Close the channel
        for handle in self.handles {
            if let Err(e) = handle.join() {
                // A panic occurred — e is a Box<dyn Any + Send + 'static>
                if let Some(msg) = e.downcast_ref::<&str>() {
                    tracing::error!("Writer thread panicked with message: {}", msg);
                } else if let Some(msg) = e.downcast_ref::<String>() {
                    tracing::error!("Writer thread panicked with message: {}", msg);
                } else {
                    tracing::error!("Writer thread panicked with unknown payload.");
                }
            }
        }
    }
}
