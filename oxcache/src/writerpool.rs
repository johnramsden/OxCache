use crate::metrics::{METRICS, MetricType};
use crate::{
    cache::{self},
    device,
    eviction::EvictionPolicyWrapper,
};
use bytes::Bytes;
use flume::{Receiver, Sender, unbounded};
use std::sync::atomic::{Ordering};
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

#[derive(Debug)]
pub struct BatchWriteRequest {
    pub data: Vec<Bytes>,
    pub responder: Sender<BatchWriteResponse>,
}

#[derive(Debug)]
pub struct BatchWriteResponse {
    pub locations: Vec<std::io::Result<cache::bucket::ChunkLocation>>,
}

fn request_update_lru(req: WriteRequest) -> WriteRequestInternal {
    WriteRequestInternal {
        data: req.data,
        responder: req.responder,
        update_lru: true,
    }
}

fn request_no_update_lru(req: WriteRequest) -> WriteRequestInternal {
    WriteRequestInternal {
        data: req.data,
        responder: req.responder,
        update_lru: false,
    }
}

/// Represents an individual writer thread
struct Writer {
    device: Arc<dyn device::Device>,
    id: usize,
    receiver: Receiver<WriteRequestInternal>,
    priority_receiver: Receiver<BatchWriteRequest>,
    eviction: Arc<Mutex<EvictionPolicyWrapper>>,
    priority_only: bool,
}

impl Writer {
    fn new(
        id: usize,
        receiver: Receiver<WriteRequestInternal>,
        priority_receiver: Receiver<BatchWriteRequest>,
        device: Arc<dyn device::Device>,
        eviction: &Arc<Mutex<EvictionPolicyWrapper>>,
        priority_only: bool,
    ) -> Self {
        Self {
            id,
            receiver,
            priority_receiver,
            device,
            eviction: eviction.clone(),
            priority_only,
        }
    }

    fn receive_all(&self) {
        loop {
            // Prioritize batch requests (eviction) over regular requests
            let batch_msg = self.priority_receiver.try_recv();
            if let Ok(batch_req) = batch_msg {
                self.process_batch_request(batch_req);
                continue;
            };

            // If no priority request, handle regular requests with timeout
            match self
                .receiver
                .recv_timeout(std::time::Duration::from_millis(10))
            {
                Ok(msg) => {
                    self.process_regular_request(msg);
                }
                Err(flume::RecvTimeoutError::Timeout) => {
                    // Check for batch requests again after timeout
                    continue;
                }
                Err(flume::RecvTimeoutError::Disconnected) => {
                    // Check if priority channel is also disconnected
                    if self.priority_receiver.is_disconnected() {
                        break;
                    }
                }
            }
        }
    }
    fn receive_priority(&self) {
        while let Ok(batch_msg) = self.priority_receiver.recv() {
            self.process_batch_request(batch_msg);
        }
    }

    fn run(self) {
        tracing::info!("Writer {} started", self.id);
        if self.priority_only {
            self.receive_priority();
        } else {
            self.receive_all();
        }
        tracing::info!("Writer {} exiting", self.id);
    }

    fn process_regular_request(&self, msg: WriteRequestInternal) {
        let start = std::time::Instant::now();

        let result = self.device.append(msg.data).inspect(|loc| {
            if msg.update_lru {
                let mtx = Arc::clone(&self.eviction);
                let policy = mtx.lock().unwrap();

                match &*policy {
                    EvictionPolicyWrapper::Promotional(p) => {
                        // Clone Arc references before dropping lock
                        let zone_idx = loc.zone as usize;
                        let nr_chunks = p.nr_chunks_per_zone;
                        let counters = Arc::clone(&p.zone_chunk_counts);
                        drop(policy);

                        // Do atomic increment outside lock
                        if zone_idx < counters.len() {
                            let count = counters[zone_idx].fetch_add(1, Ordering::Relaxed);

                            // Only re-acquire lock when zone becomes full
                            if count + 1 == nr_chunks {
                                let mut policy = mtx.lock().unwrap();
                                policy.write_update(loc.clone());
                            }
                        }
                    }
                    EvictionPolicyWrapper::Chunk(_) => {
                        drop(policy);
                        let mut policy = mtx.lock().unwrap();
                        policy.write_update(loc.clone());
                    }
                }
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

    fn process_batch_request(&self, batch_req: BatchWriteRequest) {
        let data_len = batch_req.data.len(); // Store length before moving
        let mut locations = Vec::with_capacity(data_len);

        for (_i, data) in batch_req.data.into_iter().enumerate() {
            // Use eviction bypass for priority batch requests (eviction writes)
            let result = self.device.append_with_eviction_bypass(data, true);

            if let Ok(ref loc) = result {
                let mtx = Arc::clone(&self.eviction);
                let policy = mtx.lock().unwrap();

                match &*policy {
                    EvictionPolicyWrapper::Promotional(p) => {
                        // Clone Arc references before dropping lock
                        let zone_idx = loc.zone as usize;
                        let nr_chunks = p.nr_chunks_per_zone;
                        let counters = Arc::clone(&p.zone_chunk_counts);
                        drop(policy);

                        // Do atomic increment outside lock
                        if zone_idx < counters.len() {
                            let count = counters[zone_idx].fetch_add(1, Ordering::Relaxed);

                            // Only re-acquire lock when zone becomes full
                            if count + 1 == nr_chunks {
                                let mut policy = mtx.lock().unwrap();
                                policy.write_update(loc.clone());
                            }
                        }
                    }
                    EvictionPolicyWrapper::Chunk(_) => {
                        drop(policy);
                        let mut policy = mtx.lock().unwrap();
                        policy.write_update(loc.clone());
                    }
                }
            } else {
                tracing::error!("Failed to append: {:?}", result);
            }

            locations.push(result);
        }

        let resp = BatchWriteResponse { locations };
        let snd = batch_req.responder.send(resp);
        if snd.is_err() {
            tracing::error!("Failed to send batch response from writer");
        }
    }
}

/// Pool of writer threads sharing a single receiver
#[derive(Debug)]
pub struct WriterPool {
    sender: Sender<WriteRequestInternal>,
    priority_sender: Sender<BatchWriteRequest>,
    handles: Vec<JoinHandle<()>>,
}

impl WriterPool {
    /// Creates and starts the writer pool with a given number of threads
    pub fn start(
        num_writers: usize,
        device: Arc<dyn device::Device>,
        eviction_policy: &Arc<Mutex<EvictionPolicyWrapper>>,
    ) -> Self {
        let (sender, receiver): (Sender<WriteRequestInternal>, Receiver<WriteRequestInternal>) =
            unbounded();
        let (priority_sender, priority_receiver): (
            Sender<BatchWriteRequest>,
            Receiver<BatchWriteRequest>,
        ) = unbounded();
        let mut handles = Vec::with_capacity(num_writers);

        // Regular writers
        for id in 0..=num_writers {
            let rx_clone = receiver.clone();
            let priority_rx_clone = priority_receiver.clone();

            // Will create ONE priority writer (last) via id == num_writers
            let writer = Writer::new(
                id,
                rx_clone,
                priority_rx_clone,
                device.clone(),
                eviction_policy,
                id == num_writers,
            );
            let handle = thread::spawn(move || writer.run());
            handles.push(handle);
        }

        Self {
            sender,
            priority_sender,
            handles,
        }
    }

    /// Send a message to the writer pool
    pub async fn send(&self, message: WriteRequest) -> std::io::Result<()> {
        // For now, bypass capacity checking for regular writes since the semaphore
        // approach was causing issues. The reservation system is primarily for
        // preventing eviction deadlocks, not for general capacity management.
        self.sender
            .send_async(request_update_lru(message))
            .await
            .map_err(|e| {
                std::io::Error::new(
                    std::io::ErrorKind::Other,
                    format!("WriterPool::send_async failed: {}", e),
                )
            })
    }

    pub async fn send_no_update_lru(&self, message: WriteRequest) -> std::io::Result<()> {
        // For now, bypass capacity checking for regular writes since the semaphore
        // approach was causing issues. The reservation system is primarily for
        // preventing eviction deadlocks, not for general capacity management.
        self.sender
            .send_async(request_no_update_lru(message))
            .await
            .map_err(|e| {
                std::io::Error::new(
                    std::io::ErrorKind::Other,
                    format!("WriterPool::send_async failed: {}", e),
                )
            })
    }

    /// Send a prioritized batch request for eviction writes
    pub async fn send_priority_batch(&self, message: BatchWriteRequest) -> std::io::Result<()> {
        self.priority_sender.send_async(message).await.map_err(|e| {
            std::io::Error::new(
                std::io::ErrorKind::Other,
                format!("WriterPool::send_priority_batch failed: {}", e),
            )
        })
    }

    /// Stop the pool and wait for all writer threads to finish.
    pub fn stop(self) {
        drop(self.sender); // Close the regular channel
        drop(self.priority_sender); // Close the priority channel
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
