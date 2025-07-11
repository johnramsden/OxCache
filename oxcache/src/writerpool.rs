use std::sync::Arc;
use flume::{Receiver, Sender, unbounded};
use std::thread::{self, JoinHandle};
use crate::{cache, device};

#[derive(Debug)]
pub struct WriteResponse {
    pub location: std::io::Result<cache::bucket::ChunkLocation>,
}

#[derive(Debug)]
pub struct WriteRequest {
    pub data: Vec<u8>,
    pub responder: Sender<WriteResponse>,
}

/// Represents an individual writer thread
struct Writer {
    device: Arc<dyn device::Device>,
    id: usize,
    receiver: Receiver<WriteRequest>,
}

impl Writer {
    fn new(id: usize, receiver: Receiver<WriteRequest>, device: Arc<dyn device::Device>) -> Self {
        Self {
            id,
            receiver,
            device
        }
    }

    fn run(self) {
        println!("Writer {} started", self.id);
        while let Ok(msg) = self.receiver.recv() {
            // println!("Writer {} processing: {:?}", self.id, msg);
            let resp = WriteResponse { location: self.device.append(msg.data) };
            let snd = msg.responder.send(resp);
            if snd.is_err() {
                eprintln!("Failed to send response from writer: {}", snd.err().unwrap());
            }
        }
        println!("Writer {} exiting", self.id);
    }
}

/// Pool of writer threads sharing a single receiver
#[derive(Debug)]
pub struct WriterPool {
    sender: Sender<WriteRequest>,
    handles: Vec<JoinHandle<()>>,
}

impl WriterPool {
    /// Creates and starts the writer pool with a given number of threads
    pub fn start(num_writers: usize, device: Arc<dyn device::Device>) -> Self {
        
        let (sender, receiver): (Sender<WriteRequest>, Receiver<WriteRequest>) = unbounded();
        let mut handles = Vec::with_capacity(num_writers);

        for id in 0..num_writers {
            let rx_clone = receiver.clone();
            let writer = Writer::new(id, rx_clone, device.clone());
            let handle = thread::spawn(move || writer.run());
            handles.push(handle);
        }

        Self { sender, handles }
    }

    /// Send a message to the writer pool
    pub async fn send(&self, message: WriteRequest) -> std::io::Result<()> {
        self.sender.send_async(message).await.map_err(|e| {
            std::io::Error::new(std::io::ErrorKind::Other, format!("WriterPool::send_async failed: {}", e))
        })
    }

    /// Stop the pool and wait for all writer threads to finish.
    pub fn stop(self) {
        drop(self.sender); // Close the channel
        for handle in self.handles {
            if let Err(e) = handle.join() {
                // A panic occurred — e is a Box<dyn Any + Send + 'static>
                if let Some(msg) = e.downcast_ref::<&str>() {
                    eprintln!("Writer thread panicked with message: {}", msg);
                } else if let Some(msg) = e.downcast_ref::<String>() {
                    eprintln!("Writer thread panicked with message: {}", msg);
                } else {
                    eprintln!("Writer thread panicked with unknown payload.");
                }
            }
        }
    }
}
