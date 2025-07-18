use std::sync::Arc;
use flume::{Receiver, Sender, unbounded};
use std::thread::{self, JoinHandle};
use bytes::{Bytes, BytesMut};
use crate::{cache, device};
use crate::writerpool::WriteResponse;

#[derive(Debug)]
pub struct ReadResponse {
    pub data: std::io::Result<Bytes>,
}

#[derive(Debug)]
pub struct ReadRequest {
    pub location: cache::bucket::ChunkLocation,
    pub responder: Sender<ReadResponse>,
}

/// Represents an individual reader thread
struct Reader {
    device: Arc<dyn device::Device>,
    id: usize,
    receiver: Receiver<ReadRequest>,
}

impl Reader {
    fn new(id: usize, receiver: Receiver<ReadRequest>, device: Arc<dyn device::Device>) -> Self {
        Self { device, id, receiver }
    }

    fn run(self) {
        println!("Reader {} started", self.id);
        while let Ok(msg) = self.receiver.recv() {
            // println!("Reader {} processing: {:?}", self.id, msg);
            let resp = ReadResponse { data: self.device.read(msg.location) };
            let snd = msg.responder.send(resp);
            if snd.is_err() {
                eprintln!("Failed to send response from writer: {}", snd.err().unwrap());
            }
        }
        println!("Reader {} exiting", self.id);
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
    pub fn start(num_readers: usize, device: Arc<dyn device::Device>) -> Self {
        let (sender, receiver): (Sender<ReadRequest>, Receiver<ReadRequest>) = unbounded();
        let mut handles = Vec::with_capacity(num_readers);

        for id in 0..num_readers {
            let rx_clone = receiver.clone();
            let reader = Reader::new(id, rx_clone, device.clone());
            let handle = thread::spawn(move || reader.run());
            handles.push(handle);
        }

        Self { sender, handles }
    }

    /// Send a message to the reader pool
    pub async fn send(&self, message: ReadRequest) -> std::io::Result<()> {
        self.sender.send_async(message).await.map_err(|e| {
            std::io::Error::new(std::io::ErrorKind::Other, format!("WriterPool::send_async failed: {}", e))
        })
    }

    /// Stop the pool and wait for all reader threads to finish.
    pub fn stop(self) {
        drop(self.sender); // Close the channel
        for handle in self.handles {
            if let Err(e) = handle.join() {
                // A panic occurred — e is a Box<dyn Any + Send + 'static>
                if let Some(msg) = e.downcast_ref::<&str>() {
                    eprintln!("Reader thread panicked with message: {}", msg);
                } else if let Some(msg) = e.downcast_ref::<String>() {
                    eprintln!("Reader thread panicked with message: {}", msg);
                } else {
                    eprintln!("Reader thread panicked with unknown payload.");
                }
            }
        }
    }
}
