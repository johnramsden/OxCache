use crossbeam::channel::{Receiver, Sender, unbounded};
use std::thread::{self, JoinHandle};

/// Represents an individual writer thread
struct Writer {
    id: usize,
    receiver: Receiver<String>,
}

impl Writer {
    fn new(id: usize, receiver: Receiver<String>) -> Self {
        Self { id, receiver }
    }

    fn run(self) {
        println!("Writer {} started", self.id);
        while let Ok(msg) = self.receiver.recv() {
            println!("Writer {} processing: {}", self.id, msg);
            // TODO: Real logic
        }
        println!("Writer {} exiting", self.id);
    }
}

/// Pool of writer threads sharing a single receiver
pub struct WriterPool {
    sender: Sender<String>,
    handles: Vec<JoinHandle<()>>,
}

impl WriterPool {
    /// Creates and starts the writer pool with a given number of threads
    pub fn start(num_writers: usize) -> Self {
        let (sender, receiver): (Sender<String>, Receiver<String>) = unbounded();
        let mut handles = Vec::with_capacity(num_writers);

        for id in 0..num_writers {
            let rx_clone = receiver.clone();
            let writer = Writer::new(id, rx_clone);
            let handle = thread::spawn(move || writer.run());
            handles.push(handle);
        }

        Self { sender, handles }
    }

    /// Send a message to the writer pool
    pub fn send(&self, message: String) {
        let _ = self.sender.send(message);
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
