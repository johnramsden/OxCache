use crossbeam::channel::{unbounded, Receiver, Sender};
use std::thread::{self, JoinHandle};

/// Represents an individual reader thread
struct Reader {
    id: usize,
    receiver: Receiver<String>,
}

impl Reader {
    fn new(id: usize, receiver: Receiver<String>) -> Self {
        Self { id, receiver }
    }

    fn run(self) {
        while let Ok(msg) = self.receiver.recv() {
            println!("Reader {} processing: {}", self.id, msg);
            // TODO: Real logic
        }
        println!("Reader {} exiting", self.id);
    }
}

/// Pool of writer threads sharing a single receiver
pub struct ReaderPool {
    sender: Sender<String>,
    handles: Vec<JoinHandle<()>>,
}

impl ReaderPool {
    /// Creates and starts the reader pool with a given number of threads
    pub fn start(num_readers: usize) -> Self {
        let (sender, receiver): (Sender<String>, Receiver<String>) = unbounded();
        let mut handles = Vec::with_capacity(num_readers);

        for id in 0..num_readers {
            let rx_clone = receiver.clone();
            let reader = Reader::new(id, rx_clone);
            let handle = thread::spawn(move || reader.run());
            handles.push(handle);
        }

        Self { sender, handles }
    }

    /// Send a message to the reader pool
    pub fn send(&self, message: String) {
        let _ = self.sender.send(message);
    }

    /// Stop the pool and wait for all reader threads to finish.
    pub fn stop(self) {
        drop(self.sender); // Close the channel
        for handle in self.handles {
            match handle.join() {
                Ok(()) => {
                    println!("Evictor thread exited cleanly.");
                }
                Err(e) => {
                    // A panic occurred — e is a Box<dyn Any + Send + 'static>
                    if let Some(msg) = e.downcast_ref::<&str>() {
                        eprintln!("Evictor thread panicked with message: {}", msg);
                    } else if let Some(msg) = e.downcast_ref::<String>() {
                        eprintln!("Evictor thread panicked with message: {}", msg);
                    } else {
                        eprintln!("Evictor thread panicked with unknown payload.");
                    }
                }
            }
        }
    }
}
