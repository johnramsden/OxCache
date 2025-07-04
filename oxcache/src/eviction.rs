use std::sync::{
    atomic::{AtomicBool, Ordering}, Arc, Mutex
};
use std::thread::{self, JoinHandle};
use std::time::Duration;
use core::fmt::Debug;
use crate::cache::bucket::ChunkLocation;
use crate::device;
use crate::device::Device;

pub enum EvictionPolicyWrapper {
    Dummy(DummyEvictionPolicy),
    Promotional(PromotionalEvictionPolicy),
    Chunk(ChunkEvictionPolicy),
}

impl EvictionPolicyWrapper {
    pub fn new(identifier: &str, num_evict: usize, high_water: usize, low_water: usize) -> tokio::io::Result<Self> {
        match identifier.to_lowercase().as_str() {
            "dummy" => Ok(EvictionPolicyWrapper::Dummy(DummyEvictionPolicy::new(num_evict, high_water, low_water))),
            "chunk" => Ok(EvictionPolicyWrapper::Chunk(ChunkEvictionPolicy::new(num_evict, high_water, low_water))),
            "promotional" => Ok(EvictionPolicyWrapper::Promotional(PromotionalEvictionPolicy::new(num_evict, high_water, low_water))),
            _ => Err(std::io::Error::new(std::io::ErrorKind::InvalidInput, identifier)),
        }
    }
    
    pub fn write_update(&self, chunk: ChunkLocation) {
        match self { 
            EvictionPolicyWrapper::Dummy(dummy) => dummy.write_update(chunk),
            EvictionPolicyWrapper::Promotional(promotional) => promotional.write_update(chunk),
            EvictionPolicyWrapper::Chunk(c) => c.write_update(chunk),
        }
    }    
    pub fn read_update(&self, chunk: ChunkLocation) {
        match self {
            EvictionPolicyWrapper::Dummy(dummy) => dummy.read_update(chunk),
            EvictionPolicyWrapper::Promotional(promotional) => promotional.read_update(chunk),
            EvictionPolicyWrapper::Chunk(c) => c.read_update(chunk),
        }
    }
}

pub trait EvictionPolicy: Send + Sync {
    type Target: Clone + Send + Sync + 'static;

    fn write_update(&self, chunk: ChunkLocation);
    fn read_update(&self, chunk: ChunkLocation);
    fn get_evict_targets(&self) -> Option<Vec<Self::Target>>;
    fn get_evict_target(&self) -> Option<Self::Target>;
}

pub struct DummyEvictionPolicy {
    num_evict: usize,
    high_water: usize,
    low_water: usize
}

impl DummyEvictionPolicy {
    pub fn new(num_evict: usize, high_water: usize, low_water: usize) -> Self {
        Self {
            num_evict, high_water, low_water
        }
    }
}

impl EvictionPolicy for DummyEvictionPolicy {
    type Target = usize;
    fn write_update(&self, chunk: ChunkLocation) {}

    fn read_update(&self, chunk: ChunkLocation) {}

    fn get_evict_targets(&self) -> Option<Vec<Self::Target>> {
        unimplemented!();
    }

    fn get_evict_target(&self) -> Option<Self::Target> {
        unimplemented!();
    }
}

pub struct PromotionalEvictionPolicy {
    num_evict: usize,
    high_water: usize,
    low_water: usize
}

impl PromotionalEvictionPolicy {
    pub fn new(num_evict: usize, high_water: usize, low_water: usize) -> Self {
        Self {
            num_evict, high_water, low_water
        }
    }
}

impl EvictionPolicy for PromotionalEvictionPolicy {
    type Target = usize;
    fn write_update(&self, chunk: ChunkLocation) {
        unimplemented!();
    }

    fn read_update(&self, chunk: ChunkLocation) {
        unimplemented!();
    }

    fn get_evict_targets(&self) -> Option<Vec<Self::Target>> {
        unimplemented!();
    }

    fn get_evict_target(&self) -> Option<Self::Target> {
        unimplemented!();
    }
}

pub struct ChunkEvictionPolicy {
    num_evict: usize,
    high_water: usize,
    low_water: usize
}

impl ChunkEvictionPolicy {
    pub fn new(num_evict: usize, high_water: usize, low_water: usize) -> Self {
        Self {
            num_evict, high_water, low_water
        }
    }
}

impl EvictionPolicy for ChunkEvictionPolicy {
    type Target = ChunkLocation;
    fn write_update(&self, chunk: ChunkLocation) {
        unimplemented!();
    }

    fn read_update(&self, chunk: ChunkLocation) {
        unimplemented!();
    }

    fn get_evict_targets(&self) -> Option<Vec<Self::Target>> {
        unimplemented!();
    }

    fn get_evict_target(&self) -> Option<Self::Target> {
        unimplemented!();
    }
}

pub struct Evictor {
    shutdown: Arc<AtomicBool>,
    handle: Option<JoinHandle<()>>,
    device: Arc<dyn Device>,
}

impl Evictor {
    /// Start the evictor background thread
    pub fn start(device: Arc<dyn Device>) -> Self {
        let shutdown = Arc::new(AtomicBool::new(false));
        let shutdown_clone = Arc::clone(&shutdown);

        let handle = thread::spawn({
            let device = Arc::clone(&device);
            move || {
                while !shutdown_clone.load(Ordering::Relaxed) {
                    // TODO: Put eviction logic here
                    println!("Evictor running...");

                    device.evict(1).expect("Eviction failed");

                    // Sleep to simulate periodic work
                    thread::sleep(Duration::from_secs(5));
                }

                println!("Evictor shutting down.");
            }
        });

        Self {
            shutdown,
            handle: Some(handle),
            device
        }
    }

    /// Request the evictor to stop and wait for thread to finish
    pub fn stop(mut self) {
        self.shutdown.store(true, Ordering::Relaxed);
        if let Some(handle) = self.handle.take() {
            if let Err(e) = handle.join() {
                // A panic occurred — e is a Box<dyn Any + Send + 'static>
                if let Some(msg) = e.downcast_ref::<&str>() {
                    eprintln!("Evictor thread panicked with message: {}", msg);
                } else if let Some(msg) = e.downcast_ref::<String>() {
                    eprintln!("Evictor thread panicked with message: {}", msg);
                } else {
                    eprintln!("Evictor thread panicked with unknown payload.");
                }
            }
        } else {
            eprintln!("Evictor thread was already stopped or never started.");
        }
    }
}
