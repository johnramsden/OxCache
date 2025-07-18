use std::sync::{
    atomic::{AtomicBool, Ordering}, Arc, Mutex
};
use std::thread::{self, JoinHandle};
use std::time::Duration;
use core::fmt::Debug;
use std::collections::{HashMap, VecDeque};
use crate::cache::bucket::ChunkLocation;
use crate::device;
use crate::device::Device;

use lru::LruCache;
use std::num::NonZeroUsize;

pub enum EvictionPolicyWrapper {
    Dummy(DummyEvictionPolicy),
    Promotional(PromotionalEvictionPolicy),
    Chunk(ChunkEvictionPolicy),
}

impl EvictionPolicyWrapper {
    pub fn new(identifier: &str, high_water: usize, low_water: usize, nr_zones: usize, nr_chunks_per_zone: usize) -> tokio::io::Result<Self> {
        match identifier.to_lowercase().as_str() {
            "dummy" => Ok(EvictionPolicyWrapper::Dummy(DummyEvictionPolicy::new(high_water, low_water, nr_zones, nr_chunks_per_zone))),
            "chunk" => Ok(EvictionPolicyWrapper::Chunk(ChunkEvictionPolicy::new(high_water, low_water, nr_zones, nr_chunks_per_zone))),
            "promotional" => Ok(EvictionPolicyWrapper::Promotional(PromotionalEvictionPolicy::new(high_water, low_water, nr_zones, nr_chunks_per_zone))),
            _ => Err(std::io::Error::new(std::io::ErrorKind::InvalidInput, identifier)),
        }
    }
    
    pub fn write_update(&mut self, chunk: ChunkLocation) {
        match self { 
            EvictionPolicyWrapper::Dummy(dummy) => dummy.write_update(chunk),
            EvictionPolicyWrapper::Promotional(promotional) => promotional.write_update(chunk),
            EvictionPolicyWrapper::Chunk(c) => c.write_update(chunk),
        }
    }    
    pub fn read_update(&mut self, chunk: ChunkLocation) {
        match self {
            EvictionPolicyWrapper::Dummy(dummy) => dummy.read_update(chunk),
            EvictionPolicyWrapper::Promotional(promotional) => promotional.read_update(chunk),
            EvictionPolicyWrapper::Chunk(c) => c.read_update(chunk),
        }
    }
}

pub trait EvictionPolicy: Send + Sync {
    type Target: Clone + Send + Sync + 'static;

    fn write_update(&mut self, chunk: ChunkLocation);
    fn read_update(&mut self, chunk: ChunkLocation);
    fn get_evict_targets(&mut self) -> Vec<Self::Target>;
}

pub struct DummyEvictionPolicy {
    high_water: usize,
    low_water: usize,
    nr_zones: usize,
    nr_chunks_per_zone: usize
}

impl DummyEvictionPolicy {
    pub fn new(high_water: usize, low_water: usize, nr_zones: usize, nr_chunks_per_zone: usize) -> Self {
        Self {
            high_water, low_water, nr_zones, nr_chunks_per_zone
        }
    }
}

impl EvictionPolicy for DummyEvictionPolicy {
    type Target = usize;
    fn write_update(&mut self, chunk: ChunkLocation) {}

    fn read_update(&mut self, chunk: ChunkLocation) {}

    fn get_evict_targets(&mut self) -> Vec<Self::Target> {
        unimplemented!();
    }
}

pub struct PromotionalEvictionPolicy {
    high_water: usize,
    low_water: usize,
    nr_zones: usize,
    nr_chunks_per_zone: usize,
    lru: LruCache<usize, ()>
}

impl PromotionalEvictionPolicy {
    pub fn new(high_water: usize, low_water: usize, nr_zones: usize, nr_chunks_per_zone: usize) -> Self {
        let lru = LruCache::unbounded();
        Self {
            high_water, low_water, nr_zones, nr_chunks_per_zone, lru
        }
    }
}

impl EvictionPolicy for PromotionalEvictionPolicy {
    /// Promotional LRU
    /// Performs LRU based on full zones
    type Target = usize;
    
    fn write_update(&mut self, chunk: ChunkLocation) {
        // assert!(!self.lru.contains(&chunk.zone)); // TODO: Fails sometimes
        
        // We only want to put it in the LRU once the zone is full
        if (chunk.index as usize == self.nr_chunks_per_zone-1) {
            self.lru.put(chunk.zone, ());
        }
    }

    fn read_update(&mut self, chunk: ChunkLocation) {
        // We only want to put it in the LRU once the zone is full
        // If it has filled before we want to update every time "promoting" it
        // Following this, only zones that have filled prior are updated
        if self.lru.contains(&chunk.zone) {
            self.lru.put(chunk.zone, ());
        }
    }

    fn get_evict_targets(&mut self) -> Vec<Self::Target> {
        let high_water_mark =  self.nr_zones-self.high_water;
        if self.lru.len() < high_water_mark {
            return vec![];
        }

        let mut targets = Vec::with_capacity(self.lru.len() - self.low_water);
        let low_water_mark = self.nr_zones-self.low_water;
        while self.lru.len() >= low_water_mark {
            targets.push(self.lru.pop_lru().unwrap().0)
        }

        targets
    }
}

pub struct ChunkEvictionPolicy {
    high_water: usize,
    low_water: usize,
    nr_zones: usize,
    nr_chunks_per_zone: usize,
    lru: LruCache<ChunkLocation, ()>
}

impl ChunkEvictionPolicy {
    pub fn new(high_water: usize, low_water: usize, nr_zones: usize, nr_chunks_per_zone: usize) -> Self {
        Self {
            high_water, low_water, nr_zones, nr_chunks_per_zone, lru: LruCache::unbounded(),
        }
    }
}

impl EvictionPolicy for ChunkEvictionPolicy {
    type Target = ChunkLocation;
    fn write_update(&mut self, chunk: ChunkLocation) {
        self.lru.put(chunk, ());
    }

    fn read_update(&mut self, chunk: ChunkLocation) {
        assert!(self.lru.contains(&chunk));
        self.lru.put(chunk, ());
    }

    fn get_evict_targets(&mut self) -> Vec<Self::Target> {
        let high_water_mark =  self.nr_zones*self.nr_chunks_per_zone-self.high_water;
        if self.lru.len() < high_water_mark {
            return vec![];
        }

        let mut targets = Vec::with_capacity(self.lru.len() - self.low_water);
        let low_water_mark = self.nr_zones*self.nr_chunks_per_zone-self.low_water;
        while self.lru.len() >= low_water_mark {
            targets.push(self.lru.pop_lru().unwrap().0)
        }

        targets
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

                    device.evict().expect("Eviction failed");

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

#[cfg(test)]
mod tests {
    use std::hash::Hash;
    use super::*;
    
    fn compare_order<T>(lru: &mut LruCache<T, ()>, order: &VecDeque<T>) where T: Hash + Debug, T: Eq, T: PartialEq {
        assert_eq!(order.len(), lru.len(), "Expected len = {}, but got len = {}", order.len(), lru.len());
        for (index, ((lru_key, _), order_item)) in lru.iter().zip(order.iter()).enumerate() {
            assert_eq!(order_item, lru_key, "Expected {:?}, but got {:?}", order_item, lru_key);
        }
    }

    #[test]
    fn test_chunk_update_ordering() {
        let mut policy = ChunkEvictionPolicy::new(1, 3, 2, 2);
    
        // zone=[_,_,_,_], lru=()
        let c = ChunkLocation::new(1, 0);
        let mut order: VecDeque<ChunkLocation> = VecDeque::new();
        order.push_front(c.clone());
        policy.write_update(c);
        // zone=[_,_,(1,0),_], lru=((1,0))
        compare_order(&mut policy.lru, &order);
        
        let et = policy.get_evict_targets();
        let expect_none: VecDeque<ChunkLocation> = VecDeque::new();
        assert_eq!(expect_none, et, "Expected = {:?}, but got {:?}", expect_none, et);
        
        let c = ChunkLocation::new(1, 1);
        policy.write_update(c.clone());
        // zone=[_,_,(1,0),(1,1)], lru=((1,0),(1,1))
        order.push_front(c);
        compare_order(&mut policy.lru, &order);
        let et = policy.get_evict_targets();
        assert_eq!(expect_none, et, "Expected = {:?}, but got {:?}", expect_none, et);  
        
        let c = ChunkLocation::new(1, 0);
        // Expect order to update
        policy.read_update(c.clone());
        // zone=[_,_,(1,0),(1,1)], lru=((1,1),(1,0))
        let c = order.pop_back().unwrap();
        order.push_front(c);
        compare_order(&mut policy.lru, &order);
        let et = policy.get_evict_targets();
        assert_eq!(expect_none, et, "Expected = {:?}, but got {:?}", expect_none, et);

        let c = ChunkLocation::new(0, 0);
        policy.write_update(c.clone());
        // zone=[(0,0),_,(1,0),(1,1)], lru=((1,0),(1,1),(0,0))
        order.push_front(c);
        compare_order(&mut policy.lru, &order);
        let et = policy.get_evict_targets();
        let order = order.clone().into_iter().rev().collect::<VecDeque<ChunkLocation>>();
        assert_eq!(order, et, "Expected = {:?}, but got {:?}", order, et);
    }
    
    #[test]
    fn test_promotional_update_ordering() {
        let mut policy = PromotionalEvictionPolicy::new(1, 3, 4, 2);

        // zone=[_,_,_,_], lru=()
        let mut order: VecDeque<usize> = VecDeque::new();
        policy.write_update(ChunkLocation::new(3, 0));
        compare_order(&mut policy.lru, &order);
        let et = policy.get_evict_targets();
        let expect_none: Vec<usize> = vec![];
        assert_eq!(expect_none, et, "Expected = {:?}, but got {:?}", expect_none, et);
        
        // zone=[_,_,_,_], lru=()
        policy.write_update(ChunkLocation::new(3, 1));
        // zone=[_,_,_,3], lru=(3)
        order.push_back(3);
        compare_order(&mut policy.lru, &order);
        let et = policy.get_evict_targets();
        assert_eq!(expect_none, et, "Expected = {:?}, but got {:?}", expect_none, et);
        
        policy.write_update(ChunkLocation::new(1, 0));
        // There should be no change
        // zone=[_,_,_,3], lru=(3)
        compare_order(&mut policy.lru, &order);
        
        policy.write_update(ChunkLocation::new(1, 1));
        // zone=[_,1,_,3], lru=(3, 1)
        order.push_front(1);
        compare_order(&mut policy.lru, &order);
        let et = policy.get_evict_targets();
        assert_eq!(expect_none, et, "Expected = {:?}, but got {:?}", expect_none, et);
        
        policy.write_update(ChunkLocation::new(2, 0));
        policy.write_update(ChunkLocation::new(2, 1));
        order.push_front(2);
        // zone=[_,1,2,3], lru=(3, 1, 2)
        compare_order(&mut policy.lru, &order);

        // Should update in place, and adjust order
        policy.read_update(ChunkLocation::new(3, 1));
        let c = order.pop_back().unwrap();
        order.push_front(c);
        // zone=[_,1,2,3], lru=(1, 2, 3)
        compare_order(&mut policy.lru, &order);

        let et = policy.get_evict_targets();
        let expect = VecDeque::from(vec![1, 2, 3]);
        assert_eq!(expect, et, "Expected = {:?}, but got {:?}", expect, et);

        compare_order(&mut policy.lru, &VecDeque::from(vec![]));
    }
}