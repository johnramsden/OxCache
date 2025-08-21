use std::io::ErrorKind;
use crate::cache::{Cache, bucket::ChunkLocation};
use crate::device::Device;
use crate::zone_state::zone_priority_queue::{ZoneIndex, ZonePriorityQueue};
use flume::{Receiver, Sender};
use lru::LruCache;
use nvme::types::{Chunk, Zone};
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicBool, Ordering},
};
use std::thread::{self, JoinHandle};
use std::time::Duration;
use crate::zone_state::zone_priority_queue;

pub enum EvictionPolicyWrapper {
    Dummy(DummyEvictionPolicy),
    Promotional(PromotionalEvictionPolicy),
    Chunk(ChunkEvictionPolicy),
}

#[derive(Debug)]
pub enum EvictTarget {
    Chunk(Vec<ChunkLocation>, Vec<ZoneIndex>),
    Zone(Vec<Zone>),
}

impl EvictionPolicyWrapper {
    pub fn new(
        identifier: &str,
        high_water: Zone,
        low_water: Zone,
        nr_zones: Zone,
        nr_chunks_per_zone: Chunk,
        clean_high_water: Option<Chunk>,
        clean_low_water: Option<Chunk>,
    ) -> tokio::io::Result<Self> {
        match identifier.to_lowercase().as_str() {
            "dummy" => Ok(EvictionPolicyWrapper::Dummy(DummyEvictionPolicy::new())),
            "chunk" => {
                if clean_high_water.is_none() || clean_low_water.is_none() {
                    return Err(std::io::Error::new(ErrorKind::InvalidInput, "Chunk eviction must have clean_high_water and clean_low_water"));
                }
                Ok(EvictionPolicyWrapper::Chunk(ChunkEvictionPolicy::new(
                    high_water,
                    low_water,
                    clean_high_water.unwrap(),
                    clean_low_water.unwrap(),
                    nr_zones,
                    nr_chunks_per_zone,
                )))
            },
            "promotional" => Ok(EvictionPolicyWrapper::Promotional(
                PromotionalEvictionPolicy::new(high_water, low_water, nr_zones, nr_chunks_per_zone),
            )),
            _ => Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                identifier,
            )),
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

    pub fn get_evict_targets(&mut self) -> EvictTarget {
        match self {
            EvictionPolicyWrapper::Dummy(dummy) => EvictTarget::Zone(dummy.get_evict_targets()),
            EvictionPolicyWrapper::Promotional(promotional) => {
                EvictTarget::Zone(promotional.get_evict_targets())
            }
            EvictionPolicyWrapper::Chunk(c) => {
                let et = c.get_evict_targets();
                let ct = c.get_clean_targets();
                EvictTarget::Chunk(et, ct)
            },
        }
    }
}

pub trait EvictionPolicy: Send + Sync {
    type Target: Clone + Send + Sync + 'static;
    type CleanTarget: Clone + Send + Sync + 'static;

    fn write_update(&mut self, chunk: ChunkLocation);
    fn read_update(&mut self, chunk: ChunkLocation);
    fn get_evict_targets(&mut self) -> Self::Target;

    fn get_clean_targets(&mut self) -> Self::CleanTarget;
}

pub struct DummyEvictionPolicy {}

impl DummyEvictionPolicy {
    pub fn new() -> Self {
        Self {}
    }
}

impl EvictionPolicy for DummyEvictionPolicy {
    type Target = Vec<Zone>;
    type CleanTarget = ();
    fn write_update(&mut self, _chunk: ChunkLocation) {}

    fn read_update(&mut self, _chunk: ChunkLocation) {}

    fn get_evict_targets(&mut self) -> Self::Target {
        vec![]
    }

    fn get_clean_targets(&mut self) -> Self::CleanTarget { () }
}

pub struct PromotionalEvictionPolicy {
    high_water: Zone,
    low_water: Zone,
    nr_zones: Zone,
    nr_chunks_per_zone: Chunk,
    lru: LruCache<Zone, ()>,
}

impl PromotionalEvictionPolicy {
    pub fn new(
        high_water: Zone,
        low_water: Zone,
        nr_zones: Zone,
        nr_chunks_per_zone: Chunk,
    ) -> Self {
        let lru = LruCache::unbounded();
        Self {
            high_water,
            low_water,
            nr_zones,
            nr_chunks_per_zone,
            lru,
        }
    }
}

impl EvictionPolicy for PromotionalEvictionPolicy {
    /// Promotional LRU
    /// Performs LRU based on full zones
    type Target = Vec<Zone>;
    type CleanTarget = ();

    fn write_update(&mut self, chunk: ChunkLocation) {
        // assert!(!self.lru.contains(&chunk.zone)); // We cannot assert this because we allow out of order writes

        // We only want to put it in the LRU once the zone is full
        if chunk.index == self.nr_chunks_per_zone - 1 {
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

    fn get_evict_targets(&mut self) -> Self::Target {
        let lru_len = self.lru.len() as Zone;
        let high_water_mark = self.nr_zones - self.high_water;
        if lru_len < high_water_mark {
            return vec![];
        }

        let low_water_mark = self.nr_zones - self.low_water;
        let cap = lru_len - low_water_mark;

        let mut targets = Vec::with_capacity(cap as usize);

        while self.lru.len() as Zone >= low_water_mark {
            targets.push(self.lru.pop_lru().unwrap().0)
        }

        targets
    }

    fn get_clean_targets(&mut self) -> Self::CleanTarget { () }
}

pub struct ChunkEvictionPolicy {
    high_water: Chunk,
    low_water: Chunk,
    nr_zones: Zone,
    nr_chunks_per_zone: Chunk,
    lru: LruCache<ChunkLocation, ()>,
    pq: ZonePriorityQueue
}

impl ChunkEvictionPolicy {
    pub fn new(
        high_water: Chunk,
        low_water: Chunk,
        clean_high_water: Chunk,
        clean_low_water: Chunk,
        nr_zones: Zone,
        nr_chunks_per_zone: Chunk,
    ) -> Self {
        Self {
            high_water,
            low_water,
            nr_zones,
            nr_chunks_per_zone,
            lru: LruCache::unbounded(),
            pq: ZonePriorityQueue::new(nr_zones, clean_high_water, clean_low_water)
        }
    }
}

impl EvictionPolicy for ChunkEvictionPolicy {
    type Target = Vec<ChunkLocation>;
    type CleanTarget = Vec<ZoneIndex>;
    fn write_update(&mut self, chunk: ChunkLocation) {
        self.lru.put(chunk, ());
    }

    fn read_update(&mut self, chunk: ChunkLocation) {
        // assert!(self.lru.contains(&chunk)); // TODO: Race cond with chunk evict?
        self.lru.put(chunk, ());
    }

    fn get_evict_targets(&mut self) -> Self::Target {
        let lru_len = self.lru.len() as Chunk;
        let nr_chunks = self.nr_zones * self.nr_chunks_per_zone;
        let high_water_mark = nr_chunks - self.high_water;
        if lru_len < high_water_mark {
            return vec![];
        }

        let low_water_mark = nr_chunks - self.low_water;
        let cap = lru_len - low_water_mark;

        let mut targets = Vec::with_capacity(cap as usize);
        while self.lru.len() as Chunk >= low_water_mark {
            let targ = self.lru.pop_lru().unwrap().0;
            let target_zone = targ.zone;
            targets.push(targ);

            // Adjust pq
            self.pq.modify_priority(target_zone, 1);
            tracing::trace!("Increased priority for zone {}", target_zone);
        }

        targets
    }

    fn get_clean_targets(&mut self) -> Self::CleanTarget {
        self.pq.remove_if_thresh_met()
    }
}

pub struct Evictor {
    shutdown: Arc<AtomicBool>,
    handle: Option<JoinHandle<()>>,
}

pub struct EvictorMessage {
    pub sender: Sender<Result<(), String>>, // Notify when done
}

impl Evictor {
    /// Start the evictor background thread
    pub fn start(
        device: Arc<dyn Device>,
        eviction_policy: Arc<Mutex<EvictionPolicyWrapper>>,
        cache: Arc<Cache>,
        evict_interval: Duration,
        evict_rx: Receiver<EvictorMessage>,
    ) -> std::io::Result<Self> {
        let shutdown = Arc::new(AtomicBool::new(false));

        let shutdown_clone = Arc::clone(&shutdown);

        let device_clone = Arc::clone(&device);
        let eviction_policy_clone = Arc::clone(&eviction_policy);
        let cache_clone = Arc::clone(&cache);

        let handle = thread::spawn(move || {
            while !shutdown_clone.load(Ordering::Relaxed) {
                let sender = match evict_rx.recv_timeout(evict_interval) {
                    Ok(s) => {
                        tracing::debug!("Received immediate eviction request");
                        Some(s.sender)
                    }
                    Err(flume::RecvTimeoutError::Timeout) => {
                        tracing::debug!("Timer eviction");
                        None
                    }
                    Err(flume::RecvTimeoutError::Disconnected) => {
                        tracing::debug!("Disconnected");
                        break;
                    }
                };

                let mut policy = eviction_policy_clone.lock().unwrap();
                let targets = policy.get_evict_targets();

                drop(policy);

                let result = match device_clone.evict(targets, cache_clone.clone()) {
                    Err(e) => {
                        tracing::error!("Error evicting: {}", e);
                        Err(e.to_string())
                    }
                    Ok(_) => Ok(()),
                };

                if let Some(sender) = sender {
                    tracing::debug!("Sending eviction response to sender: {:?}", result);
                    sender.send(result.clone()).unwrap();
                }

                evict_rx.drain().into_iter().for_each(|recv| {
                    tracing::debug!("Sending eviction response to drained sender: {:?}", result);
                    recv.sender.send(result.clone()).unwrap();
                })
            }

            tracing::debug!("Evictor thread exiting");
        });

        Ok(Self {
            shutdown,
            handle: Some(handle),
        })
    }

    /// Request the evictor to stop and wait for thread to finish
    pub fn stop(mut self) {
        self.shutdown.store(true, Ordering::Relaxed);
        if let Some(handle) = self.handle.take() {
            if let Err(e) = handle.join() {
                // A panic occurred — e is a Box<dyn Any + Send + 'static>
                if let Some(msg) = e.downcast_ref::<&str>() {
                    tracing::error!("Evictor thread panicked with message: {}", msg);
                } else if let Some(msg) = e.downcast_ref::<String>() {
                    tracing::error!("Evictor thread panicked with message: {}", msg);
                } else {
                    tracing::error!("Evictor thread panicked with unknown payload.");
                }
            }
        } else {
            tracing::error!("Evictor thread was already stopped or never started.");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::VecDeque;
    use std::fmt::Debug;
    use std::hash::Hash;

    fn compare_order<T>(lru: &mut LruCache<T, ()>, order: &VecDeque<T>)
    where
        T: Hash + Debug,
        T: Eq,
        T: PartialEq,
    {
        assert_eq!(
            order.len(),
            lru.len(),
            "Expected len = {}, but got len = {}",
            order.len(),
            lru.len()
        );
        for (_index, ((lru_key, _), order_item)) in lru.iter().zip(order.iter()).enumerate() {
            assert_eq!(
                order_item, lru_key,
                "Expected {:?}, but got {:?}",
                order_item, lru_key
            );
        }
    }

    #[test]
    fn test_chunk_update_ordering() {
        let mut policy = ChunkEvictionPolicy::new(1, 3, 1, 0, 2, 2);

        // zone=[_,_,_,_], lru=()
        let c = ChunkLocation::new(1, 0);
        let mut order: VecDeque<ChunkLocation> = VecDeque::new();
        order.push_front(c.clone());
        policy.write_update(c);
        // zone=[_,_,(1,0),_], lru=((1,0))
        compare_order(&mut policy.lru, &order);

        let et = policy.get_evict_targets();
        let expect_none: VecDeque<ChunkLocation> = VecDeque::new();
        assert_eq!(
            expect_none, et,
            "Expected = {:?}, but got {:?}",
            expect_none, et
        );

        let c = ChunkLocation::new(1, 1);
        policy.write_update(c.clone());
        // zone=[_,_,(1,0),(1,1)], lru=((1,0),(1,1))
        order.push_front(c);
        compare_order(&mut policy.lru, &order);
        let et = policy.get_evict_targets();
        assert_eq!(
            expect_none, et,
            "Expected = {:?}, but got {:?}",
            expect_none, et
        );

        let c = ChunkLocation::new(1, 0);
        // Expect order to update
        policy.read_update(c.clone());
        // zone=[_,_,(1,0),(1,1)], lru=((1,1),(1,0))
        let c = order.pop_back().unwrap();
        order.push_front(c);
        compare_order(&mut policy.lru, &order);
        let et = policy.get_evict_targets();
        assert_eq!(
            expect_none, et,
            "Expected = {:?}, but got {:?}",
            expect_none, et
        );

        let c = ChunkLocation::new(0, 0);
        policy.write_update(c.clone());
        // zone=[(0,0),_,(1,0),(1,1)], lru=((1,0),(1,1),(0,0))
        order.push_front(c);
        compare_order(&mut policy.lru, &order);
        let et = policy.get_evict_targets();
        let order = order
            .clone()
            .into_iter()
            .rev()
            .collect::<VecDeque<ChunkLocation>>();
        assert_eq!(order, et, "Expected = {:?}, but got {:?}", order, et);
    }

    #[test]
    fn test_promotional_update_ordering() {
        let mut policy = PromotionalEvictionPolicy::new(1, 3, 4, 2);

        // zone=[_,_,_,_], lru=()
        let mut order: VecDeque<Zone> = VecDeque::new();
        policy.write_update(ChunkLocation::new(3, 0));
        compare_order(&mut policy.lru, &order);
        let et = policy.get_evict_targets();
        let expect_none: Vec<Zone> = vec![];
        assert_eq!(
            expect_none, et,
            "Expected = {:?}, but got {:?}",
            expect_none, et
        );

        // zone=[_,_,_,_], lru=()
        policy.write_update(ChunkLocation::new(3, 1));
        // zone=[_,_,_,3], lru=(3)
        order.push_back(3);
        compare_order(&mut policy.lru, &order);
        let et = policy.get_evict_targets();
        assert_eq!(
            expect_none, et,
            "Expected = {:?}, but got {:?}",
            expect_none, et
        );

        policy.write_update(ChunkLocation::new(1, 0));
        // There should be no change
        // zone=[_,_,_,3], lru=(3)
        compare_order(&mut policy.lru, &order);

        policy.write_update(ChunkLocation::new(1, 1));
        // zone=[_,1,_,3], lru=(3, 1)
        order.push_front(1);
        compare_order(&mut policy.lru, &order);
        let et = policy.get_evict_targets();
        assert_eq!(
            expect_none, et,
            "Expected = {:?}, but got {:?}",
            expect_none, et
        );

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

    #[test]
    fn check_chunk_priority_queue() {
        // 8 zones, 1 chunks per zone. Should evict at 3 inserted
        let mut policy = ChunkEvictionPolicy::new(
            2, 6, 4, 1, 4, 2);

        for z in 0..3 {
            for i in 0..2 {
                policy.write_update(ChunkLocation::new(z, i));
            }
        }

        let got = policy.get_evict_targets().len();
        assert_eq!(5, got, "Expected 5, but got {}", got);

        let got = policy.get_clean_targets().len();
        assert_eq!(3, got, "Expected 3, but got {}", got);
    }
}
