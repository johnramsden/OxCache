use crate::cache::{Cache, bucket::ChunkLocation};
use crate::device::Device;
use crate::writerpool::WriterPool;
use crate::zone_state::zone_priority_queue::{ZoneIndex, ZonePriorityQueue};
use flume::{Receiver, Sender};
use lru_mem::{LruCache};
use nvme::types::{Chunk, Zone};
use std::io::ErrorKind;
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicBool, AtomicU64, Ordering},
};
use std::thread::{self, JoinHandle};
use std::time::Duration;

#[derive(Debug)]
pub enum EvictionPolicyWrapper {
    Promotional(PromotionalEvictionPolicy),
    Chunk(ChunkEvictionPolicy),
}

#[derive(Debug)]
pub enum EvictTarget {
    Chunk(Vec<ChunkLocation>, Option<Vec<ZoneIndex>>),
    Zone(Vec<Zone>),
}

impl EvictionPolicyWrapper {
    pub fn new(
        identifier: &str,
        high_water: Zone,
        low_water: Zone,
        nr_zones: Zone,
        nr_chunks_per_zone: Chunk,
        clean_low_water: Option<Chunk>,
    ) -> tokio::io::Result<Self> {
        match identifier.to_lowercase().as_str() {
            "chunk" => {
                if clean_low_water.is_none() {
                    return Err(std::io::Error::new(
                        ErrorKind::InvalidInput,
                        "Chunk eviction must have clean_high_water and clean_low_water",
                    ));
                }
                Ok(EvictionPolicyWrapper::Chunk(ChunkEvictionPolicy::new(
                    high_water,
                    low_water,
                    clean_low_water.unwrap(),
                    nr_zones,
                    nr_chunks_per_zone,
                )))
            }
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
            EvictionPolicyWrapper::Promotional(promotional) => promotional.write_update(chunk),
            EvictionPolicyWrapper::Chunk(c) => c.write_update(chunk),
        }
    }
    pub fn read_update(&mut self, chunk: ChunkLocation) {
        match self {
            EvictionPolicyWrapper::Promotional(promotional) => promotional.read_update(chunk),
            EvictionPolicyWrapper::Chunk(c) => c.read_update(chunk),
        }
    }

    pub fn get_evict_targets(
        &mut self,
        get_clean_targets: bool,
        always_evict: bool,
    ) -> EvictTarget {
        match self {
            EvictionPolicyWrapper::Promotional(promotional) => {
                EvictTarget::Zone(promotional.get_evict_targets(always_evict))
            }
            EvictionPolicyWrapper::Chunk(c) => {
                let et = c.get_evict_targets(always_evict);
                let ct = if get_clean_targets {
                    Some(c.get_clean_targets())
                } else {
                    None
                };
                EvictTarget::Chunk(et, ct)
            }
        }
    }

    #[cfg(feature = "eviction-metrics")]
    pub fn set_metrics(&mut self, metrics: Arc<crate::eviction_metrics::EvictionMetrics>) {
        match self {
            EvictionPolicyWrapper::Promotional(p) => p.metrics = Some(metrics),
            EvictionPolicyWrapper::Chunk(c) => c.metrics = Some(metrics),
        }
    }

    #[cfg(feature = "eviction-metrics")]
    pub fn get_metrics(&self) -> Option<Arc<crate::eviction_metrics::EvictionMetrics>> {
        match self {
            EvictionPolicyWrapper::Promotional(p) => p.metrics.clone(),
            EvictionPolicyWrapper::Chunk(c) => c.metrics.clone(),
        }
    }
}

pub trait EvictionPolicy: Send + Sync {
    type Target: Clone + Send + Sync + 'static;
    type CleanTarget: Clone + Send + Sync + 'static;

    fn write_update(&mut self, chunk: ChunkLocation);
    fn read_update(&mut self, chunk: ChunkLocation);
    fn get_evict_targets(&mut self, always_evict: bool) -> Self::Target;

    fn get_clean_targets(&mut self) -> Self::CleanTarget;
}

#[derive(Debug)]
pub struct PromotionalEvictionPolicy {
    high_water: Zone,
    low_water: Zone,
    nr_zones: Zone,
    pub nr_chunks_per_zone: Chunk,
    pub zone_chunk_counts: Arc<Vec<AtomicU64>>,
    lru: LruCache<Zone, ()>,
    #[cfg(feature = "eviction-metrics")]
    pub metrics: Option<Arc<crate::eviction_metrics::EvictionMetrics>>,
}

impl PromotionalEvictionPolicy {
    pub fn new(
        high_water: Zone,
        low_water: Zone,
        nr_zones: Zone,
        nr_chunks_per_zone: Chunk,
    ) -> Self {
        let lru = LruCache::new(usize::MAX); // Effectively unbounded
        let zone_chunk_counts = Arc::new((0..nr_zones).map(|_| AtomicU64::new(0)).collect());
        Self {
            high_water,
            low_water,
            nr_zones,
            nr_chunks_per_zone,
            zone_chunk_counts,
            lru,
            #[cfg(feature = "eviction-metrics")]
            metrics: None,
        }
    }
}

impl EvictionPolicy for PromotionalEvictionPolicy {
    /// Promotional LRU
    /// Performs LRU based on full zones
    type Target = Vec<Zone>;
    type CleanTarget = ();

    fn write_update(&mut self, chunk: ChunkLocation) {
        #[cfg(feature = "eviction-metrics")]
        if let Some(ref metrics) = self.metrics {
            metrics.record_write(&chunk);
        }

        // This is now only called when zone is full (atomic check done outside lock)
        // Just insert the zone into the LRU
        self.lru.insert(chunk.zone, ()).ok();
    }

    fn read_update(&mut self, chunk: ChunkLocation) {
        #[cfg(feature = "eviction-metrics")]
        if let Some(ref metrics) = self.metrics {
            metrics.record_read(&chunk);
        }

        // This is now only called when zone is full (atomic check done outside lock)
        // Promote the zone in the LRU if it's already there
        if self.lru.contains(&chunk.zone) {
            self.lru.insert(chunk.zone, ()).ok();
        }
    }

    fn get_evict_targets(&mut self, always_evict: bool) -> Self::Target {
        let lru_len = self.lru.len() as Zone;
        let high_water_mark = self.nr_zones - self.high_water;
        if !always_evict && lru_len < high_water_mark {
            return vec![];
        }

        let low_water_mark = self.nr_zones - self.low_water;

        // Prevent underflow when lru_len < low_water_mark
        if lru_len < low_water_mark {
            return vec![];
        }

        let cap = lru_len - low_water_mark;

        let mut targets = Vec::with_capacity(cap as usize);

        while self.lru.len() as Zone >= low_water_mark {
            targets.push(self.lru.remove_lru().unwrap().0)
        }

        #[cfg(feature = "eviction-metrics")]
        if let Some(ref metrics) = self.metrics {
            for zone in &targets {
                metrics.record_zone_eviction(*zone, self.nr_chunks_per_zone);
            }
        }

        targets
    }

    fn get_clean_targets(&mut self) -> Self::CleanTarget {
        ()
    }
}

#[derive(Debug)]
pub struct ChunkEvictionPolicy {
    high_water: Chunk,
    low_water: Chunk,
    nr_zones: Zone,
    nr_chunks_per_zone: Chunk,
    lru: LruCache<ChunkLocation, ()>,
    pq: ZonePriorityQueue,
    #[cfg(feature = "eviction-metrics")]
    pub metrics: Option<Arc<crate::eviction_metrics::EvictionMetrics>>,
}

impl ChunkEvictionPolicy {
    pub fn new(
        high_water: Chunk,
        low_water: Chunk,
        clean_low_water: Chunk,
        nr_zones: Zone,
        nr_chunks_per_zone: Chunk,
    ) -> Self {
        assert!(
            high_water > nr_chunks_per_zone,
            "high_water={} must be larger than nr_chunks_per_zone={} to leave room for eviction",
            high_water,
            nr_chunks_per_zone
        );

        Self {
            high_water,
            low_water,
            nr_zones,
            nr_chunks_per_zone,
            lru: LruCache::new(usize::MAX), // Effectively unbounded
            pq: ZonePriorityQueue::new(nr_zones, clean_low_water),
            #[cfg(feature = "eviction-metrics")]
            metrics: None,
        }
    }

}

impl EvictionPolicy for ChunkEvictionPolicy {
    type Target = Vec<ChunkLocation>;
    type CleanTarget = Vec<ZoneIndex>;
    fn write_update(&mut self, chunk: ChunkLocation) {
        #[cfg(feature = "eviction-metrics")]
        if let Some(ref metrics) = self.metrics {
            metrics.record_write(&chunk);
        }

        self.lru.insert(chunk, ()).ok();
    }

    fn read_update(&mut self, chunk: ChunkLocation) {
        #[cfg(feature = "eviction-metrics")]
        if let Some(ref metrics) = self.metrics {
            metrics.record_read(&chunk);
        }

        if self.lru.contains(&chunk) {
            self.lru.insert(chunk, ()).ok();
        }
    }

    fn get_evict_targets(&mut self, always_evict: bool) -> Self::Target {
        let lru_len = self.lru.len() as Chunk;
        let nr_chunks = self.nr_zones * self.nr_chunks_per_zone;
        let high_water_mark = nr_chunks - self.high_water;

        tracing::info!(
            "[ChunkPolicy] get_evict_targets: lru_len={}, high_water_mark={}, always_evict={}",
            lru_len, high_water_mark, always_evict
        );

        if !always_evict && lru_len < high_water_mark {
            return vec![];
        }

        let low_water_mark = nr_chunks - self.low_water;

        if lru_len < low_water_mark {
            tracing::warn!("[ChunkPolicy] LRU too small ({} < {}), cannot evict!", lru_len, low_water_mark);
            return vec![];
        }

        let count_to_evict = lru_len - low_water_mark;

        let mut targets = Vec::with_capacity(count_to_evict as usize);
        let mut zone_counts = std::collections::HashMap::new();

        // Collect evicted items and count by zone (batch the counting)
        for _ in 0..count_to_evict {
            if let Some((targ, _)) = self.lru.remove_lru() {
                let target_zone = targ.zone;
                targets.push(targ);

                // Batch count instead of individual priority queue updates
                *zone_counts.entry(target_zone).or_insert(0) += 1;
            }
        }

        // Batch update priority queue (far fewer operations)
        for (zone, count) in zone_counts {
            self.pq.modify_priority(zone, count);
        }

        #[cfg(feature = "eviction-metrics")]
        if let Some(ref metrics) = self.metrics {
            metrics.record_chunk_evictions(&targets);
        }

        targets
    }

    fn get_clean_targets(&mut self) -> Self::CleanTarget {
        let clean_targets = self.pq.remove_if_thresh_met();

        if clean_targets.is_empty() {
            return clean_targets;
        }

        clean_targets
    }
}

impl ChunkEvictionPolicy {
    /// Remove old LRU entries after chunks are relocated during zone cleaning.
    pub fn chunks_relocated(&mut self, old_locations: &[ChunkLocation]) {
        for old_loc in old_locations {
            // Remove stale entry from LRU
            // The relocated chunk's new location is already in LRU via write_update()
            self.lru.remove(old_loc);
        }
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
        writer_pool: Arc<WriterPool>,
    ) -> std::io::Result<Self> {
        let shutdown = Arc::new(AtomicBool::new(false));

        let shutdown_clone = Arc::clone(&shutdown);

        let device_clone = Arc::clone(&device);
        let eviction_policy_clone = Arc::clone(&eviction_policy);
        let cache_clone = Arc::clone(&cache);

        let handle = thread::spawn(move || {
            #[cfg(feature = "eviction-metrics")]
            let mut last_metrics_log = std::time::Instant::now();

            while !shutdown_clone.load(Ordering::Relaxed) {
                let (sender, always_evict) = match evict_rx.recv_timeout(evict_interval) {
                    Ok(s) => {
                        tracing::info!("[Evictor] Received immediate eviction request");
                        (Some(s.sender), true)
                    }
                    Err(flume::RecvTimeoutError::Timeout) => {
                        tracing::debug!("[Evictor] Timer eviction (periodic check)");
                        (None, false)
                    }
                    Err(flume::RecvTimeoutError::Disconnected) => {
                        tracing::info!("[Evictor] Channel disconnected, shutting down");
                        break;
                    }
                };

                let device_clone = device_clone.clone();
                let result = match device_clone.evict(
                    cache_clone.clone(),
                    writer_pool.clone(),
                    eviction_policy_clone.clone(),
                    always_evict,
                ) {
                    Err(e) => {
                        tracing::error!("[Evictor] Eviction FAILED: {}", e);
                        Err(e.to_string())
                    },
                    Ok(_) => {
                        tracing::info!("[Evictor] Eviction completed successfully");
                        Ok(())
                    },
                };

                if let Some(sender) = sender {
                    tracing::debug!("Sending eviction response to sender: {:?}", result);
                    sender.send(result.clone()).unwrap();
                }

                evict_rx.drain().into_iter().for_each(|recv| {
                    tracing::debug!("Sending eviction response to drained sender: {:?}", result);
                    recv.sender.send(result.clone()).unwrap();
                });

                #[cfg(feature = "eviction-metrics")]
                {
                    let now = std::time::Instant::now();
                    if now.duration_since(last_metrics_log).as_secs() >= 60 {
                        if let Ok(policy) = eviction_policy_clone.lock() {
                            if let Some(metrics) = policy.get_metrics() {
                                let policy_name = match &*policy {
                                    EvictionPolicyWrapper::Promotional(_) => "Promotional",
                                    EvictionPolicyWrapper::Chunk(_) => "Chunk",
                                };
                                tracing::info!("\n{}", metrics.generate_report(policy_name));
                            }
                        }
                        last_metrics_log = now;
                    }
                }
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
        // The lru_mem crate iterates from most recently used to least recently used
        // but our order VecDeque is constructed with push_front for most recent
        // So we need to reverse the iteration order to match
        for (_index, ((lru_key, _), order_item)) in lru.iter().zip(order.iter().rev()).enumerate() {
            assert_eq!(
                order_item, lru_key,
                "Expected {:?}, but got {:?}",
                order_item, lru_key
            );
        }
    }

    // TODO: Fix params

    #[test]
    fn test_chunk_update_ordering() {
        let mut policy = ChunkEvictionPolicy::new(9, 12, 0, 6, 2);

        // zone=[_,_,_,_], lru=()
        let c = ChunkLocation::new(1, 0);
        let mut order: VecDeque<ChunkLocation> = VecDeque::new();
        order.push_front(c.clone());
        policy.write_update(c);
        // zone=[_,_,(1,0),_], lru=((1,0))
        compare_order(&mut policy.lru, &order);

        let et = policy.get_evict_targets(false);
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
        let et = policy.get_evict_targets(false);
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
        let et = policy.get_evict_targets(false);
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
        let et = policy.get_evict_targets(false);
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
        let count = policy.zone_chunk_counts[3].fetch_add(1, Ordering::Relaxed);
        if count + 1 == policy.nr_chunks_per_zone {
            policy.write_update(ChunkLocation::new(3, 0));
        }
        compare_order(&mut policy.lru, &order);
        let et = policy.get_evict_targets(false);
        let expect_none: Vec<Zone> = vec![];
        assert_eq!(
            expect_none, et,
            "Expected = {:?}, but got {:?}",
            expect_none, et
        );

        // zone=[_,_,_,_], lru=()
        let count = policy.zone_chunk_counts[3].fetch_add(1, Ordering::Relaxed);
        if count + 1 == policy.nr_chunks_per_zone {
            policy.write_update(ChunkLocation::new(3, 1));
        }
        // zone=[_,_,_,3], lru=(3)
        order.push_back(3);
        compare_order(&mut policy.lru, &order);
        let et = policy.get_evict_targets(false);
        assert_eq!(
            expect_none, et,
            "Expected = {:?}, but got {:?}",
            expect_none, et
        );

        let count = policy.zone_chunk_counts[1].fetch_add(1, Ordering::Relaxed);
        if count + 1 == policy.nr_chunks_per_zone {
            policy.write_update(ChunkLocation::new(1, 0));
        }
        // There should be no change
        // zone=[_,_,_,3], lru=(3)
        compare_order(&mut policy.lru, &order);

        let count = policy.zone_chunk_counts[1].fetch_add(1, Ordering::Relaxed);
        if count + 1 == policy.nr_chunks_per_zone {
            policy.write_update(ChunkLocation::new(1, 1));
        }
        // zone=[_,1,_,3], lru=(3, 1)
        order.push_front(1);
        compare_order(&mut policy.lru, &order);
        let et = policy.get_evict_targets(false);
        assert_eq!(
            expect_none, et,
            "Expected = {:?}, but got {:?}",
            expect_none, et
        );
        let count = policy.zone_chunk_counts[2].fetch_add(1, Ordering::Relaxed);
        if count + 1 == policy.nr_chunks_per_zone {
            policy.write_update(ChunkLocation::new(2, 0));
        }
        let count = policy.zone_chunk_counts[2].fetch_add(1, Ordering::Relaxed);
        if count + 1 == policy.nr_chunks_per_zone {
            policy.write_update(ChunkLocation::new(2, 1));
        }
        order.push_front(2);
        // zone=[_,1,2,3], lru=(3, 1, 2)
        compare_order(&mut policy.lru, &order);

        // Should update in place, and adjust order
        if policy.zone_chunk_counts[3].load(Ordering::Relaxed) >= policy.nr_chunks_per_zone {
            policy.read_update(ChunkLocation::new(3, 1));
        }
        let c = order.pop_back().unwrap();
        order.push_front(c);
        // zone=[_,1,2,3], lru=(1, 2, 3)
        compare_order(&mut policy.lru, &order);

        let et = policy.get_evict_targets(false);
        let expect = VecDeque::from(vec![1, 2, 3]);
        assert_eq!(expect, et, "Expected = {:?}, but got {:?}", expect, et);

        compare_order(&mut policy.lru, &VecDeque::from(vec![]));
    }

    #[test]
    fn check_chunk_priority_queue() {
        // 4 zones, 2 chunks per zone = 8 total chunks
        // high_water=3, low_water=6, clean_low_water=1
        let mut policy = ChunkEvictionPolicy::new(3, 6, 1, 4, 2);

        // Fill 3 zones (6 chunks)
        for z in 0..3 {
            for i in 0..2 {
                policy.write_update(ChunkLocation::new(z, i));
            }
        }

        // Should evict: lru_len=6, need to reach low_water_mark=8-6=2
        // So evict 6-2=4 chunks
        let evicted = policy.get_evict_targets(false);
        assert_eq!(4, evicted.len(), "Should evict 4 chunks");

        // After eviction, 4 chunks invalidated across zones
        // Zones should have 2, 1, 1 invalid chunks (or similar distribution)
        // clean_low_water=1, so zones with >=1 invalid should be cleaned
        let to_clean = policy.get_clean_targets();
        assert!(to_clean.len() >= 2, "Should clean at least 2 zones with invalids");
    }
}
