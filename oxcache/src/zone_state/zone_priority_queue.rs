use priority_queue::PriorityQueue;

pub(crate) type ZoneIndex = nvme::types::Zone;
use nvme::types::{Chunk};
type ZonePriority = Chunk;

#[derive(Debug)]
pub struct ZonePriorityQueue {
    invalid_count: ZonePriority,
    invalid_queue: PriorityQueue<ZoneIndex, ZonePriority>, // max-heap by priority
    low_water_thresh: Chunk,                               // clean down to < this
}

impl ZonePriorityQueue {
    pub fn new(num_zones: ZoneIndex, low_water_thresh: Chunk) -> Self {
        let mut invalid_queue = PriorityQueue::new();
        for z in 0..num_zones {
            invalid_queue.push(z, 0);
        }

        Self {
            invalid_queue,
            invalid_count: 0,
            low_water_thresh: low_water_thresh,
        }
    }

    // Pops the most-invalid zone and resets it to 0
    fn pop_reset(&mut self) -> ZoneIndex {
        let (ind, prio) = self.invalid_queue.pop().expect("queue should not be empty");
        self.invalid_count = self.invalid_count.saturating_sub(prio);
        self.invalid_queue.push(ind, 0);
        #[cfg(debug_assertions)]
        self.assert_consistent();
        ind
    }

    // If above high watermark, clean until strictly below low watermark
    // If force_all=true, clean ALL zones with invalid chunks (desperate/always_evict mode)
    pub fn remove_if_thresh_met(&mut self, force_all: bool) -> Vec<ZoneIndex> {
        let mut zones = Vec::new();

        // Count how many zones have invalid chunks
        let zones_with_invalids = self.invalid_queue.iter().filter(|(_, p)| **p > 0).count();

        tracing::info!(
            "[PriorityQueue] remove_if_thresh_met called: invalid_count={}, low_water_thresh={}, force_all={}, zones_with_invalids={}, will_clean={}",
            self.invalid_count,
            self.low_water_thresh,
            force_all,
            zones_with_invalids,
            force_all || self.invalid_count >= self.low_water_thresh
        );

        if force_all && zones_with_invalids > 0 {
            // Desperate mode: clean ALL zones with invalid chunks
            tracing::warn!(
                "[PriorityQueue] ⚠️  FORCE CLEANING all {} zones with invalid chunks (always_evict=true)!",
                zones_with_invalids
            );
            while let Some((zone, prio)) = self.invalid_queue.peek() {
                if *prio == 0 {
                    break; // No more zones with invalid chunks
                }
                tracing::info!(
                    "[PriorityQueue] Force cleaning zone {} with {} invalid chunks",
                    zone, prio
                );
                zones.push(self.pop_reset());
            }
        } else {
            // Normal mode: clean until below threshold
            while self.invalid_count >= self.low_water_thresh {
                let (zone, prio) = self.invalid_queue.peek().unwrap();
                tracing::info!(
                    "[PriorityQueue] Cleaning zone {} with {} invalid chunks (invalid_count={} >= thresh={})",
                    zone, prio, self.invalid_count, self.low_water_thresh
                );
                zones.push(self.pop_reset());
            }
        }

        let zones_with_invalids_after = self.invalid_queue.iter().filter(|(_, p)| **p > 0).count();
        tracing::info!(
            "[PriorityQueue] After cleaning: returning {} zones, invalid_count={}, {} zones still have invalid chunks",
            zones.len(),
            self.invalid_count,
            zones_with_invalids_after
        );

        zones
    }

    pub fn modify_priority(&mut self, ind: ZoneIndex, priority_change: i64) {
        // Handle both increments and decrements
        if priority_change > 0 {
            let increase = priority_change as ZonePriority;
            if self
                .invalid_queue
                .change_priority_by(&ind, |p| *p += increase)
            {
                self.invalid_count = self.invalid_count.saturating_add(increase);
            }
        } else if priority_change < 0 {
            let decrease = (-priority_change) as ZonePriority;
            if self
                .invalid_queue
                .change_priority_by(&ind, |p| *p = p.saturating_sub(decrease))
            {
                self.invalid_count = self.invalid_count.saturating_sub(decrease);
            }
        }
        // priority_change == 0: no-op

        #[cfg(debug_assertions)]
        self.assert_consistent();
    }

    #[cfg(debug_assertions)]
    fn assert_consistent(&self) {
        let sum: ZonePriority = self.invalid_queue.iter().map(|(_, p)| *p).sum();
        assert_eq!(
            sum, self.invalid_count,
            "invalid_count out of sync with heap contents"
        );
    }

    // Helpers for tests/metrics
    #[allow(dead_code)]
    pub fn invalid_total(&self) -> ZonePriority {
        self.invalid_count
    }
}

#[cfg(test)]
mod tests {
    use super::*;
}
