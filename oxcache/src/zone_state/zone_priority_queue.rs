use priority_queue::PriorityQueue;

pub(crate) type ZoneIndex = nvme::types::Zone;
use nvme::types::{Chunk, Zone};
type ZonePriority = Chunk;

#[derive(Debug)]
pub struct ZonePriorityQueue {
    invalid_count: ZonePriority,
    invalid_queue: PriorityQueue<ZoneIndex, ZonePriority>, // max-heap by priority
    high_water_thresh: Chunk,  // trigger cleaning when >= this
    low_water_thresh: Chunk,   // clean down to < this
}

impl ZonePriorityQueue {
    pub fn new(num_zones: ZoneIndex, high_water_thresh: Chunk, low_water_thresh: Chunk) -> Self {
        assert!(high_water_thresh > low_water_thresh);

        let mut invalid_queue = PriorityQueue::new();
        for z in 0..num_zones {
            invalid_queue.push(z, 0);
        }

        Self {
            invalid_queue,
            invalid_count: 0,
            high_water_thresh: high_water_thresh,
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
    pub fn remove_if_thresh_met(&mut self) -> Vec<ZoneIndex> {
        let mut zones = Vec::new();
        tracing::trace!("[evict:Chunk] Cleaning zones, invalid={}", self.invalid_count);
        if self.invalid_count < self.high_water_thresh {
            return zones;
        }
        while self.invalid_count >= self.low_water_thresh {
            zones.push(self.pop_reset());
        }
        zones
    }

    pub fn modify_priority(&mut self, ind: ZoneIndex, priority_increase: ZonePriority) {
        // Only account for it if the entry exists
        if self.invalid_queue.change_priority_by(&ind, |p| *p += priority_increase) {
            self.invalid_count = self.invalid_count.saturating_add(priority_increase);
        }
        #[cfg(debug_assertions)]
        self.assert_consistent();
    }

    #[cfg(debug_assertions)]
    fn assert_consistent(&self) {
        let sum: ZonePriority = self.invalid_queue.iter().map(|(_, p)| *p).sum();
        assert_eq!(sum, self.invalid_count, "invalid_count out of sync with heap contents");
    }

    // Helpers for tests/metrics
    #[allow(dead_code)]
    pub fn invalid_total(&self) -> ZonePriority { self.invalid_count }
}

#[cfg(test)]
mod tests {
    use super::*;

}
