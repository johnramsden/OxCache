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
    pub fn remove_if_thresh_met(&mut self) -> Vec<ZoneIndex> {
        let mut zones = Vec::new();
        tracing::trace!(
            "[evict:Chunk] Cleaning zones, invalid={}",
            self.invalid_count
        );
        while self.invalid_count >= self.low_water_thresh {
            zones.push(self.pop_reset());
        }
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
