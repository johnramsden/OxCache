use priority_queue::PriorityQueue;

pub(crate) type ZoneIndex = nvme::types::Zone;
use nvme::types::{Chunk, Zone};
type ZonePriority = Chunk;

pub struct ZonePriorityQueue {
    invalid_count: ZonePriority,
    invalid_queue: PriorityQueue<ZoneIndex, ZonePriority>, // max-heap by priority
    high_water_thresh: Chunk,  // trigger cleaning when >= this
    low_water_thresh: Chunk,   // clean down to < this
}

impl ZonePriorityQueue {
    pub fn new(num_zones: ZoneIndex, chunks_per_zone: Chunk, high_water_thresh: Chunk, low_water_thresh: Chunk) -> Self {
        assert!(high_water_thresh < low_water_thresh);

        let mut invalid_queue = PriorityQueue::new();
        for z in 0..num_zones {
            invalid_queue.push(z, 0);
        }

        let chunks = num_zones * chunks_per_zone;

        Self {
            invalid_queue,
            invalid_count: 0,
            high_water_thresh: chunks-high_water_thresh,
            low_water_thresh: chunks-low_water_thresh,
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
        log::trace!("[evict:Chunk] Cleaning zones, invalid={}", self.invalid_count);
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

    #[test]
    fn initial_state_and_no_clean_under_high() {
        let mut q = ZonePriorityQueue::new(5 as ZoneIndex, 1, 0, 4);
        assert_eq!(q.invalid_total(), 0);
        assert!(q.remove_if_thresh_met().is_empty());

        // Still below high
        q.modify_priority(0 as ZoneIndex, 4);
        assert_eq!(q.invalid_total(), 4);
        assert!(q.remove_if_thresh_met().is_empty());
    }

    #[test]
    fn triggers_at_high_and_cleans_to_below_low() {
        let mut q = ZonePriorityQueue::new(5 as ZoneIndex, 1, 0, 2);
        // total = 2+3 = 5 (== high)
        q.modify_priority(0 as ZoneIndex, 2);
        q.modify_priority(1 as ZoneIndex, 3);
        assert_eq!(q.invalid_total(), 5);

        // Should pop the most-invalid (index 1, prio 3), leaving 2(< low)
        let removed = q.remove_if_thresh_met();
        assert_eq!(removed.len(), 1);
        assert!(removed.contains(&(1 as ZoneIndex)));
        assert_eq!(q.invalid_total(), 2);

        // That zone was reset; we can add to it again and totals reflect that
        q.modify_priority(1 as ZoneIndex, 3);
        assert_eq!(q.invalid_total(), 5);
    }

    #[test]
    fn multiple_pops_until_below_low() {
        // Make total chunks large enough to express the intended invalid thresholds:
        // Intended: internal_high = 12, internal_low = 5.
        // With chunks = 4 zones * 4 chunks/zone = 16:
        //   high_free = 16 - 12 = 4, low_free = 16 - 5 = 11 (and 4 < 11 holds).
        let mut q = ZonePriorityQueue::new(4 as ZoneIndex, 4, 4, 11);

        // total = 4 + 4 + 4 = 12 (== internal high)
        q.modify_priority(0 as ZoneIndex, 4);
        q.modify_priority(1 as ZoneIndex, 4);
        q.modify_priority(2 as ZoneIndex, 4);
        assert_eq!(q.invalid_total(), 12);

        // Pop highest (4) -> 8 (>= internal low = 5), pop next (4) -> 4 (<5) -> stop
        let removed = q.remove_if_thresh_met();
        assert_eq!(removed.len(), 2);
        assert_eq!(q.invalid_total(), 4);
    }

    #[test]
    fn unknown_index_does_not_desync_totals() {
        // Any valid free thresholds will do; we don't trigger cleaning in this test.
        let mut q = ZonePriorityQueue::new(2 as ZoneIndex, 1, 0, 1);
        // 99 doesn't exist; invalid_total must not change
        q.modify_priority(99 as ZoneIndex, 6);
        assert_eq!(q.invalid_total(), 0);
    }

    #[test]
    fn no_infinite_loop_when_priorities_zero() {
        // Intended internal thresholds: high = 2, low = 1.
        // With chunks = 3: high_free = 3 - 2 = 1, low_free = 3 - 1 = 2 (1 < 2 holds).
        let mut q = ZonePriorityQueue::new(3 as ZoneIndex, 1, 1, 2);
        // total remains 0; should not clean
        assert!(q.remove_if_thresh_met().is_empty());

        // Set two zones to 1 each -> total = 2 (== internal high)
        q.modify_priority(0 as ZoneIndex, 1);
        q.modify_priority(1 as ZoneIndex, 1);
        assert_eq!(q.invalid_total(), 2);

        // Should remove one zone (prio 1) -> total 1 (>= internal low), remove another -> total 0 (< internal low)
        let removed = q.remove_if_thresh_met();
        assert_eq!(removed.len(), 2);
        assert_eq!(q.invalid_total(), 0);
    }
}
