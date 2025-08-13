use priority_queue::PriorityQueue;

pub(crate) type ZoneIndex = nvme::types::Zone;
type ZonePriority = usize;

pub struct ZonePriorityQueue {
    invalid_count: ZonePriority,
    invalid_queue: PriorityQueue<ZoneIndex, ZonePriority>, // max-heap by priority
    high_water_thresh: usize,  // trigger cleaning when >= this
    low_water_thresh: usize,   // clean down to < this
}

impl ZonePriorityQueue {
    pub fn new(num_zones: ZoneIndex, high_water_thresh: usize, low_water_thresh: usize) -> Self {
        assert!(high_water_thresh > low_water_thresh);

        let mut invalid_queue = PriorityQueue::new();
        for z in 0..num_zones {
            invalid_queue.push(z, 0);
        }

        Self {
            invalid_queue,
            invalid_count: 0,
            high_water_thresh,
            low_water_thresh,
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
        let sum: usize = self.invalid_queue.iter().map(|(_, p)| *p).sum();
        assert_eq!(sum, self.invalid_count, "invalid_count out of sync with heap contents");
    }

    // Helpers for tests/metrics
    #[allow(dead_code)]
    pub fn invalid_total(&self) -> usize { self.invalid_count }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn initial_state_and_no_clean_under_high() {
        let mut q = ZonePriorityQueue::new(5 as ZoneIndex, 10, 4);
        assert_eq!(q.invalid_total(), 0);
        assert!(q.remove_if_thresh_met().is_empty());

        // Still below high
        q.modify_priority(0 as ZoneIndex, 9);
        assert_eq!(q.invalid_total(), 9);
        assert!(q.remove_if_thresh_met().is_empty());
    }

    #[test]
    fn triggers_at_high_and_cleans_to_below_low() {
        let mut q = ZonePriorityQueue::new(4 as ZoneIndex, 10, 4);
        // total = 3 + 7 = 10 (== high)
        q.modify_priority(0 as ZoneIndex, 3);
        q.modify_priority(1 as ZoneIndex, 7);
        assert_eq!(q.invalid_total(), 10);

        // Should pop the most-invalid (index 1, prio 7), leaving 3 (< low)
        let removed = q.remove_if_thresh_met();
        assert_eq!(removed.len(), 1);
        assert!(removed.contains(&(1 as ZoneIndex)));
        assert_eq!(q.invalid_total(), 3);

        // That zone was reset; we can add to it again and totals reflect that
        q.modify_priority(1 as ZoneIndex, 2);
        assert_eq!(q.invalid_total(), 5);
    }

    #[test]
    fn multiple_pops_until_below_low() {
        let mut q = ZonePriorityQueue::new(4 as ZoneIndex, 12, 5);
        // total = 4 + 4 + 4 = 12 (== high)
        q.modify_priority(0 as ZoneIndex, 4);
        q.modify_priority(1 as ZoneIndex, 4);
        q.modify_priority(2 as ZoneIndex, 4);
        assert_eq!(q.invalid_total(), 12);

        // Pop highest (4) -> 8 (>=5), pop next (4) -> 4 (<5) -> stop
        let removed = q.remove_if_thresh_met();
        assert_eq!(removed.len(), 2);
        assert_eq!(q.invalid_total(), 4);
    }

    #[test]
    fn unknown_index_does_not_desync_totals() {
        let mut q = ZonePriorityQueue::new(2 as ZoneIndex, 10, 4);
        // 99 doesn't exist; invalid_total must not change
        q.modify_priority(99 as ZoneIndex, 6);
        assert_eq!(q.invalid_total(), 0);
    }

    #[test]
    fn no_infinite_loop_when_priorities_zero() {
        let mut q = ZonePriorityQueue::new(3 as ZoneIndex, 2, 1);
        // total remains 0; should not clean
        assert!(q.remove_if_thresh_met().is_empty());

        // Set two zones to 1 each -> total = 2 (== high)
        q.modify_priority(0 as ZoneIndex, 1);
        q.modify_priority(1 as ZoneIndex, 1);
        assert_eq!(q.invalid_total(), 2);

        // Should remove one zone (prio 1) -> total 1 (>= low), remove another -> total 0 (< low)
        let removed = q.remove_if_thresh_met();
        assert_eq!(removed.len(), 2);
        assert_eq!(q.invalid_total(), 0);
    }
}
