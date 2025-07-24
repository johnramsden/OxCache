use std::collections::VecDeque;
use crate::cache::bucket::ChunkLocation;

pub struct ZoneList {
    available_zones: VecDeque<crate::device::Zone>,
    chunks_per_zone: usize,
}

impl ZoneList {
    pub fn new(num_zones: usize, chunks_per_zone: usize) -> Self {
        let mut avail_zones = VecDeque::with_capacity(num_zones);

        for i in 0..num_zones {
            avail_zones.push_back(crate::device::Zone {
                index: i,
                chunks_available: chunks_per_zone,
            });
        }

        ZoneList {
            available_zones: avail_zones,
            chunks_per_zone,
        }
    }

    // Get a zone to write to
    pub fn remove(&mut self) -> Result<usize, ()> {
        if self.is_full() {
            // Need to evict
            return Err(());
        }

        let mut zone = match self.available_zones.pop_front() {
            Some(z) => z,
            None => return Err(()),
        };
        if zone.chunks_available > 1 {
            zone.chunks_available -= 1;
            self.available_zones.push_front(zone);
        }

        Ok(zone.index)
    }

    // Get the location to write to, for block devices
    pub fn remove_chunk_location(&mut self) -> Result<ChunkLocation, ()> {
        if self.is_full() {
            // Need to evict
            return Err(());
        }

        let mut zone = match self.available_zones.pop_front() {
            Some(z) => z,
            None => return Err(()),
        };
        let chunk_idx = self.chunks_per_zone - zone.chunks_available;
        if zone.chunks_available > 1 {
            zone.chunks_available -= 1;
            self.available_zones.push_front(zone);
        }

        Ok(ChunkLocation {
            zone: zone.index,
            index: chunk_idx as u64,
        })
    }

    // Check if all zones are full
    pub fn is_full(&self) -> bool {
        self.available_zones.is_empty()
    }

    // Reset the selected zone
    #[allow(dead_code)]
    pub fn reset_zone(&mut self, idx: usize) {
        self.available_zones.push_back(crate::device::Zone {
            index: idx,
            chunks_available: self.chunks_per_zone,
        });
    }

    pub fn reset_zones(&mut self, indices: &[usize]) {
        for idx in indices {
            self.available_zones.push_back(crate::device::Zone {
                index: *idx,
                chunks_available: self.chunks_per_zone,
            });
        }
    }

    pub fn reset_zone_with_capacity(&mut self, idx: usize, remaining: usize) {
        self.available_zones.push_back(crate::device::Zone {
            index: idx,
            chunks_available: remaining,
        });
    }
}
