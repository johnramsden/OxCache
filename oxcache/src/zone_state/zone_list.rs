use crate::cache::bucket::ChunkLocation;
use std::collections::VecDeque;

type ZoneIndex = usize;

#[derive(Copy, Clone)]
pub struct Zone {
    pub index: ZoneIndex,
    pub chunks_available: usize,
}

pub struct ZoneList {
    free_zones: VecDeque<Zone>, // Unopened zones with full capacity
    open_zones: VecDeque<Zone>, // Opened zones
    chunks_per_zone: usize,
    max_active_resources: usize,
}

impl ZoneList {
    pub fn new(num_zones: usize, chunks_per_zone: usize, max_active_resources: usize) -> Self {
        let avail_zones = (0..num_zones)
            .map(|index| Zone {
                index,
                chunks_available: chunks_per_zone,
            })
            .collect();

        ZoneList {
            free_zones: avail_zones,
            open_zones: VecDeque::with_capacity(max_active_resources),
            chunks_per_zone,
            max_active_resources,
        }
    }

    // Get a zone to write to
    pub fn remove(&mut self) -> Result<ZoneIndex, ()> {
        if self.is_full() {
            // Need to evict
            return Err(());
        }

        let can_open_more_zones =
            self.open_zones.len() < self.max_active_resources && self.free_zones.len() > 0;
        let zone = if can_open_more_zones {
            self.free_zones.pop_front()
        } else {
            self.open_zones.pop_front()
        };
        let mut zone = zone.ok_or(())?;
        zone.chunks_available -= 1;
        if zone.chunks_available >= 1 {
            self.open_zones.push_back(zone);
        }
        Ok(zone.index)
    }

    // Get the location to write to for block devices
    pub fn remove_chunk_location(&mut self) -> Result<ChunkLocation, ()> {
        if self.is_full() {
            // Need to evict
            return Err(());
        }

        let can_open_more_zones =
            self.open_zones.len() < self.max_active_resources && self.free_zones.len() > 0;
        let zone = if can_open_more_zones {
            self.free_zones.pop_front()
        } else {
            self.open_zones.pop_front()
        };
        let mut zone = zone.ok_or(())?;
        zone.chunks_available -= 1;
        if zone.chunks_available >= 1 {
            self.open_zones.push_back(zone);
        }
        Ok(ChunkLocation {
            zone: zone.index,
            // + 1 since we subtracted by 1 earlier
            index: (self.chunks_per_zone - (zone.chunks_available + 1)) as u64,
        })
    }

    // Check if all zones are full
    pub fn is_full(&self) -> bool {
        self.free_zones.is_empty() && self.open_zones.is_empty()
    }

    // Reset the selected zone
    #[allow(dead_code)]
    pub fn reset_zone(&mut self, idx: ZoneIndex) {
        self.free_zones.push_back(Zone {
            index: idx,
            chunks_available: self.chunks_per_zone,
        });
    }

    pub fn reset_zones(&mut self, indices: &[ZoneIndex]) {
        for idx in indices {
            self.free_zones.push_back(Zone {
                index: *idx,
                chunks_available: self.chunks_per_zone,
            });
        }
    }

    pub fn reset_zone_with_capacity(&mut self, idx: ZoneIndex, remaining: usize) {
        self.free_zones.push_back(Zone {
            index: idx,
            chunks_available: remaining,
        });
    }
}
