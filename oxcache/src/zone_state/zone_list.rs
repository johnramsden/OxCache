use crate::cache::bucket::ChunkLocation;
use crate::zone_state::zone_list::ZoneObtainFailure::EvictNow;
use std::collections::{HashMap, HashSet, VecDeque};

type ZoneIndex = usize;

#[derive(Copy, Clone)]
pub struct Zone {
    pub index: ZoneIndex,
    pub chunks_available: usize,
}

pub enum ZoneObtainFailure {
    EvictNow,
    Wait,
}

pub struct ZoneList {
    free_zones: VecDeque<Zone>, // Unopened zones with full capacity
    open_zones: VecDeque<Zone>, // Opened zones
    writing_zones: HashMap<ZoneIndex, u32>, // Count of currently writing threads
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
            writing_zones: HashMap::with_capacity(max_active_resources),
            chunks_per_zone,
            max_active_resources,
        }
    }

    // Get a zone to write to
    pub fn remove(&mut self) -> Result<ZoneIndex, ZoneObtainFailure> {
        if self.is_full() {
            // Need to evict
            return Err(EvictNow);
        }

        let can_open_more_zones =
            self.get_open_zones() < self.max_active_resources && self.free_zones.len() > 0;

        // If we can't open any more zones, and we can't get existing open zones...
        if !can_open_more_zones && self.open_zones.is_empty() {
            // ... due to a lack of free zones...
            if self.free_zones.is_empty() {
                // ... then we should evict.
                return Err(EvictNow);
            }

            // ... due to all zones being unavailable...
            // ... then we should wait until there is an open zone available.
            return Err(EvictNow); // We need to evict anyway. Bottom is left for understanding.
            // return Err(Wait);
            // It's worth noting that this case only occurs when all zones are full, but are still being written to. It could be worth it to simply evict in this case instead of waiting.
        }

        let zone = if can_open_more_zones {
            self.free_zones.pop_front()
        } else {
            self.open_zones.pop_front()
        };
        let mut zone = zone.unwrap();
        zone.chunks_available -= 1;
        if zone.chunks_available >= 1 {
            self.open_zones.push_back(zone);
        }

        self.writing_zones
            .entry(zone.index)
            .and_modify(|v| *v += 1)
            .or_insert(1);

        Ok(zone.index)
    }

    // Zoned implementations should call this once they are finished with appending.
    pub fn write_finish(&mut self, zone_index: ZoneIndex) {
        let write_num = self.writing_zones.get(&zone_index).unwrap();
        if write_num - 1 == 0 {
            self.writing_zones.remove(&zone_index)
        } else {
            self.writing_zones.insert(zone_index, write_num - 1)
        };
    }

    // Get the location to write to for block devices
    // Do not need the active zone bookkeeping, so it's simpler here
    // Writing_zones should be considered unused
    pub fn remove_chunk_location(&mut self) -> Result<ChunkLocation, ZoneObtainFailure> {
        if self.is_full() {
            // Need to evict
            return Err(EvictNow);
        }

        let can_open_more_zones =
            self.open_zones.len() < self.max_active_resources && self.free_zones.len() > 0;

        let zone = if can_open_more_zones {
            self.free_zones.pop_front()
        } else {
            self.open_zones.pop_front()
        };
        let mut zone = zone.unwrap();
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

    // Gets the number of open zones by counting the unique
    // zones listed in open_zones and writing_zones
    pub fn get_open_zones(&self) -> usize {
        let open_zone_list = self
            .open_zones
            .iter()
            .map(|zone| &zone.index)
            .collect::<HashSet<&ZoneIndex>>();

        self.writing_zones
            .keys()
            .into_iter()
            .collect::<HashSet<&ZoneIndex>>()
            .intersection(&open_zone_list)
            .count()
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
