use nvme::info::report_zones_all;
use nvme::types::{Chunk, ZoneState};

use crate::cache::bucket::ChunkLocation;
use crate::device;
use crate::zone_state::zone_list::ZoneObtainFailure::{EvictNow, Wait};
use std::collections::{HashMap, HashSet, VecDeque};
use std::io::{self};

type ZoneIndex = nvme::types::Zone;

#[derive(Debug, Copy, Clone)]
pub struct Zone {
    pub index: ZoneIndex,
    pub chunks_available: Chunk,
}

#[derive(Debug)]
pub enum ZoneObtainFailure {
    EvictNow,
    Wait,
}

#[derive(Debug)]
pub struct ZoneList {
    pub free_zones: VecDeque<Zone>, // Unopened zones with full capacity
    pub open_zones: VecDeque<Zone>, // Opened zones
    pub writing_zones: HashMap<ZoneIndex, u32>, // Count of currently writing threads
    pub chunks_per_zone: Chunk,
    pub max_active_resources: usize,
}

impl ZoneList {
    pub fn new(num_zones: ZoneIndex, chunks_per_zone: Chunk, max_active_resources: usize) -> Self {
        // List of all zones, initially all are "free"
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
            return Err(Wait);
            // It's worth noting that this case only occurs when all open zones are full, but are
            // still being written to. There can still be free zones, we just can't open them yet.
            // We should wait for the zone to be free somehow
        }

        let zone = if can_open_more_zones {
            let res = self.free_zones.pop_front();
            log::debug!("[ZoneList]: Opening zone {}", res.as_ref().unwrap().index);
            res
        } else {
            let res = self.open_zones.pop_front();
            log::debug!(
                "[ZoneList]: Using existing zone {} with {} chunks",
                res.as_ref().unwrap().index,
                res.as_ref().unwrap().chunks_available
            );
            res
        };
        let mut zone = zone.unwrap();

        if zone.chunks_available <= 0 {
            panic!("[ZoneList]: Checked subtraction failed: {:?}", self);
        }

        zone.chunks_available -= 1;
        if zone.chunks_available >= 1 {
            log::debug!("[ZoneList]: Returning zone back to use: {}", zone.index);
            self.open_zones.push_back(zone);
        } else {
            log::debug!("[ZoneList]: Not returning zone back to use: {}", zone.index);
        }

        self.writing_zones
            .entry(zone.index)
            .and_modify(|v| *v += 1)
            .or_insert(1);
        log::debug!(
            "[ZoneList]: Now {} threads are writing into zone {}",
            self.writing_zones.get(&zone.index).unwrap(),
            zone.index
        );
        Ok(zone.index)
    }

    // Zoned implementations should call this once they are finished with appending.
    pub fn write_finish(
        &mut self,
        zone_index: ZoneIndex,
        device: &dyn device::Device,
        finish_zone: bool
    ) -> io::Result<()> {
        let write_num = self.writing_zones.get(&zone_index).unwrap();
        log::debug!("[ZoneList]: Finishing write to {}, writenum = {}", zone_index, write_num);
        if write_num - 1 == 0 {
                log::debug!("[ZoneList]: Removing {} from writing zones, finish = {:?}", zone_index, finish_zone);
            self.writing_zones.remove(&zone_index);

            if finish_zone {
                let res = device.finish_zone(zone_index);
                let (_nz, zones) = report_zones_all(device.get_fd(), device.get_nsid()).unwrap();
                assert!(zones[zone_index as usize].zone_state == ZoneState::Closed || zones[zone_index as usize].zone_state == ZoneState::Full, "{:?} got instead", zones[zone_index as usize].zone_state);
                if res.is_err() {
                    panic!("{:?}", res);
                }
                log::debug!("[ZoneList]: Finishing {}", zone_index);
                res
            } else {
                Ok(())
            }
        } else {
            log::debug!("[ZoneList]: Decrementing {}", zone_index);
            self.writing_zones.insert(zone_index, write_num - 1);
            Ok(())
        }
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
        // TODO: This is slow, O(n)

        // I don't think it's super slow because there will be at most
        // 2 * max_active_resources zones.
        let open_zone_list = self
            .open_zones
            .iter()
            .map(|zone| &zone.index)
            .collect::<HashSet<&ZoneIndex>>();

        self.writing_zones
            .keys()
            .into_iter()
            .collect::<HashSet<&ZoneIndex>>()
            .union(&open_zone_list)
            .count()
    }

    // Reset the selected zone
    pub fn reset_zone(
        &mut self,
        idx: ZoneIndex,
        device: &dyn device::Device,
    ) -> std::io::Result<()> {
        debug_assert!(!self.writing_zones.contains_key(&idx));

        debug_assert!({
            let mut zone_indices = self.open_zones.iter().map(|zone| zone.index);
            !zone_indices.any(|zidx| zidx == idx)
        });

        self.free_zones.push_back(Zone {
            index: idx,
            chunks_available: self.chunks_per_zone,
        });

        device.reset_zone(idx)
    }

    pub fn reset_zones(
        &mut self,
        indices: &[ZoneIndex],
        device: &dyn device::Device,
    ) -> std::io::Result<()> {
        for idx in indices {
            self.reset_zone(*idx, device)?;
        }

        Ok(())
    }

    pub fn reset_zone_with_capacity(&mut self, idx: ZoneIndex, remaining: Chunk) {
        self.free_zones.push_back(Zone {
            index: idx,
            chunks_available: remaining,
        });
    }

    pub fn get_num_available_chunks(&self) -> Chunk {
        self.open_zones
            .iter()
            .fold(0, |avail, zone| avail + zone.chunks_available)
            + (self.free_zones.len() as ZoneIndex * self.chunks_per_zone)
    }
}

#[cfg(test)]
mod zone_list_tests {
    
    use std::sync::Arc;

    use bytes::Bytes;
    use nvme::types::{Byte, LogicalBlock, NVMeConfig, Zone};
    use crate::{
        cache::{Cache, bucket::ChunkLocation},
        device::Device,
        eviction::EvictTarget,
        zone_state::zone_list::ZoneObtainFailure::{EvictNow, Wait},
    };

    use super::ZoneList;

    struct MockDevice {}

    impl Device for MockDevice {
        fn append(&self, _data: Bytes) -> std::io::Result<ChunkLocation> {
            Ok(ChunkLocation { zone: 0, index: 0 })
        }

        fn read_into_buffer(
            &self, max_write_size: Byte, lba_loc: LogicalBlock, read_buffer: &mut [u8], nvme_config: &NVMeConfig
        ) -> std::io::Result<()> {
            Ok(())
        }

        /// This is expected to remove elements from the cache as well
        fn evict(&self, _locations: EvictTarget, _cache: Arc<Cache>) -> std::io::Result<()> {
            Ok(())
        }

        fn read(&self, _location: ChunkLocation) -> std::io::Result<Bytes> {
            Ok(Bytes::new())
        }

        fn get_num_zones(&self) -> Zone {
            0
        }

        fn get_chunks_per_zone(&self) -> nvme::types::Chunk {
            0
        }
        fn get_block_size(&self) -> Byte {
            0
        }
        fn get_use_percentage(&self) -> f32 {
            0.0
        }

        fn reset(&self) -> std::io::Result<()> {
            Ok(())
        }

        fn reset_zone(&self, _zone_id: Zone) -> std::io::Result<()> {
            Ok(())
        }

        fn close_zone(&self, _zone_id: Zone) -> std::io::Result<()> {
            Ok(())
        }
        fn finish_zone(&self, _zone_id: Zone) -> std::io::Result<()> {
            Ok(())
        }

        fn get_fd(&self) -> i32 {
            0
        }
        fn get_nsid(&self) -> u32 {
            0
        }
    }

    #[macro_export]
    macro_rules! assert_state {
        ( $x:expr, Ok ) => {{
            match $x.remove() {
                Ok(_) => (),
                Err(err) => match err {
                    EvictNow => assert!(false, "Should have gotten a zone, got evict now instead"),
                    Wait => assert!(false, "Should have gotten a zone, got wait instead"),
                },
            }
        }};
        ( $x:expr, EvictNow ) => {{
            match $x.remove() {
                Ok(zone) => assert!(false, "Should evict, gotten zone instead: {}", zone),
                Err(err) => match err {
                    EvictNow => (),
                    Wait => assert!(false, "Should evict, got wait instead"),
                },
            }
        }};
        ( $x:expr, Wait ) => {{
            match $x.remove() {
                Ok(zone) => assert!(false, "Should evict, gotten zone instead: {}", zone),
                Err(err) => match err {
                    EvictNow => assert!(false, "Should wait, got evict now instead"),
                    Wait => (),
                },
            }
        }};
    }

    #[test]
    fn test_basic() {
        let md = MockDevice {};
        let num_zones = 2;
        let chunks_per_zone = 2;
        let max_active_resources = 2;

        let mut zonelist = ZoneList::new(num_zones, chunks_per_zone, max_active_resources);

        let zone = zonelist.remove().unwrap();
        assert!(zone == 0);
        assert!(zonelist.get_open_zones() == 1);

        let zone = zonelist.remove().unwrap();
        assert!(zone == 1);
        assert!(zonelist.get_open_zones() == 2);

        let zone = zonelist.remove().unwrap();
        assert!(zone == 0);
        assert!(zonelist.get_open_zones() == 2);

        let zone = zonelist.remove().unwrap();
        assert!(zone == 1);
        assert!(zonelist.get_open_zones() == 2);

        assert_state!(zonelist, EvictNow);

        assert!(zonelist.is_full());

        zonelist.write_finish(0, &md, false);
        zonelist.write_finish(0, &md, false);
        assert!(zonelist.get_open_zones() == 1);

        zonelist.reset_zone(0, &md).unwrap();

        let zone = zonelist.remove().unwrap();
        assert!(zone == 0);
        assert!(zonelist.get_open_zones() == 2);

        zonelist.write_finish(1, &md, false);
        zonelist.write_finish(1, &md, false);
        assert!(zonelist.get_open_zones() == 1);

        let zone = zonelist.remove().unwrap();
        assert!(zone == 0);
        assert!(zonelist.get_open_zones() == 1);
    }

    #[test]
    fn test_mar() {
        let md = MockDevice {};
        let num_zones = 2;
        let chunks_per_zone = 2;
        let max_active_resources = 1;

        let mut zonelist = ZoneList::new(num_zones, chunks_per_zone, max_active_resources);
        let zone = zonelist.remove().unwrap();
        assert!(zone == 0);
        let zone = zonelist.remove().unwrap();
        assert!(zone == 0);

        assert!(
            zonelist.get_open_zones() == 1,
            "Open zones is is {}",
            zonelist.get_open_zones()
        );

        assert_state!(zonelist, Wait);
        zonelist.write_finish(0, &md, false);
        assert_state!(zonelist, Wait);
        zonelist.write_finish(0, &md, false);

        let zone = zonelist.remove().unwrap();
        assert!(zone == 1);
        let zone = zonelist.remove().unwrap();
        assert!(zone == 1);
        assert_state!(zonelist, EvictNow);
    }
}
