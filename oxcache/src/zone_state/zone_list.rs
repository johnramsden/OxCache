use nvme::info::report_zones_all;
use nvme::types::{Chunk, ZoneState};

use crate::cache::bucket::ChunkLocation;
use crate::device;
use crate::zone_state::zone_list::ZoneObtainFailure::{EvictNow, Wait};
use std::collections::{HashMap, HashSet, VecDeque};
use std::io::{self};

type ZoneIndex = nvme::types::Zone;
type ZonePriority = usize;

#[derive(Debug, Clone)]
pub struct Zone {
    pub index: ZoneIndex,
    pub chunks_available: Vec<Chunk>,
}

#[derive(Debug)]
pub enum ZoneObtainFailure {
    EvictNow,
    Wait,
}

#[derive(Debug, PartialEq, Eq)]
pub enum ZoneStateDbg {
    Closed, Open, Writing, Finished
}

#[derive(Debug)]
pub struct ZoneDbg {
    pub state: ZoneStateDbg,
    pub chunk_ptr: Chunk,
}

#[derive(Debug)]
pub struct ZoneList {
    pub zones: HashMap<ZoneIndex, Zone>, // Zones referenced by {free,open}_zones
    pub free_zones: VecDeque<ZoneIndex>, // Unopened zones with full capacity
    pub open_zones: VecDeque<ZoneIndex>, // Opened zones
    pub writing_zones: HashMap<ZoneIndex, u32>, // Count of currently writing threads
    pub chunks_per_zone: Chunk,
    pub max_active_resources: usize,

    #[cfg(debug_assertions)]
    pub state_tracker: Vec<ZoneDbg>,
}

impl ZoneList {
    pub fn new(num_zones: ZoneIndex, chunks_per_zone: Chunk, max_active_resources: usize) -> Self {
        debug_assert!(num_zones > 0);
        debug_assert!(max_active_resources > 0);
        debug_assert!(chunks_per_zone > 0);
        debug_assert!(num_zones >= max_active_resources as u64);

        // List of all zones, initially all are "free"
        let avail_zones = (0..num_zones).collect();
        let zones = (0..num_zones)
            .map(|item| (item, Zone {
                index: item,
                chunks_available: (0..chunks_per_zone).rev().collect()
            })) // (key, value)
            .collect();

        // Debug list
        let state_tracker = (0..num_zones)
            .map(|_| {
                ZoneDbg {
                    state: ZoneStateDbg::Closed,
                    chunk_ptr: 0,
                }
            })
            .collect();

        ZoneList {
            free_zones: avail_zones,
            open_zones: VecDeque::with_capacity(max_active_resources),
            writing_zones: HashMap::with_capacity(max_active_resources),
            chunks_per_zone,
            max_active_resources: max_active_resources-1, // Keep one reserved for eviction
            zones,
            state_tracker,
        }
    }

    // Get a zone to write to
    pub fn remove(&mut self) -> Result<ZoneIndex, ZoneObtainFailure> {
        #[cfg(debug_assertions)]
        self.check_invariants();
        
        if self.is_full() {
            // Need to evict
            log::debug!("Full, need to evict now");
            return Err(EvictNow);
        }

        // Open first if possible
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
            // Open a new zone
            let res = self.free_zones.pop_front().unwrap();
            log::debug!("[ZoneList]: Opening zone {}", res);

            #[cfg(debug_assertions)]
            {
                assert_eq!(self.state_tracker[res as usize].state, ZoneStateDbg::Closed);
                self.state_tracker[res as usize].state = ZoneStateDbg::Writing;
            }

            self.zones.get_mut(&res)
        } else {
            // Grab an existing zone
            let res = self.open_zones.pop_front().unwrap();

            #[cfg(debug_assertions)]
            assert!(
                self.state_tracker[res as usize].state == ZoneStateDbg::Open ||
                self.state_tracker[res as usize].state == ZoneStateDbg::Writing
            );

            let res = self.zones.get_mut(&res);
            log::debug!(
                "[ZoneList]: Using existing zone {} with {:?} chunks",
                res.as_ref().unwrap().index,
                res.as_ref().unwrap().chunks_available
            );
            res
        };

        let mut zone = zone.unwrap();
        let zone_index = zone.index;

        if zone.chunks_available.len() <= 0 {
            panic!("[ZoneList]: Checked subtraction failed: {:?}", self);
        }

        zone.chunks_available.pop();
        #[cfg(debug_assertions)]
        {
            assert_eq!(self.state_tracker[zone.index as usize].state, ZoneStateDbg::Open);
            self.state_tracker[zone.index as usize].chunk_ptr += 1;
            assert_eq!(self.chunks_per_zone - (zone.chunks_available.len() as u64), self.state_tracker[zone.index as usize].chunk_ptr);
        }

        if zone.chunks_available.len() >= 1 {
            log::debug!("[ZoneList]: Returning zone back to use: {}", zone_index);
            self.open_zones.push_back(zone_index);
        } else {
            log::debug!("[ZoneList]: Not returning zone back to use: {}", zone_index);
        }

        self.writing_zones
            .entry(zone_index)
            .and_modify(|v| *v += 1)
            .or_insert(1);
        log::debug!(
            "[ZoneList]: Now {} threads are writing into zone {}",
            self.writing_zones.get(&zone_index).unwrap(),
            zone_index
        );

        #[cfg(debug_assertions)]
        self.check_invariants();

        Ok(zone_index)
    }

    // Zoned implementations should call this once they are finished with appending.
    pub fn write_finish(
        &mut self,
        zone_index: ZoneIndex,
        device: &dyn device::Device,
        finish_zone: bool,
    ) -> io::Result<()> {
        #[cfg(debug_assertions)]
        assert_eq!(self.state_tracker[zone_index as usize].state, ZoneStateDbg::Writing);

        let write_num = self.writing_zones.get(&zone_index);
        let write_num = if let Some(write_num) = write_num {
            write_num
        } else {
            return Ok(()); // We were evicting, so we didn't account
        };
        log::debug!(
            "[ZoneList]: Finishing write to {}, writenum = {}",
            zone_index,
            write_num
        );
        if write_num - 1 == 0 {
            log::debug!(
                "[ZoneList]: Removing {} from writing zones, finish = {:?}",
                zone_index,
                finish_zone
            );
            self.writing_zones.remove(&zone_index);

            #[cfg(debug_assertions)]
            {
                self.state_tracker[zone_index as usize].state = ZoneStateDbg::Open;
            }

            if finish_zone {
                let res = device.finish_zone(zone_index);
                let (_nz, zones) = report_zones_all(device.get_fd(), device.get_nsid()).unwrap();
                assert!(
                    zones[zone_index as usize].zone_state == ZoneState::Closed
                        || zones[zone_index as usize].zone_state == ZoneState::Full,
                    "{:?} got instead",
                    zones[zone_index as usize].zone_state
                );
                if res.is_err() {
                    panic!("{:?}", res);
                }
                log::debug!("[ZoneList]: Finishing {}", zone_index);

                #[cfg(debug_assertions)]
                {
                    self.state_tracker[zone_index as usize].state = ZoneStateDbg::Closed;
                    self.check_invariants();
                }

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
        #[cfg(debug_assertions)]
        self.check_invariants();

        if self.is_full() {
            // Need to evict
            return Err(EvictNow);
        }

        let can_open_more_zones =
            self.open_zones.len() < self.max_active_resources && self.free_zones.len() > 0;

        let zone = if can_open_more_zones {
            let z = self.free_zones.pop_front();
            #[cfg(debug_assertions)]
            {
                assert_eq!(self.state_tracker[z.unwrap() as usize].state, ZoneStateDbg::Closed);
                self.state_tracker[z.unwrap() as usize].state = ZoneStateDbg::Writing;
            };
            z
        } else {
            let z = self.open_zones.pop_front();
            #[cfg(debug_assertions)]
            {
                assert!(
                    self.state_tracker[z.unwrap() as usize].state == ZoneStateDbg::Open ||
                    self.state_tracker[z.unwrap() as usize].state == ZoneStateDbg::Writing);
            }
            z
        };
        let zone_index = zone.unwrap();
        let mut zone = self.zones.get_mut(&zone_index).unwrap();
        let chunk = zone.chunks_available.pop().unwrap();

        #[cfg(debug_assertions)]
        {
            assert_eq!(self.state_tracker[zone.index as usize].state, ZoneStateDbg::Open);
            self.state_tracker[zone.index as usize].chunk_ptr += 1;
            assert_eq!(self.chunks_per_zone - (zone.chunks_available.len() as u64), self.state_tracker[zone.index as usize].chunk_ptr);
        }
        
        if zone.chunks_available.len() >= 1 {
            self.open_zones.push_back(zone_index);
        }

        #[cfg(debug_assertions)]
        self.check_invariants();

        Ok(ChunkLocation {
            zone: zone_index,
            index: chunk,
        })
    }

    // Used to return chunks after chunk eviction
    pub fn return_chunk_location(&mut self, chunk: &ChunkLocation, return_zone: bool) {
        {
            let zone = self.zones.get_mut(&chunk.zone).unwrap();

            assert!(
                !zone.chunks_available.contains(&chunk.index),
                "Zone {} should not contain chunk {} we are trying to return",
                chunk.zone, chunk.index
            );

            log::trace!("[ZoneList]: Returning chunk {:?} to {:?}", chunk, zone.chunks_available);

            // Return it
            zone.chunks_available.push(chunk.index);

            // If len == 1, it was empty, must be returned to free zones, otherwise it already was
            // Goes in free to avoid exceeding max_active_resources
            if zone.chunks_available.len() == 1 && return_zone {
                self.free_zones.push_back(chunk.zone);
            }
            log::trace!("[ZoneList]: Returned chunk {:?} to {:?}", chunk, zone.chunks_available);
        }

        #[cfg(debug_assertions)]
        self.check_invariants();
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

        #[cfg(debug_assertions)]
        {
            self.check_invariants();
            assert_eq!(self.state_tracker[idx as usize].state, ZoneStateDbg::Finished);
            self.state_tracker[idx as usize].state = ZoneStateDbg::Closed;
            self.state_tracker[idx as usize].chunk_ptr = 0;
        }

        let zone = self.zones.get_mut(&idx).unwrap();
        zone.chunks_available = (0..self.chunks_per_zone).rev().collect();

        self.free_zones.push_back(idx);

        #[cfg(debug_assertions)]
        self.check_invariants();

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

    pub fn reset_zone_with_capacity(
        &mut self,
        idx: ZoneIndex,
        remaining: Chunk,
        device: &dyn device::Device
    ) -> std::io::Result<()> {

        #[cfg(debug_assertions)]
        {
            self.check_invariants();
            assert_eq!(self.state_tracker[idx as usize].state, ZoneStateDbg::Finished);
            self.state_tracker[idx as usize].state = ZoneStateDbg::Open;
            self.state_tracker[idx as usize].chunk_ptr = self.chunks_per_zone-remaining;
        }
        
        let zone = self.zones.get_mut(&idx).unwrap();
        zone.chunks_available = (self.chunks_per_zone-remaining..self.chunks_per_zone).rev().collect();

        #[cfg(debug_assertions)]
        self.check_invariants();

        device.reset_zone(idx)
    }

    /// Returns a zone back to the free list
    pub fn return_zone(
        &mut self,
        idx: ZoneIndex,
    ) {
        #[cfg(debug_assertions)]
        {
            self.check_invariants();
            self.state_tracker[idx as usize].state = ZoneStateDbg::Closed;
            self.state_tracker[idx as usize].chunk_ptr = 0;
        }

        self.free_zones.push_back(idx);

        #[cfg(debug_assertions)]
        self.check_invariants();
    }

    pub fn get_num_available_chunks(&self) -> Chunk {
        // Total available chunks in open zones
        let open_zone_chunks: Chunk = self
            .open_zones
            .iter()
            .map(|zone_index| {
                // Look up the Zone struct for this zone index
                let zone = self.zones.get(zone_index).unwrap();

                // Count how many chunks are still available in this zone
                zone.chunks_available.len() as Chunk
            })
            .sum();

        // Total available chunks in completely free zones
        let free_zone_chunks: Chunk =
            (self.free_zones.len() as Chunk) * self.chunks_per_zone;

        // Grand total
        open_zone_chunks + free_zone_chunks
    }

    /// Makes sure that the zone list is consistent with itself.
    fn check_invariants(&self) {

        // free_zones & open are unique and dont share elems
        {

            let set_free: HashSet<_> = self.free_zones.iter().collect();
            let set_open: HashSet<_> = self.open_zones.iter().collect();

            // Assert no overlap
            assert!(
                set_free.is_disjoint(&set_open),
                "Free and Open zone have overlapping elements"
            );

            if set_free.len() != self.free_zones.len() {
                println!("free_zones: {:?}", self.free_zones);
                println!("zones: {:?}", self.zones);
                // no dupes
                assert_eq!(set_free.len(), self.free_zones.len(), "Free list has duplicate elements");
            }
            
            assert_eq!(set_open.len(), self.open_zones.len(), "Open list has duplicate elements");

            for zone in &self.open_zones {
                assert!(
                    self.state_tracker[*zone as usize].state == ZoneStateDbg::Open ||
                    self.state_tracker[*zone as usize].state == ZoneStateDbg::Writing);
            }

            for zone in &self.free_zones {
                assert!(self.state_tracker[*zone as usize].state == ZoneStateDbg::Closed);
            }

            for zone in &self.writing_zones {
                assert!(self.state_tracker[*zone.0 as usize].state == ZoneStateDbg::Writing);
                assert!(!set_free.contains(zone.0));
                assert!(*zone.1 > 0);
            }

            let n_writing = self.state_tracker.iter().fold(0, |acc, zone|{
                match zone.state {
                    ZoneStateDbg::Closed => {
                        assert_eq!(zone.chunk_ptr, 0);
                        acc
                    },
                    ZoneStateDbg::Open => {
                        assert!(zone.chunk_ptr > 0 && zone.chunk_ptr < self.chunks_per_zone);
                        acc + 1
                    },
                    ZoneStateDbg::Writing => {
                        acc + 1
                    },
                    ZoneStateDbg::Finished => {
                        assert_eq!(zone.chunk_ptr, self.chunks_per_zone as u64);
                        acc
                    },
                }
            });

            assert!(n_writing < self.max_active_resources);
        }
    }
}

#[cfg(test)]
mod zone_list_tests {

    use std::sync::Arc;

    use crate::{
        cache::{Cache, bucket::ChunkLocation},
        device::Device,
        eviction::EvictTarget,
        zone_state::zone_list::ZoneObtainFailure::{EvictNow, Wait},
    };
    use bytes::Bytes;
    use nvme::types::{Byte, LogicalBlock, NVMeConfig, Zone};

    use super::ZoneList;

    struct MockDevice {}

    impl Device for MockDevice {
        fn append(&self, _data: Bytes) -> std::io::Result<ChunkLocation> {
            Ok(ChunkLocation { zone: 0, index: 0 })
        }

        fn read_into_buffer(
            &self,
            max_write_size: Byte,
            lba_loc: LogicalBlock,
            read_buffer: &mut [u8],
            nvme_config: &NVMeConfig,
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
