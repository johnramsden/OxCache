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

#[derive(Clone, Debug, PartialEq, Eq)]
/// For block devices, ignore the writing state. Don't need to keep
/// track of writing zones
pub enum ZoneStateDbg {
    Empty,
    ExplicitOpen,
    ImplicitOpen,
    Closed,
    Full
}

#[derive(Clone, Debug)]
pub struct ZoneDbg {
    pub state: ZoneStateDbg,
    /// A bit strange, but for block devices with chunk eviction, this
    /// effectively functions as a counter for the number of chunks
    /// remaining.
    pub chunk_ptr: Chunk,
    pub num_writers: usize,
}

impl ZoneDbg {
    pub fn new() -> Self {
        Self {
            state: ZoneStateDbg::Empty,
            chunk_ptr: 0,
            num_writers: 0
        }
    }
}

#[derive(Debug)]
pub struct ZoneStateTracker {
    pub state: Vec<ZoneDbg>,
    pub max_chunks: usize,
    pub mar: usize,
    pub mor: usize
}

impl ZoneStateTracker {
    pub fn new(num_zones: usize, max_chunks: usize, mar: usize, mor: usize) -> Self {
        Self {
            state: vec![ZoneDbg::new(); num_zones],
            max_chunks,
            mar, mor
        }
    }

    pub fn open_zone(&mut self, zone_index: ZoneIndex) {
        self.assert_zone_state(zone_index,
            &[ZoneStateDbg::Empty, ZoneStateDbg::Closed, ZoneStateDbg::ImplicitOpen]);
        self.check_zone_count();
        self.state[zone_index as usize].state = ZoneStateDbg::ExplicitOpen;
        self.check_zone_count();
    }

    pub fn close_zone(&mut self, zone_index: ZoneIndex) {
        self.assert_zone_state(zone_index,
            &[ZoneStateDbg::ImplicitOpen, ZoneStateDbg::ExplicitOpen]);
        self.check_zone_count();
        self.state[zone_index as usize].state = ZoneStateDbg::Closed;
        self.check_zone_count();
    }

    pub fn reset_zone(&mut self, zone_index: ZoneIndex) {
        self.check_zone_count();
        self.state[zone_index as usize].state = ZoneStateDbg::Empty;
        self.check_zone_count();
    }

    pub fn finish_zone(&mut self, zone_index: ZoneIndex) {
        self.assert_zone_state(zone_index,
            &[ZoneStateDbg::ImplicitOpen, ZoneStateDbg::ExplicitOpen, ZoneStateDbg::Closed]);
        self.check_zone_count();
        self.state[zone_index as usize].state = ZoneStateDbg::Empty;
        self.check_zone_count();
    }

    pub fn write_zone(&mut self, zone_index: ZoneIndex) {
        self.assert_zone_state(zone_index,
            &[ZoneStateDbg::ImplicitOpen,
                ZoneStateDbg::ExplicitOpen,
                ZoneStateDbg::Closed,
                ZoneStateDbg::Empty]);
        self.check_zone_count();


        self.state[zone_index as usize].chunk_ptr += 1;
        self.state[zone_index as usize].state = match self.state[zone_index as usize].state {
            ZoneStateDbg::Empty => ZoneStateDbg::ImplicitOpen,
            ZoneStateDbg::ExplicitOpen => ZoneStateDbg::ExplicitOpen,
            ZoneStateDbg::ImplicitOpen => ZoneStateDbg::ImplicitOpen,
            ZoneStateDbg::Closed => ZoneStateDbg::ImplicitOpen,
            ZoneStateDbg::Full => panic!("Error state"),
        };

        self.state[zone_index as usize].num_writers += 1;

        self.state[zone_index as usize].state = ZoneStateDbg::Empty;
        self.check_zone_count();
    }

    pub fn finish_write_zone(&mut self, zone_index: ZoneIndex) {
        self.assert_zone_state(zone_index,
            &[ZoneStateDbg::ImplicitOpen,
                ZoneStateDbg::ExplicitOpen,
                ZoneStateDbg::Closed,
                ZoneStateDbg::Empty]);
        self.check_zone_count();

        if self.state[zone_index as usize].num_writers == 0 {
            assert!(false);
        }
        self.state[zone_index as usize].num_writers -= 1;

        if self.state[zone_index as usize].chunk_ptr >= self.max_chunks as Chunk {
            self.state[zone_index as usize].state = ZoneStateDbg::Full;
        }
    }

    fn assert_zone_state(&self, zone_index: ZoneIndex, states: &[ZoneStateDbg]) {
        assert!(states.iter().any(
            |state| self.state[zone_index as usize].state == *state
        ))
    }

    fn check_zone_count(&self) {
        assert!(self.open_zone_count() <= self.mor);
        assert!(self.active_zone_count() <= self.mar);
    }

    fn open_zone_count(&self) -> usize {
        self.state.iter()
            .filter(|zone| {
                zone.state == ZoneStateDbg::ImplicitOpen ||
                    zone.state == ZoneStateDbg::ExplicitOpen
            })
            .count()
    }

    fn active_zone_count(&self) -> usize {
        self.state.iter()
            .filter(|zone| {
                zone.state == ZoneStateDbg::ImplicitOpen ||
                    zone.state == ZoneStateDbg::ExplicitOpen ||
                    zone.state == ZoneStateDbg::Closed
            })
            .count()
    }
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
    pub state_tracker: ZoneStateTracker,
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

        ZoneList {
            free_zones: avail_zones,
            open_zones: VecDeque::with_capacity(max_active_resources),
            writing_zones: HashMap::with_capacity(max_active_resources),
            chunks_per_zone,
            max_active_resources: max_active_resources-1, // Keep one reserved for eviction
            zones,
            #[cfg(debug_assertions)]
            state_tracker: ZoneStateTracker::new(
                num_zones as usize,
                chunks_per_zone as usize,
                max_active_resources,
                max_active_resources),
        }
    }

    // Get a zone to write to
    pub fn remove(&mut self) -> Result<ZoneIndex, ZoneObtainFailure> {
        #[cfg(debug_assertions)]
        self.check_invariants();

        if self.is_full() {
            // Need to evict
            tracing::debug!("Full, need to evict now");
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

            // #[cfg(debug_assertions)]
            // self.state_tracker.open_zone(res);

            self.zones.get_mut(&res)
        } else {
            // Grab an existing zone
            let res = self.open_zones.pop_front().unwrap();

            let res = self.zones.get_mut(&res);
            tracing::debug!(
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

        if zone.chunks_available.len() >= 1 {
            tracing::debug!("[ZoneList]: Returning zone back to use: {}", zone_index);
            self.open_zones.push_back(zone_index);
        } else {
            tracing::debug!("[ZoneList]: Not returning zone back to use: {}", zone_index);
        }

        self.writing_zones
            .entry(zone_index)
            .and_modify(|v| *v += 1)
            .or_insert(1);
        tracing::debug!(
            "[ZoneList]: Now {} threads are writing into zone {}",
            self.writing_zones.get(&zone_index).unwrap(),
            zone_index
        );

        // #[cfg(debug_assertions)]
        // self.state_tracker.write_zone(res);

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
        // #[cfg(debug_assertions)]
        // self.state_tracker.finish_write_zone(zone_index);

        let write_num = self.writing_zones.get(&zone_index);
        let write_num = if let Some(write_num) = write_num {
            write_num
        } else {
            // We will probably want to account for this in the state tracker somehow
            return Ok(()); // We were evicting, so we didn't account
        };
        tracing::debug!(
            "[ZoneList]: Finishing write to {}, writenum = {}",
            zone_index,
            write_num
        );
        if write_num - 1 == 0 {
            tracing::debug!(
                "[ZoneList]: Removing {} from writing zones, finish = {:?}",
                zone_index,
                finish_zone
            );
            self.writing_zones.remove(&zone_index);

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
                tracing::debug!("[ZoneList]: Finishing {}", zone_index);
                res
            } else {
                Ok(())
            }
        } else {
            tracing::debug!("[ZoneList]: Decrementing {}", zone_index);
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

            // #[cfg(debug_assertions)]
            // self.state_tracker.open_zone(zone_index);

            z
        } else {
            let z = self.open_zones.pop_front();
            z
        };
        let zone_index = zone.unwrap();
        let mut zone = self.zones.get_mut(&zone_index).unwrap();
        let chunk = zone.chunks_available.pop().unwrap();

        if zone.chunks_available.len() >= 1 {
            self.open_zones.push_back(zone_index);
        }

        // #[cfg(debug_assertions)]
        // self.state_tracker.write_zone(zone_index);

        #[cfg(debug_assertions)]
        self.check_invariants();

        Ok(ChunkLocation {
            zone: zone_index,
            index: chunk,
        })
    }

    // Used to return chunks after chunk eviction
    pub fn return_chunk_location(&mut self, chunk: &ChunkLocation, return_zone: bool) {
        #[cfg(debug_assertions)]
        self.check_invariants();

        // #[cfg(debug_assertions)]
        // self.state_tracker.finish_write_zone(chunk.zone);

        let zone = self.zones.get_mut(&chunk.zone).unwrap();
        tracing::trace!("[ZoneList]: Returning chunk {:?} to {:?}", chunk, zone.chunks_available);

        assert!(
            !zone.chunks_available.contains(&chunk.index),
            "Zone {} should not contain chunk {} we are trying to return",
            chunk.zone, chunk.index
        );

        tracing::trace!("[ZoneList]: Returning chunk {:?} to {:?}", chunk, zone.chunks_available);

        // Return it
        zone.chunks_available.push(chunk.index);

        // If len == 1, it was empty, must be returned to free zones, otherwise it already was
        // Goes in free to avoid exceeding max_active_resources
        if zone.chunks_available.len() == 1 && return_zone {
            self.free_zones.push_back(chunk.zone);
        }
        tracing::trace!("[ZoneList]: Returned chunk {:?} to {:?}", chunk, zone.chunks_available);

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
        self.check_invariants();

        // #[cfg(debug_assertions)]
        // self.state_tracker.reset_zone(idx);

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
        // I think we need to add a special method here for the state tracker


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
        // TODO: this doesn't map to any state transitions
        // #[cfg(debug_assertions)]
        // self.state_tracker.reset_zone(idx);

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

            // for zone in &self.open_zones {
            //     assert!(
            //         self.state_tracker[*zone as usize].state == ZoneStateDbg::Open ||
            //         self.state_tracker[*zone as usize].state == ZoneStateDbg::Writing);
            // }

            // for zone in &self.free_zones {
            //     assert!(self.state_tracker[*zone as usize].state == ZoneStateDbg::Closed);
            // }

            // for zone in &self.writing_zones {
            //     assert!(self.state_tracker[*zone.0 as usize].state == ZoneStateDbg::Writing);
            //     assert!(!set_free.contains(zone.0));
            //     assert!(*zone.1 > 0);
            // }

            // let n_writing = self.state_tracker.iter().fold(0, |acc, zone|{
            //     match zone.state {
            //         ZoneStateDbg::Closed => {
            //             assert_eq!(zone.chunk_ptr, 0);
            //             acc
            //         },
            //         ZoneStateDbg::Open => {
            //             assert!(zone.chunk_ptr > 0 && zone.chunk_ptr < self.chunks_per_zone);
            //             acc + 1
            //         },
            //         ZoneStateDbg::Writing => {
            //             acc + 1
            //         },
            //         ZoneStateDbg::Finished => {
            //             assert_eq!(zone.chunk_ptr, self.chunks_per_zone as u64);
            //             acc
            //         },
            //     }
            // });

            // assert!(n_writing < self.max_active_resources);
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
            _max_write_size: Byte,
            _lba_loc: LogicalBlock,
            _read_buffer: &mut [u8],
            _nvme_config: &NVMeConfig,
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
    #[ignore]  // TODO: FIX AFTER CHUNK EVICT MERGED
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

        zonelist.write_finish(0, &md, false).unwrap();
        zonelist.write_finish(0, &md, false).unwrap();
        assert!(zonelist.get_open_zones() == 1);

        zonelist.reset_zone(0, &md).unwrap();

        let zone = zonelist.remove().unwrap();
        assert!(zone == 0);
        assert!(zonelist.get_open_zones() == 2);

        zonelist.write_finish(1, &md, false).unwrap();
        zonelist.write_finish(1, &md, false).unwrap();
        assert!(zonelist.get_open_zones() == 1);

        let zone = zonelist.remove().unwrap();
        assert!(zone == 0);
        assert!(zonelist.get_open_zones() == 1);
    }

    #[test]
    #[ignore]  // TODO: FIX AFTER CHUNK EVICT MERGED
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
        zonelist.write_finish(0, &md, false).unwrap();
        assert_state!(zonelist, Wait);
        zonelist.write_finish(0, &md, false).unwrap();

        let zone = zonelist.remove().unwrap();
        assert!(zone == 1);
        let zone = zonelist.remove().unwrap();
        assert!(zone == 1);
        assert_state!(zonelist, EvictNow);
    }
}
