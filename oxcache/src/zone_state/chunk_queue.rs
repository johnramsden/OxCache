use crate::cache::bucket::ChunkLocation;
use std::collections::VecDeque;

type ChunkIndex = nvme::types::Chunk;
type ZoneIndex = nvme::types::Zone;

type Zone = VecDeque<ChunkIndex>;

pub struct ChunkQueue {
    free_zones: VecDeque<ZoneIndex>,
    zones: Vec<Zone>,
    chunks_per_zone: usize,
}

impl ChunkQueue {
    pub fn new(num_zones: usize, chunks_per_zone: usize) -> Self {
        let free_zones = (0..num_zones as u64).collect();
        let zones = vec![(0..chunks_per_zone as ChunkIndex).collect(); num_zones];
        Self {
            free_zones,
            zones,
            chunks_per_zone,
        }
    }

    // Get a chunk to write to
    pub fn remove(&mut self) -> Result<ChunkLocation, ()> {
        if self.is_full() {
            // Need to evict
            return Err(());
        }

        let zone_index: ZoneIndex = self.free_zones.pop_front().ok_or(())?;
        let chunk_index: ChunkIndex = self.zones[zone_index as usize].pop_front().ok_or(())?;
        if !self.zones[zone_index as usize].is_empty() {
            self.free_zones.push_back(zone_index);
        }
        Ok(ChunkLocation::new(zone_index, chunk_index))
    }

    // Check if all zones are full
    pub fn is_full(&self) -> bool {
        self.free_zones.is_empty()
    }

    // Reset the selected zone
    #[allow(dead_code)]
    pub fn reset_zone(&mut self, idx: ZoneIndex) {
        self.free_zones.push_back(idx);
        self.zones[idx as usize] = (0..self.chunks_per_zone as ChunkIndex).collect();
    }

    pub fn reset_zones(&mut self, indices: &[ZoneIndex]) {
        self.free_zones.extend(indices);
        for idx in indices {
            self.zones[*idx as usize] = (0..self.chunks_per_zone as ChunkIndex).collect();
        }
    }

    pub fn reset_chunk(&mut self, location: &ChunkLocation) {
        if self.zones[location.zone as usize].is_empty() {
            self.free_zones.push_back(location.zone);
        }
        self.zones[location.zone as usize].push_back(location.index);
    }

    pub fn reset_chunks(&mut self, locations: &[ChunkLocation]) {
        for location in locations {
            self.reset_chunk(location);
        }
    }
}
