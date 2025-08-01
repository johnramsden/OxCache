use std::error::Error;

use flume::Sender;
use oxcache::{
    device::Device,
    eviction::EvictorMessage,
    zone_state::zone_list::{self, ZoneList},
};

struct Chunk {
    zone_index: usize,
    chunk_location: usize,
    written_to: bool,
}

impl Chunk {
    pub fn new(zone_index: usize, chunk_location: usize) -> Self {
        Self {
            zone_index,
            chunk_location,
            written_to: false,
        }
    }
}

struct Zone {
    zone_index: usize,
    chunk_state: Vec<Chunk>,
}

impl Zone {
    pub fn new(zone_index: usize, chunks_per_zone: usize) -> Self {
        Self {
            zone_index,
            chunk_state: (0..chunks_per_zone)
                .map(|idx| Chunk::new(zone_index, idx))
                .collect(),
        }
    }
}

pub struct MockZonedDevice {
    chunk_size: usize,
    eviction_channel: Sender<EvictorMessage>,
    zones: Vec<Zone>,

    zone_pointers: Vec<usize>, // Stores the write pointer in chunks
    open_resources: usize,
    max_open_resources: usize,
    zone_list: ZoneList,
}

impl MockZonedDevice {
    fn new(
        _device: &str,
        chunk_size: usize,
        eviction_channel: Sender<EvictorMessage>,
    ) -> Result<Self, Box<dyn Error>> {
        let nzones = 30;
        let chunks_per_zone = 100;
        let max_open_resources = 10;

        let zones = (0..nzones)
            .map(|zidx| Zone::new(zidx, chunks_per_zone))
            .collect();

        let zone_pointers = (0..nzones).collect();
        let open_resources = 0;
        let zone_list = ZoneList::new(nzones, chunks_per_zone, max_open_resources);

        Ok(Self {
            chunk_size,
            eviction_channel,
            zones,
            zone_pointers,
            open_resources,
            max_open_resources,
            zone_list,
        })
    }
}

/// Does not actually write data. Only validates it
impl Device for MockZonedDevice {
    fn append(&self, data: bytes::Bytes) -> std::io::Result<oxcache::cache::bucket::ChunkLocation> {
        todo!()
    }

    fn read_into_buffer(
        &self,
        location: oxcache::cache::bucket::ChunkLocation,
        read_buffer: &mut [u8],
    ) -> std::io::Result<()> {
        todo!()
    }

    fn evict(
        &self,
        locations: oxcache::eviction::EvictTarget,
        cache: std::sync::Arc<oxcache::cache::Cache>,
    ) -> std::io::Result<()> {
        todo!()
    }

    fn read(
        &self,
        location: oxcache::cache::bucket::ChunkLocation,
    ) -> std::io::Result<bytes::Bytes> {
        todo!()
    }

    fn get_num_zones(&self) -> usize {
        todo!()
    }

    fn get_chunks_per_zone(&self) -> usize {
        todo!()
    }

    fn get_block_size(&self) -> usize {
        todo!()
    }

    fn get_use_percentage(&self) -> f32 {
        todo!()
    }

    fn reset(&self) -> std::io::Result<()> {
        todo!()
    }

    fn reset_zone(&self, zone_id: usize) -> std::io::Result<()> {
        todo!()
    }

    fn close_zone(&self, zone_id: usize) -> std::io::Result<()> {
        todo!()
    }
}
