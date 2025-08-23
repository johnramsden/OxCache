use std::{io::{Error, ErrorKind}, sync::{atomic::AtomicUsize, Arc, Condvar}};

use bytes::Bytes;
use flume::Sender;
use nvme::{ops::zns_append, types::{Byte, LogicalBlock, NVMeConfig}};
use std::sync::Mutex;
use oxcache::{
    cache::bucket::ChunkLocation, device::Device, eviction::EvictorMessage, zone_state::zone_list::{self, ZoneList, ZoneObtainFailure}
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
    zone_state: Vec<Zone>,
    nvme_config: NVMeConfig,

    zone_pointers: Vec<usize>, // Stores the write pointer in chunks
    open_resources: usize,
    max_open_resources: AtomicUsize,
    zones: Arc<(Mutex<ZoneList>, Condvar)>,
}

impl MockZonedDevice {
    fn new(
        _device: &str,
        chunk_size: usize,
        eviction_channel: Sender<EvictorMessage>,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        let nzones = 30;
        let chunks_per_zone = 100;
        let max_open_resources = AtomicUsize::new(10);

        let zone_state = (0..nzones)
            .map(|zidx| Zone::new(zidx, chunks_per_zone))
            .collect();

        let zone_pointers = (0..nzones).collect();
        let open_resources = 0;
        let zone_list = ZoneList::new(nzones as u64, chunks_per_zone as u64, 10);

        Ok(Self {
            chunk_size,
            eviction_channel,
            zone_state,
            zone_pointers,
            open_resources,
            max_open_resources,
            zones: Arc::new((Mutex::new(zone_list), Condvar::new())),
            nvme_config: todo!(),
        })
    }

    fn get_free_zone(&self) -> std::io::Result<nvme::types::Zone> {
        let (mtx, wait_notify) = &*self.zones;
        let mut zone_list = mtx.lock().unwrap();
        match zone_list.remove() {
            Ok(zone_idx) => Ok(zone_idx),
            Err(error) => match error {
                ZoneObtainFailure::EvictNow => {
                    Err(Error::new(ErrorKind::StorageFull, "Cache is full"))
                }
                ZoneObtainFailure::Wait => loop {
                    zone_list = wait_notify.wait(zone_list).unwrap();
                    match zone_list.remove() {
                        Ok(idx) => return Ok(idx),
                        Err(err) => match err {
                            ZoneObtainFailure::EvictNow => {
                                return Err(Error::new(ErrorKind::Other, "Cache is full"));
                            }
                            ZoneObtainFailure::Wait => continue,
                        },
                    }
                },
            },
        }
    }
}



/// Does not actually write data. Only validates that the state of things are correct
impl Device for MockZonedDevice {
    fn append(&self, data: Bytes) -> std::io::Result<oxcache::cache::bucket::ChunkLocation> {
        // let zone_index = loop {
        //     match self.get_free_zone() {
        //         Ok(res) => break res,
        //         Err(err) => {
        //             eprintln!("[append] Failed to get free zone: {}", err);
        //         }
        //     };
        //     trigger_eviction(self.eviction_channel.clone())?;
        // };
        // // Note: this performs a copy every time because we need to
        // // pass in a mutable vector to libnvme
        // assert_eq!(
        //     data.as_ptr() as usize % self.nvme_config.logical_block_size as usize,
        //     0
        // );

        // match zns_append(
        //     &self.nvme_config,
        //     &self.config,
        //     zone_index as u64,
        //     data.as_ref(),
        // ) {
        //     Ok(lba) => {
        //         self.complete_write(zone_index)?;
        //         let chunk = lba / self.config.chunk_size as u64;
        //         Ok(ChunkLocation::new(zone_index, chunk))
        //     }
        //     Err(mut err) => {
        //         self.complete_write(zone_index)?;
        //         err.add_context(format!("Write failed at zone {}\n", zone_index));
        //         Err(err.try_into().unwrap())
        //     }
        // }

        Ok(ChunkLocation { zone: 1, index: 2 })
    }

    fn read_into_buffer(
        &self,
        _max_write_size: Byte,
        _lba_loc: LogicalBlock,
        _read_buffer: &mut [u8],
        _nvme_config: &NVMeConfig,
    ) -> std::io::Result<()> {
        // no-op
        return Ok(())
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

    fn get_num_zones(&self) -> nvme::types::Zone {
        todo!()
    }

    fn get_chunks_per_zone(&self) -> nvme::types::Chunk {
        todo!()
    }

    fn get_block_size(&self) -> Byte {
        todo!()
    }

    fn get_use_percentage(&self) -> f32 {
        todo!()
    }

    fn reset(&self) -> std::io::Result<()> {
        todo!()
    }

    fn reset_zone(&self, zone_id: nvme::types::Zone) -> std::io::Result<()> {
        todo!()
    }

    fn close_zone(&self, zone_id: nvme::types::Zone) -> std::io::Result<()> {
        todo!()
    }

    fn finish_zone(&self, zone_id: nvme::types::Zone) -> std::io::Result<()> {
        todo!()
    }

    fn get_fd(&self) -> std::os::unix::prelude::RawFd {
        todo!()
    }

    fn get_nsid(&self) -> u32 {
        todo!()
    }
}
