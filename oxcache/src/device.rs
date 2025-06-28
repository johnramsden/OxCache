use nvme::ops::zns_append;
use nvme::types::ZNSConfig;
use std::collections::VecDeque;
use std::fs::OpenOptions;
use std::os::fd::{AsRawFd, RawFd};
use std::sync::{Arc, Mutex};
use std::thread::available_parallelism;

use crate::cache::bucket::ChunkLocation;
use crate::device;

#[derive(Copy, Clone)]
struct Zone {
    index: usize,
    chunks_available: usize,
}

struct ZoneList {
    available_zones: VecDeque<Zone>,
}

impl ZoneList {
    fn new(num_zones: usize, chunks_available: usize) -> Self {
        let mut avail_zones = VecDeque::with_capacity(num_zones as usize);

        for i in 0..num_zones {
            avail_zones.push_back(Zone {
                index: i,
                chunks_available: chunks_available,
            });
        }
        ZoneList {
            available_zones: avail_zones,
        }
    }

    fn remove(&mut self) -> Result<usize, ()> {
        if self.is_full() {
            return Err(());
        }

        let mut zone = self.available_zones.pop_front().unwrap();
        if zone.chunks_available > 1 {
            zone.chunks_available -= 1;
            self.available_zones.push_front(zone);
        }
        Ok(zone.index)
    }

    fn is_full(&self) -> bool {
        self.available_zones.is_empty()
    }

    fn reset(&mut self, idx: usize, reset_to: usize) {
        self.available_zones.push_back(Zone {
            index: idx,
            chunks_available: reset_to,
        });
    }
}

pub struct Zoned {
    config: ZNSConfig,
    zones: Arc<Mutex<ZoneList>>,
}

pub struct BlockInterface {
    fd: RawFd,
}

pub trait Device: Send + Sync {
    fn append(&self, data: Vec<u8>) -> std::io::Result<ChunkLocation>;

    fn new(device: &str, chunk_size: usize) -> std::io::Result<Self>
    where
        Self: Sized; // Args

	fn reset(&self, zone_idx: usize) -> std::io::Result<()>;

    // fn read()
}
pub fn get_device(device: &str, chunk_size: usize) -> std::io::Result<Arc<dyn Device>> {
    // TODO: If dev type Zoned..., else
    Ok(Arc::new(device::BlockInterface::new(device, chunk_size)?))
}

impl Zoned {
    fn get_free_zone(&self) -> std::io::Result<usize> {
        let mtx = Arc::clone(&self.zones);
        let mut zone_list = mtx.lock().unwrap();
        match zone_list.remove() {
            Ok(zone_idx) => Ok(zone_idx),
            Err(()) => {
                Err(std::io::Error::new(
                    std::io::ErrorKind::StorageFull,
                    "Cache is full",
                ))
            }
        }
    }
}

impl Device for Zoned {
    /// Hold internal state to keep track of zone state
    fn new(device: &str, chunk_size: usize) -> std::io::Result<Self> {
        match nvme::info::zns_get_info(device) {
            Ok(mut config) => {
                config.chunks_per_zone = config.zone_size / chunk_size as u64;
                let zone_list = ZoneList::new(
                    config.num_zones as usize,
                    chunk_size / config.zone_size as usize,
                );

                Ok(Self {
                    config,
                    zones: Arc::new(Mutex::new(zone_list)),
                })
            }
            Err(err) => Err(err.try_into().unwrap()),
        }
    }

    fn append(&self, data: Vec<u8>) -> std::io::Result<ChunkLocation> {
        let zone_index = self.get_free_zone()?;
        let mut mut_data = Vec::clone(&data);
        match zns_append(&self.config, zone_index as u64, mut_data.as_mut_slice()) {
            Ok(lba) => Ok(ChunkLocation::new(zone_index, lba)),
            Err(err) => Err(err.try_into().unwrap()),
        }
    }

	fn reset(&self, zone_idx: usize) -> std::io::Result<()> {

		let mtx = Arc::clone(&self.zones)
		let mut zones = mtx.lock().unwrap();

		zones.reset(zone_idx, self.config.chunks_per_zone as usize);

		Ok(())
	}
}

impl Device for BlockInterface {
    /// Hold internal state to keep track of "ssd" zone state
    fn new(device: &str) -> std::io::Result<Self> {
        let handle = OpenOptions::new().read(true).write(true).open(device)?;
        let fd = handle.as_raw_fd();
        Ok(Self { fd })
    }

    fn append(&self, data: Vec<u8>) -> std::io::Result<ChunkLocation> {
        Ok(ChunkLocation::new(0, 0))
    }
}
