use std::collections::VecDeque;
use std::fs::OpenOptions;
use std::os::fd::{AsRawFd, RawFd};
use std::sync::Arc;
use nvme::ops::zns_append;
use nvme::types::ZNSConfig;

use crate::cache::bucket::ChunkLocation;
use crate::device;

struct Zone {
    index: usize,
    chunks_available: usize
}

pub struct Zoned {
    config: ZNSConfig,
    available_zones: VecDeque<Zone>
}

pub struct BlockInterface {
    fd: RawFd,
}

pub trait Device: Send + Sync {
    fn append(&self, data: Vec<u8>) -> std::io::Result<ChunkLocation>;
    
    fn new(device: &str) -> std::io::Result<Self> where Self: Sized; // Args

    // fn read()
}
pub fn get_device(device: &str) -> std::io::Result<Arc<dyn Device>> {
    // TODO: If dev type Zoned..., else
    Ok(Arc::new(device::BlockInterface::new(device)?))
}

impl Zoned {
    fn get_free_zone(&self) -> std::io::Result<usize> {
        todo!();
    }
}

impl Device for Zoned {
    /// Hold internal state to keep track of zone state
    fn new(device: &str) -> std::io::Result<Self> {
        match nvme::info::zns_get_info(device) {
            Ok(config) => {
                // let 
                config.num_zones;

                Ok(Self {
                    config,
                    available_zones: todo!()
                })   
            },
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
}

impl Device for BlockInterface {
    /// Hold internal state to keep track of "ssd" zone state
    fn new(device: &str) -> std::io::Result<Self> {
        let handle = OpenOptions::new()
            .read(true)
            .write(true)
            .open(device)?;
        let fd = handle.as_raw_fd();
        Ok(Self { fd })
    }
    
    fn append(&self, data: Vec<u8>) -> std::io::Result<ChunkLocation> {
        Ok(ChunkLocation::new(0, 0))
    }
}
