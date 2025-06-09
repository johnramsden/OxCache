use std::os::fd::RawFd;
use crate::cache::bucket::ChunkLocation;

pub struct Zoned {
    fd: RawFd,
    timeout: u32,
    logical_block_size: usize,
    nsid: u32, // Can we set once?
    zone_size: u64,
}

pub struct BlockInterface {}

pub trait Device: Send + Sync {
    fn append(&self, data: Vec<u8>) -> std::io::Result<ChunkLocation>;
    
    fn new(device: &str) -> std::io::Result<Self> where Self: Sized; // Args?
}

impl Device for Zoned {
    /// Hold internal state to keep track of zone state
    fn new(device: &str) -> std::io::Result<Self> {
        unimplemented!(); // TODO: REMOVE
        Ok(Self {
            fd: nvme::zns_open(device).expect(format!("Failed to open zoned device {}", device).as_str()),
            // TODO:
            timeout: 0,
            logical_block_size: 0,
            nsid: 0,
            zone_size: 0,
        })
    }
    fn append(&self, data: Vec<u8>) -> std::io::Result<ChunkLocation> {
        Ok(ChunkLocation::new(0, 0))
    }
}

impl Device for BlockInterface {
    /// Hold internal state to keep track of "ssd" zone state
    fn new(device: &str) -> std::io::Result<Self> {
        Ok(Self {})
    }
    
    fn append(&self, data: Vec<u8>) -> std::io::Result<ChunkLocation> {
        Ok(ChunkLocation::new(0, 0))
    }
}