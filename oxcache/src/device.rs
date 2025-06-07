use std::os::fd::RawFd;
use crate::cache::bucket::ChunkLocation;

struct Zoned {
    fd: RawFd,
    timeout: u32,
    logical_block_size: usize,
    nsid: u32, // Can we set once?
    zone_size: u64,
}

struct BlockInterface {}

trait Device {
    fn append(&self, data: Vec<u8>) -> tokio::io::Result<ChunkLocation>;
    
    fn new() -> Self; // Args?
}

impl Device for Zoned {
    /// Hold internal state to keep track of zone state
    fn new() -> Self {
        unimplemented!()
    }
    fn append(&self, data: Vec<u8>) -> std::io::Result<ChunkLocation> {
        Ok(ChunkLocation::new(0, 0))
    }
}

impl Device for BlockInterface {
    /// Hold internal state to keep track of "ssd" zone state
    fn new() -> Self {
        unimplemented!()
    }
    
    fn append(&self, data: Vec<u8>) -> std::io::Result<ChunkLocation> {
        Ok(ChunkLocation::new(0, 0))
    }
}