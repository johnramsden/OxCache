use std::fs::OpenOptions;
use std::os::fd::{AsRawFd, RawFd};
use std::sync::Arc;
use crate::cache::bucket::ChunkLocation;
use crate::device;

pub struct Zoned {
    fd: RawFd,
    timeout: u32,
    logical_block_size: usize,
    nsid: u32, // Can we set once?
    zone_size: u64,
}

pub struct BlockInterface {
    fd: RawFd,
}

pub trait Device: Send + Sync {
    fn append(&self, data: Vec<u8>) -> std::io::Result<ChunkLocation>;
    
    fn read(&self, location: ChunkLocation) -> std::io::Result<Vec<u8>>;
    
    fn new(device: &str) -> std::io::Result<Self> where Self: Sized; // Args
}
pub fn get_device(device: &str) -> std::io::Result<Arc<dyn Device>> {
    // TODO: If dev type Zoned..., else
    Ok(Arc::new(device::BlockInterface::new(device)?))
}

impl Device for Zoned {
    /// Hold internal state to keep track of zone state
    fn new(device: &str) -> std::io::Result<Self> {
        unimplemented!();
        // TODO: PANICS
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
    
    fn read(&self, location: ChunkLocation) -> std::io::Result<Vec<u8>> {
        Ok(Vec::new())
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

    fn read(&self, location: ChunkLocation) -> std::io::Result<Vec<u8>> {
        Ok(Vec::new())
    }
}