use std::os::fd::RawFd;

struct Zoned {
    fd: RawFd,
    timeout: u32,
    logical_block_size: usize,
    nsid: u32, // Can we set once?
    zone_size: u64,
}

struct BlockInterface {}

trait Device {
    fn append(data: Vec<u8>, zone: u64) -> tokio::io::Result<u64>;
}

impl Device for Zoned {
    fn append(data: Vec<u8>, zone: u64) -> std::io::Result<u64> {
        Ok(0)
    }
}

impl Device for BlockInterface {
    fn append(data: Vec<u8>, zone: u64) -> std::io::Result<u64> {
        Ok(0)
    }
}