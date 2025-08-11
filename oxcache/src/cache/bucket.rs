use crate::request::GetRequest;
use nvme::types::{Byte, Zone};
use std::sync::Arc;
use tokio::sync::Notify;

#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub struct Chunk {
    pub uuid: String,
    pub offset: Byte,
    pub size: Byte,
}

#[derive(Debug, PartialEq, Clone, Eq, Hash)]
pub struct ChunkLocation {
    pub zone: Zone,
    pub index: nvme::types::Chunk,
}

impl ChunkLocation {
    pub fn new(zone: Zone, index: nvme::types::Chunk) -> Self {
        Self { zone, index }
    }

    pub fn as_index(&self) -> [usize; 2] {
        [self.zone as usize, self.index as usize]
    }
}

impl Chunk {
    pub fn new(uuid: String, offset: Byte, size: Byte) -> Self {
        Self { uuid, offset, size }
    }
}

impl From<GetRequest> for Chunk {
    fn from(req: GetRequest) -> Self {
        Chunk {
            uuid: req.key,
            offset: req.offset,
            size: req.size,
        }
    }
}

#[derive(Debug)]
pub enum ChunkState {
    Ready(Arc<ChunkLocation>),
    Waiting(Arc<Notify>),
}
