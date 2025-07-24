use crate::request::GetRequest;
use std::sync::Arc;
use tokio::sync::Notify;

#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub struct Chunk {
    pub uuid: String,
    pub offset: usize,
    pub size: usize,
}

#[derive(Debug, PartialEq, Clone, Eq, Hash)]
pub struct ChunkLocation {
    pub zone: usize,
    pub index: u64,
}

impl ChunkLocation {
    pub fn new(zone: usize, index: u64) -> Self {
        Self { zone, index }
    }

    pub fn as_index(&self) -> [usize; 2] {
        [self.zone, self.index as usize]
    }
}

impl Chunk {
    pub fn new(uuid: String, offset: usize, size: usize) -> Self {
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
