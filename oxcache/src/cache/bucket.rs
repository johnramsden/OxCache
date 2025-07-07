use dashmap::DashMap;
use crate::cache::Cache;
use tokio::sync::{RwLock, Notify};
use std::sync::Arc;
use crate::request::GetRequest;

#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub struct Chunk {
    pub uuid: String,
    pub offset: usize,
    pub size: usize,
}

#[derive(Debug)]
#[derive(Clone)]
pub struct ChunkLocation {
    pub zone: usize,
    pub index: u64, // The chunk index
}

impl ChunkLocation {
    pub fn new(zone: usize, addr: u64) -> Self {
        Self { zone, index: addr }
    }
}

impl Chunk {
    pub fn new(uuid: String, offset: usize, size: usize) -> Self {
        Self {
            uuid,
            offset,
            size
        }
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
