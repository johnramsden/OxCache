use dashmap::DashMap;
use crate::cache::Cache;
use tokio::sync::{RwLock, Notify};
use std::sync::Arc;
use crate::request::GetRequest;

#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub struct Chunk {
    uuid: String,
    offset: usize,
    size: usize,
}

#[derive(Debug)]
pub struct ChunkLocation {
    zone: usize,
    addr: u64,
}

impl ChunkLocation {
    pub fn new(zone: usize, addr: u64) -> Self {
        Self { zone, addr }
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
pub enum BucketState<T> {
    Waiting(Arc<Notify>),
    Ready(Arc<T>),
}

#[derive(Debug)]
pub struct SharedBucketState<T> {
    pub state: RwLock<BucketState<T>>,
}
