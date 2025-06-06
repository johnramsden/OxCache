use std::hash::Hash;
use std::sync::Arc;
use dashmap::DashMap;
use crate::cache::bucket::{ChunkLocation, SharedBucketState, Chunk};

pub mod bucket;

#[derive(Debug)]
pub struct Cache {
    buckets: DashMap<Chunk, Arc<SharedBucketState<ChunkLocation>>>,
}

impl Cache {
    pub fn new() -> Self {
        Self {
            buckets: DashMap::new(),
        }
    }
}