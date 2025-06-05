use dashmap::DashMap;
use crate::cache::Cache;

#[derive(Debug, Clone, Eq, PartialEq, Hash)]
struct Chunk {
    uuid: String,
    offset: usize,
    size: usize,
}

impl Chunk {
    pub fn new(uuid: String, offset: usize, size: usize) -> Self {
        Self {
            uuid, offset, size
        }
    }
}

#[derive(Debug)]
pub struct Bucket {
    state: DashMap<String, String>,
}

impl Bucket {
    pub fn new() -> Self {
        Self {
            state: DashMap::new(),
        }
    }
}
