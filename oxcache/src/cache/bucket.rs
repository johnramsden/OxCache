use crate::request::GetRequest;
use nvme::types::{Byte, Zone};
use std::sync::{Arc, atomic::{AtomicUsize, Ordering}};
use tokio::sync::Notify;
use lru_mem::HeapSize;

#[derive(Debug, Clone)]
pub struct Chunk {
    pub uuid: String,
    pub offset: Byte,
    pub size: Byte,
}

// Only hash and compare based on uuid - offset/size are just read parameters
impl PartialEq for Chunk {
    fn eq(&self, other: &Self) -> bool {
        self.uuid == other.uuid
    }
}

impl Eq for Chunk {}

impl std::hash::Hash for Chunk {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.uuid.hash(state);
    }
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

impl HeapSize for ChunkLocation {
    fn heap_size(&self) -> usize {
        // ChunkLocation contains no heap-allocated data
        0
    }
}

// ValueSize is automatically implemented via blanket implementation

/// A ChunkLocation with pin counting for coordinating with eviction
#[derive(Debug)]
pub struct PinnedChunkLocation {
    pub location: ChunkLocation,
    pub pin_count: AtomicUsize,
    pub unpin_notify: Notify,
}

impl PinnedChunkLocation {
    pub fn new(location: ChunkLocation) -> Self {
        Self {
            location,
            pin_count: AtomicUsize::new(0),
            unpin_notify: Notify::new(),
        }
    }

    /// Pin this location (increment ref count). Returns a guard that will unpin on drop.
    pub fn pin(self: &Arc<Self>) -> PinGuard {
        let new_count = self.pin_count.fetch_add(1, Ordering::SeqCst) + 1;
        tracing::debug!("Pinning location {:?}, pin_count now: {}", self.location, new_count);
        PinGuard::new(self)
    }

    pub fn pin_count(&self) -> usize {
        self.pin_count.load(Ordering::SeqCst)
    }

    /// Check if this location can be evicted (pin count is 0)
    pub fn can_evict(&self) -> bool {
        self.pin_count() == 0
    }

    /// Wait until this location can be evicted (pin count reaches 0)
    pub async fn wait_for_unpin(&self) {
        while !self.can_evict() {
            let notified = self.unpin_notify.notified();
            if self.can_evict() {
                break;
            }
            notified.await;
        }
    }

    fn unpin(&self) {
        let old_count = self.pin_count.fetch_sub(1, Ordering::SeqCst);
        let new_count = old_count - 1;
        tracing::debug!("Unpinning location {:?}, pin_count now: {}", self.location, new_count);
        
        // If pin count reached 0, notify any waiting eviction threads
        if new_count == 0 {
            self.unpin_notify.notify_waiters();
        }
    }
}

/// Guard that unpins a ChunkLocation when dropped
#[derive(Debug)]
pub struct PinGuard {
    location: ChunkLocation,
    pinned_location: Arc<PinnedChunkLocation>,
}

impl PinGuard {
    fn new(pinned_location: &Arc<PinnedChunkLocation>) -> Self {
        Self { 
            location: pinned_location.location.clone(),
            pinned_location: Arc::clone(pinned_location),
        }
    }

    pub fn location(&self) -> &ChunkLocation {
        &self.location
    }
}

impl Drop for PinGuard {
    fn drop(&mut self) {
        self.pinned_location.unpin();
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
    Ready(Arc<PinnedChunkLocation>),
    Waiting(Arc<Notify>),
}
