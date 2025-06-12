use std::hash::Hash;
use std::sync::Arc;
use dashmap::{DashMap, Entry};
use tokio::sync::{Notify, RwLock};
use crate::cache::bucket::{ChunkLocation, SharedBucketState, Chunk, BucketState};
use crate::readerpool::ReaderPool;
use crate::writerpool::WriterPool;

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
    
    // TODO: Confusing, locking, waiting, async locking, PROBABLY REDO COMPLETELY
    // pub async fn get<PresentF, VacantF, Fut>(&self, chunk: Chunk, present: PresentF, vacant: VacantF) -> tokio::io::Result<Vec<u8>>
    // where
    //     PresentF: Fn(Arc<SharedBucketState<ChunkLocation>>) -> Fut + Send + 'static,
    //     VacantF: Fn(Arc<SharedBucketState<ChunkLocation>>) -> Fut + Send + 'static,
    //     Fut: Future<Output = tokio::io::Result<Vec<u8>>> + Send + 'static {
    //     match self.buckets.entry(chunk) { // Implicit write lock on shard
    //         Entry::Occupied(entry) => {
    //             let shared = Arc::clone(entry.get());
    // 
    //             // Use a read lock, allows multiple concurrent reads
    //             let state_guard = shared.state.read().await;
    // 
    //             match &*state_guard {
    //                 BucketState::Ready(_) => {
    //                     // Release read lock before awaiting
    //                     drop(state_guard);
    //                     present(Arc::clone(&shared)).await
    //                 },
    //                 BucketState::Waiting(notify) => {
    //                     // Release read lock before awaiting
    //                     let notified = notify.notified();
    //                     drop(state_guard);
    //                     notified.await;
    //                     // EVICTED???
    //                     present(Arc::clone(&shared)).await
    //                 }
    //             }
    //         }
    //         Entry::Vacant(entry) => {
    //             let notify = Notify::new();
    //             let shared = Arc::new(SharedBucketState {
    //                 state: RwLock::new(BucketState::Waiting(notify)),
    //             });
    //             entry.insert(Arc::clone(&shared));
    //             // notify...
    //         }
    //     }
    // }
}