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

    /// Returns the current `SharedBucketState` for the given `chunk`, or inserts a new `Waiting` entry if not present.
    ///
    /// Returns:
    /// - Arc to the shared state
    /// - `true` if the caller inserted it and is responsible for producing the value (the "filler")
    pub fn get_or_insert_waiting(&self, chunk: Chunk) -> (Arc<SharedBucketState<ChunkLocation>>, bool) {
        match self.buckets.entry(chunk) {
            // If already present, clone and return it.
            Entry::Occupied(e) => (Arc::clone(e.get()), false),

            // If absent, insert a `Waiting` state.
            Entry::Vacant(e) => {
                // Each waiting entry starts with a Notify for coordination.
                let shared = Arc::new(SharedBucketState {
                    state: RwLock::new(BucketState::Waiting(Arc::new(Notify::new()))),
                });

                // Insert it into the map.
                e.insert(Arc::clone(&shared));

                // Return the inserted state and flag that we're the filler.
                (shared, true)
            }
        }
    }

    /// Handles either filling the value or waiting for it to become ready, based on `is_filler`.
    ///
    /// This avoids holding any locks across `.await`, reducing deadlock and race risks.
    /// fill function executes required work to get ChunkLocation
    pub async fn wait_or_fill<F>(
        shared: Arc<SharedBucketState<ChunkLocation>>,
        is_filler: bool,
        fill: F,
    ) -> tokio::io::Result<Arc<ChunkLocation>>
    where
        F: FnOnce() -> tokio::io::Result<ChunkLocation>,
    {
        if is_filler { // Fill map
            let result = fill();

            match result {
                Ok(loc) => {
                    let arc_loc = Arc::new(loc);

                    // Acquire write lock to change from Waiting -> Ready
                    let mut guard = shared.state.write().await;

                    // Replace the state safely. Drop the old one to notify waiters.
                    if let BucketState::Waiting(notify) = std::mem::replace(&mut *guard, BucketState::Ready(arc_loc.clone())) {
                        // Notify everyone who was waiting on this chunk
                        notify.notify_waiters();
                    }

                    Ok(arc_loc)
                }

                Err(e) => {
                    // TODO:
                    //   Could remove from map here to prevent stale waiting state
                    // Return the error to caller
                    Err(e)
                }
            }
        } else {
            // Waiter
            loop {
                let state_guard = shared.state.read().await;

                match &*state_guard {
                    BucketState::Ready(loc) => {
                        return Ok(Arc::clone(loc));
                    }
                    BucketState::Waiting(notify_arc) => {
                        let notify = Arc::clone(notify_arc);
                        drop(state_guard);

                        // Await without holding any lock
                        notify.notified().await;
                    }
                }
            }
        }
    }

}