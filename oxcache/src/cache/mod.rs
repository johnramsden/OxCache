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

    pub async fn get_or_insert_with<R, W, RFut, WFut>(
        &self,
        key: Chunk,
        reader: R,
        writer: W,
    ) -> tokio::io::Result<()>
    where
        R: FnOnce(Arc<ChunkLocation>) -> RFut + Send + 'static,
        RFut: Future<Output = tokio::io::Result<()>> + Send + 'static,
        W: FnOnce() -> WFut + Send + 'static,
        WFut: Future<Output = tokio::io::Result<ChunkLocation>> + Send + 'static
    {
        match self.buckets.entry(key.clone()) {
            Entry::Occupied(e) => {
                let shared = Arc::clone(e.get());

                loop {
                    let state = shared.state.read().await;

                    match &*state {
                        BucketState::Ready(inner) => {
                            reader(Arc::clone(inner)).await?;
                            return Ok(());
                        }
                        BucketState::Waiting(notify) => {
                            let notify = Arc::clone(notify);
                            drop(state);
                            notify.notified().await;
                        }
                    }
                }
            }

            Entry::Vacant(e) => {
                let notify = Arc::new(Notify::new());
                let shared = Arc::new(SharedBucketState {
                    state: RwLock::new(BucketState::Waiting(notify.clone())),
                });

                e.insert(Arc::clone(&shared));

                let result = writer().await;

                let mut state = shared.state.write().await;
                match result {
                    Ok(data) => {
                        let data = Arc::new(data);
                        *state = BucketState::Ready(Arc::clone(&data));
                        notify.notify_waiters();
                        
                        Ok(())
                    }
                    Err(e) => {
                        self.buckets.remove(&key); // cleanup
                        Err(e)
                    }
                }
            }
        }
    }


}