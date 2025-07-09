use std::collections::HashMap;
use std::hash::Hash;
use std::sync::Arc;
use tokio::sync::{Notify, RwLock};
use crate::cache::bucket::{ChunkLocation, Chunk, ChunkState};
use crate::readerpool::ReaderPool;
use crate::writerpool::WriterPool;
use std::collections::hash_map::Entry;
use std::io::ErrorKind;

pub mod bucket;

#[derive(Debug)]
pub struct Cache {
    buckets: RwLock<HashMap<Chunk, Arc<RwLock<ChunkState>>>>,
}

impl Cache {
    pub fn new() -> Self {
        Self {
            buckets: RwLock::new(HashMap::new()),
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
        // NOTE: If we are ever clearing something from the map we need to acquire exclusive lock
        //       on both the entire map and the individual chunk location

        loop { // Bucket read locked -- no one can write to map 
            let bucket_guard = self.buckets.read().await;
            
            if let Some(state) = bucket_guard.get(&key) {

                // We now have the entry
                let bucket_state_guard = Arc::clone(state);
                let bucket_state_guard = bucket_state_guard.read().await;

                match &*bucket_state_guard {
                    ChunkState::Waiting(notify) => {
                        let notified = notify.notified(); // queue notifies
                        // Now we can drop full map lock, we have lock on chunkstate
                        drop(bucket_guard); // Writes can proceed on outer map

                        notified.await; // retry loop required
                        continue; // loop to recheck state
                    }
                    ChunkState::Ready(loc) => {
                        return reader(Arc::clone(loc)).await;
                    }
                };
            } else {
                break;
            }
        }

        loop {   // Bucket write locked -- no one can read or write to map
            let mut bucket_guard = self.buckets.write().await;

            // Incase it was inserted inbetween
            if let Some(state) = bucket_guard.get(&key) {
                
                // We now have the entry
                let bucket_state_guard = Arc::clone(state);
                let bucket_state_guard = bucket_state_guard.read().await;
                
                match &*bucket_state_guard {
                    ChunkState::Waiting(notify) => {
                        let notified = notify.notified(); // queue notifies
                        // Now we can drop full map lock, we have lock on chunkstate
                        drop(bucket_guard); // Writes can proceed on outer map

                        notified.await; // retry loop required
                        continue; // loop to recheck state
                    }
                    ChunkState::Ready(loc) => {
                        return reader(Arc::clone(loc)).await;
                    }
                };
            } else {
                // Otherwise we need to write, the entire map is still locked
                let locked_chunk_location: Arc<RwLock<ChunkState>> = Arc::new(RwLock::new(ChunkState::Waiting(Arc::new(Notify::new()))));
                let mut chunk_loc_guard = locked_chunk_location.write().await;
                // We now have something in the waiting state
                // It is locked and should not be unlocked until it's out of the waiting state
                bucket_guard.insert(key.clone(), Arc::clone(&locked_chunk_location)); // Place locked waiting state

                drop(bucket_guard); // Bucket write unlocked -- Other writes can proceed on the outer map
                let write_result = writer().await;
                match write_result {
                    Err(e) => {
                        let mut bucket_guard = self.buckets.write().await;
                        bucket_guard.remove(&key);
                        match &*chunk_loc_guard {
                            ChunkState::Waiting(notify) => {
                                notify.notify_waiters();
                            },
                            _ => {
                                // This should never happen here
                                panic!("Chunk was not in waiting state");
                            }
                        }
                        // This is the condition where you could be left with something in the waiting state
                        return Err(e);
                    },
                    Ok(location) => {
                        *chunk_loc_guard = ChunkState::Ready(Arc::new(location));
                    }
                }
                return Ok(())
            }
        }
    }   
}


