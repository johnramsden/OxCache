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

        {   // Bucket read locked
            let bucket_guard = self.buckets.read().await;
            if let Some(state) = bucket_guard.get(&key) {
                let bucket_state_guard = Arc::clone(state);
                let bucket_state_guard = bucket_state_guard.read().await;
                // Now we can drop full map lock, we have lock on bucketstate
                drop(bucket_guard); // Other reads can proceed on outer map
                if let ChunkState::Ready(state) = &*bucket_state_guard {
                    reader(Arc::clone(state)).await;
                    return Ok(())
                }
                // Waiting State should not occur, this means something couldn't be
                // successfully written to the map and it was inserted and then deleted
                // (it should no longer be present now).
                // Need to acquire exclusive lock because state must have changed, proceed
            }
        }

        {   // Bucket write locked
            let mut bucket_guard = self.buckets.write().await;

            // Incase it was inserted inbetween
            if let Some(state) = bucket_guard.get(&key) {
                let bucket_state_guard = Arc::clone(state);
                let bucket_state_guard = bucket_state_guard.read().await;
                // Now we can drop full map lock, we have lock on bucketstate
                drop(bucket_guard); // Other reads can proceed on the outer map
                if let ChunkState::Ready(state) = &*bucket_state_guard {
                    reader(Arc::clone(state)).await;
                    return Ok(())
                }
                // Waiting State should not occur, this means something couldn't be
                // successfully written to the map and it was inserted and then deleted
                // (it should no longer be present now). We had An exclusive lock so clearly something went wrong
                panic!("Invalid bucket state");
            }

            // Otherwise we need to write
            let locked_chunk_location: Arc<RwLock<ChunkState>> = Arc::new(RwLock::new(ChunkState::Waiting));
            let mut chunk_loc_guard = locked_chunk_location.write().await;
            bucket_guard.insert(key.clone(), Arc::clone(&locked_chunk_location));
            // Now we can drop bucket lock, but chunklocation remains locked
            drop(bucket_guard);
            let write_result = writer().await;
            match write_result { 
                Err(e) => {
                    let mut bucket_guard = self.buckets.write().await;
                    bucket_guard.remove(&key);
                    return Err(e);
                },
                Ok(location) => {
                    *chunk_loc_guard = ChunkState::Ready(Arc::new(location));
                }
            }
            Ok(())
        }
    }
}


