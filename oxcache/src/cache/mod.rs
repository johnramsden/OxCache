use std::collections::HashMap;
use std::sync::Arc;
use futures::lock::Mutex;
use ndarray::{s, Array2, ArrayBase};
use tokio::sync::{Notify, RwLock};
use crate::cache::bucket::{ChunkLocation, Chunk, ChunkState};
use std::io::ErrorKind;

pub mod bucket;

// An entry 
type EntryType = Arc<RwLock<ChunkState>>;
fn new_entry() -> EntryType {
    Arc::new(RwLock::new(ChunkState::Waiting(Arc::new(Notify::new()))))
}

#[derive(Debug)]
pub struct Cache {
    buckets: RwLock<HashMap<Chunk, EntryType>>,
    zone_to_entry: Mutex<Array2<Option<Chunk>>>
}

impl Cache {
    pub fn new(num_zones: usize, chunks_per_zone: usize) -> Self {
        Self {
            buckets: RwLock::new(HashMap::new()),
            // zone_to_entry: Mutex::new(vec![vec![; chunks_per_zone]; num_zones])
            zone_to_entry: Mutex::new(ArrayBase::from_elem((num_zones, chunks_per_zone), Option::None))
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
                let locked_chunk_location = new_entry();
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
                        *chunk_loc_guard = ChunkState::Ready(Arc::new(location.clone()));
                        let mut reverse_mapping_guard = self.zone_to_entry.lock().await;
                        reverse_mapping_guard[location.as_index()] = Some(key);
                    }
                }
                return Ok(())
            }
        }
    }
 
    ///

    /// Remove zones from the map
    pub async fn remove_zones(&self, zone_indices: &[usize]) -> tokio::io::Result<()> {
        let mut map_guard = self.buckets.write().await;
        let mut guard = self.zone_to_entry.lock().await;
        for zone_index in zone_indices {
            guard.slice(s![*zone_index, ..])
                .iter()
                .filter_map(|chunk| chunk.clone()) // Remove None instances
                .for_each(|chunk| {
                    map_guard.remove(&chunk);
                });

            guard.slice_mut(s![*zone_index, ..]).map_inplace(|v| {
                *v = None;
            });
        }

        Ok(())
    }

    /// Updates entries 
    pub async fn update_entries<R, W, RFut, WFut>(&self, to_relocate: &[ChunkLocation], writer: W) {
        // TODO: to_relocate is a list of ChunkLocations that the caller wants to update
        // We pass in each chunk location and the writer should return back with the 
    }

    /// Modify entries
    /// Returns an error if the entry doesn't exist
    pub async fn modify_entry<R, W, RFut, WFut>(
        &self,
        key: Chunk,
        writer: W,
    ) -> tokio::io::Result<()>
    where
        W: FnOnce() -> WFut + Send + 'static,
        WFut: Future<Output = tokio::io::Result<ChunkLocation>> + Send + 'static {
            let mut bucket_guard = self.buckets.write().await;
            let state = bucket_guard.get(&key).ok_or(std::io::Error::new(ErrorKind::NotFound, "Couldn't find entry"))?;

            // We now have the entry
            let bucket_state_guard = Arc::clone(state);
            let bucket_state_guard = bucket_state_guard.write().await;

            match &*bucket_state_guard {
                ChunkState::Waiting(_) => {
                    drop(bucket_guard);
                    Err(std::io::Error::new(ErrorKind::Other, "Another thread is already modifying the entry"))
                }
                ChunkState::Ready(_) => {
                    let locked_chunk_location = new_entry();
                    let mut chunk_loc_guard = locked_chunk_location.write().await;
                    bucket_guard.insert(key.clone(), Arc::clone(&locked_chunk_location));     
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
                            Err(e)
                        },
                        Ok(location) => {
                            *chunk_loc_guard = ChunkState::Ready(Arc::new(location.clone()));
                            let mut reverse_mapping_guard = self.zone_to_entry.lock().await;
                            reverse_mapping_guard[location.as_index()] = Some(key);
                            Ok(())
                        }
                    }
                }
            }
        }
}

#[cfg(test)]
mod mod_tests {

    use std::sync::Arc;

    use crate::cache::{bucket::{Chunk, ChunkLocation}, Cache};

    #[tokio::test]
    async fn test_insert() {
        let cache = Cache::new(10, 100);
        match cache.get_or_insert_with(Chunk::new(String::from("fake-uuid"), 120, 10),
            |_| async move {
                assert!(false, "Shouldn't reach here");
                Ok(())
            },
            || async move {
                Ok(ChunkLocation::new(0, 20))
            }
        ).await {
            Ok(()) => (),
            Err(err) => assert!(false, "Error occurred: {err}"),
        }

        match cache.get_or_insert_with(Chunk::new(String::from("fake-uuid"), 120, 10),
            |loc| async move {
                assert!(loc.zone == 0);
                assert!(loc.index == 20);
                Ok(())
            },
            || async move {
                assert!(false, "Shouldn't reach here");
                Ok(ChunkLocation::new(0, 0))
            }
        ).await {
            Ok(()) => (),
            Err(err) => assert!(false, "Error occurred: {err}"),
        }
    }

    #[tokio::test]
    async fn test_insert_similar() {
        let cache = Cache::new(10, 100);

        let fail_path = async |_: Arc<ChunkLocation>| {
            assert!(false, "Shouldn't reach here");
            Ok(())
        };

        let fail_write_path = async || -> tokio::io::Result<ChunkLocation> {
            assert!(false, "Shouldn't reach here");
            Ok(ChunkLocation { zone: 0, index: 0 })
        };

        let check_err = |result| {
            match result {
                Ok(()) => (),
                Err(err) => assert!(false, "Error occurred: {err}"),
            }
        };

        // Insert multiple similar entries
        let entry1 = Chunk::new(String::from("fake-uuid"), 120, 10);
        let entry2 = Chunk::new(String::from("fake-uuid!"), 120, 10);
        let entry3 = Chunk::new(String::from("fake-uuid"), 121, 10);
        let entry4 = Chunk::new(String::from("fake-uuid"), 121, 11);

        let chunk_loc1 = ChunkLocation::new(0, 20);
        let chunk_loc2 = ChunkLocation::new(1, 22);
        let chunk_loc3 = ChunkLocation::new(9, 25);
        let chunk_loc4 = ChunkLocation::new(7, 29);

        check_err(cache.get_or_insert_with(entry1.clone(),
            fail_path,
            {
                let chunk_loc1 = chunk_loc1.clone();
                || async { Ok(chunk_loc1) }
            }).await);

        check_err(cache.get_or_insert_with(entry2.clone(),
            fail_path,
            {
                let chunk_loc2 = chunk_loc2.clone();
                || async { Ok(chunk_loc2) }
            }).await);

        check_err(cache.get_or_insert_with(entry3.clone(),
            fail_path,
            {
                let chunk_loc3 = chunk_loc3.clone();
                || async { Ok(chunk_loc3) }
            }).await);

        check_err(cache.get_or_insert_with(entry4.clone(),
            fail_path,
            {
                let chunk_loc4 = chunk_loc4.clone();
                || async { Ok(chunk_loc4) }
            }).await);

        check_err(cache.get_or_insert_with(entry1,
            {
                |cl| async move {
                    let cl = Arc::clone(&cl);
                    assert_eq!(*cl, chunk_loc1);
                    Ok(())
                }
            },
            fail_write_path
        ).await);

        check_err(cache.get_or_insert_with(entry2,
            {
                |cl| async move {
                    let cl = Arc::clone(&cl);
                    assert_eq!(*cl, chunk_loc2);
                    Ok(())
                }
            },
            fail_write_path
        ).await);

        check_err(cache.get_or_insert_with(entry3,
            {
                |cl| async move {
                    let cl = Arc::clone(&cl);
                    assert_eq!(*cl, chunk_loc3);
                    Ok(())
                }
            },
            fail_write_path
        ).await);

        check_err(cache.get_or_insert_with(entry4,
            {
                |cl| async move {
                    let cl = Arc::clone(&cl);
                    assert_eq!(*cl, chunk_loc4);
                    Ok(())
                }
            },
            fail_write_path
        ).await);
    }

    #[tokio::test]
    async fn test_remove() {
        let cache = Cache::new(10, 100);

        let fail_path = async |_: Arc<ChunkLocation>| {
            assert!(false, "Shouldn't reach here");
            Ok(())
        };

        let fail_write_path = async || -> tokio::io::Result<ChunkLocation> {
            assert!(false, "Shouldn't reach here");
            Ok(ChunkLocation { zone: 0, index: 0 })
        };

        let check_err = |result| {
            match result {
                Ok(()) => (),
                Err(err) => assert!(false, "Error occurred: {err}"),
            }
        };

        let entry = Chunk::new(String::from("fake-uuid"), 120, 10);
        let chunk_loc = ChunkLocation::new(0, 20);

        // Insert
        check_err(cache.get_or_insert_with(entry.clone(),
            fail_path,
            {
                let chunk_loc = chunk_loc.clone();
                || async { Ok(chunk_loc) }
            }).await);

        // Check insertion
        check_err(cache.get_or_insert_with(entry.clone(),
            {
                let chunk_loc = chunk_loc.clone();
                |cl| async move {
                    let cl = Arc::clone(&cl);
                    assert_eq!(*cl, chunk_loc);
                    Ok(())
                }
            },
            fail_write_path
        ).await);

        // Remove
        check_err(cache.remove_zones(&[0]).await);

        // Check insertion is removed
        check_err(cache.get_or_insert_with(entry.clone(),
            fail_path,
            {
                let chunk_loc = chunk_loc.clone();
                || async { Ok(chunk_loc) }
            }).await);

    }

    #[tokio::test]
    async fn test_multiple_remove() {
        let cache = Cache::new(10, 100);

        let fail_path = async |_: Arc<ChunkLocation>| {
            assert!(false, "Shouldn't reach here");
            Ok(())
        };

        let fail_write_path = async || -> tokio::io::Result<ChunkLocation> {
            assert!(false, "Shouldn't reach here");
            Ok(ChunkLocation { zone: 0, index: 0 })
        };

        let check_err = |result| {
            match result {
                Ok(()) => (),
                Err(err) => assert!(false, "Error occurred: {err}"),
            }
        };

        let entry = Chunk::new(String::from("fake-uuid"), 120, 10);
        let chunk_loc = ChunkLocation::new(0, 20);

        let entry2 = Chunk::new(String::from("fake-uuid"), 121, 10);
        let chunk_loc2 = ChunkLocation::new(0, 21);

        // Insert
        check_err(cache.get_or_insert_with(entry.clone(),
            fail_path,
            {
                let chunk_loc = chunk_loc.clone();
                || async { Ok(chunk_loc) }
            }).await);

        check_err(cache.get_or_insert_with(entry2.clone(),
            fail_path,
            {
                let chunk_loc = chunk_loc2.clone();
                || async { Ok(chunk_loc) }
            }).await);

        // Check insertion
        check_err(cache.get_or_insert_with(entry.clone(),
            {
                let chunk_loc = chunk_loc.clone();
                |cl| async move {
                    let cl = Arc::clone(&cl);
                    assert_eq!(*cl, chunk_loc);
                    Ok(())
                }
            },
            fail_write_path
        ).await);

        check_err(cache.get_or_insert_with(entry2.clone(),
            {
                let chunk_loc = chunk_loc2.clone();
                |cl| async move {
                    let cl = Arc::clone(&cl);
                    assert_eq!(*cl, chunk_loc);
                    Ok(())
                }
            },
            fail_write_path
        ).await);

        // Remove
        check_err(cache.remove_zones(&[0]).await);

        // Check insertion is removed
        check_err(cache.get_or_insert_with(entry.clone(),
            fail_path,
            {
                let chunk_loc = chunk_loc.clone();
                || async { Ok(chunk_loc) }
            }).await);

        check_err(cache.get_or_insert_with(entry2.clone(),
            fail_path,
            {
                let chunk_loc = chunk_loc2.clone();
                || async { Ok(chunk_loc) }
            }).await);

    }
}
