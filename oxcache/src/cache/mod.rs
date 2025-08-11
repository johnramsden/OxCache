use crate::cache::bucket::{Chunk, ChunkLocation, ChunkState};
use ndarray::{Array2, ArrayBase, s};
use nvme::types::{self, Zone};
use std::io::ErrorKind;
use std::iter::zip;
use std::sync::Arc;
use std::{collections::HashMap, io};
use tokio::sync::{Notify, RwLock};

pub mod bucket;

// An entry
type EntryType = Arc<RwLock<ChunkState>>;
fn new_entry() -> EntryType {
    Arc::new(RwLock::new(ChunkState::Waiting(Arc::new(Notify::new()))))
}

// Entry not found
fn entry_not_found() -> std::io::Error {
    std::io::Error::new(ErrorKind::NotFound, "Couldn't find entry")
}

#[derive(Debug)]
struct BucketMap {
    buckets: HashMap<Chunk, EntryType>,
    zone_to_entry: Array2<Option<Chunk>>,
}

#[derive(Debug)]
pub struct Cache {
    // Make sure to lock buckets before locking zone_to_entry, to avoid deadlock errors
    bm: RwLock<BucketMap>,
}

impl Cache {
    pub fn new(num_zones: Zone, chunks_per_zone: types::Chunk) -> Self {
        Self {
            bm: RwLock::new(BucketMap {
                buckets: HashMap::new(),
                zone_to_entry: ArrayBase::from_elem(
                    (num_zones as usize, chunks_per_zone as usize),
                    None,
                ),
            }),
        }
    }

    // If we're locking the entry, do we still need a notify function? Why not just await on the lock?
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
        WFut: Future<Output = tokio::io::Result<ChunkLocation>> + Send + 'static,
    {
        // NOTE: If we are ever clearing something from the map we need to acquire exclusive lock
        //       on both the entire map and the individual chunk location

        loop {
            // Bucket read locked -- no one can write to map
            let bucket_guard = self.bm.read().await;

            if let Some(state) = bucket_guard.buckets.get(&key) {
                let state = Arc::clone(state);
                drop(bucket_guard);
                // We now have the entry
                let bucket_state_guard = state;
                let bucket_state_guard = bucket_state_guard.read().await;

                match &*bucket_state_guard {
                    ChunkState::Waiting(notify) => {
                        let notify = notify.clone();
                        let notified = notify.notified(); // queue notifies
                        // Now we can drop full map lock, we have lock on chunkstate

                        drop(bucket_state_guard);

                        notified.await; // retry loop required
                        continue; // loop to recheck state
                    }
                    ChunkState::Ready(loc) => {
                        let r = reader(Arc::clone(loc)).await;
                        return r;
                    }
                };
            } else {
                break;
            }
        }

        loop {
            // Bucket write locked -- no one can read or write to map
            let mut bucket_guard = self.bm.write().await;

            // Incase it was inserted inbetween
            if let Some(state) = bucket_guard.buckets.get(&key) {
                let state = Arc::clone(state);
                drop(bucket_guard);
                // We now have the entry
                let bucket_state_guard = state;
                let bucket_state_guard = bucket_state_guard.read().await;

                match &*bucket_state_guard {
                    ChunkState::Waiting(notify) => {
                        let notify = notify.clone();
                        let notified = notify.notified(); // queue notifies
                        // Now we can drop full map lock, we have lock on chunkstate
                        drop(bucket_state_guard);

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
                bucket_guard
                    .buckets
                    .insert(key.clone(), Arc::clone(&locked_chunk_location)); // Place locked waiting state
                drop(bucket_guard); // Bucket write unlocked -- Other writes can proceed on the outer map
                let write_result = writer().await;
                match write_result {
                    Err(e) => {
                        // extract notify and drop chunk_loc_guard BEFORE acquiring buckets.write()
                        let notify = match &*chunk_loc_guard {
                            ChunkState::Waiting(n) => Some(n.clone()),
                            _ => panic!("Chunk was not in waiting state"),
                        };
                        drop(chunk_loc_guard); // Prevent deadlock

                        let mut bucket_guard = self.bm.write().await;
                        bucket_guard.buckets.remove(&key);

                        if let Some(n) = notify {
                            n.notify_waiters();
                        }

                        return Err(e);
                    }
                    Ok(location) => {
                        *chunk_loc_guard = ChunkState::Ready(Arc::new(location.clone()));
                        drop(chunk_loc_guard);

                        let mut reverse_mapping_guard = self.bm.write().await;
                        reverse_mapping_guard.zone_to_entry[location.as_index()] = Some(key);
                    }
                }

                return Ok(());
            }
        }
    }

    /// Remove zones from the map
    ///
    /// Internal use: won't remove them from the map if they don't
    /// exist in the reverse mapping
    pub async fn remove_zones(&self, zone_indices: &[Zone]) -> tokio::io::Result<()> {
        let mut map_guard = self.bm.write().await;

        // Loop over zones
        for zone_index in zone_indices {
            // Get slice representing a zone
            let zone_slice = s![*zone_index as usize, ..];
            // loop over zone chunks
            let zone_view = map_guard.zone_to_entry.slice(zone_slice);

            // Collect all valid chunks first
            let chunks_to_remove: Vec<_> = zone_view
                .iter()
                .filter_map(|opt_chunk| opt_chunk.as_ref())
                .cloned()
                .collect();

            // Now safely mutate map_guard and reverse_mapping
            for chunk in chunks_to_remove {
                let entry = match map_guard.buckets.remove(&chunk) {
                    Some(v) => v,
                    None => {
                        return Err(io::Error::new(
                            io::ErrorKind::NotFound,
                            format!("Couldn't find entry while removing zones: {:?}", chunk),
                        ));
                    }
                };
                // Get the write lock so that we can be sure that no one is reading this chunk anymore
                let _ = entry.write().await;
            }

            // Now safe to mutate reverse_mapping
            map_guard
                .zone_to_entry
                .slice_mut(zone_slice)
                .map_inplace(|v| *v = None);
        }

        Ok(())
    }

    /// Updates entries, should only be called by the evictor. Writer
    /// should return a list of new locations on disk
    pub async fn remove_zones_and_update_entries<W>(
        &self,
        zones_to_reset: &[Zone],
        to_relocate: &[ChunkLocation],
        writer: W,
    ) -> tokio::io::Result<()>
    where
        W: FnOnce() -> tokio::io::Result<Vec<ChunkLocation>>,
    {
        // to_relocate is a list of ChunkLocations that the caller wants to update
        // We pass in each chunk location and the writer function should return back with the list of updated chunk locations
        let mut bucket_guard = self.bm.write().await;
        let mut entry_lock_list = Vec::new();

        // Remove to_relocate elements from the reverse_mapping, and
        // change their entries to be in the waiting state.
        for location in to_relocate {
            let chunk_id = match bucket_guard.zone_to_entry[location.as_index()].clone() {
                Some(id) => {
                    bucket_guard.zone_to_entry[location.as_index()].take();
                    id
                }
                None => return Err(io::Error::new(ErrorKind::NotFound, "Couldn't find chunk")),
            };

            let state_guard = bucket_guard
                .buckets
                .get(&chunk_id)
                .ok_or(entry_not_found())?
                .clone();
            let state_guard = state_guard.write().await;
            match &*state_guard {
                ChunkState::Ready(loc) => {
                    assert!(**loc == *location);
                    let entry = new_entry();
                    bucket_guard.buckets.insert(chunk_id.clone(), entry.clone());
                    entry_lock_list.push(entry);
                }
                ChunkState::Waiting(_notify) => {
                    panic!("Error state")
                }
            }
        }

        drop(bucket_guard);

        // Taking advantage of an implementation detail, this will
        // remove the entries in the zone without touching the
        // relocated chunks, because it's already been removed from
        // the reverse mapping
        self.remove_zones(zones_to_reset).await?;

        let write_result = writer();
        match write_result {
            Err(e) => {
                // TODO
                return Err(e);
            }
            Ok(new_locations) => {
                for (entry_lock, new_location) in zip(entry_lock_list, new_locations) {
                    let mut entry_guard = entry_lock.write().await;
                    let notif = match &*entry_guard {
                        ChunkState::Ready(_chunk_location) => panic!("Wrong state"),
                        ChunkState::Waiting(notify) => notify.clone(),
                    };
                    *entry_guard = ChunkState::Ready(Arc::new(new_location));
                    notif.notify_waiters();
                }
            }
        }

        Ok(())
    }

    pub async fn remove_entry(&self, chunk: &ChunkLocation) -> tokio::io::Result<()> {
        // to_relocate is a list of ChunkLocations that the caller wants to update
        // We pass in each chunk location and the writer function should return back with the list of updated chunk locations
        let mut bucket_guard = self.bm.write().await;

        let chunk_id = match bucket_guard.zone_to_entry[chunk.as_index()].clone() {
            Some(id) => {
                bucket_guard.zone_to_entry[chunk.as_index()].take();
                id
            }
            None => return Err(io::Error::new(ErrorKind::NotFound, "Couldn't find chunk")),
        };

        bucket_guard
            .buckets
            .remove(&chunk_id)
            .ok_or(entry_not_found())?;
        Ok(())
    }

    pub async fn remove_entries(&self, chunks: &[ChunkLocation]) -> tokio::io::Result<()> {
        // to_relocate is a list of ChunkLocations that the caller wants to update
        // We pass in each chunk location and the writer function should return back with the list of updated chunk locations
        let mut bucket_guard = self.bm.write().await;

        for chunk in chunks {
            let chunk_id = match bucket_guard.zone_to_entry[chunk.as_index()].clone() {
                Some(id) => {
                    bucket_guard.zone_to_entry[chunk.as_index()].take();
                    id
                }
                None => return Err(io::Error::new(ErrorKind::NotFound, "Couldn't find chunk")),
            };

            bucket_guard
                .buckets
                .remove(&chunk_id)
                .ok_or(entry_not_found())?;
        }

        Ok(())
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
        WFut: Future<Output = tokio::io::Result<ChunkLocation>> + Send + 'static,
    {
        let mut bucket_guard = self.bm.write().await;
        let state = bucket_guard.buckets.get(&key).ok_or(std::io::Error::new(
            ErrorKind::NotFound,
            format!("Couldn't find entry {:?}", key),
        ))?;

        // We now have the entry
        let bucket_state_guard = Arc::clone(state);
        let bucket_state_guard = bucket_state_guard.write().await;

        match &*bucket_state_guard {
            ChunkState::Waiting(_) => {
                drop(bucket_guard);
                Err(std::io::Error::new(
                    ErrorKind::Other,
                    "Another thread is already modifying the entry",
                ))
            }
            ChunkState::Ready(_) => {
                let locked_chunk_location = new_entry();
                let mut chunk_loc_guard = locked_chunk_location.write().await;
                bucket_guard
                    .buckets
                    .insert(key.clone(), Arc::clone(&locked_chunk_location));
                drop(bucket_guard); // Bucket write unlocked -- Other writes can proceed on the outer map

                let write_result = writer().await;
                match write_result {
                    Err(e) => {
                        let mut bucket_guard = self.bm.write().await;
                        bucket_guard.buckets.remove(&key);
                        match &*chunk_loc_guard {
                            ChunkState::Waiting(notify) => {
                                notify.notify_waiters();
                            }
                            _ => {
                                // This should never happen here
                                panic!("Chunk was not in waiting state");
                            }
                        }
                        // This is the condition where you could be left with something in the waiting state
                        Err(e)
                    }
                    Ok(location) => {
                        *chunk_loc_guard = ChunkState::Ready(Arc::new(location.clone()));
                        let mut reverse_mapping_guard = self.bm.write().await;
                        reverse_mapping_guard.zone_to_entry[location.as_index()] = Some(key);
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

    use crate::cache::{
        Cache,
        bucket::{Chunk, ChunkLocation},
    };

    #[tokio::test]
    async fn test_insert() {
        let cache = Cache::new(10, 100);
        match cache
            .get_or_insert_with(
                Chunk::new(String::from("fake-uuid"), 120, 10),
                |_| async move {
                    assert!(false, "Shouldn't reach here");
                    Ok(())
                },
                || async move { Ok(ChunkLocation::new(0, 20)) },
            )
            .await
        {
            Ok(()) => (),
            Err(err) => assert!(false, "Error occurred: {err}"),
        }

        match cache
            .get_or_insert_with(
                Chunk::new(String::from("fake-uuid"), 120, 10),
                |loc| async move {
                    assert!(loc.zone == 0);
                    assert!(loc.index == 20);
                    Ok(())
                },
                || async move {
                    assert!(false, "Shouldn't reach here");
                    Ok(ChunkLocation::new(0, 0))
                },
            )
            .await
        {
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

        let check_err = |result| match result {
            Ok(()) => (),
            Err(err) => assert!(false, "Error occurred: {err}"),
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

        check_err(
            cache
                .get_or_insert_with(entry1.clone(), fail_path, {
                    let chunk_loc1 = chunk_loc1.clone();
                    || async { Ok(chunk_loc1) }
                })
                .await,
        );

        check_err(
            cache
                .get_or_insert_with(entry2.clone(), fail_path, {
                    let chunk_loc2 = chunk_loc2.clone();
                    || async { Ok(chunk_loc2) }
                })
                .await,
        );

        check_err(
            cache
                .get_or_insert_with(entry3.clone(), fail_path, {
                    let chunk_loc3 = chunk_loc3.clone();
                    || async { Ok(chunk_loc3) }
                })
                .await,
        );

        check_err(
            cache
                .get_or_insert_with(entry4.clone(), fail_path, {
                    let chunk_loc4 = chunk_loc4.clone();
                    || async { Ok(chunk_loc4) }
                })
                .await,
        );

        check_err(
            cache
                .get_or_insert_with(
                    entry1,
                    {
                        |cl| async move {
                            let cl = Arc::clone(&cl);
                            assert_eq!(*cl, chunk_loc1);
                            Ok(())
                        }
                    },
                    fail_write_path,
                )
                .await,
        );

        check_err(
            cache
                .get_or_insert_with(
                    entry2,
                    {
                        |cl| async move {
                            let cl = Arc::clone(&cl);
                            assert_eq!(*cl, chunk_loc2);
                            Ok(())
                        }
                    },
                    fail_write_path,
                )
                .await,
        );

        check_err(
            cache
                .get_or_insert_with(
                    entry3,
                    {
                        |cl| async move {
                            let cl = Arc::clone(&cl);
                            assert_eq!(*cl, chunk_loc3);
                            Ok(())
                        }
                    },
                    fail_write_path,
                )
                .await,
        );

        check_err(
            cache
                .get_or_insert_with(
                    entry4,
                    {
                        |cl| async move {
                            let cl = Arc::clone(&cl);
                            assert_eq!(*cl, chunk_loc4);
                            Ok(())
                        }
                    },
                    fail_write_path,
                )
                .await,
        );
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

        let check_err = |result| match result {
            Ok(()) => (),
            Err(err) => assert!(false, "Error occurred: {err}"),
        };

        let entry = Chunk::new(String::from("fake-uuid"), 120, 10);
        let chunk_loc = ChunkLocation::new(0, 20);

        // Insert
        check_err(
            cache
                .get_or_insert_with(entry.clone(), fail_path, {
                    let chunk_loc = chunk_loc.clone();
                    || async { Ok(chunk_loc) }
                })
                .await,
        );

        // Check insertion
        check_err(
            cache
                .get_or_insert_with(
                    entry.clone(),
                    {
                        let chunk_loc = chunk_loc.clone();
                        |cl| async move {
                            let cl = Arc::clone(&cl);
                            assert_eq!(*cl, chunk_loc);
                            Ok(())
                        }
                    },
                    fail_write_path,
                )
                .await,
        );

        // Remove
        check_err(cache.remove_zones(&[0]).await);

        // Check insertion is removed
        check_err(
            cache
                .get_or_insert_with(entry.clone(), fail_path, {
                    let chunk_loc = chunk_loc.clone();
                    || async { Ok(chunk_loc) }
                })
                .await,
        );
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

        let check_err = |result| match result {
            Ok(()) => (),
            Err(err) => assert!(false, "Error occurred: {err}"),
        };

        let entry = Chunk::new(String::from("fake-uuid"), 120, 10);
        let chunk_loc = ChunkLocation::new(0, 20);

        let entry2 = Chunk::new(String::from("fake-uuid"), 121, 10);
        let chunk_loc2 = ChunkLocation::new(0, 21);

        // Insert
        check_err(
            cache
                .get_or_insert_with(entry.clone(), fail_path, {
                    let chunk_loc = chunk_loc.clone();
                    || async { Ok(chunk_loc) }
                })
                .await,
        );

        check_err(
            cache
                .get_or_insert_with(entry2.clone(), fail_path, {
                    let chunk_loc = chunk_loc2.clone();
                    || async { Ok(chunk_loc) }
                })
                .await,
        );

        // Check insertion
        check_err(
            cache
                .get_or_insert_with(
                    entry.clone(),
                    {
                        let chunk_loc = chunk_loc.clone();
                        |cl| async move {
                            let cl = Arc::clone(&cl);
                            assert_eq!(*cl, chunk_loc);
                            Ok(())
                        }
                    },
                    fail_write_path,
                )
                .await,
        );

        check_err(
            cache
                .get_or_insert_with(
                    entry2.clone(),
                    {
                        let chunk_loc = chunk_loc2.clone();
                        |cl| async move {
                            let cl = Arc::clone(&cl);
                            assert_eq!(*cl, chunk_loc);
                            Ok(())
                        }
                    },
                    fail_write_path,
                )
                .await,
        );

        // Remove
        check_err(cache.remove_zones(&[0]).await);

        // Check insertion is removed
        check_err(
            cache
                .get_or_insert_with(entry.clone(), fail_path, {
                    let chunk_loc = chunk_loc.clone();
                    || async { Ok(chunk_loc) }
                })
                .await,
        );

        check_err(
            cache
                .get_or_insert_with(entry2.clone(), fail_path, {
                    let chunk_loc = chunk_loc2.clone();
                    || async { Ok(chunk_loc) }
                })
                .await,
        );
    }
}
