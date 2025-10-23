use crate::cache::bucket::{Chunk, ChunkLocation, ChunkState, PinnedChunkLocation, PinGuard};
use ndarray::{Array2, ArrayBase, s};
use nvme::types::{self, Zone};
use std::io::ErrorKind;
use std::iter::zip;
use std::sync::Arc;
use std::{collections::HashMap, io};
use bytes::Bytes;
use tokio::sync::{Notify, RwLock};
use crate::cache::bucket::Chunk as CacheKey;
use crate::writerpool::{WriterPool};
use std::future::Future;
use crate::server::validate_read_response;

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
        R: FnOnce(PinGuard) -> RFut + Send + 'static,
        RFut: Future<Output=tokio::io::Result<()>> + Send + 'static,
        W: FnOnce() -> WFut + Send + 'static,
        WFut: Future<Output=tokio::io::Result<ChunkLocation>> + Send + 'static,
    {
        let mut reader = Some(reader);
        // NOTE: If we are ever clearing something from the map we need to acquire exclusive lock
        //       on both the entire map and the individual chunk location

        loop {
            // Bucket read locked -- no one can write to map
            tracing::debug!("READER: Acquiring bucket read lock for key {:?}", key);
            let bucket_guard = self.bm.read().await;
            tracing::debug!("READER: Acquired bucket read lock for key {:?}", key);

            if let Some(state) = bucket_guard.buckets.get(&key) {
                let state = Arc::clone(state);
                drop(bucket_guard);
                // We now have the entry
                let bucket_state_guard = state;
                let bucket_state_guard = bucket_state_guard.read().await;

                let result = match &*bucket_state_guard {
                    ChunkState::Waiting(notify) => {
                        let notify = notify.clone();
                        drop(bucket_state_guard);
                        let notified = notify.notified(); // queue notifies
                        notified.await; // retry loop required
                        None // Continue the loop
                    }
                    ChunkState::Ready(pinned_loc) => {
                        let pin_guard = pinned_loc.pin();
                        // drop(bucket_state_guard); // Drop the lock early, pin protects from eviction
                        if let Some(reader_fn) = reader.take() {
                            Some(reader_fn(pin_guard).await) // Return the result
                        } else {
                            panic!("Reader function called multiple times");
                        }
                    }
                };

                match result {
                    Some(r) => return r,
                    None => continue, // loop to recheck state
                }
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

                let result = match &*bucket_state_guard {
                    ChunkState::Waiting(notify) => {
                        let notify = notify.clone();
                        drop(bucket_state_guard);
                        let notified = notify.notified(); // queue notifies
                        notified.await; // retry loop required
                        None // Continue the loop
                    }
                    ChunkState::Ready(pinned_loc) => {
                        let pin_guard = pinned_loc.pin();
                        // drop(bucket_state_guard); // Drop the lock early, pin protects from eviction
                        if let Some(reader_fn) = reader.take() {
                            Some(reader_fn(pin_guard).await) // Return the result
                        } else {
                            panic!("Reader function called multiple times");
                        }
                    }
                };

                match result {
                    Some(r) => return r,
                    None => continue, // loop to recheck state
                }
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
                        drop(chunk_loc_guard);

                        let mut reverse_mapping_guard = self.bm.write().await;
                        reverse_mapping_guard.zone_to_entry[location.as_index()] = Some(key);
                        tracing::debug!("LRU_SYNC: Added chunk {:?} to bucket map reverse mapping", location);
                        tracing::debug!("[map-dbg] location {:?} map updated", location);
                        // tracing::debug!("[map-dbg] state is {:#?}", *reverse_mapping_guard);

                        let mut chunk_loc_guard = locked_chunk_location.write().await;
                        tracing::debug!("[map-dbg] location {:?} being written to", location);
                        let notify = match &*chunk_loc_guard {
                            ChunkState::Waiting(n) => n.clone(),
                            _ => panic!("Chunk was not in waiting state"),
                        };
                        *chunk_loc_guard = ChunkState::Ready(Arc::new(PinnedChunkLocation::new(location.clone())));
                        notify.notify_waiters();
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

    pub async fn clean_zone_and_update_map<R, W, RFut, WFut>(
        &self,
        zone: Zone,
        reader: R,
        writer: W,
        writer_pool: Arc<WriterPool>,
    ) -> io::Result<()>
    where
        R: FnOnce(Vec<(CacheKey, ChunkLocation)>) -> RFut + Send,
        RFut: Future<Output=io::Result<Vec<(CacheKey, Bytes)>>> + Send,
        W: FnOnce(Vec<(CacheKey, Bytes)>) -> WFut + Send,
        WFut: Future<Output=io::Result<Vec<(CacheKey, ChunkLocation, Bytes)>>> + Send,
    {

        // Reset the existing entries
        // Collect items and corresponding notifiers
        let (items, notifies) = {
            let mut bm = self.bm.write().await;

            let zone_slice = s![zone as usize, ..];
            let mut out = Vec::new();
            let mut notifies = Vec::new();

            let mut chunks_processed = 0;
            let mut chunks_found = 0;

            // Iterate through the entire list of chunks in the zone
            for opt_key in bm.zone_to_entry.slice(zone_slice).iter() {
                chunks_processed += 1;
                // Collect only if Some
                if let Some(key) = opt_key.clone() {
                    chunks_found += 1;
                    let entry = bm
                        .buckets
                        .get(&key)
                        .ok_or_else(|| io::Error::new(ErrorKind::NotFound, "Missing entry"))?
                        .clone();

                    // Wait for pins to be released before proceeding
                    loop {
                        let mut st = entry.write().await;
                        let old_loc = match &*st {
                            ChunkState::Ready(pinned_loc) => {
                                // Wait for this location to be unpinned
                                if !pinned_loc.can_evict() {
                                    let pinned_loc_clone = Arc::clone(pinned_loc);
                                    drop(st); // Release lock before waiting

                                    pinned_loc_clone.wait_for_unpin().await;
                                    continue; // Retry after being notified
                                }
                                pinned_loc.location.clone()
                            },
                            ChunkState::Waiting(_) => {
                                // TODO: Shouldnt occur since zone was full
                                tracing::debug!("Encountered invalid waiting state during zone cleaning");
                                return Err(io::Error::new(ErrorKind::Other, "Encountered invalid waiting state during zone cleaning"))
                            }
                        };

                        let notify = Arc::new(Notify::new());
                        // Update state to waiting
                        *st = ChunkState::Waiting(Arc::clone(&notify));
                        drop(st);

                        out.push((key, old_loc, entry));
                        notifies.push(notify);
                        break; // Successfully processed this entry
                    }
                }
            }

            // Clear old reverse slots
            for (key, old_loc, _) in &out {
                if bm.zone_to_entry[old_loc.as_index()].as_ref() == Some(key) {
                    bm.zone_to_entry[old_loc.as_index()] = None;
                }
            }
            (out, notifies)
        };

        // Read the valid chunks from the zone
        // Buffer all chunks
        let read_input: Vec<_> = items
            .iter()
            .map(|(k, l, _)| (k.clone(), l.clone()))
            .collect();

        let payloads = match reader(read_input).await {
            Ok(p) => {
                p
            },
            Err(e) => {
                // TODO: Should we bother? Probably still fatal
                // rollback
                for (_, old_loc, entry) in &items {
                    let mut st = entry.write().await;
                    *st = ChunkState::Ready(Arc::new(PinnedChunkLocation::new(old_loc.clone())));
                }
                for n in notifies {
                    n.notify_waiters();
                }
                return Err(e);
            }
        };

        // Write data out using reserved space
        let new_locs = writer(payloads).await?;

        // Update states & reverse map
        for (key, new_loc, b) in &new_locs {
            if let Some((_, _, entry)) = items.iter().find(|(k, _, _)| *k == *key) {
                let mut st = entry.write().await;
                *st = ChunkState::Ready(Arc::new(PinnedChunkLocation::new(new_loc.clone())));
            } else {
                return Err(io::Error::new(
                    ErrorKind::NotFound,
                    format!("Missing entry for {:?}", key),
                ));
            }

            #[cfg(debug_assertions)]
            validate_read_response(&b, &key.uuid, key.offset, key.size);
        }

        // Batch update reverse map (single bm lock)
        {
            let mut bm = self.bm.write().await;
            for (key, new_loc, _) in new_locs {
                bm.zone_to_entry[new_loc.as_index()] = Some(key);
            }
        }

        for n in notifies {
            n.notify_waiters();
        }

        Ok(())
    }

    pub async fn remove_entries(&self, chunks: &[ChunkLocation]) -> tokio::io::Result<()> {
        let thread_id = std::thread::current().id();
        // to_relocate is a list of ChunkLocations that the caller wants to update
        // We pass in each chunk location and the writer function should return back with the list of updated chunk locations
        let mut bucket_guard = self.bm.write().await;

        for chunk in chunks {
            tracing::debug!("Removing {:?} from map", chunk);
            let chunk_id = match &bucket_guard.zone_to_entry[chunk.as_index()] {
                Some(id) => id.clone(),
                None => {
                    // This should not happen - indicates LRU/bucket map sync issue
                    tracing::error!("SYNC_BUG: Chunk {:?} was in LRU but not found in reverse map", chunk);
                    return Err(io::Error::new(ErrorKind::NotFound, format!("LRU/bucket map sync bug: chunk {:?} missing from reverse map", chunk)))
                },
            };

            // Get entry reference first while holding bucket lock
            let entry = match bucket_guard.buckets.get(&chunk_id) {
                Some(v) => Arc::clone(v),
                None => {
                    tracing::error!("Not found chunk {:?} when removing entries, chunk id is {:?} state is {:#?}", chunk, chunk_id, *bucket_guard);
                    return Err(entry_not_found());
                },
            };

            // Get the write lock FIRST to prevent new pins
            let entry_guard = entry.write().await;

            // Check if pinned and wait if necessary
            if let ChunkState::Ready(pinned_loc) = &*entry_guard {
                if !pinned_loc.can_evict() {
                    let pinned_loc_clone = Arc::clone(pinned_loc);
                    drop(entry_guard);
                    drop(bucket_guard); // Release bucket lock before waiting
                    tracing::warn!("EVICTION: Waiting for chunk at {:?} to be unpinned during entry removal (pin_count={})",
                                   pinned_loc_clone.location, pinned_loc_clone.pin_count());
                    pinned_loc_clone.wait_for_unpin().await;
                    tracing::warn!("EVICTION: Chunk at {:?} unpinned during entry removal (pin_count={})",
                                   pinned_loc_clone.location, pinned_loc_clone.pin_count());
                    // Re-acquire bucket lock and entry lock
                    bucket_guard = self.bm.write().await;
                    let _entry_guard = entry.write().await;
                }
            }

            // Now safe to remove from maps while holding entry write lock
            bucket_guard.zone_to_entry[chunk.as_index()].take();
            let _removed_entry = bucket_guard.buckets.remove(&chunk_id);
            tracing::debug!("LRU_SYNC: Removed chunk {:?} from bucket map via remove_entries", chunk);
            tracing::debug!("Found chunk {:?} when removing entries", _removed_entry.is_some());
            // entry_guard is dropped here, releasing the entry write lock
        }

        tracing::debug!("DEADLOCK_DEBUG: [Thread {:?}] Releasing BM write lock for remove_entries", thread_id);
        // bucket_guard is dropped here
        tracing::debug!("DEADLOCK_DEBUG: [Thread {:?}] Completed remove_entries for {} chunks", thread_id, chunks.len());
        Ok(())
    }
}

#[cfg(test)]
mod mod_tests {
    use std::sync::Arc;

    use crate::cache::{
        Cache,
        bucket::{Chunk, ChunkLocation},
    };
    use crate::cache::bucket::PinGuard;

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
                |pin_guard| async move {
                    assert_eq!(pin_guard.location().zone, 0);
                    assert_eq!(pin_guard.location().index, 20);
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

        let fail_path = async |_: PinGuard| {
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
                        |pin_guard| async move {
                            assert_eq!(pin_guard.location(), &chunk_loc1);
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
                        |pin_guard| async move {
                            assert_eq!(pin_guard.location(), &chunk_loc2);
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
                        |pin_guard| async move {
                            assert_eq!(pin_guard.location(), &chunk_loc3);
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
                        |pin_guard| async move {
                            assert_eq!(pin_guard.location(), &chunk_loc4);
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

        let fail_path = async |_: PinGuard| {
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
                        |pin_guard| async move {
                            assert_eq!(pin_guard.location(), &chunk_loc);
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

        let fail_path = async |_: PinGuard| {
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
                        |pin_guard| async move {
                            assert_eq!(pin_guard.location(), &chunk_loc);
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
                        |pin_guard| async move {
                            assert_eq!(pin_guard.location(), &chunk_loc);
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
