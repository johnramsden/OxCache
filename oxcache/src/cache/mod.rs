use crate::cache::bucket::{Chunk, ChunkLocation, ChunkState, PinnedChunkLocation};
use ndarray::{Array2, ArrayBase, s};
use nvme::types::{self, Zone};
use std::io::ErrorKind;
use std::iter::zip;
use std::sync::Arc;
use std::{collections::HashMap, io};
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use bytes::Bytes;
use tokio::sync::{Notify, RwLock};
use crate::cache::bucket::Chunk as CacheKey;
use crate::writerpool::{WriterPool};
use std::future::Future;
use crate::server::validate_read_response;

pub mod bucket;
pub use bucket::{DataSource, PinGuard};

// An entry
type EntryType = Arc<RwLock<ChunkState>>;
fn new_entry() -> EntryType {
    Arc::new(RwLock::new(ChunkState::Waiting {
        notify: Arc::new(Notify::new()),
        buffer: Arc::new(RwLock::new(None)),
    }))
}

// Entry not found
fn entry_not_found() -> std::io::Error {
    std::io::Error::new(ErrorKind::NotFound, "Couldn't find entry")
}

#[derive(Debug)]
struct BucketMap {
    buckets: HashMap<Chunk, EntryType>,
}

#[derive(Debug)]
pub struct Cache {
    // Sharded bucket maps for parallel access (key -> entry)
    shards: Vec<RwLock<BucketMap>>,
    num_shards: usize,
    // Shared reverse mapping (zone, chunk_index) -> key
    // This needs to be shared because we look up by location, not by key
    zone_to_entry: RwLock<Array2<Option<Chunk>>>,
}

impl Cache {
    pub fn new(num_zones: Zone, chunks_per_zone: types::Chunk, num_shards: usize) -> Self {
        let shards = (0..num_shards)
            .map(|_| {
                RwLock::new(BucketMap {
                    buckets: HashMap::new(),
                })
            })
            .collect();

        let zone_to_entry = RwLock::new(ArrayBase::from_elem(
            (num_zones as usize, chunks_per_zone as usize),
            None,
        ));

        Self {
            shards,
            num_shards,
            zone_to_entry,
        }
    }

    /// Hash a key to determine which shard it belongs to
    fn get_shard_index(&self, key: &Chunk) -> usize {
        let mut hasher = DefaultHasher::new();
        key.uuid.hash(&mut hasher);
        (hasher.finish() as usize) % self.num_shards
    }

    // If we're locking the entry, do we still need a notify function? Why not just await on the lock?
    pub async fn get_or_insert_with<R, W, RFut, WFut>(
        &self,
        key: Chunk,
        reader: R,
        writer: W,
    ) -> tokio::io::Result<()>
    where
        R: FnOnce(DataSource) -> RFut + Send + 'static,
        RFut: Future<Output=tokio::io::Result<()>> + Send + 'static,
        W: FnOnce(Arc<RwLock<Option<Bytes>>>, Arc<Notify>) -> WFut + Send + 'static,
        WFut: Future<Output=tokio::io::Result<ChunkLocation>> + Send + 'static,
    {
        let mut reader = Some(reader);
        // NOTE: If we are ever clearing something from the map we need to acquire exclusive lock
        //       on both the entire map and the individual chunk location

        let shard_idx = self.get_shard_index(&key);

        loop {
            // Bucket read locked -- no one can write to map
            let bucket_guard = self.shards[shard_idx].read().await;

            if let Some(state) = bucket_guard.buckets.get(&key) {
                let state = Arc::clone(state);
                drop(bucket_guard);
                // We now have the entry
                let bucket_state_guard = state;
                let bucket_state_guard = bucket_state_guard.read().await;

                let result = match &*bucket_state_guard {
                    ChunkState::Waiting { notify, buffer } => {
                        // Try to get buffer
                        let buffer_data = buffer.read().await;
                        if let Some(data) = buffer_data.clone() {
                            // Buffer available! Serve from RAM
                            drop(buffer_data);
                            drop(bucket_state_guard);

                            if let Some(reader_fn) = reader.take() {
                                Some(reader_fn(DataSource::Ram(data)).await)
                            } else {
                                panic!("Reader function called multiple times");
                            }
                        } else {
                            // Buffer not ready yet, wait
                            let notify = notify.clone();
                            drop(buffer_data);
                            drop(bucket_state_guard);

                            let notified = notify.notified();
                            notified.await;
                            None // Loop back to re-check
                        }
                    }
                    ChunkState::Ready(pinned_loc) => {
                        let pin_guard = pinned_loc.pin();
                        drop(bucket_state_guard);

                        if let Some(reader_fn) = reader.take() {
                            Some(reader_fn(DataSource::Disk(pin_guard)).await)
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
            let mut bucket_guard = self.shards[shard_idx].write().await;

            // Incase it was inserted inbetween
            if let Some(state) = bucket_guard.buckets.get(&key) {
                let state = Arc::clone(state);
                drop(bucket_guard);
                // We now have the entry
                let bucket_state_guard = state;
                let bucket_state_guard = bucket_state_guard.read().await;

                let result = match &*bucket_state_guard {
                    ChunkState::Waiting { notify, buffer } => {
                        // Try to get buffer
                        let buffer_data = buffer.read().await;
                        if let Some(data) = buffer_data.clone() {
                            // Buffer available! Serve from RAM
                            drop(buffer_data);
                            drop(bucket_state_guard);

                            if let Some(reader_fn) = reader.take() {
                                Some(reader_fn(DataSource::Ram(data)).await)
                            } else {
                                panic!("Reader function called multiple times");
                            }
                        } else {
                            // Buffer not ready yet, wait
                            let notify = notify.clone();
                            drop(buffer_data);
                            drop(bucket_state_guard);

                            let notified = notify.notified();
                            notified.await;
                            None // Loop back to re-check
                        }
                    }
                    ChunkState::Ready(pinned_loc) => {
                        let pin_guard = pinned_loc.pin();
                        drop(bucket_state_guard);

                        if let Some(reader_fn) = reader.take() {
                            Some(reader_fn(DataSource::Disk(pin_guard)).await)
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
                // Create buffer and notify for this new entry
                let notify = Arc::new(Notify::new());
                let buffer = Arc::new(RwLock::new(None));

                let locked_chunk_location = Arc::new(RwLock::new(ChunkState::Waiting {
                    notify: notify.clone(),
                    buffer: buffer.clone(),
                }));

                bucket_guard
                    .buckets
                    .insert(key.clone(), Arc::clone(&locked_chunk_location)); // Place locked waiting state
                drop(bucket_guard); // Bucket write unlocked -- Other writes can proceed on the outer map

                // Call writer, passing buffer and notify
                let write_result = writer(buffer.clone(), notify.clone()).await;
                match write_result {
                    Err(e) => {
                        // Clean up on error - remove entry and notify waiters
                        let mut bucket_guard = self.shards[shard_idx].write().await;
                        bucket_guard.buckets.remove(&key);
                        notify.notify_waiters(); // Wake waiters to let them retry

                        return Err(e);
                    }
                    Ok(location) => {
                        // Update reverse mapping
                        let mut reverse_mapping_guard = self.zone_to_entry.write().await;
                        reverse_mapping_guard[location.as_index()] = Some(key.clone());
                        tracing::debug!("LRU_SYNC: Added chunk {:?} to bucket map reverse mapping", location);
                        tracing::debug!("[map-dbg] location {:?} map updated", location);
                        drop(reverse_mapping_guard);

                        // Transition to Ready state
                        let mut chunk_loc_guard = locked_chunk_location.write().await;
                        tracing::debug!("[map-dbg] location {:?} being written to", location);
                        *chunk_loc_guard = ChunkState::Ready(Arc::new(PinnedChunkLocation::new(location.clone())));
                        drop(chunk_loc_guard);
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
        // First, get the zone_to_entry lock to find all chunks in the zones
        let mut zone_mapping = self.zone_to_entry.write().await;

        // Loop over zones
        for zone_index in zone_indices {
            // Get slice representing a zone
            let zone_slice = s![*zone_index as usize, ..];
            // loop over zone chunks
            let zone_view = zone_mapping.slice(zone_slice);

            // Collect all valid chunks first and group by shard
            let mut chunks_by_shard: Vec<Vec<Chunk>> = vec![Vec::new(); self.num_shards];
            for opt_chunk in zone_view.iter() {
                if let Some(chunk) = opt_chunk.as_ref() {
                    let shard_idx = self.get_shard_index(chunk);
                    chunks_by_shard[shard_idx].push(chunk.clone());
                }
            }

            // Remove from each shard
            for (shard_idx, chunks) in chunks_by_shard.iter().enumerate() {
                if !chunks.is_empty() {
                    let mut shard_guard = self.shards[shard_idx].write().await;
                    for chunk in chunks {
                        let entry = match shard_guard.buckets.remove(&chunk) {
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
                }
            }

            // Now safe to mutate reverse_mapping
            zone_mapping
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
            let mut out = Vec::new();
            let mut notifies = Vec::new();

            // Get zone_to_entry mapping to find all chunks in this zone
            let mut zone_mapping = self.zone_to_entry.write().await;
            let zone_slice = s![zone as usize, ..];

            // Collect all keys in this zone and group by shard
            let mut keys_by_shard: Vec<Vec<(Chunk, ChunkLocation)>> = vec![Vec::new(); self.num_shards];
            for (chunk_idx, opt_key) in zone_mapping.slice(zone_slice).iter().enumerate() {
                if let Some(key) = opt_key.clone() {
                    let old_loc = ChunkLocation::new(zone, chunk_idx as nvme::types::Chunk);
                    let shard_idx = self.get_shard_index(&key);
                    keys_by_shard[shard_idx].push((key, old_loc));
                }
            }

            // Process each shard
            for (shard_idx, keys) in keys_by_shard.iter().enumerate() {
                if keys.is_empty() {
                    continue;
                }

                for (key, old_loc) in keys {
                    let shard_guard = self.shards[shard_idx].read().await;
                    let entry = shard_guard
                        .buckets
                        .get(&key)
                        .ok_or_else(|| io::Error::new(ErrorKind::NotFound, "Missing entry"))?
                        .clone();
                    drop(shard_guard);

                    // Wait for pins to be released before proceeding
                    loop {
                        let mut st = entry.write().await;
                        let actual_old_loc = match &*st {
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
                            ChunkState::Waiting { .. } => {
                                // TODO: Shouldnt occur since zone was full
                                tracing::debug!("Encountered invalid waiting state during zone cleaning");
                                return Err(io::Error::new(ErrorKind::Other, "Encountered invalid waiting state during zone cleaning"))
                            }
                        };

                        let notify = Arc::new(Notify::new());
                        let buffer = Arc::new(RwLock::new(None));
                        // Update state to waiting
                        *st = ChunkState::Waiting {
                            notify: Arc::clone(&notify),
                            buffer,
                        };
                        drop(st);

                        out.push((key.clone(), actual_old_loc, entry));
                        notifies.push(notify);
                        break; // Successfully processed this entry
                    }
                }
            }

            // Clear old reverse slots
            for (key, old_loc, _) in &out {
                if zone_mapping[old_loc.as_index()].as_ref() == Some(key) {
                    zone_mapping[old_loc.as_index()] = None;
                }
            }

            drop(zone_mapping);
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

        // Batch update reverse map
        {
            let mut zone_mapping = self.zone_to_entry.write().await;
            for (key, new_loc, _) in new_locs {
                zone_mapping[new_loc.as_index()] = Some(key);
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

        let mut zone_mapping = self.zone_to_entry.write().await;

        for chunk in chunks {
            tracing::debug!("Removing {:?} from map", chunk);
            let chunk_id = match &zone_mapping[chunk.as_index()] {
                Some(id) => id.clone(),
                None => {
                    // This should not happen - indicates LRU/bucket map sync issue
                    tracing::error!("SYNC_BUG: Chunk {:?} was in LRU but not found in reverse map", chunk);
                    return Err(io::Error::new(ErrorKind::NotFound, format!("LRU/bucket map sync bug: chunk {:?} missing from reverse map", chunk)))
                },
            };

            let shard_idx = self.get_shard_index(&chunk_id);

            // Get entry reference first while holding shard lock
            let mut shard_guard = self.shards[shard_idx].write().await;
            let entry = match shard_guard.buckets.get(&chunk_id) {
                Some(v) => Arc::clone(v),
                None => {
                    tracing::error!("Not found chunk {:?} when removing entries, chunk id is {:?}", chunk, chunk_id);
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
                    drop(shard_guard); // Release shard lock before waiting
                    tracing::warn!("EVICTION: Waiting for chunk at {:?} to be unpinned during entry removal (pin_count={})",
                                   pinned_loc_clone.location, pinned_loc_clone.pin_count());
                    pinned_loc_clone.wait_for_unpin().await;
                    tracing::warn!("EVICTION: Chunk at {:?} unpinned during entry removal (pin_count={})",
                                   pinned_loc_clone.location, pinned_loc_clone.pin_count());
                    // Re-acquire shard lock and entry lock
                    shard_guard = self.shards[shard_idx].write().await;
                    let _entry_guard = entry.write().await;
                }
            }

            // Now safe to remove from maps while holding entry write lock
            zone_mapping[chunk.as_index()].take();
            let _removed_entry = shard_guard.buckets.remove(&chunk_id);
            tracing::debug!("LRU_SYNC: Removed chunk {:?} from bucket map via remove_entries", chunk);
            tracing::debug!("Found chunk {:?} when removing entries", _removed_entry.is_some());
            // entry_guard is dropped here, releasing the entry write lock
        }

        tracing::debug!("DEADLOCK_DEBUG: [Thread {:?}] Releasing zone_to_entry write lock for remove_entries", thread_id);
        // zone_mapping is dropped here
        tracing::debug!("DEADLOCK_DEBUG: [Thread {:?}] Completed remove_entries for {} chunks", thread_id, chunks.len());
        Ok(())
    }
}

#[cfg(test)]
mod mod_tests {
    use std::sync::Arc;

    use crate::cache::{
        Cache,
        bucket::{Chunk, ChunkLocation, DataSource},
    };
    use bytes::Bytes;

    #[tokio::test]
    async fn test_insert() {
        let cache = Cache::new(10, 100, 4);
        match cache
            .get_or_insert_with(
                Chunk::new(String::from("fake-uuid"), 120, 10),
                |_| async move {
                    assert!(false, "Shouldn't reach here");
                    Ok(())
                },
                |_buffer, _notify| async move { Ok(ChunkLocation::new(0, 20)) },
            )
            .await
        {
            Ok(()) => (),
            Err(err) => assert!(false, "Error occurred: {err}"),
        }

        match cache
            .get_or_insert_with(
                Chunk::new(String::from("fake-uuid"), 120, 10),
                |data_source| async move {
                    match data_source {
                        DataSource::Disk(pin_guard) => {
                            assert_eq!(pin_guard.location().zone, 0);
                            assert_eq!(pin_guard.location().index, 20);
                        }
                        DataSource::Ram(_) => assert!(false, "Should read from disk, not RAM"),
                    }
                    Ok(())
                },
                |_buffer, _notify| async move {
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
        let cache = Cache::new(10, 100, 4);

        let fail_path = async |_: DataSource| {
            assert!(false, "Shouldn't reach here");
            Ok(())
        };

        let fail_write_path = async |_buffer, _notify| -> tokio::io::Result<ChunkLocation> {
            assert!(false, "Shouldn't reach here");
            Ok(ChunkLocation { zone: 0, index: 0 })
        };

        let check_err = |result| match result {
            Ok(()) => (),
            Err(err) => assert!(false, "Error occurred: {err}"),
        };

        // Test UUID-only hashing: entries with same UUID should map to same cache entry
        // even with different offset/size
        let entry1 = Chunk::new(String::from("fake-uuid-1"), 120, 10);
        let entry2 = Chunk::new(String::from("fake-uuid-2"), 120, 10);
        let entry3 = Chunk::new(String::from("fake-uuid-3"), 121, 10);
        let entry4 = Chunk::new(String::from("fake-uuid-4"), 121, 11);

        // Different UUIDs, so different cache entries
        let chunk_loc1 = ChunkLocation::new(0, 20);
        let chunk_loc2 = ChunkLocation::new(1, 22);
        let chunk_loc3 = ChunkLocation::new(9, 25);
        let chunk_loc4 = ChunkLocation::new(7, 29);

        // Insert all four entries (all have different UUIDs)
        check_err(
            cache
                .get_or_insert_with(entry1.clone(), fail_path, {
                    let chunk_loc1 = chunk_loc1.clone();
                    |_buffer, _notify| async { Ok(chunk_loc1) }
                })
                .await,
        );

        check_err(
            cache
                .get_or_insert_with(entry2.clone(), fail_path, {
                    let chunk_loc2 = chunk_loc2.clone();
                    |_buffer, _notify| async { Ok(chunk_loc2) }
                })
                .await,
        );

        check_err(
            cache
                .get_or_insert_with(entry3.clone(), fail_path, {
                    let chunk_loc3 = chunk_loc3.clone();
                    |_buffer, _notify| async { Ok(chunk_loc3) }
                })
                .await,
        );

        check_err(
            cache
                .get_or_insert_with(entry4.clone(), fail_path, {
                    let chunk_loc4 = chunk_loc4.clone();
                    |_buffer, _notify| async { Ok(chunk_loc4) }
                })
                .await,
        );

        // Verify all entries hit cache with correct locations
        check_err(
            cache
                .get_or_insert_with(
                    entry1,
                    {
                        |data_source| async move {
                            match data_source {
                                DataSource::Disk(pin_guard) => {
                                    assert_eq!(pin_guard.location(), &chunk_loc1);
                                }
                                DataSource::Ram(_) => assert!(false, "Should read from disk, not RAM"),
                            }
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
                        |data_source| async move {
                            match data_source {
                                DataSource::Disk(pin_guard) => {
                                    assert_eq!(pin_guard.location(), &chunk_loc2);
                                }
                                DataSource::Ram(_) => assert!(false, "Should read from disk, not RAM"),
                            }
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
                        |data_source| async move {
                            match data_source {
                                DataSource::Disk(pin_guard) => {
                                    assert_eq!(pin_guard.location(), &chunk_loc3);
                                }
                                DataSource::Ram(_) => assert!(false, "Should read from disk, not RAM"),
                            }
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
                        |data_source| async move {
                            match data_source {
                                DataSource::Disk(pin_guard) => {
                                    assert_eq!(pin_guard.location(), &chunk_loc4);
                                }
                                DataSource::Ram(_) => assert!(false, "Should read from disk, not RAM"),
                            }
                            Ok(())
                        }
                    },
                    fail_write_path,
                )
                .await,
        );
    }

    #[tokio::test]
    async fn test_uuid_only_hashing() {
        let cache = Cache::new(10, 100, 4);

        let fail_write_path = async |_buffer, _notify| -> tokio::io::Result<ChunkLocation> {
            assert!(false, "Write shouldn't be called for cache hit");
            Ok(ChunkLocation { zone: 0, index: 0 })
        };

        let check_err = |result| match result {
            Ok(()) => (),
            Err(err) => assert!(false, "Error occurred: {err}"),
        };

        // Insert entry with specific offset/size
        let entry1 = Chunk::new(String::from("same-uuid"), 120, 10);
        let chunk_loc1 = ChunkLocation::new(0, 20);

        check_err(
            cache
                .get_or_insert_with(entry1.clone(), |_| async { Ok(()) }, {
                    let chunk_loc1 = chunk_loc1.clone();
                    |_buffer, _notify| async { Ok(chunk_loc1) }
                })
                .await,
        );

        // Try to insert with same UUID but different offset/size
        // Should hit cache (reader path), not write
        let entry2 = Chunk::new(String::from("same-uuid"), 0, 100);

        check_err(
            cache
                .get_or_insert_with(
                    entry2,
                    {
                        |data_source| async move {
                            match data_source {
                                DataSource::Disk(pin_guard) => {
                                    // Should hit cache and return same location as entry1
                                    assert_eq!(pin_guard.location(), &chunk_loc1);
                                }
                                DataSource::Ram(_) => assert!(false, "Should read from disk, not RAM"),
                            }
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
        let cache = Cache::new(10, 100, 4);

        let fail_path = async |_: DataSource| {
            assert!(false, "Shouldn't reach here");
            Ok(())
        };

        let fail_write_path = async |_buffer, _notify| -> tokio::io::Result<ChunkLocation> {
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
                    |_buffer, _notify| async { Ok(chunk_loc) }
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
                        |data_source| async move {
                            match data_source {
                                DataSource::Disk(pin_guard) => {
                                    assert_eq!(pin_guard.location(), &chunk_loc);
                                }
                                DataSource::Ram(_) => assert!(false, "Should read from disk, not RAM"),
                            }
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
                    |_buffer, _notify| async { Ok(chunk_loc) }
                })
                .await,
        );
    }

    #[tokio::test]
    async fn test_multiple_remove() {
        let cache = Cache::new(10, 100, 4);

        let fail_path = async |_: DataSource| {
            assert!(false, "Shouldn't reach here");
            Ok(())
        };

        let fail_write_path = async |_buffer, _notify| -> tokio::io::Result<ChunkLocation> {
            assert!(false, "Shouldn't reach here");
            Ok(ChunkLocation { zone: 0, index: 0 })
        };

        let check_err = |result| match result {
            Ok(()) => (),
            Err(err) => assert!(false, "Error occurred: {err}"),
        };

        let entry = Chunk::new(String::from("fake-uuid-1"), 120, 10);
        let chunk_loc = ChunkLocation::new(0, 20);

        let entry2 = Chunk::new(String::from("fake-uuid-2"), 121, 10);
        let chunk_loc2 = ChunkLocation::new(0, 21);

        // Insert
        check_err(
            cache
                .get_or_insert_with(entry.clone(), fail_path, {
                    let chunk_loc = chunk_loc.clone();
                    |_buffer, _notify| async { Ok(chunk_loc) }
                })
                .await,
        );

        check_err(
            cache
                .get_or_insert_with(entry2.clone(), fail_path, {
                    let chunk_loc = chunk_loc2.clone();
                    |_buffer, _notify| async { Ok(chunk_loc) }
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
                        |data_source| async move {
                            match data_source {
                                DataSource::Disk(pin_guard) => {
                                    assert_eq!(pin_guard.location(), &chunk_loc);
                                }
                                DataSource::Ram(_) => assert!(false, "Should read from disk, not RAM"),
                            }
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
                        |data_source| async move {
                            match data_source {
                                DataSource::Disk(pin_guard) => {
                                    assert_eq!(pin_guard.location(), &chunk_loc);
                                }
                                DataSource::Ram(_) => assert!(false, "Should read from disk, not RAM"),
                            }
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
                    |_buffer, _notify| async { Ok(chunk_loc) }
                })
                .await,
        );

        check_err(
            cache
                .get_or_insert_with(entry2.clone(), fail_path, {
                    let chunk_loc = chunk_loc2.clone();
                    |_buffer, _notify| async { Ok(chunk_loc) }
                })
                .await,
        );
    }
}
