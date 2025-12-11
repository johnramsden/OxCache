use std::{fs, io};
use crate::cache::Cache;
use crate::cache::bucket::ChunkLocation;
use crate::eviction::{EvictTarget, EvictionPolicyWrapper, EvictorMessage};
use crate::server::RUNTIME;
use crate::writerpool::{WriterPool, BatchWriteRequest};
use crate::zone_state::zone_list::{ZoneList, ZoneObtainFailure};
use aligned_vec::{AVec, Alignment, RuntimeAlign};
use bytes::Bytes;
use flume::Sender;
use nvme::info::{get_active_zones, get_lba_at, is_zoned_device, nvme_get_info, report_zones_all};
use nvme::ops::{close_zone, finish_zone, reset_zone, zns_append};
use nvme::types::{Byte, Chunk, LogicalBlock, NVMeConfig, PerformOn, ZNSConfig, Zone, ZoneState};
use std::io::ErrorKind;
use std::os::fd::RawFd;
use std::sync::{Arc, Condvar, Mutex, MutexGuard, RwLock};
use std::time::Duration;
use crate::metrics::{MetricType, METRICS};
use crate::zone_state::zone_priority_queue::ZonePriorityQueue;
use crate::cache::bucket::Chunk as CacheKey;
use std::sync::atomic::Ordering;

#[derive(Debug)]
pub struct Zoned {
    nvme_config: NVMeConfig,
    config: ZNSConfig,
    // Notification that the number of writers has decreased to below the MAR and zone opening/reset can be attempted
    zones: Arc<(Mutex<ZoneList>, Condvar)>,
    eviction_channel: Sender<EvictorMessage>,
    max_write_size: Byte,
    zone_append_lock: Vec<RwLock<()>>,
    #[cfg(feature = "eviction-metrics")]
    pub eviction_metrics: Option<Arc<crate::eviction_metrics::EvictionMetrics>>,
}

// Information about each zone
#[derive(Clone)]
pub struct BlockZoneInfo {
    _write_pointer: LogicalBlock,
}

pub struct BlockDeviceState {
    _zones: Vec<BlockZoneInfo>,
    active_zones: ZoneList,
    _chunk_size: LogicalBlock,
}

impl BlockDeviceState {
    fn new(num_zones: Zone, chunks_per_zone: Chunk, chunk_size: LogicalBlock) -> Self {
        let zones = vec![BlockZoneInfo { _write_pointer: 0 }; num_zones as usize];
        Self {
            _zones: zones,
            active_zones: ZoneList::new(num_zones, chunks_per_zone, num_zones as usize),
            _chunk_size: chunk_size,
        }
    }
}

pub struct BlockInterface {
    nvme_config: NVMeConfig,
    chunk_size_in_bytes: Byte,
    chunk_size_in_lbas: LogicalBlock,
    chunks_per_zone: Chunk,
    num_zones: Zone,
    state: Arc<Mutex<BlockDeviceState>>,
    eviction_channel: Sender<EvictorMessage>,
    max_write_size: Byte,
    #[cfg(feature = "eviction-metrics")]
    pub eviction_metrics: Option<Arc<crate::eviction_metrics::EvictionMetrics>>,
}

pub trait Device: Send + Sync {
    fn append(&self, data: Bytes) -> std::io::Result<ChunkLocation> {
        self.append_with_eviction_bypass(data, false)
    }

    fn append_with_eviction_bypass(&self, data: Bytes, is_eviction: bool) -> std::io::Result<ChunkLocation>;

    /// This is expected to remove elements from the cache as well
    fn evict(self: Arc<Self>, cache: Arc<Cache>, writer_pool: Arc<WriterPool>, eviction_policy: Arc<Mutex<EvictionPolicyWrapper>>, always_evict: bool) -> io::Result<()>;

    fn read(&self, location: ChunkLocation) -> std::io::Result<Bytes>;

    fn read_subset(&self, location: ChunkLocation, offset: Byte, size: Byte) -> std::io::Result<(Bytes, Bytes)>;

    fn get_num_zones(&self) -> Zone;

    fn get_chunks_per_zone(&self) -> Chunk;
    fn get_block_size(&self) -> Byte;
    fn get_use_percentage(&self) -> f32;

    fn reset(&self) -> io::Result<()>;

    fn reset_zone(&self, zone_id: Zone) -> io::Result<()>;

    fn close_zone(&self, zone_id: Zone) -> io::Result<()>;

    fn finish_zone(&self, zone_id: Zone) -> io::Result<()>;

    fn read_into_buffer(
        &self,
        max_write_size: Byte,
        lba_loc: LogicalBlock,
        read_buffer: &mut [u8],
        nvme_config: &NVMeConfig,
    ) -> io::Result<()> {
        let total_sz = read_buffer.len();
        let write_sz = total_sz.min(max_write_size as usize);
        let mut byte_ind = 0;

        let mut lba_loc = lba_loc;

        while byte_ind < total_sz {
            let end = (byte_ind + write_sz).min(read_buffer.len());
            let chunk_size = end - byte_ind;
            let lbas_read = chunk_size as u64 / nvme_config.logical_block_size;

            assert_eq!(
                chunk_size % nvme_config.logical_block_size as usize,
                0,
                "Unaligned read size"
            );

            if let Err(err) = nvme::ops::read(nvme_config, lba_loc, &mut read_buffer[byte_ind..end]) {
                return Err(err.try_into().unwrap());
            }

            byte_ind += chunk_size;
            lba_loc += lbas_read;
        }

        Ok(())
    }

    fn get_fd(&self) -> RawFd;
    fn get_nsid(&self) -> u32;

    #[cfg(feature = "eviction-metrics")]
    fn get_eviction_metrics(&self) -> Option<Arc<crate::eviction_metrics::EvictionMetrics>> {
        None
    }
}

pub fn get_device(
    device: &str,
    chunk_size: Byte,
    block_zone_capacity: Byte,
    eviction_channel: Sender<EvictorMessage>,
    max_write_size: Byte,
    max_zones: Option<u64>,
) -> io::Result<Arc<dyn Device>> {
    let device = fs::canonicalize(device)?;
    let device = device.to_str().unwrap();
    let is_zoned = is_zoned_device(device)?;
    if is_zoned {
        Ok(Arc::new(Zoned::new(
            device,
            chunk_size,
            eviction_channel,
            max_write_size,
            max_zones,
        )?))
    } else {
        Ok(Arc::new(BlockInterface::new(
            device,
            chunk_size,
            block_zone_capacity,
            eviction_channel,
            max_write_size,
            max_zones,
        )?))
    }
}

fn check_first_evict_bench() {
    // Only run if duration-based benchmark is enabled
    if !METRICS.benchmark_state.duration_benchmark_enabled.load(Ordering::SeqCst) {
        return;
    }

    // Check if this is the first eviction for benchmark mode (duration-based)
    if !METRICS.benchmark_state.first_eviction_triggered.swap(true, Ordering::SeqCst) {
        // This is the first eviction - start duration-based benchmark timer
        let current_bytes = METRICS.get_counter("bytes_total").unwrap_or(0);
        *METRICS.benchmark_state.initial_bytes_total.lock().unwrap() = Some(current_bytes);
        *METRICS.benchmark_state.benchmark_start_time.lock().unwrap() = Some(std::time::Instant::now());
        tracing::info!("=== BENCHMARK: First eviction triggered at bytes_total={} ===", current_bytes);
    }
}

fn trigger_eviction(eviction_channel: Sender<EvictorMessage>) -> io::Result<()> {
    tracing::debug!("DEVICE: [Thread {:?}] Sending eviction trigger", std::thread::current().id());
    let (resp_tx, resp_rx) = flume::bounded(1);
    if let Err(e) = eviction_channel.send(EvictorMessage { sender: resp_tx }) {
        tracing::error!("[append] Failed to send eviction message: {}", e);
        return Err(std::io::Error::new(
            std::io::ErrorKind::Other,
            "Failed to send eviction message",
        ));
    };

    match resp_rx.recv() {
        Ok(result) => {
            match result {
                Ok(_) => {
                    tracing::debug!("DEVICE: [Thread {:?}] Eviction completed successfully", std::thread::current().id());
                }
                Err(e) => {
                    tracing::error!("DEVICE: [Thread {:?}] Eviction failed: {}", std::thread::current().id(), e);
                    return Err(std::io::Error::new(std::io::ErrorKind::Other, format!("Eviction failed: {}", e)));
                }
            }
        }
        Err(e) => {
            tracing::error!("DEVICE: [Thread {:?}] Failed to receive eviction response: {}", std::thread::current().id(), e);
            return Err(std::io::Error::new(std::io::ErrorKind::Other, format!("Failed to receive eviction response: {}", e)));
        }
    }

    Ok(())
}

impl Zoned {
    /// Wrapper for ZoneList, handles mutex and notification
    fn get_free_zone(&self, is_eviction: bool) -> io::Result<Zone> {
        let (mtx, wait_notify) = &*self.zones;
        let mut zone_list = mtx.lock().unwrap();

        debug_assert!(get_active_zones(self.nvme_config.fd, self.nvme_config.nsid).unwrap() <= self.config.max_active_resources as usize);

        match zone_list.remove_with_eviction_bypass(is_eviction) {
            Ok(zone_idx) => Ok(zone_idx),
            Err(error) => match error {
                ZoneObtainFailure::EvictNow => {
                    Err(io::Error::new(ErrorKind::StorageFull, "Cache is full"))
                }
                ZoneObtainFailure::Wait => loop {
                    zone_list = wait_notify.wait(zone_list).unwrap();
                    match zone_list.remove_with_eviction_bypass(is_eviction) {
                        Ok(idx) => return Ok(idx),
                        Err(err) => match err {
                            ZoneObtainFailure::EvictNow => {
                                return Err(io::Error::new(ErrorKind::Other, "Cache is full"));
                            }
                            ZoneObtainFailure::Wait => continue,
                        },
                    }
                },
            },
        }
    }

    fn complete_write(&self, zone_idx: Zone, finish_zone: bool) -> io::Result<()> {
        let (mtx, notify) = &*self.zones;
        let mut zone_list = mtx.lock().unwrap();
        // assert!(zone_list.get_open_zones() == active_zones, "{} vs {}", zone_list.get_open_zones(), active_zones);
        debug_assert!(get_active_zones(self.nvme_config.fd, self.nvme_config.nsid).unwrap() <= self.config.max_active_resources as usize);

        zone_list.write_finish(zone_idx, self, finish_zone)?;
        // Tell other threads that we finished writing, so they can
        // come and try to open a new zone if needed.
        notify.notify_all();

        Ok(())
    }


}

impl Zoned {
    fn new(
        device: &str,
        chunk_size: Byte,
        eviction_channel: Sender<EvictorMessage>,
        max_write_size: Byte,
        max_zones: Option<u64>,
    ) -> io::Result<Self> {
        let nvme_config = match nvme::info::nvme_get_info(device) {
            Ok(config) => config,
            Err(err) => return Err(err.try_into().unwrap()),
        };

        match nvme::info::zns_get_info(&nvme_config) {
            Ok(mut config) => {
                tracing::debug!("ZNSConfig: {:?}", config);

                let chunk_size_in_logical_blocks: LogicalBlock =
                    nvme_config.byte_address_to_lba(chunk_size);
                config.chunks_per_zone = config.zone_cap / chunk_size_in_logical_blocks;
                config.chunk_size_in_lbas = chunk_size_in_logical_blocks;
                config.chunk_size_in_bytes = chunk_size;
                let num_zones: Zone = config.num_zones;

                // Apply max_zones restriction if specified
                let restricted_num_zones = if let Some(max_zones) = max_zones {
                    if max_zones > num_zones {
                        return Err(std::io::Error::new(
                            std::io::ErrorKind::InvalidInput,
                            format!("max_zones ({}) cannot be larger than the maximum number of zones available on the device ({})", max_zones, num_zones)
                        ));
                    }
                    max_zones
                } else {
                    num_zones
                };

                let zone_list = ZoneList::new(
                    restricted_num_zones,
                    config.chunks_per_zone,
                    config.max_active_resources as usize,
                );

                let zone_append_lock: Vec<RwLock<()>> =
                    (0..restricted_num_zones).map(|_| RwLock::new(())).collect();

                #[cfg(feature = "eviction-metrics")]
                let eviction_metrics = Some(crate::eviction_metrics::EvictionMetrics::new(restricted_num_zones));

                // Update config to reflect the restricted number of zones
                config.num_zones = restricted_num_zones;

                Ok(Self {
                    nvme_config,
                    config,
                    eviction_channel,
                    zones: Arc::new((Mutex::new(zone_list), Condvar::new())),
                    max_write_size,
                    zone_append_lock,
                    #[cfg(feature = "eviction-metrics")]
                    eviction_metrics,
                })
            }
            Err(err) => Err(err.try_into().unwrap()),
        }
    }

    fn lba_to_chunk_index(&self, lba: LogicalBlock, zone_index: Zone) -> u64 {
        // Make sure LBA is zone-relative
        let zone_start_lba = self.config.get_starting_lba(zone_index);
        let rel_lba: LogicalBlock = lba
            .checked_sub(zone_start_lba)
            .expect("LBA was not inside the specified zone");

        // Get chunk index
        let chunk_ind = rel_lba / self.config.chunk_size_in_lbas;

        tracing::debug!(
            "zone_index={}, zone_start_lba={}, rel_lba={}, chunk_size={}, chunk_ind={}",
            zone_index,
            zone_start_lba,
            rel_lba,
            self.config.chunk_size_in_lbas,
            chunk_ind
        );

        chunk_ind
    }

    fn chunked_append(&self, data: Bytes, zone_index: Zone) -> io::Result<ChunkLocation> {
        tracing::debug!("Chunk appending to zone {}", zone_index);

        let total_sz = data.len() as Byte;
        let write_sz = total_sz.min(self.max_write_size);

        tracing::debug!(
            "[Device]: Total size = {}, Write size = {}, max_write_size = {}",
            total_sz,
            write_sz,
            self.max_write_size
        );

        // Only locks if needed
        // this is AWFUL
        let _maybe_guard = if total_sz > self.max_write_size {
            (None, Some(self.zone_append_lock[zone_index as usize].write().unwrap()))
        } else {
            (Some(self.zone_append_lock[zone_index as usize].read().unwrap()), None)
        };

        // Sequentially write looped

        let mut byte_ind: Byte = 0;

        let mut first_chunk: Option<ChunkLocation> = None;
        let mut last_lba: Option<LogicalBlock> = None;

        while byte_ind < total_sz {
            let end: Byte = (byte_ind + write_sz).min(data.len() as Byte);
            match zns_append(
                &self.nvme_config,
                &self.config,
                zone_index as u64,
                &data[byte_ind as usize..end as usize],
            ) {
                Ok(lba) => {
                    // println!("[append] wrote to lba {} at zone {} from bytes ({}..{})", lba, zone_index, byte_ind, end);
                    let lbas_written = self.nvme_config.byte_address_to_lba(end - byte_ind);
                    if first_chunk.is_none() {
                        let chunk = self.lba_to_chunk_index(lba, zone_index);
                        first_chunk = Some(ChunkLocation::new(zone_index, chunk));
                        // println!("Chunk {:?}", first_chunk);
                        // println!("lba={}, self.config.chunk_size = {}, lba / write_sz = {}", lba, self.config.chunk_size, lba/write_sz as u64);
                    }
                    if let Some(last_lba) = last_lba {
                        let lba_check = last_lba + lbas_written;
                        assert_eq!(
                            lba_check, lba,
                            "lbas are not contiguous=({}, {})",
                            lba_check, lba
                        );
                    }
                    last_lba = Some(lba);
                }
                Err(err) => {
                    return Err(err
                        .add_context(format!("Write failed at zone {}", zone_index))
                        .add_context(format!("Zone state: {:#?}", {
                            let (_nz, state) =
                                report_zones_all(self.nvme_config.fd, self.nvme_config.nsid)
                                    .unwrap();
                            state
                                .iter()
                                .map(|state| state.zone_state.clone())
                                .collect::<Vec<ZoneState>>()
                        }))
                        .add_context(format!(
                            "Zone list state:\n{:#?}",
                            self.zones.0.lock().unwrap()
                        ))
                        .try_into()
                        .unwrap());
                }
            }
            byte_ind += write_sz;
        }

        let cl = first_chunk.unwrap();

        let finish_zone = cl.index + 1 == self.config.chunks_per_zone;

        tracing::debug!(
            "[Device]: Finished writing {}. Finish zone: {} with comparison {} == {} ",
            zone_index,
            finish_zone,
            cl.index + 1,
            self.config.chunks_per_zone
        );

        // println!("Finished writing to zone {} - {:?} - finish_zone={:?}", zone_index, cl, finish_zone);

        self.complete_write(zone_index, finish_zone)?;
        Ok(cl)
    }
}


impl Device for Zoned {
    /// Hold internal state to keep track of zone state
    fn append_with_eviction_bypass(&self, data: Bytes, is_eviction: bool) -> std::io::Result<ChunkLocation> {

        let zone_index: Zone = loop {
            match self.get_free_zone(is_eviction) {
                Ok(res) => {
                    break res;
                },
                Err(err) => {
                    if is_eviction {
                        // If eviction itself can't get a zone, we're truly stuck
                        tracing::error!("DEVICE: [Thread {:?}] Eviction failed to get free zone: {}", std::thread::current().id(), err);
                        return Err(err);
                    }
                    tracing::debug!("DEVICE: [Thread {:?}] Failed to get free zone: {}, triggering eviction", std::thread::current().id(), err);
                }
            };
            // Add a small delay to prevent eviction spam
            std::thread::sleep(std::time::Duration::from_millis(10));
            trigger_eviction(self.eviction_channel.clone())?;
        };

        assert_eq!(
            data.as_ptr() as u64 % self.nvme_config.logical_block_size,
            0
        );

        let start = std::time::Instant::now();
        let res = self.chunked_append(data, zone_index);
        METRICS.update_metric_histogram_latency("disk_write_latency_ms", start.elapsed(), MetricType::MsLatency);
        res
    }

    fn read(&self, location: ChunkLocation) -> std::io::Result<Bytes> {
        let (_header, data) = self.read_subset(location, 0, self.config.chunk_size_in_bytes)?;
        // When offset=0, data already includes header as first bytes, so just return data
        Ok(data)
    }

    fn read_subset(&self, location: ChunkLocation, offset: Byte, size: Byte) -> std::io::Result<(Bytes, Bytes)> {
        const HEADER_SIZE: Byte = 24; // 8 bytes key hash + 8 bytes offset + 8 bytes size

        // Validate LBA alignment
        if offset % self.nvme_config.logical_block_size != 0 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!(
                    "Offset {} must be aligned to logical block size {}",
                    offset, self.nvme_config.logical_block_size
                ),
            ));
        }
        if size % self.nvme_config.logical_block_size != 0 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!(
                    "Size {} must be aligned to logical block size {}",
                    size, self.nvme_config.logical_block_size
                ),
            ));
        }

        let slba = self.config.get_address_at(location.zone, location.index);

        // Header (always first 24 bytes for validation)
        let header_buffer_size = get_aligned_buffer_size(HEADER_SIZE, self.nvme_config.logical_block_size);
        let mut header_buffer: AVec<u8, RuntimeAlign> = AVec::with_capacity(
            self.nvme_config.logical_block_size as usize,
            header_buffer_size as usize
        );
        header_buffer.resize(header_buffer_size as usize, 0);

        tracing::trace!("Read subset header at slba = {} for {:?}", slba, location);

        let start = std::time::Instant::now();
        self.read_into_buffer(self.max_write_size, slba, &mut header_buffer, &self.nvme_config)?;
        let header = Bytes::from_owner(header_buffer).slice(0..HEADER_SIZE as usize);

        // Data (size bytes starting from offset position in chunk)
        let data_buffer_size = get_aligned_buffer_size(size, self.nvme_config.logical_block_size);
        let mut data_buffer: AVec<u8, RuntimeAlign> = AVec::with_capacity(
            self.nvme_config.logical_block_size as usize,
            data_buffer_size as usize
        );
        data_buffer.resize(data_buffer_size as usize, 0);

        // Calculate LBA offset for the data read
        let byte_offset = offset;
        let lba_offset = byte_offset / self.nvme_config.logical_block_size;
        let data_slba = slba + lba_offset;

        tracing::trace!("Read subset data at slba = {} for {:?}, offset={}, size={}", data_slba, location, offset, size);

        self.read_into_buffer(self.max_write_size, data_slba, &mut data_buffer, &self.nvme_config)?;
        METRICS.update_metric_histogram_latency("disk_read_latency_ms", start.elapsed(), MetricType::MsLatency);

        let data = Bytes::from_owner(data_buffer).slice(0..size as usize);

        Ok((header, data))
    }

    fn evict(self: Arc<Self>, cache: Arc<Cache>, writer_pool: Arc<WriterPool>, eviction_policy: Arc<Mutex<EvictionPolicyWrapper>>, always_evict: bool) -> io::Result<()> {
        let usage = self.get_use_percentage();
        METRICS.update_metric_gauge("usage_percentage", usage as f64);

        let targets = {
            let mut policy = eviction_policy.lock().unwrap();
            policy.get_evict_targets(true, always_evict)
        };

        match targets {
            EvictTarget::Chunk(chunk_locations, clean_locations) => {
                let clean_locations = clean_locations.unwrap();
                if chunk_locations.is_empty() {
                    tracing::debug!("[evict:Chunk] No chunks evicted");
                    return Ok(());
                }

                check_first_evict_bench();

                // Remove from map (invalidation)
                RUNTIME.block_on(cache.remove_entries(&chunk_locations))?;

                // Cleaning
                let self_clone = self.clone();
                for zone in clean_locations.iter() {

                    let cache_clone = cache.clone();
                    let self_clone = self_clone.clone();
                    let writer_pool = writer_pool.clone();

                    RUNTIME.block_on(
                            cache_clone.clean_zone_and_update_map(
                                zone.clone(),
                                // Reads all valid chunks in zone and returns buffer [(Chunk, Bytes)]
                                // which is the list of chunks that need to be written back
                                {
                                    let self_clone = self_clone.clone();
                                    |items: Vec<(CacheKey, ChunkLocation)>| {
                                        async move {
                                            let mut items = items;
                                            items.sort_by_key(|(_, loc)| loc.index);

                                            if items.is_empty() {
                                                return Ok(Vec::new());
                                            }

                                            // Batch reads to avoid overwhelming the device
                                            const BATCH_SIZE: usize = 16;
                                            let mut all_results = Vec::with_capacity(items.len());

                                            for chunk in items.chunks(BATCH_SIZE) {

                                                let futures: Vec<_> = chunk.iter().map(|(key, loc)| {
                                                    let self_clone = self_clone.clone();
                                                    let key = key.clone();
                                                    let loc = loc.clone();

                                                    tokio::task::spawn_blocking(move || {
                                                        tracing::trace!("Reading chunk at {:?}", loc);
                                                        self_clone.read(loc.clone()).map(|bytes| (key, bytes))
                                                    })
                                                }).collect();

                                                let batch_results: Result<Vec<_>, _> = futures::future::join_all(futures)
                                                    .await
                                                    .into_iter()
                                                    .map(|join_result| {
                                                        join_result.map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, format!("Task join error: {}", e)))?
                                                    })
                                                    .collect();

                                                all_results.extend(batch_results?);
                                            }

                                            Ok(all_results)
                                        }
                                    }
                                },

                                // After this point, the chunks that are invalid should have been removed, and the chunks that are valid should be in a waiting state.
                                {
                                    let writer_pool = writer_pool.clone();
                                    let self_clone = self_clone;

                                    // Writer callback
                                    |payloads: Vec<(CacheKey, bytes::Bytes)>| {
                                        async move {
                                            { // Return zones back to the zone list and reset the zone
                                                let _guard = self_clone.zone_append_lock[*zone as usize].write().unwrap();
                                                let (zone_mtx, cv) = &*self_clone.zones;
                                                let mut zones = zone_mtx.lock().unwrap();
                                                zones.reset_zone(*zone, &*self_clone)?;
                                                cv.notify_all();
                                            } // Drop the mutex, so we don't have to put it in an await

                                            // Use prioritized batch write for eviction
                                            let keys: Vec<_> = payloads.iter().map(|(key, _)| key.clone()).collect();
                                            let data_vec: Vec<_> = payloads.iter().map(|(_, data)| data.clone()).collect();

                                            // Used to verify no RACE, TODO: Remove!
                                            // tokio::time::sleep(Duration::from_secs(5)).await;

                                            let (batch_tx, batch_rx) = flume::bounded(1);

                                            let batch_request = BatchWriteRequest {
                                                data: data_vec,
                                                responder: batch_tx,
                                            };

                                            writer_pool.send_priority_batch(batch_request).await?;

                                            let batch_response = batch_rx.recv_async().await.map_err(|e| {
                                                io::Error::new(io::ErrorKind::Other,
                                                    format!("failed to receive batch write response: {}", e))
                                            })?;

                                            // Convert batch response back to individual results
                                            let write_results: Result<Vec<(CacheKey, ChunkLocation, bytes::Bytes)>, io::Error> =
                                                keys.into_iter()
                                                    .zip(batch_response.locations.into_iter())
                                                    .zip(payloads.into_iter())
                                                    .map(|((key, location_result), (_, data))| {
                                                        location_result.map(|loc| (key, loc, data))
                                                    })
                                                    .collect();

                                            let write_results = write_results?;

                                            Ok(write_results) // Vec<(Chunk, ChunkLocation)>
                                        }
                                    }
                                },
                                writer_pool.clone(),
                            )
                        )?;
                }
                Ok(())
            }
            EvictTarget::Zone(zones_to_evict) => {
                if zones_to_evict.is_empty() {
                    return Ok(());
                }

                check_first_evict_bench();

                RUNTIME.block_on(cache.remove_zones(&zones_to_evict))?;

                let (zone_mtx, _) = &*self.zones;
                let mut zones = zone_mtx.lock().unwrap();
                zones.reset_zones(&zones_to_evict, &*self)?;

                Ok(())
            }
        }
    }

    fn get_num_zones(&self) -> Zone {
        self.config.num_zones
    }

    fn get_chunks_per_zone(&self) -> Chunk {
        self.config.chunks_per_zone
    }

    fn get_block_size(&self) -> Byte {
        self.nvme_config.logical_block_size
    }

    fn get_use_percentage(&self) -> f32 {
        let (zones, _) = &*self.zones;
        let zones = zones.lock().unwrap();
        let total_chunks = (self.config.chunks_per_zone * self.config.num_zones) as f32;
        let available_chunks = zones.get_num_available_chunks() as f32;
        (total_chunks - available_chunks) / total_chunks
    }

    fn reset(&self) -> io::Result<()> {
        reset_zone(&self.nvme_config, &self.config, PerformOn::AllZones).map_err(|err| {
            std::io::Error::new(
                ErrorKind::Other,
                err.add_context(format!("Performed finish zone on all zones"))
                    .to_string(),
            )
        })
    }

    fn reset_zone(&self, zone_id: Zone) -> io::Result<()> {
        reset_zone(&self.nvme_config, &self.config, PerformOn::Zone(zone_id)).map_err(|err| {
            std::io::Error::new(
                ErrorKind::Other,
                err.add_context(format!("Performed reset zone on {}", zone_id))
                    .to_string(),
            )
        })
    }

    fn close_zone(&self, zone_id: Zone) -> io::Result<()> {
        close_zone(&self.nvme_config, &self.config, PerformOn::Zone(zone_id)).map_err(|err| {
            std::io::Error::new(
                ErrorKind::Other,
                err.add_context(format!("Performed close zone on {}", zone_id))
                    .add_context(format!("Zone state is: {:?}", {
                        let val = nvme::info::report_zones_all(
                            self.nvme_config.fd,
                            self.nvme_config.nsid,
                        )
                        .unwrap()
                        .1;
                        val[zone_id as usize].clone()
                    }))
                    .to_string(),
            )
        })
    }

    fn finish_zone(&self, zone_id: Zone) -> io::Result<()> {
        finish_zone(&self.nvme_config, &self.config, PerformOn::Zone(zone_id)).map_err(|err| {
            std::io::Error::new(
                ErrorKind::Other,
                err.add_context(format!("Performed finish zone on {}", zone_id))
                    .add_context(format!("Zone state is: {:?}", {
                        nvme::info::report_zones_all(self.nvme_config.fd, self.nvme_config.nsid)
                            .unwrap()
                            .1
                    }))
                    .to_string(),
            )
        })
    }

    fn get_fd(&self) -> RawFd {
        self.nvme_config.fd
    }
    fn get_nsid(&self) -> u32 {
        self.nvme_config.nsid
    }

    #[cfg(feature = "eviction-metrics")]
    fn get_eviction_metrics(&self) -> Option<Arc<crate::eviction_metrics::EvictionMetrics>> {
        self.eviction_metrics.clone()
    }
}

impl BlockInterface {
    fn new(
        device: &str,
        chunk_size: Byte,
        block_zone_capacity: Byte,
        eviction_channel: Sender<EvictorMessage>,
        max_write_size: Byte,
        max_zones: Option<u64>,
    ) -> io::Result<Self> {
        let nvme_config = match nvme_get_info(device) {
            Ok(config) => config,
            Err(err) => return Err(err.try_into().unwrap()),
        };

        assert!(
            block_zone_capacity >= chunk_size,
            "Block zone capacity {} must be at least chunk size {}",
            block_zone_capacity,
            chunk_size
        );

        // Num_zones
        let num_zones = nvme_config.total_size_in_bytes / block_zone_capacity;

        // Apply max_zones restriction if specified
        let restricted_num_zones = if let Some(max_zones) = max_zones {
            if max_zones > num_zones {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    format!("max_zones ({}) cannot be larger than the maximum number of zones available on the device ({})", max_zones, num_zones)
                ));
            }
            max_zones
        } else {
            num_zones
        };

        // Chunks per zone
        let chunks_per_zone = block_zone_capacity / chunk_size;

        let chunk_size_in_lbas = nvme_config.byte_address_to_lba(chunk_size);

        #[cfg(feature = "eviction-metrics")]
        let eviction_metrics = Some(crate::eviction_metrics::EvictionMetrics::new(restricted_num_zones));

        Ok(Self {
            nvme_config,
            state: Arc::new(Mutex::new(BlockDeviceState::new(
                restricted_num_zones,
                chunks_per_zone,
                chunk_size,
            ))),
            chunk_size_in_bytes: chunk_size,
            chunk_size_in_lbas,
            chunks_per_zone,
            num_zones: restricted_num_zones,
            eviction_channel,
            max_write_size,
            #[cfg(feature = "eviction-metrics")]
            eviction_metrics,
        })
    }

    fn zone_size(&self) -> LogicalBlock {
        self.chunks_per_zone as u64 * self.chunk_size_in_lbas
    }

    fn get_lba_at(&self, location: &ChunkLocation) -> LogicalBlock {
        get_lba_at(
            location.zone,
            location.index,
            self.zone_size(),
            self.chunk_size_in_lbas,
        )
    }

    fn chunked_append(&self, data: Bytes, write_addr: LogicalBlock) -> io::Result<()> {
        let total_sz = data.len() as Byte;
        let write_sz = total_sz.min(self.max_write_size);

        assert!(
            total_sz % self.nvme_config.logical_block_size == 0,
            "Unaligned write: {} bytes (LBA size {})",
            total_sz,
            self.nvme_config.logical_block_size
        );

        let mut byte_ind = 0;
        let mut write_addr_lba = write_addr;

        while byte_ind < total_sz {
            let end = (byte_ind + write_sz).min(total_sz);
            if let Err(err) = nvme::ops::write(
                write_addr_lba,
                self.nvme_config.fd,
                0,
                self.nvme_config.nsid,
                self.nvme_config.logical_block_size,
                &data[byte_ind as usize..end as usize],
            ) {
                return Err(err.try_into().unwrap());
            }
            let bytes_written = end - byte_ind;
            // println!("Wrote {} bytes starting at {} from lba write_addr_lba {}, bytes ({}..{})", bytes_written, write_addr, write_addr_lba, byte_ind, end);
            write_addr_lba += bytes_written / self.nvme_config.logical_block_size;
            byte_ind += bytes_written;
        }

        Ok(())
    }
}

impl Device for BlockInterface {
    /// Hold internal state to keep track of "ssd" zone state
    fn append_with_eviction_bypass(&self, data: Bytes, _is_eviction: bool) -> std::io::Result<ChunkLocation> {
        // Block devices don't need eviction bypass logic
        let sz = data.len() as u64;
        let mtx = self.state.clone();

        let chunk_location = loop {
            let mut state = mtx.lock().unwrap();
            match state.active_zones.remove_chunk_location() {
                Ok(location) => break location,
                Err(_) => {
                    tracing::trace!(
                        "[append] Failed to allocate chunk: no available space in active zones"
                    );
                }
            };
            drop(state);

            trigger_eviction(self.eviction_channel.clone())?;
        };

        assert_eq!(data.len() % self.nvme_config.logical_block_size as usize, 0);

        let write_addr = self.get_lba_at(&chunk_location);

        // println!("[append] writing chunk to {} bytes at addr {}", chunk_location.zone, write_addr);

        let start = std::time::Instant::now();
        self.chunked_append(data, write_addr)?;
        METRICS.update_metric_histogram_latency("disk_write_latency_ms", start.elapsed(), MetricType::MsLatency);
        Ok(chunk_location)
    }

    fn read(&self, location: ChunkLocation) -> std::io::Result<Bytes> {
        let (_header, data) = self.read_subset(location, 0, self.chunk_size_in_bytes)?;
        // When offset=0, data already includes header as first bytes, so just return data
        Ok(data)
    }

    fn read_subset(&self, location: ChunkLocation, offset: Byte, size: Byte) -> std::io::Result<(Bytes, Bytes)> {
        const HEADER_SIZE: Byte = 24; // 8 bytes key hash + 8 bytes offset + 8 bytes size

        // Validate LBA alignment
        if offset % self.nvme_config.logical_block_size != 0 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!(
                    "Offset {} must be aligned to logical block size {}",
                    offset, self.nvme_config.logical_block_size
                ),
            ));
        }
        if size % self.nvme_config.logical_block_size != 0 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                format!(
                    "Size {} must be aligned to logical block size {}",
                    size, self.nvme_config.logical_block_size
                ),
            ));
        }

        let base_lba = self.get_lba_at(&location);

        // Header (always first 24 bytes for validation)
        let header_buffer_size = get_aligned_buffer_size(HEADER_SIZE, self.nvme_config.logical_block_size);
        let mut header_buffer = vec![0u8; header_buffer_size as usize];

        let start = std::time::Instant::now();
        self.read_into_buffer(
            self.max_write_size,
            base_lba,
            &mut header_buffer,
            &self.nvme_config,
        )?;
        let header = Bytes::copy_from_slice(&header_buffer[0..HEADER_SIZE as usize]);

        // Data buffer - size is already validated to be aligned
        let mut data_buffer = vec![0u8; size as usize];

        // Calculate LBA offset for the data read
        let byte_offset = offset;
        let lba_offset = byte_offset / self.nvme_config.logical_block_size;
        let data_lba = base_lba + lba_offset;

        self.read_into_buffer(
            self.max_write_size,
            data_lba,
            &mut data_buffer,
            &self.nvme_config,
        )?;
        METRICS.update_metric_histogram_latency("disk_read_latency_ms", start.elapsed(), MetricType::MsLatency);

        let data = Bytes::from(data_buffer);

        Ok((header, data))
    }

    fn evict(self: Arc<Self>, cache: Arc<Cache>, _writer_pool: Arc<WriterPool>, eviction_policy: Arc<Mutex<EvictionPolicyWrapper>>, always_evict: bool) -> io::Result<()> {
        let usage = self.get_use_percentage();
        METRICS.update_metric_gauge("usage_percentage", usage as f64);

        let targets = {
            let mut policy = eviction_policy.lock().unwrap();
            policy.get_evict_targets(false, always_evict)
        };

        match targets {
            EvictTarget::Chunk(chunk_locations, _) => {

                if chunk_locations.is_empty() {
                    return Ok(());
                }

                check_first_evict_bench();

                tracing::debug!("[evict:Chunk] Evicting chunks {:?}", chunk_locations);

                RUNTIME.block_on(cache.remove_entries(&chunk_locations))?;
                let state_mtx = Arc::clone(&self.state);
                let mut state = state_mtx.lock().unwrap();
                for c in chunk_locations {
                    state.active_zones.return_chunk_location(&c);
                }

                Ok(())
            }
            EvictTarget::Zone(locations) => {
                if locations.is_empty() {
                    return Ok(());
                }

                check_first_evict_bench();

                tracing::debug!("[evict:Zone] Evicting zones {:?}", locations);
                RUNTIME.block_on(cache.remove_zones(&locations))?;
                let state_mtx = Arc::clone(&self.state);
                let mut state = state_mtx.lock().unwrap();
                state.active_zones.reset_zones(&locations, &*self)?;
                Ok(())
            }
        }
    }

    fn get_num_zones(&self) -> Zone {
        self.num_zones
    }

    fn get_chunks_per_zone(&self) -> Chunk {
        self.chunks_per_zone
    }

    fn get_block_size(&self) -> Byte {
        self.nvme_config.logical_block_size
    }

    fn get_use_percentage(&self) -> f32 {
        let state = &*self.state.lock().unwrap();
        let total_chunks = (self.chunks_per_zone * self.num_zones) as f32;
        let available_chunks = state.active_zones.get_num_available_chunks() as f32;
        (total_chunks - available_chunks) / total_chunks
    }

    fn reset(&self) -> io::Result<()> {
        Ok(())
    }

    fn reset_zone(&self, _zone_id: Zone) -> io::Result<()> {
        Ok(())
    }

    fn close_zone(&self, _zone_id: Zone) -> io::Result<()> {
        Ok(())
    }
    fn finish_zone(&self, _zone_id: Zone) -> io::Result<()> {
        Ok(())
    }

    fn get_fd(&self) -> RawFd {
        self.nvme_config.fd
    }
    fn get_nsid(&self) -> u32 {
        self.nvme_config.nsid
    }

    #[cfg(feature = "eviction-metrics")]
    fn get_eviction_metrics(&self) -> Option<Arc<crate::eviction_metrics::EvictionMetrics>> {
        self.eviction_metrics.clone()
    }
}

fn get_aligned_buffer_size(buffer_size: Byte, block_size: Byte) -> Byte {
    if buffer_size.rem_euclid(block_size) != 0 {
        buffer_size + (block_size - buffer_size.rem_euclid(block_size))
    } else {
        buffer_size
    }
}

