use crate::cache::Cache;
use crate::cache::bucket::ChunkLocation;
use crate::eviction::{EvictTarget, EvictorMessage};
use crate::server::RUNTIME;
use crate::zone_state::zone_list::{ZoneList, ZoneObtainFailure};
use bytes::Bytes;
use flume::Sender;
use nvme::info::{get_active_zones, get_lba_at, is_zoned_device, nvme_get_info, report_zones_all};
use nvme::ops::{close_zone, reset_zone, zns_append, finish_zone};
use nvme::types::{Byte, Chunk, LogicalBlock, NVMeConfig, NVMeError, PerformOn, ZNSConfig, Zone, ZoneState};
use std::io::{self, ErrorKind};
use std::os::fd::RawFd;
use std::sync::{Arc, Condvar, Mutex, MutexGuard};

pub struct Zoned {
    nvme_config: NVMeConfig,
    config: ZNSConfig,
    zones: Arc<(Mutex<ZoneList>, Condvar)>,
    eviction_channel: Sender<EvictorMessage>,
    max_write_size: Byte,
    zone_append_lock: Vec<Mutex<()>>,
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
    chunk_size: Byte,
    chunks_per_zone: Chunk,
    num_zones: Zone,
    state: Arc<Mutex<BlockDeviceState>>,
    eviction_channel: Sender<EvictorMessage>,
    max_write_size: Byte
}

pub trait Device: Send + Sync {
    fn append(&self, data: Bytes) -> std::io::Result<ChunkLocation>;

    /// This is expected to remove elements from the cache as well
    fn evict(&self, locations: EvictTarget, cache: Arc<Cache>) -> io::Result<()>;

    fn read(&self, location: ChunkLocation) -> std::io::Result<Bytes>;

    fn get_num_zones(&self) -> Zone;

    fn get_chunks_per_zone(&self) -> Chunk;
    fn get_block_size(&self) -> Byte;
    fn get_use_percentage(&self) -> f32;

    fn reset(&self) -> io::Result<()>;

    fn reset_zone(&self, zone_id: Zone) -> io::Result<()>;

    fn close_zone(&self, zone_id: Zone) -> io::Result<()>;

    fn finish_zone(&self, zone_id: Zone) -> io::Result<()>;

    fn read_into_buffer(&self, max_write_size: Byte, lba_loc: LogicalBlock, read_buffer: &mut [u8], nvme_config: &NVMeConfig) -> io::Result<()> {
        let total_sz = read_buffer.len();
        let write_sz = total_sz.min(max_write_size as usize);
        let mut byte_ind = 0;

        let mut lba_loc = lba_loc;

        while byte_ind < total_sz {
            let end = (byte_ind + write_sz).min(read_buffer.len());
            let chunk_size = end - byte_ind;
            let lbas_read = chunk_size as u64 / nvme_config.logical_block_size;

            assert_eq!(chunk_size % nvme_config.logical_block_size as usize, 0, "Unaligned read size");

            // println!("Reading {} lbas, lba loc = {}, into ({}..{})", lbas_read, lba_loc, byte_ind, end);

            if let Err(err) = nvme::ops::read(
                nvme_config,
                lba_loc,
                &mut read_buffer[byte_ind..end],
            ) {
                return Err(err.try_into().unwrap());
            }

            // println!("Read {} lbas, lba loc = {}, into ({}..{})", lbas_read, lba_loc, byte_ind, end);

            byte_ind += chunk_size;
            lba_loc += lbas_read;
        }

        Ok(())
    }

    fn get_fd(&self) -> RawFd;
    fn get_nsid(&self) -> u32;
}

pub fn get_device(
    device: &str,
    chunk_size: Byte,
    block_zone_capacity: Byte,
    eviction_channel: Sender<EvictorMessage>,
    max_write_size: Byte,
) -> io::Result<Arc<dyn Device>> {
    let is_zoned = is_zoned_device(device)?;
    if is_zoned {
        Ok(Arc::new(Zoned::new(device, chunk_size, eviction_channel, max_write_size)?))
    } else {
        Ok(Arc::new(BlockInterface::new(
            device,
            chunk_size,
            block_zone_capacity,
            eviction_channel,
            max_write_size
        )?))
    }
}

fn trigger_eviction(eviction_channel: Sender<EvictorMessage>) -> io::Result<()> {
    let (resp_tx, resp_rx) = flume::bounded(1);
    if let Err(e) = eviction_channel.send(EvictorMessage { sender: resp_tx }) {
        eprintln!("[append] Failed to send eviction message: {}", e);
        return Err(std::io::Error::new(
            std::io::ErrorKind::Other,
            "Failed to send eviction message",
        ));
    };

    if let Err(e) = resp_rx.recv() {
        eprintln!("[append] Failed to receive eviction message: {}", e);
    }

    Ok(())
}

impl Zoned {
    fn compact_zone(
        &self,
        zone_to_compact: Zone,
        chunks_to_keep: &[ChunkLocation],
        buffer: &mut [u8],
    ) -> io::Result<Vec<ChunkLocation>> {
        let mut new_locations = Vec::with_capacity(chunks_to_keep.len());
        for chunk in chunks_to_keep {
            let starting_byte_loc: Byte = self.config.chunks_to_bytes(&self.nvme_config, chunk.index);
            let ending_byte_loc: Byte = self.config.chunks_to_bytes(&self.nvme_config, chunk.index + 1);
            let new_idx = zns_append(
                &self.nvme_config,
                &self.config,
                zone_to_compact,
                &mut buffer[starting_byte_loc as usize..ending_byte_loc as usize],
            )
            .map_err(|err| std::io::Error::new(ErrorKind::Other, err.to_string()))?;
            new_locations.push(ChunkLocation::new(zone_to_compact, new_idx));
        }
        Ok(new_locations)
    }

    /// Wrapper for ZoneList, handles mutex and notification
    fn get_free_zone(&self) -> io::Result<Zone> {
        let (mtx, wait_notify) = &*self.zones;
        let mut zone_list = mtx.lock().unwrap();

        let active_zones = get_active_zones(self.nvme_config.fd, self.nvme_config.nsid).unwrap();
        println!("active zones: {}", active_zones);
        // assert!(zone_list.get_open_zones() == active_zones, "{} vs {}", zone_list.get_open_zones(), active_zones);
        assert!(active_zones <= self.config.max_active_resources as usize);

        match zone_list.remove() {
            Ok(zone_idx) => Ok(zone_idx),
            Err(error) => match error {
                ZoneObtainFailure::EvictNow => {
                    Err(io::Error::new(ErrorKind::StorageFull, "Cache is full"))
                }
                ZoneObtainFailure::Wait => loop {
                    zone_list = wait_notify.wait(zone_list).unwrap();
                    match zone_list.remove() {
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
        let active_zones = get_active_zones(self.nvme_config.fd, self.nvme_config.nsid).unwrap();
        // assert!(zone_list.get_open_zones() == active_zones, "{} vs {}", zone_list.get_open_zones(), active_zones);
        assert!(active_zones <= self.config.max_active_resources as usize);

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
    ) -> io::Result<Self> {
        let nvme_config = match nvme::info::nvme_get_info(device) {
            Ok(config) => config,
            Err(err) => return Err(err.try_into().unwrap()),
        };

        match nvme::info::zns_get_info(&nvme_config) {
            Ok(mut config) => {
                println!("ZNSConfig: {:?}", config); 
                
                let chunk_size_in_logical_blocks: LogicalBlock =
                    nvme_config.byte_address_to_lba(chunk_size);
                config.chunks_per_zone = config.zone_cap / chunk_size_in_logical_blocks;
                config.chunk_size = chunk_size;
                let num_zones: Zone = config.num_zones;
                let zone_list = ZoneList::new(
                    num_zones,
                    config.chunks_per_zone,
                    config.max_active_resources as usize,
                );

                let zone_append_lock: Vec<Mutex<()>> = (0..num_zones)
                    .map(|_| Mutex::new(()))
                    .collect();

                Ok(Self {
                    nvme_config,
                    config,
                    eviction_channel,
                    zones: Arc::new((Mutex::new(zone_list), Condvar::new())),
                    max_write_size,
                    zone_append_lock
                })
            }
            Err(err) => Err(err.try_into().unwrap()),
        }
    }

    fn lba_to_chunk_index(&self, lba: LogicalBlock, zone_index: Zone) -> u64 {
        // Make sure LBA is zone-relative
        let zone_start_lba = self.config.get_starting_lba(zone_index);
        let rel_lba: LogicalBlock = lba.checked_sub(zone_start_lba)
            .expect("LBA was not inside the specified zone");

        // Get chunk index
        rel_lba / self.config.chunk_size
    }

    fn chunked_append(&self, data: Bytes, zone_index: Zone) -> io::Result<ChunkLocation> {
        println!("Chunk appending to zone {}", zone_index);

        let total_sz = data.len() as Byte;
        let write_sz = total_sz.min(self.max_write_size);

        println!("[Device]: Total size = {}, Write size = {}, max_write_size = {}", total_sz, write_sz, self.max_write_size);

        // Only locks if needed
        let _maybe_guard: Option<MutexGuard<'_, ()>> = if total_sz > self.max_write_size {
            Some(self.zone_append_lock[zone_index as usize].lock().unwrap())
        } else {
            None
        };

        // Sequentially write looped

        let mut byte_ind: Byte= 0;

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
                    println!("[append] wrote to lba {} at zone {} from bytes ({}..{})", lba, zone_index, byte_ind, end);
                    let lbas_written = self.nvme_config.byte_address_to_lba(end - byte_ind);
                    if first_chunk.is_none() {
                        let chunk = self.lba_to_chunk_index(lba, zone_index);
                        first_chunk = Some(ChunkLocation::new(zone_index, chunk));
                        // println!("Chunk {:?}", first_chunk);
                        // println!("lba={}, self.config.chunk_size = {}, lba / write_sz = {}", lba, self.config.chunk_size, lba/write_sz as u64);
                    }
                    if let Some(last_lba) = last_lba {
                        let lba_check = last_lba + lbas_written;
                        assert_eq!(lba_check, lba, "lbas are not contiguous=({}, {})", lba_check, lba);
                    }
                    last_lba = Some(lba);
                }
                Err(err) => {
                    return Err(err
                        .add_context(format!("Write failed at zone {}", zone_index))
                        .add_context(format!("Zone state: {:#?}", {
                            let (_nz, state) = report_zones_all(self.nvme_config.fd, self.nvme_config.nsid).unwrap();
                            state.iter().map(|state|{
                                state.zone_state.clone()
                            }).collect::<Vec<ZoneState>>()
                        }))
                        .add_context(format!("Zone list state:\n{:#?}", self.zones.0.lock().unwrap()))
                        .try_into().unwrap());

                    // return match self.complete_write(zone_index, false) {
                    //     Ok(()) => Err(err
                    //         .add_context(format!("Write failed at zone {}\n", zone_index))
                    //         .try_into()
                    //         .unwrap()),
                    //     Err(err2) => Err(err
                    //         .add_context(format!("Write failed at zone {}", zone_index))
                    //         .add_context(format!("Zone state: {:#?}", {
                    //             let (_nz, state) = report_zones_all(self.nvme_config.fd, self.nvme_config.nsid).unwrap();
                    //             state.iter().map(|state|{
                    //                 state.zone_state.clone()
                    //             }).collect::<Vec<ZoneState>>()
                    //         }))
                    //         .add_context(format!("Zone list state:\n{:#?}", self.zones.0.lock().unwrap()))
                    //         .add_context(format!("Additional failure while trying to handle error: {}\n", err2.to_string()))
                    //         .try_into()
                    //         .unwrap()),
                    // };
                }
            }
            byte_ind += write_sz;
        }

        let cl = first_chunk.unwrap();

        let finish_zone = cl.index+1 == self.config.chunks_per_zone;

        println!("[Device]: Finished writing {}. Finish zone: {} with comparison {} == {} ", zone_index, finish_zone, cl.index+1, self.config.chunks_per_zone);

        // println!("Finished writing to zone {} - {:?} - finish_zone={:?}", zone_index, cl, finish_zone);

        self.complete_write(zone_index, finish_zone)?;
        Ok(cl)
    }
}

impl Device for Zoned {
    /// Hold internal state to keep track of zone state
    fn append(&self, data: Bytes) -> std::io::Result<ChunkLocation> {
        println!("Appending");

        let zone_index: Zone = loop {
            match self.get_free_zone() {
                Ok(res) => break res,
                Err(err) => {
                    eprintln!("[append] Failed to get free zone: {}", err);
                }
            };
            trigger_eviction(self.eviction_channel.clone())?;
        };
        // Note: this performs a copy every time because we need to
        // pass in a mutable vector to libnvme
        assert_eq!(
            data.as_ptr() as u64 % self.nvme_config.logical_block_size,
            0
        );

        self.chunked_append(data, zone_index)
    }

    fn read(&self, location: ChunkLocation) -> std::io::Result<Bytes> {
        let mut data = vec![0u8; self.config.chunk_size as usize];
        let slba = self.config.get_address_at(location.zone, location.index);
        println!("Read slba = {} for {:?}", slba, location);
        self.read_into_buffer(
            self.max_write_size,
            slba,
            &mut data,
            &self.nvme_config
        )?;

        Ok(Bytes::from(data))
    }

    fn evict(&self, locations: EvictTarget, cache: Arc<Cache>) -> io::Result<()> {
        println!("Current device usage is at: {}", self.get_use_percentage());

        match locations {
            EvictTarget::Chunk(mut chunk_locations) => {
                // TODO: Check that the units match

                // Check all zones are the same, this isn't a
                // restriction but it makes implementation
                // easier. This can be changed later
                assert!(
                    chunk_locations
                        .iter()
                        .all(|loc| loc.zone == chunk_locations[0].zone)
                );
                let zone_to_evict = chunk_locations[0].zone;

                // TODO: Does this need to be aligned?
                let mut read_buf = vec![
                    0_u8;
                    (self.config.zone_cap * self.nvme_config.logical_block_size)
                        as usize
                ];
                self.read_into_buffer(
                    self.max_write_size,
                    self.config.get_starting_lba(zone_to_evict),
                    &mut read_buf,
                    &self.nvme_config
                )?;
                let zones_to_reset = [chunk_locations[0].zone];

                chunk_locations.sort_by(|cl1, cl2| cl1.index.cmp(&cl2.index));

                let mut to_keep: Vec<ChunkLocation> = Vec::new();
                // Iterates through the chunks to discard, skipping
                // them and adding the chunks between instead.
                chunk_locations.iter().fold(0, |prev, cur| {
                    to_keep.extend(
                        (prev..cur.index)
                            .map(|chunk_idx| ChunkLocation::new(zone_to_evict, chunk_idx)),
                    );
                    cur.index + 1
                });

                RUNTIME.block_on(cache.remove_zones_and_update_entries(
                    &zones_to_reset,
                    &to_keep,
                    || Ok(self.compact_zone(zone_to_evict, &to_keep, &mut read_buf)?),
                ))?;

                let (zone_mtx, _) = &*self.zones;
                let mut zones = zone_mtx.lock().unwrap();
                // TODO: reset zones for device
                zones.reset_zone_with_capacity(
                    zone_to_evict,
                    self.get_chunks_per_zone() - to_keep.len() as Chunk,
                );
                Ok(())
            }
            EvictTarget::Zone(zones_to_evict) => {
                RUNTIME.block_on(cache.remove_zones(&zones_to_evict))?;

                let (zone_mtx, _) = &*self.zones;
                let mut zones = zone_mtx.lock().unwrap();
                zones.reset_zones(&zones_to_evict, self)?;

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
        reset_zone(&self.nvme_config, &self.config, PerformOn::AllZones)
            .map_err(|err| {
                std::io::Error::new(ErrorKind::Other, err
                    .add_context(format!("Performed finish zone on all zones"))
                    .to_string())
            })
    }

    fn reset_zone(&self, zone_id: Zone) -> io::Result<()> {
        reset_zone(
            &self.nvme_config,
            &self.config,
            PerformOn::Zone(zone_id),
        )
        .map_err(|err| {
                std::io::Error::new(ErrorKind::Other, err
                    .add_context(format!("Performed reset zone on {}", zone_id))
                    .to_string())
            })
    }

    fn close_zone(&self, zone_id: Zone) -> io::Result<()> {
        close_zone(
            &self.nvme_config,
            &self.config,
            PerformOn::Zone(zone_id),
        )
        .map_err(|err| {
                std::io::Error::new(ErrorKind::Other, err
                    .add_context(format!("Performed close zone on {}", zone_id))
                    .add_context(format!("Zone state is: {:?}", {
                        let val = nvme::info::report_zones_all(self.nvme_config.fd, self.nvme_config.nsid).unwrap().1;
                        val[zone_id as usize].clone()
                    }))
                    .to_string())
            })
    }

    fn finish_zone(&self, zone_id: Zone) -> io::Result<()> {
        finish_zone(
            &self.nvme_config,
            &self.config,
            PerformOn::Zone(zone_id),
        )
            .map_err(|err| {
                std::io::Error::new(ErrorKind::Other, err
                    .add_context(format!("Performed finish zone on {}", zone_id))
                    .add_context(format!("Zone state is: {:?}", {
                        nvme::info::report_zones_all(self.nvme_config.fd, self.nvme_config.nsid).unwrap().1
                    }))
                    .to_string())
            })
    }

    fn get_fd(&self) -> RawFd {
        self.nvme_config.fd
    }
    fn get_nsid(&self) -> u32 {
        self.nvme_config.nsid
    }
}

impl BlockInterface {
    fn new(
        device: &str,
        chunk_size: Byte,
        block_zone_capacity: Byte,
        eviction_channel: Sender<EvictorMessage>,
        max_write_size: Byte
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
        // Chunks per zone
        let chunks_per_zone = block_zone_capacity / chunk_size;

        Ok(Self {
            nvme_config,
            state: Arc::new(Mutex::new(BlockDeviceState::new(
                num_zones,
                chunks_per_zone,
                chunk_size,
            ))),
            chunk_size,
            chunks_per_zone,
            num_zones,
            eviction_channel,
            max_write_size
        })
    }

    fn chunk_lba_size(&self) -> LogicalBlock {
        self.nvme_config.byte_address_to_lba(self.chunk_size)
    }

    fn zone_size(&self) -> LogicalBlock {
        self.chunks_per_zone as u64 * self.chunk_lba_size()
    }

    fn get_lba_at(&self, location: &ChunkLocation) -> LogicalBlock {
        get_lba_at(
            location.zone,
            location.index,
            self.zone_size(),
            self.chunk_lba_size(),
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

        let mut byte_ind= 0;
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
            let bytes_written = end-byte_ind;
            // println!("Wrote {} bytes starting at {} from lba write_addr_lba {}, bytes ({}..{})", bytes_written, write_addr, write_addr_lba, byte_ind, end);
            write_addr_lba += bytes_written / self.nvme_config.logical_block_size;
            byte_ind += bytes_written;
        }

        Ok(())
    }
}

impl Device for BlockInterface {

    /// Hold internal state to keep track of "ssd" zone state
    fn append(&self, data: Bytes) -> std::io::Result<ChunkLocation> {
        println!("Appending");
        let mtx = self.state.clone();

        let chunk_location = loop {
            let mut state = mtx.lock().unwrap();
            match state.active_zones.remove_chunk_location() {
                Ok(location) => break location,
                Err(_) => {
                    eprintln!(
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

        self.chunked_append(data, write_addr)?;

        Ok(chunk_location)
    }

    fn read(&self, location: ChunkLocation) -> std::io::Result<Bytes> {
        let mut data = vec![0u8; self.chunk_size as usize];

        let write_addr = self.get_lba_at(&location);

        self.read_into_buffer(
            self.max_write_size,
            write_addr,
            &mut data,
            &self.nvme_config
        )?;
        Ok(Bytes::from(data))
    }

    fn evict(&self, locations: EvictTarget, cache: Arc<Cache>) -> io::Result<()> {
        match locations {
            EvictTarget::Chunk(chunk_locations) => {
                // TODO: Check units
                
                if chunk_locations.is_empty() {
                    println!("[evict:Chunk] No zones to evict");
                    return Ok(());
                }
                println!("[evict:Chunk] Evicting zones {:?}", chunk_locations);

                RUNTIME.block_on(cache.remove_entries(&chunk_locations))?;
                let state_mtx = Arc::clone(&self.state);
                let _state = state_mtx.lock().unwrap();
                // Need to change the block interface bookkeeping so
                // that it can keep track of the list of empty chunks
                todo!();
            }
            EvictTarget::Zone(locations) => {
                if locations.is_empty() {
                    println!("[evict:Zone] No zones to evict");
                    return Ok(());
                }
                println!("[evict:Zone] Evicting zones {:?}", locations);
                let rt = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()?;
                rt.block_on(cache.remove_zones(&locations))?;
                let state_mtx = Arc::clone(&self.state);
                let mut state = state_mtx.lock().unwrap();
                state.active_zones.reset_zones(&locations, self)?;
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
    fn finish_zone(&self, _zone_id: Zone) -> io::Result<()> { Ok(()) }

    fn get_fd(&self) -> RawFd {
        self.nvme_config.fd
    }
    fn get_nsid(&self) -> u32 {
        self.nvme_config.nsid
    }
}
