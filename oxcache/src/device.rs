use crate::cache::Cache;
use crate::cache::bucket::ChunkLocation;
use crate::eviction::{EvictTarget, EvictorMessage};
use crate::server::RUNTIME;
use crate::zone_state::zone_list::{ZoneList, ZoneObtainFailure};
use bytes::Bytes;
use nvme::info::{get_address_at, is_zoned_device, nvme_get_info};
use nvme::ops::{reset_zone, zns_append, zns_read, close_zone};
use nvme::types::{NVMeConfig, PerformOn, ZNSConfig};
use std::collections::VecDeque;
use std::io::{self, ErrorKind};
use std::sync::{Arc, Condvar, Mutex};
use flume::Sender;

pub struct Zoned {
    nvme_config: NVMeConfig,
    config: ZNSConfig,
    zones: Arc<(Mutex<ZoneList>, Condvar)>,
    eviction_channel: Sender<EvictorMessage>,
}

// Information about each zone
#[derive(Clone)]
pub struct BlockZoneInfo {
    _write_pointer: u64,
}

pub struct BlockDeviceState {
    _zones: Vec<BlockZoneInfo>,
    active_zones: ZoneList,
    _chunk_size: usize,
}

impl BlockDeviceState {
    fn new(num_zones: usize, chunks_per_zone: usize, chunk_size: usize) -> Self {
        let zones = vec![BlockZoneInfo { _write_pointer: 0 }; num_zones];
        Self {
            _zones: zones,
            active_zones: ZoneList::new(num_zones, chunks_per_zone, num_zones),
            _chunk_size: chunk_size,
        }
    }
}

pub struct BlockInterface {
    nvme_config: NVMeConfig,
    chunk_size: usize,
    chunks_per_zone: usize,
    num_zones: usize,
    state: Arc<Mutex<BlockDeviceState>>,
    eviction_channel: Sender<EvictorMessage>,
}

pub trait Device: Send + Sync {
    fn append(&self, data: Bytes) -> std::io::Result<ChunkLocation>;

    fn read_into_buffer(&self, location: ChunkLocation, read_buffer: &mut [u8]) -> io::Result<()>;

    /// This is expected to remove elements from the cache as well
    fn evict(&self, locations: EvictTarget, cache: Arc<Cache>) -> io::Result<()>;

    fn read(&self, location: ChunkLocation) -> std::io::Result<Bytes>;

    fn get_num_zones(&self) -> usize;

    fn get_chunks_per_zone(&self) -> usize;
    fn get_block_size(&self) -> usize;
    fn get_use_percentage(&self) -> f32;

    fn reset(&self) -> io::Result<()>;

    fn reset_zone(&self, zone_id: usize) -> io::Result<()>;

    fn close_zone(&self, zone_id: usize) -> io::Result<()>;
}

pub fn get_device(
    device: &str,
    chunk_size: usize,
    block_zone_capacity: usize,
    eviction_channel: Sender<EvictorMessage>
) -> io::Result<Arc<dyn Device>> {
    let is_zoned = is_zoned_device(device)?;
    if is_zoned {
        Ok(Arc::new(Zoned::new(device, chunk_size, eviction_channel)?))
    } else {
        Ok(Arc::new(BlockInterface::new(
            device,
            chunk_size,
            block_zone_capacity,
            eviction_channel,
        )?))
    }
}

fn trigger_eviction(eviction_channel: Sender<EvictorMessage>) -> io::Result<()>{
    let (resp_tx, resp_rx) = flume::bounded(1);
    if let Err(e) = eviction_channel.send(EvictorMessage {
        sender: resp_tx,
    }) {
        eprintln!("[append] Failed to send eviction message: {}", e);
        return Err(std::io::Error::new(
            std::io::ErrorKind::Other,
            "Failed to send eviction message",
        ));
    };

    if let Err(e) =  resp_rx.recv() {
        eprintln!("[append] Failed to receive eviction message: {}", e);
    }
    
    Ok(())
}

impl Zoned {
    fn compact_zone(
        &self,
        zone_to_compact: usize,
        chunks_to_keep: &[ChunkLocation],
        buffer: &mut [u8],
    ) -> io::Result<Vec<ChunkLocation>> {
        let mut new_locations = Vec::with_capacity(chunks_to_keep.len());
        for chunk in chunks_to_keep {
            let starting_byte_loc = chunk.index as usize * self.config.chunk_size;
            let ending_byte_loc = (chunk.index + 1) as usize * self.config.chunk_size;
            let new_idx = zns_append(
                &self.nvme_config,
                &self.config,
                zone_to_compact as u64,
                &mut buffer[starting_byte_loc..ending_byte_loc],
            )
            .map_err(|err| std::io::Error::new(ErrorKind::Other, err.to_string()))?;
            new_locations.push(ChunkLocation::new(zone_to_compact, new_idx));
        }
        Ok(new_locations)
    }

    fn get_free_zone(&self) -> io::Result<usize> {
        let (mtx, wait_notify) = &*self.zones;
        let mut zone_list = mtx.lock().unwrap();
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

    fn complete_write(&self, zone_idx: usize) -> io::Result<()> {
        let (mtx, notify) = &*self.zones;
        let mut zone_list = mtx.lock().unwrap();
        zone_list.write_finish(zone_idx, self)?;
        // Tell other threads that we finished writing, so they can
        // come and try to open a new zone if needed.
        notify.notify_all();

        Ok(())
    }
}

impl Zoned {
    fn new(device: &str, chunk_size: usize, eviction_channel: Sender<EvictorMessage>,) -> io::Result<Self> {
        let nvme_config = match nvme::info::nvme_get_info(device) {
            Ok(config) => config,
            Err(err) => return Err(err.try_into().unwrap()),
        };

        match nvme::info::zns_get_info(&nvme_config) {
            Ok(mut config) => {
                let chunk_size_in_logical_blocks =
                    chunk_size as u64 / nvme_config.logical_block_size;
                config.chunks_per_zone = config.zone_cap / chunk_size_in_logical_blocks;
                config.chunk_size = chunk_size;
                let zone_list = ZoneList::new(
                    config.num_zones as usize,
                    config.chunks_per_zone as usize,
                    config.max_active_resources as usize,
                );

                Ok(Self {
                    nvme_config,
                    config,
                    eviction_channel,
                    zones: Arc::new((Mutex::new(zone_list), Condvar::new())),
                })
            }
            Err(err) => Err(err.try_into().unwrap()),
        }
    }
}

impl Device for Zoned {
    /// Hold internal state to keep track of zone state
    fn append(&self, data: Bytes) -> std::io::Result<ChunkLocation> {
        let zone_index = loop {
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
            data.as_ptr() as usize % self.nvme_config.logical_block_size as usize,
            0
        );

        match zns_append(
            &self.nvme_config,
            &self.config,
            zone_index as u64,
            data.as_ref(),
        ) {
            Ok(lba) => {
                self.complete_write(zone_index)?;
                let chunk = lba / self.config.chunk_size as u64;
                Ok(ChunkLocation::new(zone_index, chunk))
            }
            Err(mut err) => {
                self.complete_write(zone_index)?;
                err.add_context(format!("Write failed at zone {}\n", zone_index));
                Err(err.try_into().unwrap())
            }
        }
    }

    fn read_into_buffer(&self, location: ChunkLocation, read_buffer: &mut [u8]) -> io::Result<()>
    where
        Self: Sized,
    {
        match zns_read(
            &self.nvme_config,
            &self.config,
            location.zone as u64,
            location.index,
            read_buffer,
        ) {
            Ok(()) => Ok(()),
            Err(err) => Err(err.try_into().unwrap()),
        }
    }

    fn read(&self, location: ChunkLocation) -> std::io::Result<Bytes> {
        let mut data = vec![0u8; self.config.chunk_size];
        self.read_into_buffer(location, &mut data)?;

        Ok(Bytes::from(data))
    }

    fn evict(&self, locations: EvictTarget, cache: Arc<Cache>) -> io::Result<()> {
        println!("Current device usage is at: {}", self.get_use_percentage());

        match locations {
            EvictTarget::Chunk(mut chunk_locations) => {
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
                    ChunkLocation::new(zone_to_evict, 0),
                    read_buf.as_mut_slice(),
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
                    self.get_chunks_per_zone() - to_keep.len(),
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

    fn get_num_zones(&self) -> usize {
        self.config.num_zones as usize
    }

    fn get_chunks_per_zone(&self) -> usize {
        self.config.chunks_per_zone as usize
    }

    fn get_block_size(&self) -> usize {
        self.nvme_config.logical_block_size as usize
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
            .map_err(|err| std::io::Error::new(ErrorKind::Other, err.to_string()))
    }
    fn reset_zone(&self, zone_id: usize) -> io::Result<()> {
        reset_zone(&self.nvme_config, &self.config, PerformOn::Zone(zone_id as u64))
            .map_err(|err| std::io::Error::new(ErrorKind::Other, err.to_string()))
    }

    fn close_zone(&self, zone_id: usize) -> io::Result<()> {
        close_zone(&self.nvme_config, &self.config, PerformOn::Zone(zone_id as u64))
            .map_err(|err| std::io::Error::new(ErrorKind::Other, err.to_string()))
    }
}

impl BlockInterface {
    fn new(device: &str, chunk_size: usize, block_zone_capacity: usize, eviction_channel: Sender<EvictorMessage>) -> io::Result<Self> {
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

        // Num_zones: how to get?
        let num_zones = nvme_config.total_size_in_bytes as usize / block_zone_capacity;
        // Chunks per zone: how to get?
        let chunks_per_zone = block_zone_capacity / chunk_size;
        
        // Num_zones: how to get?
        let num_zones = 10;
        // Chunks per zone: how to get?
        let chunks_per_zone = 2;

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
            eviction_channel
        })
    }
}

impl Device for BlockInterface {
    /// Hold internal state to keep track of "ssd" zone state
    fn append(&self, data: Bytes) -> std::io::Result<ChunkLocation> {
       
        let mtx = self.state.clone();

        let chunk_location = loop {
            let mut state = mtx.lock().unwrap();
            match state.active_zones.remove_chunk_location() {
                Ok(location) => break location,
                Err(_) => {
                    eprintln!("[append] Failed to allocate chunk: no available space in active zones");
                }
            };
            drop(state);

            trigger_eviction(self.eviction_channel.clone())?;
        };

        assert_eq!(data.len() % self.nvme_config.logical_block_size as usize, 0);

        let write_addr = get_address_at(
            chunk_location.zone as u64,
            chunk_location.index,
            (self.chunks_per_zone * self.chunk_size) as u64,
            self.chunk_size as u64,
        );

        // println!("[append] writing chunk to {} bytes at addr {}", chunk_location.zone, write_addr);

        match nvme::ops::write(
            write_addr,
            self.nvme_config.fd,
            0,
            self.nvme_config.nsid,
            self.nvme_config.logical_block_size,
            data.as_ref(),
        ) {
            Ok(()) => Ok(chunk_location),
            Err(err) => Err(err.try_into().unwrap()),
        }
    }

    fn read_into_buffer(&self, location: ChunkLocation, read_buffer: &mut [u8]) -> io::Result<()>
    where
        Self: Sized,
    {
        let slba = get_address_at(
            location.zone as u64,
            location.index,
            (self.chunks_per_zone * self.chunk_size) as u64,
            self.chunk_size as u64,
        );

        match nvme::ops::read(&self.nvme_config, slba, read_buffer) {
            Ok(()) => Ok(()),
            Err(err) => Err(err.try_into().unwrap()),
        }
    }

    fn read(&self, location: ChunkLocation) -> std::io::Result<Bytes> {
        let mut data = vec![0u8; self.chunk_size];
        self.read_into_buffer(location, &mut data)?;
        Ok(Bytes::from(data))
    }

    fn evict(&self, locations: EvictTarget, cache: Arc<Cache>) -> io::Result<()> {
        match locations {
            EvictTarget::Chunk(chunk_locations) => {
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

    fn get_num_zones(&self) -> usize {
        self.num_zones
    }

    fn get_chunks_per_zone(&self) -> usize {
        self.chunks_per_zone
    }

    fn get_block_size(&self) -> usize {
        self.nvme_config.logical_block_size as usize
    }

    fn get_use_percentage(&self) -> f32 {
        let state = &*self.state.lock().unwrap();
        let total_chunks = (self.chunks_per_zone * self.num_zones) as f32;
        let available_chunks = state.active_zones.get_num_available_chunks() as f32;
        (total_chunks - available_chunks) / total_chunks
    }

    fn reset(&self) -> io::Result<()> { Ok(()) }

    fn reset_zone(&self, zone_id: usize) -> io::Result<()> { Ok(()) }

    fn close_zone(&self, zone_id: usize) -> io::Result<()> { Ok(()) }
}
