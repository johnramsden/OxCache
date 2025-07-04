use nvme::info::{get_address_at, is_zoned_device, nvme_get_info};
use nvme::ops::{zns_append, zns_read};
use nvme::types::{NVMeConfig, ZNSConfig};
use std::collections::VecDeque;
use std::fs::OpenOptions;
use std::io::Error;
use std::ops::Deref;
use std::os::fd::{AsRawFd, RawFd};
use std::sync::{Arc, Mutex};

use crate::cache::bucket::ChunkLocation;
use crate::device;
use crate::eviction::{EvictionPolicy, EvictionPolicyWrapper};

#[derive(Copy, Clone)]
struct Zone {
    index: usize,
    chunks_available: usize,
}

struct ZoneList {
    available_zones: VecDeque<Zone>,
    chunks_per_zone: usize,
}

impl ZoneList {
    fn new(num_zones: usize, chunks_per_zone: usize) -> Self {
        let mut avail_zones = VecDeque::with_capacity(num_zones as usize);

        for i in 0..num_zones {
            avail_zones.push_back(Zone {
                index: i,
                chunks_available: chunks_per_zone,
            });
        }

        ZoneList {
            available_zones: avail_zones,
            chunks_per_zone,
        }
    }

    // Get a zone to write to
    fn remove(&mut self) -> Result<usize, ()> {
        if self.is_full() {
            // Need to evict
            return Err(());
        }

        let mut zone = match self.available_zones.pop_front() {
            Some(z) => z,
            None => return Err(()),
        };
        if zone.chunks_available > 1 {
            zone.chunks_available -= 1;
            self.available_zones.push_front(zone);
        }

        Ok(zone.index)
    }

    // Get the location to write to, for block devices
    fn remove_chunk_location(&mut self) -> Result<ChunkLocation, ()> {
        if self.is_full() {
            // Need to evict
            return Err(());
        }

        let mut zone = match self.available_zones.pop_front() {
            Some(z) => z,
            None => return Err(()),
        };
        let chunk_idx = self.chunks_per_zone - zone.chunks_available;
        if zone.chunks_available > 1 {
            zone.chunks_available -= 1;
            self.available_zones.push_front(zone);
        }

        Ok(ChunkLocation {
            zone: zone.index,
            addr: chunk_idx as u64,
        })
    }

    // Check if all zones are full
    fn is_full(&self) -> bool {
        self.available_zones.is_empty()
    }

    // Reset the selected zone
    fn reset_zone(&mut self, idx: usize) {
        self.available_zones.push_back(Zone {
            index: idx,
            chunks_available: self.chunks_per_zone,
        });
    }

    fn reset_zones(&mut self, indices: Vec<usize>) {
        for idx in indices {
            self.available_zones.push_back(Zone {
                index: idx,
                chunks_available: self.chunks_per_zone,
            });
        }
    }
}

pub struct Zoned {
    nvme_config: NVMeConfig,
    config: ZNSConfig,
    zones: Arc<Mutex<ZoneList>>,
    evict_policy: Arc<Mutex<EvictionPolicyWrapper>>,
}

// Information about each zone
#[derive(Clone)]
pub struct BlockZoneInfo {
    write_pointer: u64,
}

pub struct BlockDeviceState {
    zones: Vec<BlockZoneInfo>,
    active_zones: ZoneList,
    chunk_size: usize,
}

impl BlockDeviceState {
    fn new(num_zones: usize, chunks_per_zone: usize, chunk_size: usize) -> Self {
        let zones = vec![BlockZoneInfo { write_pointer: 0 }; num_zones];
        Self {
            zones,
            active_zones: ZoneList::new(num_zones, chunks_per_zone),
            chunk_size,
        }
    }
}

pub struct BlockInterface {
    nvme_config: NVMeConfig,
    chunk_size: usize,
    chunks_per_zone: usize,
    num_zones: usize,
    state: Arc<Mutex<BlockDeviceState>>,
    evict_policy: Arc<Mutex<EvictionPolicyWrapper>>,
}

pub trait Device: Send + Sync {
    fn append(&self, data: Vec<u8>) -> std::io::Result<ChunkLocation>;

    fn new(
        device: &str,
        chunk_size: usize,
        eviction_policy: Arc<Mutex<EvictionPolicyWrapper>>,
    ) -> std::io::Result<Self>
    where
        Self: Sized; // Args

    fn read_into_buffer(
        &self,
        location: ChunkLocation,
        read_buffer: &mut [u8],
    ) -> std::io::Result<()>;

    fn evict(&self, num_eviction: usize) -> std::io::Result<()>;

    fn read(&self, location: ChunkLocation) -> std::io::Result<Vec<u8>>;
}

pub fn get_device(
    device: &str,
    chunk_size: usize,
    eviction_policy: Arc<Mutex<EvictionPolicyWrapper>>,
) -> std::io::Result<Arc<dyn Device>> {
    let is_zoned = is_zoned_device(device)?;
    if is_zoned {
        Ok(Arc::new(device::Zoned::new(
            device,
            chunk_size,
            eviction_policy,
        )?))
    } else {
        Ok(Arc::new(device::BlockInterface::new(
            device,
            chunk_size,
            eviction_policy,
        )?))
    }
}

impl Zoned {
    fn get_free_zone(&self) -> std::io::Result<usize> {
        let mtx = Arc::clone(&self.zones);
        let mut zone_list = mtx.lock().unwrap();
        match zone_list.remove() {
            Ok(zone_idx) => Ok(zone_idx),
            Err(()) => Err(std::io::Error::new(
                std::io::ErrorKind::StorageFull,
                "Cache is full",
            )),
        }
    }
}

impl Device for Zoned {
    /// Hold internal state to keep track of zone state
    fn new(
        device: &str,
        chunk_size: usize,
        eviction_policy: Arc<Mutex<EvictionPolicyWrapper>>,
    ) -> std::io::Result<Self> {
        let nvme_config = match nvme::info::nvme_get_info(device) {
            Ok(config) => config,
            Err(err) => return Err(err.try_into().unwrap()),
        };

        match nvme::info::zns_get_info(&nvme_config) {
            Ok(mut config) => {
                config.chunks_per_zone = config.zone_size / chunk_size as u64;
                config.chunk_size = chunk_size;
                let zone_list =
                    ZoneList::new(config.num_zones as usize, config.chunks_per_zone as usize);

                Ok(Self {
                    nvme_config,
                    config,
                    zones: Arc::new(Mutex::new(zone_list)),
                    evict_policy: eviction_policy,
                })
            }
            Err(err) => Err(err.try_into().unwrap()),
        }
    }

    fn append(&self, data: Vec<u8>) -> std::io::Result<ChunkLocation> {
        let zone_index = self.get_free_zone()?;
        let mut mut_data = Vec::clone(&data);
        match zns_append(
            &self.nvme_config,
            &self.config,
            zone_index as u64,
            mut_data.as_mut_slice(),
        ) {
            Ok(lba) => {
                let chunk = lba / self.config.chunk_size as u64;

                let mtx = Arc::clone(&self.evict_policy);
                let policy = mtx.lock().unwrap();
                policy.write_update(ChunkLocation::new(
                    zone_index, chunk, // addr should be in chunks
                ));

                Ok(ChunkLocation::new(zone_index, chunk))
            }
            Err(err) => Err(err.try_into().unwrap()),
        }
    }

    fn read_into_buffer(
        &self,
        location: ChunkLocation,
        read_buffer: &mut [u8],
    ) -> std::io::Result<()>
    where
        Self: Sized,
    {
        match zns_read(
            &self.nvme_config,
            &self.config,
            location.zone as u64,
            location.addr,
            read_buffer,
        ) {
            Ok(()) => {
                let mtx = Arc::clone(&self.evict_policy);
                let policy = mtx.lock().unwrap();
                policy.read_update(ChunkLocation::new(location.zone, location.addr));
                Ok(())
            }
            Err(err) => Err(err.try_into().unwrap()),
        }
    }

    fn read(&self, location: ChunkLocation) -> std::io::Result<Vec<u8>> {
        let mut data = vec![0; self.config.chunk_size];
        self.read_into_buffer(location, &mut data)?;
        Ok(data)
    }

    fn evict(&self, num_eviction: usize) -> std::io::Result<()> {
        let mtx = Arc::clone(&self.evict_policy);
        let policy = mtx.lock().unwrap();
        match &*policy {
            EvictionPolicyWrapper::Dummy(p) => Ok(()),
            EvictionPolicyWrapper::Chunk(p) => {
                unimplemented!()
            }
            EvictionPolicyWrapper::Promotional(p) => match p.get_evict_targets() {
                Some(evict_targets) => {
                    let zone_mtx = Arc::clone(&self.zones);
                    let mut zones = zone_mtx.lock().unwrap();
                    zones.reset_zones(evict_targets);
                    Ok(())
                }
                None => Err(Error::new(std::io::ErrorKind::Other, "No items to evict")),
            },
        }
    }
}

impl BlockInterface {
    fn read(&self, location: ChunkLocation) -> std::io::Result<Vec<u8>> {
        Ok(Vec::new())
    }
}

impl Device for BlockInterface {
    /// Hold internal state to keep track of "ssd" zone state
    fn new(
        device: &str,
        chunk_size: usize,
        eviction_policy: Arc<Mutex<EvictionPolicyWrapper>>,
    ) -> std::io::Result<Self> {
        let nvme_config = match nvme::info::nvme_get_info(device) {
            Ok(config) => config,
            Err(err) => return Err(err.try_into().unwrap()),
        };

        // Num_zones: how to get?
        let num_zones = 100;
        // Chunks per zone: how to get?
        let chunks_per_zone = 100;

        Ok(Self {
            nvme_config,
            state: Arc::new(Mutex::new(BlockDeviceState::new(
                num_zones,
                chunks_per_zone,
                chunk_size,
            ))),
            evict_policy: eviction_policy,
            chunk_size,
            chunks_per_zone,
            num_zones,
        })
    }

    fn append(&self, data: Vec<u8>) -> std::io::Result<ChunkLocation> {
        let mtx = self.state.clone();
        let mut state = mtx.lock().unwrap();
        let chunk_location = match state.active_zones.remove_chunk_location() {
            Ok(location) => location,
            Err(()) => {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::StorageFull,
                    "Cache is full",
                ));
            }
        };
        drop(state);

        let mut mut_data = Vec::clone(&data);

        match nvme::ops::write(
            get_address_at(
                chunk_location.zone as u64,
                chunk_location.addr,
                (self.chunks_per_zone * self.chunk_size) as u64,
                self.chunk_size as u64,
            ),
            self.nvme_config.fd,
            0,
            self.nvme_config.nsid,
            self.nvme_config.logical_block_size,
            mut_data.as_mut_slice(),
        ) {
            Ok(()) => Ok(chunk_location),
            Err(err) => Err(err.try_into().unwrap()),
        }
    }

    fn read_into_buffer(
        &self,
        location: ChunkLocation,
        read_buffer: &mut [u8],
    ) -> std::io::Result<()>
    where
        Self: Sized,
    {
        let slba = get_address_at(
            location.zone as u64,
            location.addr,
            (self.chunks_per_zone * self.chunk_size) as u64,
            self.chunk_size as u64,
        );

        match nvme::ops::read(&self.nvme_config, slba, read_buffer) {
            Ok(()) => Ok(()),
            Err(err) => Err(err.try_into().unwrap()),
        }
    }

    fn read(&self, location: ChunkLocation) -> std::io::Result<Vec<u8>> {
        let mut buffer = vec![0; self.chunk_size];
        self.read_into_buffer(location, &mut buffer);
        Ok(buffer)
    }

    fn evict(&self, num_eviction: usize) -> std::io::Result<()> {
        let mtx = Arc::clone(&self.evict_policy);
        let policy = mtx.lock().unwrap();
        match &*policy {
            EvictionPolicyWrapper::Dummy(p) => Ok(()),
            EvictionPolicyWrapper::Chunk(p) => {
                unimplemented!()
            }
            EvictionPolicyWrapper::Promotional(p) => match p.get_evict_targets() {
                Some(evict_targets) => {
                    let state_mtx = Arc::clone(&self.state);
                    let mut state = state_mtx.lock().unwrap();
                    state.active_zones.reset_zones(evict_targets);
                    Ok(())
                }
                None => Err(Error::new(std::io::ErrorKind::Other, "No items to evict")),
            },
        }
    }
}
