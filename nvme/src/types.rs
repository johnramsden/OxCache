use std::{
    convert::Infallible,
    fmt::{self, Debug},
    io,
    os::fd::RawFd,
};

use errno::Errno;
use libnvme_sys::bindings::nvme_status_result;

use crate::util::get_error_string;

pub type Byte = u64;
pub type LogicalBlock = u64;
pub type Zone = u64;
pub type Chunk = u64;

/// Represents the state of a ZNS (Zoned Namespace) zone.
#[repr(u8)]
#[derive(Debug, PartialEq, Eq, Clone)]
pub enum ZoneState {
    Empty = 1,
    ImplicitlyOpened = 2,
    ExplicitlyOpened = 3,
    Closed = 4,
    ReadOnly = 13,
    Full = 14,
    Offline = 15,
}

impl TryFrom<u8> for ZoneState {
    type Error = ();

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0x01 => Ok(ZoneState::Empty),
            0x02 => Ok(ZoneState::ImplicitlyOpened),
            0x03 => Ok(ZoneState::ExplicitlyOpened),
            0x04 => Ok(ZoneState::Closed),
            0xD => Ok(ZoneState::ReadOnly),
            0xE => Ok(ZoneState::Full),
            0xF => Ok(ZoneState::Offline),
            _ => Err(()),
        }
    }
}

#[derive(Debug)]
pub struct NVMeConfig {
    pub fd: RawFd,
    pub nsid: u32,
    pub logical_block_size: Byte,
    pub total_size_in_bytes: Byte,
    pub current_lba_index: usize,
    pub maximum_data_transfer_size: usize, // Zero means that there is no limit, otherwise in units of minimum memory page size
    pub lba_perf: u64,                     // Unimplemented
    pub timeout: u32,                      // Default 0
}

impl NVMeConfig {
    pub fn lba_to_byte_address(&self, lba: LogicalBlock) -> Byte {
        self.logical_block_size * lba
    }

    pub fn byte_address_to_lba(&self, byte_address: Byte) -> LogicalBlock {
        byte_address / self.logical_block_size
    }
}

// Assumes that the zone capacity is the same for every zone
/// Holds configuration and metadata for a ZNS (Zoned Namespace) device.
#[derive(Debug)]
pub struct ZNSConfig {
    // These are 1-based, unlike what is returned in libnvme
    pub max_active_resources: u32, // Open and closed zones
    pub max_open_resources: u32,   // Just open zones

    // These are per-controller
    pub zasl: Byte, // The zone append size limit. Max append size is zasl bytes.
    pub zone_descriptor_extension_size: u64, // The size of the data that can be associated with a zone, in bytes
    pub zone_size: LogicalBlock,             // This is in number of logical blocks
    pub zone_cap: LogicalBlock,              // This is in number of logical blocks

    pub chunks_per_zone: Chunk, // Number of chunks that can be allocated in a zone
    pub chunk_size_in_lbas: LogicalBlock, // This is in logical blocks
    pub chunk_size_in_bytes: Byte, // This is in logical blocks

    pub num_zones: Zone,
}

impl ZNSConfig {
    /// Returns the starting byte address of the zone
    pub fn get_starting_address(&self, nvme_config: &NVMeConfig, zone_index: Zone) -> Byte {
        self.zone_size * zone_index * nvme_config.logical_block_size
    }

    /// Returns the starting logical block address of the zone
    pub fn get_starting_lba(&self, zone_index: Zone) -> LogicalBlock {
        self.zone_size * zone_index
    }

    /// Get the logical block address at a chunk
    pub fn get_address_at(&self, zone_index: Zone, chunk_index: Chunk) -> LogicalBlock {
        self.zone_size * zone_index + chunk_index * self.chunk_size_in_lbas as u64
    }

    pub fn chunks_to_logical_blocks(&self, chunk_count: Chunk) -> LogicalBlock {
        self.chunk_size_in_lbas * chunk_count
    }

    pub fn chunks_to_bytes(&self, nvme_config: &NVMeConfig, chunk_count: Chunk) -> Byte {
        nvme_config.lba_to_byte_address(self.chunks_to_logical_blocks(chunk_count))
    }
}

/// Specifies which zones to perform an operation on (all or a specific zone).
#[derive(PartialEq)]
pub enum PerformOn {
    AllZones,
    Zone(Zone),
}

/// Describes the properties and state of a single ZNS (Zoned Namespace) zone.
///
/// This struct provides key information about a zone, including its state, capacity, start address, and write pointer.
///
/// # Fields
///
/// * `seq_write_required` - Indicates if sequential writes are required for this zone.
/// * `zone_state` - The current state of the zone, as a `ZoneState`.
/// * `zone_capacity` - The total capacity of the zone in *logical blocks*.
/// * `zone_start_address` - The starting logical block address (LBA) of the zone.
/// * `write_pointer` - The current write pointer LBA for the zone.
///
#[derive(Debug, Clone)]
pub struct ZNSZoneDescriptor {
    pub seq_write_required: bool,
    pub zone_state: ZoneState,
    pub zone_capacity: LogicalBlock,
    pub zone_start_address: LogicalBlock,
    pub write_pointer: LogicalBlock,
}

/// Represents errors that can occur during NVMe operations.
#[derive(Clone, Debug)]
pub enum NVMeError {
    Errno(Errno),
    StatusResult(nvme_status_result),
    UnalignedDataBuffer {
        want: Byte,
        has: Byte,
    },
    AppendSizeTooLarge {
        max_append: Byte,
        trying_to_append: Byte,
    },
    ExtraContext {
        context: String,
        context_index: usize,
        original_error: Box<NVMeError>,
    },
}

impl NVMeError {
    pub fn add_context(mut self, context: String) -> Self {
        self = NVMeError::ExtraContext {
            context,
            context_index: match self {
                NVMeError::ExtraContext {
                    context: _,
                    context_index,
                    original_error: _,
                } => context_index + 1,
                _ => 0,
            },
            original_error: Box::new(self.clone()),
        };
        self
    }
}

impl fmt::Display for NVMeError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "{}",
            match self {
                NVMeError::Errno(errno) => format!("Error: errno {}: {}", errno.0, errno),
                NVMeError::StatusResult(res) => format!(
                    "Error: NVMe Status Result {}: {}",
                    res,
                    get_error_string(*res)
                ),
                NVMeError::UnalignedDataBuffer { want, has } => format!(
                    "Unaligned data buffer: want buffer size of multiple {} but got size {}",
                    want, has
                ),
                NVMeError::AppendSizeTooLarge {
                    max_append,
                    trying_to_append,
                } => format!(
                    "Append size too large: max append is {} bytes while trying to append {} bytes",
                    max_append, trying_to_append
                ),
                NVMeError::ExtraContext {
                    context,
                    context_index,
                    original_error,
                } => format!(
                    "{}\nContext backtrace #{}: {}",
                    *original_error, context_index, context
                ),
            }
        )
    }
}

impl std::error::Error for NVMeError {}

impl TryFrom<NVMeError> for std::io::Error {
    type Error = Infallible;

    fn try_from(value: NVMeError) -> Result<Self, Self::Error> {
        match value {
            NVMeError::Errno(errno) => Ok(io::Error::from_raw_os_error(errno.0)),
            NVMeError::StatusResult(status) => Ok(io::Error::other(get_error_string(status))),
            _ => Ok(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("{}", value),
            )),
        }
    }
}
