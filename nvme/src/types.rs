use std::{
    convert::Infallible,
    fmt::{self, Debug, Display},
    io,
    os::fd::RawFd,
};

use errno::Errno;
use libnvme_sys::bindings::nvme_status_result;

use crate::util::get_error_string;

/// Represents the state of a ZNS (Zoned Namespace) zone.
#[repr(u8)]
#[derive(Debug, PartialEq, Eq)]
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
            0x13 => Ok(ZoneState::ReadOnly),
            0x14 => Ok(ZoneState::Full),
            0x15 => Ok(ZoneState::Offline),
            _ => Err(()),
        }
    }
}

#[derive(Debug)]
pub struct NVMeConfig {
    pub fd: RawFd,
    pub nsid: u32,
    pub logical_block_size: u64,
    pub total_size_in_bytes: u64,
    pub current_lba_index: usize,
    pub lba_perf: u64, // Unimplemented
    pub timeout: u32,  // Default 0
}

// Assumes that the zone capacity is the same for every zone
/// Holds configuration and metadata for a ZNS (Zoned Namespace) device.
#[derive(Debug)]
pub struct ZNSConfig {
    // These are 1-based, unlike what is returned in libnvme
    pub max_active_resources: u32, // Open and closed zones
    pub max_open_resources: u32,   // Just open zones

    // These are per-controller
    pub zasl: u32, // The zone append size limit. Max append size is zasl bytes.
    pub zone_descriptor_extension_size: u64, // The size of the data that can be associated with a zone, in bytes
    pub zone_size: u64,                      // This is in number of logical blocks

    pub chunks_per_zone: u64, // Number of chunks that can be allocated in a zone
    pub chunk_size: usize,    // This is in logical blocks

    pub num_zones: u64,
}

impl ZNSConfig {
    // The ZSLBA field is referenced in logical blocks
    pub fn get_starting_addr(&self, zone_index: u64) -> u64 {
        self.zone_size * zone_index
    }

    pub fn get_address_at(&self, zone_index: u64, chunk_index: u64) -> u64 {
        self.zone_size * zone_index + chunk_index * self.chunk_size as u64
    }
}

/// Specifies which zones to perform an operation on (all or a specific zone).
#[derive(PartialEq)]
pub enum PerformOn {
    AllZones,
    Zone(u64),
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
/// # Example
///
/// ```rust
/// use nvme::{ZNSZoneDescriptor, ZoneState};
///
/// let desc = ZNSZoneDescriptor {
///     seq_write_required: true,
///     zone_state: ZoneState::Empty,
///     zone_capacity: 1024,
///     zone_start_address: 0,
///     write_pointer: 0,
/// };
/// println!("Zone state: {:?}", desc.zone_state);
/// ```
#[derive(Debug)]
pub struct ZNSZoneDescriptor {
    pub seq_write_required: bool,
    pub zone_state: ZoneState,
    pub zone_capacity: u64,
    pub zone_start_address: u64,
    pub write_pointer: u64,
}

/// Represents errors that can occur during NVMe operations.
#[derive(Debug)]
pub enum NVMeError {
    Errno(Errno),
    StatusResult(nvme_status_result),
    UnalignedDataBuffer {
        want: u64,
        has: u64,
    },
    AppendSizeTooLarge {
        max_append: u32,
        trying_to_append: u32,
    },
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
