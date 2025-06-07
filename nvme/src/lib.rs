#![allow(non_upper_case_globals)]
#![allow(non_camel_case_types)]
#![allow(non_snake_case)]
#![feature(iter_array_chunks)]

use std::{
    ffi::{CStr, CString, c_void},
    os::fd::RawFd,
    ptr::null,
};

use libnvme_sys::bindings::{nvme_open, *};

use std::convert::TryFrom;

macro_rules! const_assert {
    ($($tt:tt)*) => {
        const _: () = assert!($($tt)*);
    }
}

#[repr(u8)]
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
/// let desc = ZNSZoneDescriptor {
///     seq_write_required: true,
///     zone_state: ZoneState::Empty,
///     zone_capacity: 1024,
///     zone_start_address: 0,
///     write_pointer: 0,
/// };
/// println!("Zone state: {:?}", desc.zone_state);
/// ```
pub struct ZNSZoneDescriptor {
    seq_write_required: bool,
    zone_state: ZoneState,
    zone_capacity: u64,
    zone_start_address: u64,
    write_pointer: u64,
}

/// Returns a human-readable error string for a given NVMe status code.
///
/// # Arguments
///
/// * `status` - The NVMe status code (`nvme_status_field`) to convert.
///
/// # Returns
///
/// * `&'static str` - A static string describing the NVMe status code.
///
/// # Safety
///
/// This function calls into unsafe FFI code and assumes the status code is valid.
///
/// # Example
///
/// ```rust
/// let msg = get_error_string(status);
/// println!("NVMe error: {}", msg);
/// ```
pub fn get_error_string(status: nvme_status_field) -> &'static str {
    // These are all defined as static const char in the C code, so they should have static lifetime
    unsafe {
        CStr::from_ptr(nvme_status_to_string(status as i32, false))
            .to_str()
            .unwrap()
    }
}

/// Converts an NVMe status code to a standard errno value.
///
/// # Arguments
///
/// * `status` - The NVMe status code (`nvme_status_field`) to convert.
///
/// # Returns
///
/// * `u8` - The corresponding errno value.
///
/// # Safety
///
/// This function calls into unsafe FFI code and assumes the status code is valid.
///
/// # Example
///
/// ```rust
/// let errno = get_errno(status);
/// println!("Errno: {}", errno);
/// ```
pub fn get_errno(status: nvme_status_field) -> u8 {
    unsafe { nvme_status_to_errno(status as i32, false) }
}

/// Opens an NVMe device and returns its file descriptor.
///
/// # Arguments
///
/// * `device_name` - The path to the NVMe device (e.g., "/dev/nvme0n1").
///
/// # Returns
///
/// * `Ok(RawFd)` - The file descriptor for the opened NVMe device.
/// * `Err(())` - If the operation fails.
///
/// # Safety
///
/// This function calls into unsafe FFI code and assumes the provided device name is valid.
///
/// # Example
///
/// ```rust
/// let fd = zns_nvme_open("/dev/nvme0n1").expect("Failed to open NVMe device");
/// ```
pub fn zns_open(device_name: &str) -> Result<RawFd, ()> {
    unsafe {
        let fd = nvme_open(CString::new(device_name).unwrap().as_ptr());
        if fd == -1 { Err(()) } else { Ok(fd) }
    }
}

/// Appends data to a ZNS (Zoned Namespace) zone on an NVMe device.
///
/// # Arguments
///
/// * `fd` - The file descriptor for the open NVMe device.
/// * `zone_num` - The index of the zone to which data will be appended.
/// * `zone_size` - The size of each zone in bytes.
/// * `data` - The buffer containing data to append. The length should be a multiple of `logical_block_size`.
/// * `timeout` - Timeout for the operation, in milliseconds.
/// * `nsid` - Namespace ID, obtained from an identify command.
/// * `logical_block_size` - The size of a logical block, in bytes.
///
/// # Returns
///
/// * `Ok(u64)` - The logical block address (LBA) where the data was appended.
/// * `Err(nvme_status_field)` - An NVMe status code if the operation failed.
///
/// # Errors
///
/// Returns an error if the NVMe append command fails, with the corresponding NVMe status code.
///
/// # Safety
///
/// This function calls into unsafe FFI code and assumes the provided arguments are valid for the underlying NVMe device.
///
/// # Example
///
/// ```rust
/// let fd = zns_nvme_open("/dev/nvme0n1");
/// let mut buffer = vec![0u8; 4096];
/// let lba = zns_append(fd, 0, 1024, &mut buffer, 1000, 1, 4096)?;
/// ```
pub fn zns_append(
    fd: RawFd,
    zone_num: u64,
    zone_size: u64,
    data: &mut [u8],
    timeout: u32,
    nsid: u32, // obtained from an identify command
    logical_block_size: usize,
) -> Result<u64, nvme_status_field> {
    let mut result: u64 = 0;

    // See fig. 26 of NVME ZNS Command Set Spec Rev 1.2
    const lr: u16 = 0;
    const fua: u16 = 0;
    const prinfo: u16 = 0;
    const piremap: u16 = 0;
    const stc: u16 = 0;
    const dtype: u16 = 0;
    const cetype: u16 = 0;

    let ctrl: u16 =
        { lr << 15 | fua << 14 | prinfo << 13 | piremap << 9 | stc << 8 | dtype << 7 | cetype };

    // zslba is the starting logical block address
    let mut args: nvme_zns_append_args = nvme_zns_append_args {
        zslba: zone_num * zone_size,
        result: &mut result,
        data: data.as_mut_ptr() as *mut c_void,
        metadata: null::<c_void>() as *mut c_void, // type *mut c_void
        args_size: size_of::<nvme_zns_append_args>() as i32,
        fd: fd,
        timeout: timeout,
        nsid: nsid,
        // Only used for end-to-end protection
        ilbrt: 0, // Initial logical block reference tag
        data_len: data.len() as u32,
        metadata_len: 0,
        nlb: (data.len() / logical_block_size) as u16,
        control: ctrl,
        lbat: 0,
        lbatm: 0,
        rsvd1: [0; 4usize],
        ilbrt_u64: 0,
    };
    unsafe {
        let error_code = nvme_zns_append(&mut args) as u32;
        if error_code != nvme_status_field_NVME_SC_SUCCESS {
            return Err(error_code);
        }
    }

    return Ok(result);
}

/// Opens a specific zone or all zones on a Zoned Namespace (ZNS) NVMe device.
///
/// # Arguments
///
/// * `fd` - The file descriptor for the open NVMe device.
/// * `zone_num` - The index of the zone to open.
/// * `zone_size` - The size of each zone in bytes.
/// * `timeout` - Timeout for the operation, in milliseconds.
/// * `nsid` - Namespace ID, obtained from an identify command.
/// * `open_all` - If `true`, open all zones; if `false`, open only the specified zone.
///
/// # Returns
///
/// * `Ok(bool)` - Returns `true` if the zone capacity changed (zone opened), `false` otherwise.
/// * `Err(nvme_status_field)` - An NVMe status code if the operation failed.
///
/// # Errors
///
/// Returns an error if the NVMe zone management command fails, with the corresponding NVMe status code.
///
/// # Safety
///
/// This function calls into unsafe FFI code and assumes the provided arguments are valid for the underlying NVMe device.
///
/// # Example
///
/// ```rust
/// let fd = zns_nvme_open("/dev/nvme0n1")?;
/// let opened = open_zone(fd, 0, 1024, 1000, 1, false)?;
/// if opened {
///     println!("Zone opened successfully");
/// }
/// ```
pub fn open_zone(
    fd: RawFd,
    zone_num: u64,
    zone_size: u64,
    timeout: u32,
    nsid: u32,
    open_all: bool,
) -> Result<bool, nvme_status_field> {
    let mut zone_cap_changed: u32 = 0;

    let mut args = nvme_zns_mgmt_send_args {
        slba: zone_num * zone_size,
        result: &mut zone_cap_changed,
        data: null::<c_void>() as *mut c_void,
        args_size: size_of::<nvme_zns_mgmt_send_args>() as i32,
        fd: fd,
        timeout: timeout,
        nsid: nsid,
        zsa: nvme_zns_send_action_NVME_ZNS_ZSA_OPEN,
        data_len: 0,
        select_all: open_all,
        zsaso: 0,
    };

    unsafe {
        let status = nvme_zns_mgmt_send(&mut args) as u32;
        if status == nvme_status_field_NVME_SC_SUCCESS {
            Ok(zone_cap_changed == 1)
        } else {
            Err(status)
        }
    }
}

/// Retrieves a report of zones from a Zoned Namespace (ZNS) NVMe device.
///
/// # Arguments
///
/// * `fd` - The file descriptor for the open NVMe device.
/// * `nsid` - Namespace ID, obtained from an identify command.
/// * `zone_num` - The starting zone index for the report.
/// * `zone_size` - The size of each zone in bytes.
/// * `max_zones` - The maximum number of zones to report.
/// * `timeout` - Timeout for the operation, in milliseconds.
///
/// # Returns
///
/// * `Ok(Vec<ZNSZoneDescriptor>)` - A vector of zone descriptors for the reported zones.
/// * `Err(nvme_status_field)` - An NVMe status code if the operation failed.
///
/// # Errors
///
/// Returns an error if the NVMe report zones command fails, with the corresponding NVMe status code.
///
/// # Safety
///
/// This function calls into unsafe FFI code and assumes the provided arguments are valid for the underlying NVMe device.
///
/// # Example
///
/// ```rust
/// let fd = zns_nvme_open("/dev/nvme0n1")?;
/// let zones = report_zones(fd, 1, 0, 1024, 128, 1000)?;
/// for zone in zones {
///     println!("Zone start: {:#x}, state: {:?}", zone.zone_start_address, zone.zone_state);
/// }
/// ```
pub fn report_zones(
    fd: RawFd,
    nsid: u32,
    zone_num: u64,
    zone_size: u64,
    max_zones: u64,
    timeout: u32,
) -> Result<Vec<ZNSZoneDescriptor>, nvme_status_field> {
    // dword0, has nothing useful
    let mut result: u32 = 0;

    const_assert!(
        std::mem::size_of::<__IncompleteArrayField<nvme_zns_desc>>() == 0,
        "The flexible array field should have size 0"
    );
    const_assert!(align_of::<nvme_zone_report>() <= 64);

    const nr_hdr_sz: usize = std::mem::size_of::<nvme_zone_report>();
    const zone_desc_sz: usize = std::mem::size_of::<nvme_zns_desc>();
    let alloc_size = (nr_hdr_sz + zone_desc_sz * max_zones as usize) / 64;
    let mut report_buf = vec![0_u64; alloc_size];

    // extended reports store a little extra data in each zone, unneeded in this case
    unsafe {
        let err = nvme_zns_report_zones_wrapper(
            fd,
            nsid,
            zone_num * zone_size,
            nvme_zns_report_options_NVME_ZNS_ZRAS_REPORT_ALL,
            false,
            true,
            report_buf.len() as u32,
            report_buf.as_mut_ptr() as *mut c_void,
            timeout,
            &mut result,
        );

        match err as u32 {
            nvme_status_field_NVME_SC_SUCCESS => Ok(report_buf
                .into_iter()
                .skip(nr_hdr_sz)
                .array_chunks::<{ zone_desc_sz / 64 }>()
                .map(|data: [u64; 1]| {
                    let zns_desc = std::ptr::read(data.as_ptr() as *const nvme_zns_desc);
                    ZNSZoneDescriptor {
                        seq_write_required: zns_desc.zt == 2,
                        zone_state: zns_desc.zs.try_into().unwrap(),
                        zone_capacity: zns_desc.zcap,
                        zone_start_address: zns_desc.zslba,
                        write_pointer: zns_desc.wp,
                    }
                })
                .collect::<Vec<ZNSZoneDescriptor>>()),
            _ => Err(err as u32),
        }
    }
}

