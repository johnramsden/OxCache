use std::{
    ffi::c_void,
    fs::{self},
    io::{self},
    mem,
    os::fd::RawFd,
};

use libnvme_sys::bindings::*;

use crate::{
    ops::open_device,
    types::{Byte, Chunk, LogicalBlock, NVMeConfig, NVMeError, ZNSConfig, ZNSZoneDescriptor, Zone},
    util::{check_error, shift_and_mask},
};

macro_rules! const_assert {
    ($($tt:tt)*) => {
        const _: () = assert!($($tt)*);
    }
}

const_assert!(
    std::mem::size_of::<__IncompleteArrayField<nvme_zns_desc>>() == 0,
    "The flexible array field should have size 0"
);

const u64_size: usize = std::mem::size_of::<u64>();
// This checks if the nvme_zone_report alignment is the same, to avoid undefined behaviour
const_assert!(align_of::<nvme_zone_report>() == align_of::<u64>());

// The header stores number of zones and the rest are zone descriptors in a flexible array
const nr_hdr_sz: usize = std::mem::size_of::<nvme_zone_report>();
// The zone descriptor zone type, zone state, zone attributes, zone capacity, zone starting LBA, and the write pointer.
const zone_desc_sz: usize = std::mem::size_of::<nvme_zns_desc>();
const_assert!(nr_hdr_sz == 64);
const_assert!(zone_desc_sz == 64);

unsafe fn id_ns_fn<IdType, IdFunc>(fd: RawFd, nsid: u32, func: IdFunc) -> Result<IdType, NVMeError>
where
    IdFunc: Fn(RawFd, u32, &mut IdType) -> i32,
{
    unsafe {
        let mut data: IdType = mem::zeroed();
        let status = func(fd, nsid, &mut data);
        match check_error(status) {
            Some(err) => return Err(err),
            None => Ok(data),
        }
    }
}

fn oxcache_id_ns(fd: RawFd, nsid: u32) -> Result<nvme_id_ns, NVMeError> {
    unsafe {
        id_ns_fn(fd, nsid, |fd, nsid, data| {
            nvme_identify_ns_wrapper(fd, nsid, data)
        })
    }
}

fn oxcache_id_zns_ns(fd: RawFd, nsid: u32) -> Result<nvme_zns_id_ns, NVMeError> {
    unsafe {
        id_ns_fn(fd, nsid, |fd, nsid, data| {
            nvme_zns_identify_ns_wrapper(fd, nsid, data)
        })
    }
}

fn oxcache_id_ctrl(fd: RawFd) -> Result<nvme_id_ctrl, NVMeError> {
    unsafe {
        let mut data: nvme_id_ctrl = mem::zeroed();
        let status = nvme_identify_ctrl_wrapper(fd, &mut data);
        match check_error(status) {
            Some(err) => return Err(err),
            None => Ok(data),
        }
    }
}

fn oxcache_id_zns_ctrl(fd: RawFd) -> Result<nvme_zns_id_ctrl, NVMeError> {
    unsafe {
        let mut data: nvme_zns_id_ctrl = mem::zeroed();
        let status = nvme_zns_identify_ctrl_wrapper(fd, &mut data);
        match check_error(status) {
            Some(err) => return Err(err),
            None => Ok(data),
        }
    }
}

fn device_name(device: &str) -> &str {
    if device.starts_with("/dev/") {
        device.trim_start_matches("/dev/")
    } else {
        device
    }
}

/// Retrieves ZNS configuration and metadata for a device by opening it and querying its properties.
/// Returns a `ZNSConfig` struct on success or an `NVMeError` on failure.
// Trim the device name so that it's only the filename after /dev/
pub fn nvme_get_info(device: &str) -> Result<NVMeConfig, NVMeError> {
    let device_name_ = device_name(device);
    let fd: RawFd = match open_device(device_name_) {
        Ok(opened_fd) => opened_fd,
        Err(err) => return Err(err),
    };

    let ctrl_data = oxcache_id_ctrl(fd)?;
    let maximum_data_transfer_size = ctrl_data.mdts as usize;

    let mut nsid = 0;
    unsafe {
        let status = nvme_get_nsid(fd, &mut nsid);
        match check_error(status) {
            Some(err) => return Err(err),
            None => {}
        }
    }

    let ns_data = oxcache_id_ns(fd, nsid)?;

    let current_lba_index = {
        // Lower 4 bits
        let mut bits: u32 = shift_and_mask(
            ns_data.flbas,
            nvme_flbas_NVME_FLBAS_LOWER_SHIFT,
            nvme_flbas_NVME_FLBAS_LOWER_MASK,
        );

        // Upper 4 bits needed if number of formats > 16
        if ns_data.nlbaf > 16 {
            bits |= shift_and_mask::<u8, u32, u32>(
                ns_data.flbas,
                nvme_flbas_NVME_FLBAS_HIGHER_SHIFT,
                nvme_flbas_NVME_FLBAS_HIGHER_MASK,
            ) << 4
        }

        bits as usize
    };

    // LBA format
    let lbaf_fmt = ns_data.lbaf[current_lba_index];
    let logical_block_size = 1 << lbaf_fmt.ds;
    // We can also get the total size of the controller via nvme_id_ctrl
    let total_size_in_bytes = ns_data.nuse * logical_block_size;

    Ok(NVMeConfig {
        fd,
        nsid,
        logical_block_size,
        total_size_in_bytes,
        current_lba_index,
        lba_perf: 0,
        timeout: 0,
        maximum_data_transfer_size,
    })
}

/// Retrieves ZNS configuration and metadata for a device by opening it and querying its properties.
/// Returns a `ZNSConfig` struct on success or an `NVMeError` on failure.
pub fn zns_get_info(nvme_config: &NVMeConfig) -> Result<ZNSConfig, NVMeError> {
    let zns_ns_data = oxcache_id_zns_ns(nvme_config.fd, nvme_config.nsid)?;
    // Max active & open resources
    // Add 1 because mar and mor ar 0 based values
    let mar = zns_ns_data.mar + 1;
    let mor = zns_ns_data.mor + 1;

    let zone_fmt = zns_ns_data.lbafe[nvme_config.current_lba_index];
    let zone_size = zone_fmt.zsze;
    let zdesc_ext_size = (zone_fmt.zdes * 64) as u64;
    let variable_zone_cap = zns_ns_data.zoc & 1 == 1;
    // Variable zone cap means that we'll have to check the return value every time for operations that modify zones. This shouldn't be common at all
    if variable_zone_cap {
        panic!("Variable zone capacity not yet implemented");
    }

    // Zone append size limit
    let zns_ctrl_data = oxcache_id_zns_ctrl(nvme_config.fd)?;
    let zasl = if zns_ctrl_data.zasl == 0 {
        0
    } else {
        (1 << zns_ctrl_data.zasl) * nvme_config.logical_block_size as Byte
    };

    // Number of zones
    let nzones = match get_num_zones(nvme_config.fd, nvme_config.nsid) {
        Ok(nz) => nz,
        Err(err) => return Err(err),
    };

    // Zone cap
    let zone_cap = match get_zone_capacity(nvme_config.fd, nvme_config.nsid) {
        Ok(nz) => nz,
        Err(err) => return Err(err),
    };

    Ok(ZNSConfig {
        max_active_resources: mar,
        max_open_resources: mor,
        num_zones: nzones,
        zasl: zasl,
        zone_descriptor_extension_size: zdesc_ext_size,
        zone_cap: zone_cap,
        zone_size: zone_size,
        chunks_per_zone: 0,
        chunk_size: 0,
    })
}

/// Returns the number of zones for the given NVMe device and namespace.
pub fn get_num_zones(fd: RawFd, nsid: u32) -> Result<Zone, NVMeError> {
    match report_zones(fd, nsid, 0, 0, 0) {
        Ok((nz, _)) => Ok(nz),
        Err(err) => Err(err),
    }
}

/// Returns the zone capacity for the given NVMe device and namespace.
pub fn get_zone_capacity(fd: RawFd, nsid: u32) -> Result<LogicalBlock, NVMeError> {
    match report_zones(fd, nsid, 0, 1, 0) {
        Ok((_, inf)) => Ok(inf[0].zone_capacity),
        Err(err) => Err(err),
    }
}

/// Returns the zone capacity for the given NVMe device and namespace.
pub fn get_zone_state(fd: RawFd, nsid: u32) -> Result<LogicalBlock, NVMeError> {
    match report_zones(fd, nsid, 0, 1, 0) {
        Ok((_, inf)) => Ok(inf[0].zone_capacity),
        Err(err) => Err(err),
    }
}

/// Returns a report of all zones for the given NVMe device and namespace.
/// The result includes the number of zones and a vector of zone descriptors.
pub fn report_zones_all(fd: RawFd, nsid: u32) -> Result<(Zone, Vec<ZNSZoneDescriptor>), NVMeError> {
    match get_num_zones(fd, nsid) {
        Ok(nz) => report_zones(fd, nsid, 0, nz, 0),
        Err(err) => Err(err),
    }
}

/// Retrieves a report of zones from a Zoned Namespace (ZNS) NVMe device.
/// Returns the number of zones and a vector of zone descriptors for the reported zones.
pub fn report_zones(
    fd: RawFd,
    nsid: u32,
    starting_address: LogicalBlock,
    report_count: Zone, // 0 to just report the number of zones
    timeout: u32,
) -> Result<(Zone, Vec<ZNSZoneDescriptor>), NVMeError> {
    // dword0, has nothing useful
    let mut result: u32 = 0;

    let data_size_in_bytes = nr_hdr_sz + zone_desc_sz * report_count as usize;
    let mut data_buffer = vec![0_u64; data_size_in_bytes / u64_size];

    // extended reports store a little extra data in each zone, unneeded in this case
    let err = unsafe {
        nvme_zns_report_zones_wrapper(
            fd,
            nsid,
            starting_address,
            nvme_zns_report_options_NVME_ZNS_ZRAS_REPORT_ALL,
            false,
            report_count != 0,
            data_size_in_bytes as u32,
            data_buffer.as_mut_ptr() as *mut c_void,
            timeout,
            &mut result,
        )
    };

    match check_error(err) {
        Some(err) => return Err(err),
        None => {}
    }

    // The first 8 bytes reports the number of zones
    let nzones = data_buffer[0];

    // We skip 56 bytes to get to the first zone descriptor
    let zone_descriptors = {
        if data_buffer.len() == 8 {
            Vec::new()
        } else {
            // Transform data from
            data_buffer[8..]
                .iter()
                .as_slice()
                .chunks_exact(8)
                .map(|data: &[u64]| {
                    let data_ptr = data.as_ptr() as *const nvme_zns_desc;
                    let zns_desc = unsafe { std::ptr::read(data_ptr) };
                    let zs: u8 = zns_desc.zs >> 4;
                    ZNSZoneDescriptor {
                        seq_write_required: (zns_desc.zt & 0b111) == 2,
                        zone_state: zs.try_into().unwrap_or_else(|e| {
                            panic!("Invalid zone state: {:?} from raw value {}", e, zs)
                        }),
                        zone_capacity: zns_desc.zcap,
                        zone_start_address: zns_desc.zslba,
                        write_pointer: zns_desc.wp,
                    }
                })
                .collect::<Vec<ZNSZoneDescriptor>>()
        }
    };

    Ok((nzones, zone_descriptors))
}

pub fn is_zoned_device(device: &str) -> Result<bool, io::Error> {
    let device_name_ = device_name(device);
    let zoned = fs::read_to_string(format!("/sys/block/{}/queue/zoned", device_name_))?;
    Ok(zoned.starts_with("host-managed") || zoned.starts_with("host-aware"))
}

/// Zone size is in logical blocks
pub fn get_lba_at(zone_index: Zone, chunk_index: Chunk, zone_size: LogicalBlock, chunk_size: LogicalBlock) -> LogicalBlock {
    zone_size * zone_index + chunk_index * chunk_size
}

pub fn get_address_at(zone_index: Zone, chunk_index: Chunk, zone_size: LogicalBlock, chunk_size: LogicalBlock, lba_size: Byte) -> Byte {
    (zone_size * zone_index + chunk_index * chunk_size) * lba_size
}
