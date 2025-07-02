use std::{
    ffi::{CString, c_void},
    ops::Rem,
    os::fd::RawFd,
    ptr::null,
};

use errno::errno;
use libnvme_sys::bindings::*;

use crate::{
    types::{NVMeError, PerformOn, ZNSConfig},
    util::{check_error, nullptr},
};

/// Opens an NVMe device and returns its file descriptor.
///
/// This function does **not** require you to prepend "/dev/" to the device name.
/// For example, `"nvme0n1"` is sufficient.
///
/// # Arguments
///
/// * `device_name` - The name of the NVMe device (e.g., `"nvme0n1"`).
///
/// # Returns
///
/// * `Ok(RawFd)` - File descriptor for the opened NVMe device.
/// * `Err(NVMeError)` - If the operation fails.
///
/// # Example
///
/// ```rust
/// use nvme::ops::zns_open;
///
/// let fd = zns_open("nvme0n1").expect("Failed to open NVMe device");
/// ```
pub fn zns_open(device_name: &str) -> Result<RawFd, NVMeError> {
    match unsafe { nvme_open(CString::new(device_name).unwrap().as_ptr()) } {
        -1 => Err(NVMeError::Errno(errno())),
        fd => Ok(fd),
    }
}

/// Appends data to a ZNS (Zoned Namespace) zone on an NVMe device.
///
/// # Arguments
///
/// * `config` - ZNSConfig containing device parameters.
/// * `zone_index` - The index of the zone to append to.
/// * `data` - Mutable buffer containing data to append. Length must be a multiple of `logical_block_size`.
///
/// # Returns
///
/// * `Ok(u64)` - Starting LBA (in logical blocks) where the data was appended.
/// * `Err(NVMeError)` - If the operation fails or data buffer is not aligned.
///
/// # Example
///
/// ```rust
/// use nvme::ops::{zns_open, zns_append};
/// use nvme::types::ZNSConfig;
///
/// let fd = zns_open("nvme0n1").unwrap();
/// let config = ZNSConfig {
///     fd,
///     timeout: 1000,
///     nsid: 1,
///     block_size: 4096,
/// };
/// let mut buffer = vec![0u8; 4096];
/// let lba = zns_append(config, 0, &mut buffer, 4096).unwrap();
/// ```
pub fn zns_append(config: &ZNSConfig, zone_index: u64, data: &mut [u8]) -> Result<u64, NVMeError> {
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

    if (data.len() & (config.block_size - 1) as usize) != 0 {
        return Err(NVMeError::UnalignedDataBuffer {
            want: config.block_size,
            has: data.len() as u64,
        });
    }

    if config.zasl < data.len() as u32 {
        return Err(NVMeError::AppendSizeTooLarge {
            max_append: config.zasl,
            trying_to_append: data.len() as u32,
        });
    }

    // zslba is the starting logical block address
    let mut args: nvme_zns_append_args = nvme_zns_append_args {
        zslba: config.get_starting_addr(zone_index),
        result: &mut result,
        data: data.as_mut_ptr() as *mut c_void,
        metadata: nullptr, // type *mut c_void
        args_size: size_of::<nvme_zns_append_args>() as i32,
        fd: config.fd,
        timeout: config.timeout,
        nsid: config.nsid,
        // Only used for end-to-end protection
        ilbrt: 0,                    // Initial logical block reference tag
        data_len: data.len() as u32, // nvme-cli sets these as bytes
        metadata_len: 0,             // Unimplemented
        nlb: (data.len() / config.block_size as usize - 1) as u16, // 0 based value
        control: ctrl,
        lbat: 0,
        lbatm: 0,
        rsvd1: [0; 4usize],
        ilbrt_u64: 0,
    };

    match check_error(unsafe { nvme_zns_append(&mut args) }) {
        Some(err) => Err(err),
        None => Ok(result),
    }
}

pub fn zns_write(config: &ZNSConfig, zone_index: u64, offset: u64, data: &mut [u8]) -> Result<(), NVMeError> {
    if data.len().rem(config.block_size as usize) != 0 {
        return Err(NVMeError::UnalignedDataBuffer {
            want: config.block_size,
            has: data.len() as u64,
        });
    }

    // Zero based value, so subtract by 1
    let nlb = data.len() as u64 / config.block_size - 1;

    let mut args = nvme_io_args {
        slba: config.get_starting_addr(zone_index) + offset,
        storage_tag: 0,                    // End to end protection
        result: null::<u32>() as *mut u32, // submit_io in nvme-cli sets this as null
        data: data.as_mut_ptr() as *mut c_void,
        metadata: nullptr,
        args_size: size_of::<nvme_io_args>() as i32,
        fd: config.fd,
        timeout: config.timeout,
        nsid: config.nsid,
        reftag: 0,
        data_len: data.len() as u32,
        metadata_len: 0,
        nlb: nlb as u16,
        control: 0, // Not applicable in our case. It's DWORD12 in the command set spec
        apptag: 0,
        appmask: 0,
        // Dataset Management (DW13). This could be experimented but unsure of impact
        dspec: 0,
        dsm: 0,
        rsvd1: [0],
        reftag_u64: 0,
        sts: 0, // Storage tag size
        pif: 0, // Protection Information Format
    };

    unsafe {
        let result = nvme_io(&mut args, nvme_io_opcode_nvme_cmd_write.try_into().unwrap());
        match check_error(result) {
            Some(err) => Err(err),
            None => Ok(()),
        }
    }
}

pub fn write(slba: u64, fd: RawFd, timeout: u32, nsid: u32, block_size: u64, data: &mut [u8]) -> Result<(), NVMeError> {
    if data.len().rem(block_size as usize) != 0 {
        return Err(NVMeError::UnalignedDataBuffer {
            want: block_size,
            has: data.len() as u64,
        });
    }

    // Zero based value, so subtract by 1
    let nlb = data.len() as u64 / block_size - 1;

    let mut args = nvme_io_args {
        slba,
        storage_tag: 0,                    // End to end protection
        result: null::<u32>() as *mut u32, // submit_io in nvme-cli sets this as null
        data: data.as_mut_ptr() as *mut c_void,
        metadata: nullptr,
        args_size: size_of::<nvme_io_args>() as i32,
        fd,
        timeout,
        nsid,
        reftag: 0,
        data_len: data.len() as u32,
        metadata_len: 0,
        nlb: nlb as u16,
        control: 0, // Not applicable in our case. It's DWORD12 in the command set spec
        apptag: 0,
        appmask: 0,
        // Dataset Management (DW13). This could be experimented but unsure of impact
        dspec: 0,
        dsm: 0,
        rsvd1: [0],
        reftag_u64: 0,
        sts: 0, // Storage tag size
        pif: 0, // Protection Information Format
    };

    unsafe {
        let result = nvme_io(&mut args, nvme_io_opcode_nvme_cmd_write.try_into().unwrap());
        match check_error(result) {
            Some(err) => Err(err),
            None => Ok(()),
        }
    }
}
/// Reads data from a ZNS (Zoned Namespace) NVMe device.
///
/// # Arguments
///
/// * `config` - ZNSConfig containing device parameters.
/// * `zone_index` - The index of the zone to read from.
/// * `offset` - Offset (in logical blocks) from the start of the zone.
/// * `read_buffer` - Mutable buffer to store the read data. Length must be a multiple of `block_size`.
///
/// # Returns
///
/// * `Ok(())` - If the read was successful.
/// * `Err(NVMeError)` - If the operation fails or buffer is not aligned.
///
/// # Example
///
/// ```rust
/// use nvme::ops::{zns_open, zns_read};
/// use nvme::types::ZNSConfig;
///
/// let fd = zns_open("nvme0n1").unwrap();
/// let config = ZNSConfig {
///     fd,
///     timeout: 1000,
///     nsid: 1,
///     block_size: 4096,
/// };
/// let mut buffer = vec![0u8; 4096];
/// zns_read(config, 0, 0, &mut buffer).unwrap();
/// ```
pub fn zns_read(
    config: &ZNSConfig,
    zone_index: u64,
    offset: u64,
    read_buffer: &mut [u8],
) -> Result<(), NVMeError> {
    if read_buffer.len().rem(config.block_size as usize) != 0 {
        return Err(NVMeError::UnalignedDataBuffer {
            want: config.block_size,
            has: read_buffer.len() as u64,
        });
    }

    // Zero based value, so subtract by 1
    let nlb = read_buffer.len() as u64 / config.block_size - 1;

    let mut args = nvme_io_args {
        slba: config.get_starting_addr(zone_index) + offset,
        storage_tag: 0,                    // End to end protection
        result: null::<u32>() as *mut u32, // submit_io in nvme-cli sets this as null
        data: read_buffer.as_mut_ptr() as *mut c_void,
        metadata: nullptr,
        args_size: size_of::<nvme_io_args>() as i32,
        fd: config.fd,
        timeout: config.timeout,
        nsid: config.nsid,
        reftag: 0,
        data_len: read_buffer.len() as u32,
        metadata_len: 0,
        nlb: nlb as u16,
        control: 0, // Not applicable in our case. It's DWORD12 in the command set spec
        apptag: 0,
        appmask: 0,
        // Dataset Management (DW13). This could be experimented but unsure of impact
        dspec: 0,
        dsm: 0,
        rsvd1: [0],
        reftag_u64: 0,
        sts: 0, // Storage tag size
        pif: 0, // Protection Information Format
    };

    unsafe {
        let result = nvme_io(&mut args, nvme_io_opcode_nvme_cmd_read.try_into().unwrap());
        match check_error(result) {
            Some(err) => Err(err),
            None => Ok(()),
        }
    }
}

fn zone_op(
    config: &ZNSConfig,
    perform_on: PerformOn,
    action: nvme_zns_send_action,
) -> Result<(), NVMeError> {
    let mut args = nvme_zns_mgmt_send_args {
        slba: match perform_on {
            PerformOn::AllZones => 0,
            PerformOn::Zone(idx) => config.get_starting_addr(idx),
        },
        result: null::<u32>() as *mut u32,
        data: nullptr,
        args_size: size_of::<nvme_zns_mgmt_send_args>() as i32,
        fd: config.fd,
        timeout: config.timeout,
        nsid: config.nsid,
        zsa: action,
        data_len: 0,
        select_all: perform_on == PerformOn::AllZones,
        zsaso: 0, // Random write area specific, can ignore
    };
    match check_error(unsafe { nvme_zns_mgmt_send(&mut args) }) {
        None => Ok(()),
        Some(err) => Err(err),
    }
}

/// Opens a specific zone or all zones on a Zoned Namespace (ZNS) NVMe device.
///
/// # Arguments
///
/// * `config` - ZNSConfig containing device parameters.
/// * `perform_on` - Which zone(s) to operate on (`PerformOn::Zone(idx)` or `PerformOn::AllZones`).
///
/// # Returns
///
/// * `Ok(())` - If the operation succeeded.
/// * `Err(NVMeError)` - If the operation failed.
///
/// # Example
///
/// ```rust
/// use nvme::ops::{zns_open, open_zone};
/// use nvme::types::{ZNSConfig, PerformOn};
///
/// let fd = zns_open("nvme0n1").unwrap();
/// let config = ZNSConfig {
///     fd,
///     timeout: 1000,
///     nsid: 1,
///     block_size: 4096,
/// };
/// open_zone(config, PerformOn::Zone(0)).unwrap();
/// ```
pub fn open_zone(config: &ZNSConfig, perform_on: PerformOn) -> Result<(), NVMeError> {
    zone_op(config, perform_on, nvme_zns_send_action_NVME_ZNS_ZSA_OPEN)
}

/// Closes a specific zone or all zones on a Zoned Namespace (ZNS) NVMe device.
///
/// # Arguments
///
/// * `config` - ZNSConfig containing device parameters.
/// * `perform_on` - Which zone(s) to operate on (`PerformOn::Zone(idx)` or `PerformOn::AllZones`).
///
/// # Returns
///
/// * `Ok(())` - If the operation succeeded.
/// * `Err(NVMeError)` - If the operation failed.
///
/// # Example
///
/// ```rust
/// use nvme::ops::{zns_open, close_zone};
/// use nvme::types::{ZNSConfig, PerformOn};
///
/// let fd = zns_open("nvme0n1").unwrap();
/// let config = ZNSConfig {
///     fd,
///     timeout: 1000,
///     nsid: 1,
///     block_size: 4096,
/// };
/// close_zone(config, PerformOn::Zone(0)).unwrap();
/// ```
pub fn close_zone(config: &ZNSConfig, perform_on: PerformOn) -> Result<(), NVMeError> {
    zone_op(config, perform_on, nvme_zns_send_action_NVME_ZNS_ZSA_CLOSE)
}

/// Resets a specific zone or all zones on a Zoned Namespace (ZNS) NVMe device.
///
/// # Arguments
///
/// * `config` - ZNSConfig containing device parameters.
/// * `perform_on` - Which zone(s) to operate on (`PerformOn::Zone(idx)` or `PerformOn::AllZones`).
///
/// # Returns
///
/// * `Ok(())` - If the operation succeeded.
/// * `Err(NVMeError)` - If the operation failed.
///
/// # Example
///
/// ```rust
/// use nvme::ops::{zns_open, reset_zone};
/// use nvme::types::{ZNSConfig, PerformOn};
///
/// let fd = zns_open("nvme0n1").unwrap();
/// let config = ZNSConfig {
///     fd,
///     timeout: 1000,
///     nsid: 1,
///     block_size: 4096,
/// };
/// reset_zone(config, PerformOn::Zone(0)).unwrap();
/// ```
pub fn reset_zone(config: &ZNSConfig, perform_on: PerformOn) -> Result<(), NVMeError> {
    zone_op(config, perform_on, nvme_zns_send_action_NVME_ZNS_ZSA_RESET)
}
