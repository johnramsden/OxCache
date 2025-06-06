#![allow(non_upper_case_globals)]
#![allow(non_camel_case_types)]
#![allow(non_snake_case)]

use std::{
    ffi::{CStr, CString, c_void},
    os::fd::RawFd,
    ptr::null,
};

use libnvme_sys::bindings::{nvme_open, *};

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
pub fn zns_nvme_open(device_name: &str) -> Result<RawFd, ()> {
    unsafe { 
		let fd = nvme_open(CString::new(device_name).unwrap().as_ptr());
		if fd == -1 {
			Err(())
		} else {
			Ok(fd)
		}
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
            return Err(error_code)
        }
    }

    return Ok(result);
}

