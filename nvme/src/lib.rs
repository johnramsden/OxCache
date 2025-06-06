#![allow(non_upper_case_globals)]
#![allow(non_camel_case_types)]
#![allow(non_snake_case)]

use std::{
    ffi::{CStr, CString, c_void},
    os::fd::{FromRawFd, IntoRawFd, OwnedFd, RawFd},
    ptr::null,
};

use libnvme_sys::bindings::{nvme_open, *};

pub struct ZNSAppendResult {
    error_code: nvme_status_field,
    written_addr: u64,
}

pub fn get_error_string(status: nvme_status_field) -> &'static str {
    // These are all defined as static const char in the C code, so they should have static lifetime
    unsafe {
        CStr::from_ptr(nvme_status_to_string(status as i32, false))
            .to_str()
            .unwrap()
    }
}

pub fn get_errno(status: nvme_status_field) -> u8 {
    unsafe { nvme_status_to_errno(status as i32, false) }
}

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

