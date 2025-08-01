use std::{
    ffi::{CStr, c_void},
    ops::{BitAnd, Shr},
    ptr::null,
};

use errno::errno;
use libnvme_sys::bindings::{
    nvme_status_field, nvme_status_field_NVME_SC_SUCCESS, nvme_status_to_errno,
    nvme_status_to_string,
};

use crate::types::NVMeError;

pub const nullptr: *mut c_void = null::<c_void>() as *mut c_void;

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
#[allow(dead_code)]
pub fn get_errno(status: nvme_status_field) -> u8 {
    unsafe { nvme_status_to_errno(status as i32, false) }
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
pub fn get_error_string(status: nvme_status_field) -> &'static str {
    // These are all defined as static const char in the C code, so they should have static lifetime
    unsafe {
        CStr::from_ptr(nvme_status_to_string(status as i32, false))
            .to_str()
            .unwrap()
    }
}

/// Checks the result of an NVMe operation and returns an error if one occurred.
///
/// # Arguments
///
/// * `status` - The status code returned by an NVMe operation.
///
/// # Returns
///
/// * `Some(NVMeError)` if an error occurred, or `None` if successful.
///
pub fn check_error(status: i32) -> Option<NVMeError> {
    // Check if errno is set
    // The majority of libnvme functions will either return -1 and set errno, or return some status result that can be extracted as a string.
    if status == -1 {
        Some(NVMeError::Errno(errno()))
    } else if status as u32 != nvme_status_field_NVME_SC_SUCCESS {
        Some(NVMeError::StatusResult(status as u32))
    } else {
        None
    }
}

/// Shifts a value right by a given amount and applies a mask.
///
/// # Arguments
///
/// * `to_shift` - The value to shift.
/// * `shift` - The number of bits to shift right.
/// * `mask` - The mask to apply after shifting.
///
/// # Returns
///
/// * The result of shifting and masking, converted to the desired type.
///
pub fn shift_and_mask<ToShift, ShiftAs, Result>(
    to_shift: ToShift,
    shift: ShiftAs,
    mask: ShiftAs,
) -> Result
where
    ToShift: Into<ShiftAs>,
    ShiftAs: Shr<ShiftAs, Output = ShiftAs> + BitAnd<ShiftAs, Output = ShiftAs> + Into<Result>,
{
    ((to_shift.into() >> shift) & mask).into()
}
