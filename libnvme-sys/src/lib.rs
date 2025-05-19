#![allow(non_upper_case_globals)]
#![allow(non_camel_case_types)]
#![allow(non_snake_case)]

include!(concat!(env!("OUT_DIR"), "/bindings.rs"));

// Included here because "gen" is a keyword in Rust, so this struct needs to be modified manually to incorporate that
#[doc = " struct nvme_resv_status - Reservation Status Data Structure\n @gen:\tGeneration\n @rtype:\tReservation Type\n @regctl:\tNumber of Registered Controllers\n @rsvd7:\tReserved\n @ptpls:\tPersist Through Power Loss State\n @rsvd10:\tReserved\n @rsvd24:\tReserved\n @regctl_eds: Registered Controller Extended Data Structure\n @regctl_ds:\tRegistered Controller Data Structure"]
#[repr(C)]
pub struct nvme_resv_status {
    pub r#gen: __le32,
    pub rtype: __u8,
    pub regctl: [__u8; 2usize],
    pub rsvd7: [__u8; 2usize],
    pub ptpls: __u8,
    pub rsvd10: [__u8; 14usize],
    pub __bindgen_anon_1: nvme_resv_status__bindgen_ty_1,
}
