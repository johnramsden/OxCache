#![allow(non_upper_case_globals)]
#![allow(non_camel_case_types)]
#![allow(non_snake_case)]
#[allow(warnings)] // suppress all warnings from bindings
pub mod bindings {
    include!(concat!(env!("OUT_DIR"), "/bindings.rs"));
    
    // Included here because "gen" is a keyword in Rust, so this struct needs to be modified manually to incorporate that
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
}



