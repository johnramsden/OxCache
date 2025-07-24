use std::fs;
use std::path::Path;
use std::process::Command;

fn emit_rerun_if_changed_recursive<P: AsRef<Path>>(path: P) {
    let path = path.as_ref();
    if path.is_dir() {
        for entry in fs::read_dir(path).unwrap() {
            let entry = entry.unwrap();
            let entry_path = entry.path();
            if entry_path.is_dir() {
                emit_rerun_if_changed_recursive(entry_path);
            } else {
                println!("cargo:rerun-if-changed={}", entry_path.display());
            }
        }
    } else {
        println!("cargo:rerun-if-changed={}", path.display());
    }
}

fn main() {
    let out_dir = std::env::var("OUT_DIR").unwrap();
    let manifest_path = std::env::var("CARGO_MANIFEST_DIR").unwrap();
    let libnvme_dir = format!("{}/external/libnvme", manifest_path);
    let build_dir = format!("{}/libnvme_build/", out_dir);

    // Only run meson setup if build directory doesn't exist
    // This seems buggy. If the meson compile is complaining then it
    // may be due to this failing to run. Remove the check if needed
    if !Path::new(&build_dir).exists() {
        let status = Command::new("meson")
            .arg("setup")
            .arg(&build_dir)
            .arg(&libnvme_dir)
            .status()
            .expect("failed to run meson setup");
        assert!(status.success(), "Meson setup failed");
    }

    // Build the project
    let status = Command::new("meson")
        .arg("compile")
        .arg("-C")
        .arg(&build_dir)
        .status()
        .expect("failed to run meson compile");
    assert!(status.success(), "Meson compile failed");

    // Tell cargo to look for shared libraries in the specified directory
    println!("cargo:rustc-link-search={}/src", build_dir);
    println!("cargo:rustc-link-lib=nvme");
    println!("cargo:rustc-link-lib=libnvme_wrapper");

    emit_rerun_if_changed_recursive("external/libnvme");
    println!("cargo:rerun-if-changed=src/libnvme_wrapper.h");
    println!("cargo:rerun-if-changed=src/libnvme_wrapper.c");

    cc::Build::new()
        .file("src/libnvme_wrapper.c")
        .include(format!("{}/src", libnvme_dir))
        .flag("-Wno-unused-parameter")
        .compile("libnvme_wrapper");

    // The bindgen::Builder is the main entry point
    // to bindgen, and lets you build up options for
    // the resulting bindings.
    let bindings = bindgen::Builder::default()
        // The input header we would like to generate
        // bindings for.
        .header("src/libnvme_wrapper.h")
        .clang_arg(format!("-I{}/src", libnvme_dir))
        // Tell cargo to invalidate the built crate whenever any of the
        // included header files changed.
        .parse_callbacks(Box::new(bindgen::CargoCallbacks::new()))
        .blocklist_type("nvme_resv_status")
        .generate_comments(false)
        // Finish the builder and generate the bindings.
        .generate()
        // Unwrap the Result and panic on failure.
        .expect("Unable to generate bindings");

    // Write the bindings to OUT_DIR/bindings.rs
    let out = Path::new(out_dir.as_str()).join("bindings.rs");
    bindings
        .write_to_file(out)
        .expect("Couldn't write bindings!");
}
