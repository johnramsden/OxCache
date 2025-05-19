use std::env;
use std::path::PathBuf;
use std::process::Command;
use std::path::Path;

fn main() {

    let out_dir = std::env::var("OUT_DIR").unwrap();
    let libnvme_dir = "external/libnvme";
    let build_dir = format!("{}/libnvme_build", out_dir);

    // Only run meson setup if build directory doesn't exist
    if !Path::new(&build_dir).exists() {
        let status = Command::new("meson")
            .arg("setup")
            .arg(&build_dir)
            .arg(&libnvme_dir) 
            .status()
            .expect("failed to run meson setup");
        assert!(status.success(), "Meson setup failed");
    }

    // Now build the project
    let status = Command::new("meson")
        .arg("compile")
        .arg("-C")
        .arg(&build_dir)
        .status()
        .expect("failed to run meson compile");
    assert!(status.success(), "Meson compile failed");

    // Optionally link the resulting library
    println!("cargo:rustc-link-search=native={}/meson_build", out_dir);
    println!("cargo:rustc-link-lib=static=your_library_name"); // adjust as needed

    // Tell cargo to look for shared libraries in the specified directory
    println!("cargo:rustc-link-search=/path/to/lib");

    // Tell cargo to tell rustc to link the system bzip2
    // shared library.
    println!("cargo:rustc-link-lib=bz2");

    // The bindgen::Builder is the main entry point
    // to bindgen, and lets you build up options for
    // the resulting bindings.
    let bindings = bindgen::Builder::default()
        // The input header we would like to generate
        // bindings for.
        .header("wrapper.h")
        // Tell cargo to invalidate the built crate whenever any of the
        // included header files changed.
        .parse_callbacks(Box::new(bindgen::CargoCallbacks::new()))
        // Finish the builder and generate the bindings.
        .generate()
        // Unwrap the Result and panic on failure.
        .expect("Unable to generate bindings");

    // Write the bindings to the $OUT_DIR/bindings.rs file.
    let out_path = PathBuf::from(env::var("OUT_DIR").unwrap());
    bindings
        .write_to_file(out_path.join("bindings.rs"))
        .expect("Couldn't write bindings!");
}
