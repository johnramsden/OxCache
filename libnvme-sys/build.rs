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
			.arg("--default-library=static")
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
    println!("cargo:rustc-link-lib=static=nvme");
    println!("cargo:rustc-link-lib=static=nvme-mi");

	println!("cargo:rerun-if-changed=libnvme_wrapper.h");

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
