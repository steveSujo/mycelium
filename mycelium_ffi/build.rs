use std::env;
use std::path::PathBuf;
use std::process::Command;

fn main() {
    let _out_dir = PathBuf::from(env::var("OUT_DIR").unwrap());
    let header_dir = PathBuf::from("include");

    std::fs::create_dir_all(&header_dir).expect("Failed to create include directory");

    // Only run cbindgen if it is available on the system.
    // If it's not found, we print a warning instead of panicking,
    // as the core logic and tests don't depend on the header file.
    let status = Command::new("cbindgen")
        .arg("--config")
        .arg("cbindgen.toml")
        .arg("--output")
        .arg("include/mycelium.h")
        .status();

    match status {
        Ok(s) if s.success() => println!("cbindgen header generated"),
        Ok(_) => println!("cargo:warning=cbindgen failed to generate header"),
        Err(e) => println!(
            "cargo:warning=cbindgen not found, skipping header generation: {}",
            e
        ),
    }

    println!("cargo:rerun-if-changed=src/lib.rs");
    println!("cargo:rerun-if-changed=cbindgen.toml");
}
