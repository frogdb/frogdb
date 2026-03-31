//! Build script for the frogdb-debug crate.
//!
//! Automatically vendors JS/CSS assets from npm packages when `package.json`
//! or `bun.lock` change. Falls back gracefully to existing checked-in assets
//! when `bun` is not available (e.g., CI environments).

use std::process::Command;

fn main() {
    let manifest_dir = std::env::var("CARGO_MANIFEST_DIR").unwrap();

    // Only rerun when the package manifest or lockfile change.
    println!("cargo:rerun-if-changed=package.json");
    println!("cargo:rerun-if-changed=bun.lock");

    let Ok(install) = Command::new("bun")
        .args(["install", "--frozen-lockfile"])
        .current_dir(&manifest_dir)
        .status()
    else {
        println!("cargo:warning=bun not found; using existing vendored assets");
        return;
    };

    if !install.success() {
        println!("cargo:warning=bun install failed; using existing vendored assets");
        return;
    }

    let Ok(vendor) = Command::new("bun")
        .args(["run", "vendor"])
        .current_dir(&manifest_dir)
        .status()
    else {
        println!("cargo:warning=bun run vendor failed; using existing vendored assets");
        return;
    };

    if !vendor.success() {
        println!("cargo:warning=bun run vendor failed; using existing vendored assets");
    }
}
