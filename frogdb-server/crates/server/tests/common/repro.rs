//! Repro-file read/write for the generated-workload seed sweep.
//!
//! On a sweep failure the harness writes
//! `target/concurrency-repros/<seed>-<profile>-<ops>.json` capturing the exact
//! `(seed, profile, config)` so the failure can be replayed in isolation via
//! `just concurrency-repro <file>`.

#![allow(dead_code)]

use std::path::{Path, PathBuf};

use frogdb_testing::Profile;
use serde::{Deserialize, Serialize};

/// Everything needed to deterministically reconstruct a failing run.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ReproFile {
    pub seed: u64,
    pub profile: Profile,
    pub num_clients: usize,
    pub ops_per_client: usize,
    pub num_shards: usize,
}

/// Serialize a repro file as pretty JSON, creating parent directories.
pub fn write_repro(path: &Path, repro: &ReproFile) -> std::io::Result<()> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let json = serde_json::to_string_pretty(repro)
        .map_err(|e| std::io::Error::other(format!("serialize repro: {e}")))?;
    std::fs::write(path, json)
}

/// Read a repro file from JSON.
pub fn read_repro(path: impl AsRef<Path>) -> ReproFile {
    let data = std::fs::read_to_string(path.as_ref())
        .unwrap_or_else(|e| panic!("read repro {}: {e}", path.as_ref().display()));
    serde_json::from_str(&data)
        .unwrap_or_else(|e| panic!("parse repro {}: {e}", path.as_ref().display()))
}

/// The default repro-file path for a run, keyed by `(seed, profile, ops)`:
/// `<workspace-root>/target/concurrency-repros/<seed>-<profile>-<ops>.json`.
///
/// The key is the full triple rather than the bare seed because the same seed is reused
/// across profiles and workload sizes (`seed_sweep_short_workloads` and `seed_sweep_txheavy`
/// both walk `0..20`, and the nightly tier reruns those seeds at a larger `ops_per_client`).
/// Keying on the seed alone made those runs collide: the last failure to be written silently
/// overwrote every earlier one, so the uploaded artifact directory could not describe more
/// than one failing configuration per seed and `just concurrency-repro` would replay the
/// wrong workload.
pub fn repro_path(seed: u64, profile: Profile, ops_per_client: usize) -> PathBuf {
    concurrency_repro_dir().join(format!(
        "{seed}-{}-{ops_per_client}.json",
        profile_slug(profile)
    ))
}

/// Lowercase filename-safe profile token (`TxHeavy` -> `txheavy`).
fn profile_slug(profile: Profile) -> String {
    format!("{profile:?}").to_lowercase()
}

/// The repro directory, anchored deterministically rather than trusting the test process's
/// CWD: nextest runs each test binary with CWD set to its *crate* root
/// (`frogdb-server/crates/server`), not the repo root, so a CWD-relative `target/...` path
/// silently resolves to `frogdb-server/crates/server/target/concurrency-repros` — a directory
/// nothing else reads or uploads. Honor `CARGO_TARGET_DIR` if the build was configured with a
/// custom target dir; otherwise derive the workspace root from this crate's
/// `CARGO_MANIFEST_DIR`, which `rustc` bakes in at compile time and is therefore independent of
/// the runtime CWD.
fn concurrency_repro_dir() -> PathBuf {
    match std::env::var_os("CARGO_TARGET_DIR") {
        Some(dir) => PathBuf::from(dir).join("concurrency-repros"),
        None => workspace_root().join("target").join("concurrency-repros"),
    }
}

/// The workspace root: `CARGO_MANIFEST_DIR` for this crate is
/// `<workspace-root>/frogdb-server/crates/server`, so the workspace root is three ancestors up.
fn workspace_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .ancestors()
        .nth(3)
        .expect("CARGO_MANIFEST_DIR (frogdb-server/crates/server) has at least 3 ancestors")
        .to_path_buf()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn repro_round_trips() {
        let dir = std::env::temp_dir().join(format!("frogdb-repro-{}", std::process::id()));
        let path = dir.join("7.json");
        let r = ReproFile {
            seed: 7,
            profile: Profile::TxHeavy,
            num_clients: 4,
            ops_per_client: 30,
            num_shards: 2,
        };
        write_repro(&path, &r).unwrap();
        assert_eq!(read_repro(&path), r);
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// The path key is the full `(seed, profile, ops)` triple: two runs of the same seed at
    /// different profiles or workload sizes must not overwrite each other's repro file.
    #[test]
    fn repro_path_is_keyed_by_seed_profile_and_ops() {
        let base = repro_path(42, Profile::Mixed, 30);
        assert!(
            base.ends_with("target/concurrency-repros/42-mixed-30.json"),
            "unexpected repro path {}",
            base.display()
        );
        assert_ne!(
            base,
            repro_path(42, Profile::TxHeavy, 30),
            "profile ignored"
        );
        assert_ne!(base, repro_path(42, Profile::Mixed, 90), "ops ignored");
        assert_ne!(base, repro_path(43, Profile::Mixed, 30), "seed ignored");
    }

    /// `repro_path()` must resolve to the workspace-root `target/concurrency-repros/`,
    /// regardless of the test process's CWD — nextest runs test binaries with CWD set to the
    /// crate root, not the repo root, so a CWD-relative path would silently miss the directory
    /// the nightly CI workflow uploads as an artifact.
    #[test]
    fn repro_path_resolves_under_workspace_root_target_dir() {
        let path = repro_path(42, Profile::Mixed, 30);
        assert!(
            path.ends_with("target/concurrency-repros/42-mixed-30.json"),
            "expected path to end with target/concurrency-repros/42-mixed-30.json, got {}",
            path.display()
        );

        // Workspace root is `CARGO_MANIFEST_DIR` (this crate: frogdb-server/crates/server)
        // minus three components (server, crates, frogdb-server) — verify the repo's own
        // Cargo.toml (the workspace manifest) actually lives there, so this isn't just
        // asserting the suffix in isolation.
        if std::env::var_os("CARGO_TARGET_DIR").is_none() {
            let workspace_root = path
                .parent() // target/concurrency-repros
                .and_then(Path::parent) // target
                .and_then(Path::parent) // workspace root
                .expect("repro_path has at least 3 ancestors");
            assert!(
                workspace_root.join("Cargo.toml").is_file(),
                "expected {} to be the workspace root (contain Cargo.toml)",
                workspace_root.display()
            );
        }
    }
}
