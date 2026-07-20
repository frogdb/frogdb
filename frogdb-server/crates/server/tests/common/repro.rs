//! Repro-file read/write for the generated-workload seed sweep.
//!
//! On a sweep failure the harness writes `target/concurrency-repros/<seed>.json`
//! capturing the exact `(seed, profile, config)` so the failure can be replayed
//! in isolation via `just concurrency-repro <file>`.

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

/// The default repro-file path for a seed: `target/concurrency-repros/<seed>.json`.
pub fn repro_path(seed: u64) -> PathBuf {
    PathBuf::from("target/concurrency-repros").join(format!("{seed}.json"))
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
}
