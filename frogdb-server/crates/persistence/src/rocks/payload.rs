//! What a checkpoint payload *is*, and how a reader proves it before trusting
//! it.
//!
//! A "payload" here is one directory holding a complete RocksDB database: the
//! `checkpoint/` subtree of a `BGSAVE` snapshot, the `checkpoint_ready` dir a
//! replica full sync lands, and the directory an operator copies between the
//! two ([`crate::rocks::staged`] owns the names). Everything downstream — the
//! staged install, `frogctl backup checkpoint-verify` — asks the same two
//! questions of it:
//!
//! 1. **Structure.** Does `CURRENT` parse, and does the `MANIFEST-NNNNNN` it
//!    names exist and hold bytes? This is the cheap, always-available check.
//!    Before this module the whole completeness test was `CURRENT` *existing*,
//!    which a truncated operator copy or a half-finished download passes.
//! 2. **Payload manifest.** If the payload carries a
//!    [`PAYLOAD_MANIFEST_FILE`], is every file it lists still there, at the
//!    size it was written with — and, when the caller asks for it, hashing to
//!    the same value? This is the integrity story for a long-lived backup on
//!    media that bit-rots: the completion marker in `metadata.json` proves the
//!    *stager* finished, not that the bytes it wrote are still the bytes on
//!    disk.
//!
//! Deliberately *not* here: a hand-rolled reader for RocksDB's MANIFEST
//! (`VersionEdit` log) to enumerate the live SST set. That format is
//! version-coupled, and a mis-parse would refuse a *good* checkpoint — turning
//! a verification feature into an unbootable node. The party that owns the
//! format does that check instead: the staged install trial-opens the payload
//! through RocksDB itself before it touches the live database
//! (`RocksStore::trial_open_payload`), which resolves `CURRENT` → MANIFEST →
//! the files the MANIFEST references, using the code that writes them.
use serde::{Deserialize, Serialize};
use std::io::{self, Read};
use std::path::{Path, PathBuf};

/// The payload manifest a stager writes *inside* the payload directory.
///
/// Inside the payload — not in the snapshot's `metadata.json` — because the
/// restore path defined by FM-PERSISTENCE-021 copies `snapshot_NNNNN/checkpoint/.`
/// and leaves `metadata.json` behind, so a manifest that lived beside the
/// payload would never reach the installer that needs it. RocksDB ignores file
/// names it cannot parse, so carrying it inside the database directory is inert.
pub const PAYLOAD_MANIFEST_FILE: &str = "frogdb_payload.json";

/// RocksDB's manifest pointer: a one-line file naming the MANIFEST that
/// describes the database's current version.
pub const ROCKSDB_CURRENT_MANIFEST: &str = "CURRENT";

/// The prefix every RocksDB MANIFEST file name carries.
const MANIFEST_PREFIX: &str = "MANIFEST-";

/// Bound on how much of `CURRENT` is read before calling it malformed. The real
/// file is one short line; anything larger is not a manifest pointer, and this
/// keeps a hostile or corrupt payload from driving a large read.
const MAX_CURRENT_LEN: u64 = 4096;

/// Read buffer for checksumming payload files.
const HASH_BUF_LEN: usize = 256 * 1024;

/// Why a payload was refused. Every variant names the file it is about: the
/// refusal is read by an operator mid-incident, so "which file, and what was
/// wrong with it" is the whole point of the type.
#[derive(Debug, thiserror::Error)]
pub enum PayloadError {
    #[error("payload at {dir} has no {ROCKSDB_CURRENT_MANIFEST} manifest pointer: {source}")]
    UnreadableCurrent { dir: PathBuf, source: io::Error },
    #[error(
        "payload at {dir} has a malformed {ROCKSDB_CURRENT_MANIFEST} \
         (expected a single `{MANIFEST_PREFIX}NNNNNN` line, got {found:?})"
    )]
    MalformedCurrent { dir: PathBuf, found: String },
    #[error(
        "payload at {dir} names manifest {manifest} in {ROCKSDB_CURRENT_MANIFEST}, \
         but that file is missing or empty"
    )]
    MissingManifest { dir: PathBuf, manifest: String },
    #[error("payload manifest at {path} is unreadable: {source}")]
    UnreadablePayloadManifest { path: PathBuf, source: io::Error },
    #[error("payload manifest at {path} does not parse: {source}")]
    MalformedPayloadManifest {
        path: PathBuf,
        source: serde_json::Error,
    },
    #[error(
        "payload manifest at {path} is version {found}, but this build understands \
         version {expected}"
    )]
    UnsupportedPayloadManifest {
        path: PathBuf,
        found: u8,
        expected: u8,
    },
    #[error("payload file {name} listed in the manifest is missing from {dir}: {source}")]
    MissingFile {
        dir: PathBuf,
        name: String,
        source: io::Error,
    },
    #[error(
        "payload file {name} in {dir} is {actual} bytes, but the manifest recorded \
         {expected} (truncated or overwritten)"
    )]
    SizeMismatch {
        dir: PathBuf,
        name: String,
        expected: u64,
        actual: u64,
    },
    #[error(
        "payload file {name} in {dir} is the recorded size but hashes to {actual:#018x} \
         instead of {expected:#018x} (silent corruption)"
    )]
    ChecksumMismatch {
        dir: PathBuf,
        name: String,
        expected: u64,
        actual: u64,
    },
}

impl From<PayloadError> for io::Error {
    /// Every payload refusal is `InvalidData` on the install path: the
    /// directory is there, its contents are not what they claim to be.
    fn from(e: PayloadError) -> Self {
        io::Error::new(io::ErrorKind::InvalidData, e.to_string())
    }
}

/// One file of a payload, as recorded when the payload was written.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PayloadFile {
    /// Path relative to the payload directory, `/`-separated.
    pub name: String,
    pub size_bytes: u64,
    /// xxh3-64 of the file's contents. Not a cryptographic digest: this detects
    /// bit rot and truncation, not a forged backup (an attacker who can rewrite
    /// the payload can rewrite the manifest beside it).
    pub xxh3: u64,
}

/// The manifest of every file in a payload, written by the party that produced
/// it and checked by every party that consumes it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PayloadManifest {
    pub version: u8,
    pub files: Vec<PayloadFile>,
}

/// What a completed verification looked at, for the operator-facing report.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PayloadReport {
    /// The MANIFEST `CURRENT` pointed at.
    pub manifest: String,
    /// Was a [`PAYLOAD_MANIFEST_FILE`] present? Absent is not a failure — a
    /// full-sync payload is covered by the transfer's own checksum, and
    /// payloads written before this existed still install.
    pub payload_manifest_present: bool,
    /// Files listed in the payload manifest (0 when it is absent).
    pub files_checked: usize,
    /// Bytes covered by the size check.
    pub bytes_checked: u64,
    /// Were contents hashed, or only sized?
    pub checksums_verified: bool,
}

/// How hard [`verify_payload`] looks: sizes are a `stat` per file, checksums
/// read every byte.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PayloadCheck {
    /// Structure + every listed file present at its recorded size. What the
    /// staged install runs: it is O(files) syscalls on the boot path.
    Sizes,
    /// The above plus a content hash per file. What `frogctl backup
    /// checkpoint-verify` runs by default: an operator validating a backup
    /// before the outage they need it for is paying for the read deliberately.
    Checksums,
}

impl PayloadManifest {
    /// The only version this build writes.
    pub const VERSION: u8 = 1;

    /// Hash and size every file under `dir`, excluding the manifest file
    /// itself. Entries are sorted by name so two manifests of the same tree are
    /// byte-identical.
    pub fn build(dir: &Path) -> io::Result<Self> {
        let mut files = Vec::new();
        collect_files(dir, Path::new(""), &mut files)?;
        files.sort_by(|a, b| a.name.cmp(&b.name));
        Ok(Self {
            version: Self::VERSION,
            files,
        })
    }

    /// Serialize to the bytes that belong at `<dir>/frogdb_payload.json`.
    ///
    /// Returns bytes rather than writing, so the caller can publish them
    /// through its own durability seam (the snapshot stager writes every file
    /// through [`crate::fs_seam::SnapshotFs`]).
    pub fn to_bytes(&self) -> serde_json::Result<Vec<u8>> {
        serde_json::to_vec_pretty(self)
    }

    /// Read the manifest from `dir`, or `None` if the payload carries none.
    ///
    /// An *absent* manifest is a payload written by a producer that does not
    /// write one (a full sync, or a backup taken before this existed) and is
    /// not an error. A manifest that is present but unreadable or unparseable
    /// **is**: something wrote it, so its silence is damage rather than
    /// absence.
    pub fn read_from(dir: &Path) -> Result<Option<Self>, PayloadError> {
        let path = dir.join(PAYLOAD_MANIFEST_FILE);
        let raw = match std::fs::read(&path) {
            Ok(raw) => raw,
            Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(None),
            Err(source) => return Err(PayloadError::UnreadablePayloadManifest { path, source }),
        };
        let manifest: Self = serde_json::from_slice(&raw).map_err(|source| {
            PayloadError::MalformedPayloadManifest {
                path: path.clone(),
                source,
            }
        })?;
        if manifest.version != Self::VERSION {
            return Err(PayloadError::UnsupportedPayloadManifest {
                path,
                found: manifest.version,
                expected: Self::VERSION,
            });
        }
        Ok(Some(manifest))
    }

    /// Check every listed file against `dir`. Files present in `dir` but *not*
    /// listed are ignored: a manifest is a claim about the files it names, and
    /// an installed payload legitimately grows new files afterwards.
    fn verify_files(&self, dir: &Path, check: PayloadCheck) -> Result<u64, PayloadError> {
        let mut bytes = 0u64;
        for f in &self.files {
            let path = dir.join(&f.name);
            let meta = std::fs::metadata(&path).map_err(|source| PayloadError::MissingFile {
                dir: dir.to_path_buf(),
                name: f.name.clone(),
                source,
            })?;
            if meta.len() != f.size_bytes {
                return Err(PayloadError::SizeMismatch {
                    dir: dir.to_path_buf(),
                    name: f.name.clone(),
                    expected: f.size_bytes,
                    actual: meta.len(),
                });
            }
            if check == PayloadCheck::Checksums {
                let actual = hash_file(&path).map_err(|source| PayloadError::MissingFile {
                    dir: dir.to_path_buf(),
                    name: f.name.clone(),
                    source,
                })?;
                if actual != f.xxh3 {
                    return Err(PayloadError::ChecksumMismatch {
                        dir: dir.to_path_buf(),
                        name: f.name.clone(),
                        expected: f.xxh3,
                        actual,
                    });
                }
            }
            bytes += f.size_bytes;
        }
        Ok(bytes)
    }
}

/// Prove `dir` is a payload worth trusting: structure first, then the payload
/// manifest if it carries one.
///
/// This is the check every consumer shares — the staged install (before it
/// moves anything) and `frogctl backup checkpoint-verify` (on demand, against
/// a backup an operator is about to depend on).
pub fn verify_payload(dir: &Path, check: PayloadCheck) -> Result<PayloadReport, PayloadError> {
    let manifest = read_current_pointer(dir)?;
    match std::fs::metadata(dir.join(&manifest)) {
        Ok(m) if m.is_file() && m.len() > 0 => {}
        _ => {
            return Err(PayloadError::MissingManifest {
                dir: dir.to_path_buf(),
                manifest,
            });
        }
    }

    let payload_manifest = PayloadManifest::read_from(dir)?;
    let (files_checked, bytes_checked) = match &payload_manifest {
        Some(pm) => (pm.files.len(), pm.verify_files(dir, check)?),
        None => (0, 0),
    };
    Ok(PayloadReport {
        manifest,
        payload_manifest_present: payload_manifest.is_some(),
        files_checked,
        bytes_checked,
        checksums_verified: payload_manifest.is_some() && check == PayloadCheck::Checksums,
    })
}

/// Read `CURRENT` and return the MANIFEST file name it points at.
///
/// The whole point of parsing rather than `stat`ing: a partial copy that
/// happens to include a zero-length or garbage `CURRENT` passed the old check.
fn read_current_pointer(dir: &Path) -> Result<String, PayloadError> {
    let path = dir.join(ROCKSDB_CURRENT_MANIFEST);
    let meta = std::fs::metadata(&path).map_err(|source| PayloadError::UnreadableCurrent {
        dir: dir.to_path_buf(),
        source,
    })?;
    if meta.len() > MAX_CURRENT_LEN {
        return Err(PayloadError::MalformedCurrent {
            dir: dir.to_path_buf(),
            found: format!("{} bytes", meta.len()),
        });
    }
    let raw = std::fs::read(&path).map_err(|source| PayloadError::UnreadableCurrent {
        dir: dir.to_path_buf(),
        source,
    })?;
    let text = String::from_utf8_lossy(&raw);
    let name = text.trim();
    let digits = name.strip_prefix(MANIFEST_PREFIX).unwrap_or("");
    if digits.is_empty() || !digits.bytes().all(|b| b.is_ascii_digit()) {
        return Err(PayloadError::MalformedCurrent {
            dir: dir.to_path_buf(),
            found: name.chars().take(64).collect(),
        });
    }
    Ok(name.to_string())
}

/// Depth-first walk collecting one [`PayloadFile`] per regular file, with
/// `/`-separated names relative to the payload root. A RocksDB checkpoint is
/// flat today; the recursion is what keeps "every file in the payload" true if
/// that ever changes.
fn collect_files(root: &Path, rel: &Path, out: &mut Vec<PayloadFile>) -> io::Result<()> {
    for entry in std::fs::read_dir(root.join(rel))? {
        let entry = entry?;
        let name = rel.join(entry.file_name());
        let ft = entry.file_type()?;
        if ft.is_dir() {
            collect_files(root, &name, out)?;
            continue;
        }
        if !ft.is_file() {
            continue;
        }
        let rel_name = name.to_string_lossy().replace('\\', "/");
        if rel_name == PAYLOAD_MANIFEST_FILE {
            continue;
        }
        let path = root.join(&name);
        out.push(PayloadFile {
            size_bytes: entry.metadata()?.len(),
            xxh3: hash_file(&path)?,
            name: rel_name,
        });
    }
    Ok(())
}

/// Streamed xxh3-64 of a file's contents.
fn hash_file(path: &Path) -> io::Result<u64> {
    let mut f = std::fs::File::open(path)?;
    let mut hasher = xxhash_rust::xxh3::Xxh3::new();
    let mut buf = vec![0u8; HASH_BUF_LEN];
    loop {
        let n = f.read(&mut buf)?;
        if n == 0 {
            break;
        }
        hasher.update(&buf[..n]);
    }
    Ok(hasher.digest())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    /// A payload directory that passes the structural check: a `CURRENT`
    /// pointing at a non-empty MANIFEST, plus one "SST".
    fn plant_payload(dir: &Path) {
        std::fs::create_dir_all(dir).unwrap();
        std::fs::write(dir.join(ROCKSDB_CURRENT_MANIFEST), b"MANIFEST-000007\n").unwrap();
        std::fs::write(dir.join("MANIFEST-000007"), b"version edits").unwrap();
        std::fs::write(dir.join("000012.sst"), b"table data").unwrap();
    }

    // FM-PERSISTENCE-024
    /// `CURRENT` *existing* was the whole completeness check before this row was
    /// strengthened, and every one of these shapes passed it: an empty file, a
    /// pointer to a MANIFEST that never arrived, a pointer to one that arrived
    /// as zero bytes. Each is a real half-copied backup.
    #[test]
    fn a_current_pointer_that_resolves_to_nothing_is_not_a_payload() {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path().join("payload");
        std::fs::create_dir_all(&dir).unwrap();

        assert!(
            matches!(
                verify_payload(&dir, PayloadCheck::Sizes),
                Err(PayloadError::UnreadableCurrent { .. })
            ),
            "no CURRENT at all"
        );

        std::fs::write(dir.join(ROCKSDB_CURRENT_MANIFEST), b"").unwrap();
        assert!(
            matches!(
                verify_payload(&dir, PayloadCheck::Sizes),
                Err(PayloadError::MalformedCurrent { .. })
            ),
            "a zero-length CURRENT names no manifest"
        );

        std::fs::write(dir.join(ROCKSDB_CURRENT_MANIFEST), b"not-a-manifest\n").unwrap();
        assert!(
            matches!(
                verify_payload(&dir, PayloadCheck::Sizes),
                Err(PayloadError::MalformedCurrent { .. })
            ),
            "CURRENT must name a MANIFEST-NNNNNN"
        );

        std::fs::write(dir.join(ROCKSDB_CURRENT_MANIFEST), b"MANIFEST-000007\n").unwrap();
        assert!(
            matches!(
                verify_payload(&dir, PayloadCheck::Sizes),
                Err(PayloadError::MissingManifest { .. })
            ),
            "the MANIFEST it names has to be there"
        );

        std::fs::write(dir.join("MANIFEST-000007"), b"").unwrap();
        assert!(
            matches!(
                verify_payload(&dir, PayloadCheck::Sizes),
                Err(PayloadError::MissingManifest { .. })
            ),
            "a zero-length MANIFEST describes no version"
        );

        std::fs::write(dir.join("MANIFEST-000007"), b"version edits").unwrap();
        let report = verify_payload(&dir, PayloadCheck::Sizes).expect("now it resolves");
        assert_eq!(report.manifest, "MANIFEST-000007");
        assert!(
            !report.payload_manifest_present,
            "structure alone verifies with no payload manifest"
        );
    }

    // FM-PERSISTENCE-056
    /// The manifest is the difference between "the stager finished" and "the
    /// bytes are still here": a file that vanished, and one that was truncated
    /// on the way in, are both caught by a `stat` per entry.
    #[test]
    fn a_payload_manifest_catches_a_missing_or_truncated_file() {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path().join("payload");
        plant_payload(&dir);
        let manifest = PayloadManifest::build(&dir).unwrap();
        std::fs::write(
            dir.join(PAYLOAD_MANIFEST_FILE),
            manifest.to_bytes().unwrap(),
        )
        .unwrap();
        assert_eq!(
            manifest.files.len(),
            3,
            "CURRENT, MANIFEST and the SST — never the manifest itself: {:?}",
            manifest.files
        );

        let report = verify_payload(&dir, PayloadCheck::Checksums).expect("a whole payload");
        assert!(report.payload_manifest_present);
        assert_eq!(report.files_checked, 3);
        assert!(report.checksums_verified);

        // A file that never arrived.
        std::fs::remove_file(dir.join("000012.sst")).unwrap();
        assert!(matches!(
            verify_payload(&dir, PayloadCheck::Sizes),
            Err(PayloadError::MissingFile { ref name, .. }) if name == "000012.sst"
        ));

        // A file that arrived short.
        std::fs::write(dir.join("000012.sst"), b"table").unwrap();
        assert!(matches!(
            verify_payload(&dir, PayloadCheck::Sizes),
            Err(PayloadError::SizeMismatch {
                expected: 10,
                actual: 5,
                ..
            })
        ));
    }

    // FM-PERSISTENCE-056
    /// Bit rot is the case sizes cannot see: same length, different bytes. It
    /// is caught only when the caller pays for the read, which is why the
    /// operator command hashes and the boot path does not.
    #[test]
    fn only_the_checksum_pass_catches_same_size_corruption() {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path().join("payload");
        plant_payload(&dir);
        std::fs::write(
            dir.join(PAYLOAD_MANIFEST_FILE),
            PayloadManifest::build(&dir).unwrap().to_bytes().unwrap(),
        )
        .unwrap();

        std::fs::write(dir.join("000012.sst"), b"tab1e data").unwrap();
        verify_payload(&dir, PayloadCheck::Sizes)
            .expect("the size check cannot see a flipped bit, and does not pretend to");
        assert!(matches!(
            verify_payload(&dir, PayloadCheck::Checksums),
            Err(PayloadError::ChecksumMismatch { ref name, .. }) if name == "000012.sst"
        ));
    }

    // FM-PERSISTENCE-056
    /// Two directions the manifest must *not* be strict in, because both are
    /// normal: a payload from a producer that writes no manifest (a full sync,
    /// or a backup older than this format) still installs, and a file nobody
    /// listed is not evidence of damage. A manifest that is present but
    /// damaged is the opposite case — something wrote it, so it must parse.
    #[test]
    fn an_absent_manifest_verifies_but_a_damaged_one_refuses() {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path().join("payload");
        plant_payload(&dir);

        let report = verify_payload(&dir, PayloadCheck::Checksums).expect("no manifest, no claim");
        assert!(!report.payload_manifest_present);
        assert!(
            !report.checksums_verified,
            "nothing to hash against, so the report must not claim a checksum pass"
        );

        std::fs::write(
            dir.join(PAYLOAD_MANIFEST_FILE),
            PayloadManifest::build(&dir).unwrap().to_bytes().unwrap(),
        )
        .unwrap();
        std::fs::write(dir.join("999999.sst"), b"written after the manifest").unwrap();
        verify_payload(&dir, PayloadCheck::Checksums)
            .expect("an unlisted file is not a missing one");

        std::fs::write(dir.join(PAYLOAD_MANIFEST_FILE), b"{ truncated").unwrap();
        assert!(matches!(
            verify_payload(&dir, PayloadCheck::Sizes),
            Err(PayloadError::MalformedPayloadManifest { .. })
        ));

        std::fs::write(
            dir.join(PAYLOAD_MANIFEST_FILE),
            br#"{"version":9,"files":[]}"#,
        )
        .unwrap();
        assert!(matches!(
            verify_payload(&dir, PayloadCheck::Sizes),
            Err(PayloadError::UnsupportedPayloadManifest { found: 9, .. })
        ));
    }

    // FM-PERSISTENCE-056
    /// The manifest describes the whole payload subtree, with `/`-separated
    /// relative names, and is stable across builds of the same tree — it is
    /// compared by a reader that has only the bytes, not the producer.
    #[test]
    fn the_manifest_names_every_file_relative_to_the_payload_root() {
        let tmp = TempDir::new().unwrap();
        let dir = tmp.path().join("payload");
        plant_payload(&dir);
        std::fs::create_dir(dir.join("nested")).unwrap();
        std::fs::write(dir.join("nested/000013.sst"), b"deeper").unwrap();

        let manifest = PayloadManifest::build(&dir).unwrap();
        let names: Vec<_> = manifest.files.iter().map(|f| f.name.as_str()).collect();
        assert_eq!(
            names,
            vec![
                "000012.sst",
                "CURRENT",
                "MANIFEST-000007",
                "nested/000013.sst"
            ],
            "sorted, relative, and complete"
        );
        assert_eq!(
            manifest,
            PayloadManifest::build(&dir).unwrap(),
            "two manifests of one tree must agree"
        );

        std::fs::write(
            dir.join(PAYLOAD_MANIFEST_FILE),
            manifest.to_bytes().unwrap(),
        )
        .unwrap();
        let report = verify_payload(&dir, PayloadCheck::Checksums).unwrap();
        assert_eq!(report.files_checked, 4);
        assert_eq!(report.bytes_checked, 16 + 13 + 10 + 6);
    }
}
