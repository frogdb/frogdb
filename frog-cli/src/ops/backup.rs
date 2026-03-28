use std::io::Write;
use std::path::Path;

use anyhow::{Context, Result, bail};
use serde::{Deserialize, Serialize};


/// Progress updates during export.
pub enum ExportProgress {
    Scanning { keys_found: usize },
    Exporting { done: usize, total: usize },
    Done,
}

/// Progress updates during import.
pub enum ImportProgress {
    Importing { done: usize, total: usize },
    Done { restored: usize, errors: usize },
}

/// Summary of a completed export.
#[derive(Debug, Serialize)]
pub struct ExportSummary {
    pub keys_exported: usize,
    pub bytes_written: u64,
    pub checksum: String,
    pub output_dir: String,
}

/// Summary of a completed import.
#[derive(Debug, Serialize)]
pub struct ImportSummary {
    pub keys_restored: usize,
    pub keys_skipped: usize,
    pub errors: usize,
}

/// Verify summary.
#[derive(Debug, Serialize)]
pub struct VerifySummary {
    pub valid: bool,
    pub keys: usize,
    pub data_files: usize,
    pub checksum_ok: bool,
    pub errors: Vec<String>,
}

/// Manifest stored alongside exported data.
#[derive(Debug, Serialize, Deserialize)]
pub struct ExportManifest {
    pub version: u32,
    pub key_count: usize,
    pub data_files: Vec<DataFileEntry>,
    pub checksum: String,
    pub timestamp: u64,
    pub source: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct DataFileEntry {
    pub filename: String,
    pub key_count: usize,
    pub sha256: String,
}

const BATCH_SIZE: usize = 1000;
const MANIFEST_VERSION: u32 = 1;

/// Export the entire dataset using SCAN + DUMP + TTL.
pub async fn export_dataset(
    conn: &mut redis::aio::MultiplexedConnection,
    output_dir: &Path,
    pattern: &str,
    key_type: Option<&str>,
    batch: u64,
    on_progress: impl Fn(ExportProgress),
) -> Result<ExportSummary> {
    std::fs::create_dir_all(output_dir)
        .with_context(|| format!("failed to create output dir: {}", output_dir.display()))?;
    let data_dir = output_dir.join("data");
    std::fs::create_dir_all(&data_dir)?;

    // Phase 1: SCAN all keys
    let mut all_keys = Vec::new();
    let mut cursor: u64 = 0;
    loop {
        let mut cmd = redis::cmd("SCAN");
        cmd.arg(cursor)
            .arg("MATCH")
            .arg(pattern)
            .arg("COUNT")
            .arg(batch);
        if let Some(kt) = key_type {
            cmd.arg("TYPE").arg(kt);
        }
        let (new_cursor, keys): (u64, Vec<String>) =
            cmd.query_async(conn).await.context("SCAN failed")?;
        all_keys.extend(keys);
        on_progress(ExportProgress::Scanning {
            keys_found: all_keys.len(),
        });
        cursor = new_cursor;
        if cursor == 0 {
            break;
        }
    }

    let total = all_keys.len();
    let mut data_files = Vec::new();
    let mut total_bytes: u64 = 0;
    let mut exported = 0;

    // Phase 2: DUMP + TTL each key in batches, write to numbered files
    for (file_idx, chunk) in all_keys.chunks(BATCH_SIZE).enumerate() {
        let mut pipe = redis::pipe();
        for key in chunk {
            pipe.cmd("DUMP").arg(key);
            pipe.cmd("PTTL").arg(key);
        }
        let results: Vec<redis::Value> = pipe
            .query_async(conn)
            .await
            .context("DUMP/PTTL pipeline failed")?;

        let filename = format!("batch_{file_idx:06}.dat");
        let filepath = data_dir.join(&filename);
        let mut file = std::fs::File::create(&filepath)?;
        let mut hasher = sha2::Sha256::new();
        let mut batch_keys = 0;

        use sha2::Digest;

        for (i, key) in chunk.iter().enumerate() {
            let dump_val = &results[i * 2];
            let pttl_val = &results[i * 2 + 1];

            // Skip keys that have been deleted between SCAN and DUMP
            if matches!(dump_val, redis::Value::Nil) {
                continue;
            }

            let dump_bytes = match dump_val {
                redis::Value::BulkString(b) => b,
                _ => continue,
            };
            let pttl = match pttl_val {
                redis::Value::Int(n) => *n,
                _ => 0,
            };

            // Wire format: key_len(u32) | key | pttl(i64) | dump_len(u32) | dump_data
            let key_bytes = key.as_bytes();
            file.write_all(&(key_bytes.len() as u32).to_le_bytes())?;
            file.write_all(key_bytes)?;
            file.write_all(&pttl.to_le_bytes())?;
            file.write_all(&(dump_bytes.len() as u32).to_le_bytes())?;
            file.write_all(dump_bytes)?;

            hasher.update(&(key_bytes.len() as u32).to_le_bytes());
            hasher.update(key_bytes);

            batch_keys += 1;
            exported += 1;
        }

        let file_size = file.metadata().map(|m| m.len()).unwrap_or(0);
        total_bytes += file_size;

        let hash = hex::encode(hasher.finalize());
        data_files.push(DataFileEntry {
            filename,
            key_count: batch_keys,
            sha256: hash,
        });

        on_progress(ExportProgress::Exporting {
            done: exported,
            total,
        });
    }

    // Phase 3: Write manifest
    use sha2::Digest;
    let mut manifest_hasher = sha2::Sha256::new();
    for df in &data_files {
        manifest_hasher.update(df.sha256.as_bytes());
    }
    let checksum = hex::encode(manifest_hasher.finalize());

    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();

    // Get server info for source field
    let server_info: String = redis::cmd("INFO")
        .arg("server")
        .query_async(conn)
        .await
        .unwrap_or_default();
    let source = server_info
        .lines()
        .find(|l| l.starts_with("frogdb_version:") || l.starts_with("redis_version:"))
        .unwrap_or("unknown")
        .to_string();

    let manifest = ExportManifest {
        version: MANIFEST_VERSION,
        key_count: exported,
        data_files,
        checksum: checksum.clone(),
        timestamp: now,
        source,
    };

    let manifest_json = serde_json::to_string_pretty(&manifest)?;
    std::fs::write(output_dir.join("manifest.json"), &manifest_json)?;

    on_progress(ExportProgress::Done);

    Ok(ExportSummary {
        keys_exported: exported,
        bytes_written: total_bytes,
        checksum,
        output_dir: output_dir.display().to_string(),
    })
}

/// Import a previously exported dataset using RESTORE pipelining.
pub async fn import_dataset(
    conn: &mut redis::aio::MultiplexedConnection,
    input_dir: &Path,
    replace: bool,
    pipeline_depth: u64,
    preserve_ttl: bool,
    on_progress: impl Fn(ImportProgress),
) -> Result<ImportSummary> {
    let manifest_path = input_dir.join("manifest.json");
    let manifest_str = std::fs::read_to_string(&manifest_path)
        .with_context(|| format!("failed to read manifest: {}", manifest_path.display()))?;
    let manifest: ExportManifest = serde_json::from_str(&manifest_str)?;

    let data_dir = input_dir.join("data");
    let total = manifest.key_count;
    let mut restored = 0usize;
    let mut skipped = 0usize;
    let mut errors = 0usize;

    for data_file in &manifest.data_files {
        let filepath = data_dir.join(&data_file.filename);
        let data = std::fs::read(&filepath)
            .with_context(|| format!("failed to read data file: {}", filepath.display()))?;

        let entries = parse_data_file(&data)?;

        // Pipeline RESTORE commands
        for chunk in entries.chunks(pipeline_depth as usize) {
            let mut pipe = redis::pipe();
            for (key, pttl, dump) in chunk {
                let ttl = if preserve_ttl && *pttl > 0 {
                    *pttl
                } else {
                    0
                };
                let mut restore_cmd = redis::cmd("RESTORE");
                restore_cmd
                    .arg(key.as_str())
                    .arg(ttl)
                    .arg(dump.as_slice());
                if replace {
                    restore_cmd.arg("REPLACE");
                }
                pipe.add_command(restore_cmd);
            }

            let results: Vec<redis::Value> = pipe
                .query_async(conn)
                .await
                .unwrap_or_else(|_| vec![redis::Value::Nil; chunk.len()]);

            for result in &results {
                match result {
                    redis::Value::SimpleString(s) if s == "OK" => restored += 1,
                    redis::Value::Okay => restored += 1,
                    redis::Value::ServerError(_) if !replace => skipped += 1,
                    _ => errors += 1,
                }
            }

            on_progress(ImportProgress::Importing {
                done: restored + skipped + errors,
                total,
            });
        }
    }

    on_progress(ImportProgress::Done { restored, errors });

    Ok(ImportSummary {
        keys_restored: restored,
        keys_skipped: skipped,
        errors,
    })
}

/// Parse a data file into (key, pttl, dump_bytes) tuples.
fn parse_data_file(data: &[u8]) -> Result<Vec<(String, i64, Vec<u8>)>> {
    let mut entries = Vec::new();
    let mut pos = 0;

    while pos + 4 <= data.len() {
        // key_len
        let key_len = u32::from_le_bytes(data[pos..pos + 4].try_into()?) as usize;
        pos += 4;
        if pos + key_len > data.len() {
            bail!("truncated data file: key extends past end");
        }
        let key = String::from_utf8_lossy(&data[pos..pos + key_len]).to_string();
        pos += key_len;

        // pttl
        if pos + 8 > data.len() {
            bail!("truncated data file: pttl extends past end");
        }
        let pttl = i64::from_le_bytes(data[pos..pos + 8].try_into()?);
        pos += 8;

        // dump_len + dump_data
        if pos + 4 > data.len() {
            bail!("truncated data file: dump_len extends past end");
        }
        let dump_len = u32::from_le_bytes(data[pos..pos + 4].try_into()?) as usize;
        pos += 4;
        if pos + dump_len > data.len() {
            bail!("truncated data file: dump data extends past end");
        }
        let dump = data[pos..pos + dump_len].to_vec();
        pos += dump_len;

        entries.push((key, pttl, dump));
    }

    Ok(entries)
}

/// Verify integrity of an export archive (no connection needed).
pub fn verify_export(dir: &Path) -> Result<VerifySummary> {
    use sha2::Digest;

    let manifest_path = dir.join("manifest.json");
    let manifest_str = std::fs::read_to_string(&manifest_path)
        .with_context(|| format!("failed to read manifest: {}", manifest_path.display()))?;
    let manifest: ExportManifest = serde_json::from_str(&manifest_str)
        .context("failed to parse manifest.json")?;

    let data_dir = dir.join("data");
    let mut errors = Vec::new();
    let mut total_keys = 0;

    for df in &manifest.data_files {
        let filepath = data_dir.join(&df.filename);
        if !filepath.exists() {
            errors.push(format!("missing data file: {}", df.filename));
            continue;
        }

        let data = std::fs::read(&filepath)?;

        // Verify SHA256 by re-parsing and hashing keys
        let entries = match parse_data_file(&data) {
            Ok(e) => e,
            Err(e) => {
                errors.push(format!("{}: parse error: {e}", df.filename));
                continue;
            }
        };

        let mut hasher = sha2::Sha256::new();
        for (key, _, _) in &entries {
            let key_bytes = key.as_bytes();
            hasher.update(&(key_bytes.len() as u32).to_le_bytes());
            hasher.update(key_bytes);
        }
        let hash = hex::encode(hasher.finalize());

        if hash != df.sha256 {
            errors.push(format!(
                "{}: checksum mismatch (expected {}, got {})",
                df.filename, df.sha256, hash
            ));
        }

        if entries.len() != df.key_count {
            errors.push(format!(
                "{}: key count mismatch (expected {}, got {})",
                df.filename,
                df.key_count,
                entries.len()
            ));
        }

        total_keys += entries.len();
    }

    // Verify overall checksum
    let mut manifest_hasher = sha2::Sha256::new();
    for df in &manifest.data_files {
        manifest_hasher.update(df.sha256.as_bytes());
    }
    let overall_checksum = hex::encode(manifest_hasher.finalize());
    let checksum_ok = overall_checksum == manifest.checksum;
    if !checksum_ok {
        errors.push(format!(
            "overall checksum mismatch (expected {}, got {})",
            manifest.checksum, overall_checksum
        ));
    }

    Ok(VerifySummary {
        valid: errors.is_empty(),
        keys: total_keys,
        data_files: manifest.data_files.len(),
        checksum_ok,
        errors,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_data_file_roundtrip() {
        let mut buf = Vec::new();
        let key = b"test:key";
        let pttl: i64 = 5000;
        let dump = b"\x00\x03foo\x0a\x00";

        buf.extend_from_slice(&(key.len() as u32).to_le_bytes());
        buf.extend_from_slice(key);
        buf.extend_from_slice(&pttl.to_le_bytes());
        buf.extend_from_slice(&(dump.len() as u32).to_le_bytes());
        buf.extend_from_slice(dump);

        let entries = parse_data_file(&buf).unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].0, "test:key");
        assert_eq!(entries[0].1, 5000);
        assert_eq!(entries[0].2, dump);
    }

    #[test]
    fn test_verify_export_missing_manifest() {
        let dir = tempfile::tempdir().unwrap();
        let result = verify_export(dir.path());
        assert!(result.is_err());
    }

    #[test]
    fn test_verify_export_empty() {
        use sha2::Digest;

        let dir = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(dir.path().join("data")).unwrap();

        let manifest = ExportManifest {
            version: 1,
            key_count: 0,
            data_files: vec![],
            checksum: hex::encode(sha2::Sha256::new().finalize()),
            timestamp: 0,
            source: "test".into(),
        };
        std::fs::write(
            dir.path().join("manifest.json"),
            serde_json::to_string(&manifest).unwrap(),
        )
        .unwrap();

        let summary = verify_export(dir.path()).unwrap();
        assert!(summary.valid);
        assert_eq!(summary.keys, 0);
        assert!(summary.checksum_ok);
    }
}
