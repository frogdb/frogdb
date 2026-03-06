//! ZIP bundle generation from diagnostic data.

use std::io::Write;

use zip::ZipWriter;
use zip::write::SimpleFileOptions;

use super::BundleConfig;
use super::collector::DiagnosticData;

/// Generates ZIP bundles from diagnostic data.
pub struct BundleGenerator {
    _config: BundleConfig,
}

impl BundleGenerator {
    /// Create a new bundle generator.
    pub fn new(config: BundleConfig) -> Self {
        Self { _config: config }
    }

    /// Generate a unique bundle ID.
    pub fn generate_id() -> String {
        use std::time::SystemTime;

        let now = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap_or_default();

        // Use timestamp + random suffix for uniqueness
        let ts = now.as_millis();
        let rand_part: u32 = (now.subsec_nanos() ^ 0xDEAD_BEEF) & 0xFFFF;
        format!("{:x}-{:04x}", ts, rand_part)
    }

    /// Create a ZIP archive containing the diagnostic data.
    pub fn create_zip(
        &self,
        id: &str,
        data: &DiagnosticData,
        duration_secs: u64,
    ) -> Result<Vec<u8>, std::io::Error> {
        let buf = Vec::new();
        let mut zip = ZipWriter::new(std::io::Cursor::new(buf));
        let options =
            SimpleFileOptions::default().compression_method(zip::CompressionMethod::Deflated);

        // Write metadata
        let metadata = serde_json::json!({
            "bundle_id": id,
            "duration_secs": duration_secs,
            "collected_at": data.metadata.collected_at,
            "version": data.metadata.version,
        });
        zip.start_file(format!("{id}/metadata.json"), options)?;
        zip.write_all(
            serde_json::to_string_pretty(&metadata)
                .unwrap_or_default()
                .as_bytes(),
        )?;

        // Write shard memory stats
        zip.start_file(format!("{id}/shard_memory.json"), options)?;
        zip.write_all(
            serde_json::to_string_pretty(&data.shard_memory)
                .unwrap_or_default()
                .as_bytes(),
        )?;

        // Write traces
        zip.start_file(format!("{id}/traces.json"), options)?;
        zip.write_all(
            serde_json::to_string_pretty(&data.traces)
                .unwrap_or_default()
                .as_bytes(),
        )?;

        // Write cluster state
        zip.start_file(format!("{id}/cluster_state.json"), options)?;
        zip.write_all(
            serde_json::to_string_pretty(&data.cluster_state)
                .unwrap_or_default()
                .as_bytes(),
        )?;

        let cursor = zip.finish()?;
        Ok(cursor.into_inner())
    }
}
