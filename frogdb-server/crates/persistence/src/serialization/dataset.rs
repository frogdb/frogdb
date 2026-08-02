//! Framing for a *dataset blob*: many entries packed into one byte buffer.
//!
//! [`serialize`](super::serialize) owns one entry's bytes (header + payload).
//! This module owns the framing that turns a run of them into a single blob and
//! reads it back, so a whole shard's live keyspace can travel as one opaque
//! payload — the in-memory equivalent of handing over a checkpoint file.
//!
//! It exists for the persistence-disabled full sync: a primary with no RocksDB
//! has no checkpoint to cut, so it serializes what it holds in memory instead
//! (Redis's diskless replication ships the dataset the same way — the absence of
//! an RDB file on disk never means the absence of a dataset on the wire). The
//! producer is the shard worker in `frogdb-core`; the consumer is the replica's
//! installer in `frogdb-server`. Neither owns the format, so it lives here,
//! beside the per-entry codec it wraps.
//!
//! Wire format, repeated until the buffer is exhausted:
//!
//! ```text
//! [key_len: u32 LE][key bytes][entry_len: u32 LE][entry bytes from `serialize`]
//! ```
//!
//! There is no count prefix and no trailer: the blob is length-delimited by its
//! carrier (a checkpoint-stream file header, a test buffer), and a truncated
//! blob is an error rather than a short read, so a partial dataset can never be
//! mistaken for a complete one.

use bytes::Bytes;
use frogdb_types::types::{KeyMetadata, Value};

use super::{SerializationError, deserialize, serialize};

/// One entry of a dataset blob: the key plus everything
/// [`deserialize`](super::deserialize) recovers for it.
#[derive(Debug, Clone)]
pub struct DatasetEntry {
    pub key: Bytes,
    pub value: Value,
    pub metadata: KeyMetadata,
}

/// Append one entry to a dataset blob.
pub fn append_entry(out: &mut Vec<u8>, key: &[u8], value: &Value, metadata: &KeyMetadata) {
    let entry = serialize(value, metadata);
    // Two u32 length prefixes plus the two byte runs. Reserving is only a hint,
    // so a wrong figure here is invisible at runtime; asserting it against what
    // was actually appended keeps the figure honest as the framing changes.
    let framed_len = 8 + key.len() + entry.len();
    out.reserve(framed_len);
    let before = out.len();
    out.extend_from_slice(&(key.len() as u32).to_le_bytes());
    out.extend_from_slice(key);
    out.extend_from_slice(&(entry.len() as u32).to_le_bytes());
    out.extend_from_slice(&entry);
    debug_assert_eq!(
        out.len() - before,
        framed_len,
        "the reservation must match the framed entry"
    );
}

/// Read a dataset blob back into its entries.
///
/// Fails on truncation and on any entry whose bytes do not deserialize: a blob
/// is all-or-nothing, because the caller installs it as a *complete* dataset
/// and silently dropping the tail would install a subset of the primary's
/// keyspace while claiming to be in sync.
pub fn read_entries(blob: &[u8]) -> Result<Vec<DatasetEntry>, SerializationError> {
    let mut entries = Vec::new();
    let mut pos = 0usize;
    while pos < blob.len() {
        let key = take_chunk(blob, &mut pos)?;
        let payload = take_chunk(blob, &mut pos)?;
        let (value, metadata) = deserialize(payload)?;
        entries.push(DatasetEntry {
            key: Bytes::copy_from_slice(key),
            value,
            metadata,
        });
    }
    Ok(entries)
}

/// Read one `u32`-length-prefixed chunk, advancing `pos`.
fn take_chunk<'a>(blob: &'a [u8], pos: &mut usize) -> Result<&'a [u8], SerializationError> {
    let header_end = pos
        .checked_add(4)
        .ok_or_else(|| SerializationError::InvalidPayload("dataset blob offset overflow".into()))?;
    if header_end > blob.len() {
        return Err(SerializationError::Truncated {
            expected: header_end,
            actual: blob.len(),
        });
    }
    let len = u32::from_le_bytes(blob[*pos..header_end].try_into().unwrap()) as usize;
    let end = header_end.checked_add(len).ok_or_else(|| {
        SerializationError::InvalidPayload("dataset blob chunk length overflow".into())
    })?;
    if end > blob.len() {
        return Err(SerializationError::Truncated {
            expected: end,
            actual: blob.len(),
        });
    }
    *pos = end;
    Ok(&blob[header_end..end])
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{Duration, Instant};

    fn entry_blob(entries: &[(&str, &str, Option<Duration>)]) -> Vec<u8> {
        let mut blob = Vec::new();
        for (key, val, ttl) in entries {
            let value = Value::string(val.to_string());
            let mut metadata = KeyMetadata::new(value.memory_size());
            metadata.expires_at = ttl.map(|d| Instant::now() + d);
            append_entry(&mut blob, key.as_bytes(), &value, &metadata);
        }
        blob
    }

    // FM-REPLICATION-003
    #[test]
    fn blob_round_trips_keys_values_and_expiry() {
        let blob = entry_blob(&[
            ("a", "1", None),
            ("bb", "22", Some(Duration::from_secs(600))),
            ("", "empty-key", None),
        ]);

        let entries = read_entries(&blob).expect("blob decodes");

        assert_eq!(entries.len(), 3);
        assert_eq!(entries[0].key, Bytes::from_static(b"a"));
        assert!(entries[0].metadata.expires_at.is_none());
        assert_eq!(entries[1].key, Bytes::from_static(b"bb"));
        assert!(
            entries[1].metadata.expires_at.is_some(),
            "TTL survives the blob"
        );
        assert_eq!(entries[2].key, Bytes::new(), "empty keys are framed too");
        let Value::String(s) = &entries[1].value else {
            panic!("expected a string value")
        };
        assert_eq!(s.as_bytes(), Bytes::from_static(b"22"));
    }

    // FM-REPLICATION-003
    #[test]
    fn empty_blob_decodes_to_no_entries() {
        assert!(read_entries(&[]).expect("empty blob decodes").is_empty());
    }

    /// A blob cut short is an error, never a short read: a truncated dataset
    /// installed as a complete one is silent data loss.
    // FM-REPLICATION-003
    #[test]
    fn truncated_blob_is_an_error() {
        let blob = entry_blob(&[("a", "1", None), ("b", "2", None)]);
        // `4` is the boundary case: the length prefix ends exactly at the end
        // of the buffer, so the header itself is intact and it is the *chunk*
        // that is missing.
        for cut in [1, 3, 4, 7, blob.len() - 1] {
            let err = read_entries(&blob[..cut]).expect_err("truncation must not decode");
            let SerializationError::Truncated { expected, actual } = err else {
                panic!("cut {cut}: unexpected error {err:?}");
            };
            assert_eq!(actual, cut, "cut {cut}: the error names the bytes it had");
            assert!(
                expected > actual,
                "cut {cut}: a truncation must need *more* bytes than it has, got \
                 expected={expected} actual={actual}"
            );
        }
    }

    /// A well-framed chunk whose payload is garbage fails the whole blob rather
    /// than yielding the entries around it.
    // FM-REPLICATION-003
    #[test]
    fn corrupt_entry_payload_fails_the_blob() {
        let mut blob = Vec::new();
        blob.extend_from_slice(&1u32.to_le_bytes());
        blob.push(b'k');
        blob.extend_from_slice(&4u32.to_le_bytes());
        blob.extend_from_slice(b"junk");

        read_entries(&blob).expect_err("an undecodable entry fails the blob");
    }
}
