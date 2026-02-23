//! Write-Ahead Log (WAL) traits.
//!
//! The WAL provides durability by logging operations before they're applied
//! to the in-memory store. In case of crash, the log can be replayed to
//! recover state.

use bytes::Bytes;
use std::time::Instant;

/// WAL (Write-Ahead Log) writer trait.
///
/// Implementations write operations to persistent storage before
/// they're applied to the in-memory store.
pub trait WalWriter: Send + Sync {
    /// Append an operation to the WAL.
    ///
    /// Returns the sequence number assigned to this operation.
    fn append(&mut self, operation: &WalOperation) -> u64;

    /// Flush pending writes to disk.
    fn flush(&mut self) -> std::io::Result<()>;

    /// Get the current sequence number.
    fn current_sequence(&self) -> u64;
}

/// Operation to be written to the WAL.
#[derive(Debug, Clone)]
pub enum WalOperation {
    /// SET key value
    Set { key: Bytes, value: Bytes },
    /// SET key value with expiry
    SetWithExpiry {
        key: Bytes,
        value: Bytes,
        expires_at: Instant,
    },
    /// DEL key
    Delete { key: Bytes },
    /// Expire a key
    Expire { key: Bytes, at: Instant },
}

/// Noop WAL writer that does nothing.
///
/// Use this when WAL functionality is disabled or for testing.
#[derive(Debug, Default)]
pub struct NoopWalWriter {
    sequence: u64,
}

impl NoopWalWriter {
    /// Create a new noop WAL writer.
    pub fn new() -> Self {
        Self { sequence: 0 }
    }
}

impl WalWriter for NoopWalWriter {
    fn append(&mut self, _operation: &WalOperation) -> u64 {
        self.sequence += 1;
        tracing::trace!(seq = self.sequence, "Noop WAL append");
        self.sequence
    }

    fn flush(&mut self) -> std::io::Result<()> {
        tracing::trace!("Noop WAL flush");
        Ok(())
    }

    fn current_sequence(&self) -> u64 {
        self.sequence
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_noop_wal_writer() {
        let mut wal = NoopWalWriter::new();
        assert_eq!(wal.current_sequence(), 0);

        let seq = wal.append(&WalOperation::Set {
            key: Bytes::from("key"),
            value: Bytes::from("value"),
        });
        assert_eq!(seq, 1);
        assert_eq!(wal.current_sequence(), 1);

        wal.flush().unwrap();
    }
}
