//! Deterministic in-process [`WalSink`] for simulation tests.
//!
//! [`FakeWalSink`] records an ordered `(effect, key)` log into a shared handle
//! and can inject write failures by op-index or predicate — no RocksDB, no
//! background thread, deterministic under turmoil. The recorded log makes
//! wake-before-WAL-persist observable: correlating the log's order with the
//! recorded `History` pins the documented `WRITE_EFFECT_ORDER`.
use super::WalSink;
use super::config::WalLagStats;
use async_trait::async_trait;
use frogdb_types::types::{KeyMetadata, Value};
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};

/// One recorded WAL effect, in global call order.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecordedWalEffect {
    /// Monotonic order across all effects on this sink (0-based).
    pub order: u64,
    pub kind: WalEffectKind,
    pub key: Option<Vec<u8>>,
    /// Sequence the sink assigned (mirrors RocksWalWriter's fetch_add discipline).
    pub seq: u64,
}

/// The kind of a recorded WAL effect.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WalEffectKind {
    Set,
    Merge,
    Delete,
    Clear,
    FlushAsync,
    FlushThrough,
}

impl WalEffectKind {
    /// Whether this effect is a write (as opposed to a flush).
    fn is_write(self) -> bool {
        matches!(
            self,
            WalEffectKind::Set
                | WalEffectKind::Merge
                | WalEffectKind::Delete
                | WalEffectKind::Clear
        )
    }
}

/// Predicate deciding whether a write should be failed, given its 0-based
/// write-index and optional key.
pub type FailurePredicate = Arc<dyn Fn(usize, Option<&[u8]>) -> bool + Send + Sync>;

/// Injectable failure: fail the Nth write, or any write matching a predicate.
#[derive(Clone)]
pub enum FakeFailure {
    None,
    /// Fail the write whose 0-based write-index equals `n` (writes only,
    /// not flushes) with an injected io error — exercises the rollback /
    /// EXECABORT persist-failure branch.
    AtWriteIndex(usize),
    /// Fail every write for which the predicate (write_index, key) is true.
    Predicate(FailurePredicate),
}

impl FakeFailure {
    fn should_fail(&self, write_index: usize, key: Option<&[u8]>) -> bool {
        match self {
            FakeFailure::None => false,
            FakeFailure::AtWriteIndex(n) => *n == write_index,
            FakeFailure::Predicate(p) => p(write_index, key),
        }
    }
}

/// Shared, inspectable log. The harness holds a clone; the sink writes into it.
#[derive(Clone, Default)]
pub struct FakeWalLog(pub Arc<Mutex<Vec<RecordedWalEffect>>>);

impl FakeWalLog {
    /// A snapshot of all recorded effects, in order.
    pub fn effects(&self) -> Vec<RecordedWalEffect> {
        self.0.lock().expect("FakeWalLog poisoned").clone()
    }

    /// Assert every recorded WAL write appears in non-decreasing `order`, one
    /// per action, matching the per-command execution order — the projection
    /// of `WRITE_EFFECT_ORDER` a WAL-only sink can observe. Fuller cross-effect
    /// ordering (wake vs persist) is asserted at the harness level by
    /// correlating with History. Returns `Err` with the offending pair on
    /// violation.
    pub fn assert_write_order(&self) -> Result<(), String> {
        let effects = self.effects();
        let mut last: Option<&RecordedWalEffect> = None;
        for e in effects.iter().filter(|e| e.kind.is_write()) {
            if let Some(prev) = last
                && e.order <= prev.order
            {
                return Err(format!(
                    "out-of-order WAL writes: {:?} (order {}) not after {:?} (order {})",
                    e.kind, e.order, prev.kind, prev.order
                ));
            }
            last = Some(e);
        }
        Ok(())
    }
}

/// Deterministic in-process [`WalSink`]. See module docs.
pub struct FakeWalSink {
    shard_id: usize,
    seq: AtomicU64,
    order: AtomicU64,
    write_index: AtomicUsize,
    log: FakeWalLog,
    failure: FakeFailure,
}

impl FakeWalSink {
    /// A fake sink with no failure injection.
    pub fn new(shard_id: usize) -> Self {
        Self::with_failure(shard_id, FakeFailure::None)
    }

    /// A fake sink that injects `failure`.
    pub fn with_failure(shard_id: usize, failure: FakeFailure) -> Self {
        Self {
            shard_id,
            seq: AtomicU64::new(0),
            order: AtomicU64::new(0),
            write_index: AtomicUsize::new(0),
            log: FakeWalLog::default(),
            failure,
        }
    }

    /// Clone of the shared log handle for post-run assertions.
    pub fn log(&self) -> FakeWalLog {
        self.log.clone()
    }

    /// Record a write effect, honoring failure injection. Returns the assigned
    /// sequence on success, or the injected io error (without recording).
    fn record_write(&self, kind: WalEffectKind, key: Option<&[u8]>) -> std::io::Result<u64> {
        let write_index = self.write_index.load(Ordering::SeqCst);
        if self.failure.should_fail(write_index, key) {
            // The failed write "did not happen": do not record, do not advance
            // the write index (mirrors the `?`-propagated rollback path).
            return Err(std::io::Error::other("injected WAL failure"));
        }
        let order = self.order.fetch_add(1, Ordering::SeqCst);
        let seq = self.seq.fetch_add(1, Ordering::SeqCst) + 1;
        self.write_index.fetch_add(1, Ordering::SeqCst);
        self.log
            .0
            .lock()
            .expect("FakeWalLog poisoned")
            .push(RecordedWalEffect {
                order,
                kind,
                key: key.map(|k| k.to_vec()),
                seq,
            });
        Ok(seq)
    }

    /// Record a flush effect (never fails).
    fn record_flush(&self, kind: WalEffectKind) {
        let order = self.order.fetch_add(1, Ordering::SeqCst);
        let seq = self.seq.load(Ordering::SeqCst);
        self.log
            .0
            .lock()
            .expect("FakeWalLog poisoned")
            .push(RecordedWalEffect {
                order,
                kind,
                key: None,
                seq,
            });
    }
}

#[async_trait]
impl WalSink for FakeWalSink {
    async fn write_set(
        &self,
        key: &[u8],
        _value: &Value,
        _metadata: &KeyMetadata,
    ) -> std::io::Result<u64> {
        self.record_write(WalEffectKind::Set, Some(key))
    }
    async fn write_merge(
        &self,
        key: &[u8],
        _pairs: &[(u16, u8)],
        _metadata: &KeyMetadata,
    ) -> std::io::Result<u64> {
        self.record_write(WalEffectKind::Merge, Some(key))
    }
    async fn write_delete(&self, key: &[u8]) -> std::io::Result<u64> {
        self.record_write(WalEffectKind::Delete, Some(key))
    }
    async fn write_clear(&self) -> std::io::Result<u64> {
        self.record_write(WalEffectKind::Clear, None)
    }
    async fn flush_async(&self) -> std::io::Result<()> {
        self.record_flush(WalEffectKind::FlushAsync);
        Ok(())
    }
    async fn flush_through(&self, _after_seq: u64) -> std::io::Result<()> {
        self.record_flush(WalEffectKind::FlushThrough);
        Ok(())
    }
    fn sequence(&self) -> u64 {
        self.seq.load(Ordering::SeqCst)
    }
    fn durable_sequence(&self) -> u64 {
        // The fake is synchronously durable.
        self.seq.load(Ordering::SeqCst)
    }
    fn lag_stats(&self) -> WalLagStats {
        let seq = self.seq.load(Ordering::SeqCst);
        WalLagStats {
            pending_ops: 0,
            pending_bytes: 0,
            durability_lag_ms: 0,
            sequence: seq,
            durable_sequence: seq,
            flush_failures: 0,
            lost_ops: 0,
            lost_bytes: 0,
            last_flush_ok: true,
            shard_id: self.shard_id,
            last_flush_timestamp_ms: 0,
        }
    }
    fn shard_id(&self) -> usize {
        self.shard_id
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::wal::WalSink;
    use frogdb_types::types::{KeyMetadata, Value};

    fn meta() -> KeyMetadata {
        KeyMetadata::new(0)
    }

    #[tokio::test]
    async fn records_effects_in_order() {
        let sink = FakeWalSink::new(0);
        let log = sink.log();
        sink.write_set(b"k", &Value::string("v"), &meta())
            .await
            .unwrap();
        sink.write_delete(b"g").await.unwrap();
        let e = log.effects();
        assert_eq!(e.len(), 2);
        assert_eq!(e[0].kind, WalEffectKind::Set);
        assert_eq!(e[0].key.as_deref(), Some(&b"k"[..]));
        assert!(e[1].order > e[0].order);
        assert!(log.assert_write_order().is_ok());
    }

    #[tokio::test]
    async fn injects_failure_at_index() {
        let sink = FakeWalSink::with_failure(0, FakeFailure::AtWriteIndex(1));
        let log = sink.log();
        assert!(
            sink.write_set(b"a", &Value::string("1"), &meta())
                .await
                .is_ok()
        );
        assert!(
            sink.write_set(b"b", &Value::string("2"), &meta())
                .await
                .is_err()
        );
        // The failed write is not recorded (it did not happen).
        assert_eq!(log.effects().len(), 1);
    }
}
