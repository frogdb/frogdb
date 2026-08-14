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
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};

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
    /// A write group opened — every write recorded until the matching
    /// `GroupEnd` must commit as one storage batch in production.
    GroupBegin,
    /// The innermost open write group closed.
    GroupEnd,
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
    /// Like [`Self::AtWriteIndex`], but the failure also latches the poison
    /// (FM-PERSISTENCE-053): it models a *commit* that was lost rather than an
    /// enqueue that was refused, which is the failure the fail-stop policies
    /// react to (FM-PERSISTENCE-055).
    PoisoningAtWriteIndex(usize),
    /// Fail every write for which the predicate (write_index, key) is true.
    Predicate(FailurePredicate),
}

impl FakeFailure {
    fn should_fail(&self, write_index: usize, key: Option<&[u8]>) -> bool {
        match self {
            FakeFailure::None => false,
            FakeFailure::AtWriteIndex(n) | FakeFailure::PoisoningAtWriteIndex(n) => {
                *n == write_index
            }
            FakeFailure::Predicate(p) => p(write_index, key),
        }
    }

    /// Whether a failure injected by this setting also poisons the shard.
    fn poisons(&self) -> bool {
        matches!(self, FakeFailure::PoisoningAtWriteIndex(_))
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

    /// The writes a crash would leave behind, under the same page-cache model
    /// `PageCacheSink` implements one layer down (FM-PERSISTENCE-002): a staged
    /// entry is only past the crash once a `flush_through` has followed it.
    /// Everything recorded after the last `FlushThrough` marker is still in the
    /// cache when the power goes out, and is gone.
    ///
    /// `flush_async` deliberately does *not* count — it empties the buffer into
    /// storage without fsyncing, which is exactly the distinction the `sync`
    /// durability mode is about.
    pub fn durable_writes(&self) -> Vec<RecordedWalEffect> {
        let effects = self.effects();
        let Some(last_flush) = effects
            .iter()
            .rposition(|e| e.kind == WalEffectKind::FlushThrough)
        else {
            return Vec::new();
        };
        effects[..last_flush]
            .iter()
            .filter(|e| e.kind.is_write())
            .cloned()
            .collect()
    }

    /// The writes of each outermost write group, in order.
    ///
    /// Each inner `Vec` is one group's writes — in production exactly the
    /// entries that must land in a single committed storage batch, so a test
    /// can assert "all of this transaction's writes are in one group".
    /// Writes recorded outside any group are not returned. Errors if the
    /// markers are unbalanced (a close with no open, or an unclosed group).
    pub fn groups(&self) -> Result<Vec<Vec<RecordedWalEffect>>, String> {
        let mut groups: Vec<Vec<RecordedWalEffect>> = Vec::new();
        let mut depth: usize = 0;
        for e in self.effects() {
            match e.kind {
                WalEffectKind::GroupBegin => {
                    if depth == 0 {
                        groups.push(Vec::new());
                    }
                    depth += 1;
                }
                WalEffectKind::GroupEnd => {
                    depth = depth.checked_sub(1).ok_or_else(|| {
                        format!("GroupEnd with no open group at order {}", e.order)
                    })?;
                }
                k if k.is_write() && depth > 0 => {
                    groups
                        .last_mut()
                        .expect("depth > 0 implies a group")
                        .push(e);
                }
                _ => {}
            }
        }
        if depth > 0 {
            return Err(format!("{depth} write group(s) left unclosed"));
        }
        Ok(groups)
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
    /// Poison latch (FM-PERSISTENCE-053), set by [`Self::poison`] and by a
    /// [`FakeFailure::PoisoningAtWriteIndex`] injection.
    ///
    /// Plain injected write failures deliberately do **not** set it: such a
    /// failure models the enqueue that never happened (TR-PERSISTENCE-013 case
    /// (a)), which loses nothing already accepted. Poisoning is the *flush*
    /// failure, which the fake has no equivalent of, so tests state it.
    poisoned: AtomicBool,
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
            poisoned: AtomicBool::new(false),
        }
    }

    /// Clone of the shared log handle for post-run assertions.
    pub fn log(&self) -> FakeWalLog {
        self.log.clone()
    }

    /// Latch the poison, as a lost flush would (FM-PERSISTENCE-053).
    pub fn poison(&self) {
        self.poisoned.store(true, Ordering::SeqCst);
    }

    /// Record a write effect, honoring failure injection. Returns the assigned
    /// sequence on success, or the injected io error (without recording).
    fn record_write(&self, kind: WalEffectKind, key: Option<&[u8]>) -> std::io::Result<u64> {
        let write_index = self.write_index.load(Ordering::SeqCst);
        if self.failure.should_fail(write_index, key) {
            // The failed write "did not happen": do not record, do not advance
            // the write index (mirrors the `?`-propagated rollback path).
            if self.failure.poisons() {
                self.poison();
            }
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

    /// Record a non-write effect — flush or group marker (never fails).
    fn record_marker(&self, kind: WalEffectKind) {
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
    async fn begin_group(&self) -> std::io::Result<()> {
        self.record_marker(WalEffectKind::GroupBegin);
        Ok(())
    }
    async fn end_group(&self) -> std::io::Result<()> {
        self.record_marker(WalEffectKind::GroupEnd);
        Ok(())
    }
    async fn flush_async(&self) -> std::io::Result<()> {
        self.record_marker(WalEffectKind::FlushAsync);
        Ok(())
    }
    async fn flush_through(&self, _after_seq: u64) -> std::io::Result<()> {
        self.record_marker(WalEffectKind::FlushThrough);
        if self.poisoned.load(Ordering::SeqCst) {
            // Like the real sink: a poisoned shard confirms nothing, however
            // well later writes go (FM-PERSISTENCE-007/053).
            return Err(std::io::Error::other("shard is poisoned"));
        }
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
            committed_sequence: seq,
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
    fn poisoned(&self) -> bool {
        self.poisoned.load(Ordering::SeqCst)
    }
    fn clear_poison(&self) {
        self.poisoned.store(false, Ordering::SeqCst);
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

    /// A failed write must not consume a write index either, or every later
    /// index-based injection would be off by one against the production
    /// rollback path it mirrors.
    #[tokio::test]
    async fn a_failed_write_does_not_consume_its_write_index() {
        let sink = FakeWalSink::with_failure(0, FakeFailure::AtWriteIndex(0));
        let log = sink.log();
        assert!(
            sink.write_set(b"a", &Value::string("1"), &meta())
                .await
                .is_err()
        );
        // Still write-index 0, so the injection fires again.
        assert!(sink.write_delete(b"b").await.is_err());
        assert!(log.effects().is_empty());
        assert_eq!(sink.sequence(), 0, "a refused write assigns no sequence");
    }

    /// Predicate injection selects by key, not just by position.
    #[tokio::test]
    async fn predicate_failure_selects_by_key() {
        let sink = FakeWalSink::with_failure(
            0,
            FakeFailure::Predicate(Arc::new(|_idx, key| key == Some(&b"poison"[..]))),
        );
        let log = sink.log();
        sink.write_delete(b"fine").await.unwrap();
        assert!(sink.write_delete(b"poison").await.is_err());
        sink.write_delete(b"also-fine").await.unwrap();
        assert_eq!(
            log.effects()
                .iter()
                .map(|e| e.key.clone().unwrap())
                .collect::<Vec<_>>(),
            vec![b"fine".to_vec(), b"also-fine".to_vec()]
        );
    }

    /// Sequences mirror `RocksWalWriter`'s discipline: 1-based and assigned
    /// only by writes. A marker carries the sequence current at the time it was
    /// recorded, which is what makes "this flush covers writes up to N"
    /// checkable.
    #[tokio::test]
    async fn writes_assign_sequences_and_markers_only_observe_them() {
        let sink = FakeWalSink::new(3);
        let log = sink.log();
        assert_eq!(
            sink.write_set(b"a", &Value::string("1"), &meta())
                .await
                .unwrap(),
            1,
            "sequences are 1-based"
        );
        assert_eq!(sink.write_merge(b"h", &[(1, 2)], &meta()).await.unwrap(), 2);
        sink.flush_async().await.unwrap();
        assert_eq!(sink.write_clear().await.unwrap(), 3);
        sink.flush_through(3).await.unwrap();

        let kinds: Vec<_> = log.effects().iter().map(|e| e.kind).collect();
        assert_eq!(
            kinds,
            vec![
                WalEffectKind::Set,
                WalEffectKind::Merge,
                WalEffectKind::FlushAsync,
                WalEffectKind::Clear,
                WalEffectKind::FlushThrough,
            ]
        );
        let seqs: Vec<_> = log.effects().iter().map(|e| e.seq).collect();
        assert_eq!(
            seqs,
            vec![1, 2, 2, 3, 3],
            "the flush at index 2 sees sequence 2, not 3"
        );
        assert_eq!(
            log.effects().iter().map(|e| e.order).collect::<Vec<_>>(),
            vec![0, 1, 2, 3, 4],
            "order counts every effect, markers included"
        );

        assert_eq!(sink.sequence(), 3);
        assert_eq!(
            sink.durable_sequence(),
            3,
            "the fake is synchronously durable"
        );
        assert_eq!(sink.shard_id(), 3);
        let stats = sink.lag_stats();
        assert_eq!(stats.shard_id, 3);
        assert_eq!(stats.sequence, 3);
        assert_eq!(stats.committed_sequence, 3);
        assert_eq!(stats.pending_ops, 0);
        assert!(stats.last_flush_ok);
    }

    /// `groups()` is what a transaction test asserts atomicity with: one inner
    /// `Vec` per *outermost* group, holding that group's writes and nothing
    /// else. Writes outside any group are not a group of their own, and a
    /// nested group does not open a second one.
    #[tokio::test]
    async fn groups_collect_the_writes_of_each_outermost_group() {
        let sink = FakeWalSink::new(0);
        let log = sink.log();

        sink.write_delete(b"loose").await.unwrap();

        sink.begin_group().await.unwrap();
        sink.write_set(b"t1", &Value::string("v"), &meta())
            .await
            .unwrap();
        sink.begin_group().await.unwrap();
        sink.write_delete(b"t2").await.unwrap();
        sink.end_group().await.unwrap();
        sink.write_delete(b"t3").await.unwrap();
        sink.end_group().await.unwrap();

        sink.begin_group().await.unwrap();
        sink.write_delete(b"u1").await.unwrap();
        sink.end_group().await.unwrap();

        let groups = log.groups().unwrap();
        let keys: Vec<Vec<Vec<u8>>> = groups
            .iter()
            .map(|g| g.iter().map(|e| e.key.clone().unwrap()).collect())
            .collect();
        assert_eq!(
            keys,
            vec![
                vec![b"t1".to_vec(), b"t2".to_vec(), b"t3".to_vec()],
                vec![b"u1".to_vec()],
            ],
            "the nested group folds into its parent; the loose write is in no group"
        );
    }

    /// Unbalanced group markers are a bug in the code under test, so they are
    /// reported rather than silently repaired.
    #[tokio::test]
    async fn unbalanced_group_markers_are_reported() {
        let sink = FakeWalSink::new(0);
        let log = sink.log();
        sink.begin_group().await.unwrap();
        sink.write_delete(b"k").await.unwrap();
        assert!(
            log.groups().unwrap_err().contains("unclosed"),
            "a group left open is an error"
        );

        let sink = FakeWalSink::new(0);
        let log = sink.log();
        sink.end_group().await.unwrap();
        assert!(
            log.groups().unwrap_err().contains("no open group"),
            "a close with no open is an error"
        );
    }

    /// `assert_write_order` has to be able to fail: it is the only thing
    /// standing between a reordered WAL log and a green test.
    #[test]
    fn out_of_order_writes_are_rejected() {
        let log = FakeWalLog::default();
        log.0.lock().unwrap().extend([
            RecordedWalEffect {
                order: 5,
                kind: WalEffectKind::Set,
                key: Some(b"a".to_vec()),
                seq: 1,
            },
            // A flush out of order is not a write, so it is ignored...
            RecordedWalEffect {
                order: 0,
                kind: WalEffectKind::FlushAsync,
                key: None,
                seq: 1,
            },
            // ...but a write out of order is the violation.
            RecordedWalEffect {
                order: 4,
                kind: WalEffectKind::Delete,
                key: Some(b"b".to_vec()),
                seq: 2,
            },
        ]);
        let err = log.assert_write_order().unwrap_err();
        assert!(err.contains("out-of-order"), "unexpected message: {err}");
        // The violation reported is the *write* pair, not the interleaved
        // flush: a marker never participates in write ordering, so a checker
        // that treated every effect as a write would blame the flush here and
        // then blame flushes in real logs, where they are legitimately
        // interleaved.
        assert!(
            err.contains("Delete") && err.contains("Set"),
            "the offending pair must be the two writes: {err}"
        );
        assert!(
            !err.contains("FlushAsync"),
            "a flush is not a write and must not be blamed: {err}"
        );
    }
}
