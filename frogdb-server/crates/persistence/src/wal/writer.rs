//! RocksDB-backed WAL writer implementation.
use super::config::{DurabilityMode, WalConfig, WalLagStats};
use super::flush::{
    FlushEngine, FlushOutcomes, RocksSink, WalCommand, WalEntry, WalLagAtomics,
    current_timestamp_ms, flush_thread_loop,
};
use crate::rocks::RocksStore;
use crate::serialization::{serialize, serialize_hll_delta};
use bytes::Bytes;
use frogdb_types::types::{KeyMetadata, Value};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::time::Duration;
use tracing::{debug, error};

pub struct RocksWalWriter {
    shard_id: usize,
    sequence: AtomicU64,
    cmd_tx: flume::Sender<WalCommand>,
    lag: Arc<WalLagAtomics>,
    outcomes: Arc<FlushOutcomes>,
    /// Live batch-size flush threshold (bytes). Shared with the flush thread,
    /// which re-reads it on every batch decision, so `CONFIG SET
    /// persistence-batch-size-threshold-kb` retunes batching without a restart.
    batch_size_threshold: Arc<AtomicUsize>,
    flush_thread: Option<std::thread::JoinHandle<()>>,
}

impl RocksWalWriter {
    pub fn new(
        rocks: Arc<RocksStore>,
        shard_id: usize,
        config: WalConfig,
        metrics_recorder: Arc<dyn frogdb_types::traits::MetricsRecorder>,
    ) -> Self {
        let dm = match &config.mode {
            DurabilityMode::Async => "async".to_string(),
            DurabilityMode::Periodic { interval_ms } => format!("periodic_{}ms", interval_ms),
            DurabilityMode::Sync => "sync".to_string(),
        };
        debug!(shard_id, durability_mode = %dm, "WAL writer created");
        let now_ms = current_timestamp_ms();
        let (cmd_tx, cmd_rx) = flume::bounded(config.channel_capacity);
        let lag = Arc::new(WalLagAtomics {
            pending_ops: std::sync::atomic::AtomicUsize::new(0),
            pending_bytes: std::sync::atomic::AtomicUsize::new(0),
            last_flush_timestamp_ms: AtomicU64::new(now_ms),
        });
        let outcomes = Arc::new(FlushOutcomes::new());
        // Register this shard's durability watermark with the store *before*
        // the sink takes ownership of the handle: in `periodic`/`async` the
        // commit seam never fsyncs, so `RocksStore::durable_sync` is the only
        // thing that can advance it.
        rocks.register_sync_target(
            &(Arc::clone(&outcomes) as Arc<dyn crate::rocks::DurableSyncTarget>),
        );
        // Adopt the caller's shared handle when one is supplied so every shard's
        // flush thread and `CONFIG SET batch-size-threshold-kb` observe one
        // cell; otherwise mint a private one seeded from the static config.
        let batch_size_threshold = match &config.batch_size_threshold_handle {
            Some(handle) => Arc::clone(handle),
            None => Arc::new(AtomicUsize::new(config.batch_size_threshold)),
        };
        let flush_thread = {
            let engine = FlushEngine::new(
                RocksSink::new(rocks, shard_id),
                shard_id,
                &config.mode,
                Arc::clone(&lag),
                Arc::clone(&outcomes),
                metrics_recorder,
            )
            // Adopted the same way as the batch-size handle: the shard task
            // arms this cell at the pre-checkpoint drain and the full-sync
            // coordinator releases it after the cut, so the flush thread and
            // both of them must see one `FlushHold`, not a copy.
            .with_flush_hold(config.flush_hold_handle.clone());
            let bst = Arc::clone(&batch_size_threshold);
            let bt = Duration::from_millis(config.batch_timeout_ms);
            std::thread::Builder::new()
                .name(format!("wal-flush-{shard_id}"))
                .spawn(move || {
                    flush_thread_loop(cmd_rx, engine, bst, bt);
                })
                .expect("failed to spawn WAL flush thread")
        };
        Self {
            shard_id,
            sequence: AtomicU64::new(0),
            cmd_tx,
            lag,
            outcomes,
            batch_size_threshold,
            flush_thread: Some(flush_thread),
        }
    }

    /// Live flush batch-size threshold in bytes. The flush thread re-reads this
    /// on every batch decision (drain loop + flush trigger), so a store takes
    /// effect on the next batch without restarting the flush thread.
    pub fn batch_size_threshold(&self) -> usize {
        self.batch_size_threshold.load(Ordering::Relaxed)
    }

    /// Update the flush batch-size threshold (bytes). Reachable from
    /// `ConfigManager` for `CONFIG SET persistence-batch-size-threshold-kb`.
    pub fn set_batch_size_threshold(&self, bytes: usize) {
        self.batch_size_threshold.store(bytes, Ordering::Relaxed);
    }
    pub async fn write_set(
        &self,
        key: &[u8],
        value: &Value,
        metadata: &KeyMetadata,
    ) -> std::io::Result<u64> {
        let serialized = serialize(value, metadata);
        let size_estimate = key.len() + serialized.len() + 32;
        let seq = self.sequence.fetch_add(1, Ordering::SeqCst) + 1;
        self.cmd_tx
            .send_async(WalCommand::Write(WalEntry::Put {
                seq,
                key: Bytes::copy_from_slice(key),
                value: serialized,
                size_estimate,
            }))
            .await
            .map_err(|_| {
                error!(
                    shard_id = self.shard_id,
                    "WAL flush thread dead (write_set)"
                );
                std::io::Error::other("WAL flush thread disconnected")
            })?;
        Ok(seq)
    }
    /// Enqueue a HyperLogLog register-max delta as a `Merge` operand for `key`.
    ///
    /// The `pairs` are encoded via [`serialize_hll_delta`] into a type-tagged
    /// operand that the CF's merge operator folds onto the base value. Uses the
    /// same sequence assignment and size accounting as [`Self::write_set`].
    pub async fn write_merge(
        &self,
        key: &[u8],
        pairs: &[(u16, u8)],
        metadata: &KeyMetadata,
    ) -> std::io::Result<u64> {
        let operand = serialize_hll_delta(pairs, metadata);
        let size_estimate = key.len() + operand.len() + 32;
        let seq = self.sequence.fetch_add(1, Ordering::SeqCst) + 1;
        self.cmd_tx
            .send_async(WalCommand::Write(WalEntry::Merge {
                seq,
                key: Bytes::copy_from_slice(key),
                operand,
                size_estimate,
            }))
            .await
            .map_err(|_| {
                error!(
                    shard_id = self.shard_id,
                    "WAL flush thread dead (write_merge)"
                );
                std::io::Error::other("WAL flush thread disconnected")
            })?;
        Ok(seq)
    }
    /// Enqueue a full-shard clear as a `Clear` entry.
    ///
    /// The flush thread applies it as a full-range delete over the shard's
    /// primary column family, after first draining every lower-sequence entry
    /// (see [`super::flush::FlushEngine`]) so the tombstone covers exactly the
    /// pre-clear keyspace. Uses the same `fetch_add` sequence discipline as
    /// [`Self::write_set`]; an entry accepted after this one (higher seq) lands
    /// after the tombstone and is not cleared.
    pub async fn write_clear(&self) -> std::io::Result<u64> {
        // Nominal accounting weight for a keyless range-tombstone entry.
        let size_estimate = 64;
        let seq = self.sequence.fetch_add(1, Ordering::SeqCst) + 1;
        self.cmd_tx
            .send_async(WalCommand::Write(WalEntry::Clear { seq, size_estimate }))
            .await
            .map_err(|_| {
                error!(
                    shard_id = self.shard_id,
                    "WAL flush thread dead (write_clear)"
                );
                std::io::Error::other("WAL flush thread disconnected")
            })?;
        Ok(seq)
    }
    pub async fn write_delete(&self, key: &[u8]) -> std::io::Result<u64> {
        let size_estimate = key.len() + 32;
        let seq = self.sequence.fetch_add(1, Ordering::SeqCst) + 1;
        self.cmd_tx
            .send_async(WalCommand::Write(WalEntry::Delete {
                seq,
                key: Bytes::copy_from_slice(key),
                size_estimate,
            }))
            .await
            .map_err(|_| {
                error!(
                    shard_id = self.shard_id,
                    "WAL flush thread dead (write_delete)"
                );
                std::io::Error::other("WAL flush thread disconnected")
            })?;
        Ok(seq)
    }

    /// Open a write group: every entry enqueued before the matching
    /// [`Self::end_group`] lands in one committed storage batch.
    ///
    /// The markers travel the same FIFO channel as the entries themselves, so
    /// the grouping is decided by *enqueue* order, not by wall-clock timing —
    /// the shard task may be descheduled mid-group without the flush thread
    /// cutting a batch underneath it. This is what makes a `MULTI`/`EXEC` (and
    /// any multi-key command) atomic with respect to a checkpoint cut or a
    /// crash: see [`super::flush::FlushEngine::begin_group`].
    ///
    /// Callers must pair this with [`Self::end_group`] on every path,
    /// including errors; an unclosed group suppresses background flushing
    /// until the next explicit flush or shutdown.
    pub async fn begin_group(&self) -> std::io::Result<()> {
        self.send_marker(WalCommand::GroupBegin, "begin_group")
            .await
    }

    /// Close the innermost open write group, re-arming the flush triggers.
    pub async fn end_group(&self) -> std::io::Result<()> {
        self.send_marker(WalCommand::GroupEnd, "end_group").await
    }

    async fn send_marker(&self, cmd: WalCommand, what: &'static str) -> std::io::Result<()> {
        self.cmd_tx.send_async(cmd).await.map_err(|_| {
            error!(shard_id = self.shard_id, "WAL flush thread dead ({what})");
            std::io::Error::other("WAL flush thread disconnected")
        })
    }

    /// Flush the buffered entries, returning this flush attempt's result.
    ///
    /// This reports only the outcome of the flush it triggers; failures of
    /// earlier background flushes are surfaced through [`Self::flush_through`]
    /// (for durability confirmation) and [`Self::lag_stats`] (for
    /// observability). Use this for "drain the buffer now" call sites
    /// (shutdown), and `flush_through` when acknowledging a write depends on
    /// the answer.
    pub async fn flush_async(&self) -> std::io::Result<()> {
        self.send_flush().await
    }

    /// Confirm that every WAL entry written after `after_seq` is durable.
    ///
    /// Flushes the buffer, then checks the recorded flush outcomes: the
    /// confirmation fails if this flush failed **or** any background
    /// (size-threshold / timeout) flush that carried entries assigned after
    /// `after_seq` failed since. This closes the window where a write was
    /// batched, background-flushed unsuccessfully, and the final explicit
    /// flush of an emptied buffer reported `Ok`.
    ///
    /// Callers capture `after_seq = self.sequence()` *before* writing their
    /// entries.
    pub async fn flush_through(&self, after_seq: u64) -> std::io::Result<()> {
        let target_seq = self.sequence.load(Ordering::SeqCst);
        let flush_result = self.send_flush().await;
        self.outcomes
            .confirm_durable_through(after_seq, target_seq, flush_result)
    }

    async fn send_flush(&self) -> std::io::Result<()> {
        let (done_tx, done_rx) = flume::bounded(1);
        self.cmd_tx
            .send_async(WalCommand::Flush { done_tx })
            .await
            .map_err(|_| {
                error!(
                    shard_id = self.shard_id,
                    "WAL flush thread dead (flush_async)"
                );
                std::io::Error::other("WAL flush thread disconnected")
            })?;
        done_rx.recv_async().await.map_err(|_| {
            error!(shard_id = self.shard_id, "WAL flush response lost");
            std::io::Error::other("WAL flush response channel disconnected")
        })?
    }
    pub fn lag_stats(&self) -> WalLagStats {
        let now_ms = current_timestamp_ms();
        let lft = self.lag.last_flush_timestamp_ms.load(Ordering::Acquire);
        let dlm = now_ms.saturating_sub(lft);
        WalLagStats {
            pending_ops: self.lag.pending_ops.load(Ordering::Acquire),
            pending_bytes: self.lag.pending_bytes.load(Ordering::Acquire),
            durability_lag_ms: dlm,
            sequence: self.sequence.load(Ordering::SeqCst),
            committed_sequence: self.outcomes.committed_sequence(),
            flush_failures: self.outcomes.flush_failures(),
            lost_ops: self.outcomes.lost_ops(),
            lost_bytes: self.outcomes.lost_bytes(),
            last_flush_ok: self.outcomes.last_flush_ok(),
            shard_id: self.shard_id,
            last_flush_timestamp_ms: lft,
        }
    }
    /// Highest sequence assigned to a WAL entry so far.
    pub fn sequence(&self) -> u64 {
        self.sequence.load(Ordering::SeqCst)
    }
    /// Highest sequence an fsync has covered.
    ///
    /// In `sync` mode this tracks every commit; in `periodic`/`async` it only
    /// advances when [`RocksStore::durable_sync`] (or an operator `sync_wal`)
    /// publishes the sequence its flush covered. Use
    /// [`Self::committed_sequence`] for "reached storage".
    pub fn durable_sequence(&self) -> u64 {
        self.outcomes.durable_sequence()
    }
    /// Highest sequence committed to storage, fsynced or not.
    pub fn committed_sequence(&self) -> u64 {
        self.outcomes.committed_sequence()
    }
    pub fn shard_id(&self) -> usize {
        self.shard_id
    }
    /// Whether this shard has lost a WAL entry and not been reset since
    /// (FM-PERSISTENCE-053).
    pub fn poisoned(&self) -> bool {
        self.outcomes.poisoned()
    }
    /// The first lost sequence, or `0` while the shard is healthy.
    pub fn poisoned_sequence(&self) -> u64 {
        self.outcomes.poisoned_sequence()
    }
    /// Clear the poison latch. Operator action only — see
    /// [`super::WalSink::clear_poison`].
    pub fn clear_poison(&self) {
        self.outcomes.clear_poison();
    }
}

impl Drop for RocksWalWriter {
    fn drop(&mut self) {
        let (dead_tx, _) = flume::bounded(0);
        let _ = std::mem::replace(&mut self.cmd_tx, dead_tx);
        if let Some(h) = self.flush_thread.take() {
            let _ = h.join();
        }
    }
}

#[async_trait::async_trait]
impl super::WalSink for RocksWalWriter {
    async fn write_set(
        &self,
        key: &[u8],
        value: &Value,
        metadata: &KeyMetadata,
    ) -> std::io::Result<u64> {
        RocksWalWriter::write_set(self, key, value, metadata).await
    }
    async fn write_merge(
        &self,
        key: &[u8],
        pairs: &[(u16, u8)],
        metadata: &KeyMetadata,
    ) -> std::io::Result<u64> {
        RocksWalWriter::write_merge(self, key, pairs, metadata).await
    }
    async fn write_delete(&self, key: &[u8]) -> std::io::Result<u64> {
        RocksWalWriter::write_delete(self, key).await
    }
    async fn write_clear(&self) -> std::io::Result<u64> {
        RocksWalWriter::write_clear(self).await
    }
    async fn begin_group(&self) -> std::io::Result<()> {
        RocksWalWriter::begin_group(self).await
    }
    async fn end_group(&self) -> std::io::Result<()> {
        RocksWalWriter::end_group(self).await
    }
    async fn flush_async(&self) -> std::io::Result<()> {
        RocksWalWriter::flush_async(self).await
    }
    async fn flush_through(&self, after_seq: u64) -> std::io::Result<()> {
        RocksWalWriter::flush_through(self, after_seq).await
    }
    fn sequence(&self) -> u64 {
        RocksWalWriter::sequence(self)
    }
    fn durable_sequence(&self) -> u64 {
        RocksWalWriter::durable_sequence(self)
    }
    fn lag_stats(&self) -> WalLagStats {
        RocksWalWriter::lag_stats(self)
    }
    fn shard_id(&self) -> usize {
        RocksWalWriter::shard_id(self)
    }
    fn poisoned(&self) -> bool {
        RocksWalWriter::poisoned(self)
    }
    fn clear_poison(&self) {
        RocksWalWriter::clear_poison(self);
    }
}

#[cfg(test)]
mod sink_tests {
    use super::*;

    #[test]
    fn rocks_wal_writer_is_a_wal_sink() {
        fn assert_wal_sink<T: super::super::WalSink>() {}
        assert_wal_sink::<RocksWalWriter>();
        // Object-safety: the trait must be usable as a boxed trait object.
        fn _accepts_dyn(_: Box<dyn super::super::WalSink>) {}
    }
}
