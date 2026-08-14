//! The `WalSink` trait: the WAL-write surface a shard needs, as a seam.
use super::config::WalLagStats;
use async_trait::async_trait;
use frogdb_types::types::{KeyMetadata, Value};

/// The WAL-write surface a shard needs, as a seam. [`RocksWalWriter`] is the
/// production implementation; `FakeWalSink` (testing) is the deterministic
/// in-process one. Introduced so `ShardPersistence` can hold either behind a
/// trait object without the shard knowing which.
///
/// [`RocksWalWriter`]: super::RocksWalWriter
#[async_trait]
pub trait WalSink: Send + Sync {
    /// Enqueue a full-value write for `key`, returning the assigned sequence.
    async fn write_set(
        &self,
        key: &[u8],
        value: &Value,
        metadata: &KeyMetadata,
    ) -> std::io::Result<u64>;
    /// Enqueue a merge (HLL register-max delta) operand for `key`.
    async fn write_merge(
        &self,
        key: &[u8],
        pairs: &[(u16, u8)],
        metadata: &KeyMetadata,
    ) -> std::io::Result<u64>;
    /// Enqueue a tombstone for `key`.
    async fn write_delete(&self, key: &[u8]) -> std::io::Result<u64>;
    /// Enqueue a full-shard clear.
    async fn write_clear(&self) -> std::io::Result<u64>;
    /// Open a write group: entries enqueued until the matching
    /// [`Self::end_group`] must land in one committed storage batch, so a
    /// checkpoint or a crash can never observe a prefix of them.
    ///
    /// Groups nest, and every caller must close what it opens — including on
    /// the error path.
    async fn begin_group(&self) -> std::io::Result<()>;
    /// Close the innermost open write group.
    async fn end_group(&self) -> std::io::Result<()>;
    /// Flush buffered entries now.
    async fn flush_async(&self) -> std::io::Result<()>;
    /// Confirm every entry written after `after_seq` is durable.
    async fn flush_through(&self, after_seq: u64) -> std::io::Result<()>;
    /// Highest sequence assigned so far.
    fn sequence(&self) -> u64;
    /// Highest sequence confirmed durable.
    fn durable_sequence(&self) -> u64;
    /// Lag / durability observability snapshot.
    fn lag_stats(&self) -> WalLagStats;
    /// The shard this sink serves.
    fn shard_id(&self) -> usize;
    /// Whether this shard has lost a WAL entry and not been reset since
    /// (FM-PERSISTENCE-053). A poisoned sink persists nothing further, so the
    /// shard reads this to decide whether to keep accepting writes
    /// (FM-PERSISTENCE-055).
    fn poisoned(&self) -> bool;
    /// Clear the poison latch — the operator reset (FM-PERSISTENCE-053). The
    /// only other way back is a restart, which re-reads what is on disk.
    /// Nothing on an automatic path may call this: a shard that un-poisons
    /// itself because a later flush worked is the fsyncgate bug.
    fn clear_poison(&self);
}
