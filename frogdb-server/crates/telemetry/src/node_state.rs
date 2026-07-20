//! The one node-observable state snapshot.
//!
//! Node-observable per-shard state — memory, keyspace, eviction/expiry, tiered
//! occupancy, dirty counters, keysize histograms, and WAL durability lag — was
//! folded **three times**: once by INFO's `ShardInfoSnapshot` (a single
//! `InfoSnapshot` scatter), and twice more by telemetry's `/status`
//! (`MemoryStats` **and** `WalLagStats` scatters), plus a fourth bespoke fold in
//! the debug UI. Each surface re-declared the concept and re-derived the
//! aggregate, so the surfaces could — and did — disagree.
//!
//! [`NodeStateSnapshot`] is the single source of truth. One [`collect`] performs
//! **one** `InfoSnapshot` fleet scatter and folds every reply into this value;
//! INFO, telemetry `/status`, and the debug UI all render from it. The
//! aggregation lives here, once, so the three surfaces can never disagree.
//!
//! [`collect`]: NodeStateSnapshot::collect

use std::time::Duration;

use frogdb_core::{
    InfoShardSnapshot, KeysizeHistograms, ShardMessage, ShardSender, TieredCounts, WalLagAggregate,
};
use tokio::sync::oneshot;

/// One shard's row, retained verbatim so per-shard surfaces (telemetry
/// `/status` shard array, debug UI shard-stats panel) render without a second
/// scatter or a parallel fold.
#[derive(Debug, Clone, Default)]
pub struct ShardState {
    /// Shard identifier.
    pub shard_id: usize,
    /// Number of keys resident on the shard.
    pub keys: usize,
    /// Data memory used by the shard in bytes.
    pub data_memory: usize,
    /// Peak (high-water-mark) memory used by the shard in bytes.
    pub peak_memory: u64,
    /// Keys resident in the hot tier.
    pub hot_keys: usize,
    /// Keys resident in the warm (spilled) tier.
    pub warm_keys: usize,
}

/// Node-observable state, folded once per request from a single `InfoSnapshot`
/// fleet scatter.
///
/// The aggregate fields (summed memory/keys/eviction, merged keysizes, folded
/// WAL lag) are exactly the view INFO renders; [`per_shard`] adds the per-shard
/// rows the `/status` and debug surfaces need. Adding a per-shard observable is
/// a new field here plus one line in [`NodeStateSnapshot::absorb`] — never a new
/// round trip and never a new parallel struct.
///
/// [`per_shard`]: NodeStateSnapshot::per_shard
#[derive(Debug, Clone, Default)]
pub struct NodeStateSnapshot {
    /// Total data memory across all shards (bytes).
    pub used_memory: usize,
    /// Sum of per-shard peak memory high-water marks (bytes).
    pub peak_memory: u64,
    /// Total number of keys across all shards.
    pub keys: usize,
    /// Total keys evicted across all shards.
    pub evicted_keys: u64,
    /// Total keys expired across all shards.
    pub expired_keys: u64,
    /// Total objects freed via lazyfree across all shards.
    pub lazyfreed_objects: u64,
    /// Total writes since last snapshot across all shards.
    pub dirty: u64,
    /// Summed tiered-storage counters.
    pub tiered: TieredCounts,
    /// Keysize histograms merged across all shards.
    pub keysizes: KeysizeHistograms,
    /// Aggregated WAL lag; `None` when persistence is disabled on every shard.
    pub wal: Option<WalLagAggregate>,
    /// Primary host when running as a replica (from shard identity).
    pub master_host: Option<String>,
    /// Primary port when running as a replica (from shard identity).
    pub master_port: Option<u16>,
    /// Per-shard rows, in shard order.
    pub per_shard: Vec<ShardState>,
}

impl NodeStateSnapshot {
    /// Fold one shard's `InfoSnapshot` reply into the aggregate and record its
    /// per-shard row.
    pub fn absorb(&mut self, snap: InfoShardSnapshot) {
        self.used_memory += snap.memory.data_memory;
        self.peak_memory += snap.memory.peak_memory;
        self.keys += snap.memory.keys;
        self.evicted_keys += snap.memory.evicted_keys;
        self.expired_keys += snap.memory.expired_keys;
        self.lazyfreed_objects += snap.memory.lazyfreed_objects;
        self.dirty += snap.dirty;
        self.tiered.hot_keys += snap.tiered.hot_keys;
        self.tiered.warm_keys += snap.tiered.warm_keys;
        self.tiered.unspills += snap.tiered.unspills;
        self.tiered.spills += snap.tiered.spills;
        self.tiered.expired_on_unspill += snap.tiered.expired_on_unspill;
        self.keysizes.merge(&snap.keysizes);
        if let Some(lag) = &snap.wal_lag {
            self.wal
                .get_or_insert_with(WalLagAggregate::new)
                .absorb(lag);
        }
        if self.master_host.is_none() {
            self.master_host = snap.master_host.clone();
        }
        if self.master_port.is_none() {
            self.master_port = snap.master_port;
        }
        self.per_shard.push(ShardState {
            shard_id: snap.shard_id,
            keys: snap.memory.keys,
            data_memory: snap.memory.data_memory,
            peak_memory: snap.memory.peak_memory,
            hot_keys: snap.tiered.hot_keys,
            warm_keys: snap.tiered.warm_keys,
        });
    }

    /// The one place a node-state snapshot is gathered: a single
    /// [`ShardMessage::InfoSnapshot`] scatter to every shard, folded under a
    /// shared deadline.
    ///
    /// Sends exactly one message per shard — the single-round-trip invariant the
    /// three observability surfaces share. A shard that never receives the
    /// request, drops its reply channel, or misses the deadline is a hard error
    /// ([`ShardScatterError`]) so a caller can never silently under-report; the
    /// best-effort surfaces map the error to an empty snapshot themselves.
    pub async fn collect(
        senders: &[ShardSender],
        timeout: Duration,
    ) -> Result<Self, ShardScatterError> {
        let mut rxs = Vec::with_capacity(senders.len());
        for (shard_id, sender) in senders.iter().enumerate() {
            let (response_tx, rx) = oneshot::channel();
            if sender
                .send(ShardMessage::InfoSnapshot { response_tx })
                .await
                .is_err()
            {
                return Err(ShardScatterError::Unavailable { shard_id });
            }
            rxs.push((shard_id, rx));
        }

        // One deadline for the whole gather, not one per receiver.
        let deadline = tokio::time::sleep(timeout);
        tokio::pin!(deadline);
        let mut snapshot = Self::default();
        for (shard_id, rx) in rxs {
            tokio::select! {
                reply = rx => match reply {
                    Ok(snap) => snapshot.absorb(snap),
                    Err(_) => return Err(ShardScatterError::Dropped { shard_id }),
                },
                _ = &mut deadline => return Err(ShardScatterError::Timeout { shard_id }),
            }
        }
        Ok(snapshot)
    }
}

/// Why a [`NodeStateSnapshot::collect`] scatter failed, with the offending
/// shard. INFO renders each variant as a distinct wire error; best-effort
/// surfaces discard it and fall back to an empty snapshot.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ShardScatterError {
    /// The shard's channel was closed before the request could be sent.
    Unavailable {
        /// Shard that could not be reached.
        shard_id: usize,
    },
    /// The shard received the request but dropped its reply channel.
    Dropped {
        /// Shard that dropped the request.
        shard_id: usize,
    },
    /// The shard did not reply before the shared deadline elapsed.
    Timeout {
        /// Shard that missed the deadline.
        shard_id: usize,
    },
}

impl ShardScatterError {
    /// The shard the failure is attributed to.
    pub fn shard_id(&self) -> usize {
        match self {
            Self::Unavailable { shard_id }
            | Self::Dropped { shard_id }
            | Self::Timeout { shard_id } => *shard_id,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use frogdb_core::{ShardMemoryStats, ShardReceiver, WalLagStats};
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tokio::sync::mpsc;

    fn shard_snap(shard_id: usize) -> InfoShardSnapshot {
        InfoShardSnapshot {
            shard_id,
            memory: ShardMemoryStats {
                shard_id,
                data_memory: 100,
                keys: 10,
                peak_memory: 200,
                evicted_keys: 1,
                expired_keys: 2,
                lazyfreed_objects: 3,
                ..Default::default()
            },
            dirty: 5,
            tiered: TieredCounts {
                hot_keys: 4,
                warm_keys: 3,
                unspills: 2,
                spills: 1,
                expired_on_unspill: 1,
            },
            keysizes: KeysizeHistograms::new(),
            wal_lag: None,
            master_host: None,
            master_port: None,
        }
    }

    #[test]
    fn absorb_sums_aggregate_and_records_per_shard_rows() {
        let mut agg = NodeStateSnapshot::default();
        agg.absorb(shard_snap(0));
        agg.absorb(shard_snap(1));
        // Aggregate.
        assert_eq!(agg.used_memory, 200);
        assert_eq!(agg.peak_memory, 400);
        assert_eq!(agg.keys, 20);
        assert_eq!(agg.evicted_keys, 2);
        assert_eq!(agg.expired_keys, 4);
        assert_eq!(agg.lazyfreed_objects, 6);
        assert_eq!(agg.dirty, 10);
        assert_eq!(agg.tiered.hot_keys, 8);
        assert_eq!(agg.tiered.spills, 2);
        assert!(agg.wal.is_none());
        // Per-shard rows.
        assert_eq!(agg.per_shard.len(), 2);
        assert_eq!(agg.per_shard[0].shard_id, 0);
        assert_eq!(agg.per_shard[1].keys, 10);
        assert_eq!(agg.per_shard[1].data_memory, 100);
        assert_eq!(agg.per_shard[1].peak_memory, 200);
        assert_eq!(agg.per_shard[1].hot_keys, 4);
        assert_eq!(agg.per_shard[1].warm_keys, 3);
    }

    #[test]
    fn absorb_folds_wal_lag_through_shared_aggregate() {
        let lag = |shard_id, pending, dlag, ok| WalLagStats {
            pending_ops: pending,
            pending_bytes: pending * 10,
            durability_lag_ms: dlag,
            sequence: 0,
            durable_sequence: 0,
            flush_failures: 1,
            lost_ops: 2,
            lost_bytes: 200,
            last_flush_ok: ok,
            shard_id,
            last_flush_timestamp_ms: 1_000,
        };
        let mut a = shard_snap(0);
        a.wal_lag = Some(lag(0, 3, 50, true));
        let mut b = shard_snap(1);
        b.wal_lag = Some(lag(1, 4, 80, false));

        let mut agg = NodeStateSnapshot::default();
        agg.absorb(a);
        agg.absorb(b);
        let wal = agg.wal.expect("wal present");
        assert_eq!(wal.pending_ops, 7);
        assert_eq!(wal.max_durability_lag_ms, 80);
        assert!(!wal.last_flush_ok);
        assert_eq!(wal.per_shard.len(), 2);
    }

    /// Mock shards that answer `InfoSnapshot` and count every message received,
    /// guarding the single-round-trip invariant.
    fn mock_shards(n: usize) -> (Vec<ShardSender>, Arc<AtomicUsize>) {
        let messages = Arc::new(AtomicUsize::new(0));
        let mut senders = Vec::new();
        for shard_id in 0..n {
            let (tx, rx) = mpsc::channel(16);
            senders.push(ShardSender::new(tx));
            let messages = Arc::clone(&messages);
            let mut receiver = ShardReceiver::new(rx);
            tokio::spawn(async move {
                while let Some(env) = receiver.recv().await {
                    messages.fetch_add(1, Ordering::SeqCst);
                    if let ShardMessage::InfoSnapshot { response_tx } = env.message {
                        let _ = response_tx.send(shard_snap(shard_id));
                    }
                }
            });
        }
        (senders, messages)
    }

    #[tokio::test]
    async fn collect_sends_exactly_one_message_per_shard() {
        let (senders, messages) = mock_shards(4);
        let snap = NodeStateSnapshot::collect(&senders, Duration::from_secs(5))
            .await
            .expect("collect succeeds");
        assert_eq!(snap.keys, 40, "all four shards folded");
        assert_eq!(snap.per_shard.len(), 4);
        assert_eq!(
            messages.load(Ordering::SeqCst),
            4,
            "one collect = one message per shard, no second scatter"
        );
    }

    #[tokio::test]
    async fn collect_errors_when_a_shard_drops_the_request() {
        let (tx0, rx0) = mpsc::channel(16);
        let (tx1, rx1) = mpsc::channel(16);
        let senders = vec![ShardSender::new(tx0), ShardSender::new(tx1)];
        let mut r0 = ShardReceiver::new(rx0);
        tokio::spawn(async move {
            while let Some(env) = r0.recv().await {
                if let ShardMessage::InfoSnapshot { response_tx } = env.message {
                    let _ = response_tx.send(shard_snap(0));
                }
            }
        });
        let mut r1 = ShardReceiver::new(rx1);
        tokio::spawn(async move {
            while let Some(env) = r1.recv().await {
                drop(env); // drops response_tx without replying
            }
        });
        let err = NodeStateSnapshot::collect(&senders, Duration::from_secs(5))
            .await
            .expect_err("a dropped shard must not silently under-report");
        assert_eq!(err, ShardScatterError::Dropped { shard_id: 1 });
    }
}
