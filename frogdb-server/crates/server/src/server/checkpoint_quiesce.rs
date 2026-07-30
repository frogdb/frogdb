//! The one contract every RocksDB checkpoint cut must honour first.
//!
//! A checkpoint is a snapshot of what RocksDB *holds*, and under the default
//! (non-`sync`) durability a write is acknowledged as soon as it is staged in
//! its shard's WAL flush engine — the engine commits it to RocksDB on a later
//! size/timeout trigger. Cutting a checkpoint without draining that engine
//! therefore produces an artifact that is missing the most recent acknowledged
//! writes.
//!
//! Two call sites cut checkpoints and both must drain first:
//!
//! * `BGSAVE` / periodic snapshots (the snapshot coordinator's pre-snapshot
//!   hook) — issue 13, where the missing writes made a *recovery* artifact
//!   silently incomplete.
//! * A `FULLRESYNC` checkpoint for a replica — where the hole is worse: the
//!   checkpoint is the only carrier of every write the primary made before the
//!   replica attached (with no replica connected there is nothing in the
//!   backlog to replay), so a write missing from it is missing from the replica
//!   *forever*.
//!
//! Living in one place is the point: the two paths cut the same kind of
//! artifact from the same database, and the second one shipped without the
//! drain precisely because the contract lived inline at the first.
//!
//! # Why a fan-out of shard messages is sufficient
//!
//! Ordering, not timing. A write's WAL entry is enqueued into its shard's flush
//! channel by the `WalPersistence` write effect, which runs *before* the
//! `ReplicationBroadcast` effect acknowledges it to any observer (see
//! `WRITE_EFFECT_ORDER`). The drain below is delivered to the same shard task
//! afterwards, and it forwards a `Flush` command down the same FIFO channel the
//! entry went into. So every write acknowledged before this function is called
//! is committed to RocksDB by the time it returns, whatever the batch timeout
//! is set to. Writes that land *during* the drain may or may not be captured —
//! that direction is safe, it only adds data to the checkpoint.

use frogdb_core::{SearchMsg, ShardSender};

/// Quiesce every shard's persistence pipeline so a checkpoint cut after this
/// returns contains every write acknowledged before it was called.
///
/// Search indexes are committed first so their `search_meta` writes are
/// themselves drained by the WAL pass that follows. Errors are the shards' to
/// log: a shard that cannot be reached (or that drops the ack) leaves its
/// writes uncaptured, which is exactly the pre-existing behaviour of a
/// checkpoint cut while a shard is wedged — it must not abort the checkpoint.
pub(super) async fn quiesce_shards_for_checkpoint(senders: &[ShardSender]) {
    fan_out(senders, |response_tx| SearchMsg::FlushSearchIndexes {
        response_tx,
    })
    .await;
    fan_out(senders, |response_tx| SearchMsg::FlushWal { response_tx }).await;
}

/// Send one ack-carrying message to every shard, then await all the acks.
///
/// Two passes on purpose: all shards drain concurrently instead of one at a
/// time, so the quiesce costs one flush latency rather than `num_shards` of
/// them.
async fn fan_out<F>(senders: &[ShardSender], message: F)
where
    F: Fn(tokio::sync::oneshot::Sender<()>) -> SearchMsg,
{
    let mut receivers = Vec::with_capacity(senders.len());
    for sender in senders {
        let (tx, rx) = tokio::sync::oneshot::channel();
        let _ = sender.send(message(tx)).await;
        receivers.push(rx);
    }
    for rx in receivers {
        let _ = rx.await;
    }
}
