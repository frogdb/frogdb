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
//!
//! # Why an undrained shard fails the checkpoint
//!
//! The drain used to discard both halves of every exchange (`let _ = send`,
//! `let _ = rx.await`) so a wedged shard could not hang a checkpoint. That kept
//! the cut alive but made the resulting artifact a lie: it was reported as a
//! successful save, or shipped as a replica's entire dataset, while missing
//! every acknowledged write still sitting in that shard's flush engine — with
//! no log line, no counter, and nothing at restore time to tell it apart from a
//! complete one (issue 05).
//!
//! There is no timeout here, so an exchange can only fail when the shard task
//! is *gone*: a closed channel (the task ended or panicked) or a dropped ack
//! (the responder was dropped without answering). A merely slow shard still
//! blocks, exactly as before — the drain waits for it. So a failure is never
//! "this shard is busy", it is "this shard is not running", and a checkpoint of
//! a database whose shards are not all running is not a checkpoint anyone
//! should be handed. Both callers therefore surface it as a failure rather than
//! producing the artifact:
//!
//! * `BGSAVE` reports the save as failed (`rdb_last_bgsave_status:err` with the
//!   cause, `rdb_bgsave_failures`), leaves `LASTSAVE` where it was, and keeps
//!   the previous snapshot as the newest one on disk — a known-good artifact
//!   beats a knowingly-incomplete one, which is also Redis' behaviour when a
//!   background save cannot complete.
//! * `FULLRESYNC` fails the handshake, so the replica retries on its reconnect
//!   backoff instead of silently inheriting a permanent hole.

use frogdb_core::{SearchMsg, ShardSender};

/// Shards that did not complete one wave of the pre-checkpoint drain.
///
/// Carries the wave it happened in and the shard indices, so the operator-facing
/// message names what is wedged rather than just reporting a count.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("{stage}: {} of {total} shard(s) did not drain (shard {shards:?})", .shards.len())]
pub(super) struct QuiesceIncomplete {
    /// Which fan-out wave failed — the search-index commit or the WAL drain.
    stage: &'static str,
    /// Indices of the shards that could not be reached or dropped the ack.
    shards: Vec<usize>,
    /// How many shards the wave was sent to.
    total: usize,
}

/// Quiesce every shard's persistence pipeline so a checkpoint cut after this
/// returns contains every write acknowledged before it was called.
///
/// Search indexes are committed first so their `search_meta` writes are
/// themselves drained by the WAL pass that follows. A shard that cannot be
/// reached (its task is gone) or that drops the ack fails the whole drain: its
/// staged writes would be missing from the artifact, and the caller must not
/// publish one that is silently incomplete (see the module docs).
pub(super) async fn quiesce_shards_for_checkpoint(
    senders: &[ShardSender],
) -> Result<(), QuiesceIncomplete> {
    fan_out(senders, "search-index flush", |response_tx| {
        SearchMsg::FlushSearchIndexes { response_tx }
    })
    .await?;
    fan_out(senders, "WAL drain", |response_tx| SearchMsg::FlushWal {
        response_tx,
    })
    .await
}

/// Send one ack-carrying message to every shard, then await all the acks.
///
/// Two passes on purpose: all shards drain concurrently instead of one at a
/// time, so the quiesce costs one flush latency rather than `num_shards` of
/// them. Every shard is still attempted after one fails, so the error names the
/// full set rather than the first index the loop happened to reach.
async fn fan_out<F>(
    senders: &[ShardSender],
    stage: &'static str,
    message: F,
) -> Result<(), QuiesceIncomplete>
where
    F: Fn(tokio::sync::oneshot::Sender<()>) -> SearchMsg,
{
    let mut receivers = Vec::with_capacity(senders.len());
    let mut failed = Vec::new();
    for (shard, sender) in senders.iter().enumerate() {
        let (tx, rx) = tokio::sync::oneshot::channel();
        if sender.send(message(tx)).await.is_ok() {
            receivers.push((shard, rx));
        } else {
            failed.push(shard);
        }
    }
    for (shard, rx) in receivers {
        if rx.await.is_err() {
            failed.push(shard);
        }
    }
    if failed.is_empty() {
        return Ok(());
    }
    failed.sort_unstable();
    let incomplete = QuiesceIncomplete {
        stage,
        shards: failed,
        total: senders.len(),
    };
    // Logged here rather than at each caller: both of them turn this into their
    // own failure, and this is the only place that knows *which* shards.
    tracing::error!(
        error = %incomplete,
        "Pre-checkpoint quiesce incomplete; the checkpoint will not be cut"
    );
    Err(incomplete)
}

#[cfg(test)]
mod tests {
    use super::*;
    use frogdb_core::{ShardMessage, ShardReceiver};
    use tokio::sync::mpsc;

    /// A shard task that answers every drain message, like a healthy shard.
    fn healthy_shard() -> ShardSender {
        let (tx, rx) = mpsc::channel(16);
        let mut receiver = ShardReceiver::new(rx);
        tokio::spawn(async move {
            while let Some(env) = receiver.recv().await {
                match env.message {
                    ShardMessage::Search(SearchMsg::FlushSearchIndexes { response_tx })
                    | ShardMessage::Search(SearchMsg::FlushWal { response_tx }) => {
                        let _ = response_tx.send(());
                    }
                    _ => {}
                }
            }
        });
        ShardSender::new(tx)
    }

    /// A shard whose task is gone: the receiver is dropped, so every send fails.
    fn dead_shard() -> ShardSender {
        let (tx, rx) = mpsc::channel(16);
        drop(rx);
        ShardSender::new(tx)
    }

    /// A shard that accepts the message and never answers: the responder is
    /// dropped with the message, so the ack resolves to an error instead of
    /// hanging (a *slow* shard would block, and is meant to).
    fn ack_dropping_shard() -> ShardSender {
        let (tx, rx) = mpsc::channel(16);
        let mut receiver = ShardReceiver::new(rx);
        tokio::spawn(async move { while receiver.recv().await.is_some() {} });
        ShardSender::new(tx)
    }

    // FM-PERSISTENCE-020
    /// Every shard acking means the drain reports success — the healthy case
    /// both checkpoint paths depend on.
    #[tokio::test]
    async fn quiesce_succeeds_when_every_shard_acks() {
        let senders: Vec<_> = (0..4).map(|_| healthy_shard()).collect();
        assert_eq!(quiesce_shards_for_checkpoint(&senders).await, Ok(()));
    }

    // FM-PERSISTENCE-020
    /// A shard whose task is gone (closed channel) fails the drain, and the
    /// error names that shard rather than reporting a bare count.
    #[tokio::test]
    async fn quiesce_fails_when_a_shard_channel_is_closed() {
        let senders = vec![healthy_shard(), dead_shard(), healthy_shard()];
        let err = quiesce_shards_for_checkpoint(&senders)
            .await
            .expect_err("an unreachable shard must fail the drain");
        assert_eq!(err.shards, vec![1]);
        assert_eq!(err.total, 3);
        assert_eq!(err.stage, "search-index flush");
        assert!(
            err.to_string().contains("1 of 3 shard(s) did not drain"),
            "the cause must be operator-readable: {err}"
        );
    }

    // FM-PERSISTENCE-020
    /// A shard that takes the message and drops the ack fails the drain too —
    /// the half the old `let _ = rx.await` swallowed. Search indexes commit
    /// first, so a shard that only drops the WAL ack is reported against the
    /// WAL wave.
    #[tokio::test]
    async fn quiesce_fails_when_a_shard_drops_the_ack() {
        let senders = vec![healthy_shard(), ack_dropping_shard()];
        let err = quiesce_shards_for_checkpoint(&senders)
            .await
            .expect_err("a dropped ack must fail the drain");
        assert_eq!(err.shards, vec![1]);
        assert_eq!(err.stage, "search-index flush");
    }

    // FM-PERSISTENCE-020
    /// Every failing shard is reported, not just the first: the drain attempts
    /// all of them before returning, so an operator sees the whole blast radius
    /// in one line.
    #[tokio::test]
    async fn quiesce_reports_every_undrained_shard() {
        let senders = vec![
            dead_shard(),
            healthy_shard(),
            ack_dropping_shard(),
            dead_shard(),
        ];
        let err = quiesce_shards_for_checkpoint(&senders)
            .await
            .expect_err("three wedged shards must fail the drain");
        assert_eq!(err.shards, vec![0, 2, 3]);
        assert_eq!(err.total, 4);
    }
}
