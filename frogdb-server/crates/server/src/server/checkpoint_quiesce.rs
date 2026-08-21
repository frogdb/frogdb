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
//! is set to.
//!
//! # Why `FULLRESYNC` needs more than the drain
//!
//! For `BGSAVE` the other direction is free: writes that land *during* the drain
//! may or may not be captured, and that is safe — it only adds data to a
//! recovery artifact.
//!
//! It is **not** safe for `FULLRESYNC`. The replica is told the payload covers
//! everything up to a captured offset and then replays `(offset, current]` from
//! the backlog verbatim, so a write that slipped into the checkpoint *above*
//! that offset is applied twice — and `INCR`/`LPUSH`/`APPEND` are not
//! idempotent. That path therefore does two extra things: each shard reports the
//! offset of the last write it broadcast (its watermark `Y_s`, which the replica
//! installs as a per-shard skip floor), and the shard's flush engine is *held*
//! from the drain until the cut, so nothing above `Y_s` can enter the payload
//! behind the claim. The hold is self-expiring; a shard whose hold lapsed before
//! the cut **fails the sync** — no payload is sent, the link drops and the
//! replica retries from scratch. Claiming `0` for that shard instead would be a
//! sound-looking degradation and is not: no floor means the overshipped range
//! re-executes, which is the very corruption the vector exists to prevent, back
//! again in the slow-cut shape and invisible to the replica.
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

use std::sync::{Arc, Mutex};

use frogdb_core::{FULL_SYNC_HOLD, FlushHold, SearchMsg, ShardSender, WalDrainAck};
use frogdb_replication::fullsync::ShardCoverage;
use frogdb_replication::primary::{CaptureHold, FullSyncCapture};

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
    quiesce(senders, None).await.map(|_| ())
}

/// Quiesce every shard's persistence pipeline for a `FULLRESYNC` payload, and
/// report what that payload will already contain.
///
/// Does everything [`quiesce_shards_for_checkpoint`] does, plus the two things
/// only the replica-facing path needs (see the module docs): it collects each
/// shard's coverage watermark `Y_s`, and it leaves each shard's flush engine
/// **held**, so no write above that watermark can be committed into the payload
/// between this call and the cut.
///
/// The returned capture owns the holds. Releasing it is not optional — drop it
/// as soon as the cut is done, on success *and* on failure. It is not a latch:
/// every hold also expires on its own [`FULL_SYNC_HOLD`] deadline, so a caller
/// that dies mid-sync stalls this node's `sync`-durability write acks for at
/// most that long rather than forever.
///
/// If the drain itself fails, the holds of the shards that *did* answer are
/// left to expire on that deadline: there is no payload to protect any more,
/// and the caller has no capture to release them through.
pub(super) async fn quiesce_shards_for_full_sync(
    senders: &[ShardSender],
) -> Result<FullSyncCapture, QuiesceIncomplete> {
    let acks = quiesce(senders, Some(FULL_SYNC_HOLD)).await?;
    let mut watermarks = Vec::with_capacity(acks.len());
    let mut holds = Vec::with_capacity(acks.len());
    for (shard, ack) in acks.into_iter().enumerate() {
        watermarks.push(ack.last_broadcast_offset);
        if let Some(hold) = ack.hold {
            holds.push((shard as u16, hold));
        }
    }
    Ok(FullSyncCapture {
        coverage: ShardCoverage::from_watermarks(watermarks),
        hold: Some(Box::new(FullSyncHoldGuard::new(holds))),
    })
}

/// The two entry points' shared body. `hold_for` is what separates them: `None`
/// is a plain drain, `Some(window)` additionally arms each shard's flush hold
/// for that long, in the same message that reports its watermark.
async fn quiesce(
    senders: &[ShardSender],
    hold_for: Option<std::time::Duration>,
) -> Result<Vec<WalDrainAck>, QuiesceIncomplete> {
    fan_out(senders, "search-index flush", |response_tx| {
        SearchMsg::FlushSearchIndexes { response_tx }
    })
    .await?;
    fan_out(senders, "WAL drain", move |response_tx| {
        SearchMsg::FlushWal {
            hold_for,
            response_tx,
        }
    })
    .await
}

/// The live holds of one in-flight full sync, released together.
///
/// Modelled on the staging-directory guard: the release is a `Drop`, so an
/// early return or a failed cut cannot leave the node's flush engines pinned.
/// [`CaptureHold::release`] is the same operation made explicit, for the caller
/// that wants the breach report before the guard goes out of scope.
struct FullSyncHoldGuard {
    /// Emptied by the first release; a second one has nothing to lift.
    holds: Mutex<Vec<(u16, Arc<FlushHold>)>>,
}

impl FullSyncHoldGuard {
    fn new(holds: Vec<(u16, Arc<FlushHold>)>) -> Self {
        Self {
            holds: Mutex::new(holds),
        }
    }
}

impl CaptureHold for FullSyncHoldGuard {
    fn release(&self) -> Vec<u16> {
        let holds = std::mem::take(&mut *self.holds.lock().expect("hold guard mutex"));
        let mut breached = Vec::new();
        for (shard, hold) in holds {
            if hold.release() {
                // The engine was free to commit again before the cut, so this
                // shard's watermark is no longer a claim the payload honours —
                // and the sync that armed it is abandoned rather than shipped
                // with a claim nobody can trust.
                tracing::warn!(
                    shard,
                    "Full-sync flush hold expired before the checkpoint cut; \
                     abandoning the sync rather than shipping a coverage claim \
                     this shard's payload may not honour"
                );
                breached.push(shard);
            }
        }
        breached
    }
}

impl Drop for FullSyncHoldGuard {
    fn drop(&mut self) {
        self.release();
    }
}

/// Send one ack-carrying message to every shard, then await all the acks and
/// return what they carried, in shard order.
///
/// Two passes on purpose: all shards drain concurrently instead of one at a
/// time, so the quiesce costs one flush latency rather than `num_shards` of
/// them. Every shard is still attempted after one fails, so the error names the
/// full set rather than the first index the loop happened to reach.
///
/// Generic over the ack payload because the two waves report different things:
/// the search-index commit reports nothing, the WAL drain reports the shard's
/// coverage watermark and its flush hold. The returned vector is positional —
/// index `i` is shard `i` — which only holds because it is returned solely when
/// every shard answered.
async fn fan_out<F, T>(
    senders: &[ShardSender],
    stage: &'static str,
    message: F,
) -> Result<Vec<T>, QuiesceIncomplete>
where
    F: Fn(tokio::sync::oneshot::Sender<T>) -> SearchMsg,
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
    let mut acks = Vec::with_capacity(receivers.len());
    for (shard, rx) in receivers {
        match rx.await {
            Ok(ack) => acks.push(ack),
            Err(_) => failed.push(shard),
        }
    }
    if failed.is_empty() {
        return Ok(acks);
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
        healthy_shard_at(0, None)
    }

    /// A healthy shard that reports `watermark` as its coverage and, when the
    /// drain asks for a hold, arms `hold` for the window it was given — the
    /// real shard's behaviour, minus the keyspace.
    fn healthy_shard_at(watermark: u64, hold: Option<Arc<FlushHold>>) -> ShardSender {
        spawn_shard(watermark, hold, |hold, window| {
            hold.arm(frogdb_types::clock::now() + window)
        })
    }

    /// A shard whose hold lapses before the coordinator can release it: it arms
    /// with a deadline that is already in the past, so the flush engine was
    /// free to commit into the payload behind the watermark's back.
    fn lapsing_shard_at(watermark: u64, hold: Arc<FlushHold>) -> ShardSender {
        spawn_shard(watermark, Some(hold), |hold, _| {
            hold.arm(frogdb_types::clock::now())
        })
    }

    fn spawn_shard(
        watermark: u64,
        hold: Option<Arc<FlushHold>>,
        arm: impl Fn(&FlushHold, std::time::Duration) + Send + 'static,
    ) -> ShardSender {
        let (tx, rx) = mpsc::channel(16);
        let mut receiver = ShardReceiver::new(rx);
        tokio::spawn(async move {
            while let Some(env) = receiver.recv().await {
                match env.message {
                    ShardMessage::Search(SearchMsg::FlushSearchIndexes { response_tx }) => {
                        let _ = response_tx.send(());
                    }
                    ShardMessage::Search(SearchMsg::FlushWal {
                        hold_for,
                        response_tx,
                    }) => {
                        let armed = match (hold_for, hold.as_ref()) {
                            (Some(window), Some(hold)) => {
                                arm(hold, window);
                                Some(Arc::clone(hold))
                            }
                            _ => None,
                        };
                        let _ = response_tx.send(WalDrainAck {
                            last_broadcast_offset: watermark,
                            hold: armed,
                        });
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

    /// The `FULLRESYNC` entry point reports one watermark per shard, in shard
    /// order, and leaves every shard's flush engine held until the capture is
    /// released — the two things the `BGSAVE` entry point deliberately does not
    /// do.
    // FM-REPLICATION-066
    #[tokio::test]
    async fn full_sync_quiesce_reports_one_watermark_per_shard_and_holds_the_engines() {
        let holds: Vec<_> = (0..3).map(|_| FlushHold::shared()).collect();
        let senders: Vec<_> = [7u64, 0, 11]
            .iter()
            .zip(&holds)
            .map(|(watermark, hold)| healthy_shard_at(*watermark, Some(Arc::clone(hold))))
            .collect();

        let mut capture = quiesce_shards_for_full_sync(&senders)
            .await
            .expect("every shard drained");

        assert_eq!(capture.coverage.as_slice(), &[7, 0, 11]);
        assert!(
            holds.iter().all(|h| h.is_held()),
            "the engines stay held from the drain until the cut"
        );

        assert!(
            capture.release_hold().is_empty(),
            "no hold lapsed, so nothing is breached"
        );
        assert!(
            holds.iter().all(|h| !h.is_held()),
            "releasing the capture lifts every shard's hold"
        );
        assert_eq!(
            capture.coverage.as_slice(),
            &[7, 0, 11],
            "no hold lapsed, so every claim stands"
        );
    }

    /// A shard whose hold lapsed before the cut may have committed a write
    /// above its watermark into the payload, so the release **names** it and
    /// the driver fails the sync (`a_breached_hold_aborts_the_sync`). What it
    /// deliberately does not do is rewrite the vector: a `0` there would read as
    /// "no floor for this shard", which is exactly the double-apply the vector
    /// exists to prevent, shipped silently instead of loudly refused.
    // FM-REPLICATION-066
    #[tokio::test]
    async fn a_breached_hold_names_its_shard_and_downgrades_no_claim() {
        let held = FlushHold::shared();
        let lapsed = FlushHold::shared();
        let senders = vec![
            healthy_shard_at(7, Some(Arc::clone(&held))),
            lapsing_shard_at(11, Arc::clone(&lapsed)),
            healthy_shard_at(13, Some(Arc::clone(&held))),
        ];

        let mut capture = quiesce_shards_for_full_sync(&senders)
            .await
            .expect("every shard drained");
        assert_eq!(capture.coverage.as_slice(), &[7, 11, 13]);

        assert_eq!(
            capture.release_hold(),
            vec![1],
            "the breach names the shard whose hold lapsed, and only that one"
        );
        assert_eq!(
            capture.coverage.as_slice(),
            &[7, 11, 13],
            "the claim is never degraded behind the sync's back; the sync is \
             abandoned instead"
        );
    }

    /// `BGSAVE` arms nothing: the drain is the whole contract there, and a
    /// recovery artifact that picks up a few extra writes is still correct.
    // FM-REPLICATION-066
    #[tokio::test]
    async fn the_bgsave_entry_point_never_arms_a_hold() {
        let hold = FlushHold::shared();
        let senders = vec![healthy_shard_at(7, Some(Arc::clone(&hold)))];

        assert_eq!(quiesce_shards_for_checkpoint(&senders).await, Ok(()));

        assert!(
            !hold.is_held(),
            "a snapshot drain must not pin the flush engine"
        );
    }
}
