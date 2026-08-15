//! Cross-shard VLL coordinator.
//!
//! [`VllCoordinator`] owns the cross-shard VLL protocol — both the SCA
//! scatter-gather flow used by multi-key commands (MGET, MSET, DEL, ...)
//! and the continuation-lock acquisition used by Lua / MULTI.
//!
//! Both flows share a common skeleton: send a VLL message to each
//! participating shard, wait for all shards to report ready, and then either
//! proceed or abort. The coordinator encapsulates that skeleton so callers
//! express their command at a higher level.
//!
//! # What prevents deadlock
//!
//! Not the dispatch order. Callers do sort their shard lists, and that keeps
//! dispatch and diagnostics predictable, but every message is sent before any
//! reply is awaited — acquisition is pipelined across transactions, not
//! serialized — so sorted order cannot by itself stop two transactions from
//! each winning a different shard. What stops it is the shards' priority rule
//! on the global `txid` order, applied on both locks: an older transaction
//! *wounds* a younger claim rather than being refused by (continuation lock)
//! or parked behind (SCA lock table) it, so of any two colliding transactions
//! the older one is guaranteed to finish. Refusing both (the pre-wound-wait
//! behavior on the continuation lock) is deadlock-free but not livelock-free;
//! parking behind a younger holder (the pre-wound-wait behavior on the SCA
//! path) is not even deadlock-free.
//!
//! See [`VllCoordinator::scatter`] and
//! [`VllCoordinator::acquire_continuation_and_run`].
//!
//! # Drop semantics
//!
//! [`ContinuationGuard`] sends release signals on drop, so the coordinator
//! cannot leak continuation locks even if the caller-supplied primary work
//! panics between acquisition and cleanup. The guard is owned by
//! [`VllCoordinator::acquire_continuation_and_run`] for the entire duration
//! of that work, so callers never hand-manage its lifetime.

use std::future::Future;
use std::pin::Pin;
use std::task::Poll;
use std::time::Duration;

use bytes::Bytes;
use tokio::sync::oneshot;
// The timer's clock, not the OS clock — see the note in `queue.rs`.
use tokio::time::Instant;

use crate::traits::{LockRequest, MetricsSink, ShardSink, ShardSinkError};
use crate::{LockMode, ShardReadyResult, VllError};

/// Default timeout used when the caller does not supply one explicitly.
pub const DEFAULT_LOCK_ACQUISITION_TIMEOUT: Duration = Duration::from_millis(4000);

/// One participant in a scatter operation: a shard plus its slice of the
/// keys (and optionally a per-shard payload override).
#[derive(Debug, Clone)]
pub struct ScatterParticipant<O> {
    pub shard_id: usize,
    pub keys: Vec<Bytes>,
    pub operation: O,
}

/// Inputs to [`VllCoordinator::scatter`].
#[derive(Debug)]
pub struct ScatterRequest<O> {
    /// Globally-unique transaction id.
    pub txid: u64,
    /// Lock mode requested on every participating shard.
    pub mode: LockMode,
    /// One entry per participating shard. The coordinator dispatches in
    /// the order given — callers should sort by shard id to match the
    /// rest of the system's deadlock-prevention convention.
    pub participants: Vec<ScatterParticipant<O>>,
    /// Total bound on the whole request, not an allowance per wait: the
    /// coordinator turns it into one absolute deadline at entry and every
    /// receiver wait in phases 2 and 4 runs against that deadline, so the
    /// observed bound does not grow with participant count.
    pub timeout: Duration,
    /// Command name used for metrics labels (e.g. `"MGET"`).
    pub command: &'static str,
}

/// Outcome of a successful [`VllCoordinator::scatter`] — one entry per
/// participating shard, in dispatch order.
#[derive(Debug)]
pub struct ScatterOutcome<R> {
    pub responses: Vec<(usize, R)>,
}

/// Errors returned by [`VllCoordinator::scatter`].
#[derive(Debug)]
pub enum ScatterError {
    /// Failed to dispatch a message — channel closed.
    ShardUnavailable(ShardSinkError),
    /// At least one shard reported lock-acquisition failure.
    LockFailed { shard_id: usize, error: VllError },
    /// At least one shard's ready channel closed prematurely.
    LockChannelClosed { shard_id: usize },
    /// At least one shard's lock acquisition timed out.
    LockTimeout { shard_id: usize },
    /// At least one shard's result channel closed prematurely.
    ResultChannelClosed { shard_id: usize },
    /// A shard's result wait timed out **after** its execute was dispatched:
    /// the op may already have applied there, and the participants that had
    /// not yet been gathered are in the same position. The outcome is
    /// AMBIGUOUS, not aborted — nothing here decides it, and a wall clock is
    /// not allowed to.
    ///
    /// `applied` names the participants whose results were gathered before
    /// the timeout (definitely applied); `unknown` names `shard_id` plus every
    /// participant not yet gathered. Both lists exist so the caller can say
    /// *what* is unknown instead of collapsing "possibly applied" and
    /// "definitely not" into one string — the distinction
    /// [FM-TXN-032](../../../../specs/txn.md) keeps.
    ResultAmbiguous {
        shard_id: usize,
        applied: Vec<usize>,
        unknown: Vec<usize>,
    },
    /// An older transaction needs a lock this one was granted on `shard_id`,
    /// and this transaction is the younger of the two. Every participant has
    /// been aborted; the caller retries — **keeping its txid**, which is the
    /// seniority the next round is decided on.
    Wounded { shard_id: usize },
}

impl ScatterError {
    /// Whether retrying this scatter can make progress. Only a wound can, and
    /// only under the original txid — a retry that mints a fresh (higher) txid
    /// hands its seniority away and can be wounded again forever.
    pub fn is_wound(&self) -> bool {
        matches!(self, ScatterError::Wounded { .. })
    }
}

impl std::fmt::Display for ScatterError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ScatterError::ShardUnavailable(e) => write!(f, "{e}"),
            ScatterError::LockFailed { shard_id, error } => {
                write!(f, "VLL lock failed on shard {shard_id}: {error}")
            }
            ScatterError::LockChannelClosed { shard_id } => {
                write!(f, "VLL ready channel dropped by shard {shard_id}")
            }
            ScatterError::LockTimeout { shard_id } => {
                write!(f, "VLL lock acquisition timeout on shard {shard_id}")
            }
            ScatterError::ResultChannelClosed { shard_id } => {
                write!(f, "shard {shard_id} dropped VLL result")
            }
            ScatterError::ResultAmbiguous {
                shard_id, unknown, ..
            } => {
                write!(
                    f,
                    "VLL result timeout on shard {shard_id}; effects unknown on shards {unknown:?}"
                )
            }
            ScatterError::Wounded { shard_id } => {
                write!(f, "wounded by an older transaction on shard {shard_id}")
            }
        }
    }
}

impl std::error::Error for ScatterError {}

/// RAII guard for a set of held continuation locks.
///
/// While the guard is alive, every participating shard holds a continuation
/// lock that excludes other connections. Dropping the guard releases all
/// locks. The drop is non-blocking — release signals are delivered through
/// `oneshot::Sender::send`.
#[must_use = "dropping the guard releases the continuation lock; bind it to keep the lock held"]
#[derive(Debug)]
pub struct ContinuationGuard {
    release_txs: Vec<oneshot::Sender<()>>,
    /// The shards' back-channel: any participant can tell this coordinator to
    /// let go — because an older transaction wounded it, because the script
    /// was killed, or because the hold cap expired.
    revocations: RevocationWatch,
}

/// The set of per-shard revocation receivers one transaction listens on —
/// a continuation holder's revocations, or a scatter's wound notices.
///
/// A revocation can arrive from *any* participating shard at *any* point
/// between acquisition and release, so it cannot be awaited shard-by-shard the
/// way ready signals are. This polls every receiver on each wake and resolves
/// with the first shard that speaks. A receiver that resolves — with a
/// revocation or with a close — is taken out, so no receiver is ever polled
/// after completion.
///
/// With every receiver retired the watch is `Pending` forever, which is the
/// right answer: it is only ever used as a `select!` arm against the work it
/// guards.
#[derive(Debug, Default)]
struct RevocationWatch {
    rxs: Vec<(usize, Option<oneshot::Receiver<VllError>>)>,
}

impl RevocationWatch {
    fn push(&mut self, shard_id: usize, rx: oneshot::Receiver<VllError>) {
        self.rxs.push((shard_id, Some(rx)));
    }

    /// Resolve with the first shard to revoke this transaction's lock.
    async fn next(&mut self) -> (usize, VllError) {
        std::future::poll_fn(|cx| {
            for (shard_id, slot) in self.rxs.iter_mut() {
                let Some(rx) = slot.as_mut() else { continue };
                match Pin::new(rx).poll(cx) {
                    Poll::Ready(Ok(reason)) => {
                        *slot = None;
                        return Poll::Ready((*shard_id, reason));
                    }
                    // The shard dropped its sender — it can no longer revoke,
                    // so retire the slot rather than re-polling a completed
                    // receiver.
                    Poll::Ready(Err(_)) => *slot = None,
                    Poll::Pending => {}
                }
            }
            Poll::Pending
        })
        .await
    }
}

impl ContinuationGuard {
    /// Explicitly release all continuation locks. Equivalent to dropping.
    pub fn release(self) {
        // Drop runs and sends on each release_tx.
    }

    /// Number of shards still holding a lock through this guard.
    pub fn shard_count(&self) -> usize {
        self.release_txs.len()
    }
}

impl Drop for ContinuationGuard {
    fn drop(&mut self) {
        for tx in self.release_txs.drain(..) {
            let _ = tx.send(());
        }
    }
}

/// Errors returned by [`VllCoordinator::acquire_continuation_and_run`].
#[derive(Debug)]
pub enum ContinuationError {
    ShardUnavailable(ShardSinkError),
    LockFailed {
        shard_id: usize,
        error: VllError,
    },
    LockChannelClosed {
        shard_id: usize,
    },
    LockTimeout {
        shard_id: usize,
    },
    /// A participating shard took the lock away. `reason` separates the two
    /// cases the caller must treat differently: [`VllError::Wounded`] is an
    /// older transaction claiming priority and the caller should retry
    /// **keeping its txid**; [`VllError::Revoked`] is a kill or an expired
    /// hold cap and must not be retried.
    Revoked {
        shard_id: usize,
        reason: VllError,
    },
}

impl ContinuationError {
    /// Whether retrying this transaction can make progress.
    ///
    /// Only a wound is retryable, and only under the original txid — see
    /// [`VllError::Wounded`].
    pub fn is_wound(&self) -> bool {
        matches!(
            self,
            ContinuationError::Revoked {
                reason: VllError::Wounded,
                ..
            }
        )
    }
}

impl std::fmt::Display for ContinuationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ContinuationError::ShardUnavailable(e) => write!(f, "{e}"),
            ContinuationError::LockFailed { shard_id, error } => {
                write!(f, "continuation lock failed on shard {shard_id}: {error}")
            }
            ContinuationError::LockChannelClosed { shard_id } => {
                write!(f, "continuation ready channel dropped by shard {shard_id}")
            }
            ContinuationError::LockTimeout { shard_id } => {
                write!(
                    f,
                    "continuation lock acquisition timeout on shard {shard_id}"
                )
            }
            ContinuationError::Revoked { shard_id, reason } => {
                write!(f, "continuation lock revoked by shard {shard_id}: {reason}")
            }
        }
    }
}

impl std::error::Error for ContinuationError {}

/// Cross-shard VLL coordinator.
pub struct VllCoordinator<S, M> {
    sink: S,
    metrics: M,
}

impl<S, M> VllCoordinator<S, M>
where
    S: ShardSink,
    M: MetricsSink,
{
    /// Construct a new coordinator from a sink + metrics adapter.
    pub fn new(sink: S, metrics: M) -> Self {
        Self { sink, metrics }
    }

    /// Borrow the underlying shard sink (useful for tests).
    pub fn sink(&self) -> &S {
        &self.sink
    }

    /// Run a scatter operation through the 4-phase VLL choreography.
    ///
    /// Phases:
    /// 1. Send `VllLockRequest` to each participant in dispatch order.
    /// 2. Wait for every shard to signal `ShardReadyResult::Ready`.
    /// 3. Send `VllExecute` to each shard.
    /// 4. Gather `Response`s and return them in dispatch order.
    ///
    /// On any failure the coordinator aborts every shard still holding
    /// locks. `command` is used solely for metric labels.
    ///
    /// Phase 2 is also the wound window: while this transaction waits on one
    /// shard it is holding grants on the others, and an older transaction that
    /// wants one of them says so on the wound channel. That gives
    /// [`ScatterError::Wounded`], which the caller must retry **under the same
    /// txid** — a retry that mints a fresh, higher txid gives up its seniority
    /// and can be wounded again by every later arrival. The window closes
    /// before phase 3: past the first `VllExecute` there is nothing left to
    /// cancel, so a late wound finds nobody listening.
    pub async fn scatter(
        &self,
        request: ScatterRequest<S::Operation>,
    ) -> Result<ScatterOutcome<S::Response>, ScatterError> {
        let start = Instant::now();
        // One deadline for the whole request, fixed here and never recomputed.
        // Phases 2 and 4 both wait on their receivers one at a time; a
        // *relative* timeout per receiver would restart at each one and let a
        // request with N slow-but-answering participants run for N × timeout.
        // Phase 4 spends what phase 2 left rather than starting over: past
        // phase 3 the outcome is ambiguous whenever the wait ends, and what
        // the caller asked to be bounded is the request.
        let deadline = start + request.timeout;
        let shard_count = request.participants.len();

        // Phase 1: Dispatch lock requests, tracking ready receivers.
        let mut ready_rxs: Vec<(usize, oneshot::Receiver<ShardReadyResult>)> =
            Vec::with_capacity(shard_count);
        // Wound notices from every participant. A shard fires one when an
        // *older* transaction needs a lock this one has been granted; it can
        // arrive from any participant while this coordinator is still waiting
        // on the others, so it is watched as a set rather than per shard.
        let mut wounds = RevocationWatch::default();

        for participant in request.participants {
            let (ready_tx, ready_rx) = oneshot::channel();
            let (wound_tx, wound_rx) = oneshot::channel();

            if let Err(err) = self
                .sink
                .send_lock_request(
                    participant.shard_id,
                    LockRequest {
                        txid: request.txid,
                        keys: participant.keys,
                        mode: request.mode,
                        operation: participant.operation,
                        ready_tx,
                        wound_tx,
                    },
                )
                .await
            {
                self.abort_pending(&ready_rxs, request.txid).await;
                self.record_outcome(request.command, "error", start, shard_count);
                return Err(ScatterError::ShardUnavailable(err));
            }

            ready_rxs.push((participant.shard_id, ready_rx));
            wounds.push(participant.shard_id, wound_rx);
        }

        // Every participant received a lock request; from here on failures
        // abort by real shard id.
        let shard_ids: Vec<usize> = ready_rxs.iter().map(|(id, _)| *id).collect();

        // Phase 2: Wait for every shard to report ready — or for an older
        // transaction to wound this one.
        //
        // The wound race belongs to this phase alone. Between being told
        // `Ready` and being sent `VllExecute`, this transaction holds granted
        // locks on shards it is no longer doing anything for: that is the only
        // window where it can be part of a wait cycle, and the only window
        // where giving way costs nothing but a retry.
        for (shard_id, ready_rx) in ready_rxs {
            let ready = tokio::select! {
                biased;
                (wounded_by, _reason) = wounds.next() => {
                    self.abort_shards(&shard_ids, request.txid).await;
                    self.record_outcome(request.command, "wounded", start, shard_count);
                    return Err(ScatterError::Wounded { shard_id: wounded_by });
                }
                ready = tokio::time::timeout_at(deadline, ready_rx) => ready,
            };
            match ready {
                Ok(Ok(ShardReadyResult::Ready)) => {}
                Ok(Ok(ShardReadyResult::Failed(error))) => {
                    self.abort_shards(&shard_ids, request.txid).await;
                    self.record_outcome(request.command, "error", start, shard_count);
                    return Err(ScatterError::LockFailed { shard_id, error });
                }
                Ok(Err(_)) => {
                    self.abort_shards(&shard_ids, request.txid).await;
                    self.record_outcome(request.command, "error", start, shard_count);
                    return Err(ScatterError::LockChannelClosed { shard_id });
                }
                Err(_) => {
                    self.abort_shards(&shard_ids, request.txid).await;
                    self.record_outcome(request.command, "timeout", start, shard_count);
                    return Err(ScatterError::LockTimeout { shard_id });
                }
            }
        }

        // Past the wound window: from here the round is committed to running,
        // and dropping the receivers is what says so. A shard that tries to
        // wound this transaction now finds nobody listening and waits for it
        // to finish instead — which it will, since it is no longer waiting on
        // anyone.
        drop(wounds);

        // Phase 3: Dispatch VllExecute requests.
        //
        // On a partial failure, every participant that has not received
        // `VllExecute` still holds locks and must be aborted by its *real*
        // shard id — including the participant whose dispatch just failed.
        // Participants that already received `VllExecute` release their own
        // locks when execution completes, so they must not be aborted.
        let mut result_rxs: Vec<(usize, oneshot::Receiver<S::Response>)> =
            Vec::with_capacity(shard_count);

        for (idx, &shard_id) in shard_ids.iter().enumerate() {
            let (response_tx, response_rx) = oneshot::channel();
            if let Err(err) = self
                .sink
                .send_execute(shard_id, request.txid, response_tx)
                .await
            {
                self.abort_shards(&shard_ids[idx..], request.txid).await;
                self.record_outcome(request.command, "error", start, shard_count);
                return Err(ScatterError::ShardUnavailable(err));
            }
            result_rxs.push((shard_id, response_rx));
        }

        // Phase 4: Gather results.
        //
        // Every participant has already received `VllExecute` — phase 3 returns
        // on the first dispatch failure, so reaching here means `result_rxs` is
        // complete. A timeout in this phase therefore cannot mean "never ran";
        // it means "ran, outcome unknown". It resolves nothing.
        //
        // The wait runs against the request's one deadline, so the gather gets
        // whatever acquisition left of the budget rather than a fresh one.
        let mut responses: Vec<(usize, S::Response)> = Vec::with_capacity(shard_count);
        let mut pending: Vec<usize> = result_rxs.iter().map(|(id, _)| *id).collect();
        for (shard_id, rx) in result_rxs {
            pending.retain(|&id| id != shard_id);
            match tokio::time::timeout_at(deadline, rx).await {
                Ok(Ok(response)) => responses.push((shard_id, response)),
                Ok(Err(_)) => {
                    self.record_outcome(request.command, "error", start, shard_count);
                    return Err(ScatterError::ResultChannelClosed { shard_id });
                }
                Err(_) => {
                    self.record_outcome(request.command, "timeout", start, shard_count);
                    let mut unknown = vec![shard_id];
                    unknown.extend(pending);
                    return Err(ScatterError::ResultAmbiguous {
                        shard_id,
                        applied: responses.iter().map(|(id, _)| *id).collect(),
                        unknown,
                    });
                }
            }
        }

        self.record_outcome(request.command, "success", start, shard_count);
        Ok(ScatterOutcome { responses })
    }

    /// Acquire a continuation lock on every shard in `shards`, run the
    /// caller-supplied primary work while every lock is held, then release
    /// all locks before returning.
    ///
    /// This is the continuation analogue of [`Self::scatter`]: the caller
    /// hands the coordinator the whole "lock N shards, run, release"
    /// choreography instead of juggling a [`ContinuationGuard`] by hand.
    /// `run` is the primary-shard work — e.g. dispatching a Lua script to
    /// the primary shard and awaiting its response — and its value is
    /// returned verbatim on success.
    ///
    /// The continuation guard is owned by this method for the entire
    /// duration of `run`; it is dropped (releasing every lock) as soon as
    /// `run` completes, whether it returns normally or panics. On
    /// acquisition failure `run` is never invoked and all partially
    /// acquired locks are released before the error is returned.
    ///
    /// `run` is raced against the shards' revocation back-channel. A shard
    /// revokes when an older transaction wounds this one, when the script is
    /// killed, or when the hold cap expires; the `run` future is dropped and
    /// [`ContinuationError::Revoked`] is returned so no continuation lock can
    /// be held for longer than the shard permits.
    ///
    /// Dispatch order does *not* buy deadlock-freedom here. Every
    /// `send_continuation_lock` is issued before any ready signal is awaited,
    /// so acquisition is pipelined, not serialized: two transactions over
    /// overlapping shard sets can still each win a different shard. What makes
    /// that safe is the shards' wound-wait rule (the older `txid` always
    /// wins, on every shard, so one of the two is guaranteed to complete) —
    /// see [`crate::VllShardState::request_continuation_lock`]. `shards` is
    /// still expected sorted, for predictable dispatch and diagnostics.
    pub async fn acquire_continuation_and_run<F, Fut, T>(
        &self,
        txid: u64,
        conn_id: u64,
        shards: &[usize],
        timeout: Duration,
        run: F,
    ) -> Result<T, ContinuationError>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = T>,
    {
        let mut guard = self
            .acquire_continuation(txid, conn_id, shards, timeout)
            .await?;

        let outcome = {
            let revocations = &mut guard.revocations;
            tokio::select! {
                biased;
                (shard_id, reason) = revocations.next() => {
                    Err(ContinuationError::Revoked { shard_id, reason })
                }
                value = run() => Ok(value),
            }
        };

        // Release every continuation lock now that the primary work is done
        // (or has been abandoned), before handing the result back.
        drop(guard);
        outcome
    }

    /// Acquire a continuation lock on every participating shard.
    ///
    /// On success returns a [`ContinuationGuard`] which releases all locks
    /// when dropped. On any failure all already-acquired locks are
    /// released before the error is returned.
    ///
    /// Crate-private building block for [`Self::acquire_continuation_and_run`],
    /// which owns the guard's lifetime so callers never manage it directly.
    ///
    /// `timeout` bounds the acquisition as a whole, not each shard's share of
    /// it: the ready signals are awaited one at a time, so a per-receiver
    /// timeout would let `shards.len()` slow-but-answering shards multiply it.
    async fn acquire_continuation(
        &self,
        txid: u64,
        conn_id: u64,
        shards: &[usize],
        timeout: Duration,
    ) -> Result<ContinuationGuard, ContinuationError> {
        let deadline = Instant::now() + timeout;
        let mut release_txs: Vec<oneshot::Sender<()>> = Vec::with_capacity(shards.len());
        let mut revocations = RevocationWatch::default();
        let mut ready_rxs: Vec<(usize, oneshot::Receiver<ShardReadyResult>)> =
            Vec::with_capacity(shards.len());

        for &shard_id in shards {
            let (ready_tx, ready_rx) = oneshot::channel();
            let (release_tx, release_rx) = oneshot::channel();
            let (revoke_tx, revoke_rx) = oneshot::channel();

            if let Err(err) = self
                .sink
                .send_continuation_lock(shard_id, txid, conn_id, ready_tx, release_rx, revoke_tx)
                .await
            {
                // Dropping `release_txs` here would NOT release any locks
                // because we never got past send_continuation_lock for the
                // failing shard. The earlier shards that did receive the
                // request will have their release_rx receivers signaled
                // when their tx is dropped here.
                drop(release_txs);
                return Err(ContinuationError::ShardUnavailable(err));
            }

            release_txs.push(release_tx);
            revocations.push(shard_id, revoke_rx);
            ready_rxs.push((shard_id, ready_rx));
        }

        for (shard_id, ready_rx) in ready_rxs {
            // A wound can land while this coordinator still holds locks on
            // earlier shards and waits on a later one — exactly the state a
            // livelocked pair sits in. Racing the two is what lets the younger
            // transaction give way immediately instead of at the deadline.
            let waited = tokio::select! {
                biased;
                (wounded_by, reason) = revocations.next() => {
                    drop(release_txs);
                    return Err(ContinuationError::Revoked { shard_id: wounded_by, reason });
                }
                waited = tokio::time::timeout_at(deadline, ready_rx) => waited,
            };

            match waited {
                Ok(Ok(ShardReadyResult::Ready)) => {}
                Ok(Ok(ShardReadyResult::Failed(VllError::Wounded))) => {
                    drop(release_txs);
                    return Err(ContinuationError::Revoked {
                        shard_id,
                        reason: VllError::Wounded,
                    });
                }
                Ok(Ok(ShardReadyResult::Failed(error))) => {
                    drop(release_txs);
                    return Err(ContinuationError::LockFailed { shard_id, error });
                }
                Ok(Err(_)) => {
                    drop(release_txs);
                    return Err(ContinuationError::LockChannelClosed { shard_id });
                }
                Err(_) => {
                    drop(release_txs);
                    return Err(ContinuationError::LockTimeout { shard_id });
                }
            }
        }

        Ok(ContinuationGuard {
            release_txs,
            revocations,
        })
    }

    async fn abort_pending(
        &self,
        ready_rxs: &[(usize, oneshot::Receiver<ShardReadyResult>)],
        txid: u64,
    ) {
        let shard_ids: Vec<usize> = ready_rxs.iter().map(|(id, _)| *id).collect();
        self.abort_shards(&shard_ids, txid).await;
    }

    /// Best-effort abort of the given shard ids. `send_abort` is fire-and-
    /// forget; an unreachable shard is already unable to hold its locks past
    /// its own lifetime.
    async fn abort_shards(&self, shard_ids: &[usize], txid: u64) {
        for &shard_id in shard_ids {
            self.sink.send_abort(shard_id, txid).await;
        }
    }

    fn record_outcome(
        &self,
        command: &'static str,
        status: &'static str,
        start: Instant,
        shards: usize,
    ) {
        let elapsed = start.elapsed().as_secs_f64();
        self.metrics.increment_counter(
            "frogdb_scatter_gather_total",
            1,
            &[("command", command), ("status", status)],
        );
        if status == "success" {
            self.metrics.record_histogram(
                "frogdb_scatter_gather_duration_seconds",
                elapsed,
                &[("command", command)],
            );
            self.metrics.record_histogram(
                "frogdb_scatter_gather_shards",
                shards as f64,
                &[("command", command)],
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::traits::NoopMetricsSink;
    use std::sync::Arc;
    use std::time::Duration;
    use tokio::sync::Mutex;

    type ReadyCallback = Arc<Mutex<Box<dyn FnMut(usize, u64) -> ShardReadyResult + Send>>>;
    type ExecuteCallback =
        Arc<Mutex<Box<dyn FnMut(usize, u64) -> Result<u32, ShardSinkError> + Send>>>;
    type DispatchCallback =
        Arc<Mutex<Box<dyn FnMut(usize, u64) -> Result<(), ShardSinkError> + Send>>>;
    /// Revocation senders paired with the shard that handed each one over.
    type RevokeSenders = Arc<Mutex<Vec<(usize, oneshot::Sender<VllError>)>>>;

    /// Test sink that records every call and lets each test script the
    /// shard responses (ready / failed / dropped) and execute outcomes.
    struct TestSink {
        // Per-shard dispatch outcome for lock-request: whether the message
        // reaches the shard at all (a closed shard channel returns Err).
        on_lock_send: DispatchCallback,
        // Per-shard callbacks for lock-request: send Ready or Failed.
        on_lock: ReadyCallback,
        // Per-shard execute callback: produce the Response or a send error.
        on_execute: ExecuteCallback,
        // Shard ids that received a send_abort, in order.
        aborted_shards: Arc<Mutex<Vec<usize>>>,
        cont_outcomes: ReadyCallback,
        // Shard ids whose execute response channel is parked instead of
        // answered: the only way to make phase 4 time out rather than fail
        // fast, since dropping the sender closes the channel.
        stash_execute: Arc<Mutex<Vec<usize>>>,
        stashed_results: Arc<Mutex<Vec<oneshot::Sender<u32>>>>,
        // Revocation senders the coordinator handed over with each
        // continuation-lock request. A test fires one to model a shard taking
        // the lock back (wound / SCRIPT KILL / hold cap).
        revoke_txs: RevokeSenders,
        // Release receivers handed to the sink by continuation-lock
        // acquisition. The coordinator holds the paired sender inside the
        // guard and fires it on release, so tests can observe whether a
        // lock is still held (`Empty`) or has been released (`Ok`).
        release_rxs: Arc<Mutex<Vec<oneshot::Receiver<()>>>>,
    }

    impl TestSink {
        fn ok_sink() -> (Self, Arc<Mutex<Vec<usize>>>) {
            let aborts = Arc::new(Mutex::new(Vec::new()));
            (
                TestSink {
                    on_lock_send: Arc::new(Mutex::new(Box::new(|_, _| Ok(())))),
                    on_lock: Arc::new(Mutex::new(Box::new(|_, _| ShardReadyResult::Ready))),
                    on_execute: Arc::new(Mutex::new(Box::new(|s, _| Ok((s as u32) + 100)))),
                    aborted_shards: aborts.clone(),
                    cont_outcomes: Arc::new(Mutex::new(Box::new(|_, _| ShardReadyResult::Ready))),
                    stash_execute: Arc::new(Mutex::new(Vec::new())),
                    stashed_results: Arc::new(Mutex::new(Vec::new())),
                    release_rxs: Arc::new(Mutex::new(Vec::new())),
                    revoke_txs: Arc::new(Mutex::new(Vec::new())),
                },
                aborts,
            )
        }
    }

    impl ShardSink for TestSink {
        type Operation = u64;
        type Response = u32;

        async fn send_lock_request(
            &self,
            shard_id: usize,
            request: LockRequest<Self::Operation>,
        ) -> Result<(), ShardSinkError> {
            {
                let mut send_cb = self.on_lock_send.lock().await;
                send_cb(shard_id, request.txid)?;
            }
            let mut cb = self.on_lock.lock().await;
            let result = cb(shard_id, request.txid);
            let _ = request.ready_tx.send(result);
            Ok(())
        }

        async fn send_execute(
            &self,
            shard_id: usize,
            txid: u64,
            response_tx: oneshot::Sender<Self::Response>,
        ) -> Result<(), ShardSinkError> {
            if self.stash_execute.lock().await.contains(&shard_id) {
                // Dispatch succeeded — the op is running on the shard — but the
                // answer never comes back. This is the only shape that reaches
                // phase 4's timeout arm.
                self.stashed_results.lock().await.push(response_tx);
                return Ok(());
            }
            let mut cb = self.on_execute.lock().await;
            let result = cb(shard_id, txid)?;
            let _ = response_tx.send(result);
            Ok(())
        }

        async fn send_abort(&self, shard_id: usize, _txid: u64) {
            self.aborted_shards.lock().await.push(shard_id);
        }

        async fn send_continuation_lock(
            &self,
            shard_id: usize,
            txid: u64,
            _conn_id: u64,
            ready_tx: oneshot::Sender<ShardReadyResult>,
            release_rx: oneshot::Receiver<()>,
            revoke_tx: oneshot::Sender<VllError>,
        ) -> Result<(), ShardSinkError> {
            let mut cb = self.cont_outcomes.lock().await;
            let result = cb(shard_id, txid);
            let _ = ready_tx.send(result);
            drop(cb);
            self.release_rxs.lock().await.push(release_rx);
            self.revoke_txs.lock().await.push((shard_id, revoke_tx));
            Ok(())
        }
    }

    // FM-VLL-010
    //
    // `is_wound` is what the executor's retry loop branches on, and a retry is
    // only safe for the one error a wound produces: every other failure has
    // either not run or already run, and re-running it under the same txid
    // would be a second attempt at work nobody asked for. Pinned per variant
    // so a widened predicate cannot pass unnoticed.
    #[test]
    fn only_a_wound_is_a_retryable_scatter_error() {
        assert!(ScatterError::Wounded { shard_id: 1 }.is_wound());

        for err in [
            ScatterError::ShardUnavailable(ShardSinkError {
                shard_id: 1,
                reason: "closed",
            }),
            ScatterError::LockFailed {
                shard_id: 1,
                error: VllError::LockTimeout,
            },
            ScatterError::LockChannelClosed { shard_id: 1 },
            ScatterError::LockTimeout { shard_id: 1 },
            ScatterError::ResultChannelClosed { shard_id: 1 },
            ScatterError::ResultAmbiguous {
                shard_id: 1,
                applied: vec![0],
                unknown: vec![1],
            },
        ] {
            assert!(!err.is_wound(), "{err:?} is not retryable");
        }
    }

    // FM-VLL-010
    //
    // Same contract on the continuation path: a revocation is retryable only
    // when its reason is a wound. A lock the shard took back because the
    // script was killed or outran its hold cap is not an invitation to retry.
    #[test]
    fn only_a_wound_is_a_retryable_continuation_error() {
        assert!(
            ContinuationError::Revoked {
                shard_id: 1,
                reason: VllError::Wounded,
            }
            .is_wound()
        );

        for err in [
            ContinuationError::Revoked {
                shard_id: 1,
                reason: VllError::LockTimeout,
            },
            ContinuationError::LockTimeout { shard_id: 1 },
            ContinuationError::LockChannelClosed { shard_id: 1 },
            ContinuationError::LockFailed {
                shard_id: 1,
                error: VllError::LockTimeout,
            },
            ContinuationError::ShardUnavailable(ShardSinkError {
                shard_id: 1,
                reason: "closed",
            }),
        ] {
            assert!(!err.is_wound(), "{err} is not retryable");
        }
    }

    fn participant(shard_id: usize) -> ScatterParticipant<u64> {
        ScatterParticipant {
            shard_id,
            keys: vec![Bytes::from(format!("key{shard_id}"))],
            operation: shard_id as u64,
        }
    }

    #[tokio::test]
    async fn scatter_returns_responses_in_dispatch_order() {
        let (sink, aborts) = TestSink::ok_sink();
        let coord = VllCoordinator::new(sink, NoopMetricsSink);
        let outcome = coord
            .scatter(ScatterRequest {
                txid: 1,
                mode: LockMode::Write,
                participants: vec![participant(0), participant(1), participant(2)],
                timeout: Duration::from_secs(1),
                command: "TEST",
            })
            .await
            .expect("scatter ok");
        assert_eq!(outcome.responses, vec![(0, 100), (1, 101), (2, 102)]);
        assert!(aborts.lock().await.is_empty());
    }

    #[tokio::test]
    async fn scatter_aborts_when_shard_lock_fails() {
        let (sink, aborts) = TestSink::ok_sink();
        // Shard 1 fails lock acquisition.
        *sink.on_lock.lock().await = Box::new(|s, _| {
            if s == 1 {
                ShardReadyResult::Failed(VllError::QueueFull)
            } else {
                ShardReadyResult::Ready
            }
        });

        let coord = VllCoordinator::new(sink, NoopMetricsSink);
        let err = coord
            .scatter(ScatterRequest {
                txid: 7,
                mode: LockMode::Write,
                participants: vec![participant(0), participant(1), participant(2)],
                timeout: Duration::from_secs(1),
                command: "TEST",
            })
            .await
            .expect_err("expected lock failure");

        assert!(matches!(err, ScatterError::LockFailed { shard_id: 1, .. }));
        // 3 shards received lock requests; on failure all 3 are aborted.
        let mut aborted = aborts.lock().await.clone();
        aborted.sort_unstable();
        assert_eq!(aborted, vec![0, 1, 2]);
    }

    // FM-VLL-008
    #[tokio::test]
    async fn phase2_failure_aborts_real_shard_ids_for_sparse_participants() {
        let (sink, aborts) = TestSink::ok_sink();
        // Participants are a sparse shard subset — ids must not be
        // reconstructed from vector positions.
        *sink.on_lock.lock().await = Box::new(|s, _| {
            if s == 5 {
                ShardReadyResult::Failed(VllError::QueueFull)
            } else {
                ShardReadyResult::Ready
            }
        });

        let coord = VllCoordinator::new(sink, NoopMetricsSink);
        let err = coord
            .scatter(ScatterRequest {
                txid: 9,
                mode: LockMode::Write,
                participants: vec![participant(2), participant(5), participant(7)],
                timeout: Duration::from_secs(1),
                command: "TEST",
            })
            .await
            .expect_err("expected lock failure");

        assert!(matches!(err, ScatterError::LockFailed { shard_id: 5, .. }));
        let mut aborted = aborts.lock().await.clone();
        aborted.sort_unstable();
        assert_eq!(aborted, vec![2, 5, 7]);
    }

    /// A phase-1 dispatch failure must abort the shards that already received
    /// a lock request — they have declared intents and are holding (or
    /// queueing for) locks that nothing else will ever release. The shard
    /// whose dispatch failed never got the message, and the shards after it
    /// were never reached, so neither is aborted.
    // FM-VLL-008
    #[tokio::test]
    async fn phase1_dispatch_failure_aborts_shards_already_holding_intents() {
        let (sink, aborts) = TestSink::ok_sink();
        *sink.on_lock_send.lock().await = Box::new(|s, _| {
            if s == 5 {
                Err(ShardSinkError {
                    shard_id: 5,
                    reason: "shard channel closed",
                })
            } else {
                Ok(())
            }
        });

        let coord = VllCoordinator::new(sink, NoopMetricsSink);
        let err = coord
            .scatter(ScatterRequest {
                txid: 13,
                mode: LockMode::Write,
                participants: vec![participant(2), participant(5), participant(7)],
                timeout: Duration::from_secs(1),
                command: "TEST",
            })
            .await
            .expect_err("expected dispatch failure");

        assert!(matches!(
            err,
            ScatterError::ShardUnavailable(ShardSinkError { shard_id: 5, .. })
        ));
        assert_eq!(
            *aborts.lock().await,
            vec![2],
            "only shard 2 received a lock request and must be unwound"
        );
    }

    /// Regression test: a phase-3 dispatch failure must abort every shard
    /// that has not received (or could not receive) `VllExecute`, addressed
    /// by its *real* shard id — not an id reconstructed from its position in
    /// the participant vector.
    ///
    /// Participants are shards [2, 5, 7]. `send_execute` succeeds for shard
    /// 2 and fails for shard 5. Shard 2 has already received execute and
    /// releases its own locks; shards 5 and 7 still hold locks and must be
    /// aborted. The buggy position-based loop instead aborted "shard 2"
    /// (a foreign abort for a shard that is executing) and left the locks
    /// on shards 5 and 7 held forever — no GC reclaims them.
    // FM-VLL-008
    #[tokio::test]
    async fn phase3_failure_aborts_remaining_holders_not_positions() {
        let (sink, aborts) = TestSink::ok_sink();
        *sink.on_execute.lock().await = Box::new(|s, _| {
            if s == 5 {
                Err(ShardSinkError {
                    shard_id: 5,
                    reason: "shard channel closed",
                })
            } else {
                Ok((s as u32) + 100)
            }
        });

        let coord = VllCoordinator::new(sink, NoopMetricsSink);
        let err = coord
            .scatter(ScatterRequest {
                txid: 11,
                mode: LockMode::Write,
                participants: vec![participant(2), participant(5), participant(7)],
                timeout: Duration::from_secs(1),
                command: "TEST",
            })
            .await
            .expect_err("expected dispatch failure");

        assert!(matches!(
            err,
            ScatterError::ShardUnavailable(ShardSinkError { shard_id: 5, .. })
        ));
        let mut aborted = aborts.lock().await.clone();
        aborted.sort_unstable();
        // Shard 5 (the failed dispatch) and shard 7 (never dispatched) must
        // be aborted. Shard 2 already received VllExecute and must NOT be —
        // it releases its own locks when execution completes.
        assert_eq!(aborted, vec![5, 7]);
    }

    #[tokio::test]
    async fn acquire_continuation_returns_guard_that_releases_on_drop() {
        let (sink, _) = TestSink::ok_sink();
        let coord = VllCoordinator::new(sink, NoopMetricsSink);
        let guard = coord
            .acquire_continuation(42, 7, &[0, 1, 2], Duration::from_secs(1))
            .await
            .expect("continuation ok");
        assert_eq!(guard.shard_count(), 3);
        // Dropping guard sends release.
        drop(guard);
    }

    // FM-VLL-002
    #[tokio::test]
    async fn acquire_continuation_releases_partially_acquired_on_failure() {
        let (sink, _) = TestSink::ok_sink();
        // Shard 2 reports busy.
        *sink.cont_outcomes.lock().await = Box::new(|s, _| {
            if s == 2 {
                ShardReadyResult::Failed(VllError::ShardBusy)
            } else {
                ShardReadyResult::Ready
            }
        });
        let coord = VllCoordinator::new(sink, NoopMetricsSink);
        let err = coord
            .acquire_continuation(42, 7, &[0, 1, 2], Duration::from_secs(1))
            .await
            .expect_err("expected busy");
        assert!(matches!(
            err,
            ContinuationError::LockFailed { shard_id: 2, .. }
        ));
    }

    /// The owned continuation method must hold every lock for the whole
    /// duration of the caller's primary work and release them all once the
    /// work returns.
    #[tokio::test]
    async fn acquire_continuation_and_run_holds_lock_across_run_then_releases() {
        use std::sync::atomic::{AtomicBool, Ordering};
        use tokio::sync::oneshot::error::TryRecvError;

        let (sink, _) = TestSink::ok_sink();
        let release_rxs = sink.release_rxs.clone();
        let coord = VllCoordinator::new(sink, NoopMetricsSink);

        let ran = Arc::new(AtomicBool::new(false));
        let ran_in_run = ran.clone();
        let release_rxs_in_run = release_rxs.clone();

        let result: u32 = coord
            .acquire_continuation_and_run(
                42,
                7,
                &[0, 1, 2],
                Duration::from_secs(1),
                move || async move {
                    // While the primary work runs, every lock is still held:
                    // no release signal has fired yet.
                    let mut guard = release_rxs_in_run.lock().await;
                    assert_eq!(guard.len(), 3);
                    for rx in guard.iter_mut() {
                        assert!(matches!(rx.try_recv(), Err(TryRecvError::Empty)));
                    }
                    ran_in_run.store(true, Ordering::SeqCst);
                    99
                },
            )
            .await
            .expect("continuation run ok");

        assert_eq!(result, 99);
        assert!(ran.load(Ordering::SeqCst), "run closure must be invoked");

        // After the method returns, the guard has dropped and released
        // every lock.
        let mut guard = release_rxs.lock().await;
        for rx in guard.iter_mut() {
            assert!(matches!(rx.try_recv(), Ok(())));
        }
    }

    /// On acquisition failure the primary work must never run and the error
    /// is surfaced; any partially acquired lock is released.
    // FM-VLL-002
    #[tokio::test]
    async fn acquire_continuation_and_run_skips_run_and_releases_on_failure() {
        use std::sync::atomic::{AtomicBool, Ordering};
        use tokio::sync::oneshot::error::TryRecvError;

        let (sink, _) = TestSink::ok_sink();
        // Shard 2 reports busy; shards 0 and 1 acquire first.
        *sink.cont_outcomes.lock().await = Box::new(|s, _| {
            if s == 2 {
                ShardReadyResult::Failed(VllError::ShardBusy)
            } else {
                ShardReadyResult::Ready
            }
        });
        let release_rxs = sink.release_rxs.clone();
        let coord = VllCoordinator::new(sink, NoopMetricsSink);

        let ran = Arc::new(AtomicBool::new(false));
        let ran_in_run = ran.clone();

        let err = coord
            .acquire_continuation_and_run(
                42,
                7,
                &[0, 1, 2],
                Duration::from_secs(1),
                move || async move {
                    ran_in_run.store(true, Ordering::SeqCst);
                    0u32
                },
            )
            .await
            .expect_err("expected busy");

        assert!(matches!(
            err,
            ContinuationError::LockFailed { shard_id: 2, .. }
        ));
        assert!(
            !ran.load(Ordering::SeqCst),
            "run closure must not be invoked on acquisition failure"
        );

        // All shards that received the lock request (phase 1 dispatches to
        // every shard before phase 2 detects the failure) had their locks
        // released when acquisition unwound — no lock is left held.
        let mut guard = release_rxs.lock().await;
        assert_eq!(guard.len(), 3);
        for rx in guard.iter_mut() {
            assert!(matches!(rx.try_recv(), Ok(()) | Err(TryRecvError::Closed)));
        }
    }

    /// A wound reported through the ready channel while acquisition is still
    /// walking the shard list must abandon the acquisition immediately, and be
    /// distinguishable from an ordinary `ShardBusy` refusal — the caller retries
    /// a wound (keeping its txid) and does not retry a refusal the same way.
    // FM-VLL-006
    #[tokio::test]
    async fn wound_during_acquisition_gives_way_immediately() {
        let (sink, _) = TestSink::ok_sink();
        *sink.cont_outcomes.lock().await = Box::new(|s, _| {
            if s == 1 {
                ShardReadyResult::Failed(VllError::Wounded)
            } else {
                ShardReadyResult::Ready
            }
        });
        let release_rxs = sink.release_rxs.clone();
        let coord = VllCoordinator::new(sink, NoopMetricsSink);

        let err = coord
            .acquire_continuation(42, 7, &[0, 1, 2], Duration::from_secs(1))
            .await
            .expect_err("expected a wound");

        assert!(
            err.is_wound(),
            "a wound is retryable, unlike a plain refusal"
        );
        assert!(matches!(
            err,
            ContinuationError::Revoked {
                shard_id: 1,
                reason: VllError::Wounded
            }
        ));

        // Every lock this transaction did take is released before the error
        // surfaces, or the older transaction it gave way to would never get in.
        let mut guard = release_rxs.lock().await;
        for rx in guard.iter_mut() {
            assert!(matches!(
                rx.try_recv(),
                Ok(()) | Err(tokio::sync::oneshot::error::TryRecvError::Closed)
            ));
        }
    }

    /// The revocation back-channel has to reach work that is already running:
    /// a `SCRIPT KILL` (or the shard's hold cap) arriving mid-`run` must drop
    /// the run future and release the locks, not wait for work nobody is
    /// waiting on to finish.
    // FM-VLL-007
    #[tokio::test]
    async fn continuation_revoked_mid_run_abandons_the_work_and_releases() {
        let (sink, _) = TestSink::ok_sink();
        let revoke_txs = sink.revoke_txs.clone();
        let release_rxs = sink.release_rxs.clone();
        let coord = VllCoordinator::new(sink, NoopMetricsSink);

        let revoker = async {
            loop {
                {
                    let mut pending = revoke_txs.lock().await;
                    if pending.len() == 3 {
                        let (_, tx) = pending.remove(1);
                        let _ = tx.send(VllError::Revoked);
                        break;
                    }
                }
                tokio::task::yield_now().await;
            }
        };

        let running = coord.acquire_continuation_and_run(
            42,
            7,
            &[0, 1, 2],
            Duration::from_secs(1),
            // Work that never finishes on its own: only the revocation can end
            // this call.
            std::future::pending::<u32>,
        );

        let (outcome, ()) = tokio::join!(running, revoker);
        let err = outcome.expect_err("revocation ends the run");
        assert!(matches!(
            err,
            ContinuationError::Revoked {
                shard_id: 1,
                reason: VllError::Revoked
            }
        ));
        assert!(!err.is_wound(), "a kill is not a retryable wound");

        let mut guard = release_rxs.lock().await;
        assert_eq!(guard.len(), 3);
        for rx in guard.iter_mut() {
            assert!(matches!(rx.try_recv(), Ok(())), "every lock released");
        }
    }

    /// Phase 4 is past the point of no return: every participant already
    /// received `VllExecute`, so a gather timeout cannot mean "never ran". The
    /// error must therefore name what is *unknown* rather than collapsing to a
    /// single shard id, and must not be reported as an abort.
    // FM-VLL-009
    #[tokio::test(start_paused = true)]
    async fn phase4_gather_timeout_reports_which_shards_are_unknown() {
        let (sink, aborts) = TestSink::ok_sink();
        sink.stash_execute.lock().await.push(1);
        let coord = VllCoordinator::new(sink, NoopMetricsSink);

        let err = coord
            .scatter(ScatterRequest {
                txid: 1,
                mode: LockMode::Write,
                participants: vec![participant(0), participant(1), participant(2)],
                timeout: Duration::from_millis(50),
                command: "MSET",
            })
            .await
            .expect_err("expected an ambiguous outcome");

        match err {
            ScatterError::ResultAmbiguous {
                shard_id,
                applied,
                unknown,
            } => {
                assert_eq!(shard_id, 1);
                assert_eq!(applied, vec![0], "shard 0 answered before the timeout");
                assert_eq!(
                    unknown,
                    vec![1, 2],
                    "the silent shard and every shard behind it are unresolved"
                );
            }
            other => panic!("expected ResultAmbiguous, got {other:?}"),
        }

        assert!(
            aborts.lock().await.is_empty(),
            "an executed op is never unwound by abort — that would claim an outcome the \
             coordinator does not know"
        );
    }

    /// End-to-end wound-wait over real shard state machines.
    ///
    /// The livelock this forces is not hypothetical: acquisition is pipelined
    /// (every `send_continuation_lock` is issued before any ready signal is
    /// awaited), so two continuations over overlapping shard sets routinely each
    /// hold one shard the other wants. Under the old no-priority refusal both
    /// give up, both retry, and nothing orders the retries.
    ///
    /// Here the younger transaction (txid 50) is holding shard 1 when the older
    /// one (txid 20) asks for shards 0 and 1. Wound-wait says the older wins:
    /// the younger is revoked, releases, and its retry — **under the same
    /// txid** — succeeds afterwards. Time is paused, and the assertion that no
    /// virtual time passes is what proves the resolution came from priority and
    /// not from a drain or acquisition timeout.
    // FM-VLL-006, LV-VLL-001
    #[tokio::test(start_paused = true)]
    async fn older_continuation_wins_over_a_younger_partial_holder() {
        use crate::{CONTINUATION_DRAIN_TIMEOUT, VllShardState};
        use tokio::sync::mpsc;

        /// One message the shard actor understands — the only one this test
        /// needs, since continuation locks are the whole subject.
        struct ContinuationRequest {
            txid: u64,
            conn_id: u64,
            ready_tx: oneshot::Sender<ShardReadyResult>,
            release_rx: oneshot::Receiver<()>,
            revoke_tx: oneshot::Sender<VllError>,
        }

        /// A shard worker in miniature: owns a real [`VllShardState`] and
        /// drives `next_continuation_event` from its event loop, exactly as
        /// `frogdb-core`'s shard does.
        fn spawn_shard() -> mpsc::UnboundedSender<ContinuationRequest> {
            let (tx, mut rx) = mpsc::unbounded_channel::<ContinuationRequest>();
            tokio::spawn(async move {
                let mut state: VllShardState<()> = VllShardState::default();
                loop {
                    tokio::select! {
                        biased;
                        msg = rx.recv() => match msg {
                            Some(req) => state.request_continuation_lock(
                                req.txid,
                                req.conn_id,
                                req.ready_tx,
                                req.release_rx,
                                req.revoke_tx,
                            ),
                            None => break,
                        },
                        _ = state.next_continuation_event() => {}
                    }
                }
            });
            tx
        }

        struct ShardActorSink {
            shards: Vec<mpsc::UnboundedSender<ContinuationRequest>>,
        }

        impl ShardSink for ShardActorSink {
            type Operation = u64;
            type Response = u32;

            async fn send_lock_request(
                &self,
                _shard_id: usize,
                _request: LockRequest<Self::Operation>,
            ) -> Result<(), ShardSinkError> {
                unreachable!("this harness only exercises continuation locks")
            }

            async fn send_execute(
                &self,
                _shard_id: usize,
                _txid: u64,
                _response_tx: oneshot::Sender<Self::Response>,
            ) -> Result<(), ShardSinkError> {
                unreachable!("this harness only exercises continuation locks")
            }

            async fn send_abort(&self, _shard_id: usize, _txid: u64) {}

            async fn send_continuation_lock(
                &self,
                shard_id: usize,
                txid: u64,
                conn_id: u64,
                ready_tx: oneshot::Sender<ShardReadyResult>,
                release_rx: oneshot::Receiver<()>,
                revoke_tx: oneshot::Sender<VllError>,
            ) -> Result<(), ShardSinkError> {
                self.shards[shard_id]
                    .send(ContinuationRequest {
                        txid,
                        conn_id,
                        ready_tx,
                        release_rx,
                        revoke_tx,
                    })
                    .map_err(|_| ShardSinkError {
                        shard_id,
                        reason: "shard channel closed",
                    })
            }
        }

        let shards = vec![spawn_shard(), spawn_shard()];
        let sink = |shards: &Vec<mpsc::UnboundedSender<ContinuationRequest>>| ShardActorSink {
            shards: shards.clone(),
        };
        let younger = VllCoordinator::new(sink(&shards), NoopMetricsSink);
        let older = VllCoordinator::new(sink(&shards), NoopMetricsSink);

        let timeout = Duration::from_secs(4);
        let start = tokio::time::Instant::now();

        // The younger transaction takes shard 1 and sits on it. `holding`
        // fires once its work is actually running, so the older request below
        // genuinely collides with a *held* lock rather than racing acquisition.
        let (holding_tx, holding_rx) = oneshot::channel();
        let (finish_tx, finish_rx) = oneshot::channel::<()>();
        let younger_run = async {
            younger
                .acquire_continuation_and_run(50, 2, &[1], timeout, || async move {
                    let _ = holding_tx.send(());
                    let _ = finish_rx.await;
                    "younger done"
                })
                .await
        };

        let older_run = async {
            holding_rx.await.expect("younger acquired shard 1");
            older
                .acquire_continuation_and_run(20, 1, &[0, 1], timeout, || async { "older done" })
                .await
        };

        let (younger_first, older_result) = tokio::join!(younger_run, older_run);

        let wound = younger_first.expect_err("the younger transaction is wounded");
        assert!(wound.is_wound(), "wounds are retryable: {wound}");
        assert_eq!(
            older_result.expect("the older transaction wins"),
            "older done"
        );

        // Liveness, the whole point: the wounded transaction retries under its
        // *original* txid (a fresh, higher txid could be wounded again by every
        // later arrival, forever) and gets through.
        let _ = finish_tx.send(());
        // The winner's release is a message its shard's own event loop still
        // has to observe — the same round-trip a real retry pays. One tick of
        // (virtual) time is enough; the wait below is what the assertion on
        // elapsed time then bounds.
        tokio::time::sleep(Duration::from_millis(1)).await;
        let younger_retry = younger
            .acquire_continuation_and_run(50, 2, &[1], timeout, || async { "younger done" })
            .await
            .expect("the wounded transaction makes progress on retry");
        assert_eq!(younger_retry, "younger done");

        assert!(
            start.elapsed() < Duration::from_millis(10),
            "priority resolved the collision; no drain ({CONTINUATION_DRAIN_TIMEOUT:?}) or \
             acquisition ({timeout:?}) timeout was involved, but {:?} elapsed",
            start.elapsed()
        );
    }

    /// End-to-end wound-wait on the *scatter* path, over real shard state
    /// machines.
    ///
    /// Two cross-shard writes touch the same two shards and win them in
    /// opposite orders: txn 50 holds shard 0 and wants shard 1, txn 20 holds
    /// shard 1 and wants shard 0. Without wound-wait each request parks behind
    /// the other's grant — a wait-for cycle — and neither moves until the
    /// phase-2 acquisition timeout fires on both, aborting both. With it the
    /// older transaction (txid 20) wounds the younger, the younger unwinds
    /// through its ordinary abort path, and its retry — **under the same
    /// txid** — gets through behind the winner.
    ///
    /// Time is paused: the assertion that essentially no virtual time passes
    /// is what proves priority resolved this and not the timeout.
    // FM-VLL-010, LV-VLL-002
    #[tokio::test(start_paused = true)]
    async fn opposite_shard_orders_resolve_by_seniority_instead_of_deadlocking() {
        use crate::VllShardState;
        use tokio::sync::{Mutex, mpsc};

        enum ShardMsg {
            Lock {
                txid: u64,
                keys: Vec<Bytes>,
                mode: LockMode,
                ready_tx: oneshot::Sender<ShardReadyResult>,
                wound_tx: oneshot::Sender<VllError>,
            },
            Execute {
                txid: u64,
                response_tx: oneshot::Sender<u32>,
            },
            Abort {
                txid: u64,
            },
        }

        /// A shard worker in miniature: owns a real [`VllShardState`] and
        /// serves the three SCA messages, executing and releasing exactly as
        /// `frogdb-core`'s shard does.
        fn spawn_shard() -> mpsc::UnboundedSender<ShardMsg> {
            let (tx, mut rx) = mpsc::unbounded_channel::<ShardMsg>();
            tokio::spawn(async move {
                let mut state: VllShardState<()> = VllShardState::default();
                while let Some(msg) = rx.recv().await {
                    match msg {
                        ShardMsg::Lock {
                            txid,
                            keys,
                            mode,
                            ready_tx,
                            wound_tx,
                        } => {
                            state.enqueue_lock_request(txid, keys, mode, (), ready_tx, wound_tx);
                        }
                        ShardMsg::Execute { txid, response_tx } => {
                            if let Some(op) = state.dequeue_for_execution(txid) {
                                let _ = response_tx.send(txid as u32);
                                state.release_after_execution(op.txid, &op.keys);
                            }
                        }
                        ShardMsg::Abort { txid } => state.abort(txid),
                    }
                }
            });
            tx
        }

        /// Holds one shard's lock request back until the test opens the gate,
        /// so each transaction reaches the other's shard only after that shard
        /// is already held. The gate is consumed on first use — a retry runs
        /// ungated.
        struct GatedSink {
            shards: Vec<mpsc::UnboundedSender<ShardMsg>>,
            gate: Mutex<Option<(usize, oneshot::Receiver<()>)>>,
        }

        impl ShardSink for GatedSink {
            type Operation = ();
            type Response = u32;

            async fn send_lock_request(
                &self,
                shard_id: usize,
                request: LockRequest<Self::Operation>,
            ) -> Result<(), ShardSinkError> {
                let LockRequest {
                    txid,
                    keys,
                    mode,
                    ready_tx,
                    wound_tx,
                    ..
                } = request;
                let gate = {
                    let mut held = self.gate.lock().await;
                    match held.as_ref() {
                        Some((gated, _)) if *gated == shard_id => held.take().map(|(_, rx)| rx),
                        _ => None,
                    }
                };
                if let Some(gate) = gate {
                    let _ = gate.await;
                }
                self.shards[shard_id]
                    .send(ShardMsg::Lock {
                        txid,
                        keys,
                        mode,
                        ready_tx,
                        wound_tx,
                    })
                    .map_err(|_| ShardSinkError {
                        shard_id,
                        reason: "shard channel closed",
                    })
            }

            async fn send_execute(
                &self,
                shard_id: usize,
                txid: u64,
                response_tx: oneshot::Sender<Self::Response>,
            ) -> Result<(), ShardSinkError> {
                self.shards[shard_id]
                    .send(ShardMsg::Execute { txid, response_tx })
                    .map_err(|_| ShardSinkError {
                        shard_id,
                        reason: "shard channel closed",
                    })
            }

            async fn send_abort(&self, shard_id: usize, txid: u64) {
                let _ = self.shards[shard_id].send(ShardMsg::Abort { txid });
            }

            async fn send_continuation_lock(
                &self,
                _shard_id: usize,
                _txid: u64,
                _conn_id: u64,
                _ready_tx: oneshot::Sender<ShardReadyResult>,
                _release_rx: oneshot::Receiver<()>,
                _revoke_tx: oneshot::Sender<VllError>,
            ) -> Result<(), ShardSinkError> {
                unreachable!("this harness only exercises scatter locks")
            }
        }

        let shards = vec![spawn_shard(), spawn_shard()];
        let (open_younger, younger_gate) = oneshot::channel();
        let (open_older, older_gate) = oneshot::channel();
        let younger = VllCoordinator::new(
            GatedSink {
                shards: shards.clone(),
                gate: Mutex::new(Some((1, younger_gate))),
            },
            NoopMetricsSink,
        );
        let older = VllCoordinator::new(
            GatedSink {
                shards: shards.clone(),
                gate: Mutex::new(Some((0, older_gate))),
            },
            NoopMetricsSink,
        );

        let timeout = Duration::from_secs(4);
        let request = |txid: u64, order: [usize; 2]| ScatterRequest {
            txid,
            mode: LockMode::Write,
            participants: order
                .into_iter()
                .map(|shard_id| ScatterParticipant {
                    shard_id,
                    keys: vec![Bytes::from(format!("key{shard_id}"))],
                    operation: (),
                })
                .collect(),
            timeout,
            command: "MSET",
        };

        let start = tokio::time::Instant::now();

        // Each transaction dispatches to its own shard first, then blocks at
        // the gate before reaching the other's.
        let younger_run = async {
            let wound = younger
                .scatter(request(50, [0, 1]))
                .await
                .expect_err("the younger transaction gives way");
            assert!(wound.is_wound(), "wounds are retryable: {wound}");
            assert!(
                matches!(wound, ScatterError::Wounded { shard_id: 0 }),
                "wounded on the shard the older transaction wanted: {wound:?}"
            );
            // Liveness, the whole point: the retry keeps the *original* txid.
            // A fresh, higher txid could be wounded again by every later
            // arrival, forever.
            younger.scatter(request(50, [0, 1])).await
        };
        let older_run = async { older.scatter(request(20, [1, 0])).await };
        let opener = async {
            // Both requests are parked on their gates by the time this runs:
            // under paused time the sleep only advances once the runtime is
            // otherwise idle.
            tokio::time::sleep(Duration::from_millis(1)).await;
            let _ = open_younger.send(());
            let _ = open_older.send(());
        };

        let (younger_retry, older_result, ()) = tokio::join!(younger_run, older_run, opener);

        let older_ok = older_result.expect("the older transaction wins");
        assert_eq!(older_ok.responses.len(), 2);
        assert!(
            older_ok.responses.iter().all(|&(_, txid)| txid == 20),
            "the winner's own execute ran on both shards: {:?}",
            older_ok.responses
        );
        let younger_ok = younger_retry.expect("the wounded transaction makes progress on retry");
        assert_eq!(younger_ok.responses.len(), 2);

        assert!(
            start.elapsed() < Duration::from_millis(10),
            "priority resolved the collision; the phase-2 acquisition timeout ({timeout:?}) was \
             not involved, but {:?} elapsed",
            start.elapsed()
        );
    }

    /// Sink whose shard `k` answers only after `(k + 1) * stagger`, scheduled
    /// from the moment of dispatch. Every answer is therefore exactly one
    /// stagger later than the previous one — measured from where a sequential
    /// receiver loop reaches it, each wait is short, while the request as a
    /// whole runs `participants * stagger`. That is the shape a per-receiver
    /// relative timeout cannot catch and an absolute deadline can.
    struct StaggeredSink {
        /// Applied to lock-request and continuation-lock ready signals.
        ready_stagger: Duration,
        /// Applied to execute responses.
        result_stagger: Duration,
        aborted: Arc<Mutex<Vec<usize>>>,
    }

    impl StaggeredSink {
        fn new(
            ready_stagger: Duration,
            result_stagger: Duration,
        ) -> (Self, Arc<Mutex<Vec<usize>>>) {
            let aborted = Arc::new(Mutex::new(Vec::new()));
            (
                StaggeredSink {
                    ready_stagger,
                    result_stagger,
                    aborted: aborted.clone(),
                },
                aborted,
            )
        }
    }

    impl ShardSink for StaggeredSink {
        type Operation = u64;
        type Response = u32;

        async fn send_lock_request(
            &self,
            shard_id: usize,
            request: LockRequest<Self::Operation>,
        ) -> Result<(), ShardSinkError> {
            let delay = self.ready_stagger * (shard_id as u32 + 1);
            tokio::spawn(async move {
                tokio::time::sleep(delay).await;
                let _ = request.ready_tx.send(ShardReadyResult::Ready);
            });
            Ok(())
        }

        async fn send_execute(
            &self,
            shard_id: usize,
            _txid: u64,
            response_tx: oneshot::Sender<Self::Response>,
        ) -> Result<(), ShardSinkError> {
            let delay = self.result_stagger * (shard_id as u32 + 1);
            tokio::spawn(async move {
                tokio::time::sleep(delay).await;
                let _ = response_tx.send(shard_id as u32 + 100);
            });
            Ok(())
        }

        async fn send_abort(&self, shard_id: usize, _txid: u64) {
            self.aborted.lock().await.push(shard_id);
        }

        async fn send_continuation_lock(
            &self,
            shard_id: usize,
            _txid: u64,
            _conn_id: u64,
            ready_tx: oneshot::Sender<ShardReadyResult>,
            _release_rx: oneshot::Receiver<()>,
            _revoke_tx: oneshot::Sender<VllError>,
        ) -> Result<(), ShardSinkError> {
            let delay = self.ready_stagger * (shard_id as u32 + 1);
            tokio::spawn(async move {
                tokio::time::sleep(delay).await;
                let _ = ready_tx.send(ShardReadyResult::Ready);
            });
            Ok(())
        }
    }

    // FM-VLL-011
    //
    // Three shards, each ready one stagger after the last, with a request
    // timeout longer than one stagger and shorter than three. Timed per
    // receiver, every individual wait fits and the scatter runs to completion
    // at three staggers; timed against one deadline taken at entry, the
    // request gives up at the bound its caller asked for.
    #[tokio::test(start_paused = true)]
    async fn phase2_receiver_waits_share_one_absolute_deadline() {
        let stagger = Duration::from_secs(3);
        let timeout = Duration::from_secs(4);
        let (sink, aborted) = StaggeredSink::new(stagger, Duration::ZERO);
        let coord = VllCoordinator::new(sink, NoopMetricsSink);

        let start = Instant::now();
        let err = coord
            .scatter(ScatterRequest {
                txid: 1,
                mode: LockMode::Write,
                participants: vec![participant(0), participant(1), participant(2)],
                timeout,
                command: "TEST",
            })
            .await
            .expect_err("the request deadline must fire before every shard is ready");
        let elapsed = start.elapsed();

        assert!(
            matches!(err, ScatterError::LockTimeout { shard_id: 1 }),
            "the deadline names the shard being waited on: {err:?}"
        );
        assert!(
            elapsed <= timeout,
            "one request, one budget: {elapsed:?} elapsed against a {timeout:?} timeout \
             (per-receiver waits would have run to {:?})",
            stagger * 3
        );
        assert_eq!(
            *aborted.lock().await,
            vec![0, 1, 2],
            "every dispatched participant is unwound"
        );
    }

    // FM-VLL-011
    //
    // Phase 4 spends what phase 2 left of the same deadline rather than
    // starting a fresh allowance per result receiver. Locks are granted at
    // once here, so the whole budget goes to the gather.
    #[tokio::test(start_paused = true)]
    async fn phase4_gather_shares_the_request_deadline() {
        let stagger = Duration::from_secs(3);
        let timeout = Duration::from_secs(4);
        let (sink, _aborted) = StaggeredSink::new(Duration::ZERO, stagger);
        let coord = VllCoordinator::new(sink, NoopMetricsSink);

        let start = Instant::now();
        let err = coord
            .scatter(ScatterRequest {
                txid: 1,
                mode: LockMode::Write,
                participants: vec![participant(0), participant(1), participant(2)],
                timeout,
                command: "TEST",
            })
            .await
            .expect_err("the request deadline must fire before every result is in");
        let elapsed = start.elapsed();

        match err {
            ScatterError::ResultAmbiguous {
                shard_id,
                applied,
                unknown,
            } => {
                assert_eq!(shard_id, 1);
                assert_eq!(applied, vec![0], "shard 0 answered inside the deadline");
                assert_eq!(unknown, vec![1, 2], "the rest are unresolved, not aborted");
            }
            other => panic!("expected an ambiguous gather outcome, got {other:?}"),
        }
        assert!(
            elapsed <= timeout,
            "one request, one budget: {elapsed:?} elapsed against a {timeout:?} timeout \
             (per-receiver waits would have run to {:?})",
            stagger * 3
        );
    }

    // FM-VLL-011
    //
    // `acquire_continuation` has the same sequential receiver loop and the
    // same bound.
    #[tokio::test(start_paused = true)]
    async fn continuation_acquisition_waits_share_one_absolute_deadline() {
        let stagger = Duration::from_secs(3);
        let timeout = Duration::from_secs(4);
        let (sink, _aborted) = StaggeredSink::new(stagger, Duration::ZERO);
        let coord = VllCoordinator::new(sink, NoopMetricsSink);

        let start = Instant::now();
        let err = coord
            .acquire_continuation(7, 1, &[0, 1, 2], timeout)
            .await
            .expect_err("the deadline must fire before every shard grants");
        let elapsed = start.elapsed();

        assert!(
            matches!(err, ContinuationError::LockTimeout { shard_id: 1 }),
            "the deadline names the shard being waited on: {err}"
        );
        assert!(
            elapsed <= timeout,
            "one acquisition, one budget: {elapsed:?} elapsed against a {timeout:?} timeout \
             (per-receiver waits would have run to {:?})",
            stagger * 3
        );
    }
}
