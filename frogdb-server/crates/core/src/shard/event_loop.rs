use std::time::Duration;

use frogdb_types::metrics::definitions::{FieldsExpired, KeysExpired, ShardQueueLatency};

use crate::store::Store;

use super::active_expiry::ExpiryResult;
#[cfg(any(test, feature = "shard-driver"))]
use super::message::Envelope;
use super::message::ShardMessage;
use super::panic_guard;
use super::post_execution::{ENGINE_INTERNAL_CONN_ID, RemovalPropagation, RemovalReason};
use super::worker::ShardWorker;
use crate::clock;
use crate::vll::ContinuationEvent;
#[cfg(any(test, feature = "shard-driver"))]
use bytes::Bytes;

impl ShardWorker {
    /// Run the shard worker event loop.
    pub async fn run(mut self) {
        tracing::info!(shard_id = self.shard_id(), "Shard worker started");

        // Active expiry runs every 100ms
        let mut expiry_interval = tokio::time::interval(Duration::from_millis(100));

        // Metrics collection runs every 10 seconds
        let mut metrics_interval = tokio::time::interval(Duration::from_secs(10));

        // Blocking waiter timeout check runs every 100ms
        let mut waiter_timeout_interval = tokio::time::interval(Duration::from_millis(100));

        // Search index commit runs every 1 second
        let mut search_commit_interval = tokio::time::interval(Duration::from_secs(1));

        loop {
            // Re-read every iteration so the flag can be flipped at runtime
            // (the seam's setter takes `&mut self`, i.e. only between
            // iterations, so this is a plain read of settled state).
            let timer_sweeps = self.timer_sweeps_enabled();

            // `biased;` (determinism audit R7/A51): this is the hottest
            // continuously-looping `select!` in the system, so branch order is
            // a real production fairness decision, not just a determinism one
            // — a naive top-to-bottom bias that put the data-plane arms first
            // would let a sustained message backlog starve the maintenance
            // arms below them indefinitely (tokio's random tie-break is what
            // gives them a chance today). None of the arms below are
            // "always ready" the way `message_rx`/`new_conn_rx` can be under
            // load — each is a periodic tick or a rare event that resolves
            // once and goes back to pending — so putting them ahead of the
            // data-plane arms costs at most one poll's worth of dispatch
            // latency per firing, while guaranteeing none of them can be
            // starved by a continuous queue. `message_rx` is deliberately
            // last: it is the one arm that can be perpetually ready, so
            // nothing may be placed after it.
            tokio::select! {
                biased;
                // 1. Continuation lock lifecycle. VLL correctness: a delayed
                // release notification or drain-timeout report can convoy the
                // next lock holder, so this must not wait behind a backlog.
                event = self.vll.next_continuation_event() => {
                    match event {
                        ContinuationEvent::Released => {
                            tracing::debug!(shard_id = self.shard_id(), "Continuation lock released");
                        }
                        ContinuationEvent::DrainTimedOut => {
                            tracing::warn!(
                                shard_id = self.shard_id(),
                                "Continuation lock request timed out waiting for the shard queue to drain"
                            );
                        }
                        // Nobody was left to hand the lock to. Not a warning:
                        // the requester leaving is ordinary, and the point is
                        // that the shard stopped refusing other work for it.
                        ContinuationEvent::ParkAbandoned => {
                            tracing::debug!(
                                shard_id = self.shard_id(),
                                "Parked continuation request abandoned by its requester; drain barrier lifted"
                            );
                        }
                        // The holder has been told to let go; the lock clears
                        // when its release actually arrives. A script that
                        // legitimately runs this long is a bug in the script,
                        // not in the shard, so this is worth a warning.
                        ContinuationEvent::HoldCapExpired => {
                            tracing::warn!(
                                shard_id = self.shard_id(),
                                "Continuation lock held past the cap; revoked its holder"
                            );
                        }
                    }
                }

                // 2. Blocking waiter timeout check (100ms) — coarse GC only;
                // `BlockingWaitCoordinator` (server/connection/blocking) is
                // the canonical timeout authority and races its own deadline
                // independently, so this tick lagging behind `message_rx`
                // delays cleanup bookkeeping, not the client-visible timeout.
                // Suppressed in a driven run, where the sweep arrives as a
                // `DriveTick` message instead (see `ShardWorker::set_driven_ticks`).
                _ = waiter_timeout_interval.tick(), if timer_sweeps => {
                    self.check_waiter_timeouts();
                    // Same tick serves the pops that parked behind a node-global
                    // `CLIENT PAUSE` and have no waking write coming
                    // (`specs/blocking.md` TR-BLOCKING-026). Coarse by design,
                    // like the timeout sweep above: the pause lift is observed
                    // within one tick, not at the instant the deadline passes.
                    self.resume_pops_deferred_by_pause().await;
                }

                // 3. Active expiry task (100ms) — proactive reclaim; reads
                // still lazily purge, so a delayed cycle costs memory
                // headroom under load, not correctness. Suppressed in a
                // driven run, like the waiter sweep above (audit R6).
                _ = expiry_interval.tick(), if timer_sweeps => {
                    if self.per_request_spans.load(std::sync::atomic::Ordering::Relaxed) {
                        // Build the span before creating the future so `shard_id()`'s
                        // borrow ends before `run_active_expiry` takes `&mut self`;
                        // `Instrument` carries the span across the await correctly
                        // (never hold an entered guard across `.await`).
                        use tracing::Instrument;
                        let span = tracing::info_span!("active_expiry", shard_id = self.shard_id());
                        self.run_active_expiry().instrument(span).await;
                    } else {
                        self.run_active_expiry().await;
                    }
                }

                // 4. Periodic search index commit (1s).
                _ = search_commit_interval.tick() => {
                    let sid = self.identity.shard_id();
                    for idx in self.search.indexes.values_mut() {
                        if idx.is_dirty() && let Err(e) = idx.commit() {
                            tracing::error!(shard_id = sid, error = %e, "Failed to commit search index");
                        }
                    }
                }

                // 5. Periodic metrics collection (10s) — observability only.
                _ = metrics_interval.tick() => {
                    self.collect_shard_metrics();
                }

                // 6. Handle new connections — rare relative to steady-state
                // message traffic, so prioritizing it ahead of dispatch costs
                // nothing and keeps CLIENT accept latency low.
                Some(new_conn) = self.new_conn_rx.recv() => {
                    self.handle_new_connection(new_conn).await;
                }

                // 7. Handle shard messages — dispatch to grouped sub-handlers.
                // The data plane: kept last because it is the one arm that
                // can be continuously ready under load.
                Some(envelope) = self.message_rx.recv() => {
                    let queue_latency = clock::elapsed(envelope.enqueued_at).as_secs_f64();
                    let msg = envelope.message;

                    let msg_kind = msg.probe_type_str();

                    crate::probes::fire_shard_message_received(
                        self.shard_id() as u64,
                        msg_kind,
                        self.message_rx.len() as u64,
                    );

                    ShardQueueLatency::observe(
                        self.observability.metrics(),
                        queue_latency,
                        self.identity.shard_label(),
                    );

                    // Panic isolation (c2-07), outer net. The inner guards
                    // (`dispatch_core`, `vll::handle_vll_execute`,
                    // `execute_transaction`) exist because they can still answer
                    // the waiting client; this one covers every remaining
                    // message category, where the caller sees only a dropped
                    // oneshot. Losing one reply is the correct trade against
                    // unwinding the task and letting the supervisor abort the
                    // process. Deliberately *not* extended to the maintenance
                    // arms above: a panic there is not attributable to a client
                    // message, and fail-stop stays the right answer for it.
                    let outcome = panic_guard::caught(self.dispatch_message(msg)).await;
                    let should_stop = match outcome {
                        Ok(should_stop) => should_stop,
                        Err(panic_message) => {
                            self.recover_from_panic(
                                panic_guard::PanicSite::Message,
                                msg_kind,
                                &panic_message,
                            );
                            false
                        }
                    };
                    if should_stop {
                        break;
                    }
                }

                else => break,
            }
        }

        // Final search index commit
        {
            let sid = self.identity.shard_id();
            for idx in self.search.indexes.values_mut() {
                if idx.is_dirty()
                    && let Err(e) = idx.commit()
                {
                    tracing::error!(shard_id = sid, error = %e, "Failed to commit search index on shutdown");
                }
            }
        }

        // Final WAL flush
        if let Some(wal) = self.persistence.wal_writer()
            && let Err(e) = wal.flush_async().await
        {
            tracing::error!(shard_id = self.shard_id(), error = %e, "Failed to flush WAL on exit");
        }
    }

    /// Whether the event loop's two periodic-sweep timer branches (active
    /// expiry, blocking-waiter timeout) are live this iteration.
    ///
    /// Always `true` in a production build — the seam feature is not compiled,
    /// so this folds to a constant and the `select!` guards vanish. Under the
    /// `shard-driver` seam a driven run flips it off and delivers both sweeps
    /// as [`ShardMessage::DriveTick`](super::message::ShardMessage) messages
    /// instead (determinism audit R6).
    #[cfg(any(test, feature = "shard-driver"))]
    fn timer_sweeps_enabled(&self) -> bool {
        !self.driven_ticks
    }

    /// See the seam-enabled twin above: without `shard-driver` the sweeps are
    /// always timer-driven.
    #[cfg(not(any(test, feature = "shard-driver")))]
    fn timer_sweeps_enabled(&self) -> bool {
        true
    }

    /// Run active expiry with time budget.
    ///
    /// Thin shard-side wrapper: the pause/disable gates read shard-owned atomics
    /// and stay here; the decision + deletion half is delegated to
    /// [`ActiveExpiryCoordinator::run_cycle`], and the side effects are applied
    /// past the seam from the returned [`ExpiryResult`].
    pub(crate) async fn run_active_expiry(&mut self) {
        // Sync the expiry_paused flag to the store for passive expiry suppression.
        let paused = self
            .expiry_paused
            .load(std::sync::atomic::Ordering::Relaxed);
        self.store.set_expiry_suppressed(paused);

        // Skip active expiry during CLIENT PAUSE to prevent master/replica divergence.
        if paused {
            return;
        }

        // Skip active expiry when disabled via DEBUG SET-ACTIVE-EXPIRE 0.
        if self.debug_active_expire_disabled {
            return;
        }

        // Invariant the discard below relies on: the lazy-purge buffers are
        // empty when a cycle starts. The shard event loop is a single
        // `tokio::select!` with no `.await` between a command's drain
        // (`apply_lazy_purge_effects`, run at every command seam) and this arm,
        // so no lazily-purged/emptied report can be pending here. If a future
        // refactor introduces a yield point that interleaves a partially-drained
        // command with this sweep, this fails loud rather than letting the
        // discard silently drop a genuine lazy report.
        debug_assert!(
            self.store.lazy_purge_buffers_empty(),
            "lazy-purge buffers must be empty at active-expiry cycle start; \
             a command's lazy drain was interleaved with the sweep"
        );

        // Disjoint-field borrow: `self.expiry` and `self.store` are distinct fields.
        let result = self.expiry.run_cycle(&mut self.store, crate::clock::now());
        // The sweep reaps last-hash-field deaths and hash-field reaps through the
        // *same* `purge_expired_hash_fields` seam a lazy read uses, so it also
        // fills the store's lazily-emptied buffer, lazily-shrunk buffer, and
        // lazily-expired-fields counter. But the sweep already owns reporting for
        // the removals + field count via `result.emptied_keys` /
        // `result.fields_expired` — `apply_expiry_effects` fires their `del`
        // events and metric bumps below. Discard those here so a later command's
        // lazy drain (`drain_lazy_purge_effects`) does not re-fire `del` or
        // re-count metrics for what the sweep already reported. Between event-loop
        // iterations the buffers are empty (every command drains at its own seam;
        // asserted above), so this discards only what this cycle just produced —
        // never a pending lazy read.
        //
        // The shrunk-survivor buffer is discarded on the same grounds: the sweep
        // owns that reporting too, through `ExpiryResult::field_shrunk_keys`. The
        // survivors are re-indexed (their search doc still holds the reaped
        // field's stale value) and their slots bumped from the *result*, inside
        // `apply_expiry_effects`, so a later command's lazy drain cannot re-fire
        // either effect for what this cycle already reported.
        self.store.take_lazily_shrunk();
        self.store.take_lazily_emptied();
        self.store.take_lazily_expired_fields();
        self.apply_expiry_effects(result).await;
    }

    /// Apply the side effects of an active-expiry cycle.
    ///
    /// This is the shard side of the seam: it owns the state the coordinator is
    /// deliberately blind to. The **removals** (whole-key TTL deaths and
    /// hash-emptied keys) are driven through the canonical write-effect pipeline
    /// via [`ShardWorker::run_internal_removal_effects`] — reconstructed as
    /// synthetic `DEL`s — so they inherit the *same* effect set + order as every
    /// other write path (tracking invalidation, `expired`/`del` keyspace
    /// notification, dirty counter, XREADGROUP NOGROUP drain, WAL delete,
    /// search-index delete) instead of a hand-rolled partial subset. Only the
    /// expiry-specific observability the pipeline does not own — the per-key USDT
    /// probes and the aggregate expired-key/field metrics — stays here (mirroring
    /// how eviction keeps its own metrics local).
    ///
    /// Propagation policy (explicit, not accidental): `wal = true` drops the
    /// stale RocksDB entry at the source; `replicate = false` preserves FrogDB's
    /// independent-expiry model (each node expires on its own clock — a
    /// documented divergence from Redis's primary-drives-expiry; flipping it is a
    /// deliberate ADR, out of scope here).
    pub(crate) async fn apply_expiry_effects(&mut self, mut result: ExpiryResult) {
        if result.is_empty() {
            return;
        }

        // Hashes shrunk in place by this cycle: not removals, so they never flow
        // through the pipeline below. Taken up front because the removal lists are
        // moved into it.
        let field_shrunk = std::mem::take(&mut result.field_shrunk_keys);

        // Expiry-specific observability (not pipeline effects): fire the per-key
        // USDT probe for every removed key, then bump the aggregate metrics.
        // Count every removed key exactly once — key-level TTL AND field-emptied
        // — so INFO `expired_keys` / `frogdb_keys_expired_total` do not
        // under-count; the fields that triggered an emptied key are counted
        // separately, so no double-count.
        for key in result.deleted_keys.iter().chain(result.emptied_keys.iter()) {
            crate::probes::fire_key_expired(
                std::str::from_utf8(key).unwrap_or("<binary>"),
                self.shard_id() as u64,
            );
        }
        let shard_label = self.shard_id().to_string();
        let keys_expired = result.keys_expired();
        if keys_expired > 0 {
            self.store.add_expired_keys(keys_expired);
            KeysExpired::inc_by(self.observability.metrics(), keys_expired, &shard_label);
        }
        if result.fields_expired > 0 {
            FieldsExpired::inc_by(
                self.observability.metrics(),
                result.fields_expired,
                &shard_label,
            );
        }

        // Route both removal groups through the pipeline in a SINGLE call so the
        // whole cycle coalesces to ONE version bump (not one per group), while
        // still emitting `expired` for whole-key deaths and generic `del` for
        // hash-emptied keys.
        self.run_internal_removal_effects(
            vec![
                (RemovalReason::Expired, result.deleted_keys),
                (RemovalReason::FieldEmptied, result.emptied_keys),
            ],
            RemovalPropagation {
                wal: true,
                replicate: false,
            },
            ENGINE_INTERNAL_CONN_ID,
        )
        .await;

        // A cycle that reaped hash *fields* from a surviving hash is a mutation,
        // not a removal, so it does not flow through the removal pipeline above —
        // but it still changed a watched hash, and its search-index doc still
        // holds the reaped field's stale value. The sweep enumerates exactly
        // which survivors it shrank, so both effects are keyed to those keys:
        // re-index each one, and bump each one's *slot* (never the shard-wide
        // epoch, which would abort every unrelated watch on the shard and let a
        // tenant's continuously-firing field TTLs starve every other CAS loop
        // forever — `specs/txn.md` FM-TXN-033). Removals that happened in the
        // same cycle already bumped their own slots via the pipeline above; a
        // shrunk survivor is not among them, which is why this is unconditional
        // on what else the cycle did.
        //
        // Guarded on non-empty: `bump_versions_for` reads an empty key set as a
        // keyless dirtying write and bumps the epoch — the exact over-abort this
        // path exists to stop.
        if !field_shrunk.is_empty() {
            self.reindex_shrunk_hash_keys(&field_shrunk);
            self.bump_versions_for(field_shrunk.iter().map(|key| key.as_ref()));
        }
    }

    /// Dispatch a shard message to the appropriate handler.
    /// Returns `true` if the event loop should break (shutdown).
    pub(crate) async fn dispatch_message(&mut self, msg: ShardMessage) -> bool {
        match msg {
            ShardMessage::Core(m) => self.dispatch_core(m).await,
            ShardMessage::PubSub(m) => {
                self.dispatch_pubsub(m);
                false
            }
            ShardMessage::Tracking(m) => {
                self.dispatch_tracking(m);
                false
            }
            ShardMessage::Scripting(m) => self.dispatch_scripting(m).await,
            ShardMessage::Blocking(m) => {
                self.dispatch_blocking(m);
                false
            }
            ShardMessage::Observability(m) => {
                self.dispatch_observability(m);
                false
            }
            ShardMessage::Vll(m) => self.dispatch_vll(m).await,
            ShardMessage::DebugIntrospection(m) => {
                self.dispatch_debug_introspection(m);
                false
            }
            ShardMessage::Cluster(m) => self.dispatch_cluster(m).await,
            ShardMessage::Replication(m) => self.dispatch_replication(m).await,
            ShardMessage::Search(m) => {
                // `FlushWal` needs to await the WAL flush thread, so it is handled
                // here in the async event loop rather than in the sync
                // `dispatch_search`. All other search messages are synchronous.
                match m {
                    super::message::SearchMsg::FlushWal {
                        hold_for,
                        response_tx,
                    } => {
                        if let Some(wal) = self.persistence.wal_writer()
                            && let Err(e) = wal.flush_async().await
                        {
                            tracing::error!(shard_id = self.shard_id(), error = %e, "Failed to flush WAL for snapshot");
                        }
                        // Armed synchronously, with **no `.await` between the
                        // drain above and here**. This task is the only producer
                        // of WAL entries for this shard, so nothing can have been
                        // staged in the gap: the armed instant *is* the drain
                        // point, and `last_broadcast_offset` read below is the
                        // exact watermark the pinned payload covers. That is why
                        // no new `WalCommand` is needed — the hold does not have
                        // to travel the WAL channel to be correctly placed.
                        let hold: Option<std::sync::Arc<crate::persistence::FlushHold>> =
                            match (hold_for, self.persistence.flush_hold()) {
                                (Some(window), Some(hold)) => {
                                    hold.arm(frogdb_types::clock::now() + window);
                                    Some(std::sync::Arc::clone(hold))
                                }
                                _ => None,
                            };
                        let _ = response_tx.send(super::message::WalDrainAck {
                            last_broadcast_offset: self.last_broadcast_offset(),
                            hold,
                        });
                    }
                    other => self.dispatch_search(other),
                }
                false
            }
            // Driven run (see `ShardWorker::set_driven_ticks`): the periodic
            // sweep arrives as a queued message, so it is totally ordered
            // against commands instead of racing them in the `select!`.
            #[cfg(any(test, feature = "shard-driver"))]
            ShardMessage::DriveTick(kind) => {
                match kind {
                    super::message::TickKind::Expiry => self.drive_expiry_tick().await,
                    super::message::TickKind::WaiterTimeout => {
                        self.drive_waiter_timeout_tick().await
                    }
                }
                false
            }
            ShardMessage::Shutdown => {
                tracing::info!(shard_id = self.shard_id(), "Shard worker shutting down");
                if let Some(wal) = self.persistence.wal_writer()
                    && let Err(e) = wal.flush_async().await
                {
                    tracing::error!(shard_id = self.shard_id(), error = %e, "Failed to flush WAL on shutdown");
                }
                true
            }
        }
    }

    /// Shard-driver harness seam: dispatch one message, returning the event
    /// loop's shutdown signal (`true` == break). Wraps [`Self::dispatch_message`].
    #[cfg(any(test, feature = "shard-driver"))]
    #[doc(hidden)]
    #[allow(dead_code)]
    pub async fn drive<M: Into<ShardMessage>>(&mut self, msg: M) -> bool {
        self.dispatch_message(msg.into()).await
    }

    /// Shard-driver harness seam: run one active-expiry cycle synchronously,
    /// without waiting on the event loop's 100 ms timer. Wraps
    /// [`Self::run_active_expiry`].
    #[cfg(any(test, feature = "shard-driver"))]
    #[doc(hidden)]
    #[allow(dead_code)]
    pub async fn drive_expiry_tick(&mut self) {
        self.run_active_expiry().await;
    }

    /// Shard-driver harness seam: fire one 100 ms blocking sweep, without
    /// waiting on the event loop's timer. Wraps both halves of that tick —
    /// [`Self::check_waiter_timeouts`] and
    /// [`Self::resume_pops_deferred_by_pause`] — so a driven run exercises the
    /// same work the timer branch does.
    #[cfg(any(test, feature = "shard-driver"))]
    #[doc(hidden)]
    #[allow(dead_code)]
    pub async fn drive_waiter_timeout_tick(&mut self) {
        self.check_waiter_timeouts();
        self.resume_pops_deferred_by_pause().await;
    }

    /// Shard-driver harness seam mirroring the event loop's continuation-event
    /// arm: await the stored release signal — fired when the coordinator's
    /// `ContinuationGuard` drops — or a parked request's drain deadline, and
    /// apply whichever fires.
    ///
    /// Only call when a continuation lock is held (and its guard has been, or
    /// is about to be, dropped) or a request is parked; with neither,
    /// `next_continuation_event` resolves to `pending()` and this future never
    /// completes. The shard-driver harness pumps this per shard, in a permuted
    /// order, after inducing the guard drop (scenario 4).
    #[cfg(any(test, feature = "shard-driver"))]
    #[doc(hidden)]
    #[allow(dead_code)]
    pub async fn drive_continuation_release(&mut self) -> ContinuationEvent {
        self.vll.next_continuation_event().await
    }

    /// Shard-driver harness seam: non-blocking receive of the next queued
    /// envelope off this worker's own message channel. See
    /// [`ShardReceiver::try_recv`](super::message::ShardReceiver::try_recv).
    #[cfg(any(test, feature = "shard-driver"))]
    #[doc(hidden)]
    #[allow(dead_code)]
    pub fn try_recv_queued(&mut self) -> Option<Envelope> {
        self.message_rx.try_recv()
    }

    /// Shard-driver harness seam: enable the given keyspace-event mask and
    /// register a capture PSUBSCRIBE for each glob in `patterns` on this
    /// worker's own subscription table, returning the receiver every matching
    /// emitted notification is delivered into.
    ///
    /// Single-shard drivers run the `Local` keyspace topology
    /// ([`KeyspaceNotificationCoordinator::new`] with `num_shards == 1`), so
    /// `emit_keyspace_notification` publishes straight into `self.subscriptions`
    /// — the same table this seam subscribes into. That is exactly the
    /// synchronous fast path a real single-shard server takes, so a driven
    /// schedule's notifications land in the returned receiver in emission order.
    /// A broad pattern (e.g. `__keyevent@0__:*`) captures every keyevent, so the
    /// consistency checker can detect *extra* notifications, not only missing or
    /// reordered ones. This makes the "keyspace notifications consistent with
    /// the chosen serialization order" half of scenario S8 observable (design
    /// doc S8 note).
    #[cfg(any(test, feature = "shard-driver"))]
    #[doc(hidden)]
    #[allow(dead_code)]
    pub fn drive_capture_keyspace(
        &mut self,
        patterns: Vec<Bytes>,
        conn_id: u64,
        flags: u32,
    ) -> crate::pubsub::PubSubReceiver {
        self.set_notify_keyspace_events(std::sync::Arc::new(std::sync::atomic::AtomicU32::new(
            flags,
        )));
        let (tx, rx) = crate::pubsub::PubSubSender::unbounded();
        for pat in patterns {
            self.subscriptions.psubscribe(pat, conn_id, tx.clone());
        }
        rx
    }
}

#[cfg(test)]
mod effect_tests {
    use std::collections::HashMap;
    use std::sync::atomic::{AtomicU32, AtomicU64};
    use std::sync::{Arc, Mutex};

    use bytes::Bytes;
    use tokio::sync::mpsc;

    use super::ExpiryResult;
    use crate::command::{Arity, Command, CommandContext, CommandFlags, WaiterWake, WalStrategy};
    use crate::command_spec::{
        AccessSpec, CommandSpec, EventSpec, KeySpec, LookupSpec, ReindexSpec,
    };
    use crate::eviction::EvictionConfig;
    use crate::keyspace_event::KeyspaceEventFlags;
    use crate::noop::MetricsRecorder;
    use crate::pubsub::{PubSubMessage, PubSubReceiver, PubSubSender};
    use crate::registry::CommandRegistry;
    use crate::replication::NoopBroadcaster;
    use crate::shard::ShardWorker;
    use crate::shard::message::{Envelope, ShardReceiver};
    use frogdb_protocol::Response;

    /// Minimal `DEL` stand-in: active expiry reconstructs each removal as a
    /// synthetic `DEL` through the write-effect pipeline, which resolves the
    /// handler from the registry and reads its spec (`KeySpec::All`,
    /// `WalStrategy::DeleteKeys`, `ReindexSpec::DeleteKeys`, `WaiterWake::All`).
    /// Its `execute` is never called (the store removal already happened), so it
    /// is a stub. The keyspace-event class is overridden per removal reason, so
    /// `EventSpec` here is irrelevant.
    struct MockDel;
    impl Command for MockDel {
        fn spec(&self) -> &'static CommandSpec {
            static SPEC: CommandSpec = CommandSpec {
                name: "DEL",
                docs: crate::command_spec::CommandDocs {
                    summary: "Deletes one or more keys.",
                    since: "1.0.0",
                    group: "generic",
                    complexity: Some(
                        "O(N) where N is the number of keys that will be removed. When a key to remove holds a value other than a string, the individual complexity for this key is O(M) where M is the number of elements in the list, set, sorted set or hash. Removing a single key that holds a string value is O(1).",
                    ),
                },
                arity: Arity::AtLeast(1),
                flags: CommandFlags::WRITE,
                keys: KeySpec::All,
                access: AccessSpec::Uniform,
                wal: WalStrategy::DeleteKeys,
                wakes: WaiterWake::All,
                event: EventSpec::Emits {
                    class: KeyspaceEventFlags::GENERIC,
                    name: "del",
                },
                requires_same_slot: false,
                reindex: ReindexSpec::DeleteKeys,
                lookup: LookupSpec::None,
                mutation: crate::command::ConnMutation::None,
                strategy: crate::command::ExecutionStrategy::Standard,
            };
            &SPEC
        }
        fn execute(
            &self,
            _ctx: &mut CommandContext,
            _args: &[Bytes],
        ) -> Result<Response, frogdb_types::CommandError> {
            Ok(Response::ok())
        }
    }

    /// A registry carrying the `DEL` handler the internal-removal pipeline needs.
    fn registry_with_del() -> CommandRegistry {
        let mut reg = CommandRegistry::new();
        reg.register(MockDel);
        reg
    }

    /// Records counter increments so tests can read cumulative totals back.
    ///
    /// `counters` aggregates across label sets; `labeled` keeps the per-label-set
    /// breakdown, which is what a `{reason=...}`-style contract has to be pinned
    /// against (a counter that moves under the wrong label is a wrong answer, not
    /// a missing one).
    #[derive(Default)]
    struct RecordingRecorder {
        counters: Mutex<HashMap<String, u64>>,
        labeled: Mutex<HashMap<String, u64>>,
    }

    impl RecordingRecorder {
        /// Cumulative value of `name` restricted to one label value, e.g.
        /// `labeled_value("frogdb_transactions_watch_aborted_total", "reason", "expiry")`.
        fn labeled_value(&self, name: &str, label: &str, value: &str) -> Option<u64> {
            self.labeled
                .lock()
                .unwrap()
                .get(&format!("{name}{{{label}={value}}}"))
                .copied()
        }
    }

    impl MetricsRecorder for RecordingRecorder {
        fn increment_counter(&self, name: &str, value: u64, labels: &[(&str, &str)]) {
            for (label, label_value) in labels {
                *self
                    .labeled
                    .lock()
                    .unwrap()
                    .entry(format!("{name}{{{label}={label_value}}}"))
                    .or_insert(0) += value;
            }
            *self
                .counters
                .lock()
                .unwrap()
                .entry(name.to_string())
                .or_insert(0) += value;
        }
        fn record_gauge(&self, _name: &str, _value: f64, _labels: &[(&str, &str)]) {}
        fn record_histogram(&self, _name: &str, _value: f64, _labels: &[(&str, &str)]) {}
        fn counter_value(&self, name: &str) -> Option<u64> {
            self.counters.lock().unwrap().get(name).copied()
        }
    }

    /// Build a bare in-memory shard worker (no persistence) using the given
    /// metrics recorder. Holds the channel send-halves alive so the receivers
    /// stay open for the worker's lifetime.
    fn build_worker(
        recorder: Arc<dyn MetricsRecorder>,
    ) -> (
        ShardWorker,
        mpsc::Sender<Envelope>,
        mpsc::Sender<crate::shard::NewConnection>,
    ) {
        let (msg_tx, msg_rx) = mpsc::channel::<Envelope>(8);
        let (conn_tx, conn_rx) = mpsc::channel::<crate::shard::NewConnection>(8);
        let worker = ShardWorker::with_eviction(
            0,
            1,
            ShardReceiver::new(msg_rx),
            conn_rx,
            Arc::new(vec![]),
            Arc::new(registry_with_del()),
            EvictionConfig::default(),
            recorder,
            Arc::new(AtomicU64::new(0)),
            Arc::new(NoopBroadcaster),
        );
        (worker, msg_tx, conn_tx)
    }

    /// Enable keyspace notifications (keyspace + keyevent, generic + expired
    /// classes) and subscribe `rx` to the given key-event channels.
    fn enable_notifications_and_subscribe(
        worker: &mut ShardWorker,
        event_channels: &[&str],
    ) -> PubSubReceiver {
        let flags = KeyspaceEventFlags::KEYSPACE
            | KeyspaceEventFlags::KEYEVENT
            | KeyspaceEventFlags::GENERIC
            | KeyspaceEventFlags::EXPIRED;
        worker.set_notify_keyspace_events(Arc::new(AtomicU32::new(flags.bits())));

        let (tx, rx) = PubSubSender::unbounded();
        for ch in event_channels {
            worker
                .subscriptions
                .subscribe(Bytes::from(ch.to_string()), 1, tx.clone());
        }
        rx
    }

    /// Collect all currently-queued (channel, payload) pairs.
    fn drain(rx: &mut PubSubReceiver) -> Vec<(String, String)> {
        let mut out = Vec::new();
        while let Ok(msg) = rx.try_recv() {
            if let PubSubMessage::Message { channel, payload } = msg {
                out.push((
                    String::from_utf8_lossy(&channel).into_owned(),
                    String::from_utf8_lossy(&payload).into_owned(),
                ));
            }
        }
        out
    }

    #[tokio::test]
    async fn notifications_fired_for_both_deletion_paths() {
        let recorder = Arc::new(RecordingRecorder::default());
        let (mut worker, _msg_tx, _conn_tx) = build_worker(recorder);
        let mut rx = enable_notifications_and_subscribe(
            &mut worker,
            &["__keyevent@0__:expired", "__keyevent@0__:del"],
        );

        let result = ExpiryResult {
            deleted_keys: vec![Bytes::from("plain")],
            emptied_keys: vec![Bytes::from("h")],
            fields_expired: 1,
            field_shrunk_keys: vec![],
            budget_exhausted: false,
        };
        worker.apply_expiry_effects(result).await;

        let events = drain(&mut rx);
        // Key-level TTL key -> `expired`; field-emptied key -> `del`.
        assert!(
            events.contains(&("__keyevent@0__:expired".into(), "plain".into())),
            "expected `expired` event for key-level expiry, got {events:?}"
        );
        assert!(
            events.contains(&("__keyevent@0__:del".into(), "h".into())),
            "expected `del` event for field-emptied key, got {events:?}"
        );
    }

    #[tokio::test]
    async fn expired_keys_stat_counts_both_paths_without_double_count() {
        let recorder = Arc::new(RecordingRecorder::default());
        let (mut worker, _msg_tx, _conn_tx) = build_worker(recorder.clone());

        let result = ExpiryResult {
            // 2 key-level + 1 field-emptied = 3 keys removed.
            deleted_keys: vec![Bytes::from("a"), Bytes::from("b")],
            emptied_keys: vec![Bytes::from("h")],
            fields_expired: 4,
            field_shrunk_keys: vec![],
            budget_exhausted: false,
        };
        worker.apply_expiry_effects(result).await;

        // Key counter: 3 keys (both paths), counted once each.
        assert_eq!(recorder.counter_value("frogdb_keys_expired_total"), Some(3));
        assert_eq!(worker.store.expired_keys(), 3);
        // Field counter: independent unit, no overlap with the key counter.
        assert_eq!(
            recorder.counter_value("frogdb_fields_expired_total"),
            Some(4)
        );
    }

    /// Slot-granular version bump (proposal 18): a single active-expiry cycle
    /// that removes BOTH a whole-key-expired key (`Expired`) and a hash-emptied
    /// key (`FieldEmptied`) bumps each removed key's slot exactly ONCE — the two
    /// reason groups are driven through one `run_internal_removal_effects` call,
    /// so no slot is double-bumped. Each removed key's effective version is
    /// therefore exactly 1 (a double slot-bump would read 2, catching the
    /// regression the "exactly once" invariant guards). The cycle also carries
    /// `fields_expired > 0` — the field that emptied `h` — and that must add
    /// NOTHING here: the emptied key already bumped its own slot through the
    /// pipeline, and no hash survived a shrink, so there is nothing left for the
    /// shard-wide epoch to compensate for. Also pins the dirty counter
    /// (previously skipped by the hand-rolled expiry path): it advances by the
    /// removed keys.
    // FM-TXN-033
    #[tokio::test]
    async fn expiry_coalesces_version_bump_and_advances_dirty() {
        use crate::store::Store;

        let recorder = Arc::new(RecordingRecorder::default());
        let (mut worker, _msg_tx, _conn_tx) = build_worker(recorder);
        assert_eq!(worker.get_key_version(b"plain"), 0);
        assert_eq!(worker.get_key_version(b"h"), 0);
        assert_eq!(worker.store.dirty(), 0);

        let result = ExpiryResult {
            deleted_keys: vec![Bytes::from("plain")],
            emptied_keys: vec![Bytes::from("h")],
            fields_expired: 1,
            field_shrunk_keys: vec![],
            budget_exhausted: false,
        };
        worker.apply_expiry_effects(result).await;

        assert_eq!(
            worker.get_key_version(b"plain"),
            1,
            "the whole-key-expired key's slot bumps exactly once, and the cycle's \
             field expiry adds no shard-wide epoch bump on top"
        );
        assert_eq!(
            worker.get_key_version(b"h"),
            1,
            "the field-emptied key's slot bumps exactly once (through the removal \
             pipeline), not once per reason group"
        );
        assert_eq!(
            worker.store.dirty(),
            2,
            "dirty counter advances by the two removed keys (was skipped before)"
        );
    }

    /// Regression (whole-branch review, amended by spec-gaps issue 23): a single
    /// active-expiry cycle that BOTH removes a whole key AND field-shrinks a
    /// *surviving* watched hash must still invalidate a watch on that hash. The
    /// survivor is a mutation, not a removal, so it never flows through the
    /// removal pipeline — the sweep reports it separately in `field_shrunk_keys`
    /// and the effects bump ITS slot. That bump must NOT be gated on "no key was
    /// removed": when the same cycle also removes a key, the watched survivor
    /// would otherwise commit against a concurrently-mutated value (an
    /// optimistic-lock false negative). Pins the exact scenario:
    /// `deleted_keys=[del]` + `field_shrunk_keys=[surv]` on a DIFFERENT slot ⇒ a
    /// WATCH on `surv` (live, version-snapshotted pre-cycle) must ABORT, while a
    /// watch on a THIRD, untouched slot must survive — the bump is per-slot, not
    /// shard-wide.
    // FM-TXN-033
    #[tokio::test]
    async fn field_expiry_bumps_only_the_shrunk_survivors_slot() {
        use crate::shard::message::WatchEntry;
        use crate::shard::partition::slot_for_key;

        let recorder = Arc::new(RecordingRecorder::default());
        let (mut worker, _msg_tx, _conn_tx) = build_worker(recorder);

        // Three distinct slots: the removed key, the shrunk survivor, and a
        // bystander nobody touched. Distinct slots are what let each assertion
        // below attribute a bump to exactly one cause.
        let del = "del";
        let surv = "surv";
        let bystander = "bystander";
        assert_ne!(
            slot_for_key(del.as_bytes()),
            slot_for_key(surv.as_bytes()),
            "test precondition: removed key and survivor hash must be on distinct slots"
        );
        assert_ne!(
            slot_for_key(bystander.as_bytes()),
            slot_for_key(surv.as_bytes()),
            "test precondition: bystander must not share the survivor's slot"
        );
        assert_ne!(
            slot_for_key(bystander.as_bytes()),
            slot_for_key(del.as_bytes()),
            "test precondition: bystander must not share the removed key's slot"
        );

        // Seed both watched keys live and non-empty so `exists_unexpired` holds —
        // each watch can then only move via the version compare, isolating the
        // slot-bump path (not the `live_at_watch` liveness clause).
        seed_hash_with_mixed_fields(&mut worker.store, surv, &[], &["f1"]);
        seed_hash_with_mixed_fields(&mut worker.store, bystander, &[], &["f1"]);
        let surv_v0 = worker.get_key_version(surv.as_bytes());
        let bystander_v0 = worker.get_key_version(bystander.as_bytes());

        // One cycle: reaps whole key `del` AND field-purges a field of `surv`
        // (surv survives — absent from deleted_keys/emptied_keys, reported on
        // `field_shrunk_keys`).
        let result = ExpiryResult {
            deleted_keys: vec![Bytes::from(del)],
            emptied_keys: vec![],
            fields_expired: 1,
            field_shrunk_keys: vec![Bytes::from(surv)],
            budget_exhausted: false,
        };
        worker.apply_expiry_effects(result).await;

        // The survivor's version moved, so a live watch on it aborts.
        assert_ne!(
            worker.get_key_version(surv.as_bytes()),
            surv_v0,
            "a field-shrunk survivor's own slot must be bumped even when the same \
             cycle also removed a whole key"
        );
        let watches = [WatchEntry {
            key: Bytes::from(surv),
            version: surv_v0,
            live_at_watch: true,
        }];
        assert!(
            !worker.check_watches(&watches),
            "WATCH on a field-shrunk surviving hash must ABORT EXEC even when the \
             same expiry cycle also removed a whole key (optimistic-lock invariant)"
        );

        // ...and the bystander's did not: no shard-wide epoch bump escaped.
        assert_eq!(
            worker.get_key_version(bystander.as_bytes()),
            bystander_v0,
            "a slot the cycle never touched must not move — a shard-wide bump here \
             is the starvation bug (FM-TXN-033)"
        );
        let bystander_watch = [WatchEntry {
            key: Bytes::from(bystander),
            version: bystander_v0,
            live_at_watch: true,
        }];
        assert!(
            worker.check_watches(&bystander_watch),
            "a WATCH on an untouched slot must survive a field-expiry cycle"
        );
    }

    /// Starvation regression (spec-gaps issue 23 / distsys-review MAJ-21), driven
    /// through the REAL sweep rather than a hand-built `ExpiryResult`: a hash
    /// whose field TTLs fire on every cycle must not invalidate a watch on an
    /// unrelated key. Pre-fix, `fields_expired > 0` bumped the shard-wide epoch,
    /// so `victim`'s watch aborted on every cycle forever — an unbounded liveness
    /// violation with no error and no metric. Post-fix the sweep enumerates the
    /// hash it shrank and bumps only that slot, so the unrelated watch holds
    /// across repeated cycles.
    // FM-TXN-033
    #[tokio::test]
    async fn field_expiry_does_not_starve_a_watch_on_an_unrelated_slot() {
        use crate::shard::message::WatchEntry;
        use crate::shard::partition::slot_for_key;
        use crate::store::Store;

        let recorder = Arc::new(RecordingRecorder::default());
        let (mut worker, _msg_tx, _conn_tx) = build_worker(recorder);

        let noisy = "noisy";
        let victim = "victim";
        assert_ne!(
            slot_for_key(noisy.as_bytes()),
            slot_for_key(victim.as_bytes()),
            "test precondition: the noisy hash and the watched key must be on distinct slots"
        );

        // `victim` is the unrelated CAS target; `noisy` is a hash with an expired
        // field and a live one, so each sweep shrinks it without removing it.
        seed_hash_with_mixed_fields(&mut worker.store, victim, &[], &["f1"]);
        let victim_v0 = worker.get_key_version(victim.as_bytes());
        let watches = [WatchEntry {
            key: Bytes::from(victim),
            version: victim_v0,
            live_at_watch: true,
        }];

        // Several cycles, each reaping a field from a fresh batch on `noisy`: the
        // starvation bug reproduces on the FIRST one and compounds, so a loop
        // pins "forever", not just "once".
        for round in 0..3 {
            let expired = format!("e{round}");
            seed_hash_with_mixed_fields(&mut worker.store, noisy, &[expired.as_str()], &["live"]);
            worker.run_active_expiry().await;
            assert!(
                worker.store.contains(noisy.as_bytes()),
                "precondition: the noisy hash must SURVIVE the sweep (shrunk, not removed)"
            );
            assert!(
                worker.check_watches(&watches),
                "round {round}: a field-expiry sweep on an unrelated slot must not \
                 abort this WATCH — a shard-wide epoch bump here starves every CAS \
                 loop on the shard (FM-TXN-033)"
            );
        }
    }

    /// Safety half of the starvation fix: narrowing the bump to the shrunk keys'
    /// slots must not lose the abort a watcher of that slot is owed. Same real
    /// sweep, but the watch is on the hash being shrunk — its `EXEC` must still
    /// be refused.
    // FM-TXN-033
    #[tokio::test]
    async fn field_expiry_still_aborts_a_watch_on_the_shrunk_slot() {
        use crate::shard::message::WatchEntry;
        use crate::store::Store;

        let recorder = Arc::new(RecordingRecorder::default());
        let (mut worker, _msg_tx, _conn_tx) = build_worker(recorder);

        // One expired field, one live field: the sweep shrinks `h` and leaves it
        // alive, so the abort can only come from the survivor's own slot bump.
        seed_hash_with_mixed_fields(&mut worker.store, "h", &["gone"], &["stays"]);
        let v0 = worker.get_key_version(b"h");

        worker.run_active_expiry().await;

        assert!(
            worker.store.contains(b"h"),
            "precondition: the hash must survive the sweep (shrunk, not emptied)"
        );
        let watches = [WatchEntry {
            key: Bytes::from_static(b"h"),
            version: v0,
            live_at_watch: true,
        }];
        assert!(
            !worker.check_watches(&watches),
            "a WATCH on a hash the sweep field-shrunk must still ABORT — the \
             per-slot bump replaces the shard-wide one, it does not drop it"
        );
    }

    /// A WATCH abort must name its cause. Class 1: the watched key's slot version
    /// moved, so `EXEC` is refused with `reason="watched-slot-write"`. Without
    /// this counter a CAS loop that never commits is silent — the exact failure
    /// mode the field-expiry starvation bug hid behind (`specs/txn.md`
    /// FM-TXN-033).
    // FM-TXN-033
    #[tokio::test]
    async fn watch_abort_records_the_slot_write_reason() {
        use crate::shard::message::WatchEntry;
        use crate::shard::types::TransactionResult;
        use crate::store::Store;
        use crate::types::Value;
        use frogdb_protocol::ProtocolVersion;

        let recorder = Arc::new(RecordingRecorder::default());
        let (mut worker, _msg_tx, _conn_tx) = build_worker(recorder.clone());

        worker.store.set(
            Bytes::from_static(b"k"),
            Value::string(Bytes::from_static(b"v")),
        );
        let v0 = worker.get_key_version(b"k");
        // Something wrote into k's slot after the watch was taken.
        worker.bump_versions_for([b"k".as_slice()]);

        // Empty command list: the watch check is the whole transaction, so the
        // reason attribution is isolated from anything a queued command does.
        let result = worker
            .execute_transaction(
                vec![],
                &[WatchEntry {
                    key: Bytes::from_static(b"k"),
                    version: v0,
                    live_at_watch: true,
                }],
                1,
                ProtocolVersion::Resp2,
                &crate::write_seam::WriteAdmission::internal(),
                None,
            )
            .await;

        assert!(
            matches!(result, TransactionResult::WatchAborted),
            "a moved slot version must abort the transaction"
        );
        assert_eq!(
            recorder.labeled_value(
                "frogdb_transactions_watch_aborted_total",
                "reason",
                "watched-slot-write"
            ),
            Some(1),
            "the abort must be counted under the slot-write reason"
        );
        assert_eq!(
            recorder.labeled_value(
                "frogdb_transactions_watch_aborted_total",
                "reason",
                "expiry"
            ),
            None,
            "and must not be misattributed to expiry"
        );
    }

    /// Class 2 of the same contract: the watched key was live at `WATCH` time and
    /// is gone at `EXEC` with no version bump for this watcher (here: an elapsed
    /// TTL whose physical purge is suppressed, so nothing bumps). That abort is
    /// counted under `reason="expiry"` — a different operator response from a
    /// contended slot, so it must not collapse into the same bucket.
    // FM-TXN-033
    #[tokio::test]
    async fn watch_abort_records_the_expiry_reason() {
        use crate::shard::message::WatchEntry;
        use crate::shard::types::TransactionResult;
        use crate::store::Store;
        use crate::types::Value;
        use frogdb_protocol::ProtocolVersion;

        let recorder = Arc::new(RecordingRecorder::default());
        let (mut worker, _msg_tx, _conn_tx) = build_worker(recorder.clone());

        worker.store.set(
            Bytes::from_static(b"k"),
            Value::string(Bytes::from_static(b"v")),
        );
        let v0 = worker.get_key_version(b"k");
        // Elapsed TTL + suppressed purge: logically dead, physically present, no
        // version bump — so only the liveness clause can refuse this EXEC.
        worker.store.set_expiry(
            b"k",
            std::time::Instant::now() - std::time::Duration::from_secs(60),
        );
        worker.store.set_expiry_suppressed(true);

        let result = worker
            .execute_transaction(
                vec![],
                &[WatchEntry {
                    key: Bytes::from_static(b"k"),
                    version: v0,
                    live_at_watch: true,
                }],
                1,
                ProtocolVersion::Resp2,
                &crate::write_seam::WriteAdmission::internal(),
                None,
            )
            .await;

        assert!(
            matches!(result, TransactionResult::WatchAborted),
            "a watched key that died under the watcher must abort the transaction"
        );
        assert_eq!(
            recorder.labeled_value(
                "frogdb_transactions_watch_aborted_total",
                "reason",
                "expiry"
            ),
            Some(1),
            "the abort must be counted under the expiry reason"
        );
        assert_eq!(
            recorder.labeled_value(
                "frogdb_transactions_watch_aborted_total",
                "reason",
                "watched-slot-write"
            ),
            None,
            "and must not be misattributed to a slot write (no version moved)"
        );
    }

    #[tokio::test]
    async fn empty_result_records_nothing() {
        let recorder = Arc::new(RecordingRecorder::default());
        let (mut worker, _msg_tx, _conn_tx) = build_worker(recorder.clone());

        worker.apply_expiry_effects(ExpiryResult::default()).await;

        assert_eq!(recorder.counter_value("frogdb_keys_expired_total"), None);
        assert_eq!(recorder.counter_value("frogdb_fields_expired_total"), None);
        assert_eq!(worker.store.expired_keys(), 0);
    }

    /// Seed `store` with a single-field hash whose only field is already past
    /// its field TTL (both on the value and in the field-expiry index) — a
    /// last-field-death waiting to happen on the next read.
    fn seed_expiring_single_field_hash(store: &mut crate::store::HashMapStore, key: &str) {
        use crate::store::Store;
        use crate::types::{HashValue, ListpackThresholds, Value};
        let past = std::time::Instant::now() - std::time::Duration::from_secs(60);
        let mut hash = HashValue::new();
        hash.set(
            Bytes::from_static(b"f"),
            Bytes::from_static(b"v"),
            ListpackThresholds::DEFAULT_HASH,
        );
        hash.set_field_expiry(b"f", past);
        store.set(Bytes::from(key.to_string()), Value::Hash(hash));
        store.set_field_expiry(key.as_bytes(), b"f", past);
    }

    /// Seed `store` with a hash carrying `expired` already-past-TTL fields and
    /// `live` fields with no TTL — a lazy read reaps only the expired ones and
    /// leaves the key non-empty.
    fn seed_hash_with_mixed_fields(
        store: &mut crate::store::HashMapStore,
        key: &str,
        expired: &[&str],
        live: &[&str],
    ) {
        use crate::store::Store;
        use crate::types::{HashValue, ListpackThresholds, Value};
        let past = std::time::Instant::now() - std::time::Duration::from_secs(60);
        let mut hash = HashValue::new();
        for f in expired.iter().chain(live.iter()) {
            hash.set(
                Bytes::from((*f).to_string()),
                Bytes::from_static(b"v"),
                ListpackThresholds::DEFAULT_HASH,
            );
        }
        for f in expired {
            hash.set_field_expiry(f.as_bytes(), past);
        }
        store.set(Bytes::from(key.to_string()), Value::Hash(hash));
        for f in expired {
            store.set_field_expiry(key.as_bytes(), f.as_bytes(), past);
        }
    }

    /// Lazy last-hash-field death routed through the lazy-purge drain must fire
    /// a generic `del` keyevent (not `expired`), mirroring active expiry's
    /// `emptied_keys` branch. Pins the worker seam in isolation: seed a hash
    /// whose only field is expired, purge it (the lazy-read seam empties the key
    /// and records it in the store's lazily-emptied buffer), then drain — the
    /// `del` event fires and the buffer is emptied.
    #[test]
    fn lazy_emptied_hash_key_drains_del_event() {
        use crate::store::Store;

        let recorder = Arc::new(RecordingRecorder::default());
        let (mut worker, _msg_tx, _conn_tx) = build_worker(recorder.clone());
        let mut rx = enable_notifications_and_subscribe(
            &mut worker,
            &["__keyevent@0__:del", "__keyevent@0__:expired"],
        );

        seed_expiring_single_field_hash(&mut worker.store, "h");

        // Lazy-read seam: purge the expired field, which empties and removes the
        // key, recording it in the store's lazily-emptied buffer.
        assert_eq!(worker.store.purge_expired_hash_fields(b"h"), 1);
        assert!(!worker.store.contains(b"h"), "key must be physically gone");

        // Drain the lazy-purge report at the command seam.
        worker.apply_lazy_purge_effects();

        let events = drain(&mut rx);
        assert!(
            events.contains(&("__keyevent@0__:del".into(), "h".into())),
            "lazy last-field death must emit a `del` keyevent, got {events:?}"
        );
        assert!(
            !events.iter().any(|(ch, _)| ch == "__keyevent@0__:expired"),
            "lazy hash-empty must emit `del`, never `expired`, got {events:?}"
        );

        // Metric parity with the active sweep's `emptied_keys` branch: the
        // emptied key counts as one key expiration on BOTH the INFO stat and the
        // Prometheus counter, and its final field counts once toward the field
        // counter.
        assert_eq!(worker.store.expired_keys(), 1, "INFO expired_keys");
        assert_eq!(
            recorder.counter_value("frogdb_keys_expired_total"),
            Some(1),
            "KeysExpired metric"
        );
        assert_eq!(
            recorder.counter_value("frogdb_fields_expired_total"),
            Some(1),
            "FieldsExpired metric (the emptying field)"
        );

        // Buffers drained — nothing leaks to the next command.
        assert!(worker.store.take_lazily_emptied().is_empty());
        assert_eq!(worker.store.take_lazily_expired_fields(), 0);
    }

    /// Lazy field reap that does NOT empty the key still bumps the FieldsExpired
    /// metric with per-field parity to the active sweep — and removes no key, so
    /// no `del`/`expired` event and no key-counter bump. This is the surface
    /// (worker.rs:597-611 review finding 2) that would otherwise silently
    /// under-count lazily-reaped fields.
    #[test]
    fn lazy_field_reap_without_emptying_counts_fields_only() {
        use crate::store::Store;

        let recorder = Arc::new(RecordingRecorder::default());
        let (mut worker, _msg_tx, _conn_tx) = build_worker(recorder.clone());
        let mut rx = enable_notifications_and_subscribe(
            &mut worker,
            &["__keyevent@0__:del", "__keyevent@0__:expired"],
        );

        // Two expired fields + one live field: the reap shrinks but does not
        // empty the hash.
        seed_hash_with_mixed_fields(&mut worker.store, "h", &["a", "b"], &["c"]);

        assert_eq!(worker.store.purge_expired_hash_fields(b"h"), 2);
        assert!(
            worker.store.contains(b"h"),
            "key must survive (still has `c`)"
        );

        worker.apply_lazy_purge_effects();

        // No key removed → no keyspace event, no key-counter bump.
        assert!(
            drain(&mut rx).is_empty(),
            "a field reap that does not empty the key emits no keyspace event"
        );
        assert_eq!(worker.store.expired_keys(), 0);
        assert_eq!(recorder.counter_value("frogdb_keys_expired_total"), None);
        // Two fields reaped → FieldsExpired == 2.
        assert_eq!(
            recorder.counter_value("frogdb_fields_expired_total"),
            Some(2),
            "both reaped fields counted"
        );
        assert_eq!(
            worker.store.take_lazily_expired_fields(),
            0,
            "counter drained"
        );
    }

    /// No double-fire: when the *active sweep* reaps a last-field-death it both
    /// reports the key via `ExpiryResult::emptied_keys` (→ one `del` from
    /// `apply_expiry_effects`) AND populates the store's lazily-emptied buffer
    /// through the shared `purge_expired_hash_fields`. `run_active_expiry`
    /// discards that buffer, so a subsequent command-seam drain fires nothing —
    /// exactly one `del` total.
    #[tokio::test]
    async fn active_sweep_emptied_key_does_not_double_fire_del() {
        use crate::store::Store;

        let recorder = Arc::new(RecordingRecorder::default());
        let (mut worker, _msg_tx, _conn_tx) = build_worker(recorder.clone());
        let mut rx = enable_notifications_and_subscribe(&mut worker, &["__keyevent@0__:del"]);

        seed_expiring_single_field_hash(&mut worker.store, "h");

        // Active sweep reaps the last field, empties the key, and reports it.
        worker.run_active_expiry().await;

        let after_sweep = drain(&mut rx);
        assert_eq!(
            after_sweep,
            vec![("__keyevent@0__:del".to_string(), "h".to_string())],
            "active sweep must emit exactly one `del`, got {after_sweep:?}"
        );

        // A later command-seam drain must find nothing — the sweep discarded the
        // lazily-emptied buffer and field counter, so no second `del`.
        worker.apply_lazy_purge_effects();
        assert!(
            drain(&mut rx).is_empty(),
            "no second `del` may fire for a key the sweep already reported"
        );
        assert!(worker.store.take_lazily_emptied().is_empty());

        // And no double-count: the sweep counted the key and its field exactly
        // once (via apply_expiry_effects), the discarded buffers added nothing.
        assert_eq!(worker.store.expired_keys(), 1, "key counted once");
        assert_eq!(
            recorder.counter_value("frogdb_keys_expired_total"),
            Some(1),
            "KeysExpired counted once"
        );
        assert_eq!(
            recorder.counter_value("frogdb_fields_expired_total"),
            Some(1),
            "FieldsExpired counted once"
        );
    }

    /// TR-BLOCKING-020's precondition is *gated*: the 100 ms waiter-timeout
    /// branch only exists while `timer_sweeps_enabled()` holds, and a driven
    /// run turns it off and delivers the sweep as a `DriveTick(WaiterTimeout)`
    /// message instead (determinism audit R6). Reading the row without the
    /// gate, a maintainer concludes the GC backstop is always armed — and
    /// every deterministic/turmoil run is a counterexample.
    ///
    /// This is also the row's isolation the coordinator can never give: no
    /// `BlockingWaitCoordinator` exists here, so the sweep is the *only*
    /// authority that can resolve the expired waiter. Both halves of the
    /// postcondition are pinned — the op-aware `timeout_reply()` on
    /// `response_tx` and the `BlockedTimeoutTotal` increment — plus the entry
    /// leaving the queue.
    #[tokio::test]
    async fn a_driven_run_gates_off_the_waiter_sweep_timer_and_sweeps_from_the_drive_tick() {
        use crate::shard::message::{ShardMessage, TickKind};
        use crate::shard::wait_queue::WaitEntry;
        use crate::types::BlockingOp;
        use frogdb_protocol::ProtocolVersion;
        use tokio::sync::oneshot;
        use tokio::time::{Duration, Instant};

        let recorder = Arc::new(RecordingRecorder::default());
        let (mut worker, _msg_tx, _conn_tx) = build_worker(recorder.clone());

        // Production: the timer branch is live. Driven: it is gated off, and
        // whoever set the flag owes the shard a `DriveTick` instead.
        assert!(
            worker.timer_sweeps_enabled(),
            "an undriven worker keeps the 100 ms waiter-timeout branch armed"
        );
        worker.set_driven_ticks(true);
        assert!(
            !worker.timer_sweeps_enabled(),
            "a driven run must suppress the timer branch TR-BLOCKING-020 names"
        );

        let (response_tx, response_rx) = oneshot::channel();
        worker
            .wait_queue
            .register(WaitEntry {
                conn_id: 7,
                keys: vec![Bytes::from_static(b"k")],
                op: BlockingOp::BLPop,
                response_tx,
                // Already elapsed, so only a sweep can resolve it.
                deadline: Some(Instant::now() - Duration::from_millis(1)),
                protocol_version: ProtocolVersion::default(),
            })
            .expect("registration is within the queue's bounds");
        assert_eq!(worker.wait_queue.waiter_count(), 1);

        // The suppressed timer never fires; the queued tick is what runs.
        assert!(
            !worker
                .dispatch_message(ShardMessage::DriveTick(TickKind::WaiterTimeout))
                .await,
            "a drive tick must never signal shutdown"
        );

        assert_eq!(
            worker.wait_queue.waiter_count(),
            0,
            "the driven tick must reclaim the expired entry"
        );
        assert_eq!(
            response_rx.await,
            Ok(Response::NullArray),
            "the GC sweep answers with BLPOP's op-aware timeout reply, never a drop"
        );
        assert_eq!(
            recorder.counter_value("frogdb_blocked_timeout_total"),
            Some(1),
            "the sweep counts the timeout it resolved"
        );
    }
}

#[cfg(test)]
mod seam_reachability_tests {
    use std::sync::Arc;

    use bytes::Bytes;
    use frogdb_protocol::{ParsedCommand, ProtocolVersion, Response};
    use tokio::sync::mpsc;
    use tokio::sync::oneshot;

    use crate::command::{Arity, Command, CommandContext, CommandFlags, WaiterWake, WalStrategy};
    use crate::command_spec::{AccessSpec, CommandSpec, EventSpec, KeySpec, LookupSpec};
    use crate::keyspace_event::KeyspaceEventFlags;
    use crate::registry::CommandRegistry;
    use crate::shard::builder::ShardWorkerBuilder;
    use crate::shard::connection::NewConnection;
    use crate::shard::message::{CoreMsg, Envelope, ShardReceiver, ShardSender};
    use crate::shard::worker::ShardWorker;
    use crate::types::Value;

    /// Minimal in-crate `SET`: `frogdb-core` has no real command
    /// implementations of its own (those live downstream in `frogdb-commands`,
    /// which depends on `frogdb-core` and so cannot be pulled in here without a
    /// cycle) — this stand-in exercises the same `dispatch_message` ->
    /// `dispatch_core` -> `execute_command` -> registry lookup path a real
    /// command would.
    struct MockSet;
    impl Command for MockSet {
        fn spec(&self) -> &'static CommandSpec {
            static SPEC: CommandSpec = CommandSpec {
                name: "SET",
                docs: crate::command_spec::CommandDocs {
                    summary: "Sets the string value of a key, ignoring its type. The key is created if it doesn't exist.",
                    since: "1.0.0",
                    group: "string",
                    complexity: Some("O(1)"),
                },
                arity: Arity::AtLeast(2),
                flags: CommandFlags::WRITE,
                keys: KeySpec::First,
                access: AccessSpec::Uniform,
                wal: WalStrategy::PersistFirstKey,
                wakes: WaiterWake::All,
                event: EventSpec::Emits {
                    class: KeyspaceEventFlags::STRING,
                    name: "set",
                },
                requires_same_slot: false,
                reindex: crate::command_spec::ReindexSpec::None,
                lookup: LookupSpec::None,
                mutation: crate::command::ConnMutation::None,
                strategy: crate::command::ExecutionStrategy::Standard,
            };
            &SPEC
        }

        fn execute(
            &self,
            ctx: &mut CommandContext,
            args: &[Bytes],
        ) -> Result<Response, frogdb_types::CommandError> {
            ctx.store
                .set(args[0].clone(), Value::string(args[1].clone()));
            Ok(Response::ok())
        }
    }

    /// Minimal in-crate `GET` counterpart to [`MockSet`] — same rationale.
    struct MockGet;
    impl Command for MockGet {
        fn spec(&self) -> &'static CommandSpec {
            static SPEC: CommandSpec = CommandSpec {
                name: "GET",
                docs: crate::command_spec::CommandDocs {
                    summary: "Returns the string value of a key.",
                    since: "1.0.0",
                    group: "string",
                    complexity: Some("O(1)"),
                },
                arity: Arity::Fixed(1),
                flags: CommandFlags::READONLY,
                keys: KeySpec::First,
                access: AccessSpec::Uniform,
                wal: WalStrategy::NoOp,
                wakes: WaiterWake::None,
                event: EventSpec::NotApplicable,
                requires_same_slot: false,
                reindex: crate::command_spec::ReindexSpec::None,
                lookup: LookupSpec::FirstKey,
                mutation: crate::command::ConnMutation::None,
                strategy: crate::command::ExecutionStrategy::Standard,
            };
            &SPEC
        }

        fn execute(
            &self,
            ctx: &mut CommandContext,
            args: &[Bytes],
        ) -> Result<Response, frogdb_types::CommandError> {
            match ctx
                .store
                .get(&args[0])
                .and_then(|v| v.as_string().map(|s| s.as_bytes().clone()))
            {
                Some(b) => Ok(Response::bulk(b)),
                None => Ok(Response::null()),
            }
        }
    }

    fn worker() -> ShardWorker {
        let (_mtx, mrx) = mpsc::channel::<Envelope>(8);
        let (_ntx, nrx) = mpsc::channel::<NewConnection>(8);
        let (msg_tx, _msg_rx) = mpsc::channel::<Envelope>(8);
        let mut registry = CommandRegistry::new();
        registry.register(MockSet);
        registry.register(MockGet);
        ShardWorkerBuilder::new(0, 1)
            .with_message_rx(ShardReceiver::new(mrx))
            .with_new_conn_rx(nrx)
            .with_shard_senders(Arc::new(vec![ShardSender::new(msg_tx)]))
            .with_registry(Arc::new(registry))
            .build()
    }

    #[tokio::test]
    async fn promoted_seams_are_reachable_in_crate() {
        let mut w = worker();

        // `dispatch_message` (now pub(crate)) round-trips a SET then a GET.
        let (tx, rx) = oneshot::channel();
        let set = CoreMsg::Execute {
            command: Arc::new(ParsedCommand::new(
                Bytes::from_static(b"SET"),
                vec![Bytes::from_static(b"k"), Bytes::from_static(b"v")],
            )),
            conn_id: 1,
            txid: None,
            protocol_version: ProtocolVersion::Resp3,
            track_reads: false,
            no_touch: false,
            response_tx: tx,
        };
        assert!(
            !w.dispatch_message(set.into()).await,
            "SET must not signal shutdown"
        );
        assert!(matches!(rx.await.unwrap(), Response::Simple(_)));

        let (tx, rx) = oneshot::channel();
        let get = CoreMsg::Execute {
            command: Arc::new(ParsedCommand::new(
                Bytes::from_static(b"GET"),
                vec![Bytes::from_static(b"k")],
            )),
            conn_id: 1,
            txid: None,
            protocol_version: ProtocolVersion::Resp3,
            track_reads: false,
            no_touch: false,
            response_tx: tx,
        };
        w.dispatch_message(get.into()).await;
        assert_eq!(
            rx.await.unwrap(),
            Response::Bulk(Some(Bytes::from_static(b"v")))
        );

        // Tick seams (now pub(crate)) run without a timer.
        w.run_active_expiry().await;
        w.check_waiter_timeouts();
    }
}
