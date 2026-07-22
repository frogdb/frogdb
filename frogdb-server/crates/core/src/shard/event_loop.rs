use std::time::{Duration, Instant};

use frogdb_types::metrics::definitions::{FieldsExpired, KeysExpired, ShardQueueLatency};

use crate::store::Store;

use super::active_expiry::ExpiryResult;
#[cfg(any(test, feature = "shard-driver"))]
use super::message::Envelope;
use super::message::ShardMessage;
use super::post_execution::{ENGINE_INTERNAL_CONN_ID, RemovalPropagation, RemovalReason};
use super::worker::ShardWorker;
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
            tokio::select! {
                // Handle new connections
                Some(new_conn) = self.new_conn_rx.recv() => {
                    self.handle_new_connection(new_conn).await;
                }

                // Handle shard messages — dispatch to grouped sub-handlers
                Some(envelope) = self.message_rx.recv() => {
                    let queue_latency = envelope.enqueued_at.elapsed().as_secs_f64();
                    let msg = envelope.message;

                    crate::probes::fire_shard_message_received(
                        self.shard_id() as u64,
                        msg.probe_type_str(),
                        self.message_rx.len() as u64,
                    );

                    ShardQueueLatency::observe(
                        self.observability.metrics(),
                        queue_latency,
                        self.identity.shard_label(),
                    );

                    if self.dispatch_message(msg).await {
                        break;
                    }
                }

                // Active expiry task
                _ = expiry_interval.tick() => {
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

                // Periodic metrics collection
                _ = metrics_interval.tick() => {
                    self.collect_shard_metrics();
                }

                // Blocking waiter timeout check
                _ = waiter_timeout_interval.tick() => {
                    self.check_waiter_timeouts();
                }

                // Periodic search index commit
                _ = search_commit_interval.tick() => {
                    let sid = self.identity.shard_id();
                    for idx in self.search.indexes.values_mut() {
                        if idx.is_dirty() && let Err(e) = idx.commit() {
                            tracing::error!(shard_id = sid, error = %e, "Failed to commit search index");
                        }
                    }
                }

                // Check for continuation lock release signal
                _ = self.vll.await_continuation_release() => {
                    self.vll.clear_continuation_lock();
                    tracing::debug!(shard_id = self.shard_id(), "Continuation lock released");
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
        let result = self.expiry.run_cycle(&mut self.store, Instant::now());
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
        // The shrunk-survivor buffer, by contrast, is *not* owned by
        // `ExpiryResult` (the sweep counts `fields_expired` but never enumerates
        // the surviving keys), so drain it here and re-index each survivor through
        // the *same* `reindex_shrunk_hash_keys` owner the lazy read path uses.
        // This is what keeps a field-shrunk hash's search-index doc from holding
        // the reaped field's stale value after an active sweep — the search
        // analogue of the WATCH global bump below.
        let shrunk = self.store.take_lazily_shrunk();
        self.store.take_lazily_emptied();
        self.store.take_lazily_expired_fields();
        self.reindex_shrunk_hash_keys(&shrunk);
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
    pub(crate) async fn apply_expiry_effects(&mut self, result: ExpiryResult) {
        if result.is_empty() {
            return;
        }

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
        // not a removal, so it does not flow through the removal pipeline — but it
        // still changed a watched hash. The swept field-keys are NOT carried by
        // `ExpiryResult` (only a `fields_expired` count), so this is the one
        // active-expiry event whose keys the shard cannot enumerate. Bump the
        // shard-wide epoch whenever ANY field expired: a safe over-abort that
        // invalidates every watch and so never misses the watched hash whose
        // field expired (zero false negatives). This is INDEPENDENT of whether
        // the same cycle also removed whole keys — those removals bump their own
        // slots via the pipeline above, but a field-shrunk *survivor* hash is not
        // among them, so its watch would slip through if the global bump were
        // gated on `!removed_any`. `removed_any` therefore does not appear here.
        if result.fields_expired > 0 {
            self.bump_version_global();
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
            ShardMessage::Search(m) => {
                self.dispatch_search(m);
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

    /// Shard-driver harness seam: fire one blocking-waiter timeout sweep,
    /// without waiting on the event loop's 100 ms timer. Wraps
    /// [`Self::check_waiter_timeouts`].
    #[cfg(any(test, feature = "shard-driver"))]
    #[doc(hidden)]
    #[allow(dead_code)]
    pub fn drive_waiter_timeout_tick(&mut self) {
        self.check_waiter_timeouts();
    }

    /// Shard-driver harness seam mirroring the event loop's continuation-release
    /// arm (`event_loop.rs:88-91`): await the stored release signal — fired when
    /// the coordinator's `ContinuationGuard` drops — then clear the lock.
    ///
    /// Only call when a continuation lock is held and its guard has been (or is
    /// about to be) dropped; with no lock held `await_continuation_release`
    /// resolves to `pending()` and this future never completes. The shard-driver
    /// harness pumps this per shard, in a permuted order, after inducing the
    /// guard drop (scenario 4).
    #[cfg(any(test, feature = "shard-driver"))]
    #[doc(hidden)]
    #[allow(dead_code)]
    pub async fn drive_continuation_release(&mut self) {
        self.vll.await_continuation_release().await;
        self.vll.clear_continuation_lock();
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
    ) -> tokio::sync::mpsc::UnboundedReceiver<crate::pubsub::PubSubMessage> {
        self.set_notify_keyspace_events(std::sync::Arc::new(std::sync::atomic::AtomicU32::new(
            flags,
        )));
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
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
    use crate::pubsub::{PubSubMessage, PubSubSender};
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
    #[derive(Default)]
    struct RecordingRecorder {
        counters: Mutex<HashMap<String, u64>>,
    }

    impl MetricsRecorder for RecordingRecorder {
        fn increment_counter(&self, name: &str, value: u64, _labels: &[(&str, &str)]) {
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
    ) -> mpsc::UnboundedReceiver<PubSubMessage> {
        let flags = KeyspaceEventFlags::KEYSPACE
            | KeyspaceEventFlags::KEYEVENT
            | KeyspaceEventFlags::GENERIC
            | KeyspaceEventFlags::EXPIRED;
        worker.set_notify_keyspace_events(Arc::new(AtomicU32::new(flags.bits())));

        let (tx, rx): (PubSubSender, _) = mpsc::unbounded_channel();
        for ch in event_channels {
            worker
                .subscriptions
                .subscribe(Bytes::from(ch.to_string()), 1, tx.clone());
        }
        rx
    }

    /// Collect all currently-queued (channel, payload) pairs.
    fn drain(rx: &mut mpsc::UnboundedReceiver<PubSubMessage>) -> Vec<(String, String)> {
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
    /// so no slot is double-bumped. Because the cycle also carries
    /// `fields_expired > 0` (the field that emptied `h`), it additionally bumps
    /// the shard-wide global epoch (a safe over-abort — the field information is
    /// not key-attributed, so any `fields_expired > 0` cycle bumps the epoch).
    /// Each removed key's effective version is therefore its once-bumped slot
    /// PLUS the once-bumped epoch = 2 (a double slot-bump would read 3, catching
    /// the regression the "exactly once" invariant guards). Also pins the dirty
    /// counter (previously skipped by the hand-rolled expiry path): it advances
    /// by the removed keys.
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
            budget_exhausted: false,
        };
        worker.apply_expiry_effects(result).await;

        assert_eq!(
            worker.get_key_version(b"plain"),
            2,
            "the whole-key-expired key's slot bumps once (1) + global epoch bump \
             for the field expiry (1)"
        );
        assert_eq!(
            worker.get_key_version(b"h"),
            2,
            "the field-emptied key's slot bumps once (1) + global epoch bump (1)"
        );
        assert_eq!(
            worker.store.dirty(),
            2,
            "dirty counter advances by the two removed keys (was skipped before)"
        );
    }

    /// Regression (whole-branch review): a single active-expiry cycle that BOTH
    /// removes a whole key AND field-shrinks a *surviving* watched hash must
    /// still invalidate a watch on that hash. The field-only expiry carries no
    /// key (only `fields_expired`), so the shard cannot slot-attribute it and
    /// compensates with a global-epoch bump. That bump must NOT be gated on
    /// "no key was removed": when the same cycle also removes a key, the field
    /// information still went unattributed and the watched survivor hash would
    /// otherwise commit against a concurrently-mutated value (an optimistic-lock
    /// false negative). Pins the exact scenario: `deleted_keys=[del]` +
    /// `fields_expired=1` with a surviving hash `surv` on a DIFFERENT slot ⇒ a
    /// WATCH on `surv` (live, version-snapshotted pre-cycle) must ABORT.
    #[tokio::test]
    async fn field_expiry_bumps_global_epoch_even_when_a_key_is_also_removed() {
        use crate::shard::message::WatchEntry;
        use crate::shard::partition::slot_for_key;

        let recorder = Arc::new(RecordingRecorder::default());
        let (mut worker, _msg_tx, _conn_tx) = build_worker(recorder);

        // The survivor hash and the removed key must live on DIFFERENT slots, so
        // the removed key's per-slot bump cannot coincidentally touch the
        // survivor — the ONLY thing that can invalidate `surv`'s watch is the
        // global-epoch bump owed to the unattributed field expiry.
        let del = "del";
        let surv = "surv";
        assert_ne!(
            slot_for_key(del.as_bytes()),
            slot_for_key(surv.as_bytes()),
            "test precondition: removed key and survivor hash must be on distinct slots"
        );

        // Seed the survivor as a live, non-empty hash so `exists_unexpired` holds
        // — the watch can then only abort via the version compare, isolating the
        // global-epoch path (not the `live_at_watch` liveness clause).
        seed_hash_with_mixed_fields(&mut worker.store, surv, &[], &["f1"]);
        let v0 = worker.get_key_version(surv.as_bytes());

        // One cycle: reaps whole key `del` AND field-purges a field of `surv`
        // (surv survives — absent from deleted_keys/emptied_keys, present only as
        // the `fields_expired` count).
        let result = ExpiryResult {
            deleted_keys: vec![Bytes::from(del)],
            emptied_keys: vec![],
            fields_expired: 1,
            budget_exhausted: false,
        };
        worker.apply_expiry_effects(result).await;

        // The survivor's effective version must have moved (global epoch), so a
        // live watch on it aborts.
        assert_ne!(
            worker.get_key_version(surv.as_bytes()),
            v0,
            "field-only expiry in a cycle that also removed a key must still bump \
             the global epoch so the surviving hash's watch version moves"
        );
        let watches = [WatchEntry {
            key: Bytes::from(surv),
            version: v0,
            live_at_watch: true,
        }];
        assert!(
            !worker.check_watches(&watches),
            "WATCH on a field-shrunk surviving hash must ABORT EXEC even when the \
             same expiry cycle also removed a whole key (optimistic-lock invariant)"
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
