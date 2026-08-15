use tracing::Instrument;

use super::message::{CoreMsg, ScatterOp};
use super::panic_guard::{self, INTERNAL_ERROR, PanicSite};
use super::worker::ShardWorker;
use crate::store::Store;

impl ShardWorker {
    /// Dispatch core execution messages (Execute, ScatterRequest, GetVersion, ExecTransaction).
    pub(super) async fn dispatch_core(&mut self, msg: CoreMsg) -> bool {
        match msg {
            CoreMsg::Execute {
                command,
                conn_id,
                txid: _,
                protocol_version,
                track_reads,
                no_touch,
                response_tx,
            } => {
                if let Err(err) = self.can_execute_during_lock(conn_id) {
                    let _ = response_tx.send(err);
                    return false;
                }
                // Set suppress_touch on the store before execution
                self.store.set_suppress_touch(no_touch);
                let shard_id = self.shard_id();
                // Panic isolation (c2-07): a panic anywhere under
                // `execute_command` — including inside a dependency — used to
                // unwind the worker task and abort the process. Caught here so
                // the client gets an error and the shard keeps serving.
                let outcome = panic_guard::caught(async {
                    if self
                        .per_request_spans
                        .load(std::sync::atomic::Ordering::Relaxed)
                    {
                        self.execute_command(
                            command.as_ref(),
                            conn_id,
                            protocol_version,
                            track_reads,
                        )
                        .instrument(tracing::info_span!("shard_execute", shard_id))
                        .await
                    } else {
                        self.execute_command(
                            command.as_ref(),
                            conn_id,
                            protocol_version,
                            track_reads,
                        )
                        .await
                    }
                })
                .await;
                let response = match outcome {
                    Ok(response) => {
                        // Reset suppress_touch after execution
                        self.store.set_suppress_touch(false);
                        response
                    }
                    Err(panic_message) => self.recover_from_panic(
                        PanicSite::Command,
                        &command.name_uppercase_string(),
                        &panic_message,
                    ),
                };
                let _ = response_tx.send(response);
            }
            CoreMsg::ScatterRequest {
                request_id: _,
                keys,
                operation,
                conn_id,
                response_tx,
            } => {
                if let Err(err) = self.can_execute_during_lock(conn_id) {
                    let _ = response_tx.send(Self::scatter_error_reply(&operation, &keys, err));
                    return false;
                }
                // Panic isolation (c2-07). The reply is shaped through the same
                // per-op helper the continuation-lock refusal uses, so the
                // coordinator's merge recognizes the error instead of folding a
                // truncated result as success.
                let outcome =
                    panic_guard::caught(self.execute_scatter_part(&keys, &operation, conn_id))
                        .await;
                let result = match outcome {
                    Ok(result) => result,
                    Err(panic_message) => {
                        let err = self.recover_from_panic(
                            PanicSite::Scatter,
                            operation.name(),
                            &panic_message,
                        );
                        Self::scatter_error_reply(&operation, &keys, err)
                    }
                };
                let _ = response_tx.send(result);
            }
            CoreMsg::GetVersion { keys, response_tx } => {
                // Per-key liveness at watch time (the `wk->expired` inverse): a
                // key present and unexpired is "live"; an absent or
                // already-expired key records a nonexistent/stale watch. This is
                // a non-destructive probe (`exists_unexpired`), computed BEFORE
                // the no-bump purge below so the flag reflects the watch-time
                // state (order is immaterial — an expired-but-present key reads
                // `false` either way). Aligned with `keys`.
                let live_at_watch: Vec<bool> = keys
                    .iter()
                    .map(|k| self.store.exists_unexpired(k))
                    .collect();
                // Lazily purge any already-expired watched keys WITHOUT bumping
                // the version: watching an already-stale key must record a
                // "nonexistent" watch, so a later EXEC does not treat the key's
                // (already-due) removal as a modification. A key still live here
                // is left in place; if it expires during the WATCH window it is
                // purged (and the version bumped) at EXEC by
                // `purge_expired_watches` (F3). See the `GetVersion` doc.
                for key in &keys {
                    self.store.purge_if_expired(key);
                }
                // A WATCH-time purge still physically removed the key, so the
                // removal's client-visible effects (tracking invalidation,
                // search-index deletion, the `expired` keyspace notification,
                // XREADGROUP drain) must fire — Redis emits `expired` on lazy
                // expiry regardless of the triggering command. Only the
                // shard-version bump is withheld: WATCH stays no-bump (F3) so an
                // already-expired watched key records a "nonexistent" watch and
                // does not over-abort unrelated watchers.
                self.apply_lazy_purge_effects_no_version_bump();
                // Per-key WATCH versions (proposal 18): each key's slot stamp,
                // aligned with `keys`. Read AFTER the no-bump purge so a key
                // purged here reads its post-purge (unchanged, no-bump) version —
                // preserving the "already-stale watch is a nonexistent watch"
                // contract. Distinct-slot keys carry distinct versions.
                let versions: Vec<u64> = keys.iter().map(|k| self.get_key_version(k)).collect();
                let _ = response_tx.send((versions, live_at_watch));
            }
            CoreMsg::ExecTransaction {
                commands,
                watches,
                conn_id,
                protocol_version,
                admission,
                routing_fence,
                response_tx,
            } => {
                // Panic isolation (c2-07), outer half. A panic in a *queued*
                // command is caught one frame down, inside `execute_transaction`,
                // so it only fails that command; this guard covers the batch
                // frames around the loop (watch validation, the batched
                // write-effects/WAL phase), where there is no single command to
                // blame and the whole EXEC fails.
                let outcome = panic_guard::caught(async {
                    if self
                        .per_request_spans
                        .load(std::sync::atomic::Ordering::Relaxed)
                    {
                        let shard_id = self.shard_id();
                        self.execute_transaction(
                            commands,
                            &watches,
                            conn_id,
                            protocol_version,
                            &admission,
                            routing_fence,
                        )
                        .instrument(tracing::info_span!("shard_exec_txn", shard_id))
                        .await
                    } else {
                        self.execute_transaction(
                            commands,
                            &watches,
                            conn_id,
                            protocol_version,
                            &admission,
                            routing_fence,
                        )
                        .await
                    }
                })
                .await;
                let result = match outcome {
                    Ok(result) => result,
                    Err(panic_message) => {
                        self.recover_from_panic(PanicSite::Transaction, "EXEC", &panic_message);
                        super::types::TransactionResult::Error(INTERNAL_ERROR.to_string())
                    }
                };
                let _ = response_tx.send(result);
            }
        }
        false
    }

    /// Shape the Continuation-Lock conflict error for a rejected scatter part so
    /// the coordinator's per-command merge actually *recognizes* it.
    ///
    /// The reply variant must match what that op's merge reads, or the error is
    /// silently dropped. The FT.* query fan-outs (FT.SEARCH / FT.HYBRID /
    /// FT.AGGREGATE) route **no keys** through the request and their merges only
    /// inspect the typed [`FtShardReply`](frogdb_search::FtShardReply); a per-key
    /// `Keyed` reply is therefore *empty* for them, carrying no error at all —
    /// the shard would drop out of the merge and the client would receive an
    /// incomplete result set as success (issue #15 item 4). Emit the typed FT
    /// error variant those merges read instead. Every other scatter op keeps the
    /// per-key `Keyed` error shape MGET/DEL/EXISTS/… reconstruct from.
    pub(super) fn scatter_error_reply(
        operation: &ScatterOp,
        keys: &[bytes::Bytes],
        err: frogdb_protocol::Response,
    ) -> super::types::PartialResult {
        use frogdb_search::FtShardReply;

        let msg = match &err {
            frogdb_protocol::Response::Error(b) => String::from_utf8_lossy(b).into_owned(),
            _ => "ERR shard busy with continuation lock".to_string(),
        };
        match operation {
            // FT.SEARCH and FT.HYBRID both reply with `Search` hits.
            ScatterOp::FtSearch { .. } | ScatterOp::FtHybrid { .. } => {
                super::types::PartialResult::ft(FtShardReply::Search(Err(msg)))
            }
            ScatterOp::FtAggregate { .. } => {
                super::types::PartialResult::ft(FtShardReply::Aggregate(Err(msg)))
            }
            // Every remaining op. A **keyed** op (MGET/DEL/EXISTS/…) reconstructs
            // the conflict from a per-key `Keyed` reply, so keep that shape. A
            // **keyless** op (KEYS/DBSIZE/SCAN/FLUSHDB and the FT admin /
            // single-shard ops) has no keys to hang the error on — the old
            // `keys.map(...)` produced an *empty* `Keyed` reply that dropped the
            // error silently, so the merge folded a truncated result as success.
            // Emit the fatal, data-free `ShardError` those keyless merges (and
            // the direct shard-0 reads) now surface instead.
            _ if keys.is_empty() => super::types::PartialResult::shard_error(err),
            _ => super::types::PartialResult::keyed(
                keys.iter().map(|k| (k.clone(), err.clone())).collect(),
            ),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::AtomicU64;

    use bytes::Bytes;
    use frogdb_protocol::Response;
    use frogdb_search::wire::FtShardReply;
    use frogdb_search::{FtAggregateRequest, FtSearchRequest};
    use tokio::sync::{mpsc, oneshot};

    use super::CoreMsg;
    use crate::ShardReadyResult;
    use crate::eviction::EvictionConfig;
    use crate::noop::NoopMetricsRecorder;
    use crate::registry::CommandRegistry;
    use crate::replication::NoopBroadcaster;
    use crate::shard::ShardWorker;
    use crate::shard::message::{ScatterOp, ShardReceiver, ShardSender};
    use crate::shard::types::PartialResult;

    fn test_worker() -> ShardWorker {
        let (msg_tx, msg_rx) = mpsc::channel(16);
        let (_, conn_rx) = mpsc::channel(16);
        let shard_senders = Arc::new(vec![ShardSender::new(msg_tx)]);
        ShardWorker::with_eviction(
            0,
            1,
            ShardReceiver::new(msg_rx),
            conn_rx,
            shard_senders,
            Arc::new(CommandRegistry::new()),
            EvictionConfig::default(),
            Arc::new(NoopMetricsRecorder::new()),
            Arc::new(AtomicU64::new(0)),
            Arc::new(NoopBroadcaster),
        )
    }

    /// Hold the continuation lock for `owner`. The returned release sender must
    /// be kept alive for the lock to remain held.
    async fn hold_continuation_lock(worker: &mut ShardWorker, owner: u64) -> oneshot::Sender<()> {
        let (ready_tx, ready_rx) = oneshot::channel();
        let (release_tx, release_rx) = oneshot::channel();
        worker
            .vll
            .request_continuation_lock(1, owner, ready_tx, release_rx, oneshot::channel().0);
        assert!(matches!(ready_rx.await, Ok(ShardReadyResult::Ready)));
        assert_eq!(worker.vll.continuation_lock_owner(), Some(owner));
        release_tx
    }

    /// Issue #15 item 4: an FT.SEARCH scatter part colliding with another
    /// connection's Continuation Lock must come back as a *recognizable* typed
    /// FT search error, not an empty `Keyed` reply the search merge silently
    /// drops (FT.SEARCH routes no keys, so the old per-key `Keyed` reply carried
    /// no error at all — an incomplete result set returned as success).
    #[tokio::test]
    async fn ft_search_scatter_under_continuation_lock_returns_ft_error() {
        let mut worker = test_worker();
        let _release = hold_continuation_lock(&mut worker, 100).await;

        let (tx, rx) = oneshot::channel();
        let request = Box::new(FtSearchRequest::parse(&[Bytes::from_static(b"*")]));
        worker
            .dispatch_core(CoreMsg::ScatterRequest {
                request_id: 1,
                keys: vec![],
                operation: ScatterOp::FtSearch {
                    index_name: Bytes::from_static(b"idx"),
                    request,
                },
                conn_id: 200,
                response_tx: tx,
            })
            .await;

        match rx.await.expect("scatter reply") {
            PartialResult::Ft(FtShardReply::Search(Err(msg))) => {
                assert!(msg.contains("continuation lock"), "got {msg:?}");
            }
            other => panic!("expected Ft(Search(Err)), got {other:?}"),
        }
    }

    /// FT.AGGREGATE rejects into the typed aggregate error variant its merge
    /// reads (the aggregate merge only inspects `Ft(Aggregate(..))`).
    #[tokio::test]
    async fn ft_aggregate_scatter_under_continuation_lock_returns_ft_error() {
        let mut worker = test_worker();
        let _release = hold_continuation_lock(&mut worker, 100).await;

        let (tx, rx) = oneshot::channel();
        let request = Box::new(
            FtAggregateRequest::parse(&[Bytes::from_static(b"*")])
                .expect("aggregate request parses"),
        );
        worker
            .dispatch_core(CoreMsg::ScatterRequest {
                request_id: 1,
                keys: vec![],
                operation: ScatterOp::FtAggregate {
                    index_name: Bytes::from_static(b"idx"),
                    request,
                },
                conn_id: 200,
                response_tx: tx,
            })
            .await;

        match rx.await.expect("scatter reply") {
            PartialResult::Ft(FtShardReply::Aggregate(Err(msg))) => {
                assert!(msg.contains("continuation lock"), "got {msg:?}");
            }
            other => panic!("expected Ft(Aggregate(Err)), got {other:?}"),
        }
    }

    /// Issue #15 item 4 (keyless fallout): a **keyless** scatter op (FT.DROPINDEX
    /// here) colliding with another connection's Continuation Lock must come back
    /// as a fatal [`PartialResult::ShardError`], not an *empty* `Keyed` reply.
    /// FT.DROPINDEX routes no keys, so the old `keys.map(...)` per-key fallback
    /// produced `Keyed(vec![])` — carrying no error at all — and the
    /// `OkOrFirstError` merge returned `OK` while the locked shard never dropped
    /// the index (silent cross-shard divergence reported as success).
    #[tokio::test]
    async fn keyless_dropindex_scatter_under_continuation_lock_returns_shard_error() {
        let mut worker = test_worker();
        let _release = hold_continuation_lock(&mut worker, 100).await;

        let (tx, rx) = oneshot::channel();
        worker
            .dispatch_core(CoreMsg::ScatterRequest {
                request_id: 1,
                keys: vec![],
                operation: ScatterOp::FtDropIndex {
                    index_name: Bytes::from_static(b"idx"),
                },
                conn_id: 200,
                response_tx: tx,
            })
            .await;

        match rx.await.expect("scatter reply") {
            PartialResult::ShardError(Response::Error(msg)) => {
                assert!(
                    String::from_utf8_lossy(&msg).contains("continuation lock"),
                    "got {msg:?}"
                );
            }
            other => panic!("expected ShardError, got {other:?}"),
        }
    }

    /// FT.TAGVALS is likewise keyless: its rejection must be a fatal
    /// `ShardError`, not an empty `Keyed` union the `TagValsUnion` merge folds
    /// into a truncated (but "successful") tag set.
    #[tokio::test]
    async fn keyless_tagvals_scatter_under_continuation_lock_returns_shard_error() {
        let mut worker = test_worker();
        let _release = hold_continuation_lock(&mut worker, 100).await;

        let (tx, rx) = oneshot::channel();
        worker
            .dispatch_core(CoreMsg::ScatterRequest {
                request_id: 1,
                keys: vec![],
                operation: ScatterOp::FtTagvals {
                    index_name: Bytes::from_static(b"idx"),
                    field_name: Bytes::from_static(b"tag"),
                },
                conn_id: 200,
                response_tx: tx,
            })
            .await;

        assert!(
            matches!(
                rx.await.expect("scatter reply"),
                PartialResult::ShardError(Response::Error(_))
            ),
            "keyless FT.TAGVALS conflict must surface as a fatal ShardError"
        );
    }

    /// Non-FT keyed scatter ops keep their per-key `Keyed` conflict error — the
    /// fix must not disturb the shape MGET/DEL/EXISTS merges expect.
    #[tokio::test]
    async fn keyed_scatter_under_continuation_lock_still_returns_keyed_error() {
        let mut worker = test_worker();
        let _release = hold_continuation_lock(&mut worker, 100).await;

        let (tx, rx) = oneshot::channel();
        worker
            .dispatch_core(CoreMsg::ScatterRequest {
                request_id: 1,
                keys: vec![Bytes::from_static(b"a"), Bytes::from_static(b"b")],
                operation: ScatterOp::MGet,
                conn_id: 200,
                response_tx: tx,
            })
            .await;

        match rx.await.expect("scatter reply") {
            PartialResult::Keyed(results) => {
                assert_eq!(results.len(), 2);
                assert!(
                    results.iter().all(|(_, r)| matches!(r, Response::Error(_))),
                    "every requested key carries the conflict error"
                );
            }
            other => panic!("expected Keyed error reply, got {other:?}"),
        }
    }

    /// C3 forcing test — Arm 2 (`CoreMsg::GetVersion`).
    ///
    /// The WATCH-time version probe runs with no `can_execute_during_lock` gate,
    /// yet it physically calls `purge_if_expired` and fires
    /// `apply_lazy_purge_effects_no_version_bump`. The C3 investigation asked
    /// whether that violates a continuation-lock owner's isolation. It does not,
    /// on two counts this test pins down while a *foreign* connection holds the
    /// continuation lock:
    ///
    /// 1. **Only already-dead keys are purged.** A key still live (unexpired) at
    ///    watch time is left in place — the lock owner never sees a key it holds
    ///    vanish out from under a live view. Only a key already past its TTL —
    ///    which the owner would itself observe as absent — is removed.
    /// 2. **No version bump.** The lazy purge withholds the shard-version bump
    ///    (F3: an already-stale watch is a nonexistent watch), so a purge on one
    ///    key does not perturb the WATCH version of a *live* key sharing the same
    ///    slot. The live and expired keys here are hash-tag colocated (`{s}`)
    ///    precisely so a spurious bump would corrupt the live key's version and
    ///    trip the assertion.
    ///
    /// This is the machine-checked evidence for the EXEMPT disposition. If a
    /// future change purged a live key here, or bumped the version on the lazy
    /// purge, the lock owner's isolation would break and this test would fail —
    /// flipping the disposition to GATE.
    #[tokio::test]
    async fn get_version_purges_only_expired_keys_without_bumping_under_continuation_lock() {
        use crate::store::Store;
        use crate::types::Value;
        use std::time::{Duration, Instant};

        let mut worker = test_worker();
        let _release = hold_continuation_lock(&mut worker, 100).await;

        // Two keys colocated on one slot via the `{s}` hash tag, so a bump on the
        // expired-key purge would visibly change the live key's WATCH version.
        let live = Bytes::from_static(b"{s}live");
        let dead = Bytes::from_static(b"{s}dead");
        worker.store.set(live.clone(), Value::string("v"));
        worker.store.set(dead.clone(), Value::string("v"));
        // `dead` is already past its TTL; `live` carries none.
        assert!(
            worker
                .store
                .set_expiry(dead.as_ref(), Instant::now() - Duration::from_secs(60))
        );

        let slot_version_before = worker.get_key_version(live.as_ref());
        assert_eq!(
            slot_version_before,
            worker.get_key_version(dead.as_ref()),
            "the two hash-tagged keys must share a slot for this test to be meaningful"
        );

        let (tx, rx) = oneshot::channel();
        worker
            .dispatch_core(CoreMsg::GetVersion {
                keys: vec![live.clone(), dead.clone()],
                response_tx: tx,
            })
            .await;
        let (versions, live_at_watch) = rx.await.expect("GetVersion reply");

        // (1) The live key survives untouched; the expired key is purged.
        assert!(
            worker.store.exists_unexpired(live.as_ref()),
            "a key live at watch time must NOT be purged by the WATCH-time probe"
        );
        assert!(
            !worker.store.contains(dead.as_ref()),
            "an already-expired watched key is physically purged"
        );
        assert_eq!(
            live_at_watch,
            vec![true, false],
            "live_at_watch records watch-time liveness per key"
        );

        // (2) The lazy purge withheld the version bump: neither the live key's
        // slot nor the (shared) expired key's slot advanced, so a WATCH the lock
        // owner holds on the live key is not spuriously aborted.
        assert_eq!(
            versions[0], slot_version_before,
            "the live key's slot version must not change"
        );
        assert_eq!(
            versions[1], slot_version_before,
            "purging the already-expired key must NOT bump the shared slot version"
        );

        // The continuation lock is still held by conn A throughout.
        assert_eq!(worker.vll.continuation_lock_owner(), Some(100));
    }
}
