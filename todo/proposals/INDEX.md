# Architecture Proposals

Deepening refactors surfaced by an architecture review on 2026-06-12. Each proposal turns a shallow
module into a deep one: more behaviour behind a smaller interface, with locality (bugs and change
concentrate in one place) and leverage (callers and tests get more per unit of interface) as the
payoff. All evidence verified against the code at time of writing.

## Round 1 — implemented (2026-06-12)

Ordered by leverage:

1. [01-declarative-command-spec.md](01-declarative-command-spec.md) — **Implemented**
   (`8d573510`…`182898b6`, 13 commits): 377/377 commands declare a `CommandSpec`; dispatcher
   derives keys/events/waking/WAL; legacy opt-in methods deleted; registry-wide exhaustiveness
   tests. Found+fixed along the way: MSETEX/BITOP/XGROUP/XREADGROUP WAL bugs, ZINCRBY wake,
   STORE-destination WAL gap (`WalStrategy::Dynamic`).
2. [02-typed-store-access.md](02-typed-store-access.md) — **Implemented** (`e792ed42`,
   `a1a702ca`, `760944d1`, `b8db0087`): `StoreTypedExt` owns the WrongType invariant; unwrap
   chains in commands now 0, enforced by the `lint-no-typed-unwrap` gate (Justfile + lefthook);
   triplicated `get_or_create` + five utils wrappers deleted. Follow-up candidate: extend
   `typed_family_accessors!` to the probabilistic/extension families (tdigest, cuckoo, bloom,
   topk, cms, HLL, timeseries, vectorset, JSON) to retire ~128 remaining
   `as_X().ok_or(WrongType)` read-path sites.
3. [03-unified-post-execution-pipeline.md](03-unified-post-execution-pipeline.md) —
   **Implemented** (`0db27d82`, `bfe61d2e`): `shard/post_execution.rs` owns the canonical
   9-step `WRITE_EFFECT_ORDER`; `WalPhase`/`EffectScope` are data; `pipeline.rs` deleted; each
   effect step invoked from exactly one site; order-invariant regression tests added. (Drift was
   pre-fixed in `6e483280`/`23469adc`.)
4. [04-connection-state-encapsulation.md](04-connection-state-encapsulation.md) —
   **Implemented** (`694ab867`…`470004e7`, 5 commits): `ConnectionState` owns all transitions as
   methods; six state-machine fields now private; external field reach-ins 0 (40+31+6+3+3+3 → 0,
   enforced by the compiler); MULTI five-field reset and the subscribe-limit blocks collapsed to
   one method each; ASKING/LocalServe nuance preserved exactly; 12 socket-free state-machine
   tests. Phase 6 (tracking/auth/blocked fields) **now implemented** (`16529b94`): `tracking`
   and `auth` routed through named transitions (`enable_tracking`/`disable_tracking`/
   `set_caching_override`/`should_track_read`; `authenticate`/`is_authenticated`/
   `authenticated_user`/`username`) and flipped private; reach-ins 6 (auth) + 14 (tracking) → 0,
   compiler-enforced; `blocked` was already encapsulated in an earlier round; CLIENT TRACKING OFF
   early-return/teardown split preserved exactly; 5 new socket-free state-machine tests.
5. [05-single-routing-decision.md](05-single-routing-decision.md) — **Implemented** (`510fec8c`):
   dead `CommandRouter`/`RouteResult`/`ScatterStrategy`/`op_to_handler` twin deleted (zero
   non-test callers confirmed); live mapping moved into `router::route_connection_level` +
   `handler_for` as pure functions; `connection_level_handler_for` is now a one-line delegation;
   dead `ConnectionLevelHandler::Cluster` variant removed; reachability + exhaustiveness tests
   added.
6. [06-recovery-orchestrator.md](06-recovery-orchestrator.md) — **Implemented** (`1904d9a8`…
   `b03a8b97`, 6 commits): `server/src/recovery/` owns `recover(RecoveryInputs) ->
   Result<RecoveredState, RecoveryError>` over six ordered phases (checkpoint install, RocksDB
   open, shard restore, functions, replication-state reconcile, cluster storage); `init_persistence`
   + the `PersistenceInitResult` alias deleted; startup collapses to one `recover()` call + wiring.
   11 socket-free seam tests (the staged-checkpoint install path previously had zero). Search-index
   recovery deferred (non-`Send` per-shard handles) — documented at the call site.

## Round 2 — implemented (2026-06-13)

A second deepening review on 2026-06-13 (fresh fan-out over the largest crates, excluding the six
already-implemented refactors). Ordered by leverage:

7. [07-scatter-gather-executor.md](07-scatter-gather-executor.md) — **Implemented**
   (`3d89eb53`…`aedcb3c9`, 5 commits): `scatter/broadcast.rs` owns broadcast fan-out + single-shared-
   deadline timeout + merge behind a `MergeStrategy` seam (`SumIntegers`/`SortedUnion`/`SortedByKey`/
   `DedupSorted`/`CountByKey`/`ShardZeroReply`/`AllOk`/`BoolOr`). Migrated KEYS/DBSIZE/FLUSHDB, TS.*,
   the five PUBSUB introspection commands, SCRIPT LOAD/EXISTS/FLUSH/KILL, and the search fan-outs
   (FT.SEARCH/AGGREGATE/HYBRID/CREATE/ALTER/DROPINDEX/SYNUPDATE/TAGVALS/SPELLCHECK, ES.ALL) via
   per-command `MergeStrategy` impls in `search/merge.rs`; SCAN's cursor walk and RANDOMKEY's
   count-then-fetch borrow the shared `ScatterGather::query_one` per-shard helper. Dead
   `ScatterHandler`/five `merge_*`/`ScanResult` + `KeysStrategy`/`DbSizeStrategy`/`FlushDbStrategy`
   deleted (zero callers reconfirmed). 17 socket-free strategy + mock-shard runner tests (fail-fast/
   best-effort/closed-sender/single-deadline timeout). Fixed round-2 flags F1/F2/F3. Phase 5 (sharing
   folds with the keyed VLL `ScatterGatherStrategy::merge`) deferred — its `HashMap<usize,
   HashMap<Bytes, Response>>` + `key_order` shape resists the sequential-fold `MergeStrategy` model;
   the proposal sanctions leaving the two taxonomies separate rather than contorting the seam.
8. [08-acl-enforcement-seam.md](08-acl-enforcement-seam.md) — **Implemented**
   (`a1bed2be`…`bd2063e6`, 5 commits): `PermissionGuard` seam (`connection/permission_guard.rs`)
   owns the ACL command/key/channel check + denial logging + NOPERM formatting; the four call sites
   (guards/auth/transaction/routing) route through it and the existing acl-crate deep half
   (`FullAclChecker`/`AclError`/`AclLog`); dead `AclChecker` trait + `KeyAccess` enum deleted; 6
   guard unit + 6 ACL integration tests. Fixed round-2 flags: in-MULTI denial logging, NOPERM
   subcommand pipe, ACL DRYRUN key simulation.
9. [09-serialization-typecodec-registry.md](09-serialization-typecodec-registry.md) —
   **Implemented** (`cbb5c4f1`…`048dc792`, 4 commits): serialization dispatches through a
   `TypeCodec` registry keyed by a closed `#[repr(u8)] TypeMarker` whose non-exhaustive `decode_for`
   match makes a missing decode a COMPILE error; wire bytes byte-identical (discriminants pinned,
   payload builders reused); `every_marker_round_trips` covers all 17 markers (9 previously
   untested). Fixed round-2 flags: TimeSeries unknown-policy decode now errors; VectorSet M/EF/dim
   bounds enforced at creation (`types`/`commands`) to match decode → round-trip restored.
10. [10-active-expiry-coordinator.md](10-active-expiry-coordinator.md) — **Implemented**
    (`596ffacc`…`ffdd156f`, 6 commits): `shard/active_expiry.rs` owns
    `ActiveExpiryCoordinator::run_cycle → ExpiryResult`; the event loop calls it +
    `apply_expiry_effects` past the seam; 10 coordinator unit tests + 3 effect tests (no event loop
    needed). Fixed round-2 flags: field-emptied keys now emit `del` + fire the expired probe + count
    in `expired_keys` (no double-count); scan bounded in capped batches so the 25ms budget covers it.
11. [11-probabilistic-typed-accessors.md](11-probabilistic-typed-accessors.md) — **Implemented**
    (`3f3672e5`…`057d9e46`, 13 commits): completes proposal 02's deep module. Split
    `ValueType`/`DefaultValueType` (create-if-missing is now opt-in) and extended
    `typed_family_accessors!` to all 8 probabilistic/extension families (bloom, cuckoo, topk,
    tdigest, cms, hll, timeseries, vectorset), then migrated every hand-rolled
    `as_X().ok_or(WrongType)?` chain to the typed accessors — the 8 families plus the surviving core
    remnants (stream, sorted set, geo, event sourcing, json, string) that proposal 02 left behind.
    `commands/src` is now at **0** `.ok_or(WrongType)` sites; the generic WrongType matrix covers all
    14 families and the no-COW-on-wrong-type property. The `lint-no-typed-unwrap` gate now also bans
    `.ok_or[_else](…WrongType)` so the invariant cannot regress to a second home. Fixed round-2 flags:
    READONLY t-digest clone, core stream `.ok_or(WrongType)` remnants (see below).
12. [12-blocking-wait-coordinator.md](12-blocking-wait-coordinator.md) — **Implemented**
    (`98309ea3`…`db5c6e09`, 5 commits): the wait/wake machinery is concentrated behind two seams.
    Server side, `BlockingWaitCoordinator` (`connection/handlers/blocking/coordinator.rs`) owns the
    response / CLIENT-UNBLOCK / deadline race behind an `UnblockSignal` trait, so `WaitOutcome`
    selection is unit-testable with a mock channel + injected deadline; `WaitOutcome` is now public
    with `into_response(op)`, the single site that chooses the RESP2 nil shape; `handle_blocking_wait`
    collapses to a register→coordinate→cleanup skeleton and `BlockedState` moves behind
    `begin_block`/`end_block`/`blocked_shard` (the last hand-mutated `ConnectionState` field, closing
    proposal 04's Phase 6 holdout). Core side, the three near-parallel `try_satisfy_*_waiters` loops
    collapse into one `drive_satisfaction` driver (FIFO loop, recursive BLMOVE wake-cascade, depth-16
    cap, version bump, metrics, timeout re-validation) behind a `WaiterSatisfaction` strategy
    (List/Zset/Stream) that sees only the store. Dead `ClientHandle::clear_unblock` removed. 8
    socket-free coordinator + strategy/driver unit tests, per-family `*-1`/`$-1` wire-shape
    regression tests, and push-vs-timeout race tests. Fixed round-2 flags: blocking timeout nil shape
    and the lost-element timeout race (see below).
13. [13-column-family-manifest.md](13-column-family-manifest.md) — **Implemented**
    (`ff24a1a4`…`156e7890`, 2 commits): `rocks/manifest.rs` `ColumnFamilyManifest::reconcile` derives
    the required CF set from persisted state and folds the shard-count + warm-tier invariants behind
    one seam; warm on→off reopen is now a hard `WarmTierMismatch` (off→on confirmed benign); a
    `list_cf` failure propagates instead of silently disabling both guards; warm-toggle reopen tests
    cover both directions. Fixed round-2 flags: warm-tier toggle, `list_cf` swallow.

## Round 3 — implemented (2026-06-15)

A third deepening review on 2026-06-15 (fresh fan-out over the crates rounds 1-2 left untouched:
replication, cluster, scripting, search internals, vll/config/telemetry/eviction). Ordered by
leverage:

14. [14-replication-backlog.md](14-replication-backlog.md) — **Implemented**
    (`4daa18ab`…`aed8d0dd`, 6 commits): `primary/replay.rs` `PartialSyncReplay` owns ring-buffer
    lifecycle + `can_replay` + `extract_backlog` behind `handle_partial_sync_request`; `handle_psync`
    routes through it and grants `+CONTINUE` end-to-end (the `partial_sync_replay_supported` gate is
    deleted); lower-bound eviction + replid-mismatch fall back to full resync. Built on proposal 18's
    offset contract. Fixed round-2/round-3 flags: F1 full-sync handoff data loss + F2 `+CONTINUE`
    gap, both via a gap-free streamer (subscribe-before-cut + backlog replay of
    `(snapshot_offset, current]` with live-tail dedup). 90 unit + 122 integration tests.
15. [15-search-index-lifecycle.md](15-search-index-lifecycle.md) — **Implemented**
    (`b23319c0`…`135127e1`, 5 commits): `core/shard/search/lifecycle.rs` `IndexLifecycleManager` owns
    create/drop/alter/info/recover behind one seam; `shards.rs` inline recovery (~90 lines) deleted →
    `IndexLifecycleManager::recover` at worker-spawn time (subsumes proposal 06's deferred item), with
    a `RecoveryOutcome::{Corrupt,Undeserializable}` taxonomy instead of a silent `warn!`. Fixed flags:
    FT.CREATE/ALTER/DROPINDEX now persist-before-OK (roll back + error on persist/commit failure).
    10 lifecycle unit tests; search 146 + server-search 136 + regression 120.
16. [16-config-parameter-lifecycle.md](16-config-parameter-lifecycle.md) — **Implemented**
    (`699f85f4`…`ae5527d4`, 7 commits): `ConfigParam<T>` + type-erased `DynParam` registry; all **45
    mutable params** on the typed lifecycle (31 `ConfigParam` + 14 `NoopParam`, enforced by
    `test_param_registry_consistency`); inline legacy setters deleted; shard propagation now driven
    from param defs. Fixed flags: eviction-policy / durability-mode / wal-failure-policy / loglevel
    legal-value lists deduped onto enum/const sources of truth; `build_eviction_config` failure mode
    unified. (`params.rs` metadata registry stays config-side — docs-gen needs it; sanctioned.)
    config 94 + server-config 82.
17. [17-cluster-redirect-mapper.md](17-cluster-redirect-mapper.md) — **Implemented**
    (`bfb49b6a`…`736a4115`, 4 commits): `slot_migration/redirect.rs` + `RouteDecision::to_response`
    own all MOVED/ASK/CLUSTERDOWN construction; the redirect literals now live only there. SSUBSCRIBE
    routed through `coordinator.route()`; `cluster_pubsub::get_slot_owner_addr` deleted. Fixed flags:
    SSUBSCRIBE migration misroute (importing+ASKING serves local, ASK emitted, unassigned→CLUSTERDOWN),
    MOVED IPv6 bracketing, `slot_assignment.get` reach-ins. 166 cluster+pubsub + 58 regression.
    (Distinct from proposal 05, which was connection-level handler selection.)
18. [18-replication-offset-coordinator.md](18-replication-offset-coordinator.md) — **Implemented**
    (`ec025b14`…`944e8882`, 4 commits): `offset_coordinator.rs` `OffsetCoordinator` owns all three
    offset homes behind one seam (`frame_advance`/`advance_broadcast`/`current`/`min_acked`/
    `record_replica_ack`/`can_serve_partial_sync`/`reconcile_for_persist`); readers + writers routed,
    dead writer deleted. Fixed the 🔴 offset-increment mismatch (replica now counts payload only via
    `frame_advance`, matching the primary) + GETACK offset-0 (now advanced+stamped+backlogged).
    Proposal 14 builds on this. 75 unit + 119 integration.
19. [19-vector-field-state-manager.md](19-vector-field-state-manager.md) — **Implemented**
    (`1b77ab64`…`77349ebf`, 4 commits): `search/src/vector.rs` `VectorFieldManager` owns the 4 maps
    behind high-level ops with the bijection invariant + all-or-nothing index/delete. Fixed flags:
    discarded `usearch.add`/`remove` results, replace-path `key_map` orphan, `create_vector_indexes`
    id-collision on partial load (now both-or-neither `try_load`), non-atomic `save_vectors` (now
    temp+fsync+rename). search 146 + vectorset/vsim/vadd 98.
20. [20-eviction-generic-ranker.md](20-eviction-generic-ranker.md) — **Implemented**
    (`6ba518c3`…`01cd1bda`, 5 commits): `eviction/ranker.rs` `EvictionRanker` (3 `#[inline]` ZST
    rankers) collapses the 9 triplicated bodies to one `sample_with_ranker`/`evict_with_ranker`/
    `demote_with_ranker` + one `maybe_insert_with_ranker`; monomorphized (no dyn dispatch),
    bit-identical selection (verified by transitional parity tests). Fixed flag:
    `frogdb_eviction_samples_total` now carries the `policy` label. core 587 + maxmemory regression 59.

## Round 4 — implemented (2026-06-18)

A fourth deepening review on 2026-06-17 (fresh fan-out over the seams rounds 1-3 left shallow:
INFO assembly, keyspace-notification fan-out, slot/shard validation, keyspace-stats accounting,
snapshot creation, protocol response shapes, consumer-group PEL, and script command routing).
Three carried confirmed 🔴 correctness bugs, all fixed. Ordered by leverage:

21. [21-info-section-builder.md](21-info-section-builder.md) — **Implemented**
    (`c0163ab9`…`94ff8799`, 4 commits): `InfoSection` trait + `InfoSources` bundle +
    `InfoBuilder`/`SectionWriter` (server `src/info/`) replace the 271-line `.replace()`/
    `replace_range()` post-assembly patch in `scatter.rs` (−283 lines) and the anchor stubs in
    `commands/info.rs`. One `InfoSnapshot` fleet scatter replaces three separate gathers;
    Memory/Keyspace/Stats now aggregate across all shards (old INFO reported ~1/4 of a 4-shard
    server). Persistence renders real aggregated WAL lag — the placeholder-`0`s flag is fixed.
    `lint-info-seam` gate rejects post-hoc string patching. 35 unit + 8 integration tests.
22. [22-keyspace-notification-coordinator.md](22-keyspace-notification-coordinator.md) — 🔴
    **Implemented** (`e1e1914a`…`d17049e0`, 4 commits): `KeyspaceNotificationCoordinator`
    (`Topology::{Local, Sharded}`) + fire-and-forget `ShardMessage::PublishKeyspace` route
    keyspace/keyevent notifications to the subscription-owning shard (shard 0). Fixed the confirmed
    cross-shard loss; `test_lrem_emits_keyspace_notification` unpinned and passing at 4 shards.
    Saturated mailbox drops + counts `frogdb_keyspace_notifications_dropped_total` (at-most-once,
    Redis semantics). Disabled fast path untouched. `lint-keyspace-notify-routing` gate. 4 unit +
    5 integration tests.
23. [23-same-slot-validator.md](23-same-slot-validator.md) — **Implemented**
    (`d82997bf`…`5072ebd4`, 4 commits): `SlotValidator::{same_shard, same_slot}`
    (`slot_migration/validator.rs`) collapsed the 6 surviving inline CROSSSLOT sites;
    `TxnSlotAccumulator` (`connection/state.rs`) replaced the three fragmented Multi-promotion
    paths; dead same-shard helper trio in `server/src/routing.rs` deleted (whole file, −140 lines).
    🔴 MOVED IPv6 fixed: MOVED/ASK/CLUSTERDOWN/CROSSSLOT formatters moved to
    `frogdb-types::redirect` (single owner, IPv6 bracketed); the blocking path routes through it.
    `CommandError`/`ScriptError` CROSSSLOT literals pinned by parity tests. `lint-redirect-seam`
    gate. Property test: `same_slot ⇒ same_shard` over 200 keys × 6 shard counts.
24. [24-keyspace-stats-accounting.md](24-keyspace-stats-accounting.md) — **Implemented**
    (`2caff37f`…`5f50a59e` + `aea7c402`, 5 commits): `LookupSpec {None, FirstKey, EveryKey,
    Reported}` on `CommandSpec` + `LookupOutcome` replaced `CommandFlags::TRACKS_KEYSPACE` and the
    hand-placed tallies; the seam derives hit/miss from a non-mutating, expiry-aware
    `Store::exists_unexpired` probe **before** execution (the spec's post-return re-probe was wrong
    for GETDEL/GETEX). Coverage extended 6 → 23 read commands; SCAN/RANDOMKEY stay uncounted
    (Redis parity). RESETSTAT closed via the resettable `KeyspaceStats` baseline-offset; INFO
    reports reported values through `InfoSources` (re-wired onto proposal 21's seam in `aea7c402`)
    and stays live with metrics disabled; Prometheus `_total` counters remain strictly monotonic.
25. [25-snapshot-creation-stager.md](25-snapshot-creation-stager.md) — 🔴 **Implemented**
    (`e029ee14`…`22db53f8`, 5 commits): `SnapshotStager` + RAII `TmpDirGuard` own create-side
    staging. Both confirmed bugs fixed: search-copy failure now aborts (no incomplete snapshot
    installed); crash between metadata-write/renames leaks nothing (guard cleans on any early
    return; stale `.tmp` dirs reclaimed). Checkpoint now created into a non-pre-created dir.
    Dead `OnWriteHook` deleted. 6 creation-side crash-window tests mirror the 8 install-side ones.
26. [26-protocol-response-shape-builder.md](26-protocol-response-shape-builder.md) — **Implemented**
    (`e6b40e1b`…`2864f4a0`, 5 commits): `MapReply` (protocol crate) renders RESP3 Map / RESP2 flat
    Array from one pair list (HELLO + HOTKEYS GET single-builds); `PubSubConfirmation` (core) is
    the one owner of confirmation shape — all 9 inline handler builds routed through it, the
    EXEC-only Array→Push patch deleted, and 6 dead hand-built `*_response` helpers removed. RESP3
    confirmations are now Push in every path (Redis `addReplyPushLen` semantics) — the
    inconsistency flag is fixed. `write_null_array` owns the `*-1\r\n`/`_\r\n` shape once.
    `lint-pubsub-confirmation-seam` gate.
27. [27-consumer-group-pel.md](27-consumer-group-pel.md) — **Implemented**
    (`34aa66e4`…`75da9130`, 3 commits): `ConsumerGroup::claim_pending` + `drop_missing_pending`
    (types crate) own PEL reassignment; XCLAIM and XAUTOCLAIM are thin callers. Fixed: XAUTOCLAIM
    now evicts stream-deleted entries count-correctly (`sum(pending_count) == pending.len()`
    invariant restored) and XCLAIM's `TIME` argument is honored instead of discarded.
    Redis-regression TCL suite for consumer groups added.
28. [28-script-command-gate.md](28-script-command-gate.md) — 🔴 **Implemented**
    (`fd194cf2`, `1dc5df49` + `88462063`, 3 commits): `ScriptCommandGate` (`scripting/gate.rs`,
    core crate — the spec's `frogdb-scripting` crate name was wrong) owns `classify` (one key
    extraction feeding both the cross-slot check and routing) + `dispatch` (local vs remote).
    The silent wrong-shard fallthrough is dead: a cross-shard call on a current-thread runtime
    hard-errors (`ERR cross-shard script call requires a multi-thread runtime`) instead of writing
    locally; regression test pins the empty local store. call/pcall collapsed to one dispatch.
    `lint-script-gate` gate bans `block_in_place` outside the gate. The pre-existing
    `test_eval_undeclared_key_access_standalone` — which passed only via the bug — now runs
    multi-thread like production (`88462063`).

## Round 5 — implemented (2026-07-04)

A fifth deepening review on 2026-07-04 (fresh fan-out over: server connection-layer subsystems
untouched by rounds 1-4, persistence WAL/snapshot-install, cluster gossip/failover/migration,
core shard/store internals, vll, scripting internals, telemetry/ops/search-query). Implemented
four at a time in three waves. Ordered by wave:

29. [29-wal-durability-sink.md](29-wal-durability-sink.md) — **Implemented** (`17cb35fc`*,
    `4a555927`, `fc715ad8`, `d4e954d8`): `WriteSink`/`RocksSink` + `FlushOutcomes`/`FlushEngine`
    make every WAL flush (explicit/size/timeout/drain) record a fallible outcome; confirmation is
    sequence-anchored (`flush_through(after_seq)`) so an acked write can no longer outrun a
    swallowed flush failure (the 🔴 silent-data-loss flag). Failed batches drop **visibly**
    (INFO `wal_last_flush_status`/`wal_flush_failures`/`wal_lost_ops`, Prometheus counters +
    gauge, rate-limited logs) — deliberate vs Redis AOF retry, which could resurrect rolled-back
    state. Dead `sync_lag_ms` deleted end-to-end (was seeded once, climbed forever); dead
    `WalWriter` trait impl deleted (zero callers). 7 loop-level sink tests + regression pin.
    persistence 93/93, core 634/634, telemetry 167/167.
30. [30-store-entry-reconciliation.md](30-store-entry-reconciliation.md) — **Implemented**
    (`17cb35fc`, `038974bb`, `5177e302`, `b0fff995`, `02a620b0`): private `replace_entry` is the
    single insert/overwrite path reconciling all side indexes. Fixes 🔴 stale-expiry-index data
    loss (`SET k EX` → `MSET k` → active expiry deleted the persistent key) and the
    `memory_used` drift on in-place growth (deferred keysize refresh now applies the memory
    delta; charge/refund symmetric; delete-underflow closed). Beyond the survey:
    `set_with_options` missed field/label indexes, `restore_entry` replay leak, non-histogram
    growth invisible, double-`get_mut` double-count — all fixed. Active-expiry `run_cycle`
    re-checks the deadline before delete (stale index can never delete a live key). 16 new
    tests; core 634/634, regression expire/maxmemory/keysizes 193/193.
31. [31-atomic-failover-command.md](31-atomic-failover-command.md) — **Implemented**
    (`e9776aa9`, `baa9603a`, `cc583317`, `d018387e`): composite `ClusterCommand::Failover`
    applied in `ClusterState::apply_command` as one replicated transition (slot transfer,
    promotion, removal/demotion, replica re-parenting, epoch bump — Redis configEpoch parity);
    `MarkNodeFailed` bumps epoch in its own apply; both saga call sites collapse to one
    `client_write` (auto path retried 3×). Fixed three latent bugs the composite exposed:
    graceful failover never transferred slots (error hidden in unchecked `Ok(resp.data)`),
    force failover never promoted the successor's role, `ForwardedWrite` swallowed state-machine
    errors. `lint-failover-atomicity` gate. Voter-registration retry added; full
    membership reconciliation deferred (documented). 15 unit tests; cluster 81/81 + integration
    failover pass.
32. [32-lua-sandbox-builder.md](32-lua-sandbox-builder.md) — **Implemented** (`d00120e4`,
    `7a398e57`, `bdb35821`): `frogdb_scripting::sandbox::build_frogdb_lua_vm(Load | Execute)`
    is the one sandbox constructor (stdlib set, full 11-fn `bit`, `_real_G` global protection,
    cjson/cmsgpack, memory limit, timeout hook); loader is a thin capture-mode adapter; core
    lost ~850 lines. Closed a real sandbox hole beyond the survey: the load-time VM leaked
    `load`/`loadstring`/`print`/`collectgarbage` and lacked safe `os`/memory limits. 23 new
    parity/loader tests; scripting 66/66, core+scripting 700/700, server function/script 47/47.
    (5 redis-regression failures reproduce pre-change — proposal 28's known current-thread
    runtime mismatch, out of scope.)

33. [33-pubsub-subscription-kind.md](33-pubsub-subscription-kind.md) — **Implemented**
    (`79f7be0c`, `1e532c0b`, `f946f2e4`, `2cd9b0c0`): static `SubKindSpec` table (channel/
    pattern/sharded rows: routing policy + message/confirmation constructors) behind one
    `subscribe_kind`/`unsubscribe_kind` pair — six near-identical handlers now one-line
    delegations; admit-limit and unsubscribe-rearm invariants live once. Shard registration is
    batched (one `ShardMessage` per destination shard) and the previously-dead oneshot is now
    awaited as a registration barrier (publish-after-confirm can no longer race registration;
    `sleep(50ms)` test crutches deleted). `BROADCAST_SHARD` const; `send_pubsub_rpc` owns the
    cluster 2s-timeout + 4-arm mapping (shape mismatch is a typed error, not silent 0). 11 unit
    + 3 integration tests; server pubsub 56/56. Diagnosed the 6 pre-existing RESP3 pubsub
    regression failures as a test-harness bug (`Resp3TestClient::command` waits for a non-Push
    reply; confirmations are correctly Push-only since `5f288af2`) — fix queued for proposal 40.
34. [34-client-tracking-session.md](34-client-tracking-session.md) — **Implemented**
    (`561620d4`, `33836218`, `2dc13662`): tracking session cluster
    (`tracking_session_enable/disable/teardown_local`) owns enable + shard registration +
    teardown; RESET and connection close share one teardown (`ConnectionClosed`'s tracking half
    verified line-identical to `TrackingUnregister`, which TRACKING OFF keeps since it must not
    touch pub/sub). Fixed the 🔴 BCAST prefix bug per Redis tracking.c: prefixes accumulate
    across ON calls (flags/REDIRECT replaced), overlap checked against the accumulated union +
    within-batch, bare-BCAST empty-prefix quirks preserved, Redis mode-switch errors added.
    Bonus fix: re-enabling with REDIRECT no longer leaks a forwarding task per call. 10
    socket-free state tests + 3 integration; tracking suites 52/52, regression 52/53 (the 1 =
    the harness Push bug above).
38. [38-exec-outcome-executor.md](38-exec-outcome-executor.md) — **Implemented**
    (`8b565ea9`, `f086ffd7`, `7cfd8a70`): `TransactionOutcome` enum with an exhaustive
    `metric_label()` match (new variant = compile error until labeled) + single-exit recording —
    the ~13 hand-placed metric sites (incl. DISCARD's inline copy) collapse to one;
    `run_shard_transaction` owns the send/await/`TransactionResult` match once (watch-only and
    real branches were duplicates, ~110 lines deleted); deferred-command merge linear. Wire
    behavior pinned by new merge-order + watch-only reply-shape tests. transaction 23/23,
    exec-pattern regression 53/53.
39. [39-wait-coordinator.md](39-wait-coordinator.md) — **Implemented**
    (`41fde883`, `adbb5156`, `f6988148`): `WaitCoordinator` mirrors Redis `waitCommand` on
    proposal 18's `OffsetCoordinator` — snapshot, fast-path count, one `REPLCONF GETACK *`
    solicitation behind an `AckSolicitor` seam, quorum-or-deadline → verdict. The missing half
    the survey didn't flag: the replica never answered GETACK (`request_acks` had zero callers);
    the streaming loop now replies immediately with the post-ingest offset — WAIT latency drops
    from the ~1s spontaneous-ack floor (regression test: 8 SET+WAIT rounds < 2s). The
    `BlockingOp::Wait` sentinel path (3 files, 2 re-encodings, `keys: vec![]` carve-out) is
    deleted; dispatch intercepts WAIT by name like PSYNC. WAIT/ROLE now share the
    `get_streaming_replicas()` projection. `timeout 0` blocks until quorum (Redis semantics);
    WAIT-in-MULTI returns the count immediately. replication 102 (12 new), WAIT integration 19,
    wait_tcl 5.

35. [35-typed-metric-chokepoint.md](35-typed-metric-chokepoint.md) — **Implemented**
    (`f10afe0f`, `44e87848`, `305ec867`, `bd1d92fe`, `b9a9f879`, `c6e426dc`, `fef402f6`): one
    typed metric handle is both the only emission path and the registry entry (name, type, help,
    label schema fixed at definition), hosted in `frogdb-types` and driven by an upgraded
    `metrics-derive`. The three-to-four parallel systems (string consts, unused typed structs, ~105
    raw literals, the hand-scanned `DashboardMetrics`) collapse onto it; `frogdb_snapshot_epoch`
    (emitted but unregistered → invisible to dashboard-gen) is registered; dead `HELP` text
    (`Opts::new(name, name)`) and the label-arity panic risk (arity fixed by first-seen caller
    across 105 sites) are closed by construction. `lint-metrics-chokepoint` gate + a rewritten,
    un-ignored `metrics_usage` verification test; dashboard regenerated from the reconciled
    registry. Raw sites in the crates other wave agents held are enumerated as follow-ups in the
    proposal.
36. [36-ft-request-wire-types.md](36-ft-request-wire-types.md) — **Implemented**
    (`6ce13820`, `cb20209a`, `41998aef`, `c7fcc8c4`): FT.SEARCH/FT.AGGREGATE grammar is parsed
    once into shared `FtSearchOptions`/`FtAggregateRequest` (new `search/src/wire.rs`) instead of a
    coordinator partial-scan + per-shard full-parse that had to agree by convention; shard→
    coordinator results cross as typed `ShardSearchHit`/partial-aggregate values, retiring the
    `__ft_total__` sentinel + positional (`items[1]`-is-sort-value-iff-SORTBY) decode in the
    868-line `merge.rs`; `search_inner`'s 13 positional args become a request struct and the three
    pass-through wrappers are deleted. Client-facing reply shapes pinned unchanged; new full-grammar
    (HIGHLIGHT + SORTBY + LIMIT) cross-shard merge test.
37. [37-vll-lock-table.md](37-vll-lock-table.md) — **Implemented** (`6bf23678`, `7e8f925c`,
    `1bf2d21d`, `4a498709`): fixed the 🔴 `abort_remaining` bug (repro-first) — it iterated
    `result_rxs.len()+1..total` as *shard ids*, so on a phase-3 dispatch failure with sparse
    participants `[2,5,7]` it aborted the wrong shard and **permanently leaked** the real holders'
    locks (the survey's "ages out" was wrong: no timeout sweep is wired, so a leaked write intent
    blocks conflicting txns until restart). Now aborts the actual remaining `(shard_id, …)` pairs.
    `LockTable` (granted-flag-on-intent) replaces the two convention-synced maps (`intent_table.rs`
    + 245 lines of never-shared `AtomicU8`/CAS in `lock_state.rs`, both deleted); the vestigial
    `ExecuteSignal` channel is removed (scatter is now 4 phases, `ShardSink` trait narrowed). vll
    28/28, integration 46/46, turmoil chaos 81/81.
40. [40-deletions-small-seams.md](40-deletions-small-seams.md) — **Implemented** (9 commits,
    `2045505c`…`44e30f20`): a batch of small independent items. Deleted the dead `store/traits.rs`
    (323 lines, 5 sub-traits, zero impls/callers, methods duplicated verbatim into `Store`; item 1,
    `ffce1207`); deleted the never-transitioned `MigrationState` enum, documenting migration as a
    two-point ownership swap (item 2, `df0caef0`); a typed `StagedCheckpoint` now owns the
    `checkpoint_ready`/`CURRENT`/metadata install protocol across three crates and prunes the
    per-full-sync backup dirs that previously leaked a full DB copy forever (item 3, `9e23872c`);
    deleted the always-zero snapshot `num_keys` field (item 4, `2045505c`); folded the three
    per-tier RocksDB CF-accessor triplets into one `CfTier` resolver (item 5, `00483d61`); one
    `WalFailurePolicy` u8/string codec on the type (item 6, `df9de7e9`); connection-level
    `execute()` stubs (MULTI/EXEC/DISCARD/WATCH/UNWATCH) now return a loud
    `connection_level_execute_stub` error on a routing bug instead of a fabricated `+OK` (item 7,
    `6e22cf06`); the RESP3 test-harness `command()` is subscribe-aware so it accepts Push-only
    confirmations — fixing the 6 pre-existing pubsub regression failures that were a *harness* bug,
    not a server one, since round-4's `5f288af2` (item 8, `1856acd5`); and the last two hard-coded
    shard-0 pub/sub sites route through proposal 33's `BROADCAST_SHARD` (item 9, `d658c28b`).

\* `17cb35fc` carries both proposal 29 phase 1 and proposal 30 phase 1 (shared-index sweep
during concurrent implementation; content correct, attribution tangled). Several round-5 commits
carry small hunks from a concurrent wave agent for the same reason (shared working tree); content
is correct at HEAD, attribution is occasionally tangled — noted per-proposal.

## Correctness flags found during the review

Bugs adjacent to (but separable from) the proposals:

- **Replication offset never persisted after startup** — ~~tracker only updates an `AtomicU64`;
  staged `replication_metadata.json` has no reader~~ Fixed in `17f01c9d` (primary saves at
  shutdown + pre-snapshot hook; replica reconciles from staged metadata; corrupt/missing →
  full resync).
- **Partial resync never granted; checkpoints record offset 0** — ~~stale
  `state.replication_offset` read by FULLRESYNC/`can_partial_sync`~~ Fixed in `64f15bce`
  (offset captured from tracker before checkpoint cut; offset ≤ data invariant).
- **No replication backlog — partial resync structurally ungrantable** — ~~`+CONTINUE` would tail
  the live broadcast only, silently dropping `(requested, current]`. Gated explicitly behind
  `partial_sync_replay_supported()` (false). Implementing a backlog ring buffer (Redis
  `repl-backlog`) is the unlock.~~ Fixed by proposal 14 (`4daa18ab`…`aed8d0dd`):
  `PartialSyncReplay` owns the backlog and grants `+CONTINUE` end-to-end; the gate is deleted.
- **INFO replication `master_replid` reports zeros** — ~~built from `ctx.node_id` instead of the
  real replication id~~ Fixed in `99c8f91e` (`handle_info` patches from the shared
  `ReplicationState`). ~~`master_replid2`/`second_repl_offset` remain `0`/`-1` — no
  failover-continuity (replid2) concept yet; needed only when partial resync across failover
  becomes possible (see backlog flag above).~~ Fixed in `856107e6`: proposal 14 built the
  failover window (`ReplicationState::secondary_id`/`secondary_offset`, honoured by
  `window_contains`) and this commit reports it — INFO renders the previous primary's id as
  `master_replid2` and FrogDB's inclusive boundary as `second_repl_offset` when a window exists,
  falling back to the all-zero id and `-1` before any failover. The boundary is rendered
  verbatim (inclusive), not Redis's exclusive `master_repl_offset+1`, so the reported pair
  matches FrogDB's own `window_contains` continuation predicate.
- **Shard-count mismatch silently drops recovered data** — ~~`server/src/server/shards.rs:60`
  uses `unwrap_or_default()`~~ Fixed in `95da0256` (hard startup error at `RocksStore::open`,
  persisted count derived from `shard_*` column families).
- **Warm-tier toggle breaks reopen** — ~~data dir created with warm tier enabled fails to reopen
  with it disabled (`tiered_warm_*` CFs not reopened → RocksDB "column families not opened")~~ Fixed
  in `ff24a1a4` (hard `WarmTierMismatch` error; see proposal 13). Config toggling on→off is
  rejected; off→on is a benign first-enable.
- **Missing command behaviors** — ~~LREM emits no keyspace event; RPOPLPUSH/LMOVE never wake
  blocked list waiters; GEORADIUS STORE destination key not extracted.~~ Fixed by proposal 01
  (see the "class closed" entry below: 377/377 commands declare a `CommandSpec`).
- **Post-execution drift** — ~~transaction path skips keyspace metrics and keysizes flush; scatter
  BCAST tracking invalidation omitted.~~ Fixed in `6e483280`/`23469adc` (pre-fixes noted in
  proposal 03's entry; the pipeline seam makes the omissions unrepresentable).
- **Checkpoint staging untested** — ~~zero tests~~ Fixed in `165fc950` (8 tests: happy path,
  crash windows, idempotency, partial states). Testing found a real bug, fixed in `3e37ad7b`:
  an incomplete staged dir (no `CURRENT`) was installed anyway — live DB moved aside, fresh
  empty DB created in its place. Install now validates before touching the live dir.
- **STORE-destination WAL gap** — ~~arg-index WAL actions miss GEORADIUS/SORT STORE
  destinations~~ Fixed in `fc0dce48` (`WalStrategy::Dynamic` resolves from the command's own key
  extraction; restart-survival tests). Same-class WAL bugs also fixed: MSETEX, BITOP, XGROUP,
  XREADGROUP persisted a non-key arg or nothing. ZINCRBY waiter wake fixed in `182898b6`.
- **Missing command behaviors — class closed** — proposal 01 implemented (13 commits,
  `8d573510`…`182898b6`): 377/377 commands declare a `CommandSpec`; registry-wide exhaustiveness
  tests make silent event/WAL/wake omissions unrepresentable.
- **Cross-shard keyspace notifications lost** — ~~SUBSCRIBE registers on shard 0 (broadcast
  coordinator) but keyspace events emit on the key-owner shard; in multi-shard mode a keyevent for
  a key not on shard 0 never reaches the subscriber.~~ Fixed in `9ea530c0` (proposal 22:
  `KeyspaceNotificationCoordinator` forwards to shard 0 via `ShardMessage::PublishKeyspace`).
- **Keyspace hit/miss misclassification** — ~~`track_keyspace_metrics` classifies via
  `Response::Null`, but GET/HGET misses return `Response::Bulk(None)`~~ Fixed in `23469adc`
  (lookup-level classification via `CommandContext::record_keyspace_lookup`).
- **INFO stats hardcodes `keyspace_hits:0` / `keyspace_misses:0`** — ~~never wired to the real
  counters~~ Fixed in `02de350b` (`handle_info` patches from the Prometheus registry).
- **CONFIG RESETSTAT doesn't reset keyspace_hits/misses** — ~~Redis zeroes them; FrogDB's are
  Prometheus monotonic counters (reset would break `rate()`/`increase()` semantics). Needs a
  design decision (baseline-offset vs registry recreation) — deliberate divergence for now.~~
  Fixed in `2caff37f`/`15c471ec`/`aea7c402` (proposal 24: baseline-offset chosen — INFO reports
  `cumulative − baseline`, RESETSTAT advances the baseline, Prometheus counters stay monotonic).
- **Keyspace-stats command coverage gap** — ~~only GET/HGET/LINDEX/GETDEL/GETEX/MGET report
  lookups; Redis counts most read commands (LRANGE, SMEMBERS, ZRANGE, …). Enhancement, not a
  bug.~~ Fixed in `5f50a59e` (proposal 24: declarative `LookupSpec` extends coverage to 23 read
  commands; SCAN/RANDOMKEY stay uncounted, matching Redis).

### Round 2 flags (2026-06-13)

Found while writing proposals 07-13. All verified against the code; grouped by proposal.

- **Broadcast gathers have no timeout (proposal 07)** — ~~PUBSUB CHANNELS/NUMSUB/NUMPAT/SHARDCHANNELS/
  SHARDNUMSUB (`handlers/pubsub.rs:456,492,528,562,598`) and SCRIPT LOAD/EXISTS/FLUSH/KILL
  (`handlers/scripting/script.rs:76,120,133`) gather with a bare `rx.await` — unlike every
  `scatter.rs` handler, which wraps a `tokio::time::timeout`. A stalled internal shard hangs the
  connection forever. Send failures are also swallowed (`let _ = sender.send`), so a dropped shard
  silently under-reports.~~ Fixed in `d5b8de0d` (all routed through `ScatterGather::run`'s single
  shared deadline; `FailFast` errors instead of under-reporting; SCRIPT KILL keeps its sequential
  walk but its per-shard await is now timeout-bounded).
- **SCRIPT LOAD can diverge the per-shard Lua cache (proposal 07)** — ~~`handlers/scripting/script.rs:34-54`
  ignores all per-shard send results and awaits only shard 0, returning a SHA while other internal
  shards may never have received the script → EVALSHA can fail on some shards after a slow/dropped
  send.~~ Fixed in `d5b8de0d` (`ShardZeroReply` over `FailFast` requires every shard to ack before
  the SHA is returned).
- **Dead scatter code (proposal 07)** — ~~`handlers/scatter.rs:714-844` (`ScatterHandler`, five
  `merge_*`, `ScanResult`) and `scatter/strategies.rs:304-462` (`KeysStrategy`/`DbSizeStrategy`/
  `FlushDbStrategy`) have zero callers; tested copies of merge logic that diverge from the live
  inline versions. Delete in Phase 1.~~ Fixed in `dd892fc5` (deleted; zero callers reconfirmed).
- **In-transaction ACL denials skip the audit log (proposal 08, SECURITY)** — ~~queue-time key
  denials (`handlers/transaction.rs:458-462`) and channel denials (`:473-488`) return NOPERM but
  never call `log_key_denied`/`log_channel_denied`, unlike the live paths (`routing.rs:66`,
  `guards.rs:113`). EXEC does not recheck, so in-MULTI denials never reach `ACL LOG`.~~ Fixed in
  `d1d871a6` (queue-time key + channel checks route through `PermissionGuard`, so denials hit
  `ACL LOG` identically to the live paths).
- **NOPERM subcommand message uses a space, not a pipe (proposal 08)** — ~~`guards.rs:204` formats
  the error as `'config set'`; the same function logs it as `config|set` (`:181`) and
  `AclError::NoPermissionSubcommand` uses a pipe. Diverges from Redis.~~ Fixed in `a1bed2be` +
  `d1d871a6` (reply built from `AclError` Display over the lowercase fullname → `config|set`).
- **ACL DRYRUN mis-simulates key access (proposal 08)** — ~~`handlers/auth.rs:458-468` uses a
  heuristic (first arg only, always `ReadWrite`) instead of `handler.keys()` +
  `key_access_type_for_flags`. Mis-reports read-only commands, multi-key commands, and commands
  whose key isn't arg 0; the audit tool disagrees with real enforcement.~~ Fixed in `4854d00b`
  (uses the command's real key spec + `key_access_type_for_flags`).
- **VectorSet encode/decode bound asymmetry (proposal 09)** — ~~`serialization/search.rs:117-134`
  decode rejects `m>512`, `ef>4096`, `dim>65536`, but `VADD` parses `M`/`EF` with unbounded
  `parse_usize` (`vectorset/vadd.rs:135,161`) and `VectorSetValue::new_inner` applies no cap. A set
  created with e.g. `M 1000` serializes fine but fails to load on restart/replica full-sync →
  silent key loss.~~ Reproduced (usearch applies no clamp) and fixed in `048dc792`: `MAX_DIM`/
  `MAX_CONNECTIVITY`/`MAX_EF` enforced at the creation choke points (`new_inner`/`from_parts` + a
  VADD REDUCE guard); decode references the same consts (single source of truth) → round-trip
  invariant restored, oversized sets rejected at creation.
- **TimeSeries decode coerces unknown DuplicatePolicy to `Last` (proposal 09)** — ~~`serialization/
  timeseries.rs:96-104` catch-all `_ => DuplicatePolicy::Last`, unlike every other enum decode
  (which errors)~~ Fixed in `5b37cbbf` (unknown byte → `SerializationError::InvalidPayload`).
- **READONLY t-digest queries clone the digest on every call (proposal 11)** — ~~TDIGEST.QUANTILE/
  CDF/RANK/REVRANK/TRIMMED_MEAN (`tdigest.rs:364,408,450,494,660`) are flagged `READONLY`/`wal:
  NoOp` but call `get_mut` + `as_tdigest_mut` and invoke only read methods → copy-on-write clone of
  a shared digest on every query. Migrating to the read accessor removes the clone. (Same COW-before-
  check class affects ~19 write-path probabilistic sites; see proposal 11 Class A.)~~ Fixed in
  `2e7da3a4` (the five query methods actually mutated via lazy `flush()`, so they were made `&self` —
  flushing a local copy only when the unmerged buffer is non-empty, numeric results identical — and
  the commands now use the read accessor `get_tdigest(key)?` with no `get_mut`; the ~19 write-path
  Class-A sites were fixed across the per-family migrations).
- **Core stream files still carry `.ok_or(WrongType)` remnants (proposal 11)** — ~~proposal 02 killed
  the `.unwrap()` form but never banned `.ok_or`, so ~19 `as_*_mut().ok_or(WrongType)` survive in
  `commands/src/stream/*`. Fold into proposal 11 Phase 1 before the gate extension can go green.~~
  Fixed in `b53419d8` (all stream remnants migrated to `get_stream`/`get_stream_mut`; the wider sweep
  also cleared the sorted-set/geo/event-sourcing/json/string remnants in `a8276e50`/`c4185b62`, so
  the extended gate is green at 0 sites).
- **Active expiry: field-emptied key deletion is silent and under-counted (proposal 10)** — ~~when a
  hash is emptied by `purge_expired_hash_fields` and the key is deleted (`event_loop.rs:207-214`),
  the branch skips `emit_keyspace_notification` + `fire_key_expired` (unlike the key-level path at
  `:165-171`) and feeds only `frogdb_fields_expired_total`, not `expired_keys` (`:218-237`)~~ Fixed
  in `911b2525` (emit `del` + fire the expired probe for field-emptied keys) + `6017333e` (count
  them in `expired_keys` via `ExpiryResult::keys_expired()`, no double-count with the field counter).
- **Active expiry time budget excludes the scan (proposal 10)** — ~~`get_expired_keys`/
  `get_expired_fields` clone every due entry into a `Vec` up front (`store/noop.rs:99-112,223-233`)
  before the 25ms budgeted loop (`event_loop.rs:138`), so a large TTL avalanche can stall the shard
  event loop past budget~~ Fixed in `ffdd156f` (bounded `get_expired_*_limited`; `run_cycle` scans
  in capped batches with the budget re-checked between/within batches).
- **Blocking timeout returns the wrong RESP2 nil shape (proposal 12)** — ~~the timeout/channel-drop
  paths emit `Response::Null` (`$-1`) for every op (`handlers/blocking.rs:157,170,125,130`; shard
  `core/src/shard/blocking.rs:149`), but BLPOP/BRPOP/BLMPOP/BZPOPMIN/BZPOPMAX/BZMPOP/XREAD return a
  null *array* (`*-1`, `Response::NullArray` exists at `protocol/response.rs:162`) in RESP2. The
  timeout path discards the op so it can't pick the shape. RESP3 unaffected; BLMOVE/BRPOPLPUSH
  correctly want `$-1`.~~ Fixed in `98309ea3` (`BlockingOp::timeout_reply()` is the single audited
  nil-shape site; the shard's coarse safety-net calls it) + `40e88bcb` (`WaitOutcome::into_response`
  threads the op through the server timeout/unblock arms so the precise reply picks the shape; per-
  family `*-1`/`$-1` wire-shape regression tests over a raw RESP2 socket).
- **Blocking lost-element timeout race (proposal 12)** — ~~two independent timeout authorities (server
  `select!` vs shard's 100ms `check_waiter_timeouts`, `event_loop.rs:23`). If a push is processed by
  the shard before the server's `UnregisterWait` arrives, `complete_blocked_waiter`
  (`core/src/shard/blocking.rs:665-672`) pops the list element and sends it into the abandoned
  oneshot after the server already returned a timeout nil → element removed from the store, delivered
  to nobody. Narrow but genuine data loss.~~ Fixed in `733e4d67` (the server is now the single
  timeout authority; the shard re-validates every popped waiter in `drive_satisfaction` and drops any
  whose deadline has elapsed or whose receiver is gone *without* consuming store data. The shard
  reads the clock strictly before it pops and the coordinator's `biased` select favours a delivered
  response over a simultaneous deadline, so every popped element is guaranteed to reach a still-
  waiting receiver; `check_waiter_timeouts` is demoted to a coarse GC that never consumes data.
  Concurrency tests reproduce the race (push vs dropped receiver, push vs elapsed deadline)).
- **Warm-tier toggle breaks reopen (proposal 13)** — ~~`rocks/mod.rs:74-88` derives `all_cf_names`
  from the current `warm_enabled`; created-with-warm then reopened-without skips `tiered_warm_*` CFs
  → open fails at `:126-134` with "Column families not opened". (Inverse off→on is benign — `create_cf`
  is guarded at `:136`, correcting the original "duplicate CFs" hypothesis.)~~ Fixed in `ff24a1a4`
  (hard `WarmTierMismatch` on→off; off→on benign; both directions tested) and folded into
  `ColumnFamilyManifest::reconcile` (`156e7890`).
- **`list_cf(...).unwrap_or_default()` silently disables both reopen guards (proposal 13)** —
  ~~`rocks/mod.rs:91`: a failed CF enumeration (transient I/O, permissions, damaged MANIFEST) yields
  an empty `existing_cfs` even when the DB exists, bypassing the shard-count guard
  (`count_persisted_shards(&[]) == 0`) and building an empty descriptor set → confusing open failure
  instead of an actionable error~~ Fixed in `ff24a1a4` (the `list_cf` error propagates as a named
  `RocksError`).

### Round 3 flags (2026-06-15)

Found while writing proposals 14-20; grouped by proposal. The two 🔴 replication bugs were
independently verified against the source.

- 🔴 **Replication offset increment mismatch (proposal 18) — CONFIRMED** — ~~the primary advances the
  offset by RESP **payload** bytes (`primary/mod.rs:271-272`, `bytes_len = resp_bytes.len()`) but the
  replica advances by the **full frame** including the 18-byte header (`replica/streaming.rs:32`,
  `frame.encoded_size()` = `FRAME_HEADER_SIZE(18) + payload`). Primary and replica count the offset
  in different units → the replica drifts ahead 18 bytes/frame, breaking ACK comparison, saturating
  `replica_lag` to 0, letting WAIT succeed before data arrives, and falsely rejecting a caught-up
  replica from any future partial-sync window.~~ Fixed in `944e8882` (replica advances by
  `OffsetCoordinator::frame_advance` = `frame.payload.len()`, the one shared unit).
- 🔴 **Full-sync handoff drops writes during checkpoint transfer (proposal 14) — CONFIRMED** —
  ~~`handle_full` captures `snapshot_offset`, cuts + streams the whole checkpoint, and only then
  (`start_streaming`) calls `wal_broadcast.subscribe()`. `broadcast::subscribe()` delivers only future
  frames, so writes in the window between the checkpoint cut and the subscribe are in neither the
  checkpoint nor the live stream → silently lost on full-sync under write load.~~ Fixed in `312f57fb`
  (gap-free handoff: subscribe BEFORE reading the head, replay `(snapshot_offset, current]` from the
  backlog, then forward the live tail skipping `sequence <= resume_offset`; deterministic
  writes-during-fullsync no-loss test in `89f54074`).
- **REPLCONF GETACK built with offset 0 (proposal 18)** — ~~`request_acks` builds the frame with
  sequence `0` instead of the current offset, and bypasses `broadcast_command` so it doesn't advance
  the primary offset while the replica counts it~~ Fixed in `944e8882` (GETACK now advances + stamps +
  backlogs like any stream command, so both ends agree).
- **Dead second offset writer (proposal 18)** — ~~`PrimaryReplicationHandler::increment_offset`
  updates both state and tracker but has no callers; the live path touches only the tracker~~ Removed
  in `1b61649a` (the coordinator owns the contract).
- **FT.CREATE / FT.ALTER / FT.DROPINDEX persistence failures return OK then lose state on restart
  (proposal 15)** — ~~create-persist + initial-scan-commit failures swallowed → OK; alter commit +
  persist failures swallowed → OK (schema reverts on restart); drop meta-delete failure leaves
  metadata (restart resurrects an empty index); and recovery silently skips a corrupt/undeserializable
  index with only a `warn!`~~ Fixed in `38258ea5` (persist-before-OK: create/alter/drop roll back +
  error on persist/commit failure) + `d041912e` (recovery returns a
  `RecoveryOutcome::{Corrupt,Undeserializable}` taxonomy; CF-read failure is fatal).
- **SSUBSCRIBE bypasses migration routing (proposal 17)** — ~~`cluster_pubsub::get_slot_owner_addr`,
  consumed at `handlers/pubsub.rs`, decides redirects from slot ownership alone: importing-target +
  ASKING returns MOVED instead of serving locally, never emits ASK, and an *unassigned* slot
  subscribes locally instead of CLUSTERDOWN~~ Fixed in `92bfc083` (SSUBSCRIBE routes through
  `coordinator.route()` → `RouteDecision::to_response`; `get_slot_owner_addr` deleted in `736a4115`).
- **MOVED format drift, IPv6 (proposal 17)** — ~~`guards.rs` formats `MOVED {slot} {ip}:{port}` while
  `pubsub.rs` uses `MOVED {slot} {SocketAddr}` → divergent for IPv6; plus direct `slot_assignment.get`
  reach-ins bypassing the `get_slot_owner` accessor~~ Fixed in `bfb49b6a`/`736a4115` (the
  `slot_migration/redirect.rs` seam is the single authoritative formatter — IPv6 bracketed; reach-ins
  routed through `get_slot_owner`).
- **Vector field-state desync (proposal 19)** — ~~`index_vector` discards the `usearch.add` result yet
  inserts into the maps; the replace path overwrites `reverse_map` but never removes
  `vector_key_map[old_id]`; `delete_vector` discards `usearch.remove`; and `create_vector_indexes`
  loads the usearch file and `_map.json` independently → on partial load `vector_next_id` resets to 0
  while usearch keeps ids, causing id-collisions returning the wrong key; `save_vectors` writes the two
  sidecars non-atomically~~ Fixed in `7cf34fd5` (bijection + all-or-nothing index/delete, propagated
  add/remove results) + `77349ebf` (`try_load` both-or-neither, temp+fsync+rename save) — all in the
  `VectorFieldManager`.
- **Config validation duplicated (proposals 16 / 20)** — ~~the valid eviction-policy list is hardcoded
  in CONFIG SET, again in `MemoryConfig::validate`, and a third time via `EvictionPolicy::FromStr`;
  `build_eviction_config` `unreachable!()`s on parse failure while `notify_eviction_change` silently
  falls back to `NoEviction` (divergent); `durability-mode`/`wal-failure-policy` lists duplicated
  between setters and `PersistenceConfig::validate`; shard propagation keyed off a hardcoded
  `eviction_params` name list~~ Fixed across proposal 16: legal-value lists deduped onto enum/const
  sources of truth (`ef8a092f`/`70656956`/`ccf01e13`), `build_eviction_config` failure mode unified
  (`ef8a092f`), shard propagation driven from param defs (`eb4e29e9`).
- **Eviction sample metric missing `policy` label (proposal 20)** — ~~`frogdb_eviction_samples_total`
  is incremented with only a `shard` label in all three samplers, unlike sibling eviction metrics that
  carry `policy`~~ Fixed in `01cd1bda` (the single `sample_with_ranker` increment now carries
  `policy`).

### Round 4 flags (2026-06-17)

Found while writing proposals 21-28; grouped by proposal. The three 🔴 bugs were independently
verified against the source. All ten closed by the round-4 implementation (2026-06-18).

- 🔴 **Cross-shard keyspace notifications lost (proposal 22) — CONFIRMED** — ~~SUBSCRIBE registers
  the subscription on `shard_senders[0]` (`pubsub.rs:62-74`) but `emit_keyspace_notification`
  (`keyspace_notify.rs:51,58`) publishes to the key-owner shard's own `self.subscriptions`. In
  multi-shard mode a keyevent for a key not on shard 0 reaches no subscriber.
  `test_lrem_emits_keyspace_notification` is pinned to `num_shards: Some(1)` to dodge it.~~ Fixed
  in `9ea530c0` (coordinator routes to shard 0 via `ShardMessage::PublishKeyspace`; the LREM test
  is unpinned at 4 shards in `8b642ccc`).
- 🔴 **MOVED redirect breaks IPv6 in the blocking path (proposal 23) — CONFIRMED** —
  ~~`core/shard/blocking.rs:112-117` formats `MOVED {slot} {ip}:{port}` inline (unbracketed),
  unlike the `slot_migration/redirect.rs` seam round 17 made authoritative. An IPv6 owner address
  yields a malformed redirect.~~ Fixed in `5072ebd4` (formatters moved to `frogdb-types::redirect`,
  the single owner both crates use; IPv6 bracketed as `[addr]:port`; `lint-redirect-seam` bans
  inline rebuilds).
- 🔴 **Lua cross-shard fallthrough silently writes to the wrong shard (proposal 28) — CONFIRMED** —
  ~~`lua_vm.rs:108-152`: the cross-shard `Err(_)` arm falls through to local execution instead of
  erroring, so a key owned by another shard is written locally. Production runs `new_multi_thread`
  (`main.rs:208`) while `#[tokio::test]` runs current_thread, so the test suite masks the bug.
  Key-extraction also diverges: `executor.rs:302,313` uses `slot_for_key` over all keys vs
  `lua_vm.rs:116-118` `shard_for_key` over the first key.~~ Fixed in `fd194cf2`
  (`ScriptCommandGate::dispatch` hard-errors on an unavailable runtime — never a local write;
  `classify` extracts keys once for both checks).
- **XAUTOCLAIM corrupts the pending count on deleted entries (proposal 27)** — ~~XAUTOCLAIM
  reassigns a deleted entry to the new consumer (`pending_count += 1`) then
  `group.pending.remove(id)` at `pending.rs:486` without a matching decrement, breaking the
  `sum(consumer.pending_count) == group.pending.len()` invariant. XCLAIM's `TIME` argument is
  parsed into `_time_ms` then discarded.~~ Fixed in `75da9130` (`drop_missing_pending` decrements
  as it removes) and `2aca32a9` (TIME honored via `claim_pending`).
- **Snapshot creation leaks tmp dirs and installs incomplete snapshots (proposal 25)** —
  ~~`rocks_coordinator.rs:210-231`: a search-data copy failure is `warn!`-and-continue → an
  incomplete snapshot is installed; the metadata-write + two renames use `?` → a crash between
  them leaks `.snapshot_NNNNN.tmp`.~~ Fixed in `cc4d865a` (RAII `TmpDirGuard` cleans on any early
  return + reclaims stale `.tmp` dirs) and `7462374d` (search copy failure aborts the snapshot,
  preserving the previous one).
- **Persistence INFO section ships placeholder zeros (proposal 21)** — ~~`info.rs:292-341` emits
  WAL `0`s that no later patch overwrites, contradicting the STATUS JSON's real values.~~ Fixed in
  `d6d8008b` (the Persistence `InfoSection` renders aggregated live WAL lag from the shard
  snapshot; fields honestly absent when persistence is off). Note: the flag's premise that STATUS
  JSON aggregates WAL was itself wrong — it hardcodes `"enabled": false` (`handlers/status.rs`).
- **RESP3 pub/sub confirmations are Push in EXEC but Array elsewhere (proposal 26)** — ~~the EXEC
  path patches confirmations to RESP3 Push (`transaction.rs:559-567`) but the 9 inline
  confirmation builders in `pubsub.rs` emit Array even under RESP3.~~ Fixed in `5f288af2`
  (`PubSubConfirmation` shapes at construction — Push under RESP3 in every path; the EXEC patch
  is deleted).
- **CONFIG RESETSTAT can't reset keyspace_hits/misses (proposal 24)** — ~~still open from the
  original review.~~ Fixed in `15c471ec` + `aea7c402` (baseline-offset; see the struck entry in
  the original flags list above).
- **Keyspace-stats command coverage gap (proposal 24)** — ~~still open; only six commands report
  lookups.~~ Fixed in `5f50a59e` (23 read commands via declarative `LookupSpec`).
- **Dead `OnWriteHook` (proposal 25)** — ~~`handle.rs:44-51` defines the hook but only
  `NoopOnWriteHook` implements it (zero real adapters).~~ Deleted in `22db53f8`.

### Round 5 flags (2026-07-04)

Found while writing proposals 29-40; grouped by proposal. All fixed by the round-5 implementation.

- 🔴 **WAL flush errors swallowed on the common path (proposal 29)** — ~~size-threshold, recv-timeout,
  and disconnect-drain flushes discarded the write result (`let _ = do_flush(...)`); only an
  explicit `Flush` propagated it. `write_set`/`write_delete` returned `Ok(seq)` before the batch
  flushed, so under the default `Continue` policy a failed RocksDB write was acked to the client and
  lost silently.~~ Fixed by proposal 29 (sequence-anchored `flush_through`; failed batches surface
  via INFO/Prometheus/logs).
- **Dead `record_sync` → `sync_lag_ms` climbs forever (proposal 29)** — ~~zero callers; the field was
  seeded once at construction and its lag grew unbounded, surfaced through INFO/status/gauge.~~
  Deleted end-to-end.
- 🔴 **`set` leaves the expiry indexes stale → active expiry deletes a persistent key (proposal 30)**
  — ~~`HashMapStore::set` never cleared `expiry_index`/`field_expiry_index` on overwrite, and
  `run_cycle` deleted on the trusted index with no `is_expired()` re-check. `SET k EX 100` then
  `MSET k v2` left `(now+100,"k")` in the index; at +100s the persistent key was silently
  deleted.~~ Fixed by proposal 30 (`replace_entry` seam + deadline re-check guard).
- **`memory_used` drift on in-place growth (proposal 30)** — ~~`get_mut` growth (RPUSH/SADD/HSET/…)
  never updated `memory_used`; delete recomputed live size → the observed "Memory accounting
  underflow" `warn!`. maxmemory/eviction fired late or never.~~ Fixed by proposal 30 (deferred
  refresh applies the memory delta; charge/refund symmetric).
- 🔴 **Failover is a non-atomic multi-write saga (proposal 31)** — ~~manual and auto paths each
  issued RemoveNode → per-range AssignSlots → IncrementEpoch as separate Raft entries; a leader
  crash mid-saga left slots ownerless (CLUSTERDOWN for live data) and per-range failures were only
  logged. Graceful failover additionally never transferred slots (error hidden in an unchecked
  `Ok(resp.data)`) and force failover never promoted the successor's role.~~ Fixed by proposal 31
  (composite `ClusterCommand::Failover` applied atomically).
- **Lua sandbox built twice, divergently (proposal 32)** — ~~load-time and execution-time VMs had
  different `bit` libraries and global-protection schemes; a library using `bit.tohex`/`rol`/`bswap`
  at top level behaved differently between load and execute. The load VM also leaked
  `load`/`loadstring`/`print`/`collectgarbage` (a real sandbox hole).~~ Fixed by proposal 32 (one
  `build_frogdb_lua_vm` constructor).
- **BCAST tracking prefixes replaced instead of accumulated (proposal 34)** — ~~`CLIENT TRACKING ON
  BCAST PREFIX a` then `… PREFIX b` left only `b`; the overlap-against-existing check was checking a
  set the write path then discarded. Diverged from Redis, which accumulates.~~ Fixed by proposal 34.
- **Redirect-tracking task leak (proposal 34)** — ~~re-enabling tracking with REDIRECT spawned a new
  forwarding task per call without aborting the previous one.~~ Fixed by proposal 34.
- **WAIT never solicits GETACK; replica never answers it (proposal 39)** — ~~the `REPLCONF GETACK`
  arm was a no-op stub and the replica's stream loop skipped all REPLCONF, so WAIT waited on the
  ~1s spontaneous-ack cadence. `request_acks` had zero callers.~~ Fixed by proposal 39 (WAIT
  solicits once; replica answers immediately with the post-ingest offset).
- 🔴 **VLL `abort_remaining` uses dispatch indices as shard ids → permanent lock leak (proposal 37)**
  — ~~on a phase-3 dispatch failure with sparse participants it aborted the wrong shards and never
  aborted the real holders; no timeout sweep is wired, so the leaked intents blocked conflicting
  txns until restart (the survey's "ages out" was wrong).~~ Fixed by proposal 37 (abort by real
  `(shard_id, …)` pairs).
- **`frogdb_snapshot_epoch` emitted but unregistered; label-arity panic risk (proposal 35)** —
  ~~a metric absent from `ALL_METRICS` was invisible to dashboard-gen, and label names derived from
  first-seen caller args across 105 sites could panic the `prometheus` crate on a mismatched
  set.~~ Fixed by proposal 35 (typed chokepoint fixes name/label schema at definition).
- **Snapshot `num_keys` always 0; per-full-sync backup dirs leak a full DB copy (proposal 40)** —
  ~~`mark_complete(0, size)` shipped a lie; `*_backup_<ts>` install backups were never cleaned.~~
  Fixed by proposal 40 (field deleted; `StagedCheckpoint` prunes backups to 1 generation).
- **Connection-level `execute()` stubs fabricate success (proposal 40)** — ~~MULTI/EXEC/DISCARD/
  WATCH/UNWATCH returned a placeholder `+OK`/empty array; a routing bug reaching one would be
  silent.~~ Fixed by proposal 40 (loud `connection_level_execute_stub` error).
- **RESP3 test-harness `command()` hangs on Push-only confirmations (proposal 40)** — ~~since
  round-4's `5f288af2` made subscribe confirmations correctly Push-only, the harness's
  wait-for-non-Push loop timed out, failing 6 pubsub regression tests. A harness bug, not a server
  one.~~ Fixed by proposal 40 (subscribe-aware `command()`).
