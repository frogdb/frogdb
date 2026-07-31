# frogdb-server cluster wiring, slot migration, failure detection — testing gap audit (round 2)

## Scope

Audited (all under `frogdb-server/crates/server/src/`):

| path | LOC | note |
|---|---|---|
| `cluster_bus.rs` | 389 | accept loop, TLS sniff/dual-accept, `BusRpc` dispatch |
| `cluster_flags.rs` | 196 | `ClusterRuntimeFlags` live atomics + `SelfFenceGate` |
| `cluster_pubsub.rs` | 324 | `ClusterPubSubForwarder`, broadcast/forward fan-out |
| `slot_migration/mod.rs` | 153 | coordinator, Raft commit seam |
| `slot_migration/routing.rs` | 299 | `route_with_snapshot`, `route_queued_batch` |
| `slot_migration/events.rs` | 48 | completion → per-shard `ClusterMsg::SlotMigrated` |
| `slot_migration/validator.rs` | 160 | `SlotValidator` |
| `slot_migration/tests.rs` | 492 | inline unit tests |
| `migrate.rs` | 524 | `MIGRATE` command + client |
| `failure_detector.rs` | 942 | probe loop, `HealthTable`, auto-failover |
| `role_manager.rs` | 1029 | promote/demote, replica-stream seam |

**Coverage (from `target/llvm-cov/depth/depth.json`, 2026-07-28): 1974 lines, 1708 covered
= 86.5%.** Depth classes over the functions in these files:
`untested 133`, `single-test 112`, `well-covered 101`, `monoculture 58`, `covered 17`.

Percentage is misleading here. The uncovered residue is concentrated almost entirely in the
*failure* and *transition* paths: auto-failover execution, bus I/O error handling, migration
lifecycle error arms, and the real (non-fake) replica streamer. The well-covered 101 are
overwhelmingly pure decision functions with inline unit tests.

Out of scope per dispatch: `crates/cluster/`, `server/src/replication*`, `server/src/recovery/`,
`vll_adapter.rs`.

## Summary

The *decision* layer of cluster mode is genuinely well tested — `route_with_snapshot`,
`route_queued_batch`, `HealthTable`, `compute_replica_score`/`select_failover_target`, and
`SlotValidator` are all pure functions with dense inline tables (routing.rs is at 100% line
coverage). The *wiring* layer around them is not. `trigger_auto_failover` — the function that
turns a detected failure into a `ClusterCommand::Failover` — has literally zero test coverage
(`r=104/0, t=0, class=untested`), and the one integration test named after it asserts only
`cluster_state == "ok"` while leaving `auto_failover` at its default of `false`, so the code
under test never runs at all.

The single highest-value finding is not a coverage hole but a probable **correctness
divergence** the coverage hides: a single-key write to a `MIGRATING` slot whose key has
*already* been migrated away is served locally and acknowledged `+OK`, because the TRYAGAIN
stage is gated on `keys.len() >= 2` and the ASK-on-nil conversion runs only *after* execution
and only on a nil reply. Slot completion then discards the source's copy. Redis returns `-ASK`
in exactly this case. No test writes to an already-migrated key; `test_e2e_migration_concurrent_writes`
carefully writes only to keys still present on the source.

The kind of bug that escapes today: anything on a cluster transition edge — writes accepted
during the migration window, a failover that picks the wrong replica or never fires, a bus peer
that sends a malformed frame, a pubsub broadcast that counts a `FAIL`ed node. Several
replication-shaped gaps in this area are **blocked** on the `.scratch/replication-cluster-rework/`
PRDs (cluster-mode data replication is inert: default-config cluster primaries refuse PSYNC), and
those are flagged explicitly rather than proposed as tests that cannot pass.

## Existing test inventory

| surface | covers | strengths | blind spots |
|---|---|---|---|
| `slot_migration/tests.rs` (492 L) | 11 `route_with_snapshot` cases, 7 `to_response`, 12 `route_queued_batch` | exhaustive truth table over owner/migration/asking/readonly; CROSSSLOT and keyless-batch arms both pinned | coordinator lifecycle (`begin`/`complete`/`cancel`), the Raft commit error arms, and the event dispatcher are untouched; `route_with_snapshot` is `monoculture` (11 tests, all from this one file) |
| `validator.rs` inline (8 tests) | `same_shard` / `same_slot` | property test: `same_slot ⇒ same_shard` over 200 keys × 6 shard counts | — |
| `failure_detector.rs` inline | `HealthTable` state machine, `compute_replica_score`, `select_failover_target` | threshold latching, staleness boundary, quorum arithmetic, never-seen/failed nodes, priority-0 exclusion — all sharp | the probe loop → quorum gate → `MarkNodeFailed` → `trigger_auto_failover` wiring is untested end to end |
| `role_manager.rs` inline (12+ tests) | `promote`/`demote` via injected `FakeReplicaStreamer` | self-fence arming reset, boot-handler stop, link_up matrix, shared-offset wiring | the real `RealReplicaStreamer::start` (`r=31, t=0`) and `RealReplicaStream::drop` (`r=11, t=0`) never run |
| `cluster_bus.rs` inline (4 tests) | `choose_transport` truth table + a bind-failure smoke test | migration-flag semantics pinned as a pure fn | no test drives a real connection: peek timeout, TLS handshake timeout, malformed frame, `send_rpc_response` failure, and the entire `HealthProbe` response arm (267-270) are uncovered |
| `cluster_pubsub.rs` inline (5 tests) | `send_pubsub_rpc` generic seam | shape mismatch, transport error, timeout (`start_paused`), extractor discrimination | `all_nodes.len() <= 1` early return (137) and the `flags.fail` skip (151) both uncovered |
| `server/tests/integration_cluster.rs` (~12.9k L, ~200 tests) | broad multi-node behaviour over RESP | round 1 hardened the source-crash migration tests (issue 17): `assert_slot_converges_to_single_owner` / `assert_key_served_by_exactly_one` / `assert_key_served_by_no_running_node` at :3606/:3663/:3714 | an assertion-weak tier survives around replication/failover — see F4 |
| `test-harness::cluster_harness` (1100 L) | `start_cluster`, `add_node`, `add_replica`, kill/restart/shutdown, convergence waiters, `cluster_state()`/`raft()` seams | good primitives; `fake_node_version` exists for version-skew tests | `add_replica` (:969) waits only for role propagation, not for any data to arrive — it cannot, see F4 |
| `testing/jepsen` | `slot_migration.clj` checker (strengthened in round 1, issue 08) | now actually asserts | no failover-during-migration workload |

## Findings

### F1: A single-key write to a MIGRATING slot whose key already migrated is `+OK`'d, then lost

- **Severity** 5 — an acknowledged write is silently discarded when the migration completes. Silent data loss on an ordinary ops operation.
- **Likelihood** 4 — this is the *normal* window of every slot migration: for each key, the interval between `MIGRATE`-ing it and `SETSLOT NODE`. Any client still writing to that key hits it.
- **Effort** 3 — existing cluster harness; the shape of `test_e2e_migration_concurrent_writes` already does 90% of the setup.
- **Priority** 20
- **Evidence**: `frogdb-server/crates/server/src/connection/guards.rs:750` — `check_migrating_multikey` returns `None` when `keys.len() < 2`, so a single-key command never reaches the TRYAGAIN/ASK logic. `connection/dispatch.rs:630-680` — stage order is `ClusterSlotValidation` → `MigratingTryAgain` (multi-key only) → `Execute` → `migrating_ask_for_nil` at :677, i.e. the ASK conversion runs *post*-execution. `guards.rs:152` — `migrating_ask_for_nil` only fires on a nil response, and a `SET` returns `+OK`, not nil. `slot_migration/routing.rs:143` — the source's decision is `LocalServeMigrating` = serve locally. Redis `getNodeByQuery` returns `-ASK` whenever `missing_keys > 0 && existing_keys == 0`, regardless of key count and regardless of the command being a write. Completion clears the source's copy (`integration_cluster.rs:8614`, `test_slot_ownership_transfer_clears_source_keys`). `test_e2e_migration_concurrent_writes` (`integration_cluster.rs:5817-5942`) writes to `key2` only while it is *still present* on the source, so it never enters this window.
- **Proposed test**: two-node cluster; open a migration for slot S; `MIGRATE` key K to the target; then, from a client on the source, `SET K v2`. Assert the source replies `-ASK <slot> <target-addr>` (not `+OK`). Then complete with `SETSLOT NODE` and assert `GET K` on the target returns whatever the *last acknowledged* write was — i.e. the acknowledged value is never lost. Add the read variant (`GET K` after migration → already covered by `test_expired_key_during_migration_returns_ask`, so keep the new test write-focused) and a `DEL K`/`INCR K` variant to prove the rule is not `SET`-specific.
- **Boundary**: 5 — the behaviour is the interaction of two live nodes' cluster state with a real redirect over RESP; nothing below level 5 can produce a real MIGRATING slot with a partially-migrated keyspace.
- **Note**: if the test fails (expected), this is a **product bug to file**, not just a test to add. The fix is to move the migrating check ahead of `Execute` for single-key commands too, gated on a key-presence probe, which is exactly what the batch path already does at `guards.rs:910`.

### F2: `trigger_auto_failover` has zero coverage; the test named for it never enables the flag

- **Severity** 5 — this function decides which replica is promoted after a primary dies. A wrong choice promotes a lagging replica and loses acknowledged writes; a silent no-op leaves the shard unavailable.
- **Likelihood** 4 — fires on every primary failure once `auto_failover` is enabled, which is the point of enabling it.
- **Effort** 4 — needs a multi-node cluster with a killed primary and the flag on; or an injected `net`/`ClusterWriter` seam for a unit-level test.
- **Priority** 19
- **Evidence**: `failure_detector.rs:330-464` — `trigger_auto_failover`; depth class `untested`, `r=104/0, t=0` in both instantiations. It probes each replica's offset via `net.health_probe()`, scores with `select_failover_target`+`effective_priority`, and writes `ClusterCommand::Failover { force: true }` with `MAX_ATTEMPTS = 3` (`:423`) and 500 ms backoff. `failure_detector.rs:303` (`if self.flags.auto_failover()`) and `:295-296` (`MarkNodeFailed` rejected by the state machine) are also uncovered. `integration_cluster.rs:7773` `test_auto_failover_selects_most_caught_up_replica` asserts only `info.cluster_state == "ok"`, its own comment concedes it cannot measure offsets, and `auto_failover` defaults to `false` so the function never executes. The one turmoil test that does enable the flag uses a replica-less victim, so `trigger_auto_failover` early-returns before scoring.
- **Proposed test**: split in two. (a) **Level 1/2**: extract or drive `trigger_auto_failover` against a fake `ClusterNetwork` returning fixed `health_probe` offsets and a fake writer capturing proposals. Assert the captured `ClusterCommand::Failover` names the highest-scoring replica, that a `priority == 0` replica is never chosen even when most caught up, that with zero eligible replicas no command is proposed, and that a transient `Err` on attempts 1-2 still results in exactly one successful proposal (retry loop) while 3 failures propose nothing further. (b) **Level 5**: three-node cluster, `auto_failover` enabled, kill the primary, assert a specific node becomes primary and that `CLUSTER NODES` converges to exactly one primary for the slot range.
- **Boundary**: 1/2 for the selection logic (it is a pure decision over probe results — do not pay for a cluster to test arithmetic), 5 only for the "a failover actually happens end to end" assertion. Both are needed; today neither exists.
- **OPTIONS**: see below.

**OPTIONS (F2)**
- **A — unit-only with an injected network/writer fake.** Fast, deterministic, covers all 104 regions including the retry loop. Risk: asserts on the proposed `ClusterCommand` rather than the observable outcome, so a break in the Raft-apply path still escapes.
- **B — cluster-harness end-to-end only.** Asserts the real outcome. Risk: slow, and it cannot deterministically produce "replica A is 5000 bytes behind replica B", so the *selection* logic stays untested — which is the part with 104 uncovered regions.
- **C — both (recommended).** A for selection + retry semantics; one B test for "auto_failover on ⇒ a new primary is elected and CLUSTER NODES converges". This is the only combination that covers the arithmetic *and* proves the wiring is connected. Cost is roughly A's effort plus one integration test.

### F3: Assertion-free cluster replication tests mask an inert subsystem

- **Severity** 4 — these tests are the only thing standing between "cluster replicas receive data" and a regression; today they would pass with the feature entirely removed (and in fact do — it *is* entirely removed).
- **Likelihood** 4 — anyone reading the suite concludes cluster replication is tested.
- **Effort** 2 — editing existing integration tests; the *honest* version is cheap, the *full* version is blocked.
- **Priority** 18
- **Evidence**: `integration_cluster.rs:2589` `test_replica_receives_writes` — the body is `eprintln!` only, zero assertions; it accepts a MOVED error, a wrong value, and a correct value as equally fine. `integration_cluster.rs:2706` `test_promoted_replica_has_all_data` — computes `found_count` and never asserts on it, and counts a `MOVED` *error* reply as "found". Same defect class round 1 fixed for the source-crash tests (issue 17) but left in this tier.
- **BLOCKED**: a test asserting that a cluster replica actually holds primary data cannot pass today. Per `.scratch/replication-cluster-rework/wait-cluster-mode.md` §1-2.2, `PrimaryReplicationHandler` is gated on `config.replication.role == "primary"` (`replication_init.rs:57,:101,:201-205`), cluster bootstrap only ever constructs `NodeInfo::new_primary` without setting that config key (`cluster_init.rs:79-81,:340-355,:387-395`), and `replication.role` is `#[param(skip)]` so it is not runtime-settable (`config/src/replication.rs:144-146,:299-301`). Cluster nodes start as `ServerRole::Standalone` (`test-harness/src/server.rs:468-472`). A cluster primary therefore **refuses the PSYNC that `CLUSTER REPLICATE` tells its replica to open**.
- **Proposed test**: two-step. **Now**: rewrite both tests to assert only what is observable and true — that `CLUSTER REPLICATE` propagates the role (replica appears with a `slave` flag and the correct master ID in `CLUSTER NODES` on every node), and that a read on the replica without READONLY returns `MOVED` to the primary. Then add an explicit `#[ignore]`d or comment-pinned assertion documenting that data does *not* arrive, referencing `wait-cluster-mode.md`, so the day the rework lands the pin flips loudly. **After the rework**: unignore and assert the data.
- **Boundary**: 5 — cluster role propagation is inherently multi-node. But the *pin* should be a one-line named constant/test so the rework has an obvious tripwire.

### F4: The probe loop → quorum gate → `MarkNodeFailed` wiring is untested (false-positive risk)

- **Severity** 4 — a false positive marks a healthy primary as failed and (with F2's path enabled) triggers a needless failover; a false negative leaves a dead primary in place and the shard unavailable.
- **Likelihood** 4 — network blips are the single most common production event this code exists to survive.
- **Effort** 4 — needs controllable network faults; `crates/testing/` partition + fault_injection exist, and turmoil is already used for the detector.
- **Priority** 16
- **Evidence**: `HealthTable` is well unit-tested in isolation (threshold latching, staleness window `check_interval * (fail_threshold + 2)`, quorum `total/2 + 1`), but nothing exercises the composition: leader-only probing → `HealthTable` verdict → quorum check → `ClusterCommand::MarkNodeFailed`. `failure_detector.rs:295-296` — the "state machine rejected `MarkNodeFailed`" arm is uncovered, i.e. a rejected mark is never observed. The uncovered `:303` gate means no test even reaches the auto-failover branch point.
- **Proposed test**: turmoil (or `crates/testing/partition`) three-node cluster. (a) **False positive**: drop traffic to one node for `fail_threshold - 1` intervals, restore, assert no `MarkNodeFailed` was ever committed and `CLUSTER NODES` never shows the `fail` flag. (b) **False negative**: keep a node down past the threshold, assert the `fail` flag appears within a bounded number of intervals. (c) **Quorum**: partition the leader into a minority, assert it does *not* commit `MarkNodeFailed` for the majority side.
- **Boundary**: 5 (turmoil) — the behaviour is defined by timing and reachability; a deterministic network simulator is the only way to make (a) non-flaky. Level 1 already covers the arithmetic, so do not re-test it here.

### F5: The importing target does not return TRYAGAIN for a partially-arrived multi-key command

- **Severity** 4 — the target serves a multi-key read with a phantom nil for a key that exists but has not arrived yet: a wrong answer on a data path, and for a multi-key *write* a partial application.
- **Likelihood** 3 — needs ASKING + a multi-key command against the importing target mid-migration; a redirect-following client does exactly this.
- **Effort** 3 — cluster harness, or arguably level 1 if the check is added to the same seam the batch path uses.
- **Priority** 15
- **Evidence**: the batch path implements the rule explicitly — `slot_migration/routing.rs:242` `BatchRoute::ProbeImporting` with the doc "a multi-key batch with anything still missing → TRYAGAIN", handled at `guards.rs:910` `validate_queued_batch`. The per-command path has no equivalent: `check_migrating_multikey` (`guards.rs:735`) returns `None` when the local node is not the slot *owner* (it is the importing target, so the owner is the source), and `dispatch.rs`'s `MigratingTryAgain` stage is the only TRYAGAIN producer outside EXEC. Redis's `getNodeByQuery` returns `-TRYAGAIN` for `importing_slot && (CLIENT_ASKING|CMD_ASKING) && multiple_keys && missing_keys`. So `ASKING; MGET {t}:a {t}:b` on the target with only `a` arrived returns `[v, nil]` instead of `-TRYAGAIN`.
- **Proposed test**: two-node cluster, migration open on slot S, `MIGRATE` only `{t}:a`. From a client on the target: `ASKING` then `MGET {t}:a {t}:b` → assert `-TRYAGAIN`. Control: after migrating `{t}:b` too, the same command returns both values. Mirror the assertion for the EXEC path so the two paths are pinned to the same rule in the same test file.
- **Boundary**: 5 — same reasoning as F1; a real half-migrated slot cannot be built lower down.

### F6: The real replica streamer (`RealReplicaStreamer`) is never started by any test

- **Severity** 4 — promotion wires up the object that actually streams data to replicas; if `start` or `Drop` misbehaves the promoted primary serves reads but replicates nothing, and the failure is silent.
- **Likelihood** 3 — every promotion, but partly masked by the fact that cluster replication is inert today.
- **Effort** 3 — standalone primary/replica integration already exists; the gap is that `role_manager` tests use fakes exclusively.
- **Priority** 15
- **Evidence**: `role_manager.rs:474` `ReplicaStreamer::start` — `r=31, t=0, untested`; `:521` `RealReplicaStream::drop` — `r=11, t=0`; `:344` `RealReplicaStreamer::new` — `r=25, t=0`. All 12+ existing `promote`/`demote` tests inject `FakeReplicaStreamer`, so the seam is well tested and its only real implementation is not. `build_handler` is `monoculture` (t=2).
- **Proposed test**: level 4 — a standalone `TestServer` promoted via the real code path, with a real replica attached; assert a write after promotion reaches the replica, and that after `demote` the stream is torn down (replica's link goes down within a bounded time, exercising `Drop`).
- **Boundary**: 4 — needs a real socket and a real PSYNC peer; the cluster harness would add a Raft layer that contributes nothing to this behaviour. **Partially blocked**: the *cluster* promotion variant is gated on `promotion-replid-psync.md` (manual promotion mints no replid and rejects PSYNC), so scope this test to standalone promotion and note the cluster variant as a rework follow-up.

### F7: `SlotMigrationCoordinator::commit`'s non-leader redirect arm is uncovered

- **Severity** 3 — an operator running `CLUSTER SETSLOT` against a follower during leader churn gets an unverified reply shape; a wrong shape means their tooling does not retry and the migration silently does not start.
- **Likelihood** 4 — `SETSLOT` is routinely issued against whichever node the operator is connected to, and leadership moves.
- **Effort** 3
- **Priority** 14
- **Evidence**: `slot_migration/mod.rs:149` — `Err(ProposeError::Redirect(r)) => redirect_to_response(r)` is uncovered. `:150` (Raft error) likewise. The whole `commit` path is only exercised through the happy leader-commit case.
- **Proposed test**: cluster harness — issue `CLUSTER SETSLOT <s> MIGRATING <target>` against a known follower and assert the reply is a well-formed `REDIRECT`/`CLUSTERDOWN` (exact prefix + leader address), and that after following it the migration is actually visible in `CLUSTER NODES` on all nodes. Also assert the leaderless case returns `CLUSTERDOWN` rather than hanging.
- **Boundary**: 5 — requires a real Raft follower; there is no lower seam that produces `ProposeError::Redirect` honestly. If `ClusterWriter` were mockable at level 2 that would be preferable — see cross-area notes.

### F8: Slot-migration completion with an unknown target node silently drops the shard wakeup

- **Severity** 3 — blocked clients (BLPOP/WAIT-style waiters) on a migrated slot are never woken with the new MOVED address; they hang until timeout.
- **Likelihood** 3 — the target's `NodeInfo` being briefly absent from the local view during a completion is exactly the kind of convergence race cluster mode produces.
- **Effort** 2 — `run_event_dispatcher` is drivable from an in-module test with a fabricated `ClusterState` and an mpsc pair; no server needed.
- **Priority** 13
- **Evidence**: `slot_migration/events.rs:28,:31,:33` uncovered — the "target node not found in the node table" warn-and-`continue` branch; `:45` uncovered. The happy path fans `SlotMigrationCompleteEvent` → `ClusterMsg::SlotMigrated { slot, target_addr }` to shard `event.slot as usize % num_shards`. (I verified this shard mapping matches `core/src/shard/partition.rs:32` `shard_for_key` = `slot % num_shards`, so there is no mis-routing bug here — only an untested drop branch.)
- **Proposed test**: level 1/2 — drive `run_event_dispatcher` with a two-shard sender vector. Assert (a) a completion for slot S with a known target delivers `SlotMigrated { slot: S, target_addr }` to shard `S % 2` and to no other shard; (b) a completion whose target is absent from the node table delivers nothing and does not terminate the dispatcher (a subsequent good event still lands); (c) closing the receiver ends the task.
- **Boundary**: 1 — pure fan-out over channels; running a cluster to test a `%` and a `continue` is the anti-pattern the brief calls out.

### F9: The `tls_cluster_migration` runtime flip is untested against a real bus connection

- **Severity** 4 — the flag decides whether the cluster bus accepts plaintext peers. Stuck on, an unauthenticated peer on the bus port can inject `PubSubBroadcast` and Raft RPCs; stuck off, a rolling TLS upgrade bricks the cluster.
- **Likelihood** 2 — only during a deliberate rolling TLS migration.
- **Effort** 3
- **Priority** 13
- **Evidence**: `cluster_bus.rs:102-111` `choose_transport` is covered by 3 pure unit tests, but `handle_connection` (`:126-160`) reads the flag *per connection* — `ctx.tls_cluster_migration.load(Ordering::Relaxed)` at `:127` — specifically so a runtime change applies to the next peer. Nothing tests that. `:137-138,:141` (peek timeout) and `:154-155` (TLS handshake timeout / `ConnectionRefused`) are uncovered, so neither bounded-wait guard has ever run.
- **Proposed test**: level 4 — start a TLS-enabled node with `tls_cluster_migration` off; assert a plaintext TCP connection to the bus port is closed and no RPC is served. Flip the flag at runtime via `CONFIG SET`; assert the *next* plaintext connection is served while an existing TLS one is undisturbed; flip back and assert the next plaintext connection is refused again. Separately assert a peer that connects and sends nothing is dropped within `handshake_timeout` (this is the `:137` peek-timeout branch and it is a trivial slowloris on the bus port).
- **Boundary**: 4 — needs real sockets and a real TLS acceptor, but not multiple cluster nodes; a single node's bus listener is enough.

### F10: Cluster bus malformed / truncated / duplicate frame handling is untested, and errors are classified by string matching

- **Severity** 3 — a garbage frame from a peer (or a rolling-upgrade version skew) tears down the bus connection with an `InvalidData` error rather than being handled; repeated, this is a flapping bus.
- **Likelihood** 3 — rolling upgrades and version skew are the realistic source.
- **Effort** 3
- **Priority** 12
- **Evidence**: `cluster_bus.rs:167-181` — parse failures are classified as clean-vs-error by matching the *error message text* for `"connection closed"` / `"connection reset"` / `"broken pipe"`. Any change to the upstream error strings silently reclassifies a clean disconnect as a hard error (and vice versa), and no test pins it. `:195-199` (`send_rpc_response` failure) and `:267-270` (**the entire `HealthProbe` response arm**) are uncovered; the last is notable because F2's failover scoring depends on `health_probe` returning correct offsets. `:77-78,:82` (accept error) uncovered too.
- **Proposed test**: level 4 against a single node's bus listener with a hand-rolled client: (a) send a well-formed length prefix followed by non-JSON garbage → assert the server closes that connection and *remains accepting* (a second, valid connection still gets a response); (b) send a truncated frame then disconnect → assert clean close, no error log; (c) send a valid `HealthProbe` and assert the response body's offset/role fields, which nothing currently does; (d) send the same request twice on one connection and assert two independent correct responses (duplicate handling). Frame size is bounded by `MAX_FRAME_SIZE` in `cluster/src/network.rs:740`, so an oversized-frame OOM is not a live risk — do not spend effort there.
- **Boundary**: 4 — a raw socket client against the real listener; nothing lower can produce a malformed frame.

### F11: `MIGRATE`'s `AUTH` and `SELECT` handshake is entirely uncovered

- **Severity** 3 — `MIGRATE ... AUTH <pw>` against a password-protected target: if the auth or db-select step misbehaves the key is not transferred, and the user sees a confusing error rather than a clean failure.
- **Likelihood** 3 — migrating into a secured cluster is normal.
- **Effort** 3
- **Priority** 12
- **Evidence**: `migrate.rs:283-320` — `MigrateClient::auth` and `select_db` are wholly uncovered. Restore error arms `:337,:344-351` and `frame_to_response` arms `:360-365` (including the Array/Null recursion) are uncovered too, so a non-`+OK` reply from the target has never been converted to a client-visible response.
- **Proposed test**: level 4 — `MIGRATE` between two `TestServer`s where the target has `requirepass` set. Assert: correct password + `DB n` moves the key into the right db on the target and removes it from the source; wrong password leaves the source key intact and returns a clear error (not a panic or a truncated reply); target replying an error to `RESTORE` (e.g. destination key exists without `REPLACE`) surfaces as `-BUSYKEY` and leaves the source key intact.
- **Boundary**: 4 — two real servers over RESP; auth is inherently a connection-layer behaviour.

### F12: `ClusterRuntimeFlags` setters and `write_fence_reason` are untested

- **Severity** 2 — a wrong `write_fence_reason` shows the operator the wrong reason for a fenced node in `/status`; the setters not taking effect would silently disable self-fencing (that part is severity 4, but the `SelfFenceGate` logic itself *is* tested).
- **Likelihood** 3 — self-fencing engages on any quorum loss.
- **Effort** 1
- **Priority** 11
- **Evidence**: `cluster_flags.rs` — `set_auto_failover`, `set_self_fence_on_quorum_loss`, `set_replica_priority`, `Default`, and the `WriteFenceReporter::write_fence_reason` impl all show `untested` at the function level. These are live atomics driven by `CONFIG SET`.
- **Proposed test**: level 1 — round-trip each setter/getter; assert `Default` matches the documented defaults (notably `auto_failover == false`, which F2 shows nobody has verified); assert `write_fence_reason` returns `None` when not fenced and the specific quorum-loss reason string when the gate is armed and the `QuorumChecker` reports a loss. Pair with one level-4 assertion that `CONFIG SET cluster-auto-failover yes` is observable in `INFO`/`/status`, since the config→flag wiring is the part a unit test cannot see.
- **Boundary**: 1 — pure atomics and a small state machine.

### F13: Cluster pubsub never skips a FAILed node in any test, and the single-node early return is dead in tests

- **Severity** 2 — `PUBLISH` blocks on (or miscounts) a node marked `fail`: a wrong subscriber count and up to a `PUBSUB_RPC_TIMEOUT` (2 s) stall per failed node if the skip regressed.
- **Likelihood** 3 — publishing while a node is down is ordinary.
- **Effort** 3
- **Priority** 9
- **Evidence**: `cluster_pubsub.rs:149` — `info.flags.fail` skip, uncovered; `:136` — `all_nodes.len() <= 1` early return, uncovered. The 5 inline tests all target `send_pubsub_rpc` in isolation, never `broadcast_publish`'s node-selection loop. `integration_cluster.rs:11886-11901` has a cross-node `PUBLISH` control assertion but only on a fully healthy cluster.
- **Proposed test**: level 1/2 for the loop if the node list can be injected (assert a `fail`-flagged node is not dialled and does not contribute to the count; assert a single-node cluster returns the local count without any RPC); level 5 otherwise — kill one node of three, wait for the `fail` flag, then `PUBLISH` from a survivor and assert the reply returns promptly (well under 2 s) with a count covering only live subscribers.
- **Boundary**: 1 preferred — this is node-list filtering, not distributed behaviour. Recommend refactoring `broadcast_publish` to take the node list as a parameter if it does not already, so the filter is unit-testable; note this as a small production-code change, not a test-only one.

### F14: `MigrateArgs::parse` negative paths are uncovered (cheap)

- **Severity** 2 — a malformed `MIGRATE` returns the wrong error or is misparsed; the permissive `key_positions` walker (unknown option → +1) makes silent misparse plausible.
- **Likelihood** 2
- **Effort** 1
- **Priority** 9
- **Evidence**: `migrate.rs:180,:190,:203-206` uncovered — `AUTH` with no password, `AUTH2` with missing args, unknown option. `:33-42` (Display) and `:48-50` (`From<io::Error>`) uncovered. `MigrateArgs::parse` is `monoculture` (`r=73/48`).
- **Proposed test**: level 1 table test — one row per malformed form asserting the exact error string, plus a positive row per valid option combination (`COPY`, `REPLACE`, `AUTH`, `AUTH2`, `KEYS`, `DB`), and a cross-check that `key_positions` and `MigrateArgs::parse` agree on the key set for every valid form (the permissive/strict pair is exactly where a divergence hides — a property test over generated argument vectors would be better than examples here).
- **Boundary**: 1 — pure parsing; the anti-pattern would be testing this through a socket.

## Deprioritised

- **`SlotMigrationCoordinator::is_migrating` / `migration_for`** (`slot_migration/mod.rs:92-99`) — both `untested`, and `rg` finds no production caller: the only matches are their own definitions. This is dead code; the right action is deletion, not a test. Priority as a *test* would be ~4.
- **Oversized-frame / OOM on the cluster bus** — bounded by `MAX_FRAME_SIZE` (`cluster/src/network.rs:740-741`); not a live risk, no test warranted.
- **`slot_migration/validator.rs`** — already has a property test proving `same_slot ⇒ same_shard` over 200 keys × 6 shard counts. Nothing to add.
- **`route_with_snapshot` additional cases** — 100% line coverage and an exhaustive truth table. It is `monoculture` (all 11 tests from one file) but that is the *correct* shape for a pure decision function; do not manufacture a second caller just to change the class.
- **`events.rs` shard-mapping mismatch** — I checked for a bug where `slot % num_shards` in the dispatcher might disagree with key placement; it does not (`core/src/shard/partition.rs:32-35` uses the same expression). Verified, no finding.
- **Jepsen failover-during-migration workload** — genuinely valuable but effort 5 (new workload + nemesis composition) and it overlaps F1/F4; revisit after those land and after the replication rework, since a Jepsen run against inert cluster replication would assert very little.
- **Accept-loop error branch** (`cluster_bus.rs:77-82`) — forcing an `accept()` error deterministically needs fd exhaustion; low value on its own, and F10 already proves the listener survives a bad connection.

## Cross-area notes

- **Blocked on `.scratch/replication-cluster-rework/`**: F3 in full, and the cluster half of F6. Per `wait-cluster-mode.md`, cluster-mode data replication is inert (cluster primaries refuse PSYNC because `replication.role` is never set to `"primary"` and is `#[param(skip)]`). Any test asserting a cluster replica *holds primary data* cannot pass today. F3's proposal is deliberately scoped to what is observable now plus a loud tripwire. When that PRD lands it also flips the issue-48 (no-chained-replication) and issue-34 (promoted node rejects PSYNC) pins — whoever implements it should expect those two tests to need inverting.
- **To the `crates/cluster` agent** (Raft/cluster crate): (a) `ProposeError::Redirect` is unreachable from server-side tests today — a mockable `ClusterWriter`/propose seam would let F7 drop from boundary 5 to boundary 2 and would help several other server-side callers; (b) `MAX_FRAME_SIZE` and `parse_rpc_message`'s error taxonomy live in `cluster/src/network.rs` — F10 depends on those errors being *typed* rather than string-matched, so if you are refactoring that, the server's `cluster_bus.rs:167-181` string matching should become an error-kind match at the same time; (c) `ClusterCommand::MarkNodeFailed`'s rejection path (`failure_detector.rs:295`) is decided by your state machine — a test on your side for "MarkNodeFailed on an already-failed / unknown node" would let the server side assume it.
- **To the replication agent**: F6's standalone half (real `ReplicaStreamer::start` / `RealReplicaStream::drop`) sits on the boundary between `role_manager.rs` (mine) and `replication*` (yours). The seam is well designed and well faked; the gap is that the real implementation is never instantiated in a test. Suggest you own the level-4 test and I keep the level-1 promote/demote matrix.
- **Shared infrastructure**: F4 wants a turmoil or `crates/testing/partition` scenario that partitions a *specific* node from the leader for a *bounded number of health-check intervals*. If no other agent needs it, this is the one new fault primitive worth building in this area — it unlocks both false-positive and quorum-loss testing, and `cluster_flags`' `SelfFenceGate` would become end-to-end testable as a side effect.
- **F1 is a suspected product bug**, not only a test gap. If the coordinating agent is triaging bugs separately from tests, F1 should be filed in both places.
