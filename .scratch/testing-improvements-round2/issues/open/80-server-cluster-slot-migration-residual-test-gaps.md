# server cluster / slot migration — residual test gaps (12 findings)

Status: ready-for-agent
Type: AFK
Origin: round-2 testing audit 2026-07-28 — 15 parallel area audits, `.scratch/testing-improvements-round2/`
Source: proposals/04 — residual findings after promotion to issues 19–76
Score: 12 findings, priority range 9–18
Area: `frogdb-server/crates/server/src/` — `cluster_bus.rs`, `cluster_flags.rs`, `cluster_pubsub.rs`, `slot_migration/`, `migrate.rs`, `failure_detector.rs`, `role_manager.rs`

## Context

This area is the server-side wiring of cluster mode: the cluster bus accept loop and RPC
dispatch, the slot-migration coordinator/router/validator, `MIGRATE`, the failure detector
and auto-failover, and the role manager. Coverage (from `target/llvm-cov/depth/depth.json`,
2026-07-28) is **1974 lines, 1708 covered = 86.5%**, with depth classes over the functions
in these files of `untested 133`, `single-test 112`, `well-covered 101`, `monoculture 58`,
`covered 17`. The proposal's verdict on the *shape* of that coverage: the **decision** layer
of cluster mode is genuinely well tested — `route_with_snapshot`, `route_queued_batch`,
`HealthTable`, `compute_replica_score`/`select_failover_target` and `SlotValidator` are pure
functions with dense inline tables — while the **wiring** layer around them is not, the
uncovered residue being concentrated almost entirely in the failure and transition paths
(auto-failover execution, bus I/O error handling, migration lifecycle error arms, and the
real non-fake replica streamer).

## Promoted elsewhere

- F1 → issue 47, `.scratch/testing-improvements-round2/issues/` (a single-key write to a MIGRATING slot whose key already migrated is `+OK`'d, then lost)
- F2 → issue 33, `.scratch/testing-improvements-round2/issues/` (`MASTER.md` §4 tests that cannot fail — `trigger_auto_failover`'s own named test leaves `auto_failover` at its default `false`)

F3 is *partly* claimed: `MASTER.md` §4 lists its "two zero-assertion cluster-replication
tests" aspect under issue 33, `.scratch/testing-improvements-round2/issues/`, but `MASTER.md`
§8 also names 04/F3 as blocked on the unreviewed PRDs in
`.scratch/replication-cluster-rework/`. It is therefore kept whole below, in
`## Residual findings`, and counted as residual.

## Residual findings

### F3 — Assertion-free cluster replication tests mask an inert subsystem

**BLOCKED on `.scratch/replication-cluster-rework/`** — a test asserting that a cluster
replica actually holds primary data cannot pass today, because a cluster primary refuses the
PSYNC that `CLUSTER REPLICATE` tells its replica to open; only the *honest* half of the
proposed test is writable now.

Cross-reference: the "these two tests assert nothing" aspect of this finding is also covered
by issue 33, `.scratch/testing-improvements-round2/issues/` (`MASTER.md` §4, tests that
cannot fail). The blocked half — asserting data actually arrives — is not, and stays here.

- **Severity** 4 — these tests are the only thing standing between "cluster replicas receive data" and a regression; today they would pass with the feature entirely removed (and in fact do — it *is* entirely removed).
- **Likelihood** 4 — anyone reading the suite concludes cluster replication is tested.
- **Effort** 2 — editing existing integration tests; the *honest* version is cheap, the *full* version is blocked.
- **Priority** 18
- **Evidence**: `integration_cluster.rs:2589` `test_replica_receives_writes` — the body is `eprintln!` only, zero assertions; it accepts a MOVED error, a wrong value, and a correct value as equally fine. `integration_cluster.rs:2706` `test_promoted_replica_has_all_data` — computes `found_count` and never asserts on it, and counts a `MOVED` *error* reply as "found". Same defect class round 1 fixed for the source-crash tests (issue 17) but left in this tier.
- **BLOCKED**: a test asserting that a cluster replica actually holds primary data cannot pass today. Per `.scratch/replication-cluster-rework/wait-cluster-mode.md` §1-2.2, `PrimaryReplicationHandler` is gated on `config.replication.role == "primary"` (`replication_init.rs:57,:101,:201-205`), cluster bootstrap only ever constructs `NodeInfo::new_primary` without setting that config key (`cluster_init.rs:79-81,:340-355,:387-395`), and `replication.role` is `#[param(skip)]` so it is not runtime-settable (`config/src/replication.rs:144-146,:299-301`). Cluster nodes start as `ServerRole::Standalone` (`test-harness/src/server.rs:468-472`). A cluster primary therefore **refuses the PSYNC that `CLUSTER REPLICATE` tells its replica to open**.
- **Proposed test**: two-step. **Now**: rewrite both tests to assert only what is observable and true — that `CLUSTER REPLICATE` propagates the role (replica appears with a `slave` flag and the correct master ID in `CLUSTER NODES` on every node), and that a read on the replica without READONLY returns `MOVED` to the primary. Then add an explicit `#[ignore]`d or comment-pinned assertion documenting that data does *not* arrive, referencing `wait-cluster-mode.md`, so the day the rework lands the pin flips loudly. **After the rework**: unignore and assert the data.
- **Boundary**: 5 — cluster role propagation is inherently multi-node. But the *pin* should be a one-line named constant/test so the rework has an obvious tripwire.

### F4 — The probe loop → quorum gate → `MarkNodeFailed` wiring is untested (false-positive risk)

- **Severity** 4 — a false positive marks a healthy primary as failed and (with F2's path enabled) triggers a needless failover; a false negative leaves a dead primary in place and the shard unavailable.
- **Likelihood** 4 — network blips are the single most common production event this code exists to survive.
- **Effort** 4 — needs controllable network faults; `crates/testing/` partition + fault_injection exist, and turmoil is already used for the detector.
- **Priority** 16
- **Evidence**: `HealthTable` is well unit-tested in isolation (threshold latching, staleness window `check_interval * (fail_threshold + 2)`, quorum `total/2 + 1`), but nothing exercises the composition: leader-only probing → `HealthTable` verdict → quorum check → `ClusterCommand::MarkNodeFailed`. `failure_detector.rs:295-296` — the "state machine rejected `MarkNodeFailed`" arm is uncovered, i.e. a rejected mark is never observed. The uncovered `:303` gate means no test even reaches the auto-failover branch point.
- **Proposed test**: turmoil (or `crates/testing/partition`) three-node cluster. (a) **False positive**: drop traffic to one node for `fail_threshold - 1` intervals, restore, assert no `MarkNodeFailed` was ever committed and `CLUSTER NODES` never shows the `fail` flag. (b) **False negative**: keep a node down past the threshold, assert the `fail` flag appears within a bounded number of intervals. (c) **Quorum**: partition the leader into a minority, assert it does *not* commit `MarkNodeFailed` for the majority side.
- **Boundary**: 5 (turmoil) — the behaviour is defined by timing and reachability; a deterministic network simulator is the only way to make (a) non-flaky. Level 1 already covers the arithmetic, so do not re-test it here.

### F5 — The importing target does not return TRYAGAIN for a partially-arrived multi-key command

- **Severity** 4 — the target serves a multi-key read with a phantom nil for a key that exists but has not arrived yet: a wrong answer on a data path, and for a multi-key *write* a partial application.
- **Likelihood** 3 — needs ASKING + a multi-key command against the importing target mid-migration; a redirect-following client does exactly this.
- **Effort** 3 — cluster harness, or arguably level 1 if the check is added to the same seam the batch path uses.
- **Priority** 15
- **Evidence**: the batch path implements the rule explicitly — `slot_migration/routing.rs:242` `BatchRoute::ProbeImporting` with the doc "a multi-key batch with anything still missing → TRYAGAIN", handled at `guards.rs:910` `validate_queued_batch`. The per-command path has no equivalent: `check_migrating_multikey` (`guards.rs:735`) returns `None` when the local node is not the slot *owner* (it is the importing target, so the owner is the source), and `dispatch.rs`'s `MigratingTryAgain` stage is the only TRYAGAIN producer outside EXEC. Redis's `getNodeByQuery` returns `-TRYAGAIN` for `importing_slot && (CLIENT_ASKING|CMD_ASKING) && multiple_keys && missing_keys`. So `ASKING; MGET {t}:a {t}:b` on the target with only `a` arrived returns `[v, nil]` instead of `-TRYAGAIN`.
- **Proposed test**: two-node cluster, migration open on slot S, `MIGRATE` only `{t}:a`. From a client on the target: `ASKING` then `MGET {t}:a {t}:b` → assert `-TRYAGAIN`. Control: after migrating `{t}:b` too, the same command returns both values. Mirror the assertion for the EXEC path so the two paths are pinned to the same rule in the same test file.
- **Boundary**: 5 — same reasoning as F1; a real half-migrated slot cannot be built lower down.

### F6 — The real replica streamer (`RealReplicaStreamer`) is never started by any test

**BLOCKED on `.scratch/replication-cluster-rework/` (cluster half only)** — the *cluster*
promotion variant is gated on `promotion-replid-psync.md` (manual promotion mints no replid
and rejects PSYNC); the standalone half below is writable today.

- **Severity** 4 — promotion wires up the object that actually streams data to replicas; if `start` or `Drop` misbehaves the promoted primary serves reads but replicates nothing, and the failure is silent.
- **Likelihood** 3 — every promotion, but partly masked by the fact that cluster replication is inert today.
- **Effort** 3 — standalone primary/replica integration already exists; the gap is that `role_manager` tests use fakes exclusively.
- **Priority** 15
- **Evidence**: `role_manager.rs:474` `ReplicaStreamer::start` — `r=31, t=0, untested`; `:521` `RealReplicaStream::drop` — `r=11, t=0`; `:344` `RealReplicaStreamer::new` — `r=25, t=0`. All 12+ existing `promote`/`demote` tests inject `FakeReplicaStreamer`, so the seam is well tested and its only real implementation is not. `build_handler` is `monoculture` (t=2).
- **Proposed test**: level 4 — a standalone `TestServer` promoted via the real code path, with a real replica attached; assert a write after promotion reaches the replica, and that after `demote` the stream is torn down (replica's link goes down within a bounded time, exercising `Drop`).
- **Boundary**: 4 — needs a real socket and a real PSYNC peer; the cluster harness would add a Raft layer that contributes nothing to this behaviour. **Partially blocked**: the *cluster* promotion variant is gated on `promotion-replid-psync.md` (manual promotion mints no replid and rejects PSYNC), so scope this test to standalone promotion and note the cluster variant as a rework follow-up.

### F7 — `SlotMigrationCoordinator::commit`'s non-leader redirect arm is uncovered

- **Severity** 3 — an operator running `CLUSTER SETSLOT` against a follower during leader churn gets an unverified reply shape; a wrong shape means their tooling does not retry and the migration silently does not start.
- **Likelihood** 4 — `SETSLOT` is routinely issued against whichever node the operator is connected to, and leadership moves.
- **Effort** 3
- **Priority** 14
- **Evidence**: `slot_migration/mod.rs:149` — `Err(ProposeError::Redirect(r)) => redirect_to_response(r)` is uncovered. `:150` (Raft error) likewise. The whole `commit` path is only exercised through the happy leader-commit case.
- **Proposed test**: cluster harness — issue `CLUSTER SETSLOT <s> MIGRATING <target>` against a known follower and assert the reply is a well-formed `REDIRECT`/`CLUSTERDOWN` (exact prefix + leader address), and that after following it the migration is actually visible in `CLUSTER NODES` on all nodes. Also assert the leaderless case returns `CLUSTERDOWN` rather than hanging.
- **Boundary**: 5 — requires a real Raft follower; there is no lower seam that produces `ProposeError::Redirect` honestly. If `ClusterWriter` were mockable at level 2 that would be preferable — see cross-area notes.

### F8 — Slot-migration completion with an unknown target node silently drops the shard wakeup

- **Severity** 3 — blocked clients (BLPOP/WAIT-style waiters) on a migrated slot are never woken with the new MOVED address; they hang until timeout.
- **Likelihood** 3 — the target's `NodeInfo` being briefly absent from the local view during a completion is exactly the kind of convergence race cluster mode produces.
- **Effort** 2 — `run_event_dispatcher` is drivable from an in-module test with a fabricated `ClusterState` and an mpsc pair; no server needed.
- **Priority** 13
- **Evidence**: `slot_migration/events.rs:28,:31,:33` uncovered — the "target node not found in the node table" warn-and-`continue` branch; `:45` uncovered. The happy path fans `SlotMigrationCompleteEvent` → `ClusterMsg::SlotMigrated { slot, target_addr }` to shard `event.slot as usize % num_shards`. (I verified this shard mapping matches `core/src/shard/partition.rs:32` `shard_for_key` = `slot % num_shards`, so there is no mis-routing bug here — only an untested drop branch.)
- **Proposed test**: level 1/2 — drive `run_event_dispatcher` with a two-shard sender vector. Assert (a) a completion for slot S with a known target delivers `SlotMigrated { slot: S, target_addr }` to shard `S % 2` and to no other shard; (b) a completion whose target is absent from the node table delivers nothing and does not terminate the dispatcher (a subsequent good event still lands); (c) closing the receiver ends the task.
- **Boundary**: 1 — pure fan-out over channels; running a cluster to test a `%` and a `continue` is the anti-pattern the brief calls out.

### F9 — The `tls_cluster_migration` runtime flip is untested against a real bus connection

- **Severity** 4 — the flag decides whether the cluster bus accepts plaintext peers. Stuck on, an unauthenticated peer on the bus port can inject `PubSubBroadcast` and Raft RPCs; stuck off, a rolling TLS upgrade bricks the cluster.
- **Likelihood** 2 — only during a deliberate rolling TLS migration.
- **Effort** 3
- **Priority** 13
- **Evidence**: `cluster_bus.rs:102-111` `choose_transport` is covered by 3 pure unit tests, but `handle_connection` (`:126-160`) reads the flag *per connection* — `ctx.tls_cluster_migration.load(Ordering::Relaxed)` at `:127` — specifically so a runtime change applies to the next peer. Nothing tests that. `:137-138,:141` (peek timeout) and `:154-155` (TLS handshake timeout / `ConnectionRefused`) are uncovered, so neither bounded-wait guard has ever run.
- **Proposed test**: level 4 — start a TLS-enabled node with `tls_cluster_migration` off; assert a plaintext TCP connection to the bus port is closed and no RPC is served. Flip the flag at runtime via `CONFIG SET`; assert the *next* plaintext connection is served while an existing TLS one is undisturbed; flip back and assert the next plaintext connection is refused again. Separately assert a peer that connects and sends nothing is dropped within `handshake_timeout` (this is the `:137` peek-timeout branch and it is a trivial slowloris on the bus port).
- **Boundary**: 4 — needs real sockets and a real TLS acceptor, but not multiple cluster nodes; a single node's bus listener is enough.

### F10 — Cluster bus malformed / truncated / duplicate frame handling is untested, and errors are classified by string matching

- **Severity** 3 — a garbage frame from a peer (or a rolling-upgrade version skew) tears down the bus connection with an `InvalidData` error rather than being handled; repeated, this is a flapping bus.
- **Likelihood** 3 — rolling upgrades and version skew are the realistic source.
- **Effort** 3
- **Priority** 12
- **Evidence**: `cluster_bus.rs:167-181` — parse failures are classified as clean-vs-error by matching the *error message text* for `"connection closed"` / `"connection reset"` / `"broken pipe"`. Any change to the upstream error strings silently reclassifies a clean disconnect as a hard error (and vice versa), and no test pins it. `:195-199` (`send_rpc_response` failure) and `:267-270` (**the entire `HealthProbe` response arm**) are uncovered; the last is notable because F2's failover scoring depends on `health_probe` returning correct offsets. `:77-78,:82` (accept error) uncovered too.
- **Proposed test**: level 4 against a single node's bus listener with a hand-rolled client: (a) send a well-formed length prefix followed by non-JSON garbage → assert the server closes that connection and *remains accepting* (a second, valid connection still gets a response); (b) send a truncated frame then disconnect → assert clean close, no error log; (c) send a valid `HealthProbe` and assert the response body's offset/role fields, which nothing currently does; (d) send the same request twice on one connection and assert two independent correct responses (duplicate handling). Frame size is bounded by `MAX_FRAME_SIZE` in `cluster/src/network.rs:740`, so an oversized-frame OOM is not a live risk — do not spend effort there.
- **Boundary**: 4 — a raw socket client against the real listener; nothing lower can produce a malformed frame.

### F11 — `MIGRATE`'s `AUTH` and `SELECT` handshake is entirely uncovered

- **Severity** 3 — `MIGRATE ... AUTH <pw>` against a password-protected target: if the auth or db-select step misbehaves the key is not transferred, and the user sees a confusing error rather than a clean failure.
- **Likelihood** 3 — migrating into a secured cluster is normal.
- **Effort** 3
- **Priority** 12
- **Evidence**: `migrate.rs:283-320` — `MigrateClient::auth` and `select_db` are wholly uncovered. Restore error arms `:337,:344-351` and `frame_to_response` arms `:360-365` (including the Array/Null recursion) are uncovered too, so a non-`+OK` reply from the target has never been converted to a client-visible response.
- **Proposed test**: level 4 — `MIGRATE` between two `TestServer`s where the target has `requirepass` set. Assert: correct password + `DB n` moves the key into the right db on the target and removes it from the source; wrong password leaves the source key intact and returns a clear error (not a panic or a truncated reply); target replying an error to `RESTORE` (e.g. destination key exists without `REPLACE`) surfaces as `-BUSYKEY` and leaves the source key intact.
- **Boundary**: 4 — two real servers over RESP; auth is inherently a connection-layer behaviour.

### F12 — `ClusterRuntimeFlags` setters and `write_fence_reason` are untested

- **Severity** 2 — a wrong `write_fence_reason` shows the operator the wrong reason for a fenced node in `/status`; the setters not taking effect would silently disable self-fencing (that part is severity 4, but the `SelfFenceGate` logic itself *is* tested).
- **Likelihood** 3 — self-fencing engages on any quorum loss.
- **Effort** 1
- **Priority** 11
- **Evidence**: `cluster_flags.rs` — `set_auto_failover`, `set_self_fence_on_quorum_loss`, `set_replica_priority`, `Default`, and the `WriteFenceReporter::write_fence_reason` impl all show `untested` at the function level. These are live atomics driven by `CONFIG SET`.
- **Proposed test**: level 1 — round-trip each setter/getter; assert `Default` matches the documented defaults (notably `auto_failover == false`, which F2 shows nobody has verified); assert `write_fence_reason` returns `None` when not fenced and the specific quorum-loss reason string when the gate is armed and the `QuorumChecker` reports a loss. Pair with one level-4 assertion that `CONFIG SET cluster-auto-failover yes` is observable in `INFO`/`/status`, since the config→flag wiring is the part a unit test cannot see.
- **Boundary**: 1 — pure atomics and a small state machine.

### F13 — Cluster pubsub never skips a FAILed node in any test, and the single-node early return is dead in tests

- **Severity** 2 — `PUBLISH` blocks on (or miscounts) a node marked `fail`: a wrong subscriber count and up to a `PUBSUB_RPC_TIMEOUT` (2 s) stall per failed node if the skip regressed.
- **Likelihood** 3 — publishing while a node is down is ordinary.
- **Effort** 3
- **Priority** 9
- **Evidence**: `cluster_pubsub.rs:149` — `info.flags.fail` skip, uncovered; `:136` — `all_nodes.len() <= 1` early return, uncovered. The 5 inline tests all target `send_pubsub_rpc` in isolation, never `broadcast_publish`'s node-selection loop. `integration_cluster.rs:11886-11901` has a cross-node `PUBLISH` control assertion but only on a fully healthy cluster.
- **Proposed test**: level 1/2 for the loop if the node list can be injected (assert a `fail`-flagged node is not dialled and does not contribute to the count; assert a single-node cluster returns the local count without any RPC); level 5 otherwise — kill one node of three, wait for the `fail` flag, then `PUBLISH` from a survivor and assert the reply returns promptly (well under 2 s) with a count covering only live subscribers.
- **Boundary**: 1 preferred — this is node-list filtering, not distributed behaviour. Recommend refactoring `broadcast_publish` to take the node list as a parameter if it does not already, so the filter is unit-testable; note this as a small production-code change, not a test-only one.

### F14 — `MigrateArgs::parse` negative paths are uncovered (cheap)

- **Severity** 2 — a malformed `MIGRATE` returns the wrong error or is misparsed; the permissive `key_positions` walker (unknown option → +1) makes silent misparse plausible.
- **Likelihood** 2
- **Effort** 1
- **Priority** 9
- **Evidence**: `migrate.rs:180,:190,:203-206` uncovered — `AUTH` with no password, `AUTH2` with missing args, unknown option. `:33-42` (Display) and `:48-50` (`From<io::Error>`) uncovered. `MigrateArgs::parse` is `monoculture` (`r=73/48`).
- **Proposed test**: level 1 table test — one row per malformed form asserting the exact error string, plus a positive row per valid option combination (`COPY`, `REPLACE`, `AUTH`, `AUTH2`, `KEYS`, `DB`), and a cross-check that `key_positions` and `MigrateArgs::parse` agree on the key set for every valid form (the permissive/strict pair is exactly where a divergence hides — a property test over generated argument vectors would be better than examples here).
- **Boundary**: 1 — pure parsing; the anti-pattern would be testing this through a socket.

## Acceptance criteria

- [ ] F3: `test_replica_receives_writes` (`integration_cluster.rs:2589`) and `test_promoted_replica_has_all_data` (`:2706`) each assert that `CLUSTER REPLICATE` propagated the role — a `slave` flag with the correct master ID in `CLUSTER NODES` on every node — and that a non-READONLY read on the replica returns `MOVED`; a `MOVED` error reply no longer counts as "found"; and a named, comment-pinned or `#[ignore]`d assertion records that data does **not** arrive, citing `wait-cluster-mode.md`.
- [ ] F4: a test asserts that a node unreachable for `fail_threshold - 1` intervals produces no committed `MarkNodeFailed` and never shows the `fail` flag; that a node kept down past the threshold shows `fail` within a bounded number of intervals; and that a leader partitioned into a minority commits no `MarkNodeFailed`.
- [ ] F5: a test asserts `ASKING; MGET {t}:a {t}:b` against the importing target with only `{t}:a` migrated returns `-TRYAGAIN`, with a control asserting both values are returned once `{t}:b` has also arrived, and the same assertion mirrored for the EXEC path.
- [ ] F6: a test drives a standalone promotion through the real (non-fake) `RealReplicaStreamer` and asserts a post-promotion write reaches a real attached replica, and that after `demote` the replica's link goes down within a bounded time (exercising `RealReplicaStream::drop`).
- [ ] F7: a test asserts `CLUSTER SETSLOT <s> MIGRATING <target>` issued against a known follower returns a well-formed redirect with the exact prefix and the leader address, that following it makes the migration visible in `CLUSTER NODES` on all nodes, and that the leaderless case returns `CLUSTERDOWN` rather than hanging.
- [ ] F8: a test drives `run_event_dispatcher` with a two-shard sender vector and asserts (a) a completion for slot S with a known target delivers `SlotMigrated { slot: S, target_addr }` to shard `S % 2` and to no other shard, (b) a completion with an unknown target delivers nothing and a subsequent good event still lands, (c) closing the receiver ends the task.
- [ ] F9: a test asserts a plaintext connection to the bus port is closed while `tls_cluster_migration` is off, is served after the flag is flipped at runtime via `CONFIG SET` (with an existing TLS connection undisturbed), is refused again after flipping back, and that a peer sending nothing is dropped within `handshake_timeout`.
- [ ] F10: a test asserts the bus listener (a) closes a connection carrying a valid length prefix followed by non-JSON garbage and still serves a subsequent valid connection, (b) treats a truncated-then-disconnected frame as a clean close, (c) returns correct offset/role fields for a valid `HealthProbe`, and (d) answers two `HealthProbe`s on one connection independently.
- [ ] F11: a test asserts `MIGRATE ... AUTH` with the correct password and `DB n` moves the key to the right db on a `requirepass` target and removes it from the source; that a wrong password leaves the source key intact and returns a clear error; and that a `RESTORE` error from the target surfaces as `-BUSYKEY` with the source key intact.
- [ ] F12: a test round-trips every `ClusterRuntimeFlags` setter/getter, asserts `Default` matches the documented defaults including `auto_failover == false`, and asserts `write_fence_reason` is `None` when unfenced and the specific quorum-loss string when armed and the `QuorumChecker` reports a loss; plus one level-4 assertion that `CONFIG SET cluster-auto-failover yes` is observable in `INFO`/`/status`.
- [ ] F13: a test asserts a `fail`-flagged node is not dialled by `broadcast_publish` and does not contribute to the returned count, and that a single-node cluster returns the local count with no RPC.
- [ ] F14: a table test asserts the exact error string for each malformed `MIGRATE` form (`AUTH` with no password, `AUTH2` with missing args, unknown option), a positive row per valid option combination (`COPY`, `REPLACE`, `AUTH`, `AUTH2`, `KEYS`, `DB`), and that `key_positions` and `MigrateArgs::parse` agree on the key set for every valid form.

## Depends on

- issue 13, `.scratch/testing-improvements-round2/issues/` (I13 — bounded-duration partition primitive; F4 needs a partition of a *specific* node from the leader for a *bounded* number of health-check intervals, which nothing today can express)
- issue 14, `.scratch/testing-improvements-round2/issues/` (I14 — mockable `ClusterWriter` / propose seam; F7 needs an honest `ProposeError::Redirect`, and the same item's typed `parse_rpc_message` error taxonomy would let F10 replace `cluster_bus.rs:167-181`'s string matching with an error-kind match)
