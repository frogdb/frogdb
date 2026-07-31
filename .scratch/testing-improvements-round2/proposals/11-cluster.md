# frogdb-cluster crate — testing gap audit (round 2)

## Scope

Audited `frogdb-server/crates/cluster/src/` — the whole crate, no `tests/` dir, 7 inline
`#[cfg(test)]` modules:

| file | LOC | of which tests | role |
|---|---:|---:|---|
| `state.rs` | 2394 | ~1830 | `ClusterState`, `ClusterStateInner`, `ClusterStateMachine` (openraft `RaftStateMachine` + `RaftSnapshotBuilder`), epoch reconciliation |
| `network.rs` | 930 | ~270 | cluster-bus codec, `RaftNetwork`/`RaftNetworkFactory`, `spawn_add_raft_voter`, `handle_rpc_request` |
| `types.rs` | 853 | ~330 | `ClusterCommand`/`ClusterResponse`/`ClusterError`, `SlotRange`, `ClusterSnapshot`, `NodeInfo` |
| `storage.rs` | 529 | ~120 | RocksDB `RaftLogStorage`/`RaftLogReader` (`CF_LOGS`/`CF_META`) |
| `commands.rs` | 470 | 0 | `ClusterState::apply_command` — the single validated mutation path |
| `wire.rs` | 422 | ~230 | `CLUSTER NODES/SLOTS/SHARDS` rendering |
| `writer.rs` | 389 | ~200 | propose → forward → redirect saga |
| `version_gate.rs` | 211 | ~120 | rolling-upgrade gates |
| `lib.rs` | 77 | 0 | re-exports |

Coverage (`target/llvm-cov/depth/depth.json`, crate `cluster`): **89.3 % lines** (3380/3787),
88.1 % regions (4947/5616). 828 raw function records → **393** after span-dedup (generic
`::<_>` placeholder records carry `test_count == 0` even when a concrete instantiation is
hot — dedupe by `(file, line_start, line_end)` taking max `test_count`, or you will
mis-report `append`/`apply`/`try_get_log_entries` as untested):

| class | count |
|---|---:|
| untested | **74** |
| single-test | 152 |
| monoculture | 40 |
| covered | 1 |
| well-covered | 126 |

Untested spans by file: `network.rs` 20, `storage.rs` 20, `types.rs` 18, `state.rs` 15,
`commands.rs` 1. Note many `single-test` records are the test functions' own bodies; they are
not production gaps.

## Summary

The 89.3 % headline hides a sharply bimodal crate: everything reachable from a *running,
healthy, small* cluster is densely tested, and everything on the **recovery** paths is not.
Concretely, the entire Raft snapshot lifecycle (`build_snapshot`, `install_snapshot`,
`begin_receiving_snapshot`, `get_snapshot_builder`, and the network `install_snapshot` leg) and
log compaction (`purge`, 45 regions) have **zero** tests — these are exactly the paths a lagging
follower, a newly joined node, or a restarted node takes. The single test whose name suggests
otherwise (`integration_cluster.rs:4763 test_raft_snapshot_during_migration`) never triggers a
snapshot at all (openraft default `snapshot_policy = LogsSinceLast(5000)`, and the test writes a
handful of entries) and asserts only that the cluster reconverges. The bug class that escapes
today is therefore: *a node that loses or rebuilds its log silently comes back with the wrong
topology* — wrong slot map, wrong epoch, wrong voter set — while `CLUSTER INFO` reports `ok`.
Second, `commands.rs` — the one validated mutation path, and the only place where slot-map
invariants can be enforced — has **no test module of its own**; its invariants are only tested
transitively via `state.rs`'s command-by-command matrix, which is example-based, so the
*cross-command* holes (`RemoveNode` leaves dangling migrations; `RemoveSlots` ignores its own
`node_id`) are wide open. Reading found four probable production bugs, not just gaps (F3, F4,
F5, F9).

**Turmoil vs. model checking.** Round 1 issue 11 already built real openraft-over-turmoil
(`server/tests/simulation.rs`, `spawn_cluster_hosts`, 2 scenarios) and documented a hard upstream
limit: turmoil 0.7.1 leaks an ephemeral port per cancelled dial, so an *indefinite* `partition`
exhausts the host's port range and is infeasible — only `hold`/`release` works. That caps the
return on further turmoil investment for exactly the scenario the dispatch asks about
(majority-loss partition). Meanwhile `ClusterState::apply_command` is a pure, synchronous,
deterministic `&ClusterStateInner → Result<(state, events)>` function over `BTreeMap`s with no
I/O, no clock, and no async — a **proptest/model-checking target at boundary level 1–2 with zero
new infrastructure**. My recommendation is unambiguous: spend the effort on a state-machine
model (F8) plus a replay-determinism property (F2), not on more turmoil. Shuttle/loom buy nothing
here — the state machine's only concurrency is a single `RwLock` around `inner`.

**Round-1 / pending-PRD interaction.** Issue 16 landed
`test_config_epoch_round_trips_through_storage_restart` (storage.rs:475) and issue 64 landed 8
epoch-collision unit tests; I report only residue. `epoch-fold-redesign.md` task **T7
("Durability: persist the state-machine snapshot")** is the pending, unreviewed fix for F1 — that
PRD states explicitly "today no test exercises purge, `install_snapshot`, or startup with a
non-empty `last_purged`", which this audit confirms independently. F1 is therefore flagged
**blocked on T7**; F4, F7 and the storage half of F6 are *not* blocked and can land today.

## Existing test inventory

| surface | covers | strengths | blind spots |
|---|---|---|---|
| `state.rs` inline (`~1830` LOC, biggest module) | happy + error path for every `ClusterCommand` variant; the atomic `Failover` composite (force / graceful / replay / absorb / validation-failure-mutates-nothing / migration-pruning); demotion-event self-filter matrix; `FinalizeUpgrade` matrix; 8 epoch-collision tests (issue 64) | genuinely dense; asserts on returned `ClusterResponse` *and* resulting state; failover tests assert "validation failure mutates nothing" | single-command, single-node only; **no** multi-command sequence/invariant test; `apply`'s `EntryPayload::Membership` arm (state.rs:445) untested; snapshot round-trip only simulated by hand-rolling serde on `ClusterStateInner` (`test_state_snapshot_roundtrip_after_failover`), never through `build_snapshot`/`install_snapshot` |
| `storage.rs` inline (5 tests) | open/close, vote, meta on an **empty** log, issue-16 epoch replay-after-restart | issue-16 test is a real restart-fidelity test | `purge` 0 tests; `truncate`/`invalidate_cache_range` monoculture (`integration_cluster` only); `test_storage_committed` is **assertion-free** (saves then clears, never reads back); no test opens a second `ClusterStorage` on a purged dir |
| `writer.rs` inline (9 tests) | propose/forward/redirect fork via `RaftProposer`/`LeaderForwarder` fakes | exemplary seam design — the model the rest of the crate should follow | none material |
| `wire.rs`, `version_gate.rs` inline | golden `CLUSTER NODES` render, flags/health tokens, shard grouping/order, zero-slot primary; gate activation/pending matrices | golden-string assertions | `wire::standalone_snapshot` monoculture (2 tests) |
| `network.rs` inline | factory registration, postcard round-trips | — | `test_all_rpc_variants_roundtrip` asserts only `matches!` on the decoded discriminant (payload never compared); no negative frame tests; 20 untested spans |
| `commands.rs` | — | — | **no inline test module at all** |
| `server/tests/integration_cluster.rs` (12 908 LOC, ~150 tests) | formation, MEET/FORGET, failover, migration e2e, partitions, redirects, restart persistence, rolling upgrade, epoch monotonicity | broad and behaviour-level; several strong invariant helpers (`assert_slot_converges_to_single_owner`, `assert_key_served_by_exactly_one`) | several tests `return` early on setup failure → vacuous pass; `test_raft_snapshot_during_migration` never snapshots; `test_remove_node_during_active_migration` asserts only "some write somewhere succeeded" |
| `server/tests/simulation.rs` (turmoil, round-1 issue 11) | 3-node real Raft, MOVED convergence, leader isolation mid-migration | deterministic, seeded, ~2.5 s | indefinite partitions infeasible (upstream port leak); no snapshot/purge scenario |
| `test-harness::cluster_harness` | in-process multi-node cluster; `raft()` and `cluster_state()` accessors are exposed | direct access to `Arc<ClusterRaft>` makes voter-set assertions cheap | nothing asserts Raft membership metrics today |

## Findings

### F1: A purged Raft log plus an unpersisted state machine silently loses cluster topology on restart
- **Severity** 5 — the restarted node rebuilds `slot_assignment`, `nodes`, `config_epoch` from only the *unpurged* log suffix. Silent consistency loss: slots vanish from the map, epochs regress, and `CLUSTER INFO` still reports `ok`.
- **Likelihood** 3 — needs ≥5000 Raft entries (default `SnapshotPolicy::LogsSinceLast(5000)`, `max_in_snapshot_log_to_keep = 1000`; `cluster_init.rs:304-309` overrides only the timers) before a restart, or any follower that received an `InstallSnapshot` (which purges its log outright) and then restarts.
- **Effort** 2 — crate-level: `ClusterStorage` + `ClusterStateMachine` on a `tempdir`, no server.
- **Priority** 19
- **Evidence**: `crates/cluster/src/state.rs:474-527` — the state machine is pure in-memory; `install_snapshot`/`get_current_snapshot` never touch `ClusterStorage`. `crates/cluster/src/storage.rs:369-400` — `purge` deletes entries and persists `KEY_LAST_PURGED`, both **untested** (`untested`, 45 regions). openraft's `StorageHelper::get_initial_state` (`helper.rs:126-140`) rebuilds a snapshot from the *empty* state machine when `last_purged_log_id.is_some()` and no snapshot exists. `epoch-fold-redesign.md` §2.4 + T7 describe the same hazard and remain **unimplemented, awaiting user review**.
- **Proposed test**: apply ≥6000 commands through the state machine while appending them to a real `ClusterStorage`; take a snapshot, `purge` to `last_applied - 1000`, drop everything, reopen the DB into a fresh `ClusterStateMachine`, run openraft's recovery, and assert the recovered `ClusterSnapshot` equals the pre-restart one (slot map, node set, `config_epoch`, `active_version`) — not merely `>=`.
- **Boundary**: 2 (crate-level API test) — needs the real RocksDB storage and the real state machine, but nothing above them; a server-level test would add minutes of runtime for zero extra coverage.
- **Status**: **blocked on `epoch-fold-redesign.md` T7.** The assertion cannot pass until snapshot persistence exists. File this test *as T7's acceptance criterion* rather than as standalone test work.

### F2: Nothing asserts that replaying a Raft log reproduces the state that produced it
- **Severity** 4 — log replay is the recovery path on *every* node restart. A non-deterministic or order-sensitive `apply_command` diverges nodes silently; two nodes then disagree about slot ownership with no error anywhere.
- **Likelihood** 4 — every restart, every follower catch-up.
- **Effort** 2 — pure in-memory: two `ClusterStateMachine`s, no I/O.
- **Priority** 18
- **Evidence**: `crates/cluster/src/state.rs:380-457` (`apply`) is `well-covered` by *aggregate* execution but no test compares two independently-derived states. The closest existing test, issue 16's `storage.rs:475 test_config_epoch_round_trips_through_storage_restart`, replays entries into a fresh state machine but asserts **only `config_epoch == 3`**, and does so by reaching into the private `cf_logs()`/`db` — an internals-coupled test that a storage refactor breaks.
- **Proposed test**: proptest over a generated `Vec<ClusterCommand>` — (a) apply to SM-A entry-by-entry; (b) apply the identical entries to a fresh SM-B; assert `serde_json::to_vec(A.inner) == serde_json::to_vec(B.inner)` and that the `Vec<ClusterResponse>` sequences are identical; (c) apply the first half to SM-C, `build_snapshot`, `install_snapshot` into SM-D, apply the second half to D, assert D == A. (c) is the snapshot-fidelity assertion F7 also wants. Additionally: move issue 16's replay test up to this boundary so it stops reaching into `db`.
- **Boundary**: 2 — the behaviour is entirely inside the crate's public state-machine surface; the current level-3 storage-internals variant should be **moved down** to here.

### F3: A cloned `RaftLogReader` keeps a stale log cache and can serve pre-truncate entries
- **Severity** 5 — returns entries from an overwritten term at indexes that were truncated and re-appended. That is Raft log divergence: the leader ships stale entries to followers, or membership recovery reads the wrong entry.
- **Likelihood** 2 — needs leadership flap: node caches entries as leader, steps down, truncates a conflicting suffix, re-appends different content at the same indexes, becomes leader again. openraft creates the log reader **once** at startup and holds it for the node's lifetime.
- **Effort** 2 — deterministic crate-level test, no cluster.
- **Priority** 17
- **Evidence**: `crates/cluster/src/storage.rs:280-288` — `get_log_reader` returns a *new* `ClusterStorage` with `log_cache: RwLock::new(self.log_cache.read().clone())`: a detached copy that never receives `invalidate_cache_range` from the owning handle. `storage.rs:227-228` — `try_get_log_entries` iterates RocksDB for which indexes exist but returns the **cached value** when present. `truncate` (`:342-367`) and `invalidate_cache_range` (`:176-186`) are `monoculture` (5 tests, `integration_cluster` only) and only ever invalidate the caller's own cache.
- **Proposed test**: open storage, append entry E1@idx10, read `10..11` through a reader obtained from `get_log_reader()` (populating the reader's cache), then via the owning handle `truncate(9)` and append a *different* E2@idx10; re-read `10..11` through the **same** reader and assert the payload/term is E2's. Fails today.
- **Boundary**: 2 — a pure storage-layer invariant; a cluster test could never attribute the divergence to this cache.
- **This is a bug report with a regression test, not just a gap.** Cheapest fix: share one `Arc<RwLock<..>>` cache between clones, or drop the cache from the reader clone entirely.

### F4: `get_log_state` violates the openraft contract when the log is empty but entries were purged
- **Severity** 4 — the node restarts with `last_log_id = None` and `purged_next = P+1`; `LogIdList::load_log_ids` returns an empty list, so the node's Raft log appears to start at index 0 again while its purge watermark says P. Index reuse / refusal to converge; at minimum the openraft `last_log_id >= last_purged_log_id` invariant is broken.
- **Likelihood** 3 — normal for any follower that received an `InstallSnapshot` (openraft purges the log to the snapshot's last log id, leaving it empty) and then restarted.
- **Effort** 1 — plain unit test on `ClusterStorage`.
- **Priority** 17
- **Evidence**: `crates/cluster/src/storage.rs:248-274` — `last_log_id` is `None` whenever the log CF iterator is empty, regardless of `last_purged`. openraft 0.9.21 `src/storage/mod.rs:279-286`: *"The returned `last_log_id` could be the log id of the last present log entry, or the `last_purged_log_id` if there is no entry at all."* `purge` itself has 0 tests.
- **Proposed test**: append 5 entries, `purge` all 5, `get_log_state()` → assert `last_log_id == last_purged_log_id == Some(idx5)`; then drop, reopen the same dir, and assert the same. Fails today.
- **Boundary**: 1 — a pure contract property of one function.

### F5: `RemoveNode` leaves migrations pointing at the removed node, and `CompleteSlotMigration` will then assign a slot to a node that does not exist
- **Severity** 4 — the slot's owner is absent from `nodes`, so `wire.rs` drops it from `CLUSTER SLOTS`/`SHARDS` (invisible to clients), issue 36's `count_slot_health` counts it as **ok** (it explicitly buckets an owner-less slot as `ok` "rather than dropped"), and recovery via `CLUSTER ADDSLOTS` fails with `SlotAlreadyAssigned(slot, ghost)` — unrecoverable short of `CLUSTER RESET`. Unavailability for that key range with a green health signal.
- **Likelihood** 3 — `CLUSTER FORGET` of a node during an active migration is an ordinary ops event; `integration_cluster.rs:10450` demonstrates the exact sequence is reachable.
- **Effort** 1 — pure `apply_command` unit test.
- **Priority** 17
- **Evidence**: `crates/cluster/src/commands.rs:82-93` — `RemoveNode` retains slot assignments and removes the node but never touches `inner.migrations`. `commands.rs:353-385` — `CompleteSlotMigration` validates only that the migration record matches; it then `slot_assignment.insert(slot, target_node)` with **no** node-exists check, unlike `AssignSlots` (`:95-98`) and `BeginSlotMigration` (`:323-328`), which both validate. The `Failover { force: true }` arm *does* prune migrations touching the removed node — the asymmetry is the tell. `integration_cluster.rs:10450 test_remove_node_during_active_migration` `SETSLOT ... STABLE`s the slot itself and then asserts only that *some* write on *some* surviving node succeeds; it cannot catch this.
- **Proposed test**: `BeginSlotMigration{slot, A→B}` → `RemoveNode{B}` → assert either the migration is gone or `CompleteSlotMigration{slot, A, B}` returns `NodeNotFound(B)`; and assert the post-state invariant `slot_assignment.values() ⊆ nodes.keys()`. Fails today.
- **Boundary**: 1 — `apply_command` is pure and synchronous; testing this through a cluster would be the geohash anti-pattern.

### F6: The persisted Raft-log-entry and snapshot encodings have no compatibility pin
- **Severity** 4 — `Entry<TypeConfig>` is `serde_json` on disk (`storage.rs:323`) and the snapshot is `serde_json` of `ClusterStateInner` (`state.rs:509, 536`). Adding a field to a `ClusterCommand` variant or to `ClusterStateInner` without `#[serde(default)]` makes a node unable to replay **its own** log after upgrade → crash loop on start; the same break across nodes makes `install_snapshot` fail on a rolling upgrade, stranding a follower.
- **Likelihood** 3 — rolling upgrade is a first-class, tested feature (`FROGDB.FINALIZE`, version gates), and the team has already hit this once: `types.rs:73,77` carry `#[serde(default)]` on later-added `NodeInfo` fields, and `types.rs:541` documents that `SlotMigration`'s removed `state` field relies on serde ignoring unknown fields — with nothing pinning either.
- **Effort** 1 — golden fixtures as string constants.
- **Priority** 17
- **Evidence**: `crates/cluster/src/types.rs:287-288` (`ClusterCommand`, plain `Serialize, Deserialize`, no `#[serde(deny_unknown_fields)]`, no version tag), `types.rs:542-543` + doc at `:541`, `state.rs:30-48` (`ClusterStateInner`, only `active_version` has `#[serde(default)]`). The one existing serde test, `types.rs:714 test_cluster_response_serde_round_trip`, round-trips `ClusterResponse` in-process only — it cannot detect a schema break.
- **Proposed test**: (a) a frozen JSON string per `ClusterCommand` variant (captured today) that must still deserialize into the expected value; (b) a frozen `ClusterStateInner` JSON from an "older" build — one that omits `active_version` and includes a stale `SlotMigration.state` field — that must `install_snapshot` cleanly; (c) a round-trip of every variant. Passes today; guards the future.
- **Boundary**: 1 — pure encoding, the textbook level-1 case.

### F7: The Raft snapshot lifecycle has zero tests, and the one test named for it never takes a snapshot
- **Severity** 4 — `install_snapshot` does `*inner = snapshot_state` (`state.rs:489`), wholesale-replacing cluster topology on a follower. If it is wrong (or the network leg mis-frames a chunk), a catching-up node adopts a bad slot map. Also an availability cliff: a node that can never install a snapshot never catches up.
- **Likelihood** 3 — every node that joins or falls behind an active cluster past the purge point.
- **Effort** 2 — crate-level for the state-machine half; the network leg needs a loopback socket, still crate-level.
- **Priority** 16
- **Evidence**: `state.rs:459-466` (`get_snapshot_builder`), `:468-472` (`begin_receiving_snapshot`), `:474-498` (`install_snapshot`), `:531-564` (`build_snapshot`), `network.rs:586` (`RaftNetwork::install_snapshot`, 18 regions) — **all `untested`, 0 tests**. `integration_cluster.rs:4763 test_raft_snapshot_during_migration` writes a handful of entries against a default `LogsSinceLast(5000)` policy (`cluster_init.rs:304-309` overrides only timers), prints `migration_state_found`/`slot_ownership_clear` via `eprintln!` and asserts only `wait_for_cluster_convergence(...).is_ok()`. Asymmetry worth pinning while here: `get_current_snapshot` returns `Ok(None)` when `last_applied_log` is `None` (`state.rs:505-507`) whereas `build_snapshot` happily emits `"snapshot-0"` (`state.rs:544-547`).
- **Proposed test**: build a populated state (nodes + slots + a migration + `active_version` + a membership entry), `get_snapshot_builder().build_snapshot()`, feed the bytes through `begin_receiving_snapshot` + `install_snapshot` on a *fresh* state machine, assert the resulting `ClusterSnapshot` and `applied_state()` are equal to the source's; separately assert `build_snapshot` on an empty SM yields `last_log_id: None` and that `get_current_snapshot` agrees with it. Plus a `network.rs` loopback test that an `InstallSnapshot` RPC round-trips its chunk bytes intact (`test_all_rpc_variants_roundtrip` today asserts only the discriminant via `matches!`).
- **Boundary**: 2 — the seam is the crate's public `RaftStateMachine` impl. Do **not** try to force a snapshot from a server integration test by writing 5000 entries; that is slow and timing-fragile.

### F8: No property/model test over command sequences asserting slot-map and topology invariants
- **Severity** 4 — this is the layer where "full coverage, no overlap, no orphan slots" either holds or does not, and it is checked nowhere. It is the generalisation that would have caught F5 and F9 automatically.
- **Likelihood** 3 — any resharding or membership churn sequence that the example tests did not enumerate.
- **Effort** 2 — `proptest` is already a workspace dev-dependency; `apply_command` is pure and synchronous, so no harness is needed.
- **Priority** 16
- **Evidence**: `commands.rs` has **no test module** (1 untested span; the file's logic is covered only transitively from `state.rs`'s per-variant tests). Every `state.rs` command test applies exactly one command to a hand-built state. `types.rs:553-568` `ClusterSnapshot::{all_slots_assigned, get_unassigned_slots, assigned_slot_count}` — the natural invariant accessors — are themselves untested and (see Deprioritised) mostly uncalled.
- **Proposed test**: proptest generating sequences of `ClusterCommand` over a small node universe (3–5 ids, a narrowed slot space), applying each through `apply_command`, and after **every** step asserting: (1) `slot_assignment.values() ⊆ nodes.keys()`; (2) every `migrations` entry's `source_node`/`target_node` ∈ `nodes`; (3) every slot key `< CLUSTER_SLOTS`; (4) `config_epoch` never decreases; (5) `config_epoch >= max(node.config_epoch)` (the issue-64 ratchet, currently pinned only by example); (6) a rejected command (`Err`) mutates nothing — byte-compare the serialized state before/after, generalising the one hand-written `failover_validation_failure_mutates_nothing` case.
- **Boundary**: 1–2 — pure function over pure data.
- **OPTIONS**:
  - *(a) proptest over `apply_command` (level 1–2).* Milliseconds, seed-reproducible, shrinks to a minimal failing command sequence, zero new infra. Cannot see anything about replication, timing, or the server's use of the state.
  - *(b) Extend the turmoil cluster sim (level 5).* Exercises the real distributed path, but round-1 issue 11 documented that indefinite partitions are infeasible under turmoil 0.7.1 (ephemeral-port leak on cancelled dials), each scenario costs ~2.5 s plus a `turmoil`-feature build, and a failure is far harder to localise.
  - *(c) A Jepsen model (level 5).* Highest fidelity, highest cost, not in per-PR CI.
  - **Recommendation: (a) now.** It is where the invariants actually live and where F5/F9 are provable. Revisit (b) only for scenarios that need real message loss, and only if the upstream turmoil port-leak is fixed.

### F9: `RemoveSlots` ignores the `node_id` it is given — any node can unassign another node's slots
- **Severity** 4 — `CLUSTER DELSLOTS` issued against node A silently deletes slots owned by node B; the slots become unassigned with no error and no event, and clients get `CLUSTERDOWN` for that range.
- **Likelihood** 2 — requires an operator or tool to target the wrong node, or a server-side call site that does not pre-validate ownership.
- **Effort** 1.
- **Priority** 15
- **Evidence**: `crates/cluster/src/commands.rs:119-136` — `node_id` appears only in the `tracing::debug!` at `:134`; the validation loop checks solely that each slot is assigned *to somebody*, and the mutation loop `remove`s unconditionally. Compare `AssignSlots` (`:95-108`), which validates node existence *and* rejects a conflicting owner. No test asserts a wrong-owner rejection.
- **Proposed test**: assign slots 0–10 to A, apply `RemoveSlots { node_id: B, slots: 0..=10 }`, assert `Err(...)` and that A still owns 0–10. Fails today; needs a source decision (reject vs. document as intentional) — Redis's `DELSLOTS` is deliberately owner-agnostic on the *local* node but never removes another node's assignment from the shared map, so rejecting is the parity-correct behaviour.
- **Boundary**: 1.

### F10: `append` acknowledges Raft log durability without an fsync
- **Severity** 5 — openraft treats `log_io_completed(Ok(()))` as "this entry is durable"; a leader can therefore commit and act on a topology decision (slot transfer, failover, epoch bump) that is lost on power failure, which is a consensus-safety violation, not just data loss.
- **Likelihood** 2 — needs a machine/OS crash (the RocksDB WAL survives a process crash), which for a production DB is a "when", not "if", but is not everyday.
- **Effort** 4 — proving loss needs filesystem-level fault injection; a white-box assertion on `WriteOptions` is possible but tests the implementation, not the behaviour.
- **Priority** 15
- **Evidence**: `crates/cluster/src/storage.rs:333-337` — `self.db.write(batch)` with default `WriteOptions` (`sync = false`), then `callback.log_io_completed(Ok(()))` immediately. Contrast `save_vote` at `:290-296`, which *does* `self.db.flush()`. Same asymmetry noted in `epoch-fold-redesign.md` §2 ("appends use default `WriteOptions` (no per-append fsync)"). Round 1 issue 12 covered the *data-plane* fsync boundary; the Raft log was not in its scope.
- **Proposed test**: this is primarily a **source fix** (`WriteOptions::set_sync(true)`, or batch-and-flush before signalling the callback, with the cost measured). Test value is low relative to effort — the honest deliverable is a benchmark of sync-vs-async append plus a config knob, not a test.
- **Boundary**: n/a — recommend filing as a durability *issue*, not test work. Flagged here because the audit found it.

### F11: Nothing ever removes a node from the Raft voter set, and the add-voter failure path is untested
- **Severity** 4 — `CLUSTER FORGET` removes a node from `ClusterState` but leaves it a Raft voter, so quorum is still computed over the departed node: a 3→2 node shrink leaves a 3-voter Raft that cannot tolerate a single further failure. The inverse divergence (in `ClusterState`, not a voter) already has an in-source warning with no test.
- **Likelihood** 3 — any cluster shrink; also any `MEET` whose voter promotion exhausts its retries.
- **Effort** 3 — needs the multi-node harness, but `cluster_harness` already exposes `raft()` and `cluster_state()`, so the assertion is a two-liner.
- **Priority** 15
- **Evidence**: repo-wide, `change_membership`/`add_learner` appear **only** at `crates/cluster/src/network.rs:653-657` (`spawn_add_raft_voter`) — there is no removal counterpart anywhere. `network.rs:~660` retry-exhaustion arm (logs "node is in cluster state but NOT a Raft voter") is `untested`. `integration_cluster.rs:8512 test_cluster_meet_adds_node_to_raft_voters` asserts the *add* direction only; `:4247 test_cluster_forget_removes_node` asserts only `ClusterState`.
- **Proposed test**: after `CLUSTER FORGET`, assert `node.raft().metrics().borrow().membership_config.membership()` no longer lists the forgotten id, and that the surviving cluster still elects a leader after killing one of the two remaining nodes. Fails today (the removal path does not exist) — file the source gap alongside. Separately, a `spawn_add_raft_voter` unit test with an always-failing fake proposer asserting the exhaustion arm is observable (metric or `ClusterState`↔voter reconciliation), not just logged.
- **Boundary**: 5 for the membership assertion (genuinely multi-node), 2 for the retry-exhaustion arm.
- **OPTIONS**:
  - *(a) Cluster-harness assertion on `raft().metrics()` (level 5).* Directly observes the real invariant; ~seconds of runtime; the accessor already exists.
  - *(b) Introduce a `RaftMembershipAdmin` seam mirroring `writer.rs`'s `RaftProposer` and unit-test the add/remove/retry logic against a fake (level 2).* Fast and covers the retry arm properly, but needs a small refactor of `spawn_add_raft_voter` and does not prove real openraft accepts the change.
  - **Recommendation: both — (a) for the missing-removal invariant, (b) for the retry/exhaustion logic**; (b) also makes the "in cluster state but not a voter" reconciliation loop testable when it is written.

### F12: The `EntryPayload::Membership` apply arm is untested
- **Severity** 3 — `last_membership` is what `applied_state()` returns to openraft on recovery; if it is not tracked correctly, a restarted node recovers with the wrong membership view.
- **Likelihood** 3 — a membership entry is applied on every join/bootstrap.
- **Effort** 1.
- **Priority** 14
- **Evidence**: `crates/cluster/src/state.rs:445-448` — `inner.last_membership = StoredMembership::new(Some(log_id), membership)`; every `apply` test in the 1830-line module builds `EntryPayload::Normal` or `Blank` only (see `state.rs:795, 830, 863, 910, 926, 2102...`). `applied_state` (`:367-377`) is consequently only ever observed with a default membership.
- **Proposed test**: apply `Blank`, `Normal`, `Membership` in sequence; assert `applied_state()` returns the last log id **and** the membership from the `Membership` entry, and that a subsequent `Normal` entry advances `last_applied_log` without clobbering `last_membership`.
- **Boundary**: 1.

### F13: Cluster-bus wire negative paths are untested (20 untested spans in `network.rs`)
- **Severity** 3 — a malformed/oversized/truncated frame from a peer (or a version-skewed peer) should be rejected cleanly; today an unhandled path could drop a connection carrying Raft traffic, or worse, silently mis-route.
- **Likelihood** 3 — version skew during rolling upgrade, a half-open socket, an unrelated process dialling the bus port.
- **Effort** 1 — the codec is constructible standalone; `parse_rpc_message` is a pure function.
- **Priority** 14
- **Evidence**: `crates/cluster/src/network.rs` — `parse_rpc_message` deserialization-failure arm, `health_probe` (`:383`, 13 regions), `with_timeouts` (`:253`) all `untested`; no test drives a frame over the 64 MiB `LengthDelimitedCodec` cap or a truncated length prefix. `new_client` silently falls back to `"127.0.0.1:16379"` on an unparsable address — an untested silent-misdirection path. `test_all_rpc_variants_roundtrip` compares only discriminants.
- **Proposed test**: table-driven — garbage bytes, a truncated frame, a frame declaring a length above the cap, and a valid frame of an unknown variant → assert each yields a clean typed error and (for the socket-level ones) that the server responds with `ClusterRpcResponse::Error` rather than closing; plus strengthen the round-trip test to compare full payload equality; plus assert `new_client` on an unparsable address returns an error instead of dialling localhost.
- **Boundary**: 1 for `parse_rpc_message`, 2 for the loopback framing cases.

### F14: Several cluster tests are structurally incapable of failing
- **Severity** 2 — no production consequence directly, but these tests create false confidence in exactly the areas F5/F7 cover, which is how those gaps survived round 1.
- **Likelihood** 4 — they run on every CI pass today, reporting green.
- **Effort** 1 — edit assertions in place.
- **Priority** 13
- **Evidence**: `integration_cluster.rs:10503-10510` (`test_remove_node_during_active_migration`) `return`s early — a **pass** — if migration setup errors, then asserts only that a write succeeded on some surviving node; `:4763` (`test_raft_snapshot_during_migration`) computes `migration_state_found`/`slot_ownership_clear` and only `eprintln!`s them; `storage.rs` `test_storage_committed` saves then clears `committed` and never reads it back; `network.rs` `test_all_rpc_variants_roundtrip` asserts `matches!` on the discriminant only.
- **Proposed test**: convert early-`return`s to `panic!`/`assert!` on setup failure; make `test_remove_node_during_active_migration` assert the *invariant* (slot 950 is owned by exactly one **live** node, and no migration record references the forgotten node — i.e. the F5 assertion); rename `test_raft_snapshot_during_migration` to what it tests (leader failover mid-migration converges) and let F7 own the real snapshot coverage.
- **Boundary**: unchanged from each test's current level — this is an assertion-strength fix, not a relocation.

### F15: `SlotRange::new` guards with `debug_assert!` only, so release builds accept invalid ranges that silently no-op
- **Severity** 3 — in a release binary, `SlotRange { start: 100, end: 50 }` passes construction, `iter()` yields nothing, and `AssignSlots` returns `Ok` having assigned **nothing**: `CLUSTER ADDSLOTS` reports success while the slots stay unowned. `end >= CLUSTER_SLOTS` is likewise unchecked outside debug.
- **Likelihood** 2 — needs a malformed range to reach the command, which server-side parsing may catch first (that parsing is a sibling agent's area).
- **Effort** 1.
- **Priority** 12
- **Evidence**: `crates/cluster/src/types.rs` `SlotRange::new` (`debug_assert!` only) and `is_empty()` hard-coded to `false`; 18 untested spans in `types.rs`. `commands.rs:100-114` iterates `range.iter()` with no emptiness check, so an inverted range is indistinguishable from success.
- **Proposed test**: assert `AssignSlots` with an inverted range and with `end >= CLUSTER_SLOTS` returns `Err(...)` (not `Ok`) in a **release-profile-equivalent** path, i.e. by testing `apply_command`'s behaviour rather than relying on the `debug_assert`. Fails today; needs the `debug_assert!` promoted to a real validation.
- **Boundary**: 1.

### F16: `save_committed` is write-only — `read_committed` is never overridden
- **Severity** 2 — `KEY_COMMITTED` is persisted and never read, so openraft's default `read_committed → None` is used and the persisted value is dead weight; recovery relies on `last_applied` instead. Mostly a correctness-of-recovery nuance, but it interacts with F1: with a persisted snapshot (T7), reading `committed` back would matter.
- **Likelihood** 3 — every restart takes this path.
- **Effort** 1.
- **Priority** 11
- **Evidence**: `crates/cluster/src/storage.rs:298-308` writes/deletes `KEY_COMMITTED`; no `read_committed` impl exists in the file, so openraft's default (`src/storage/mod.rs:272-275`) applies. `test_storage_committed` never reads it back (see F14). `epoch-fold-redesign.md` notes "optionally override `read_committed`" as part of T7.
- **Proposed test**: `save_committed(Some(id))` → reopen → assert `read_committed() == Some(id)`; and `save_committed(None)` → reopen → `None`. Fails today until `read_committed` is implemented; fold into T7.
- **Boundary**: 1.

## Deprioritised

- **Dead public API — delete rather than test.** `ClusterState::from_snapshot` (`state.rs:58`), `ClusterState::self_node_id_atomic` (`:86`), `ClusterState::get_node_slots` (`:118`), and `ClusterSnapshot::{get_unassigned_slots, assigned_slot_count, get_primaries}` (`types.rs:553+`) have **zero callers repo-wide** (only `get_replicas` is used, by `failure_detector.rs:340`). Writing tests for them would cement dead code; `from_snapshot` is additionally a trap (it discards `last_applied_log`/`last_membership`, so any future caller would double-apply on restart).
- **`CancelSlotMigration` is unconditionally `Ok`** (`commands.rs:387-391`) even when no migration exists and regardless of caller. Idempotence is defensible; F8's invariant sweep covers the state consequences.
- **`BeginSlotMigration` skips the owner check when the slot is unassigned** (`commands.rs:330-339`). The comment explains why (followers may have no local assignment). Real, but the pending `exec-slot-revalidation.md` PRD reworks the slot-authority model — proposing an assertion now would pre-empt an unreviewed design decision.
- **`FinalizeUpgrade` re-parses the target version once per node** (`commands.rs:408`) — an O(n) inefficiency and a per-node error message for a global error; cosmetic.
- **`mint_config_epoch`/`max_node_epoch` are `monoculture`** (2 tests, unit-only). Issue 64 covered the semantics well; F8's monotonicity property subsumes the residue.
- **`wire::standalone_snapshot` `monoculture`** (2 tests). Low consequence — a rendering path for a non-cluster node.
- **More turmoil scenarios.** Deliberately deprioritised: see the Summary. The upstream turmoil 0.7.1 ephemeral-port leak makes indefinite partitions infeasible, which removes the majority-loss scenario that would justify the cost.
- **shuttle/loom on `ClusterState`.** The only shared mutable state is one `parking_lot::RwLock` around `inner`, with all mutation funnelled through `apply_command` from openraft's single apply task. No interleaving to explore.

## Cross-area notes

- **Majority-loss write refusal is not decidable inside this crate.** The cluster crate exposes no quorum predicate; `writer.rs` only surfaces `REDIRECT`/`CLUSTERDOWN` for *metadata* proposals (well covered by its 9 fake-seam tests). Whether the **data** plane refuses writes on quorum loss lives in `server/src/cluster_flags.rs` + `connection/cluster.rs` — the sibling cluster/slot-migration agent's area. Existing coverage there: `integration_cluster.rs:1534 test_split_brain_writes_fail_on_minority`, `:2791 test_minority_rejects_writes`, `:7721 test_minority_node_rejects_writes_on_quorum_loss`. **Handoff:** the ghost-owner state from F5 makes a slot invisible in `CLUSTER SLOTS` while issue-36's `count_slot_health` counts it `ok` — that agent should add the corresponding server-level assertion (`cluster_state` must not be `ok` when a slot's owner is absent from `nodes`).
- **`failure_detector.rs:340` is the only consumer of `ClusterSnapshot::get_replicas`** — if that agent proposes replica-selection tests, the accessor is currently untested at this level; F8's invariants would make it safe to rely on.
- **Shared infrastructure needed: none.** Every level-1/2 finding lands with existing dev-dependencies (`proptest`, `tempfile`, `tokio::test`). The only cross-cutting ask is the `cluster_harness` already exposing `raft()`/`cluster_state()`, which it does (`cluster_harness.rs:239,244`) — F11(a) needs no harness change.
- **Blocked on unreviewed PRDs:** F1 (and F16's read-back half) belong to `epoch-fold-redesign.md` **T7**, which the PRD itself says should be filed as its own issue if not folded into that plan. F1 is the single highest-priority finding in this crate and is currently gated on a user review decision — worth surfacing.
- **Bugs found, not just gaps** (proposed tests fail against today's source): F3 (stale log-reader cache — Raft log divergence), F4 (`get_log_state` contract), F5 (ghost slot owner), F9 (`RemoveSlots` ignores `node_id`), F11 (no Raft voter removal), F15 (`debug_assert`-only range validation), plus F10 as a durability design defect.
