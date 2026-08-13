# cluster — residual test gaps (11 findings)

Status: ready-for-agent
Type: AFK
Origin: round-2 testing audit 2026-07-28 — 15 parallel area audits, `.scratch/testing-improvements-round2/`
Source: proposals/11 — residual findings after promotion to issues 19–76
Score: 11 findings, priority range 11–19
Area: `frogdb-server/crates/cluster/src/` — `state.rs`, `network.rs`, `types.rs`, `storage.rs`, `commands.rs`, `wire.rs`, `writer.rs`, `version_gate.rs`

## Context

This area is the whole `cluster` crate: `ClusterState`/`ClusterStateMachine` (openraft
`RaftStateMachine` + `RaftSnapshotBuilder`), the RocksDB `RaftLogStorage`/`RaftLogReader`, the
cluster-bus codec and `RaftNetwork`, `apply_command` (the single validated mutation path),
`CLUSTER NODES/SLOTS/SHARDS` rendering, the propose→forward→redirect saga, and the
rolling-upgrade version gates. There is **no `tests/` dir** and 7 inline `#[cfg(test)]`
modules; coverage is **89.3 % lines (3,380/3,787)** and 88.1 % regions (4,947/5,616), with
828 raw function records reducing to **393 after span-dedup**: `untested` **74**,
`single-test` 152, `monoculture` 40, `covered` 1, `well-covered` 126 (untested spans by file:
`network.rs` 20, `storage.rs` 20, `types.rs` 18, `state.rs` 15, `commands.rs` 1). The
proposal's verdict on the *shape* of that coverage: "The 89.3 % headline hides a sharply
bimodal crate: everything reachable from a *running, healthy, small* cluster is densely
tested, and everything on the **recovery** paths is not" — the entire Raft snapshot lifecycle
and log compaction have zero tests, and `commands.rs` has no test module at all.

## Promoted elsewhere

- F3 → issue 53, `.scratch/testing-improvements-round2/issues/` (stale Raft log-reader cache serves overwritten-term entries)
- F5 → issue 62, `.scratch/testing-improvements-round2/issues/` (`RemoveNode` leaves dangling migrations → slot assigned to a ghost node)
- F9 → issue 62, `.scratch/testing-improvements-round2/issues/` (`RemoveSlots` ignores its `node_id`)
- F10 → issue 73, `.scratch/testing-improvements-round2/issues/` (Raft `append` acks durability without fsync)
- F14 → issue 33, `.scratch/testing-improvements-round2/issues/` (`MASTER.md` §4 — cluster tests structurally incapable of failing)

## Residual findings

### F1 — A purged Raft log plus an unpersisted state machine silently loses cluster topology on restart

**BLOCKED on `.scratch/replication-cluster-rework/`** — the assertion cannot pass until
`epoch-fold-redesign.md` task **T7 ("Durability: persist the state-machine snapshot")** is
implemented, and that PRD is unreviewed; the proposal's own recommendation is to file this
test *as T7's acceptance criterion* rather than as standalone test work.

- **Severity** 5 — the restarted node rebuilds `slot_assignment`, `nodes`, `config_epoch` from only the *unpurged* log suffix. Silent consistency loss: slots vanish from the map, epochs regress, and `CLUSTER INFO` still reports `ok`.
- **Likelihood** 3 — needs ≥5000 Raft entries (default `SnapshotPolicy::LogsSinceLast(5000)`, `max_in_snapshot_log_to_keep = 1000`; `cluster_init.rs:304-309` overrides only the timers) before a restart, or any follower that received an `InstallSnapshot` (which purges its log outright) and then restarts.
- **Effort** 2 — crate-level: `ClusterStorage` + `ClusterStateMachine` on a `tempdir`, no server.
- **Priority** 19
- **Evidence**: `crates/cluster/src/state.rs:474-527` — the state machine is pure in-memory; `install_snapshot`/`get_current_snapshot` never touch `ClusterStorage`. `crates/cluster/src/storage.rs:369-400` — `purge` deletes entries and persists `KEY_LAST_PURGED`, both **untested** (`untested`, 45 regions). openraft's `StorageHelper::get_initial_state` (`helper.rs:126-140`) rebuilds a snapshot from the *empty* state machine when `last_purged_log_id.is_some()` and no snapshot exists. `epoch-fold-redesign.md` §2.4 + T7 describe the same hazard and remain **unimplemented, awaiting user review**.
- **Proposed test**: apply ≥6000 commands through the state machine while appending them to a real `ClusterStorage`; take a snapshot, `purge` to `last_applied - 1000`, drop everything, reopen the DB into a fresh `ClusterStateMachine`, run openraft's recovery, and assert the recovered `ClusterSnapshot` equals the pre-restart one (slot map, node set, `config_epoch`, `active_version`) — not merely `>=`.
- **Boundary**: 2 (crate-level API test) — needs the real RocksDB storage and the real state machine, but nothing above them; a server-level test would add minutes of runtime for zero extra coverage.
- **Status**: **blocked on `epoch-fold-redesign.md` T7.** The assertion cannot pass until snapshot persistence exists. File this test *as T7's acceptance criterion* rather than as standalone test work.

### F2 — Nothing asserts that replaying a Raft log reproduces the state that produced it

- **Severity** 4 — log replay is the recovery path on *every* node restart. A non-deterministic or order-sensitive `apply_command` diverges nodes silently; two nodes then disagree about slot ownership with no error anywhere.
- **Likelihood** 4 — every restart, every follower catch-up.
- **Effort** 2 — pure in-memory: two `ClusterStateMachine`s, no I/O.
- **Priority** 18
- **Evidence**: `crates/cluster/src/state.rs:380-457` (`apply`) is `well-covered` by *aggregate* execution but no test compares two independently-derived states. The closest existing test, issue 16's `storage.rs:475 test_config_epoch_round_trips_through_storage_restart`, replays entries into a fresh state machine but asserts **only `config_epoch == 3`**, and does so by reaching into the private `cf_logs()`/`db` — an internals-coupled test that a storage refactor breaks.
- **Proposed test**: proptest over a generated `Vec<ClusterCommand>` — (a) apply to SM-A entry-by-entry; (b) apply the identical entries to a fresh SM-B; assert `serde_json::to_vec(A.inner) == serde_json::to_vec(B.inner)` and that the `Vec<ClusterResponse>` sequences are identical; (c) apply the first half to SM-C, `build_snapshot`, `install_snapshot` into SM-D, apply the second half to D, assert D == A. (c) is the snapshot-fidelity assertion F7 also wants. Additionally: move issue 16's replay test up to this boundary so it stops reaching into `db`.
- **Boundary**: 2 — the behaviour is entirely inside the crate's public state-machine surface; the current level-3 storage-internals variant should be **moved down** to here.

### F4 — `get_log_state` violates the openraft contract when the log is empty but entries were purged

- **Severity** 4 — the node restarts with `last_log_id = None` and `purged_next = P+1`; `LogIdList::load_log_ids` returns an empty list, so the node's Raft log appears to start at index 0 again while its purge watermark says P. Index reuse / refusal to converge; at minimum the openraft `last_log_id >= last_purged_log_id` invariant is broken.
- **Likelihood** 3 — normal for any follower that received an `InstallSnapshot` (openraft purges the log to the snapshot's last log id, leaving it empty) and then restarted.
- **Effort** 1 — plain unit test on `ClusterStorage`.
- **Priority** 17
- **Evidence**: `crates/cluster/src/storage.rs:248-274` — `last_log_id` is `None` whenever the log CF iterator is empty, regardless of `last_purged`. openraft 0.9.21 `src/storage/mod.rs:279-286`: *"The returned `last_log_id` could be the log id of the last present log entry, or the `last_purged_log_id` if there is no entry at all."* `purge` itself has 0 tests.
- **Proposed test**: append 5 entries, `purge` all 5, `get_log_state()` → assert `last_log_id == last_purged_log_id == Some(idx5)`; then drop, reopen the same dir, and assert the same. Fails today.
- **Boundary**: 1 — a pure contract property of one function.

### F6 — The persisted Raft-log-entry and snapshot encodings have no compatibility pin

- **Severity** 4 — `Entry<TypeConfig>` is `serde_json` on disk (`storage.rs:323`) and the snapshot is `serde_json` of `ClusterStateInner` (`state.rs:509, 536`). Adding a field to a `ClusterCommand` variant or to `ClusterStateInner` without `#[serde(default)]` makes a node unable to replay **its own** log after upgrade → crash loop on start; the same break across nodes makes `install_snapshot` fail on a rolling upgrade, stranding a follower.
- **Likelihood** 3 — rolling upgrade is a first-class, tested feature (`FROGDB.FINALIZE`, version gates), and the team has already hit this once: `types.rs:73,77` carry `#[serde(default)]` on later-added `NodeInfo` fields, and `types.rs:541` documents that `SlotMigration`'s removed `state` field relies on serde ignoring unknown fields — with nothing pinning either.
- **Effort** 1 — golden fixtures as string constants.
- **Priority** 17
- **Evidence**: `crates/cluster/src/types.rs:287-288` (`ClusterCommand`, plain `Serialize, Deserialize`, no `#[serde(deny_unknown_fields)]`, no version tag), `types.rs:542-543` + doc at `:541`, `state.rs:30-48` (`ClusterStateInner`, only `active_version` has `#[serde(default)]`). The one existing serde test, `types.rs:714 test_cluster_response_serde_round_trip`, round-trips `ClusterResponse` in-process only — it cannot detect a schema break.
- **Proposed test**: (a) a frozen JSON string per `ClusterCommand` variant (captured today) that must still deserialize into the expected value; (b) a frozen `ClusterStateInner` JSON from an "older" build — one that omits `active_version` and includes a stale `SlotMigration.state` field — that must `install_snapshot` cleanly; (c) a round-trip of every variant. Passes today; guards the future.
- **Boundary**: 1 — pure encoding, the textbook level-1 case.

### F7 — The Raft snapshot lifecycle has zero tests, and the one test named for it never takes a snapshot

- **Severity** 4 — `install_snapshot` does `*inner = snapshot_state` (`state.rs:489`), wholesale-replacing cluster topology on a follower. If it is wrong (or the network leg mis-frames a chunk), a catching-up node adopts a bad slot map. Also an availability cliff: a node that can never install a snapshot never catches up.
- **Likelihood** 3 — every node that joins or falls behind an active cluster past the purge point.
- **Effort** 2 — crate-level for the state-machine half; the network leg needs a loopback socket, still crate-level.
- **Priority** 16
- **Evidence**: `state.rs:459-466` (`get_snapshot_builder`), `:468-472` (`begin_receiving_snapshot`), `:474-498` (`install_snapshot`), `:531-564` (`build_snapshot`), `network.rs:586` (`RaftNetwork::install_snapshot`, 18 regions) — **all `untested`, 0 tests**. `integration_cluster.rs:4763 test_raft_snapshot_during_migration` writes a handful of entries against a default `LogsSinceLast(5000)` policy (`cluster_init.rs:304-309` overrides only timers), prints `migration_state_found`/`slot_ownership_clear` via `eprintln!` and asserts only `wait_for_cluster_convergence(...).is_ok()`. Asymmetry worth pinning while here: `get_current_snapshot` returns `Ok(None)` when `last_applied_log` is `None` (`state.rs:505-507`) whereas `build_snapshot` happily emits `"snapshot-0"` (`state.rs:544-547`).
- **Proposed test**: build a populated state (nodes + slots + a migration + `active_version` + a membership entry), `get_snapshot_builder().build_snapshot()`, feed the bytes through `begin_receiving_snapshot` + `install_snapshot` on a *fresh* state machine, assert the resulting `ClusterSnapshot` and `applied_state()` are equal to the source's; separately assert `build_snapshot` on an empty SM yields `last_log_id: None` and that `get_current_snapshot` agrees with it. Plus a `network.rs` loopback test that an `InstallSnapshot` RPC round-trips its chunk bytes intact (`test_all_rpc_variants_roundtrip` today asserts only the discriminant via `matches!`).
- **Boundary**: 2 — the seam is the crate's public `RaftStateMachine` impl. Do **not** try to force a snapshot from a server integration test by writing 5000 entries; that is slow and timing-fragile.
- **Cross-reference**: the "rename `test_raft_snapshot_during_migration` to what it actually tests" half is owned by issue 33, `.scratch/testing-improvements-round2/issues/` (via F14); the real snapshot coverage stays here.

### F8 — No property/model test over command sequences asserting slot-map and topology invariants

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
- **Note**: the two defects this property would have caught, 11/F5 and 11/F9, are filed as issue 62, `.scratch/testing-improvements-round2/issues/`; this issue owns the generalisation, not those two rows.

### F11 — Nothing ever removes a node from the Raft voter set, and the add-voter failure path is untested

- **STATUS: RESOLVED** (removal half). The source gap is fixed and pinned by **FM-CLUSTER-101**
  (`specs/cluster.md`): `voter_change` is now the single rule
  mapping a committed `ClusterCommand` to its voter-set effect, consulted by both leader-side commit
  sites, and `spawn_remove_raft_voter` mirrors `spawn_add_raft_voter` (same retry schedule,
  `RemoveVoters`/`RemoveNodes` classified against the membership in force). Forced by
  `every_command_that_moves_the_node_set_owes_a_voter_change`,
  `a_removal_is_planned_against_the_membership_in_force`,
  `forgetting_a_learner_drops_it_from_the_membership`, `forgetting_a_stranger_proposes_nothing`,
  `the_last_voter_is_never_removed` (all in `crates/cluster/src/network.rs`) and
  `test_cluster_forget_shrinks_the_raft_voter_set` (`server/tests/cluster_failover.rs`), which does
  the 4→3 shrink rather than the 3→2 one proposed below — 2 voters still need a quorum of 2, so only
  a 4→3 shrink actually buys a further failure. The **retry/exhaustion half of this finding stands**:
  the give-up arm of both `spawn_add_raft_voter` and `spawn_remove_raft_voter` is still logged and
  not otherwise observable, and option (b) below (a `RaftMembershipAdmin` seam) is still the way to
  force it.
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

### F12 — The `EntryPayload::Membership` apply arm is untested

- **Severity** 3 — `last_membership` is what `applied_state()` returns to openraft on recovery; if it is not tracked correctly, a restarted node recovers with the wrong membership view.
- **Likelihood** 3 — a membership entry is applied on every join/bootstrap.
- **Effort** 1.
- **Priority** 14
- **Evidence**: `crates/cluster/src/state.rs:445-448` — `inner.last_membership = StoredMembership::new(Some(log_id), membership)`; every `apply` test in the 1830-line module builds `EntryPayload::Normal` or `Blank` only (see `state.rs:795, 830, 863, 910, 926, 2102...`). `applied_state` (`:367-377`) is consequently only ever observed with a default membership.
- **Proposed test**: apply `Blank`, `Normal`, `Membership` in sequence; assert `applied_state()` returns the last log id **and** the membership from the `Membership` entry, and that a subsequent `Normal` entry advances `last_applied_log` without clobbering `last_membership`.
- **Boundary**: 1.

### F13 — Cluster-bus wire negative paths are untested (20 untested spans in `network.rs`)

- **Severity** 3 — a malformed/oversized/truncated frame from a peer (or a version-skewed peer) should be rejected cleanly; today an unhandled path could drop a connection carrying Raft traffic, or worse, silently mis-route.
- **Likelihood** 3 — version skew during rolling upgrade, a half-open socket, an unrelated process dialling the bus port.
- **Effort** 1 — the codec is constructible standalone; `parse_rpc_message` is a pure function.
- **Priority** 14
- **Evidence**: `crates/cluster/src/network.rs` — `parse_rpc_message` deserialization-failure arm, `health_probe` (`:383`, 13 regions), `with_timeouts` (`:253`) all `untested`; no test drives a frame over the 64 MiB `LengthDelimitedCodec` cap or a truncated length prefix. `new_client` silently falls back to `"127.0.0.1:16379"` on an unparsable address — an untested silent-misdirection path. `test_all_rpc_variants_roundtrip` compares only discriminants.
- **Proposed test**: table-driven — garbage bytes, a truncated frame, a frame declaring a length above the cap, and a valid frame of an unknown variant → assert each yields a clean typed error and (for the socket-level ones) that the server responds with `ClusterRpcResponse::Error` rather than closing; plus strengthen the round-trip test to compare full payload equality; plus assert `new_client` on an unparsable address returns an error instead of dialling localhost.
- **Boundary**: 1 for `parse_rpc_message`, 2 for the loopback framing cases.
- **Cross-reference**: the "strengthen `test_all_rpc_variants_roundtrip` beyond `matches!`" half is also listed by issue 33, `.scratch/testing-improvements-round2/issues/` (via F14). The negative-frame table is not, and stays here. `INFRASTRUCTURE.md`'s I14 note (issue 14, same directory) observes that typing `parse_rpc_message`'s error taxonomy in this same file would let 04/F10 drop its string matching — if that refactor happens, do both at once.

### F15 — `SlotRange::new` guards with `debug_assert!` only, so release builds accept invalid ranges that silently no-op

- **Severity** 3 — in a release binary, `SlotRange { start: 100, end: 50 }` passes construction, `iter()` yields nothing, and `AssignSlots` returns `Ok` having assigned **nothing**: `CLUSTER ADDSLOTS` reports success while the slots stay unowned. `end >= CLUSTER_SLOTS` is likewise unchecked outside debug.
- **Likelihood** 2 — needs a malformed range to reach the command, which server-side parsing may catch first (that parsing is a sibling agent's area).
- **Effort** 1.
- **Priority** 12
- **Evidence**: `crates/cluster/src/types.rs` `SlotRange::new` (`debug_assert!` only) and `is_empty()` hard-coded to `false`; 18 untested spans in `types.rs`. `commands.rs:100-114` iterates `range.iter()` with no emptiness check, so an inverted range is indistinguishable from success.
- **Proposed test**: assert `AssignSlots` with an inverted range and with `end >= CLUSTER_SLOTS` returns `Err(...)` (not `Ok`) in a **release-profile-equivalent** path, i.e. by testing `apply_command`'s behaviour rather than relying on the `debug_assert`. Fails today; needs the `debug_assert!` promoted to a real validation.
- **Boundary**: 1.

### F16 — `save_committed` is write-only — `read_committed` is never overridden

**BLOCKED on `.scratch/replication-cluster-rework/` (read-back half only)** — the proposal
folds the `read_committed` implementation into `epoch-fold-redesign.md` T7, which is
unreviewed; the "`test_storage_committed` never reads its value back" half is actionable
today.

- **Severity** 2 — `KEY_COMMITTED` is persisted and never read, so openraft's default `read_committed → None` is used and the persisted value is dead weight; recovery relies on `last_applied` instead. Mostly a correctness-of-recovery nuance, but it interacts with F1: with a persisted snapshot (T7), reading `committed` back would matter.
- **Likelihood** 3 — every restart takes this path.
- **Effort** 1.
- **Priority** 11
- **Evidence**: `crates/cluster/src/storage.rs:298-308` writes/deletes `KEY_COMMITTED`; no `read_committed` impl exists in the file, so openraft's default (`src/storage/mod.rs:272-275`) applies. `test_storage_committed` never reads it back (see F14). `epoch-fold-redesign.md` notes "optionally override `read_committed`" as part of T7.
- **Proposed test**: `save_committed(Some(id))` → reopen → assert `read_committed() == Some(id)`; and `save_committed(None)` → reopen → `None`. Fails today until `read_committed` is implemented; fold into T7.
- **Boundary**: 1.
- **Cross-reference**: the assertion-free `test_storage_committed` is listed by issue 33, `.scratch/testing-improvements-round2/issues/` (via F14); the `read_committed` implementation and its round-trip stay here.

## Acceptance criteria

- [ ] F1: a test applies ≥6000 commands through a real `ClusterStorage` + `ClusterStateMachine`, snapshots, purges to `last_applied - 1000`, reopens into a fresh state machine, and asserts the recovered `ClusterSnapshot` **equals** (not `>=`) the pre-restart slot map, node set, `config_epoch` and `active_version`.
- [ ] F2: a proptest over generated `Vec<ClusterCommand>` asserts (a) two independently-fed state machines serialize byte-identically and produce identical `Vec<ClusterResponse>`, and (b) a half-applied SM snapshotted into a fresh SM and then fed the second half equals the straight-through SM; and issue 16's replay test no longer reaches into `cf_logs()`/`db`.
- [ ] F4: a test asserts that after appending 5 entries and purging all 5, `get_log_state()` returns `last_log_id == last_purged_log_id == Some(idx5)`, both before and after reopening the same directory.
- [ ] F6: a frozen JSON string per `ClusterCommand` variant still deserializes to the expected value; a frozen "older-build" `ClusterStateInner` (no `active_version`, with a stale `SlotMigration.state`) installs cleanly via `install_snapshot`; and every variant round-trips.
- [ ] F7: a test builds a populated state, runs `get_snapshot_builder().build_snapshot()` → `begin_receiving_snapshot` → `install_snapshot` into a fresh state machine and asserts the resulting `ClusterSnapshot` and `applied_state()` equal the source's; plus assertions that `build_snapshot` on an empty SM yields `last_log_id: None` and agrees with `get_current_snapshot`; plus a `network.rs` loopback test asserting an `InstallSnapshot` RPC round-trips its chunk bytes intact.
- [ ] F8: a proptest over `ClusterCommand` sequences asserts, after every step, all six invariants — `slot_assignment.values() ⊆ nodes.keys()`; every migration's `source_node`/`target_node` ∈ `nodes`; every slot key `< CLUSTER_SLOTS`; `config_epoch` never decreases; `config_epoch >= max(node.config_epoch)`; and an `Err` command leaves the serialized state byte-identical.
- [ ] F11: ~~a test asserts that after `CLUSTER FORGET` the forgotten id is absent from `raft().metrics().borrow().membership_config.membership()` and that the surviving cluster still commits after one of the remaining nodes is killed~~ **done** — FM-CLUSTER-101, `test_cluster_forget_shrinks_the_raft_voter_set` (a 4→3 shrink; the originally-proposed 3→2 one proves nothing, since 2 voters still need a quorum of 2). Remaining: a unit test with an always-failing fake proposer asserting the retry-*exhaustion* arm of `spawn_add_raft_voter`/`spawn_remove_raft_voter` is observable in a metric or in `ClusterState`, not only logged.
- [ ] F12: a test applies `Blank`, `Normal`, `Membership` in sequence and asserts `applied_state()` returns the last log id and the membership from the `Membership` entry, and that a subsequent `Normal` advances `last_applied_log` without clobbering `last_membership`.
- [ ] F13: a table test asserts garbage bytes, a truncated frame, an over-cap length prefix and a valid frame of an unknown variant each yield a clean typed error (and a `ClusterRpcResponse::Error` rather than a closed connection at the socket level); that the RPC round-trip test compares full payloads, not discriminants; and that `new_client` on an unparsable address returns an error instead of dialling `127.0.0.1:16379`.
- [ ] F15: a test asserts `AssignSlots` with an inverted range and with `end >= CLUSTER_SLOTS` returns `Err` through `apply_command` (i.e. without relying on the `debug_assert!`), and that `SlotRange::is_empty()` reports emptiness truthfully.
- [ ] F16: a test asserts `save_committed(Some(id))` → reopen → `read_committed() == Some(id)`, and `save_committed(None)` → reopen → `None`.

## Depends on

Nothing.

This is deliberate and is `INFRASTRUCTURE.md`'s explicit conclusion: *"**11 needs nothing.**
Every level-1/2 cluster finding lands with existing dev-dependencies (`proptest`, `tempfile`,
`tokio::test`); `cluster_harness` already exposes `raft()` and `cluster_state()`
(`cluster_harness.rs:239,244`)"* — so F11(a) needs no harness change either, and model
checking (proptest over the pure, synchronous, `BTreeMap`-only `apply_command`) beats turmoil
decisively here. Two apparent dependencies that are not:

- **I2 (issue 02, `.scratch/testing-improvements-round2/issues/` — subprocess-SIGKILL crash
  primitive)** lists this area as an echo. The finding that wanted a real machine crash is F10
  (Raft `append` acks durability without fsync), now issue 73, same directory. No *residual*
  finding here needs one: F1, F4 and F16 all restart by dropping the handle and reopening the
  directory, which `tempfile` + a plain reopen already express.
- **I14 (issue 14, same directory — mockable `ClusterWriter` / propose seam)** was asked for by
  area 04 and is directed *at* this crate; nothing in this issue depends on it. Its companion
  note (typing `parse_rpc_message`'s error taxonomy) is cross-referenced from F13 as a
  do-both-at-once opportunity, not a prerequisite.

## Re-triage 2026-08-06

**Verdict: partially-fixed** — 4/11 findings discharged (F1, F4, F12 fixed; F16 superseded);
F7, F11, F13 partial.

| F | verdict | evidence |
|---|---|---|
| F1 | **fixed** | The state-machine snapshot is now persisted (`ClusterSnapshotStore`, `KEY_SNAPSHOT_META`/`KEY_SNAPSHOT_DATA` at `cluster/src/storage.rs:28,33`) — the epoch-fold T7 blocker is gone. FM-CLUSTER-017 forces it with `test_state_machine_snapshot_survives_restart_without_log_replay`, `test_snapshot_save_never_moves_backwards`, `test_snapshot_store_is_opt_in`. |
| F2 | still-valid | No replay-determinism test comparing two independently-derived state machines; `frogdb-cluster`'s dev-deps are only `tokio`, `tempfile`, `tracing-subscriber` — no `proptest`. |
| F4 | **fixed** | `get_log_state` (`storage.rs:436-466`, was cited at `:248-274`) now returns `last_log_id: last_log_id.or(last_purged)` with the openraft contract spelled out in-source. |
| F6 | still-valid | No frozen JSON fixture for any `ClusterCommand` variant or for `ClusterStateInner`; the only golden in the crate is `test_render_cluster_nodes_golden` (`wire.rs:337`), which is CLUSTER NODES text, not the on-disk encoding. |
| F7 | partially-fixed | The lifecycle is no longer at zero tests: `snapshot_builder_shares_the_live_state` (`state.rs:3906`), `begin_receiving_snapshot_hands_back_an_empty_buffer` (`:3941`) under FM-CLUSTER-017, and `test_install_snapshot_emits_{promotion,demotion}…`/`…_silent_when_self_role_unchanged` (`:3186/:3210/:3237`) under FM-CLUSTER-045. Unmet: a full source→`install_snapshot`→fresh-SM equality round trip, the empty-SM `build_snapshot` vs `get_current_snapshot` asymmetry, and the `network.rs` loopback InstallSnapshot payload check. |
| F8 | still-valid | `commands.rs` still has no property test; no `proptest` dev-dep in the crate. |
| F11 | **fixed** (removal half) | The removal path exists and is pinned by FM-CLUSTER-101: `spawn_remove_raft_voter` + the shared `voter_change` rule (`network.rs`), forced by five in-crate tests plus `test_cluster_forget_shrinks_the_raft_voter_set`. The retry/exhaustion *schedule* was already covered (`the_voter_retry_schedule_backs_off_and_then_stops`, `adding_a_voter_runs_for_a_stranger_and_skips_an_existing_member`); what still stands is only the *give-up arm's observability* — exhausting `MAX_ATTEMPTS` on either side logs and leaves `ClusterState` and the voter set divergent with no metric and no reconciliation. |
| F12 | **fixed** | FM-CLUSTER-017 names `membership_entries_are_recorded_for_applied_state`; the `EntryPayload::Membership` apply arm is forced. |
| F13 | partially-fixed | Only the disconnect/protocol-error classification is pinned (`every_disconnect_phrase_ends_the_connection_quietly`, `cluster-runtime/src/bus.rs:492`). Still untested: garbage / truncated / over-cap frames through `parse_rpc_message` (`network.rs:806`), and `new_client`'s silent `"127.0.0.1:16379"` fallback on an unparsable address (`network.rs:326-332`). `test_all_rpc_variants_roundtrip` (`:891`) still compares discriminants with `matches!`, not payloads. |
| F15 | still-valid | `SlotRange::new` (`types.rs:176-182`) is still `debug_assert!`-only and `is_empty()` (`:200`) still hard-codes `false`. |
| F16 | **superseded** | Decided the other way and documented in-source: `storage.rs:493-501` now states `read_committed` is deliberately left at openraft's `None` because `append` uses non-fsync write options, so reading the key back could hand `get_initial_state` a commit index naming lost entries. The write half is forced by `save_committed_writes_then_deletes_the_persisted_key` (FM-CLUSTER-017). The proposed read-back assertion can never hold; the T7 dependency is discharged. |

Phase 4 locked `frogdb-cluster` (99.6%) and `frogdb-cluster-runtime` (99.0%) against 78
FM-CLUSTER rows, but the lock is not blanket cover: the encoding-compat (F6) and property-test
(F2, F8) findings have no row, and `frogdb-cluster` still carries no `proptest` dev-dependency.
Stale refs corrected: `get_log_state` `storage.rs:248-274` → `:436-466`; the `read_committed`
discussion `storage.rs:298-308` → `:493-501`; `spawn_add_raft_voter` `network.rs:653-660` →
`:~700-720`; `integration_cluster.rs` split into `server/tests/cluster_*.rs` (e.g.
`test_cluster_meet_adds_node_to_raft_voters` → `cluster_topology.rs:2234`). No live production
bug newly confirmed: F11's missing voter-removal path and F15's release-build range guard are
both real source gaps, but both were already stated as such when filed.
