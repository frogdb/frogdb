# Measurement — slot-migration finalization residual window

Rework issue 02, sequencing step 1 ("measure first"). Companion to
[migration-pause-barrier-brief-2026-08-04.md](migration-pause-barrier-brief-2026-08-04.md) §7 and
[issues/open/02-migration-finalization-pause-barrier.md](issues/open/02-migration-finalization-pause-barrier.md).

Harness: `frogdb-server/crates/server/tests/cluster_finalization_window.rs` (test-only, every case
`#[ignore]`d). No production code was instrumented or changed.

<!-- RESULTS -->

## 1. What is being measured

`CLUSTER SETSLOT <slot> NODE <target>` finalizes a migration by proposing
`ClusterCommand::CompleteSlotMigration`. The entry commits on the Raft leader and every other node
applies it when the leader's next `AppendEntries` carries the new commit index. Between those two
instants the **source** node's published `ClusterSnapshot` still names itself the slot's owner, so
`route_with_snapshot` answers `LocalServe` / `LocalServeMigrating`
(`frogdb-server/crates/server/src/slot_migration/routing.rs:142-149`) and the node validates,
executes, and **acknowledges** a write for a slot the cluster has already handed away. That is
risk 7 of the exec-slot-revalidation PRD and the residual window this document measures.

Four instants are captured per finalization, all from `std::time::Instant` in one process:

| Symbol | Definition | How observed |
|---|---|---|
| `t_ack` | `SETSLOT … NODE` returns `+OK` to the admin client | return of the RESP round trip |
| `t_leader` | the leader's `ClusterState` names the target as owner | dedicated OS thread polling `ClusterState::get_slot_owner` |
| `t_source` | the **source's** `ClusterState` names the target as owner | same |
| `t_target` | the target's `ClusterState` names the target as owner | same |
| `t_last_ok` | last `SET` on a key of the migrating slot that the **source** answered `+OK` | prober client hammering the source across the handover |

and four derived quantities:

| Metric | Meaning |
|---|---|
| **residual window** = `t_source − t_leader` | the commit→apply-on-loser window; the number issue 02 asks for |
| client-visible = `t_source − t_ack` | the same window measured from the operator's `+OK`; negative when the RESP round trip outlasts the window |
| dual-ownership = `t_source − t_target` | how long both nodes simultaneously claim the slot |
| **acked-write exposure** = `t_last_ok − t_leader` | how long past commit the source kept acknowledging writes — the behavioral consequence, and the only metric that measures actual data at risk |

`t_leader` rather than `t_ack` is the commit anchor: openraft's `client_write` returns only after
the entry is committed **and** applied on the leader, so `t_leader ≤ t_ack` and using `t_ack` would
credit the RESP round trip to the barrier's account.

## 2. Method

Environment and shape:

- 3-node cluster via `ClusterTestHarness` (real servers, real Raft over real localhost TCP, real
  RocksDB-backed Raft log), 4 shards per node, all in one test process.
- `#[tokio::test(flavor = "multi_thread", worker_threads = 8)]` — production runs a multi-threaded
  runtime (`server/src/main.rs:116`), and the current-thread default every other cluster test uses
  would serialize all three nodes onto one thread and fabricate the answer.
- The **source is a Raft follower** in the primary scenarios. This is the common case: slot
  ownership and Raft leadership are independent, so with N nodes the source is the leader with
  probability 1/N. A leader-source control scenario is included for contrast.
- Per iteration: seed a key into the slot on the source → `SETSLOT <slot> IMPORTING` → wait for all
  three nodes to have applied the open → arm the three watcher threads → start the write prober →
  `SETSLOT <slot> NODE <target>` → collect. Each iteration uses a **fresh slot** owned by the
  source, so no state carries between iterations.
- The key is seeded *before* the migration opens because a key that is absent on a `MIGRATING`
  source answers `-ASK` (`connection/guards.rs:821-853`); only a resident key exercises the
  "validates and serves" arm the window is about.
- The prober's terminator is the handover itself: it writes until the source answers `-MOVED`,
  which is exactly the moment the source applied the entry.

Observation technique: each watcher runs on its own OS thread, spinning with `yield_now` for the
first 5 ms (where the resolution matters) and then sleeping 200 µs (where it does not). Watchers
signal readiness through an atomic before the finalize is issued, so thread-spawn latency is never
charged to the window. `ClusterState::snapshot`/`get_slot_owner` is a `parking_lot` read lock over a
pointer-cheap published value, so polling perturbs the apply path minimally.

Scenarios (120 iterations each):

| Scenario | Raft timing | Source | Load |
|---|---|---|---|
| A — follower-source, harness timing | hb 100 ms / election 300 ms | follower | idle |
| B — follower-source, shipped timing | hb 250 ms / election 1000 ms | follower | idle |
| C — follower-source, loaded | hb 250 ms / election 1000 ms | follower | 32 concurrent writer connections on the source + a task proposing unrelated Raft entries every 2 ms |
| D — leader-source (control) | hb 250 ms / election 1000 ms | leader | idle |

Scenario C is the adversarial-but-realistic case: the source's shards and its tokio workers are
busy, and the Raft log is not quiet, which is when a follower's apply loop is scheduled late.

Reproduce with:

```bash
cargo test -p frogdb-server --test cluster_finalization_window -- --ignored --nocapture --test-threads=1
```

<!-- RESULTS-TABLE -->

<!-- ANALYSIS -->

## Caveats

- **macOS laptop, local execution mode.** All three nodes, the load generators, and the watcher
  threads share one machine's cores with two other agents' builds running concurrently. Localhost
  TCP has no real network latency; a production cluster's commit→apply window includes a real
  network hop, so the numbers here are a **lower bound** on the transport component and an upper
  bound on nothing.
- Scheduling noise on a contended laptop inflates the tail. The p99/max figures should be read as
  "this order of magnitude", not as calibrated SLOs.
- The prober is a single connection issuing serial `SET`s; each `SET` on a migrating slot costs a
  presence probe plus the write itself, so the behavioral resolution is one such round trip
  (~10²µs). `t_last_ok` therefore *underestimates* the true exposure by up to one round trip: a
  client that happened to send at exactly the wrong moment could be acked slightly later than the
  last `+OK` this harness recorded.
- Raft log storage is the real RocksDB-backed store, but with `persistence.enabled = false` for the
  data path. Data-path durability is not on the finalization path, so this does not bias the
  measurement.
- The measurement observes `ClusterState` mutation, which is where the routing seam reads. It does
  not observe the downstream `SlotMigrated` shard notification
  (`cluster-runtime/src/migration_events.rs`), which lands strictly later; blocked-client wakeups are
  therefore *not* bounded by these numbers.
