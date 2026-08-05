# Measurement — slot-migration finalization residual window

Rework issue 02, sequencing step 1 ("measure first"). Companion to
[migration-pause-barrier-brief-2026-08-04.md](migration-pause-barrier-brief-2026-08-04.md) §7 and
[issues/open/02-migration-finalization-pause-barrier.md](issues/open/02-migration-finalization-pause-barrier.md).

Harness: `frogdb-server/crates/server/tests/cluster_finalization_window.rs` (test-only, every case
`#[ignore]`d). No production code was instrumented or changed.

## Result in one paragraph

The residual window is **real, reliably reproducible, and sub-millisecond in the typical case**:
p50 ≈ 0.22–0.25 ms, p99 ≈ 0.54–0.58 ms, max ≈ 0.90 ms on an idle 3-node cluster; under load it
degrades to p50 0.68 ms, **p99 1.93 ms, max 2.03 ms**. It is not a theoretical window: with a client
writing to the slot across the handover, the source acknowledged at least one write *after* the
cluster had committed the handoff in **68/120** idle iterations and **118/120** loaded iterations.
A leader-source control run puts the state-machine window at ≈ 0 (p99 35 µs), confirming the window
is follower apply lag rather than measurement noise.

**Recommendation: build the Option A barrier.** Rationale in [§4](#4-analysis-and-recommendation) —
in short, the availability the barrier spends is the *same* ~1–2 ms it removes, so the trade the
issue was worried about does not exist at this magnitude, while the failure it prevents is silent
acknowledged-write loss.

## 1. What is being measured

`CLUSTER SETSLOT <slot> NODE <target>` finalizes a migration by proposing
`ClusterCommand::CompleteSlotMigration`. The entry commits on the Raft leader and every other node
applies it once an `AppendEntries` carries the new commit index to it (openraft pushes this eagerly
on commit rather than waiting for the next heartbeat tick — see §3). Between those two instants the **source** node's published `ClusterSnapshot` still names itself the slot's owner, so
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
  three nodes to have applied the open → arm the three watcher threads → start the 8 write probers →
  `SETSLOT <slot> NODE <target>` → collect. Each iteration uses a **fresh slot** owned by the
  source, so no state carries between iterations.
- The key is seeded *before* the migration opens because a key that is absent on a `MIGRATING`
  source answers `-ASK` (`connection/guards.rs:821-853`); only a resident key exercises the
  "validates and serves" arm the window is about.
- Each prober's terminator is the handover itself: it writes until the source answers `-MOVED`,
  which is exactly the moment the source applied the entry. Per iteration the 8 results are folded
  by taking the *latest* `+OK` from any connection.

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

Actual run: 2026-08-05, 120 iterations per scenario, **zero iterations discarded** in any scenario
(no setup errors, no watcher timeouts).

## 3. Results

### Residual window — `t_source − t_leader`, the number issue 02 asks for

| Scenario | n | p50 (µs) | p90 (µs) | p99 (µs) | max (µs) |
|---|---|---|---|---|---|
| A — follower-source, idle, hb 100 ms | 120 | 219.4 | 446.0 | 579.6 | 895.8 |
| B — follower-source, idle, hb 250 ms (shipped) | 120 | 248.8 | 412.5 | 541.0 | 887.3 |
| **C — follower-source, loaded, hb 250 ms** | 120 | **684.2** | **1349.8** | **1927.4** | **2030.6** |
| D — leader-source (control) | 120 | −0.9 | 9.1 | 34.8 | 52.5 |

### Acked-write exposure — `t_last_ok − t_leader`, the behavioral consequence

Only iterations with a positive exposure are summarized; the count column says how often that
happened. Percentiles are nearest-rank, so where the exposed count is under 100 the p99 and max
columns select the same sample and read identically — that is an artifact of the subset size, not a
plateau in the data.

| Scenario | exposed iters | p50 (µs) | p90 (µs) | p99 (µs) | max (µs) |
|---|---|---|---|---|---|
| A — follower, idle, hb 100 ms | 68/120 | 185.7 | 442.0 | 691.3 | 691.3 |
| B — follower, idle, hb 250 ms | 67/120 | 207.9 | 374.2 | 533.0 | 533.0 |
| **C — follower, loaded** | **118/120** | **936.5** | **1540.6** | **2062.8** | **2329.8** |
| D — leader-source (control) | 45/120 | 58.4 | 122.2 | 148.2 | 148.2 |

### Secondary metrics

| Scenario | client-visible `t_source − t_ack` p50 / p99 | dual-ownership `t_source − t_target` p50 / p99 | prober mean SET RTT p50 |
|---|---|---|---|
| A | −67.9 / 392.5 | −8.6 / 349.0 | 394.0 |
| B | −57.2 / 343.3 | 13.0 / 490.1 | 386.9 |
| C | −148.6 / 1676.6 | −17.8 / 1025.8 | 851.6 |
| D | −133.9 / −64.7 | −888.1 / 25.2 | 425.8 |

All figures in microseconds. Negative client-visible values mean the source had already applied by
the time the operator's `+OK` came back — the admin RESP round trip outlasts the window about half
the time, which is why `t_leader` and not `t_ack` is the anchor.

### Cross-checks that the numbers are measuring the right thing

- **Leader-source control ≈ 0.** When the source *is* the Raft leader, the state-machine window
  collapses to p50 −0.9 µs / p99 35 µs. The watcher threads, the `parking_lot` read poll, and the
  `Instant` arithmetic therefore contribute tens of microseconds at most; scenarios A–C are
  measuring follower apply lag, not harness overhead.
- **Exposure tracks the window.** Subtracting the control's ack-path baseline (58 µs p50 / 148 µs
  p99, see below) from the follower exposure gives ≈ 150 µs p50 / 385 µs p99 for B and ≈ 878 µs
  p50 / 1915 µs p99 for C — within noise of the corresponding residual windows. Two independent
  observation paths (state polling and client acks) agree.
- **Heartbeat interval is not the driver.** A and B differ only in Raft timing (100 ms vs 250 ms
  heartbeat) and their windows are indistinguishable. This confirms the openraft behavior read out
  of `replication/mod.rs`: `Replicate::Committed` eagerly fills a heartbeat action to push the new
  commit index rather than waiting for the next tick, so the window is one localhost round trip plus
  the follower's apply scheduling — *not* a fraction of the heartbeat interval. A slower heartbeat
  does not widen the window, and lowering the heartbeat is not a mitigation.
- **Load is the driver.** Scenario C (32 writer connections on the source + an unrelated Raft entry
  every 2 ms) multiplies the p50 by 2.7× and the p99 by 3.6×. The window is dominated by how promptly the
  source's apply task gets scheduled, which is exactly what a busy node degrades.

### A second, distinct window the control run exposed

Scenario D has a state-machine window of ≈ 0 yet still recorded an acked write after commit in
45/120 iterations, at p50 58 µs / p99 148 µs. On that node commit, apply, and the ownership flip are
the same instant, so this exposure cannot be commit→apply lag. It is the **in-flight command
residual**: a write that passed the routing guard before the flip and completed after it. The
harness times the client's `+OK`, so it cannot distinguish "executed before the flip, replied after"
(harmless) from "executed after the flip" (an orphaned write); separating them needs a timestamp at
the shard-apply seam, which would require production instrumentation and was out of scope here.

The consequence for design stands regardless of which of those two it is: **a barrier that only
gates routing admission cannot drive exposure to zero.** Whatever Option A does must also cover
commands already past the guard — either by draining in-flight ops on the slot before the final
apply, or by re-checking ownership at execute/commit time (the Option C fencing token), which is
cheaper. Sizing: this residual is ~150 µs p99, roughly a third of the idle commit→apply window and a
tenth of the loaded one.

## 4. Analysis and recommendation

### What the issue asked

Issue 02 closes with: *"The pause barrier costs availability at every migration finalization; that
trade may not be worth it if the residual window is measured and found to be sub-millisecond in
practice. Measure first."* The window **is** sub-millisecond in practice — p99 0.54 ms idle — which
under a literal reading of that sentence argues for document-and-accept. It does not, and the
measurement is why.

### Why "sub-millisecond" does not settle it

1. **The window is not the cost of the barrier; it is the cost of *not* having one, and the two are
   the same size.** Option A finalizes in two phases: `PrepareSlotHandoff` commits, the source stops
   serving the slot, then `CompleteSlotMigration` commits. The extra unavailability is one more Raft
   round trip on that *single slot* — the same ~0.2–2 ms this document just measured, since it is
   the same commit path. So the barrier buys the elimination of the window at a price of roughly one
   window. A trade priced at 1:1 against silent data loss is not a close call.

2. **The exposure is not rare.** With a client actively writing the slot, the source acked a
   post-commit write in 57% of idle finalizations and **98% of loaded finalizations**. This is not a
   narrow race that needs an adversarial scheduler; it is what happens by default whenever a
   migrating slot has traffic at the moment it is finalized. Resharding a busy cluster finalizes
   thousands of slots, and the hot ones are precisely the ones with traffic.

3. **The failure mode is silent and unbounded in consequence.** The source validates the write,
   executes it into local storage for a slot it no longer owns, and returns `+OK`. Nothing
   afterwards reconciles it: the migration's key-copy phase is already finished by the time
   `SETSLOT … NODE` is issued, so the value is orphaned on the source and the client believes it was
   stored. Latency-shaped costs (a 2 ms pause) and correctness-shaped costs (an acknowledged write
   that vanishes) are not comparable quantities, and this codebase has already paid once for
   treating them as if they were — hardening issue 40 is the same shape of bug on the
   migrating-source single-key write path.

4. **The tail scales with exactly the wrong variable.** Idle p99 is 0.54 ms; loaded p99 is 1.93 ms,
   3.6× worse. The window widens with node load, and node load is what motivates resharding. The
   "sub-millisecond" reading is measured on the case that never needs a migration. A production node
   under real pressure — larger shard count, disk contention, GC-free but allocator-bound Rust under
   memory pressure — will sit further out on that curve than a laptop with 32 synthetic writers, and
   nothing in the mechanism bounds it. There is no configuration that makes it smaller: heartbeat
   interval demonstrably does not move it (A vs B).

### Cost of the alternative

Document-and-accept means writing down that FrogDB may silently drop an acknowledged write at every
slot handover, and that the probability approaches 1 for slots under write load. That statement
cannot go in user-facing docs as an accepted limitation of a database — it is the kind of sentence
that has to be a bug. If the project were to accept it anyway, the honest form is a documented
correctness caveat on `CLUSTER SETSLOT … NODE` plus a mitigation runbook ("quiesce clients on the
slot before finalizing"), which is a manual version of the barrier with worse ergonomics and no
enforcement.

### Recommendation

**Build the Option A barrier** (Raft-carried two-phase finalize, brief §5), with two amendments the
measurement forces:

- **The barrier must cover in-flight commands, not just routing admission.** The leader-source
  control shows ~150 µs p99 of post-flip acks on a node where the commit→apply window is zero. Gate
  at the execute/commit seam as well — the Option C fencing token is the cheap way to do it, and
  Options A and C are complements rather than alternatives: A closes the wide window, C closes the
  narrow one A cannot see.
- **Bound the pause explicitly.** The prepare phase must carry a deadline so a source that never
  hears the second phase (partitioned, or a leader that dies between the two commits) resumes
  serving rather than blackholing the slot forever. The measured window gives the sizing input: a
  timeout in the tens of milliseconds is three to four orders of magnitude above the observed p99
  and still imperceptible to an operator.

Suggested acceptance test for the implementation: this harness, unchanged, with the assertion
*"iterations with an acknowledged write after commit = 0"* over ≥120 loaded iterations. It currently
reports 118/120, so it is a live reproduction of the bug and becomes a regression test the moment
the barrier lands.

### If the decision goes the other way

Document-and-accept is defensible only alongside: (a) a `CLUSTER SETSLOT` doc note stating the
window and its measured magnitude, (b) a runbook step to drain or pause clients on the slot before
finalizing, and (c) keeping this harness in the tree as the record. The numbers to quote would be
p99 0.54 ms idle / 1.93 ms loaded, exposure in 57% / 98% of finalizations.

## 5. Caveats

- **macOS laptop, local execution mode.** All three nodes, the load generators, and the watcher
  threads share one machine's cores with two other agents' builds running concurrently. Localhost
  TCP has no real network latency; a production cluster's commit→apply window includes a real
  network hop, so the numbers here are a **lower bound** on the transport component and an upper
  bound on nothing.
- Scheduling noise on a contended laptop inflates the tail. The p99/max figures should be read as
  "this order of magnitude", not as calibrated SLOs.
- The prober is 8 independent connections issuing serial `SET`s. Each `SET` on a migrating slot
  costs a presence probe plus the write, measured at 387–852 µs mean round trip in this debug build,
  so the exposure resolution is that divided by 8: **~48 µs idle, ~107 µs loaded**. `t_last_ok`
  therefore *underestimates* the true exposure by up to that much, and the exposure figures are a
  lower bound. (A single serial prober was tried first and reported near-zero exposure purely by
  aliasing: its ~460 µs inter-arrival gap was the same order as the window being measured. The
  residual-window figures were unaffected — they come from state polling, not from the prober.)
- Exposure counts are conditioned on a client writing the slot continuously across the handover.
  A slot with no traffic at finalization has no exposure; the 57%/98% figures are "given traffic",
  not "of all migrations".
- Raft log storage is the real RocksDB-backed store, but with `persistence.enabled = false` for the
  data path. Data-path durability is not on the finalization path, so this does not bias the
  measurement.
- The measurement observes `ClusterState` mutation, which is where the routing seam reads. It does
  not observe the downstream `SlotMigrated` shard notification
  (`cluster-runtime/src/migration_events.rs`), which lands strictly later; blocked-client wakeups are
  therefore *not* bounded by these numbers.
