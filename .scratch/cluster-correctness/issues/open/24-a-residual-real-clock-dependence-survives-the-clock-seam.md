# 24 — A residual real-clock dependence survives the clock seam (seed 9)

Status: ready-for-agent

## Parent

[PRD](../../PRD.md) §3 W4 — residue of [issue 23](../done/), which closed the dominant leak
(`Instant::elapsed()` bypassing `frogdb_types::clock`).

## What is wrong

Issue 23 removed 46 OS-clock reads from the server crates and taught `just lint-clock-seam`
to reject `.elapsed()`. With that fix in place, 13 of the 14 scheduler seeds 1–14 produce
byte-identical fingerprints even when the replay is slowed to ~8x its normal real-time cost.
**Seed 9 still diverges**: `op[21] migrate slot=8623` records `commit-pending` in the fast
run and `committed` in the slow one (fingerprint index 56).

So a second, smaller real-clock dependence remains somewhere on the turmoil path.

## What is already known

The reproduction is deterministic — no ambient load needed. Add a real sleep after each
`sim.step()` in `step_with_faults` (the shipped `run_seed_stretched` hook does exactly this)
and compare an unstretched run against a stretched one:

| perturbation | result |
| ------------ | ------ |
| 2000us after every step, replay only | diverges |
| 2000us after every step, **both** runs | agrees |
| 4s of real sleep *between* the two runs | agrees |
| 2000us restricted to steps `[606, 747)` | diverges |
| 2000us restricted to steps `[653, 747)` or `[0, 700)` | agrees |

Read together these say: the outcome is a **deterministic function of real time consumed
during the run** (symmetric stretch cancels), it is **not** the process-global `SystemTime`
epoch in `frogdb_types::clock` (an inter-run gap cancels), and the sensitive region is a
narrow band of simulated time — roughly 606ms–747ms in, during cluster bring-up, well before
the op that ends up differing. The effect is threshold-like: adding enough unrelated real
overhead (e.g. `RUST_LOG=debug` on both runs) pushes it back under the threshold.

The first *structural* divergence in a debug-log diff is an ordering flip **within one host's
runtime** (`cluster-n3`): its "Client disconnected / Connection handler finished" pair runs
before the RaftCore replication heartbeats in one run and after them in the other. Everything
before that point, including every `session_duration_ms`, is byte-identical.

Ruled out so far:

- `Instant::elapsed()` on the server crates — that was issue 23, and the gate now rejects it
- the `SYSTEM_EPOCH` in `frogdb-server/crates/types/src/clock.rs` (inter-run gap cancels)
- `ClusterConfigSection::effective_node_id()`'s `SystemTime::now()` — the sim's node ids come
  from `hash_addr_to_node_id`, and they are identical across runs
- openraft's internal timing — it takes `Instant` from `AsyncRuntime`, i.e. `tokio::time`
- turmoil's message scheduling — `Topology::tick_by` advances by a fixed duration
- `cluster/src/stats.rs`'s `std::thread::spawn` — test-only

Still open, in rough order of suspicion:

- **tokio's paused clock inhibits auto-advance while a blocking task is in flight**, so any
  `spawn_blocking` on a simulated host makes virtual time wait on a real thread. None was
  found on the cluster path, but the search was by grep over `frogdb-server/crates/*/src`,
  not exhaustive over dependencies (`tokio::net::lookup_host` and `tokio::fs` both use the
  blocking pool).
- **RocksDB background threads.** The Raft log store is real RocksDB
  (`frogdb_cluster::storage`, `<data_dir>/raft`, `write_opts.set_sync(true)`), and its
  flush/compaction threads run on OS time entirely outside the simulation.
- `tokio::time::Instant::now()` evaluated *outside* a runtime context: under
  `cfg_test_util` that silently falls back to the OS clock. `turmoil::HostTimer::elapsed`
  (`host.rs:184`) is exactly this shape — `self.now.elapsed()` on a `tokio::time::Instant`
  read from wherever the caller happens to be — but nothing in FrogDB calls
  `turmoil::elapsed()`/`sim_elapsed()`, so it looks unreachable here.

## Why it matters

Same argument as issue 23, one order of magnitude smaller: a seed whose replay is not
bit-exact cannot be debugged from its fingerprint, and a muzzled `EXPECTED-FAILURE` seed
could flap. The shipped gate (`test_cluster_scheduler_same_seed_same_run`, seed 1, 500us of
stretch per step) does not catch this one — seed 1 is immune even at 2000us.

## What to build

Root-cause the residual and close it at a seam. Then widen the determinism gate to the seed
that exercises it, so the class stays closed.

## Acceptance criteria

- [ ] Root cause identified and named, with the leaking site at file:line
- [ ] Seed 9 agrees under a 2000us-per-step replay stretch
- [ ] The determinism gate covers a seed that would have caught it
- [ ] `just mutants-diff` triaged on any touched locked crate

## Blocked by

None.
