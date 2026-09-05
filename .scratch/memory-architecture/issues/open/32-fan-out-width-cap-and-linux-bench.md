# 32: fan-out width cap and the Linux bench for the zero-copy parse path

Status: ready-for-human
Type: AFK
Origin: split from [issue 19](../done/19-zero-copy-parse-path.md) at campaign close, 2026-09-05 — the two items that need the Linux rig and a human ruling
Area: frogdb-vll + frogdb-txn (dispatch path) + bench rig
Phase: 6 — polish. **Locked-area work: spec-first discipline applies (vll/txn gate 0.90).**

## Why

Issue 19 landed the zero-copy parse path (`FM-MEMORY-003` in `specs/memory.md`, 17
forcing tests) but two of its acceptance items could not be done in a local (macOS)
session: one needs a ruling on a locked dispatch path, the other needs the issue-04 Linux
rig. They are carried here verbatim so issue 19 can close on what shipped.

**Fan-out constraints from the Linux validation** ([spike-report-linux.md](../../spike-report-linux.md)):
cross-core costs are real and measured — ~6 µs of CPU per distinct foreign core touched, and
cross-thread p99.9 at 512 clients measured 11× the colocated figure (park/unpark stall on the
foreign core's queue). A command touching all N cores serializes ~6N µs of CPU — wide MGETs
should degrade to bounded-width waves. Hop batching (K shards → K messages) is already
forced by `a_scatter_over_k_shards_sends_exactly_k_lock_messages`; the width cap is not.

## What to build

- [ ] **Fan-out width cap** (~4 concurrent foreign-core waves, config-capped, with the
      spike's ~6 µs/core and 11× p99.9 provenance in the doc comment). Touches the LOCKED
      vll/txn dispatch path (gate 0.90) and needs an atomicity ruling: bounded-width waves
      change when a wide command's locks are requested, which interacts with wound-retry
      fairness. Spec-first work.
- [ ] Bench vs [issue 04](../done/04-linux-validation-bench.md) baselines: large-value
      SET/GET throughput improves or holds; cross-thread p99.9 at 512 clients does not
      regress. Needs the issue-04 Linux rig; issue 19's session ran in local (macOS) mode.

## Acceptance criteria

- The atomicity ruling is recorded (ADR or a `specs/txn.md` / `specs/vll.md` row) before
  any width-cap code lands; the cap ships with its forcing test and the mutation gate holds.
- Bench numbers recorded against the issue-04 baselines on the Linux rig, with the run's
  provenance (box, commit, command) in this issue's Resolution.

## Files likely touched

`frogdb-server/crates/vll/`, `frogdb-server/crates/txn/`, `specs/txn.md`, the dispatch
config surface, `.scratch/memory-architecture/spike-report-linux.md` (bench appendix).

## Out of scope

Everything issue 19 already landed (refcounted arg slices, escape-point copies, hop
batching, store boundary by convention); output-path zero-copy; io_uring; queueing
discipline changes beyond the width cap.

## Depends on

Nothing. Human decision on the atomicity ruling and access to the Linux rig.
