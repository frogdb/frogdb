# Scatter/gather merges discard per-shard errors — partial failure replies as success

Status: needs-triage
Type: AFK
Origin: round-2 testing audit 2026-07-28 — 15 parallel area audits, `.scratch/testing-improvements-round2/`
Source: proposals/03 F1 · MASTER.md §3 (consistency violations), §2 T5, §7 (decision required)
Score: severity 5 · likelihood 3 · effort 2 · priority 19
Area: frogdb-server / connection scatter-gather

## Context

The multi-key scatter strategies merge per-shard replies without inspecting them for errors.
`MSET` returns `+OK` when a shard's write errored; `DEL`/`EXISTS`/`TOUCH`/`UNLINK` return an
undercount the client reads as "those keys did not exist"; `MGET` returns `nil` for a failed
shard, indistinguishable from an absent key. A client cannot distinguish partial application
from success — silent data loss from the caller's point of view. It needs one shard to error or
drop while others succeed: OOM on one shard, a WRONGTYPE on one key, shard shutdown during
drain, or a `PartialResult` that carries an `Error`.

**This is a suspected live defect found by reading, not by test failure — the proposed test
fails against today's code.** The file:line evidence below is the auditing agent's and needs
confirmation before or during the fix; this issue is not among the two the coordinator
verified directly. MASTER.md §7 lists the scatter partial-failure contract as a decision that
must be settled before the test can assert anything, hence `needs-triage`.

## Evidence

`scatter/strategies.rs:153` `merge_sum_integers` —
`.filter_map(|r| if let Response::Integer(n) = r { Some(*n) } else { None }).sum()` silently drops
every non-integer (i.e. every error) reply. `scatter/strategies.rs:68` MGET —
`.and_then(...).cloned().unwrap_or(Response::null())`. `scatter/strategies.rs:132-138`
`MSetStrategy::merge` ignores `shard_results` entirely and returns `Response::ok()`.
`scatter/executor.rs:139` only maps a *whole-scatter* `ScatterError`; a per-key error inside a
successful shard's `PartialResult` flows straight into `merge`. `UnlinkStrategy::merge`
(`strategies.rs:291`) is `untested`; `MSetStrategy` is one of the two `hot-but-shallow` functions
in the whole workspace (exec 9232, 3 tests).

Why the existing tests pass anyway: every merge test feeds only successful shards — the only
coverage comes from full server integration runs that never inject a failing shard.
MASTER.md §1 records `merge_sum_integers` as 2 tests, 10/10 regions, both all-success.

## Options

Reproduced verbatim from proposals/03 F1 — the *contract* is the real decision, and the test
must pin whichever is chosen:

(a) **Fail loudly** — any shard error propagates as the reply (Redis's multi-key commands are
single-node and therefore atomic, so "error means nothing happened" is the closest analogue);
(b) **Partial with a distinguishable reply** — keep the sum but return `-ERR partial` when any
shard errored; (c) **Status quo, documented** — best-effort merge, documented as a divergence.
Recommendation: (a) for `MSET`, (b) for the counting commands, and the unit tests above pin it.

## Acceptance criteria

- [ ] A table test per strategy feeds shard 0 = `{k1: Integer(1)}`, shard 1 =
      `{k2: Error("OOM ...")}` and asserts `DEL` does **not** return `Integer(1)` but surfaces
      the chosen contract. Fails today.
- [ ] The same table covers `MSET` (must not be `+OK`), `MGET` (a failed shard must not be
      encoded as `nil`), `EXISTS`, `TOUCH` and `UNLINK`.
- [ ] `UnlinkStrategy::merge` (`strategies.rs:291`) gains at least one direct test.
- [ ] The chosen contract per command family is asserted explicitly and, if (c), recorded as a
      documented divergence.

## Test boundary

Level 1 — the merges are pure functions over `HashMap<usize, HashMap<Bytes, Response>>`; a
socket adds nothing. This is the anti-pattern in reverse: today the only coverage comes from
full server integration runs that never inject a failing shard.

## Depends on

nothing
