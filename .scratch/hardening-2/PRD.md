# Campaign 2 — detection-first hardening

Status: draft (awaiting approval)
Author: 2026-08-07
Predecessor: [foundation-hardening campaign](../hardening/README.md), retrospective
[`retrospective-2026-08-05.md`](../hardening/retrospective-2026-08-05.md)

## 1. Why a second campaign

Campaign 1 locked four areas — transactions/VLL, persistence/recovery, replication, cluster —
by extracting each into its own crate, writing 246 failure-mode rows with lint-enforced forcing
tests, and holding mutation gates of 0.80–0.90. It found and fixed real acked-write-loss bugs
that 5000+ passing tests had certified as fine.

Then the round-2 backlog re-triage
([`re-triage-2026-08-06.md`](../testing-improvements-round2/re-triage-2026-08-06.md)) re-baselined
82 issues against the post-campaign tree and confirmed **18 live production defects**, two of them
*inside* the locked perimeter:

- **Issue 73 — Raft `append` acks durability with a non-`sync` write**
  (`cluster/src/storage.rs:538-542`). A committed slot transfer, failover or epoch bump that a
  power cut erases. Inside locked `frogdb-cluster`, at 99.6% mutation score. Phase 4 *saw* the
  hole and wrote a comment next to it ("Revisit if `append` ever fsyncs") instead of a row.
- **Issue 50 — `CoreMsg::ExecTransaction` never calls `can_execute_during_lock`**
  (`core/src/shard/dispatch_core.rs:95-115`). `Execute` and `ScatterRequest` both gate; EXEC does
  not, so a concurrent EXEC mutates shards a cross-shard script believes it holds exclusively.

Neither was findable by the machinery campaign 1 built. That is the finding this campaign is
built on, and it decomposes into four structural blind spots.

### B1 — Mutation testing cannot see omissions

`cargo mutants` mutates code that exists. There is no mutant for a *missing* `sync_all`, a
*missing* gate call, a *missing* invalidation. Every one of these is an omission:

| defect | the missing thing |
|---|---|
| 73 | `sync` on the raft log append path |
| 50 | `can_execute_during_lock` on the EXEC arm |
| 54 | `invalidate_keys` for BCAST-mode trackers on lazy expiry |
| 22 (residue) | expiry check on the replica read arm and on `FT.SEARCH` |
| 72 | format magic/version on disk (the reserved flags byte is written `0`, read into `let _`) |
| 24 | type check before the pop in `BLMOVE`'s immediate path |

A mutation score is a floor on *the code you wrote*. It says nothing about the branch you never
wrote. **Detection mechanism: chokepoint lints (W1)** — if there is exactly one way to do a
dangerous thing, "didn't do it" becomes mechanically visible.

### B2 — The gate perimeter is much smaller than the risk surface

55 of the 82 re-triaged issues live outside the four locked areas: ACL, scripting, protocol,
search, config, the observability HTTP surface, the exotic command families. The five security
defects (37 Lua sandbox escape, 38 pre-auth CRLF frame injection, 39 `HELLO AUTH` password in
MONITOR, 40 default-open admin bearer gate, 35 `-@admin` inert for half the surface) are all
there, as is the unauthenticated-reachable shard panic (63).

And a lock is not blanket cover even inside its own area: `frogdb-vll` scores 100% against a spec
with **4 rows**, all on the continuation lock, with the scatter phases explicitly unrowed. The
score measures the rows you wrote. **Detection mechanism: a security area with its own spec and
gate (W4), plus perimeter extension over `frogdb-core` dispatch (W5).**

### B3 — Spec witnesses can assert nothing

`just lint-failure-modes` enforces that a row names a test and that the test carries the row's
tag. It cannot enforce that the test *asserts* anything about the row. The re-triage found three
locked-spec witnesses that do not:

- `crash_recovery_tests.rs:701`/`:735` (`// FM-PERSISTENCE-017`) — `assert!(result.is_ok() || result.is_err())`
- `cluster/src/storage.rs:656` (`// FM-CLUSTER-017`) — never reads `committed` back
- `cluster/src/network.rs:891` (`// FM-CLUSTER-051`) — asserts only an enum discriminant

Two spec cells are also factually wrong on today's code (`FM-PERSISTENCE-033`'s Invariant,
`FM-PERSISTENCE-019`'s Observable). **Detection mechanism: witness-quality lints and a spec
re-validation pass (W3).**

### B4 — The worst failure modes are not forceable in-process

There is no crash primitive: zero `Command::new` in `test-harness/src`, and `ClusterNode::kill()`
is a graceful `shutdown_mut()`. Every durability row is therefore witnessed by an in-process
approximation of a power cut. Issue 73 is exactly the bug that a real `SIGKILL`-then-recover
harness finds and an in-process one cannot. **Detection mechanism: a crash-durability harness
(W2).**

## 2. Thesis

Campaign 1 asked *"is the code that exists correct?"* Campaign 2 asks two different questions:

1. **Is the code that should exist present?** — chokepoints, gate coverage, format versioning.
2. **Is the evidence real?** — witnesses that assert, rows that are falsifiable, faults that are
   actually injected.

Work is only in scope if it changes what the machine can detect, or if it is a live defect the
new machinery is being built to catch. Fixing the 18 defects one at a time is the *outcome*, not
the plan.

## 3. Workstreams

### W1 — Chokepoint lints

*Answers: how would a chokepoint lint work?*

A chokepoint lint states an invariant of the form **"every X must go through Y"**, where Y is the
one implementation that gets it right, and fails the build on any X that does not. The repo
already has two working precedents, and the mechanics are copied from them rather than invented:

- `scripts/clock-seam.py` — every time read goes through `frogdb_types::clock::now()` /
  `clock::system_now()`, never `Instant::now()` / `SystemTime::now()`. Runs as `just
  lint-clock-seam`.
- `scripts/failure-modes.py` — two-way spec↔test tag agreement. Runs as `just
  lint-failure-modes`, part of `just lint`.

Anatomy of a rule:

1. **Invariant**, one sentence, stated so a violation is a defect and not a style opinion.
2. **The chokepoint** — the single function/type that satisfies it. If there isn't one, the rule
   starts by *creating* one; a lint without a chokepoint is a nag.
3. **A mechanical predicate** over `rg`/AST output: the shape of a violation.
4. **An escape hatch with a justification** — a trailing marker comment (the shape
   `clock-seam.py` already uses) so a legitimate exception is visible at the code, never in a
   blanket ignore file.
5. **A ratchet** — a checked-in baseline of known violations so a rule can land before the
   cleanup does. New violations fail; existing ones burn down. Fixing the last one deletes the
   baseline.
6. **Wiring** — `scripts/<rule>.py`, a `just lint-<rule>` recipe, membership in `just lint`, and
   the lefthook pre-push job.

The candidate rules, the current violation counts, and the KEEP/DROP verdicts are in
[§3.1 rules](#31-candidate-rules).

#### 3.1 Candidate rules

*(filled from the chokepoint survey — see `surveys/chokepoint-lints.md`)*

### W2 — Crash-durability harness

Deliverable: a real out-of-process crash primitive plus the durability assertions that only it can
make.

- Subprocess node launcher (`Command::new`) with `SIGKILL` at a chosen point, and a recover-and-
  compare step; replaces `ClusterNode::kill()`'s graceful shutdown for the tests that mean a power
  cut (round-2 infra issue 02).
- Fault-injecting filesystem layer for the durability primitive: fail/short-write/reorder at the
  seam, so "the ack happened before the fsync" is observable rather than argued.
- Applied first to the raft log (issue 73), the WAL/checkpoint install path, and the replication
  offset advance.
- Every durability FM row that today has an in-process witness gets re-witnessed here or is
  re-worded to what it actually claims.

### W3 — Witness quality and spec truth

- Lints for the assert-nothing class (§3.3, filled from the witness audit).
- A re-validation pass over all 246 rows: every row must be falsifiable (a row no test could fail
  is a row that means nothing), and every Invariant/Observable cell must match today's code — two
  are already known wrong.
- The three known assert-nothing witnesses get real assertions or the rows get re-witnessed.

#### 3.3 Witness lints

*(filled from the witness audit — see `surveys/witness-audit.md`)*

### W4 — Security area: spec + gate

Treat security as a fifth area and run campaign 1's sequence on it: spec → close → mutate → lock.

- Scope: authentication and ACL enforcement, the Lua sandbox boundary, protocol-level injection
  and resource limits, the admin HTTP surface, and redaction (MONITOR/logs).
- Live defects that seed the spec: 35, 37, 38, 39, 40, 63, 68, 70 (four unbounded allocation
  sites), 95 (unbounded RESP nesting).
- Two of these are pre-auth reachable (38 CRLF injection, 63 shard panic) and rank above every
  other item in this plan.

### W5 — Perimeter extension

- Bring `frogdb-core`'s shard dispatch inside a gate: it is where issue 50 lives, it is the
  funnel every command passes through, and it is currently ungated.
- Extraction decisions for durability isolation: see §4.

### W6 — Truth of configuration and on-disk state

- Config round-trip: every declared param must load from every documented source (issue 49 —
  a discovered `./frogdb.toml` is silently ignored), and the golden file must prove effect, not
  just presence (issue 21 — 122 golden rows, metadata-only).
- On-disk format version and magic (issue 72), so a downgrade fails loudly.
- Coverage-pipeline decision D3 (issues 27/28/31): the nightly publishes a meaningless 84.0%
  and the audit tooling built on it manufactured false "untested" backlog items. Either fix it or
  turn it off; leaving it is the one option the evidence rules out.

### W7 — Fuzz widening

- Registry-driven argument fuzzing (issue 11): today's `fuzz_targets/cmd_dispatch.rs` is a
  hand-written 37-command list against a 377-command registry.
- Structure-aware RESP fuzzing for the protocol defects (38, 95).

## 4. Additional crate extraction

*(filled from the extraction survey — see `surveys/durability-extraction.md`)*

## 5. Defect waves

The 18 confirmed live defects, ordered by blast radius. Waves run *after* the mechanism that
would have caught each class exists, so that each fix lands with a detector behind it.

| wave | defects | gated on |
|---|---|---|
| 1 — reachable pre-auth | 38 (CRLF frame injection), 63 (shard panic) | nothing; ship first |
| 2 — consensus + isolation | 73 (raft fsync), 53 (stale log reader), 50 (EXEC gate) | W2 harness for 73; W1 gate lint for 50 |
| 3 — silent data loss | 45 (`FT.ALTER` wipes JSON index), 44 (`TS.CREATERULE` not persisted), 42 (RocksDB iteration error → `None`), 24 (`BLMOVE` WRONGTYPE), 43 (`ES.SNAPSHOT` non-UTF-8) | W1 error-swallow rule |
| 4 — auth and boundaries | 37 (Lua sandbox escape), 35 (`-@admin` inert), 39 (MONITOR leak), 40 (default-open admin gate), 68 (ACL ratelimit lockout) | W4 spec |
| 5 — silent misconfiguration | 49 (`frogdb.toml` ignored), 72 (no format version), 54 (BCAST invalidation) | W6 |

## 6. Gates, metrics, and exit criteria

The campaign exits when all of the following hold:

1. Every W1 rule that reached KEEP is wired into `just lint` with its baseline at zero, or with a
   burn-down plan recorded in this directory.
2. The crash harness exists and at least the raft log, the WAL/checkpoint install, and the
   replication offset advance are witnessed by it.
3. Zero assert-nothing witnesses under the W3 lints; zero rows that no test could fail; the two
   wrong spec cells corrected.
4. The security spec is LOCKED with a mutation gate on record, on the same terms as the other
   four areas.
5. All 18 live defects fixed, each with a failure-mode row and a forcing test.
6. `frogdb-core` dispatch is inside a gate, or a written decision says why not.

## 7. Out of scope

- `frogdb-operator/**`, `frogctl/**` — unchanged from campaign 1.
- `website/**` — in scope only for documenting behavior changes this campaign makes (approved
  2026-08-06).
- Performance work, new features, and the exotic command families beyond the defects listed here.
- The redis-regression suite stays unfrozen and is treated as a normal test suite; no refreeze.

## 8. Open decisions

*(filled once the surveys land)*
