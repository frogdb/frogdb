# Work-item rulings — 2026-08-22 grill session

Rulings from the interactive triage of every open follow-up produced by the spec-gaps /
issue-31 Quint-rework session. Each ruling below is settled and binding; issue files cite
this ledger. Standing global rulings (no wall-clock, demote-don't-remove,
drops-stop-signaling, source-authoritative-until-commit, fail-closed over availability)
continue to bind.

## R1 — Issue-31 campaign staging: clustered waves

The spec-amendment + implementation campaign for issue 31 (design doc
`2026-08-14-issue31-migration-design.md`, `## Spec / impl blast radius — full verdicts`)
runs as **conflict-clustered sequential waves** on one long-lived branch, one implementer
per cluster, merging to main at wave boundaries — the spec-gaps P1..S1 pattern. Each
LOCKED row lands atomically with its forcing test and implementation (lint-spec
discipline). A wave-0 decomposition agent drafts the issue set + sequencing plan for
human review before any implementation wave starts.

## R2 — Design-owner flag process: hybrid

Of the 12 Q2-Q4 design-owner flags: the three semantics flags are ruled individually
(R3-R5 below); the five wrong-attribution flags (F6/M65, F7/M59, F13/M21, M38/M39
attribution, plus the M34 `admitted ≡ identityWritten` note) become one batched doc-fix
issue; the four modelling-gap battery misses (M06 compound-only, M22 temporal-operator,
M32 no-doc-observable, M37 dataset-discard) are **accepted as documented limitations**
unless campaign work makes closing one incidental-cheap.

## R3 — `inv_no_hold_during_staged_flip`: adoption-time rule

Coexistence of a latched drain-hold region and a *staged* (pending, unadopted) whole-node
role flip is **lawful**. The safety boundary is the applied write, not the staging:
CockroachDB serializes conflicting range reconfigurations at the committed descriptor
write, FoundationDB has movement yield to topology change at the planner — neither blocks
at intent time. The design already makes identity/role reports cancel sourced migrations
(V17-M1 arms). Replace the dropped staging-time invariant with the **adoption-time
invariant**: an applied role flip on a node leaves no sourced open migration and no held
slot on that node — cancellation and hold-release are atomic with the adoption's applied
write. Model work: assert it over `adoptReplicatedRole` (add the conjunct if missing);
design doc gains the clarifying sentence at the staged-flip fence row during the campaign.

## R4 — `isRefusalTerminal` arm 4b: carry the class in the payload

Arm 4b is **not dead in the design** — the V27-M2 negative-control trace reaches it (a
delayed `ordering`-class refusal delivered while the re-created cell's stored identity is
absent). The model diverged by recomputing the class at delivery (`identityOrderOk(None,·)`
vacuously true). Fix the model: the refusal **class is minted with the verdict and carried
in the refusal payload**; delivery splits arm 4a/4b on the current stored operand only.
Add the V27-M2 fixture as a model run test (counter loss → FORGET + re-MEET → delayed
ordering refusal → arm 4b terminal clearing) pinning the arm's reachability.

## R5 — `inv_no_record_outlives_its_registration`: stale-never-admits

The outliving state is lawful (crash-durable node-local record, level-triggered
clearing). Replace the unstatable invariant with **stale-never-admits**: no applied
adoption/stamp ever fires from a record whose `staged_registration_seq` mismatches the
live cell — a reachable-violation invariant with kill-power under guard-deleting
mutations — plus run tests pinning each cell-recreation path (FORGET + re-MEET,
other-member reset, HARD reset across reboot) followed by `clearStaleRecord`.
Eventual-clearing liveness stays with the Rust forcing tests
(`staged_record_from_a_dead_registration_is_cleared_at_boot` et al.).

## R6 — `--force-fresh-data-dir` beside foreign entries: refuse (candidate rule a)

V25-m6 ruled: the flag becomes a **fresh-start tool, not an override** — it refuses when
the data directory holds unexcused entries, and the error names them. Operator moves the
foreign bytes out and retries. Fail-closed; matches CockroachDB's refusal to init into a
directory that is not its own. Breaking change to a shipped flag accepted (pre-release).
Candidate rule (b) (second confirmation flag) rejected.

## R7 — SG-28 re-ruled: process fail-stop, not shard-restart

Issue 07 item 4's "restart the shard from its WAL" is **re-ruled**: a panic caught in a
VLL *write* path escalates to clean process exit; existing startup recovery replays the
WALs. Rationale: CockroachDB, Scylla, Redis, and FoundationDB all treat a mid-write panic
as process-fatal — none hot-restarts a shard; a bespoke single-shard hot-restart is a
large new correctness surface (in-flight cross-shard scatter parts, sibling-held locks,
client visibility during replay) bought for a rare event that replication/failover
already covers. Read paths keep isolate-and-continue. FM-VLL-005's NOT observable gains
"partial writes surviving a panic", forced by a panic-mid-write test.

## R8 — SG-29: standalone-only cross-shard MULTI/EXEC design task

Cross-shard `MULTI`/`EXEC` refusing `-CROSSSLOT` in **standalone** is a real
Redis-compat gap (standalone Redis has one keyspace; our refusal is a
deviation-that-is-not-an-improvement). Design cross-shard EXEC for standalone only:
VLL's continuation lock already provides isolation; Redis run-all semantics mean no
runtime rollback exists to build — the missing piece is **crash-atomicity across
per-shard WALs** (transaction markers / commit record, the AOF-wrapping equivalent).
DragonflyDB proves the VLL shape. Cluster mode keeps `CROSSSLOT` (Redis-cluster parity +
migration safety). Two-stage: design doc with HITL review, then impl.

## R9 — SG-30 accepted as written

Data-present blocking pop under node-global `CLIENT PAUSE WRITE` parks as a normal
waiter with a **live deadline** (deadline runs through the pause; nil-timeout during a
long pause is the documented, coherent outcome of issue 17's ruled deviation). Pause
check moves to the shard-side pop decision point. Redis `BLOCKED_POSTPONE` semantics
(suspended deadline) rejected — one deadline regime, not two.

## R10 — TR-TXN-028 window: file + fix via generation-carry

The dead-watch off-target round-trip's non-atomicity with the EXEC commit (narrow WATCH
false-negative window, stated in the row) gets closed with the issue-09 generation-carry
treatment. Filed as spec-gaps issue 31.

## R11 — Q6 promotion-retry boundedness: folded into campaign brief

No standalone issue. Recorded as a candidate doc extension in the issue-31 decomposition
brief: if the campaign's failover/residue wave touches promotion-retry, the design owner
adds the bound extension there and the model row follows.

## Filed / updated issue files (2026-08-22)

| Ruling | Issue file | Status |
| --- | --- | --- |
| R3+R4+R5 | `cluster-correctness/issues/open/43-quint-model-semantics-fixes-from-rulings.md` | ready-for-agent, M |
| R2 (batch) | `cluster-correctness/issues/open/44-design-doc-attribution-corrections-batch.md` | ready-for-agent, S |
| R6 | `spec-gaps/issues/open/32-force-fresh-data-dir-refuses-foreign-entries.md` | ready-for-agent, S |
| R7 | `spec-gaps/issues/open/28-vll-panic-restarts-the-shard-from-wal.md` (rewritten to fail-stop) | ready-for-agent, S |
| R8 | `spec-gaps/issues/open/29-cross-shard-transaction-per-shard-undo.md` (rewritten to standalone design task) | ready-for-agent (design phase), L |
| R9 | `spec-gaps/issues/open/30-blocking-immediate-pop-escapes-node-global-write-pause.md` | ready-for-agent, S |
| R10 | `spec-gaps/issues/open/31-dead-watch-generation-carry-into-exec-commit.md` | ready-for-agent, M |
| R1+R11 | issue 31 banner updated; wave-0 decomposition dispatched to `campaign-31-decomposition/` | — |

Note: R4 **supersedes** the 2026-08-19 "arm 4b deleted" ruling from the Quint-completeness
campaign ledger — arm 4b is reachable (V27-M2 trace) once the refusal class is carried in
the payload.
