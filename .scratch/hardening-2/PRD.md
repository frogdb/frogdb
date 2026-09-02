# Campaign 2 — detection-first hardening

Status: **approved 2026-08-07** — all §8 decisions ruled (see per-decision rulings)
Author: 2026-08-07
Predecessor: [foundation-hardening campaign](../hardening/README.md), retrospective
[`retrospective-2026-08-05.md`](../hardening/retrospective-2026-08-05.md)

## 1. Why a second campaign

Campaign 1 locked four areas — transactions/VLL, persistence/recovery, replication, cluster —
by extracting each into its own crate, writing 258 failure-mode rows with lint-enforced forcing
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

`just lint-spec` enforces that a row names a test and that the test carries the row's
tag. It cannot enforce that the test *asserts* anything about the row. The re-triage found three
locked-spec witnesses that do not:

- `crash_recovery_tests.rs:701`/`:735` (`// FM-PERSISTENCE-017`) — `assert!(result.is_ok() || result.is_err())`
- `cluster/src/storage.rs:656` (`// FM-CLUSTER-017`) — never reads `committed` back
- `cluster/src/network.rs:891` (`// FM-CLUSTER-051`) — asserts only an enum discriminant

Two spec cells are also factually wrong on today's code (`FM-PERSISTENCE-033`'s Invariant,
`FM-PERSISTENCE-019`'s Observable).

The campaign-2 audit measured the class rather than sampling anecdotes: over 54 rows and ~267
tests, **19% of rows and ~34% of tags** are weaker than the table implies (§3.3). Three was not
the count; three was what fell out of a re-triage that was looking for something else.
**Detection mechanism: a hand re-witness pass plus witness-quality lints (W3).**

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
- `scripts/spec-lint.py` — two-way spec↔test tag agreement. Runs as `just
  lint-spec`, part of `just lint`.

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

#### 3.0 The family already exists — and it does not run

The survey found **11 seam gates already shipped** (`lint-info-seam`, `lint-redirect-seam`,
`lint-pubsub-confirmation-seam`, `lint-failover-atomicity`, `lint-metrics-chokepoint`,
`lint-format-float`, `lint-clock-seam`, `lint-spec`, `lint-no-typed-unwrap`,
`lint-keyspace-notify-routing`, `lint-script-gate`). Campaign 2 extends a family, it does not
found one.

It also found why that family is worth less than it looks:

- **CI runs none of them.** The `lint` job is a bare `cargo clippy --all-targets -- -D warnings`
  (`workflow_gen/src/workflow_gen/workflows/test.py:158-159`).
- **Agent commits run three of them.** Eight run only via `just lint`, and lefthook's
  `rust-clippy` job skips entirely when `CLAUDECODE=1` (`lefthook.yml:34-37`). Only
  `no-typed-unwrap`, `keyspace-notify-routing` and `script-gate` are wired directly to lefthook.
- There is no `agents/` page for the family, so the convention is invisible to new agents.

**This is campaign-2 item zero.** Every rule below is worthless until the gates fire, and the fix
is small: a `just lint-gates` recipe containing only the compile-free gates (sub-second), wired
into lefthook *unconditionally* — the `CLAUDECODE=1` skip exists because clippy compiles, which
these do not — plus a `seam-gates` CI job in `workflow_gen` listed in the required-jobs array, and
`agents/seam-lints.md` linked from `CLAUDE.md`. Filed as issue 06.

#### 3.1 Candidate rules

Ten candidates surveyed with real violation counts. Seven KEEP, three DROP.

| # | invariant | canonical owner | violations today | verdict |
|---|---|---|---|---|
| C1 | a rename publishing crash-critical state fsyncs contents before and the dir after | `SnapshotFs` (`persistence/src/fs_seam.rs:32`) | **6 of 11 hits** | KEEP |
| C2 | a write acked as durable used sync write options | `set_sync(true)` at `cluster/src/storage.rs:139-143` | **5 of 6 writes in the file** | KEEP (single-file pin) |
| C3 | every shard arm reaching store execution states a gate disposition | `can_execute_during_lock` (`core/src/shard/worker.rs:844`) | **12 bypass of 17 arms; 2 real defects** | KEEP — prefer the type-level fix |
| C4 | keyed commands pass slot + admin/ACL gating before execution | `PRE_DISPATCH_ORDER` + `MUST_PRECEDE` (`dispatch.rs:122,853`) | **0** — already machine-checked | DROP |
| C5 | every registered command has an ACL category row | `COMMAND_ALL_CATEGORIES` (`acl/src/categories/data.rs`) | **189 of 391 (48.3%)** | KEEP — as a Rust test |
| C6 | a discovered `./frogdb.toml` is actually applied | `config/loader.rs:82-105` | **1** (`.nested()`) + 56 unjustified `#[param(skip)]` | KEEP (thin) |
| C7 | the clock lint sees the paren-less form | `scripts/clock-seam.py:136-139` | **2 invisible today** | KEEP (bug fix) |
| C8 | one bad command cannot kill a shard worker | *none — zero `catch_unwind` in the tree* | 62 sites, **0 of them the reported bug** | DROP the grep, KEEP panic isolation |
| C9 | `Result`s from IO/fsync/consensus are not discarded | none | **91 candidates, ~4 true positives** | DROP broad, KEEP narrow |
| C10 | every error reply is built through one CRLF-sanitizing constructor | `Response::error` (`protocol/src/response.rs:184`) | **9 direct enum constructions** | KEEP |

Detail on the ones that carry live defects:

**C1 — durable publish.** ADR-0003 already states the invariant verbatim ("a new publication path
must be routed through the trait or it is untested by construction") with zero enforcement. All 6
violations are in `frogdb-replication`, which *depends on* `frogdb-persistence` but cannot reach
the seam because it is `pub(crate)`. The whole crate contains exactly one `sync_all()`, on a
forensic log; the full-sync receive→stage→publish chain issues zero fsyncs. Blocked on the
`frogdb-fs` extraction (§4.2 #1).

**C2 — durable ack.** Not a general rule; the ack is a callback invocation, not a return value, so
the shape is not expressible over `rg`. It is a hand-crafted one-file pin on the raft storage
impl, and it catches two consensus-safety defects: issue 73's `append` (`:538`→`:542`) and
campaign-2 issue 01's `save_vote` (`:485`), the latter directly contradicting the doc comment at
`:99-102` that the correct write at `:139-143` cites.

**C3 — the VLL gate.** No shard-side funnel exists: `dispatch_message` (`event_loop.rs:346`) is a
13-arm pure router applying zero gates, and each `dispatch_*.rs` hand-rolls or omits the same
four-line idiom. Beyond issue 50's `ExecTransaction`, the survey found a **second, previously
unreported defect**: `ScriptingMsg::FunctionCall` (`dispatch_scripting.rs:77`) runs arbitrary Lua
ungated while its `EvalScript` siblings gate — filed as issue 05. The better fix than a lint is
`ShardWorker::execute_gated(conn_id, f)` with `can_execute_during_lock` made private to it, which
makes a bypass a compile error and reduces the lint to one line. Blocked on a ruling for three
arm dispositions (`GetVersion`, `FunctionCall`, `VllExecute` — none documented today).

**C5 — ACL parity, the highest-yield candidate.** `CommandSpec` has no ACL field at all, so the
390-command registry and the 202 hand-written category rows are joined only by a lowercase string.
`all_for_command` ends in `.unwrap_or_default()` (`categories/mod.rs:152-157`) → empty set → the
deny loop never matches → `allow_all` returns true. `+@all -@admin` therefore permits `cluster`,
`monitor`, `migrate`, `psync`, `sync`, `replconf`, `function`, `latency`. The check belongs next
to the seven registry-wide tests that already exist in `register.rs:254-598`, with a ratcheting
allowlist mirroring the `WAL_NOOP_ALLOWLIST` at `:328-345`. Endgame: move categories onto
`CommandSpec` so omission is uncompilable, the guarantee arity already has.

**C6 — `.nested()`.** `loader.rs:91` merges `Toml::file(default_path).nested()`, which turns
top-level tables into figment *profiles* that `extract()` under `Profile::Default` never reads.
Proof it is a bug and not a design choice: the `--config <path>` branch at `:87` omits `.nested()`,
and `.nested()` appears exactly once in the workspace. Second-order, `:196-204` still sets
`config_source_path`, so `CONFIG REWRITE` clobbers a file whose contents were never read. A
one-line grep ban permanently pins a severe live defect.

**C8 — why the panic grep is dropped.** The motivating defect (`FT.SEARCH … LIMIT 0 0`) panics
inside tantivy/usearch via an `assert_ne!`, with no `unwrap()` anywhere on the path: 62 sites of
tax for zero detections. What is actually missing is structural — `catch_unwind` appears nowhere
in `core/src/shard/` or `server/src/` outside one test, so any panic in any dependency kills a
shard worker. Campaign 2 carries panic isolation at the shard message boundary as a W4 item
instead.

**C10 — error-reply sanitization.** ~~`Response::error` and `WireResponse::error` are both
`Self::Error(msg.into())` with no sanitization~~ — **RE-SCOPED 2026-08-07 after the #38 fix
landed** (`43b41fe7`). The sanitiser (`frogdb_protocol::sanitize_error_message`) landed at the
*encoding* boundary in `protocol/src/response.rs`, covering RESP2 `Error`, the RESP2 `BlobError`
downgrade, and RESP3 `SimpleError` — so the 9 direct `Response::error` enum constructions are now
harmless (they all flow through the boundary). C10 therefore becomes a **3-site, one-file rule**:
`Resp2BytesFrame::Error` / `Resp3BytesFrame::SimpleError` / the RESP2 blob downgrade are
constructed nowhere but through `sanitize_error_message` in `protocol/src/response.rs`. The lint is
what makes that boundary unbypassable. Closest analogue is the already-shipped `lint-redirect-seam`.
No longer blocked (the fix it waited on has shipped); ready to land in W1.

#### 3.2 Mechanics

Copied from the shipped gates, not invented:

- **Location.** `scripts/<rule>.py` with a PEP-723 header and no dependencies, or an inline
  `Justfile` recipe if under ~40 lines — which is what 9 of the 11 existing gates are. Reach for
  Python when the rule needs `#[cfg(test)]`-span awareness, a structured allowlist with reasons,
  or two-way checking.
- **Shared helper.** `cfg_test_spans()` and `is_test_path()` are private to
  `scripts/clock-seam.py:153-179` and are needed by C1 and C9. Factor into `scripts/_rustscan.py`
  before the third copy drifts.
- **Suppression.** Two shipped idioms, no new ones. Count-pinned per-file allowlists
  (`clock-seam.py:73-134`, verified both ways at `:261-274` — a stale entry is an error and a
  grown count is an error), and named-gap warn-not-fail (`spec-lint.py:20-26`, where
  `MISSING ([gap: <issue>](<link>))` warns only if the link resolves to a real issue file) for
  anything blocked on a decision. Never in-code `#[allow]`: clippy cannot express these rules, and
  an in-code hatch is invisible to review.
- **Ratchet.** No new baseline mechanism. The count-pinned allowlist *is* the ratchet and is
  better than a baseline file because every entry carries a written reason and stale entries
  self-report. Land each rule with all current violations listed and
  `reason = "pre-existing, tracked by .scratch/hardening-2/issues/NN"`; burn down in batches;
  delete the list when empty, as `lint-format-float` and `lint-keyspace-notify-routing` already
  have.

**Landing order.** C2, C5, C6, C7 have no blockers and go first. C1 waits on the `frogdb-fs`
promotion, C3 on the three arm-disposition rulings, C10 on adding sanitization to
`Response::error` (a lint without the fix is churn).

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

The audit is done (§3.3): **19% of rows and ~34% of tags are weaker than the table implies.** This
workstream is the largest in the campaign, and it is mostly hand work — §3.4 shows the cheap lints
reach ~40% recall at best.

- **W3a — re-witness by hand, ordered by blast radius.** Every crash, fsync, partition, timing and
  concurrency row, area by area. The forcing tooling already exists (§3.3); this is discipline,
  not construction. Persistence first (its weakest evidence is in the unmutated `frogdb-core`),
  then cluster, then txn/vll/blocking, then replication.
- **W3b — the witness lints of §3.4.** H1 + H10 as errors with count-pinned allowlists, H2/H7/H8/H9
  as warnings, H4 rewritten against a curated injector allowlist, H5 as a `just` audit recipe over
  retained mutation logs. Ratchet value, not detection value — build them so the *next* weak tag
  costs something, then keep hand-auditing.
- **W3c — make the escape hatch the expected move.** `MISSING ([gap: <issue>](<link>))` was used
  zero times in 258 rows. Every re-witness that cannot be forced files a tooling-gap issue and uses
  the hatch. Report the hatch count as a campaign metric: a rising count is health, not debt.
- **W3d — row truth.** All 258 rows re-read for falsifiability (3 known unfalsifiable) and for
  Invariant/Observable cells matching today's code. Fix the two mis-tagged rows (issue 09) and
  re-run the gates over the 15 rows that postdate their area's lock.

#### 3.3 The witness audit: how thin the evidence actually is

54 rows sampled across all six areas (weighted to crash, fsync, partition, timing and concurrency
rows), ~267 of their named tests read.

| area | rows sampled | weak rows | tests inspected | weak tests |
|---|--:|--:|--:|--:|
| persistence | 13 | 15% | 87 | **31%** |
| cluster | 14 | 14% | 79 | **39%** |
| txn / vll / blocking | 14 | 21% | 46 | **28%** |
| replication | 13 | 23% | ~55 of 149 named | **~35%** (biased up — targeted subsample) |
| **overall** | **54** | **19%** | **~267** | **~34%** |

Read the two columns against each other. **The row rate is the campaign's honest number; the test
rate is tag inflation.** One in three FM tags does not witness the row it names. Rows survive
anyway because ~4 of 5 have at least one genuinely purpose-built witness, with a crowd of
neighbours tagged alongside — so `Forced by` lists of 12–22 tests read as depth and are not.
FM-CLUSTER-017 names 18 tests and is carried by 2. FM-PERSISTENCE-017 names 16 and is carried by
3, none of which touches its headline NOT-observable.

Zero rows were unverifiable by reading. The weakness is never ambiguity — it is absence.

Representative findings:

- `crash_recovery_tests.rs:702` — terminal assertion is `assert!(result.is_ok() || result.is_err())`.
- `wal_recovery_mode_is_pinned_to_point_in_time` (`persistence/src/rocks/tests.rs:1312`) —
  `assert!(matches!(DBRecoveryMode::PointInTime, DBRecoveryMode::PointInTime))`, constructing the
  enum locally and never reading the production open path.
- `test_explicit_sync_wal` passes with the `sync_wal()` line deleted.
- `test_bgsave_snapshot_survives_restart` restarts on the *same* data dir, so both keys return via
  RocksDB WAL replay — it passes if the checkpoint were empty.
- FM-CLUSTER-066's entire subject is dead code under test: **no test anywhere constructs a
  `ClusterBusContext` with `tls: Some(...)`**; `test_context` hardcodes `tls: None`.
- `CrashTestHarness::crash()` (`core/src/persistence/test_harness.rs:183`) is
  `self.rocks = None` — a clean in-process `Drop`, so nothing unsynced is ever lost. Its
  `with_sync_mode()`/`with_periodic_mode()`/`with_async_mode()` constructors set a `WalConfig`
  **nothing reads**. Ten weak tags trace to this one file. Filed as issue 08.

**Seven of the eight worst offenders are discipline gaps, not tooling gaps.** The tooling needed
— page-cache crash injection, partition simulation, deterministic scheduling, fault filesystem —
*already exists in this repo* (`PageCacheSink`, turmoil `sim.hold`/`sim.release`,
`LagProxy::stall_acks`, `RecordingFs`, `start_paused`, Shuttle, `shard-harness`), and three of the
eight have their forcing pattern implemented in a sibling test in the same file. Residual real
gaps are narrow: a counting global allocator, a seam to kill the periodic syncer, and a test-only
`ConnectionHandler` constructor. **The campaign is not tooling-limited. It is discipline-limited.**

#### 3.4 Witness lints

Counts over 1233 tagged tests / 258 rows.

| # | heuristic | today | disposition |
|---|---|--:|---|
| H1 | tagged test body contains no assert-family macro at all | **9 tests** | **error** (needs a `prop_assert*`/helper allowlist, else ~40% FP) |
| H2 | every assertion is `is_ok()`/`is_some()`/`is_none()`-shaped or `assert!(true)` | **22 tests** | warning |
| H8 | row `Trigger` names concurrency but no named test spawns a second task/thread/client | **21 rows** | warning — highest recall against the real class |
| H9 | every assertion sits inside a conditional branch | **17 tests** | warning |
| H7 | row names a RESP error code that appears in no named test | **10 rows** | warning |
| H10 | `eprintln!` inside a tagged test | **1** | error (ratchet) |
| H4 | fault row whose tests import no fault-injection facility | unusable as written | **rewrite with a curated injector allowlist** — then it is the direct detector for the three worst offenders |
| H5 | row's tests never killed a mutant in the crate they describe | **43 rows** with no in-crate killer; **79 rows** with no named test in any mutated crate | build as a `just` audit recipe, not a gate |
| S1/S3 | single-test rows (**31**), ≤6-substantive-line tests (**81**) | — | informational report |
| H3, H11 | identifier-overlap, NOT-observable-type-absence | 318 pairs / 5 rows | **reject** — noise |

**H5 is buildable from artifacts already on disk.** `target/mutants/*/outcomes.json` records only a
per-mutant `summary` and does *not* say which test killed each mutant — but the retained
`log/*.log` files contain `test <name> ... FAILED` lines, so killing-test sets can be extracted
with no re-run. Caveat that must ship with the number: replication/cluster/cluster-runtime were
`--iterate` runs, so their zero-kill counts are inflated; persistence/recovery/txn/vll logs are
complete.

**The uncomfortable part: combined recall of every cheap heuristic is ~40%.** Against the 10 weak
rows the hand audit found, H1 catches one, H2 catches one, H8 catches two, H7 catches none. The
two worst shapes — *the fault is never induced* and *the test passes if the fix is reverted* — are
semantic properties no regex reaches. `test_failover_force_replay_is_safe` is well-named,
well-asserted, and wrong. Only H5 and a rewritten H4 target that class. So the lints are worth
building for the ratchet effect (they make the next assert-nothing tag cost something to write),
**not** as the primary defense; the primary defense is W3's hand re-validation of the crash,
restart and race rows.

#### 3.5 Why this class exists

Four structural causes, each fixable:

1. **The lint counts names.** A row is "forced" the moment someone types a test name into a table
   cell. That is the entire enforcement surface.
2. **The escape hatch exists and was never used.** `spec-lint.py` supports
   `MISSING ([gap: <issue>](<link>))`, which warns instead of failing — exactly the pressure valve
   for "not forceable with today's tooling." **Zero uses across 258 rows**, and no fault-injection
   tooling-gap issue was ever filed. When admitting a gap is free but invisible, the path of least
   resistance is to tag the nearest existing test. That is the mechanism that produced all 90 weak
   witnesses. Campaign 2 must make using the hatch the expected move, and report its count.
3. **The campaign found this class once and declined to detect it.** Hardening issue 19 fixed 11
   conditional-assertion tests in one file and explicitly recorded that the suggested lint was
   *not* added. H10 has 1 hit today because that fix held in that file; the class regenerated in
   five others.
4. **The weak tests live where the mutants don't.** `frogdb-core` was never mutated, and
   `core/src/persistence/crash_recovery_tests.rs` holds 10 of persistence's 27 weak tags including
   the tautology. Persistence scored 99.1% on `frogdb-persistence` while its weakest evidence sat
   in an unmutated crate. This is the same finding as B2, arrived at independently.

Two further bookkeeping defects the lint cannot see, because both rows name tests that exist:
FM-CLUSTER-059's five tests manipulate `ClusterRuntimeFlags` directly while its `Trigger` is a
`CONFIG SET`, and the test that would close it exists and is not cited; FM-TXN-040's Observable
*is* witnessed, by a test tagged FM-CLUSTER-083. Filed as issue 09.

And **15 rows postdate their area's lock** (CLUSTER-079..083, PERSISTENCE-048..052,
REPLICATION-041/062/063/064, TXN-050) — they carry a mutation-tested area's badge without ever
having been in a mutation run. Campaign 2 re-runs the gates over them.

Good news: the rows themselves are tight. Only **3 of 258** are unfalsifiable as written
(FM-PERSISTENCE-013 "eventually", FM-PERSISTENCE-019 "may or may not be present",
FM-REPLICATION-027 "eventually converges"). The vagueness the campaign guarded against did not
materialize; the tag inflation it did not guard against did.

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

**Answer: one small extraction, and only because it creates the chokepoint W1 needs. Everything
else is ceremony.**

The decisive evidence is an experiment already run: `frogdb-cluster` is extracted *and*
mutation-gated at 99.6%, `frogdb-replication` at 98.7%, and both still carry live missing-fsync
defects — issue 73 at `cluster/src/storage.rs:539`, a previously unfiled wrong-column-family
`flush()` at `:483-489`, zero fsync in `replication/src/state.rs:281-284`, unsynced full-sync
writes at `fullsync.rs:505` and `stager.rs:129`. Mutation testing cannot mutate a call that was
never written, and a new boundary produces a new score that measures the same thing. Of five
candidate extractions evaluated, **zero would have caught issue 73 or issue 50**.

### 4.1 What the survey found: six hand-rolled durable writers at four correctness levels

`SnapshotFs` (`persistence/src/fs_seam.rs:32-93`) is the only correct durable-write primitive in
the repo — and it is `pub(crate)`, so nothing outside `frogdb-persistence` can use it. The result
is six independent implementations of "atomic durable file write":

| file:line | protocol | file fsync | dir fsync | atomic |
|---|---|---|---|---|
| `persistence/src/data_dir.rs:191-194` | write tmp → sync_file → rename → sync_dir | ✅ | ✅ | ✅ |
| `search/src/vector.rs:452-462` (+ private re-impls of both sync helpers at `:433-447`) | same | ✅ | ✅ | ✅ |
| `scripting/src/persistence.rs:175-200` (FUNCTION registry) | tmp → sync_all → rename | ✅ | ❌ | ✅ |
| `server/src/config_persister.rs:98-128` (`CONFIG REWRITE`) | tmp → sync_all → rename | ✅ | ❌ | ✅ |
| `replication/src/state.rs:277-291` (replid + `offset_at_save`) | `fs::write` → `fs::rename` | ❌ | ❌ | name only |
| `acl/src/manager.rs:267-277` (`ACL SAVE`) | `File::create` → `write_all` → drop | ❌ | ❌ | ❌ truncates in place |

Plus unsynced writes on the full-sync receive path (`fullsync.rs:483-505`), the staged-checkpoint
commit rename (`stager.rs:129`), the disarm rename (`state.rs:142`), and the split-brain forensic
log (`split_brain_log.rs:150-173`, file synced but not the dirent that `has_pending_logs` scans).

### 4.2 Verdicts

| # | candidate | verdict | why |
|---|---|---|---|
| 1 | **`frogdb-fs`** — promote `fs_seam.rs` (238 LOC) to a zero-dependency crate exporting `DurableFs`, `RealFs`, a composed `publish_file`, and `RecordingFs` behind a `test-support` feature | **BUILD** | It is the chokepoint W1's durable-write rule needs. Also hands `RecordingFs` to `frogdb-replication` and `frogdb-search`, where fsync-vs-rename *ordering* is untestable by construction today. `search`/`scripting`/`acl` depend on `frogdb-types` only, so a visibility change alone will not reach them without dragging in rocksdb. ~250 LOC in, ~120 LOC of duplicated/wrong code out, 6 call sites, zero cycle risk |
| 2 | **`StagedCheckpoint::publish(incoming, &dyn DurableFs)`** — the *writer* half of the staged-checkpoint contract | **BUILD (not a crate)** | `persistence/src/rocks/staged.rs` already declares the three-party contract; the installer is 99.1%-gated while the writer in replication is fsync-free. A replica can finish a full sync, ack, crash, and boot into a torn copy — `checkpoint.rs:62-77` validates `CURRENT` exists but cannot detect zero-length SSTs. ~80 LOC, no new crate |
| 3 | `frogdb-raft-storage` (split `cluster/src/storage.rs`) | **DON'T** | 1,157 LOC moved, `TypeConfig`/`NodeId`/`StoredClusterSnapshot` go public, snapshot coupling re-expressed as a trait — for a boundary the experiment has already refuted |
| 4 | `frogdb-wal` | **REJECT** | Circular: `wal/flush.rs:12,130,370` ↔ `rocks/checkpoint.rs:31`, `rocks/mod.rs:496`. The durable-sync watermark is a two-way protocol; splitting means moving `RocksStore` too |
| 5 | broad `frogdb-durability` (WAL + snapshots + offsets + raft log) | **REJECT** | Re-merges three locked crates into ~30K LOC that no longer fits a single mutation run — undoing the property extraction actually bought |

### 4.3 What to do instead: a cross-crate durability contract

The gap this survey found is not a missing crate. It is that **no spec row anywhere claims "the
replication offset file is durable" or "a staged checkpoint's bytes are on the platter before the
commit rename."** Durability decisions are scattered across nine files in four crates
(`core/src/shard/post_execution.rs:282-294`'s `WRITE_EFFECT_ORDER`, `persistence.rs:247-303`'s
`Durability::Confirm`, `checkpoint_quiesce.rs:89-108`, `offset_coordinator.rs:108-115`,
`wait_coordinator.rs:214-257`, `cluster/src/storage.rs:483-604`), and each is individually
defensible while the composition is unstated.

W2 therefore gains a deliverable: a **durability failure-mode spec** that names, for every
persisted artifact — WAL, checkpoint, staged checkpoint, replication state, raft log, raft vote,
cluster snapshot, ACL file, FUNCTION registry, config rewrite, split-brain log — who fsyncs it,
at what point it may be acked, and what a crash immediately before and after that point must
leave behind. Rows come first; the crash harness (W2) is what forces them.

### 4.4 Non-extraction findings the survey turned up

Filed as campaign-2 issues:

- **01** — `save_vote` writes `KEY_VOTE` to CF `raft_meta` then calls `DB::flush()`, which flushes
  the *default* CF (`cluster/src/storage.rs:483-489`). Raft's vote-durability precondition, and
  the crate's own doc at `:98-102` says verbatim that this is not durability. Sibling of issue 73.
- **02** — the ACL file is never read at boot. `AclManager::load()` is reachable only from
  `ACL LOAD`; `init.rs:240` only synthesizes `default` from `requirepass`. Every `ACL SETUSER`
  is silently lost on restart.
- **03** — the six-writer durable-write divergence above (the W1 rule's burn-down list).
- **04** — `durability_mode` is parsed twice from the config string —
  `server/src/server/util.rs:40-67` (`unreachable!()` on unknown) and independently at
  `server/src/server/startup.rs:95` (`== "periodic"` decides whether `spawn_periodic_sync` runs).
  Nothing pins the two equal.

## 5. Defect waves

The 18 confirmed live defects from the round-2 re-triage plus the 10 the campaign-2 surveys found,
ordered by blast radius. Waves run *after* the mechanism that would have caught each class exists,
so that each fix lands with a detector behind it. Round-2 issues are bare numbers
(`../testing-improvements-round2/issues/open/`); campaign-2 issues are `c2-NN` (`issues/open/`).

| wave | defects | gated on |
|---|---|---|
| 0 — the gates don't run | **c2-06** (11 seam gates fire in neither CI nor agent commits), 27/28/31 (coverage pipeline — fix per §8 ruling 7) | nothing; blocks every W1 item, ships first |
| 1 — reachable pre-auth | 38 (CRLF frame injection), 63 (shard panic), **c2-07** (no panic isolation at the shard boundary) | nothing |
| 2 — consensus + isolation | 73 (raft log fsync), **c2-01** (`save_vote` flushes the wrong CF), 53 (stale log reader), 50 (EXEC gate), **c2-05** (`FunctionCall` gate) | W2 harness for 73/c2-01; W1 C3 lint for 50/c2-05 |
| 3 — silent data loss | 45 (`FT.ALTER` wipes JSON index), 44 (`TS.CREATERULE` not persisted), 42 (RocksDB iteration error → `None`), 24 (`BLMOVE` WRONGTYPE), 43 (`ES.SNAPSHOT` non-UTF-8) | W1 error-swallow rule |
| 4 — auth and boundaries | 37 (Lua sandbox escape), 35 (`-@admin` inert), 39 (MONITOR leak), 40 (default-open admin gate), 68 (ACL ratelimit lockout), **c2-02** (aclfile never read at boot) | W4 spec |
| 5 — silent misconfiguration | 49 (`frogdb.toml` ignored), 72 (no format version), 54 (BCAST invalidation), **c2-04** (`durability_mode` parsed twice) | W6 |

Three campaign-2 issues are structural rather than defect fixes and belong to their workstream, not
a wave: **c2-03** (six hand-rolled durable writers) is W1's C1 burn-down list plus the `frogdb-fs`
seam of §4.3; **c2-08** (`CrashTestHarness::crash()` is a clean `Drop`, `WalConfig` unread) is a
W2 prerequisite — ten weak persistence tags cannot be re-witnessed until the harness crashes for
real; **c2-09** (two mis-tagged FM rows) and **c2-10** (the scatter error replies — every
client-visible VLL degradation string — have no tests at all) are W3d.

## 6. Gates, metrics, and exit criteria

The campaign exits when all of the following hold:

0. **The gates run.** `just lint-gates` is wired to lefthook unconditionally and to a required CI
   job, and a deliberate violation of each of the (then) rules is shown to fail both. Without this
   every other criterion is a convention, not a gate.
1. Every W1 rule that reached KEEP is wired into `just lint-gates` with its baseline at zero, or
   with a burn-down plan recorded in this directory.
2. The crash harness exists — out-of-process, `SIGKILL`, recover-and-compare — and at least the
   raft log, the WAL/checkpoint install, and the replication offset advance are witnessed by it.
   `CrashTestHarness` either crashes for real or is deleted (c2-08).
3. **Witness truth**, the campaign's headline number: every crash / fsync / partition / timing /
   concurrency row in all six areas hand-re-witnessed; the W3 lints wired with baselines at zero;
   zero rows that no test could fail; the mis-tagged rows corrected. The measured metric is the
   weak-tag rate on a fresh independent sample — target **≤5%**, from ~34% today — not a lint
   exit code, because the lints only reach ~40% of the class.
4. The security spec is LOCKED with a mutation gate on record, on the same terms as the other
   four areas.
5. All 18 round-2 live defects and the 6 campaign-2 defects fixed, each with a failure-mode row and
   a forcing test that fails when the fix is reverted.
6. `frogdb-core` dispatch is inside a mutation gate, or a written decision says why not. The 15
   rows that postdate their area's lock have been through a mutation run.
7. The `MISSING ([gap: …](…))` count is **greater than zero** and every use links a filed tooling
   issue. A campaign that re-witnesses 258 rows and admits no gaps has not been honest.

## 7. Out of scope

- `frogdb-operator/**`, `frogctl/**` — unchanged from campaign 1.
- `website/**` — in scope only for documenting behavior changes this campaign makes (approved
  2026-08-06).
- Performance work, new features, and the exotic command families beyond the defects listed here.
- The redis-regression suite stays unfrozen and is treated as a normal test suite; no refreeze.

## 8. Open decisions — ALL RULED 2026-08-07

Original questions kept for the record; ruling appended to each.

1. **Scale and sequencing.** Campaign 1 ran four areas serially over weeks. Campaign 2 is wider
   (7 workstreams, 27 defects, a 258-row re-witness). *Recommendation:* run W0/W1's item zero
   (c2-06) and waves 0–1 immediately since they are small and unblock everything, then W3a — the
   re-witness — as the long pole, with W2 built alongside it because W3a's persistence tranche
   needs the crash primitive.
   **Ruling: approved as recommended.** W0 + waves 0–1 first, then W3a as the long pole with W2
   built alongside.
2. **H1 as an error, today.** 9 tagged tests have no assertion at all. Adopting H1 as an error
   means fixing or allowlisting all 9 in the same change, and it needs a `prop_assert*`/custom-
   helper allowlist or it false-positives at ~40%. *Recommendation:* adopt as error with a
   count-pinned allowlist, mirroring `clock-seam.py`.
   **Ruling: approved as recommended** — error with count-pinned allowlist.
3. **`frogdb-core` mutation perimeter (B2).** It is the largest crate and was never mutated; the
   weakest persistence evidence lives in it. A full run is expensive. *Recommendation:* mutate the
   `shard/` dispatch and `persistence/` subtrees only, at a lower threshold (0.70) to start.
   **Ruling: approved as recommended** — `shard/` + `persistence/` subtrees, 0.70 threshold to
   start.
4. **C3 arm dispositions.** `CoreMsg::GetVersion` mutates via `purge_if_expired` — is that store
   execution under a foreign continuation lock? `VllMsg::VllExecute` hardcodes `conn_id = 0` — is
   the drain path exempt by design? Both need a recorded ruling before the lint can be exhaustive
   (c2-05).
   **Ruling: investigate-and-propose.** During C3/c2-05 work, write forcing tests for both arms,
   record proposed dispositions with evidence, and present for approval before the C3 lint lands.
   C2/C5/C6/C7 are not blocked on this.
5. **ACL missing-file-at-boot policy.** Redis refuses to start when `aclfile` is unreadable.
   FrogDB currently ignores the file entirely. *Recommendation:* match Redis and refuse — but this
   turns a silently-degraded boot into a hard failure for anyone with a stale path (c2-02).
   **Ruling: match Redis** — refuse to start when the configured `aclfile` is unreadable.
6. **Does the campaign own its own weak witnesses?** W3a's re-witnessing will surface rows whose
   *fix* is out of scope (search, timeseries, JSON). *Recommendation:* re-witness anyway, file the
   defect, and let the wave table decide whether it ships — a weak witness is in scope even when
   the subject is not.
   **Ruling: re-witness** everything regardless of subject scope; file defects; wave table decides
   what ships.
7. **Coverage pipeline.** Left broken at campaign-1 exit. *Recommendation:* fix it in W0 or turn it
   off; a coverage number nobody trusts is worse than none.
   **Ruling: fix it in W0.** Issues 27/28/31 join wave 0 alongside c2-06.

## Decisions — locked-areas-mechanical dispatch (2026-09-02)

Grill-and-dispatch ledger for issues 13–16. Appended per answer; lines never edited.

- D1: scope = issues 14, 15, 16; integration branch `locked-areas-mechanical/impl` off `worktree-locked-areas-mechanical` (13 unmerged on main); fu-a/b/c stay unfiled.
- D2: gate command = `just workflow-gen --check && just lint-gates && just lint-py && just fmt-py-check && just scratch-check` (compile-free; per-issue extras live in that issue's acceptance criteria).
- D3: live CI verification in-session = push `locked-areas-mechanical/impl` and open a draft `[ci-verify]` PR against `main` (closed unmerged) so the forcing tests run for real: a locked-crate touch spawns a red `mutants-diff` leg, a `frogdb-commands`-only change spawns none, and `mutants-weekly.yml` dispatched with `crate: frogdb-txn` prints the score against the header gate.
