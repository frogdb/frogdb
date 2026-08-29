# 28 — `replica-serve-stale-data` knob

Status: done
Landed: 2026-08-29 — spec reconciled against the shipped knob/gate, stranded-promotion gap
found and closed in the same session

## Parent

Found by the 2026-08-13 anti-pattern review of the LOCKED replication spec, finding A4
(`.scratch/formal-spec/reviews/2026-08-13-antipattern/spec-review-replication.md`), raised
against [issue 16](16-failed-promotion-strands-the-applied-gate.md)'s ruled behaviour and
`FM-REPLICATION-029`'s Invariant ("There is deliberately **no** stale-read gate on the read
path: FrogDB has no `replica-serve-stale-data` config knob at all").

## What the review found

The no-knob deviation is defensible in the ordinary lagging-but-connected replica case, and it
matches Redis's `yes` default. Issue 16's ruled outcome changes the shape of the worst case: a
node whose promotion failed strands with its applied gate frozen — it follows nobody, applies
nothing, and (in standalone mode) stays that way until a human intervenes. It keeps answering
reads from a keyspace frozen at the moment of the failed promotion, for an **unbounded** period.
The ruled observability for that case is operator-facing (a metric and an `INFO` field); a
client only has `master_link_status` to infer staleness from, and must know to look for it.

Redis and Valkey offer `replica-serve-stale-data no` for exactly this class of exposure.
CockroachDB bounds follower reads by a closed timestamp so staleness is bounded by a known
amount; etcd defaults to linearizable reads and makes serializable (possibly-stale) reads
opt-in per request. FrogDB currently has no equivalent lever at all.

## Precedent

Redis's `replica-serve-stale-data` (default `yes`) controls whether a replica whose link to its
master is down answers data commands with `-MASTERDOWN` (when `no`) or serves what it has (when
`yes`); `INFO`/auth-class commands are always answered regardless of the setting.

## Ruling (2026-08-13)

**Implement the `replica-serve-stale-data yes|no` config knob NOW with Redis semantics and
spelling: when `no`, a replica whose link with its primary is down (including the
stranded-promotion case, which produces unbounded staleness) answers errors to data commands,
excepting INFO/auth-class commands; when `yes` (default, Redis parity) it serves possibly-stale
reads. FM-REPLICATION-029 gains a scope note naming stranded promotion as an
unbounded-staleness source, and the ruling records client detection = `master_link_status` in
INFO. New FM row(s) + forcing tests; knob should be live-mutable like other config where
feasible. The user chose implement-now over doc-only.**

## What to build

- The `replica-serve-stale-data` config knob (`yes` default, `no` opt-in), live-mutable per the
  project's config conventions where feasible.
- Gate data commands on a replica whose link is down when the knob is `no`: an error reply
  (Redis-parity shape — `-MASTERDOWN`) rather than a stale answer.
- `INFO`/auth-class commands (`AUTH`, `HELLO`, `PING`, `INFO`, `REPLCONF`, ...) are exempt from
  the gate under both settings.
- The stranded-promotion state from issue 16 counts as "link down" for this gate — it is the
  unbounded-staleness case the review named, and closing it is the point of implementing the
  knob now rather than later.
- Amend `FM-REPLICATION-029`'s scope note to name stranded promotion as an unbounded-staleness
  source (today it reasons only about "lagging but connected" plus two inspected neighbours).
- Record in the FM row that client-side detection is `master_link_status` in `INFO replication`.

## Acceptance criteria

- [ ] `replica-serve-stale-data no` refuses data commands on a replica with a down link,
      `INFO`/auth-class commands still answered
- [ ] `replica-serve-stale-data yes` (default) preserves today's serve-whatever-you-have
      behaviour, unchanged
- [ ] The stranded-promotion state (issue 16) is exercised as a forcing case for the gate
- [ ] Knob is live-mutable (or the FM row states why not, if infeasible)
- [ ] New `FM-REPLICATION-NNN` row(s) naming the forcing tests
- [ ] `FM-REPLICATION-029`'s scope note amended per the ruling
- [ ] Redis-deviations table entry updated (this closes a deviation rather than opening one)

## Witness

To be added: a forcing test exercising `replica-serve-stale-data no` against a replica whose
`master_link_status` is down, including via the stranded-promotion path from issue 16.

## Amendment (2026-08-21) — the knob shipped under redis-feel issue 17, default inverted

The knob, the gate and the live-mutability requirement were built by
[redis-feel issue 17](../../../redis-feel/issues/done/17-unimplemented-admission-gates.md),
which needed the same gate to give `CommandFlags::STALE` a reader. What landed:

- `replication.replica-serve-stale-data`, live-mutable, in the config registry
  and the golden snapshot.
- The gate at the end of `PreDispatchView::run_pre_checks`, reading link state
  through `RoleController` (`primary_target().is_some() && !master_link_up()`)
  and policy through `frogdb_core::command_admission::stale_refusal`.
- Redis's verbatim `-MASTERDOWN Link with MASTER is down and
  replica-serve-stale-data is set to 'no'.`
- The exemption set is **flag-driven** (`CommandFlags::STALE`) rather than a
  hand-listed INFO/auth class, so `COMMAND INFO` and the gate cannot disagree.

**This issue's 2026-08-13 ruling of `yes` (Redis parity) is superseded.** The
2026-08-21 ruling on issue 17 sets FrogDB's default to **`no`** — a deliberate
deviation, on the CockroachDB/FoundationDB precedent this issue's own analysis
cites: fail fast rather than answer unbounded staleness silently. `yes` restores
Redis behaviour.

### Residue still owned by this issue

1. `FM-REPLICATION-029`'s Invariant still says a stale-read gate deliberately
   does not exist and no knob exists. It must be rewritten to describe the gate,
   name its forcing tests, and record `master_link_status` as the client-side
   detection signal. Not done under issue 17: `specs/replication.md` (and its
   generated website mirror) were held by another session's uncommitted work at
   the time, and `frogdb-replication` is locked.
2. The stranded-promotion forcing case (issue 16's frozen applied gate) is not
   yet exercised against the gate. Issue 17 landed the ordinary link-down cases
   only (`a_link_down_replica_refuses_reads_by_default`,
   `the_serve_stale_data_knob_restores_redis_behaviour`,
   `a_primary_is_never_stale_gated` in
   `frogdb-server/crates/server/tests/integration_replication.rs`, plus the
   policy-level cases in `frogdb-core`'s `command_admission::tests`).
3. The Redis-deviations table entry: the compatibility overview now documents
   the inverted default
   (`website/src/content/docs/compatibility/overview.mdx`, "Replication"), but
   the spec-side deviations table has not been touched — same locked/WIP reason.

## Resolution (2026-08-29)

Closed the residue listed above.

**Supersession recap.** This issue's 2026-08-13 ruling (`implement now, default yes`) was
superseded 2026-08-21 by
[redis-feel issue 17](../../../redis-feel/issues/done/17-unimplemented-admission-gates.md)
(commits `785fd51a..51f0526e`), which shipped the knob and gate with default **`no`** — the
CockroachDB/FoundationDB fail-fast precedent this issue's own analysis cited — plus the
container-`HELP` admission refinement (commit `d74f86c2`). The shipped default is authoritative
and was not touched by this session.

**Spec-side residue (item 1 and 3 above), closed in `specs/replication.md`:**

- `TR-REPLICATION-025` retitled and rewritten: it described "a replica-side read is served
  regardless of link health" (claiming no gate existed at all); it now describes the actual
  gate, its default, and its live-mutability.
- `FM-REPLICATION-029`'s Invariant and "NOT observable" cells rewritten — they said "FrogDB has
  no `replica-serve-stale-data` config knob at all", which the shipped code makes false. A scope
  note was added naming stranded promotion (issue 16, `TR-REPLICATION-009`) as an
  unbounded-staleness source, and recording `master_link_status` in `INFO replication` as the
  client-side detection signal, per the original ruling's requirement.
- A new dedicated row, `FM-REPLICATION-067`, was added for the knob/gate itself (no such row
  existed — only `TR-REPLICATION-025` and `FM-REPLICATION-029`'s prose touched on it), naming
  all six forcing tests (four existing integration tests plus two new unit tests, below).
- The Redis-deviations table row (formerly filed under `FM-REPLICATION-029`, now under
  `FM-REPLICATION-067`) was rewritten: it read backwards ("no knob... unconditional yes" vs.
  "defaults to yes but can be set to no" — i.e. it described the *pre-issue-17* state as
  FrogDB's and Redis's own behaviour as the deviation). It now correctly states FrogDB has the
  knob/gate/error-shape at parity with Redis, and the sole remaining deviation is the default
  value (FrogDB `no` vs. Redis `yes`), with the fail-fast rationale.

**Stranded-promotion finding (item 2 above) — a real gap, found and fixed.** Tracing
`RoleManager::promote()`'s failure path
(`frogdb-server/crates/server/src/role_manager.rs`) against the shipped gate
(`PreDispatchView::run_pre_checks`,
`frogdb-server/crates/server/src/connection/guards.rs`) showed the stranded-promotion case
**did not trip the gate**: the gate's condition was
`rc.primary_target().is_some() && !rc.master_link_up()`, but a failed promotion unconditionally
clears `primary_target` to `None` before the fallible identity-mint step, while leaving
`is_replica()` at `true`. A stranded node was therefore read as "this is a
primary/standalone" and the gate never engaged — exactly the unbounded-staleness case this
issue's ruling named as a forcing case for the gate, left un-gated.

Fixed within the existing seam, touching only the unlocked `frogdb-core`/`frogdb-server`
crates (no `frogdb-replication`/`frogdb-replication-runtime` change, so no mutation-gate
run applies):

- Added `RoleController::is_replica()` to the trait (`frogdb-core/src/command.rs`), documented
  against the `primary_target()` divergence.
- Switched the gate's condition in `run_pre_checks` from `primary_target().is_some()` to
  `is_replica()`.
- Implemented the new method on all four implementors: `RoleManagerHandle` (production,
  delegating to the pre-existing `RoleManager::is_replica()`), and three test doubles
  (`frogdb_core::shard::types::FixedRoleController`, `cluster_init::RecordingController`, and a
  new local `FixedRoleController` in `guards.rs`'s test module).
- Added two regression unit tests in `guards.rs`:
  `stale_gate_trips_on_stranded_promotion_with_no_primary_target` (asserts a stranded node now
  gets MASTERDOWN by default) and `stale_gate_does_not_trip_for_a_primary` (guards against
  over-correction — a genuine primary must never be gated).

All acceptance criteria are now met, including "the stranded-promotion state is exercised as a
forcing case for the gate", which was the one criterion issue 17 had not covered. No further
gap remains that is this issue's to own; issue 16 (making the stranded-promotion state itself
more observable/retryable) stays open as its own, separately-scoped issue — this closure only
required that the *existing* stale-read gate correctly treat that state as "link down", which it
now does.
