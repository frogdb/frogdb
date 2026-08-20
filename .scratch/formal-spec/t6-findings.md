# T6 — temporal (liveness) properties for the migration × failover model: findings

**Campaign**: quint-completeness (`.scratch/formal-spec/2026-08-19-quint-completeness-campaign.md`), task T6.
**Date**: 2026-08-19/20. **Mode**: local.
**Outcome**: properties **landed and typechecked**; **no backend can produce a complete verdict on
them today** (Apalache refuses fairness; TLC accepts it but cannot terminate on this model). The
nightly lane is wired, defaults to TLC, and is **report-only**: today it is a time-capped liveness
search whose expiry is inconclusive, and it starts producing full verdicts the day a backend can.

## TL;DR

| | |
|---|---|
| Properties added | 4 (`temporal_*`) in `specs/quint/cluster_migration_failover_temporal.qnt` |
| Safety model touched | **No** — new sibling root module, zero `val inv_*`, so excluded from `quint-models.sh`-derived gates |
| `quint typecheck` | passes |
| `quint verify` (Apalache 0.56.1, default backend) | **refuses**: `error: Handling fairness is not supported yet!` |
| `quint verify` with fairness stripped (probe) | **crashes**: `error: assertion failed` inside PASS #13 BoundedChecker, ~32s |
| `quint verify --backend tlc` | **accepts fairness and checks**, but **cannot terminate**: quint's TLC path emits no depth bound and the model has unbounded counters. 900s → 1,287 distinct states, 1,269 still queued, no violation found. This is the nightly lane's backend |
| Shrunk instance to rescue it | **impossible without editing the frozen model** — `NODES`/`SLOTS` are `pure val`, not `const` |
| CI | `verify-temporal` job added to `quint-verify-nightly.yml` via the generator, report-only |

## Environment

- quint `0.32.0` (mise, `npm:@informalsystems/quint`)
- Apalache `0.56.1` (auto-downloaded to `~/.quint/apalache-dist-0.56.1`)
- JVM: Temurin `21.0.12+8` (LTS)
- macOS aarch64, local mode (no testbox involved)

## What was added

`specs/quint/cluster_migration_failover_temporal.qnt` — a **sibling root module** importing the
same four satellites as `cluster_migration_failover.qnt` (`cluster_common_types`,
`..._failover_types`, `..._failover_logic`, `..._failover_machine`). It declares **only**
`temporal` properties: no `var`, no `action`, no guard, and — deliberately — no `val inv_*`.

That last point is the isolation mechanism: `scripts/quint-models.sh` selects "main models" by
`grep -qE '\bval[[:space:]]+inv_[A-Za-z0-9_]+'`, so this module is invisible to the PR-lane
`just quint-run` gate and to both existing `just quint-verify-<model>` sweeps, while
`just quint-check` (which globs every `.qnt`) still typechecks it. The safety model was never
edited; `quint test specs/quint/cluster_migration_failover.qnt` is still 77 passing.

### Properties

| Name | Claim |
|---|---|
| `temporal_no_stuck_handoff` | an open migration record does not stay open forever (`migrationOpen(s) leadsTo not(migrationOpen(s))`) — the eventually-form of the safety module's `inv_no_stuck_handoff`, which its own header lists under *Deliberately not modeled* |
| `temporal_barrier_eventually_disarms` | an armed handoff barrier eventually disarms — the fence-visible form of the same claim, true under mutations that break only the record lifecycle |
| `temporal_hold_eventually_releases` | each individual `(node, slot)` latch releases — the eventually-empties half of `inv_held_set_empty_while_latched`, whose docstring names it as a liveness property a Quint invariant cannot state. Phrased per-latch, not per-node-set: a node may legitimately latch a second slot before the first releases |
| `temporal_hold_region_eventually_empties` | the cluster is never *permanently* holding somewhere (aggregate form). Recorded as **expected-to-be-harder**: nothing in the fairness hypothesis bounds re-latching, so a counterexample here is a fairness-strength finding first, a model defect second |

Every property is asserted as `handoffFairness implies <claim>`. Without a fairness hypothesis all
four are trivially false by stuttering, so a "violation" of the unguarded form would be noise.

### Fairness hypothesis

```quint
val fairnessVars = (migrations, barriers, held, feed_bytes, defects, coverage)
temporal reconcileFair  = SLOTS.forall(s => reconcileTick(s).weakFair(fairnessVars))
temporal boundExitFair  = SLOTS.forall(s => boundAbort(s).strongFair(fairnessVars))
temporal handoffFairness = reconcileFair and boundExitFair
```

- **(F1) weak** fairness of `reconcileTick(s)` — the Raft leader keeps running its reconcile loop.
  `reconcileTick` is enabled whenever the record exists, so WF suffices.
- **(F2) strong** fairness of `boundAbort(s)` — WF is *not* enough: `applyReconcileTick` resets
  `observations` to 0 on `hasObservedProgress`, momentarily disabling `boundAbort`, so a weakly-fair
  bounded exit can be starved by an interleaving that sneaks a progress tick in before each abort.

Deliberately **not** assumed: fairness on `completeMigration`/`confirmDrained`/`abortHandoff`/
`cancelMigration` (would make the properties vacuous restatements of "the happy path fires"), on
`sourceWrite`/`applyAtTarget`/`feedBuffer` (client traffic is not something the protocol may
assume), or on any failover/membership action (a pruning failover also releases the barrier, so
assuming it would weaken the claims).

Naming is load-bearing: only `temporal temporal_*` declarations are picked up by the Justfile
recipe's `grep`. The fairness helpers deliberately lack that prefix so they are never submitted as
properties.

## Blocker 1 — Apalache rejects fairness outright

```
$ yes y | quint verify specs/quint/cluster_migration_failover_temporal.qnt \
    --main=cluster_migration_failover_temporal \
    --temporal=temporal_no_stuck_handoff --max-steps=3
  WARNING: Apalache has experimental support for temporal properties and might give incorrect results.
  Consider using --backend tlc, which fully supports temporal properties.

Do you want to proceed with Apalache anyway? (y/N) error: Handling fairness is not supported yet!
```

Apalache 0.56.1's `TemporalPass` refuses `WF_`/`SF_` before any solving happens (fails in a few
seconds, well before the SMT translation). There is no flag or encoding workaround — the feature is
simply absent from this release. Since every property here is `handoffFairness implies ...`, this
kills all four.

Note the **interactive prompt**: `quint verify --temporal` asks for confirmation even with a
non-tty stdin, so an unattended invocation blocks forever doing nothing. The Justfile recipe pipes
`yes y |` for exactly this reason — do not remove it, a bare invocation hangs the nightly.

## Blocker 2 — with fairness removed, Apalache crashes internally

A probe property with the fairness hypothesis stripped
(`SLOTS.forall(s => migrationOpen(s) leadsTo not(migrationOpen(s)))`, `--max-steps=3`) gets past
`TemporalPass` and dies inside the checker:

```
PASS #10: TransitionFinderPass
  > Found 1 initializing transitions
  > Found 168 transitions
...
PASS #13: BoundedChecker
State 0: Checking 1 state invariants
State 0: state invariant 0 holds.
Step 0: picking a transition out of 1 transition(s)
error: assertion failed
```

~32s wall. `--verbosity=5` surfaces no stack trace and `_apalache-out/*/detailed.log` contains no
exception — the message is all there is. Reproduced with a second, trivial probe
(`eventually(anyHoldLatched)`), so it is not specific to the `leadsTo` shape: Apalache's
temporal loop-finding encoding (the "Adding logic for loop finding" pass, which augments the state
with a saved-loop copy of all 14 variables) does not survive this model's 168 transitions.

So even a *fairness-free* (and therefore near-meaningless) form of these properties is unavailable
from the Apalache backend today. Both blockers are independent; fixing only the first would leave
the second.

## Blocker 3 — the TLC backend runs, checks, and cannot terminate

`--backend tlc` is what the tool's own warning recommends, and unlike Apalache it **does** support
fairness. The translation is correct — the generated TLA+ carries the fairness frame exactly as
intended:

```tla
reconcileFair == \A s_53 \in SLOTS: WF_fairnessVars(reconcileTick(s_53))
boundExitFair == \A s_63 \in SLOTS: SF_fairnessVars(boundAbort(s_63))
```

TLC accepts it and gets all the way to liveness checking:

```
PASS #5: TemporalPass
  > Rewriting temporal operators...
  > Found 1 temporal properties
  > Adding logic for loop finding
...
Checking 4 branches of temporal properties for the current state space with 5148 total distinct states
Finished checking temporal properties in 00s
Progress(7): 13,175,298 states generated (12,746,071 s/min),
             1,287 distinct states found (1,143 ds/min), 1,269 states left on queue.
```

What it cannot do is **finish**. The generated `.cfg` is only:

```
INIT q_init
NEXT q_step
PROPERTY q_temporalProps
```

`--max-steps` is silently ignored on the TLC path: quint's `dist/src/tlc.js` emits `INVARIANT` or
`PROPERTY` lines and never a `CONSTRAINT` or any other depth bound. TLC therefore does unbounded
BFS, and this model has unbounded counters (`observations`, offsets, `feed_bytes`), so the reachable
state space is infinite. Measured: a 900s-capped run with `--max-steps=3` was killed by the cap with
1,269 states still on the queue and the distinct-state count still climbing at ~1,143/min. Note the
generated:distinct ratio (13.2M : 1,287) — the fan-out is the ~168 transitions per state, so BFS
frontier growth is slow but perpetual.

The fix would be a hand-written `CONSTRAINT` (a state constraint bounding the counters) injected
into the `.cfg`, which quint offers no hook for. Doing it out-of-band would mean maintaining a
parallel TLA+/cfg pipeline outside `quint verify` — out of scope for T6's timebox, and it would
pin the checked artifact to a hand-edited translation that drifts from the `.qnt` silently.

**But this is still worth running nightly**, and it is what the lane defaults to. TLC checks the
temporal properties against the state graph it has *already* explored ("Checking 4 branches of
temporal properties for the current state space..."), and a fairness-violating lasso inside the
reachable subgraph is a genuine counterexample, not an artifact of truncation. So the lane is a
**time-capped liveness search**: `TIMED OUT (inconclusive)` is the expected outcome today, and a
`VIOLATED` would be a real finding worth triaging. Apalache, by contrast, yields literally no
information (`UNSUPPORTED` on all four), so it is not the default — it stays reachable as
`just quint-verify-temporal 6 900 apalache` for the day fairness support lands.

## Blocker 4 — no shrunk instance is available

The standard mitigation (verify a shrunk instance: 2 nodes, 1 slot) is **not available without
editing the frozen model**. `cluster_migration_failover_types.qnt` declares

```quint
pure val NODES = Set(1, 2, 3, 4)
pure val SLOTS = Set(1, 2, 3, 4)
```

as `pure val`, not `const`, so there is no `import spec(NODES = ..., SLOTS = ...)` instantiation
point. Parameterising them is a change to the safety model, which T6's constraints forbid (the
model is byte-frozen for the conformance harness and for concurrent issue-31 work).

This is worth recording as a **campaign follow-up**: converting `NODES`/`SLOTS` to `const` with a
concrete instance module for the existing gates would unlock shrunk-instance verification for both
the safety and the temporal tier. It is a mechanical change but a cross-cutting one (every existing
`--main` and every `quint run` invocation would move to the instance module), so it belongs in its
own task, not in T6.

## What was landed anyway, and why

1. **The properties themselves.** They are the design's liveness claims written down formally, in
   the language the rest of the model is written in, typechecked against the real state machine.
   They close two documented gaps (`inv_no_stuck_handoff`; the eventually-half of
   `inv_held_set_empty_while_latched`) and battery row M22 from "we cannot state this" to "stated,
   not yet checkable". A property that is written and unchecked is strictly better than one that is
   neither — it is reviewable, it is versioned next to the model, and it becomes checkable the
   moment tooling allows.
2. **The nightly lane, report-only.** `just quint-verify-temporal` (defaults: TLC backend, 900s per
   property) + a `verify-temporal` job in `quint-verify-nightly.yml` (`timeout-minutes: 90`, sized
   for the 4 × 900s worst case that is also the expected case). Report-only (`exit 0`,
   `::warning::` annotations) because a failing job would report "the tooling still cannot do this"
   every single night — noise, not signal. The recipe classifies outcomes distinctly (HOLDS /
   TIMED OUT / VIOLATED / UNSUPPORTED / BACKEND FAILURE) off `quint verify`'s own result markers
   rather than the exit code, since `verify` exits 1 both for a counterexample and for a backend
   that could not run — and telling "no verdict" apart from "counterexample" is the entire point
   here. Property names are discovered by `grep` over `temporal temporal_*`, so new properties are
   picked up automatically, and the day a backend gains the missing support the job starts
   producing full verdicts with **no rewiring**.

The lane mirrors the existing `quint-conformance-quarantine` pattern: a known-red surface kept
running so its flip to green is noticed.

## Recheck triggers

Revisit when any of these change:

- **Apalache release notes mention fairness / `WF_`/`SF_` support in `TemporalPass`.** The nightly
  auto-downloads whatever version quint pins, so a quint bump may fix this for free — check with
  `just quint-verify-temporal 6 900 apalache` and watch for `UNSUPPORTED by apalache` disappearing.
- **quint's TLC backend gains a depth bound / `CONSTRAINT` hook.** Then the existing default
  invocation goes from `TIMED OUT (inconclusive)` to a real verdict with no change here.
- **The nightly summary reports `VIOLATED`.** That is a genuine lasso in the explored subgraph, not
  a truncation artifact — triage it as a real liveness finding (or as a fairness-hypothesis
  finding, especially for `temporal_hold_region_eventually_empties`, see its note above).
- **`NODES`/`SLOTS` become `const`** (the follow-up above). A 2-node/1-slot instance would shrink
  168 transitions by roughly an order of magnitude, which may also be enough for blocker 2's
  loop-finding crash to stop reproducing.

## Commands used (reproduction)

```bash
eval "$(mise activate bash)"

# typecheck (passes)
quint typecheck specs/quint/cluster_migration_failover_temporal.qnt

# blocker 1 — fairness rejected (note the `yes y`: --temporal prompts even on a non-tty stdin)
yes y | quint verify specs/quint/cluster_migration_failover_temporal.qnt \
  --main=cluster_migration_failover_temporal \
  --temporal=temporal_no_stuck_handoff --max-steps=3

# blocker 3 — TLC, no depth bound, does not terminate
yes y | timeout -k 30 900 quint verify specs/quint/cluster_migration_failover_temporal.qnt \
  --main=cluster_migration_failover_temporal \
  --temporal=temporal_no_stuck_handoff --max-steps=3 --backend tlc

# the lane itself (report-only, always exits 0)
just quint-verify-temporal                 # nightly defaults: TLC, 900s/property
just quint-verify-temporal 3 150 apalache  # quick smoke — 4 x UNSUPPORTED in ~2 min
```

Both classification paths were smoke-tested at the end of T6, each ending in a printed summary
block and recipe exit 0:

- `just quint-verify-temporal 3 150 apalache` → 4 × `UNSUPPORTED by apalache` (~2 min).
- `just quint-verify-temporal 3 45 tlc` → 4 × `TIMED OUT (inconclusive)` (the `timeout`-124 path,
  which is what the nightly will report until a backend can finish).

Note `quint verify` drops an `_apalache-out/` directory into the cwd; it was not gitignored before
this task (added in the same commit).
