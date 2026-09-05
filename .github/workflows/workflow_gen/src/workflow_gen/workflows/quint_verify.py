"""Nightly Apalache (exhaustive, bounded) verification of the Quint design models.

`quint-run` (Justfile, wired into the PR lane via workflows/test.py) drives the
same models cheaply on every PR: `quint test` plus a sampled `quint run`.
Sampling only visits paths its RNG happens to pick, though, so it can miss a
violation that only shows up on a specific interleaving. `quint verify` uses
Apalache (SMT-based bounded model checking) to explore *every* reachable state
and enabled transition up to a step bound instead of sampling — the exhaustive
tier the design doc calls for
(.scratch/formal-spec/2026-08-12-formal-state-spec-design.md §3: "quint verify
... in the nightly lane alongside the existing model-check and seed
nightlies"). Per-state SMT solve cost grows steeply with depth, so this runs
minutes-to-hours per invariant rather than the PR lane's sub-10s budget —
that's why it's nightly, not per-PR.

MAX_STEPS defaults to >= 6 by the Justfile's `quint-verify-model` recipe,
which refuses a lower bound itself; nothing here overrides it (both jobs
below invoke their `just quint-verify-<model>` wrapper with no arguments).
That floor is CARRIED REQUIREMENT N1 from the phase-2 cluster quint plan
(.superpowers/sdd/2026-08-13-phase2-cluster-quint-plan/task-2-report.md and
progress.md): depth 3 was proven vacuous for `inv_repatriating_well_formed`
and half of `inv_abort_repatriates` on cluster_migration_failover.qnt — both
need 4-5 transitions before the property is even checkable. Do not shrink the
bound to make a run finish faster. It is a floor, not a target: the two
`quint-verify-<model>` wrappers pin different defaults above it (admission
10, migration/failover 6 — see their own Justfile docstrings for why), so a
model that can afford deeper search gets it. See the Justfile recipe's own
docstring for the full rationale, including why a timed-out invariant is
reported as inconclusive (a `::warning::` annotation, does not fail the job)
while a genuine violation is reported as `::error::` and does fail it.

Each model gets its own job (and its own `just quint-verify-<model>` Justfile
target) rather than one combined sweep — the fallback the plan calls for if
depth 6 turns out SMT-infeasible for some invariant. cluster_migration_failover.qnt
has 13 invariants against cluster_admission.qnt's 4, so splitting means a slow
invariant on the heavier model can't eat the lighter model's time budget (Task
2 found depth 6 already SMT-infeasible for a single migration/failover
invariant within a 240s budget, while the admission model verified two
invariants clean at depth 6 in 10-27s each). Within a job, the Justfile
recipe still runs one `quint verify` invocation per invariant under its own
timeout, so one SMT-infeasible invariant is reported as a timeout rather than
hiding the rest of that model's results.

`quint verify` auto-downloads Apalache on first use, but Apalache itself needs
a JVM already on PATH. jepsen_nightly.py gets Java through mise (temurin-21,
pinned in .mise.toml) because that job already needs mise for Leiningen; this
job has no other use for mise's Java plugin, so it installs the JVM directly
via actions/setup-java instead.

A third job, `verify-temporal`, checks the migration/failover model's
**liveness** properties (`specs/quint/cluster_migration_failover_temporal.qnt`,
quint-completeness campaign T6) rather than its safety invariants. It is
report-only: `just quint-verify-temporal` always exits 0 and annotates its
verdicts as `::warning::`. The reason is measured, not defensive — Apalache
0.56.1 refuses fairness outright (`Handling fairness is not supported yet!`)
and its temporal loop-finding encoding crashes on this model with an internal
`assertion failed` even with the fairness hypothesis removed, while the TLC
backend accepts the fairness hypothesis but cannot terminate (quint's TLC path
emits no depth bound and this model has unbounded counters). Every property in
that module is asserted under an explicit fairness hypothesis (without one they
are trivially false by stuttering), so no *complete* verdict is obtainable
today. The recipe defaults to TLC anyway because TLC checks the properties
against the state graph it has already explored, which makes this a time-capped
liveness search: inconclusive when the cap expires (the expected nightly
outcome), but a genuine counterexample when it fires. Full write-up:
.scratch/formal-spec/t6-findings.md. This job is the only sanctioned caller of
`quint verify --temporal` — the campaign ruling scopes it to the nightly lane,
never the dev loop or a commit gate.

A fourth job, `walk-steered`, runs the **steered sampled walk** (`just
quint-run-steered`, campaign ruling R11). It is not an Apalache lane at all —
it is `quint run` with the sampler pointed at a model's `stepSteered` relation
instead of its flat `step`, at a deeper budget (500x40 over four pinned seeds)
than the PR lane can afford. Steering changes only the sampling *distribution*
(the steered relation groups the same actions and gates cheap churn behind a
coin), so every steered trace is a legal trace of `step` and a red cell is a
real counterexample. It sits here rather than in the PR lane deliberately:
`quint-run`, the witness-floor gate and `quint verify` stay on the flat `step`
so their verdicts are not coupled to sampler tuning. It earns its slot — the
uniform PR lane had been green for months over issue 41's residue family while
the steered walk found three real defects (R8/R9a/R9b) in one pass. No JVM
involved, so no setup-java step, and it finishes in minutes rather than hours.
"""

from workflow_gen.helpers import (
    change_gate_job,
    checkout_step,
    mise_setup_step,
    run_step,
    script,
    setup_java_step,
)
from workflow_gen.schema import Job, ScheduleTrigger, Trigger, Workflow

# `node` first: the `npm:` backend depends on it and mise >= 2026.8.11 enforces that
# (jdx/mise#12234) — see .scratch/build-toolchain/issues/done/04-ci-mise-npm-backend-node-dependency.md.
MISE_JUST_QUINT = "just node npm:@informalsystems/quint"

# Free unmetered GitHub-hosted runner — same reasoning as the other nightlies
# in this package (single-machine SMT solve, no need for anything heavier).
RUNS_ON = "ubuntu-latest"

WORKFLOW_FILE = "quint-verify-nightly.yml"

# 05:05 UTC: off the hour (avoids the Actions cron spike at :00) and
# staggered away from the other nightlies' cron times (see each module's own
# NIGHTLY_CRON comment for the full set already in use).
NIGHTLY_CRON = "5 5 * * *"


def quint_verify_workflow() -> Workflow:
    w = Workflow(
        name="Quint Verify Nightly",
        on=Trigger(
            schedule=ScheduleTrigger(cron=[NIGHTLY_CRON]),
            workflow_dispatch=True,
        ),
    )

    gate = w.job("gate", change_gate_job(workflow_file=WORKFLOW_FILE))

    w.job(
        "verify-admission",
        Job(
            name="Apalache Verify: cluster_admission",
            runs_on=RUNS_ON,
            needs=gate,
            if_="needs.gate.outputs.skip != 'true'",
            # 4 invariants, up to the recipe's 1200s (20min) default TIMEOUT
            # each in the worst case, plus setup — comfortably under the
            # 360-minute GitHub Actions hosted-runner job cap.
            timeout_minutes=120,
            steps=[
                checkout_step(),
                mise_setup_step(install_args=MISE_JUST_QUINT),
                setup_java_step(),
                run_step(
                    # quint-verify-admission defaults MAX_STEPS to 10, above
                    # the N1 floor of 6 (see its Justfile docstring).
                    name="Apalache verify: cluster_admission.qnt (all invariants, depth 10)",
                    run=script("""\
                        just quint-verify-admission
                    """),
                ),
            ],
        ),
    )

    w.job(
        "verify-migration-failover",
        Job(
            name="Apalache Verify: cluster_migration_failover",
            runs_on=RUNS_ON,
            needs=gate,
            if_="needs.gate.outputs.skip != 'true'",
            # 13 invariants at up to 1200s each is a genuine ~260min (~4.3h)
            # worst case — Task 2 found depth 6 already SMT-infeasible for a
            # single invariant on this heavier model within a 240s budget, so
            # a run where several invariants time out rather than finish fast
            # is expected, not a bug (each is reported as an inconclusive
            # `::warning::`, not a job failure — see the Justfile recipe's
            # docstring). timeout-minutes is sized to that worst case (and to
            # stay under the 360-minute hosted-runner cap), not to the
            # expected runtime.
            timeout_minutes=300,
            steps=[
                checkout_step(),
                mise_setup_step(install_args=MISE_JUST_QUINT),
                setup_java_step(),
                run_step(
                    name="Apalache verify: cluster_migration_failover.qnt (all invariants, depth >= 6)",
                    run=script("""\
                        just quint-verify-migration-failover
                    """),
                ),
            ],
        ),
    )

    w.job(
        "verify-temporal",
        Job(
            name="Verify: migration liveness (temporal, report-only)",
            runs_on=RUNS_ON,
            needs=gate,
            if_="needs.gate.outputs.skip != 'true'",
            # 4 temporal properties at the recipe's 900s (15min) default
            # TIMEOUT each is a 60-minute worst case, plus setup. On the TLC
            # default that worst case IS the expected runtime: the search
            # cannot terminate on this model, so every property runs out its
            # cap doing useful (if incomplete) liveness checking and is
            # reported as inconclusive.
            timeout_minutes=90,
            steps=[
                checkout_step(),
                mise_setup_step(install_args=MISE_JUST_QUINT),
                setup_java_step(),
                run_step(
                    name="TLC verify: cluster_migration_failover_temporal.qnt (liveness, report-only)",
                    run=script("""\
                        just quint-verify-temporal
                    """),
                ),
            ],
        ),
    )

    w.job(
        "walk-steered",
        Job(
            name="Steered walk: sampled invariants (deep budget)",
            runs_on=RUNS_ON,
            needs=gate,
            if_="needs.gate.outputs.skip != 'true'",
            # Sampling, not SMT: 13 invariants x 4 seeds at 500x40 runs in
            # single-digit minutes on the migration/failover model. The cap is
            # generous headroom for a model growing invariants, not a sizing
            # estimate. A violation fails the job — unlike the temporal lane,
            # this one produces conclusive verdicts.
            timeout_minutes=60,
            steps=[
                checkout_step(),
                mise_setup_step(install_args=MISE_JUST_QUINT),
                run_step(
                    name="Steered sampled walk: every model declaring stepSteered",
                    run=script("""\
                        just quint-run-steered
                    """),
                ),
            ],
        ),
    )

    return w
