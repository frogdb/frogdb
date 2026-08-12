# Formal specification for FrogDB state — design

Date: 2026-08-12
Status: draft — awaiting user review
Origin: brainstorming session on "we keep discovering failure modes in implementation and
wondering what to do; we need a formal specification for cluster and replication state."

## Problem

FrogDB's correctness artifacts today are all *reactive* or *derived*:

- The six failure-mode specs (`.scratch/hardening/specs/*-failure-modes.md`) catalog observed
  modes with forcing tests — they record what the system does, discovered one bug at a time.
- The `INV-*` invariant catalogs (in `frogdb-cluster`, `frogdb-replication`) check predicates
  at hook points, but the predicates were reverse-engineered from defects.
- The stateright models, proptests, seeded turmoil sweeps, and jepsen suites explore behavior,
  but none of them *defines* what behavior is legal.

The closest thing to a constructive statement is the original design-spec lineage
(`spec/*.md` from 2026-01, now living as `website/src/content/docs/architecture/*.md` —
notably `clustering.md`, `replication.md`, and `consistency.md` with its
`[Tested]`/`[Design intent]` tags). Those documents do describe intended state and
transition rules, but as narrative prose: no row ids, no forcing-test links, no lint, and
no update discipline tying them to behavior changes — so they drift silently and cannot
settle a ruling. Nothing *lintable* states: what state exists, who owns each piece, and
which transitions are legal under which preconditions. The cost is a standing queue of design questions (24
pending rulings at the time of writing — parent validation, migration×failover composition,
barrier timing, membership admission, identity×dataset lifetimes) that each had to be
*discovered* in implementation before anyone could ask them. Two structural lessons from the
retro-validation gates sharpen the requirement:

- **Projection blindness** — validation layers only see what their projection carries
  (`ReplicationView` misses payload contents and byte tallies), so a spec must define the
  authoritative state, not a view of it.
- **Transcription weakness** (replication issue 26) — a model that re-states implementation
  control flow proves only the transcription; when the implementation stops consulting the
  mechanism the model still passes.

## Decisions (settled during brainstorming)

1. **Layered spec**: a constructive, human-readable authority document per area, plus
   machine-checked models for the highest-risk compositions.
2. **Absorb, all six areas**: the new spec format absorbs the existing failure-mode specs
   for all six areas (cluster, replication, persistence, txn, vll, blocking) — one authority
   per area, no sibling documents that could disagree. Migration is phased per area.
3. **Rule first, then spec**: the 24 pending rulings are settled as a standalone exercise
   *before* cluster/replication spec drafting begins; the spec transcribes the decisions.
4. **Website**: specs are published as documentation on the website, generated from the
   single source in the repo.
5. **Quint tooling**: install and use the Quint agent skills (`quint-lang`,
   `quint-modeling` from `quint-co/quint`) before authoring any `.qnt` file; adopt
   `quint-connect` for cluster conformance testing (scoped, pinned — see §4).

## Architecture: four verification layers, one authority

| Layer | Artifact / tool | What it checks | Exhaustive? |
|---|---|---|---|
| Authority | `specs/<area>.md` (markdown, linted) | Defines state + legal transitions; humans and agents rule from it | n/a |
| Design | Quint models + `quint verify` (Apalache) | The spec itself cannot reach a bad state; composition holes | Bounded-exhaustive / symbolic, on the model |
| Conformance | quint-connect trace replay | Implementation state == model state at every step of generated traces | No — sampled traces + named scenarios |
| Impl interleaving | stateright impl-driving models | Production `apply_command` under exhaustive orderings | Explicit-state, within bounds |
| Async runtime | turmoil seeded sweeps, jepsen | End-to-end behavior under faults | No — seeded sampling |

The design layer is the one FrogDB lacks entirely today: it finds composition holes *before*
implementation, and its counterexamples become failure-mode rows instead of production
surprises. Each other layer keeps a job no other layer can do.

## §1 Spec format and layout

**Location**: `specs/` at the repository root (promoted out of `.scratch` — the spec is a
first-class artifact and the website generator reads it). Files:

```
specs/
  cluster.md
  replication.md
  persistence.md
  txn.md
  vll.md
  blocking.md
  composition.md        # cross-area interactions only
  quint/                # design models (§3)
    *.qnt
```

**Per-area file structure**, in order:

1. **State space** — every state variable, typed: what it is, which struct field(s) in the
   implementation are authoritative for it, who may write it, where it is persisted, and
   what survives a restart. This section is the projection-completeness anchor: anything a
   transition's postcondition mentions must be declared here with its authoritative source.
2. **Transitions** — `TR-<AREA>-NNN` rows. One row per action: each `ClusterCommand` arm,
   each replication session event, each detector tick, each recovery step. Each row states
   precondition → postcondition over §1 variables. This section is *constructive*: it
   defines legal, not observed.
3. **Invariants** — the existing `INV-*` entries absorbed as predicates over §1 variables.
   Each cites the transitions that preserve it. The Rust invariant catalog (hook-checked)
   remains the runtime enforcement; the spec section is the authority it cites.
4. **Liveness** — `LV-<AREA>-NNN` progress properties: "if X holds continuously, Y
   eventually happens" (the issue-18 class: a missed failover must eventually be retried;
   a migration must eventually complete or cancel; a held feed gate must eventually
   release). TR/INV alone state only safety. LV rows are checked as temporal properties
   (with fairness assumptions) in the Quint models that cover them, and forced at the
   implementation level by turmoil quiesce/eventually tests — the seeded sweeps' quiesce
   check is the existing liveness oracle, now with named rows to force.
5. **Failure modes** — the existing `FM-<AREA>-NNN` rows absorbed verbatim, IDs unchanged,
   forcing-test tags unchanged. Each row gains one field: the `TR-` id(s) it perturbs.
6. **Composition pointers** — links into `specs/composition.md` for interactions that leave
   the area (e.g. handoff barrier × replica feed gate, checkpoint drain × `WAIT`).

**Why this formalism.** TR/INV is the canonical state-machine specification form — the same
one TLA+ and Quint use (state variables; named actions as guard → postcondition; invariants;
temporal properties). The mapping into Quint is one-to-one: §1 variables → `var`
declarations, TR rows → named `action` definitions, INV rows → checked invariants, LV rows →
temporal properties, CO rows → a model importing both areas' modules. The markdown spec is
therefore "Quint in prose plus forcing-test citations", and authoring a model from a
finished spec section is largely mechanical. FM rows are the one part outside the
formalism: negative-space scenario/property pairs that carry the forcing-test lint
discipline — the bridge to the existing test infrastructure. Alternatives considered and
rejected: Alloy (relational logic — strong on topology snapshots, wrong shape for
protocol traces), the P language (good async state-machine fit but no verify-grade checker,
no Rust bridge, new toolchain), Event-B-style refinement chains (heavier than the problem
warrants).

`specs/composition.md` holds cross-area interaction rules in the same TR/invariant style,
with a `CO-NNN` id space.

**LOCKED status carries over.** Absorbed areas keep the spec-first discipline: behavior
changes edit the row (now possibly a TR row), update the forcing test, then the code.
Mutation gates are untouched by the migration.

## §2 Lint

`scripts/failure-modes.py` evolves into the spec linter. The recipe is renamed
`just lint-spec` outright — no transitional alias; every Justfile, CI, lefthook, and
documentation reference to `lint-failure-modes` is updated in the same change (FrogDB is
pre-production; no backwards compatibility is kept). The linter checks:

- Every `TR-` row names at least one forcing test; every tagged test matches a row
  (both directions, as today for FM rows). `LV-` rows likewise name their forcing
  turmoil/eventually tests.
- Every `INV-` entry cites existing `TR-` ids; every `FM-` row cites the `TR-` it perturbs.
- No dangling references (`TR-`/`INV-`/`LV-`/`FM-`/`CO-` ids must resolve), including from
  `.qnt` model headers (§3).
- State-space completeness: every state variable mentioned in a TR pre/postcondition is
  declared in §1 of the same file (mechanical string-level check).

Runs where the current lint runs: the compile-free subset in lefthook on every commit, the
full family in `just lint` and CI.

## §3 Quint design models

**Scope — high-risk compositions first**, not a model per area:

- migration × failover (the issue 15/16/17/20 class)
- slot-handoff barrier timing (rework issue 02/03 class)
- membership admission window (cluster issue 25 class: solo-bootstrap usurper)
- replica feed gate (replication issue 26 class)

**Admission rule for further models.** A new Quint model is added when a composition
question or ruling arises that the example-based layers cannot settle — models earn their
slot by a defect class, not by completeness aesthetics. This is a yield judgment, not a
capability limit: since spec sections are Quint-shaped (§1), writing a model for a
finished spec area is largely mechanical, so deferring loses nothing permanently. The
initial four are where the campaigns' real defects clustered. Known reasons an area may
*never* earn one: representation-level failure modes (persistence byte formats, CRC,
truncation — model checking abstracts away exactly those bugs; fuzzing and the crash
harness own them) and wall-clock quantities (Quint models time as ordering, not
durations — right for "Complete lags the barrier", unable to check a millisecond budget).
Visible future candidates: VLL deadlock-freedom (clean model, low marginal yield today
given the 0.90 mutation gate and shuttle coverage) and persistence *recovery ordering*
(earns a model if recovery rulings recur).

Each `.qnt` file header cites the spec `TR-`/`INV-`/`CO-` ids it models; the linter checks
the citations resolve. Counterexamples found by checking are triaged like defects: ruling →
spec row → forcing test (or model fix, if the model was wrong).

**Cadence**: `quint typecheck` + a short `quint run` smoke on every CI run; `quint verify`
(Apalache backend, bounded/symbolic) in the nightly lane alongside the existing model-check
and seed nightlies. Bounded verification is a known limit: depth-k unless we author
inductive invariants, which we only do if a specific model earns it.

**Toolchain**: Quint CLI via npm (`quint`, currently v1.2.0) — added to `.mise.toml`
(npm backend). Apalache is auto-downloaded by `quint verify` (JVM + Z3); the nightly runner
needs a JVM. Quint agent skills (`quint-lang`, `quint-modeling`) are installed
project-scoped into `.claude/skills` (via `skills/install.sh` from `quint-co/quint` or the
plugin marketplace) as an implementation-phase step *before* any `.qnt` is authored.

**Drift honesty**: Quint models are a separate artifact from the implementation. Where a
quint-connect driver exists (cluster, §4) the drift is machine-checked. Where it does not
(replication session — async), the mitigations are: spec-first discipline (a behavior
change edits spec + model before code), keeping those models small, and the existing
turmoil forcing tests. This is a known, accepted gap, revisited when quint-connect grows
an async story.

## §4 quint-connect (evaluated — adopt, scoped)

**What it is** (v0.1.2, crates.io, Apache-2.0, Informal Systems / quint-co): trace-replay
conformance testing. Quint generates traces (`quint run` simulation or named spec tests);
proc-macros (`#[quint_test]`, `#[quint_run]`) replay each trace step against a Rust
implementation through a synchronous `Driver::step`; after every step the implementation's
state is projected via `State::from_driver` and diffed against the model's expected state.

**Verdict: adopt for the cluster state machine only.**

- `ClusterState::apply_to` is a synchronous, deterministic state-machine apply — an ideal
  `Driver` target. The named-actions restriction costs nothing because our specs are
  authored fresh. `QUINT_SEED` gives reproducible failures.
- Not adopted for the replication session or cluster runtime: `Driver::step` is sync-only
  with no documented async story; those stay with stateright/turmoil.

**Risks and guards**:

- *Pre-1.0 API churn*: accepted without mitigation (per ruling: don't worry about churn or
  backwards compatibility). Adopt the current API in one dev-dependency test target in
  `frogdb-cluster`; if upstream changes, adjust the harness then.
- *Serde friction*: Quint sum types serialize as `{tag, value}`; the Rust mirror types need
  `#[serde(tag = "tag", content = "value")]`. Contained in the test target.
- *Projection blindness (the structural risk)*: `State::from_driver` is a projection, and an
  omitted field makes divergence invisible — exactly the retro-gate failure mode. Guard:
  the spec's state-space section (§1.1) declares the authoritative struct fields per spec
  variable, and review requires `from_driver` to cover every variable any modeled TR's
  postcondition touches. The linter's completeness check makes the omission at least
  visible at the spec level.
- *Sampling, not proof*: trace replay checks sampled trajectories. Exhaustive exploration
  of the implementation remains stateright's job (§5).

## §5 Stateright policy

Two classes exist today; the design keeps one and bans the other.

- **Keep — impl-driving models** (handoff, failover composite): the transition function *is*
  production `apply_command`; the checker exhaustively explores real-code interleavings
  (23M states in the nightly failover model). Neither Quint (checks the model) nor
  quint-connect (replays linear traces) does this. These are re-labeled *implementation
  exploration suites* in the spec's terms and their headers cite the TR ids they exercise.
- **Ban — transcription models**: a stateright model that re-states implementation control
  flow instead of calling it proves only the transcription (issue 26). Design rule: a
  stateright model must call production code; a model of *design intent* is written in
  Quint instead. The existing feed-gate model is the candidate for retirement under this
  rule, pending the issue-26 ruling — this design does not pre-empt that ruling.

## §6 Website publishing

Single source of truth is `specs/*.md`. A generator script (consistent with the repo's
codegen rule: edit generators, not generated files) injects Astro/Starlight frontmatter and
copies the spec files into the website content collection as a "Specifications" section,
wired into the existing docs-generation `just` recipes. No hand-edited copies; the
generated output is refreshed by the same flow that regenerates other docs.

**Relationship to the existing architecture docs.** The website already carries the
original design-spec lineage as `architecture/*.md` (clustering, replication, consistency,
persistence, vll, ...), and those pages make normative claims. Two authorities cannot
coexist (the DRY documentation rule). Resolution: the generated Specifications section is
normative; as each area's spec lands (§7 phases), that area's architecture page is
rewritten as a narrative overview — the "why and how it fits together" reading — with its
normative claims replaced by links to the spec rows that now own them. `consistency.md`'s
`[Tested]`/`[Design intent]` distinction dissolves into the spec, where every row carries a
forcing test or is an explicit hole.

## §7 Migration sequencing

Phased per area, each phase leaving the tree green (lint, mutation gates, full suite):

1. **Scaffolding** — `specs/` layout, linter evolution, website generator, Quint toolchain
   in `.mise.toml`, Quint skills installed. No content migration yet.
2. **Cluster** — starts only after the pending rulings are settled (decision 3). Write
   state space + TR rows constructively, absorb FM rows and INV text, land the first two
   Quint models (migration×failover, admission window) and the quint-connect harness.
3. **Replication** — same shape; barrier-timing and feed-gate Quint models land here.
4. **Persistence.**
5. **Txn + VLL.**
6. **Blocking.**

Each area's absorption is mechanical where possible (FM rows move with IDs and tags
unchanged) so `just lint-spec` stays green mid-migration; the constructive sections are the
new writing.

## §8 Spec drafting method (how the content gets defined)

Per area, the constructive sections are produced in a fixed order. Three inputs with
distinct roles: the code is ground truth for *what exists*; the settled rulings and review
gates are ground truth for *what is legal*; and the historical design docs
(`website/src/content/docs/architecture/*.md`, plus their pre-Raft generations retrievable
from git history, e.g. `git show 5b23c4da^:docs/spec/CLUSTER.md`) are the record of
*intent* — drafting agents mine them for transition rules and guarantees the code never
made explicit, flagging any code↔doc disagreement as a ruling rather than silently siding
with either:

1. **State-space extraction** — enumerate the authoritative structs from the code
   (e.g. `ClusterState` fields, per-node epoch, runtime flags, replication session state,
   WAL/checkpoint records) and draft the §1 table: variable, authoritative field, writer,
   persistence, restart survival. Drafted by agents from source; ownership and persistence
   claims are human-reviewed — this table is the projection-completeness anchor and must
   not be guessed.
2. **Transition enumeration** — mechanically list every action source (each
   `ClusterCommand` arm, each replication session event, each detector tick, each recovery
   step) and draft one `TR-` row per action: precondition → postcondition over §1
   variables. Inputs: the code, the existing FM rows, and the settled rulings (the
   rule-first exercise feeds directly here).
3. **Absorption** — move the FM rows and INV entries in unchanged and add their `TR-`
   citations.
4. **Gap pass** — two mechanical sweeps that turn absorption into discovery: an FM row
   whose behavior no TR produces means a missing transition (write it); an INV no cited TR
   preserves means a spec hole (ruling). Gaps found here are the cheap version of what
   used to be found in production.
5. **Quint models** — authored from the finished spec sections (the `quint-modeling`
   skill's from-requirements flow), *not* from the code — the transcription ban applied at
   the design layer. Counterexamples loop back as rulings → TR/FM edits.
6. **Review gates** — the user reviews each area's state-space and transition sections
   (the authority content); mechanical absorption gets a lighter diff review.

## Testing and CI summary

- `just lint-spec` (evolved failure-modes lint) — every commit (lefthook subset) + CI.
- `quint typecheck` + smoke `quint run` — CI on changes touching `specs/quint/`.
- `quint verify` — nightly lane.
- quint-connect trace tests — default test suite of `frogdb-cluster` (fast traces), larger
  simulation counts nightly.
- Existing stateright smoke/nightly, seeded sweeps, jepsen — unchanged.

## Out of scope

- Settling the 24 pending rulings (explicitly sequenced before spec drafting, not part of
  this design).
- Any change to mutation gates, the frozen redis-regression suite, or the operator/frogctl.
- Inductive-invariant authoring for unbounded verification (only if a model earns it).
- Async quint-connect drivers (revisit when upstream has a story).
