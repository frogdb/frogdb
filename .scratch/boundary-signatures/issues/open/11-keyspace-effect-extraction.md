# `KeyspaceEffect` extraction (approach B): one mutation description for four consumers

Status: needs-triage

Size: L

**Deferred; not scheduled; filed so the decision is recorded.** Approach B was weighed against
approach A (the derived signature + lint, which is what issues 01–09 in this directory build) in
the 2026-09-02 design session and deferred — see the A/B/C row in
[PRD §9](../../PRD.md#9-decisions-log-design-session-2026-09-02) and
[PRD §10](../../PRD.md#10-non-goals-and-deferred-work-filed-as-issues). `signature.rs` (issue 01
in this directory) is its seed: the classification function this issue would materialize into a
real type.

## Why

A single "keyspace effect" enum is latent in `CommandSpec` but never extracted. `WalStrategy`,
`ReindexSpec` and the event shape all mirror the same distinctions — `ReindexSpec`'s own doc
comment notes it mirrors `WalStrategy` variant-for-variant — so the same fact about a command is
declared three or four times, once per consumer, with nothing forcing the copies to agree
([PRD §2](../../PRD.md#2-what-exists-today-findings-2026-09-02)).

Replication is the sharp edge: it has no per-command classifier at all, deriving what to ship
from `CommandFlags` alone, which round-1's PRD identified as a source of statement-shipping bugs.

Materializing `KeyspaceEffect` and having persistence, replication, reindex and notification all
consume one mutation description removes the copies and gives replication the classifier it
lacks.

## What to build

Materialize `KeyspaceEffect` as the single description of what a command does to the keyspace,
and convert the four consumers — persistence (WAL), replication, reindex, keyspace notification —
to read it instead of their own parallel projection.

**This is spec-first work across four LOCKED crates.** It touches `frogdb-persistence` /
`frogdb-recovery`, `frogdb-replication` / `frogdb-replication-runtime`, `frogdb-cluster` /
`frogdb-cluster-runtime` and `frogdb-txn` / `frogdb-vll` territory, so the locked-area discipline
applies in full (see `CLAUDE.md` "Locked core areas" and ADRs `adr/0002`–`0004`):

- Behavior changes go **spec row first** → failing forcing test → fix. Any behavior this
  extraction changes (especially replication's shipping decisions) needs its `FM-<AREA>-NNN` row
  in the owning `specs/<area>.md` before the code moves.
- `just lint-spec` must stay green throughout; forcing tests live in the mutated crate.
- `just mutants-diff <crate>` on every touched locked crate before pushing.

Unlike issues 01–09, this **does** change engine behavior — issues 01–09 add a projection and
tests only ([PRD §10](../../PRD.md#10-non-goals-and-deferred-work-filed-as-issues)). That is the
reason it was deferred rather than folded in.

Open questions for triage before this is scheduled:

- Whether `KeyspaceEffect` subsumes `WalStrategy` and `ReindexSpec` outright or sits above them
  as the shared description they are both derived from.
- What happens to `BoundarySignature::persist`, which currently derives from `WalStrategy` — it
  presumably derives from `KeyspaceEffect` afterwards, and the census must not silently change
  shape underneath `specs/signatures.md`.
- Whether the crate split (issue 10 in this directory) should land first, since it moves the same
  types.

## Acceptance criteria

- [ ] Triaged: scheduled with an owner, or closed `wontfix` with the reason recorded
- [ ] If scheduled: spec rows land before code in every touched locked area, each with a forcing
      test in the owning crate; `just lint-spec` green
- [ ] If scheduled: persistence, replication, reindex and notification all consume one
      `KeyspaceEffect` description; the duplicated `WalStrategy`/`ReindexSpec` mirroring is gone
- [ ] If scheduled: replication has a real per-command classifier, with rows covering the
      statement-shipping cases it previously derived from flags alone
- [ ] If scheduled: `just mutants-diff` triaged on each touched locked crate; the census and
      `specs/signatures.md` are re-derived and agree (`just docs-gen-check`, `just lint-spec`)

## Blocked by

Issue 01 in this directory (`signature.rs` seeds the classification). Deferred beyond that — not
scheduled.
