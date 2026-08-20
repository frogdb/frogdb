# Quint design models

Executable design models for the locked areas, checked by four lanes. The models
themselves are cited from the area specs — [cluster](../cluster.md),
[replication](../replication.md) — which remain the authority; these files are the
design layer under them.

Each model is several files: `*_types.qnt` (types + constants), `*_logic.qnt` (pure
guards and updates), `*_machine.qnt` (`var`s + actions + `step`), and a main module
that imports the rest and owns the witnesses, invariants and `run` tests. Only the main
module is runnable; `scripts/quint-models.sh` derives that set from "declares at least
one `val inv_*`".

| Lane | Command | Cadence |
|---|---|---|
| Type-check every file | `just quint-check` | every commit |
| `quint test` + sampled invariants | `just quint-run` | PR |
| Witness floor | `just quint-witness-gate` (also inside `quint-run`) | PR |
| Bounded exhaustive check (Apalache) | `just quint-verify-model <model>` | nightly |

The Justfile recipes carry the details for each; read them before changing a lane.

## Witness floor

A `val witness*` states that some behaviour is **reachable**. `just quint-run` fails if
any declared witness is observed in **0** traces.

The gate exists because the invariant and `run`-test lanes are blind, *by construction*,
to one class of edit: an action unwired from `step`. Invariants are safety predicates
over reachable states, and deleting a transition can only shrink the reachable set — no
safety property is falsifiable by removing behaviour; and the `run` tests drive actions
directly, so they never consult `step` at all. The mutation batteries under
`.scratch/formal-spec/` measured this on live rows (admission A57/A58, feed-gate M48/M49,
fullsync M112–M114): every oracle stayed green and the only signal was a witness count
collapsing to zero — for fullsync M114, one witness zeroed and six unrelated ones fell by
half or more.

Mechanics live in `scripts/quint-witness-gate.sh` (witness names come from
`scripts/quint-witnesses.sh`, the witness-lane twin of `quint-invariants.sh`). Sampling
is random, so the budget and seeds are **pinned** — 2000 samples × 40 steps over seeds
`0x1` and `0x2`, the batteries' convention — and CI and a laptop therefore see identical
counts for the same tree. A witness passes if it is observed under any pinned seed.
`QUINT_WITNESS_SAMPLES` / `QUINT_WITNESS_STEPS` / `QUINT_WITNESS_SEEDS` override the
budget for a hand-run escalation.

### Exemptions

Some witnesses are legitimately unreachable under sampling. They are listed one per line,
with a mandatory written reason, in
[`witness-floor-exemptions.txt`](witness-floor-exemptions.txt):

```
<model path> <witnessName> <reason>
```

Three kinds qualify, and the reason must say which:

- **Unreachable by construction** — the model states a state a superseded design revision
  could reach, so that reverting the narrowing makes the witness fire. The reason names
  that revert. (Both `witnessDrainingWedged*` in the migration model.)
- **Reachable but deep** — a long, specific choice sequence a uniform walk essentially
  never takes. The reason names the deterministic `run` test that carries it instead.
- **No witnesses at all** — a main model that declares none is the same hole one size
  larger, so it is written down the same way, with `*` in the witness field. Exactly one
  qualifies today: `lib_selftest.qnt`, a test model for the pure helper modules that
  models no FrogDB behaviour. The row goes stale the moment the model grows a witness.

There is no way to disable the gate, and no wildcard over witnesses — `*` names a whole
model precisely because a witness-free model has to be justified as such. The list is
checked in both directions, like `just lint-spec`: an exemption naming a model or witness
that does not exist fails, and so does an exemption for a witness the run *did* observe —
a stale exemption is a hole that has quietly closed, and the line must be deleted.

When a previously-observed witness reports 0, the first hypothesis is the one the gate was
built for: its behaviour was disconnected from `step`. Rule that out before reaching for an
exemption.

### Known limitation

The migration model's uniform walk is weak: several of its witnesses sit at single-digit
trace counts at the pinned budget, and five are exempt. The campaign's open walk-steering
item (`.scratch/formal-spec/2026-08-19-quint-completeness-campaign.md`, W1) is the fix; as
it lands, those exemptions go stale and the gate will demand their removal.
