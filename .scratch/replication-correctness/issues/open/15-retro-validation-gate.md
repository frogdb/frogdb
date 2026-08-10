# 15 — Retro-validation gate: the machine catches what the humans caught

Status: needs-triage

## Parent

[PRD](../../PRD.md) §6 exit criterion 8; revert-set size ruled in §8 D4.

## What to build

The campaign's falsifiable exit claim, run as an experiment. Revert each defect below one at a
time in a **throwaway working tree** — inverse patches, never committed — run the layers, and
record per defect which layers flag it.

The set is N=5 per D4: four verified commits plus one spec pick.

| # | defect | commit |
|---|---|---|
| a | the full-resync checkpoint was cut before the shard WALs drained, so acked writes were missing from the payload | `ebdf7d9e` |
| b | promotion minted no replid and adopted no secondary window; a promoted node served no `PSYNC` | `f6484219` |
| c | `WAIT` counted a replica from its seeded resume position instead of a wire ACK | `90fefaf7` |
| d | the replica feed ran while a slot-handoff barrier was armed | `8d55cc4f` |
| e | one FM-REPLICATION forcing-test pick from the LOCKED spec, chosen **at gate time** | n/a |

(e) is chosen at gate time on purpose: the four commits are all defects the campaign explicitly
designed layers for, which biases the gate toward what it already expects to catch. A row drawn
from the 64 LOCKED rows at gate time is the only member of the set the layers were not built
toward.

Verdict rules, from criterion 8:

- **At least one *non-forcing* layer must catch each defect.** The fix's own FM forcing tests are
  excluded from every verdict — counting them makes the gate vacuous, and "the forcing test was the
  only thing that failed" is precisely the finding the gate exists to surface.
- A miss is filed as a gap issue against the responsible workstream and **closed with a new layer
  before exit**; cluster issues 21 and 22 are the precedent for what "closed" means there.
- Run **incrementally after each workstream lands**, not only at the end — that is how the cluster
  campaign discovered two of its five were structurally out of reach in time to build layers for
  them.
- If the first full pass comes back ≥4/5 cheaply, **escalate N to 8 from the reserve list before
  declaring exit**, not after: `86a016fd` (frame-size wedge / backlog-eviction hole / early replid
  adoption — three defects in one commit, revertable individually), `85fc3095` (the five
  link-machinery bugs), `4ced6229` (checkpoint path traversal, unbounded replicated `MULTI`, a lag
  window that round-trips to "off"), `1ea25181` (a persistence-disabled primary shipping no
  dataset).

Method trap carried over from cluster issue 13, worth stating because it silently corrupts the
experiment: a failing proptest run appends its shrunk case to `proptest-regressions/`, which must
be reverted along with the source — otherwise the run leaks a "regression" seed derived from code
that no longer exists.

Deliverable is a §6.1-style results table appended to the PRD, with the same columns the cluster
campaign used, plus the short form in this issue.

## Acceptance criteria

- [ ] All five reverts run against the full layer stack in a throwaway tree; nothing committed
      except `.scratch` (and no leaked `proptest-regressions/` entries)
- [ ] Catching layer(s) named per defect, with the fix's own FM forcing tests excluded from every
      verdict
- [ ] Incremental results recorded after each workstream lands, not only in a final pass
- [ ] N escalated to 8 from the reserve list if the first pass is ≥4/5 cheaply, before exit is
      declared
- [ ] Every miss filed as a gap issue against the responsible workstream and closed with a new
      layer before exit
- [ ] §6.1-style results table appended to the PRD with the evidence

## Blocked by

- Issues 01–14 (`.scratch/replication-correctness/issues/`) — the gate measures the layers they
  build. It runs incrementally as each lands, but exit criterion 8 is only decidable once the full
  stack exists.
