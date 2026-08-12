# 15 — Retro-validation gate: the machine catches what the humans caught

Status: ready-for-human

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

- [x] All five reverts run against the full layer stack in a throwaway tree; nothing committed
      except `.scratch` (and no leaked `proptest-regressions/` entries)
- [x] Catching layer(s) named per defect, with the fix's own FM forcing tests excluded from every
      verdict
- [x] Incremental results recorded after each workstream lands, not only in a final pass
- [x] N escalated to 8 from the reserve list if the first pass is >=4/5 cheaply, before exit is
      declared — not triggered: the first pass caught 3/5, and the reasoning for holding the
      reserve list is recorded in [PRD](../../PRD.md) §6.1
- [ ] Every miss filed as a gap issue against the responsible workstream and closed with a new
      layer before exit — 25 and 27 closed; **26 open, and it is what blocks this issue**
- [x] §6.1-style results table appended to the PRD with the evidence

## Results (2026-08-11)

Full table, per-layer attribution and the two cross-cutting conclusions: [PRD](../../PRD.md) §6.1.
Short form:

| defect | verdict | caught by |
|---|---|---|
| a — checkpoint cut before the WAL drain | **MISS** | nothing but its own tests, at 7 seeds and at 500 |
| b — promotion minted no replid | **CAUGHT** | W1, W2 (R1–R4), W3, W4, integration — every layer |
| c — `WAIT` counted a resume seed | **CAUGHT** | W2 (R1–R4) alone |
| d-i — feed gate, derivation | **CAUGHT** | W3 (feed-gate model smoke + all three replay cases) alone |
| d-ii — feed gate, consumer | **MISS** | nothing; every layer green |
| e — FM-REPLICATION-063 (drawn) | **MISS** | nothing but its own tests |

Three of five caught. Misses: [25](../done/25-no-layer-sees-what-a-full-resync-payload-contains.md)
(closed — new turmoil layer `simulation::full_sync_payload`),
[27](../done/27-nothing-but-its-own-tests-watches-the-replication-byte-counters.md) (closed by the
same layer), [26](26-the-feed-gate-model-proves-a-transcription-not-the-session.md) (**open** — the
stateright models prove properties about a transcription of the session's control flow, so the
consumer side of the feed gate has no witness but its own forcing test).

Evidence commands, per revert: `just test frogdb-replication`, `just test
frogdb-replication-runtime`, `just concurrency-turmoil replication_scheduler`, `just test
frogdb-server replication`, and for (a) the escalation `just replication-seeds 500`. Runner and
its per-layer logs: `.scratch/replication-correctness/gate-evidence/layerstack.sh`.

Two method notes for whoever re-runs this:

1. `git commit` in this repo stages fixed files through lefthook, so a commit taken while an inverse
   patch is in the tree **commits the patch**. One did, and had to be amended out; the run that
   followed it was contaminated and had to be re-taken. Restore the tree before every commit, and
   verify with `git diff <base> HEAD -- frogdb-server`.
2. Attribution needs `git log -S` on each failing test name. Several tests that fail under a revert
   are cited as the FM row's forcing tests but were built *later*, by this campaign's own
   workstreams — those are independent witnesses, not the fix's own regression tests, and the
   verdict turns on the difference (defect d hangs entirely on it).

## Blocked by

- [Issue 26](26-the-feed-gate-model-proves-a-transcription-not-the-session.md) — the one miss still
  open. Exit criterion 8 is not declarable while a defect in the revert set has no witness but its
  own forcing test.
- Issues 01–14 (`.scratch/replication-correctness/issues/`) — the gate measures the layers they
  build. It runs incrementally as each lands, but exit criterion 8 is only decidable once the full
  stack exists.
