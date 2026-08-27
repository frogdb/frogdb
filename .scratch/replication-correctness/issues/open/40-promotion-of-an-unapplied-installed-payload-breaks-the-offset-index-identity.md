# 40: Promotion of an unapplied installed payload breaks the fullsync model's offset↔index identity

Status: needs-triage

Found 2026-08-27 by the issue-37 battery escalation (fullsync battery M117,
`.scratch/formal-spec/2026-08-20-fullsync-battery.md`, addendum of the same date). This is
a **pre-existing defect in the fullsync quint model at HEAD** — it reproduces on the
committed model with no issue-37 edits — plus one real-system design question the new
per-shard stamps surfaced alongside it.

## The counterexample (minimal shape, trace-verified)

`specs/quint/replication_fullsync.qnt`, sampled at 4000×40 seed `0x1` with a single
`--invariant`; an 11-state trace on the HEAD model:

1. A replica full-syncs and **installs** a torn/partial cut's payload — say `[HOLE, 2]`
   (`applyInstallPayload`): `data` holds the payload, `floors` its coverage, and
   `applied = recv = 0` (lawful: streaming restarts from 0 and floor-skips what the
   payload carried).
2. The session drops, and the replica **promotes** in that state. `promoteAs` has no
   guard relating `data.length()` to `applied`, and `applyPromote` keeps `data`
   untouched while settling `recv := applied` (= 0).
3. The next `writeOnPrimaryAs` mints offset `recv + 1 = 1` but `applyWriteOnPrimary`
   does `data.append(w)` — the write lands at list position `data.length() + 1` (= 3).

The model's core representation invariant — list index == stream offset, `HOLE`s for
skipped positions — is now broken: a write the node *claims* at offset 1 is *stored* at
position 3, and position 1 stays a hole below the claimed prefix.

Violated on lawful traces (all real `[violation]` results, not sampler noise):

- At HEAD: `inv_no_hole_below_the_claim` (the hole sits under `applied` one write
  later) and `inv_overship_is_skipped_not_reapplied` (the misplaced head corrupts the
  floor-skip dispositions downstream). These remain unguarded — masking them would be
  a semantic change this issue exists to rule on.
- The issue-37 stamp invariants (`inv_stamps_match_data`, `inv_mint_is_wire_order`,
  `inv_cut_claims_stamps`) detected it far more broadly — they fire on the first
  post-promote write (the stamps installed from the payload — e.g. shard coverage 3 —
  sit above a new history whose mint restarts at 1), where
  `inv_primary_applied_is_head` asserts only `applied == recv` and passes at the
  promote state. To keep the unseeded `just quint-run` sweep from flaking on a
  pre-existing defect, the issue-37 slice **guards the three new invariants** behind a
  coverage-family latch `promotedWithUnappliedTail` (latched in `promoteAs` when
  `data.length() > applied`; a latch rather than a live predicate because the poisoned
  data outlives the promoting node's own restart via cut payloads). Removing that
  guard is part of this issue's fix.

## Why the gates never saw it

Every seeded gate is green: `quint test`, the 500×20 battery baseline (both seeds,
conjunction *and* single-invariant modes), and the 2000×40 witness floor. The trace
needs a torn cut + install + disconnect + promote + write prefix that those budgets
happen not to sample; the seeded all-invariants conjunction run first hits it at
4000×40 seed `0x1`. The 200×20 `just quint-run` conjunction sweep, however, runs
**unseeded** — it is usually green but can land on the region by luck (observed twice
during the issue-37 session, both times as `inv_no_hole_below_the_claim`), so a rare
red `quint-run` on an untouched model is plausibly this defect: check the trace for
the promote-over-unapplied-payload shape before hunting a regression. The exploration walk is also not stable across
invariant sets (adding/removing an `--invariant` changes which traces a seed visits), so
per-seed reachability claims transfer only within one invocation shape.

## Why this was not fixed in-session (needs a ruling)

Promotion mid-sync and promotion over an overshipped tail are *deliberately* modeled
(`promotionMidSyncTest`, `overshippedTailMeetsFailoverTest`), so a blanket
`data.length() == applied` guard on `promoteAs` would delete intended coverage. The real
fix must pick a semantics for the kept-but-unapplied tail on the new primary:

- **Truncate to `applied` at promote** — wrong against the real system: the installed
  payload *is* in the promoted node's store; its effects don't vanish at role change.
- **Keep the tail, mint past it** — matches the store but breaks the offset↔index
  identity the whole model is built on; would need `data` to stop being
  offset-indexed.
- **Divergence latch** — treat a promote with `data.length() > applied` as minting a
  new history whose offsets overlap positions the store already holds from the old one,
  and track it as a defect-family latch rather than pretending one list can carry both
  histories.

The third is probably right, and it is not just model bookkeeping: it is the real
design question the stamps made visible. **Per-shard stamps installed from a full-sync
payload are claims minted under the donor's replid; after promotion the successor's
offsets restart below them, so raw stamp values are not comparable across a history
change.** The issue-37 implementation (and issues 38/24, which own replica stamps and
restart/replid rotation) must key or fence stamps by history, not just by offset. That
ruling belongs with issue 36's family (R17–R24) and should land before the model is
patched, so the model change encodes the ruled semantics instead of a guess.

## Repro

```
quint run specs/quint/replication_fullsync.qnt \
  --max-samples=4000 --max-steps=40 --seed=0x1 \
  --invariant inv_no_hole_below_the_claim        # HEAD and later; [violation]
```

Same command with `inv_overship_is_skipped_not_reapplied` (HEAD and later). The three
issue-37 stamp invariants also trip if their `promotedWithUnappliedTail` guard is
removed. `--out-itf` + the trace shows the install → promote → write prefix above.
