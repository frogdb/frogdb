# Full-sync / PSYNC handoff model — mutation battery and gap closure

**Task**: quint-completeness campaign, T10b (model lifecycle steps 4–5 for the W3 full-sync model).
**Base commit**: `82a4ee22` — `spec(quint): full-sync/PSYNC handoff model (W3)`.
**Model files**:
`specs/quint/replication_fullsync_types.qnt` (211 lines),
`specs/quint/replication_fullsync_logic.qnt` (265),
`specs/quint/replication_fullsync_machine.qnt` (687),
`specs/quint/replication_fullsync.qnt` (567 → 1056 after gap closure).
**Authority**: `.scratch/formal-spec/2026-08-19-quint-completeness-campaign.md` — "a model's checking is
exhaustive to the extent its battery verdict table says so, not to the extent its invariant count
suggests". No small-model exemption.
**Format precedent**: `.scratch/formal-spec/2026-08-19-admission-battery.md` (59 rows) and the Q4
migration-model report + its 2026-08-20 addendum.

| | before (`82a4ee22`) | after gap closure |
|---|---:|---:|
| invariants | 17 | 28 |
| witnesses | 16 | 16 |
| `run` tests | 12 | 38 |
| battery rows CAUGHT | 118 | **162** (133 CAUGHT-T + 29 CAUGHT-P) |
| battery rows MISSED | 84 | **40** |
| battery rows N/A | 2 | 2 |

44 of the 84 pass-1 misses are closed. Every one of the 40 that remain was re-run at 4000×40 for
seeds `0x1`/`0x2`/`0x3` against the closed model and stayed green, and every one carries an analysis
below — 3 detector self-mutations, 12 ghost-arming self-mutations, 3 step-unwiring rows, 18
tripwire-backed equivalent mutants, 4 residual honest misses.

Behaviour is **unchanged**: `_types`, `_logic` and `_machine` are byte-identical to `82a4ee22`.
Every closure is an addition to the properties module.

---

## Verdict vocabulary

| Verdict | Meaning |
|---|---|
| **CAUGHT-T** | At least one `run` test fails under `quint test`. Deterministic, seed-independent. |
| **CAUGHT-P** | All tests pass, but a sampled `quint run` violates a named invariant. Probabilistic. |
| **MISSED** | Tests pass *and* the escalated sampled runs stay green. Requires an honest-miss analysis below. |
| **N/A** | Not a well-formed single semantic edit (a sizing knob, or a steering-only value no guard reads). |

A mutation whose *only* effect is on a `defects.*`/`coverage.*` ghost is still a real row: the ghost is
part of the checked surface (it is what several invariants read), so a ghost that stops latching is a
detector that stopped detecting. Those rows are labelled *ghost self-mutation* in the analyses.

The table's Verdict column shows `<pass 1> → <final>` for every row whose verdict changed when the
row was re-run against the closed model.

---

## Mechanics

Every row is one exact, single-site text replacement, applied to a pristine copy, run, then restored
byte-for-byte from a backup taken before the battery started. The driver asserts the `old` string
occurs **exactly once** in its file before mutating, and byte-compares every model file against the
backup after each restore — a row cannot leak into the next one, and no other agent's concurrent work
in `specs/quint/` is ever touched (`git checkout -- specs/quint/` is never used).

> The byte-comparison is deliberate: `git diff` compares the worktree against the **index**, and a
> concurrent lefthook in this shared tree staged a mid-battery snapshot of `_logic.qnt` while the
> battery was running. The worktree was pristine throughout (`git diff HEAD` empty); the stale index
> entry was unstaged with `git restore --staged` once the battery finished.

Per row:

```bash
# 1. tests (deterministic oracle)
quint test specs/quint/replication_fullsync.qnt

# 2. invariants (sampled oracle) — all of them at once, two seeds at 500x20
quint run specs/quint/replication_fullsync.qnt \
  --invariants inv_no_acked_write_lost_across_fullsync inv_applied_covered_by_data \
    inv_payload_covers_grant inv_splice_continuity inv_partial_grant_sound \
    inv_replid_offset_paired inv_restart_pairs_offset inv_identity_pair_monotone \
    inv_failover_window_whole inv_replids_distinct inv_second_offset_not_above_live \
    inv_offsets_ordered inv_backlog_floor_sound inv_ack_never_above_live_head \
    inv_one_session_per_announced_identity inv_link_points_at_a_live_session \
    inv_superseded_records_no_departure \
    inv_primary_role_is_terminal inv_primary_applied_is_head inv_primary_holds_no_link \
    inv_replica_holds_no_window inv_only_primaries_arm_a_floor \
    inv_registered_session_replica_is_replica inv_prestream_session_is_linked \
    inv_prestream_link_has_nothing_pending inv_linked_replica_recv_within_primary_data \
    inv_replica_not_ahead_of_matching_primary inv_installed_payload_was_cut \
  --max-samples 500 --max-steps 20 --seed 0x1

# 3. escalation, for every row still green — 4000x40, seeds 0x1/0x2/0x3
```

Two notes carried from the admission battery:

- Quint's `--invariants` and `--witnesses` take **space-separated** lists, and zsh does not word-split
  an unquoted variable — every run goes through an explicit `subprocess` argv list.
- Quint reports `error: Invariant violated` **without naming which one**. Every CAUGHT-P row is
  therefore re-run once per invariant at the same seed and budget to attribute the catch; the
  attributed names are what the Evidence column carries.

**Budget.** Baseline (unmutated, closed model) at 500×20 seed `0x1`: `[ok] No violation found`,
537 ms of engine time at 931 traces/s (~1.5 s wall) for all 28 invariants; `quint test`: 38 passing,
2.2 s. At 4000×40 a run costs ~6 s wall (655 traces/s), so the escalation budget (3 seeds) is ~20 s per row and is
applied to **every** row that is green at 500×20 before it may be recorded MISSED. The escalation
matters because several witnessed behaviours are rare at the base budget — baseline witness rates at
500×20/`0x1`:

| Witness | % of traces | Witness | % of traces |
|---|---|---|---|
| `backlogEvicted` | 95.6 | `partialGrant` | 19.2 |
| `promotedWithWindow` | 96.2 | `evictionForcedFullResync` | 15.0 |
| `uncleanRestartReminted` | 46.8 | `ackIgnored` | 14.8 |
| `fullSyncStreaming` | 32.2 | `corpseSession` | 11.8 |
| `writeDuringTransfer` | 29.0 | `ackRecorded` | 9.8 |
| `spliceAbandoned` | 4.0 | `tailApplied` | 3.4 |
| `partialViaSecondary` | 2.0 | `reappliedOverlap` | 1.2 |
| `superseded` | 1.0 | `settleDiscardedFrames` | 1.0 |

The escalation earned its keep in pass 1: F03, M01, M20 and M72 were green at 500×20 and CAUGHT-P at
4000×40 — M01 and M72 being the two that prove the corpse-session guards on splice continuity and
frame apply are load-bearing once a replica has re-linked elsewhere.

**Counterexample protocol.** A new closure invariant violated by the *unmutated* model would have
stopped that thread and been written up in `.scratch/formal-spec/t10b-blocked.md`. None was: all 28
invariants are green on the unmutated closed model at 500×20 for seeds `0x1`/`0x2`/`0x3` and at the
final gate below. No invariant was weakened or quarantined at any point.

---

## Battery

| Row | Target (file: the claim the edit breaks) | Mutation (old → new) | Expected catcher (pre-registered) | Verdict | Evidence |
|---|---|---|---|---|---|
| F01 | logic: windowContains d1 — the primary arm needs an offset the primary reached | `p.replid == id and off <= p.recv,` → `p.replid == id,` | inv_no_acked_write_lost_across_fullsync (grant past the primary's head) | MISSED |  |
| F02 | logic: windowContains d1 — the primary arm needs id equality | `p.replid == id and off <= p.recv,` → `off <= p.recv,` | inv_no_acked_write_lost_across_fullsync (foreign history granted) | CAUGHT-P | seed 0x1: inv_no_acked_write_lost_across_fullsync |
| F03 | logic: windowContains d2 c1 — the secondary arm needs the inherited id | `optExists(p.replid2, g => g == id),` → `true,` | inv_no_acked_write_lost_across_fullsync / inv_splice_continuity | CAUGHT-P | 4000x40 seed 0x1: inv_no_acked_write_lost_across_fullsync |
| F04 | logic: windowContains d2 c2 — the window must be present | `p.second_offset != NO_WINDOW, ⏎         off <= p.second_offset,` → `off <= p.second_offset,` | (pre-registered miss) redundant with inv_failover_window_whole | MISSED |  |
| F05 | logic: windowContains d2 c3 — the boundary is the frozen offset, not the live head | `off <= p.second_offset,` → `off <= p.recv,` | inv_no_acked_write_lost_across_fullsync (resume past the frozen window) | CAUGHT-P | seed 0x2: inv_no_acked_write_lost_across_fullsync |
| F06 | logic: windowContains d2 c3 — the frozen boundary is inclusive (no Redis +1) | `off <= p.second_offset,` → `off < p.second_offset,` | partialResyncViaReplid2WindowTest / overshippedTailMeetsFailoverTest | CAUGHT-T | partialResyncViaReplid2WindowTest,overshippedTailMeetsFailoverTest |
| F07 | logic: floorAdmits c1 — an unarmed floor always refuses | `p.floor != UNARMED and off >= p.floor` → `off >= p.floor` | (pre-registered miss) no test forces a refusal against an unarmed floor | MISSED → **CAUGHT-T** | unarmedFloorRefusesPartialTest |
| F08 | logic: floorAdmits c2 — the resumable side is inclusive (req == floor continues) | `p.floor != UNARMED and off >= p.floor` → `p.floor != UNARMED and off > p.floor` | (pre-registered miss) no test resumes at exactly the floor | CAUGHT-T | partialResyncViaReplid2WindowTest,overshippedTailMeetsFailoverTest |
| F09 | logic: decideArm arm1 — a replica with no history always full-syncs | `if (r.replid == NO_HISTORY) FullSnapshot` → `if (false) FullSnapshot` | (pre-registered miss) equivalent: no primary ever heads NO_HISTORY | MISSED → **CAUGHT-T** | noHistoryStillFullSyncsTest |
| F10 | logic: decideArm arm1 — inverted: no history gets +CONTINUE | `if (r.replid == NO_HISTORY) FullSnapshot ⏎     else if (not(windowCon…` → `if (r.replid == NO_HISTORY) PartialGrant ⏎     else if (not(windowCon…` | happyFullSyncTest + inv_partial_grant_sound (grantForeignHistory) | CAUGHT-T | happyFullSyncTest,writeDuringTransferSurvivesTest,promotionMidSyncTest,partialResyncViaRe… |
| F11 | logic: decideArm arm2 — the window is checked at the applied offset | `else if (not(windowContains(p, r.replid, r.applied))) FullSnapshot` → `else if (not(windowContains(p, r.replid, r.recv))) FullSnapshot` | inv_partial_grant_sound (grantForeignHistory ghost recomputes from applied) | MISSED → **CAUGHT-T** | windowCheckedAtAppliedTest |
| F12 | logic: decideArm arm3 — the floor is checked at the applied offset | `else if (not(floorAdmits(p, r.applied))) FullSnapshot` → `else if (not(floorAdmits(p, r.recv))) FullSnapshot` | inv_partial_grant_sound (grantBelowFloor ghost recomputes from applied) | CAUGHT-P | seed 0x1: inv_partial_grant_sound |
| F13 | logic: decideArm tail — a satisfied window+floor grants +CONTINUE | `else PartialGrant` → `else FullSnapshot` | partialResyncViaReplid2WindowTest, spliceOverEvictedRangeFailsTest | CAUGHT-T | partialResyncViaReplid2WindowTest,spliceOverEvictedRangeFailsTest,truncatedSpliceIsAbando… |
| F14 | logic: degradedByFloor c2 — only a floor-caused degradation counts | `windowContains(p, r.replid, r.applied), ⏎       not(floorAdmits(p, r.…` → `not(floorAdmits(p, r.applied)),` | (pre-registered miss) coverage over-latch, no negative assertion | MISSED → **CAUGHT-T** | restartRefusesAheadReplicaTest |
| F15 | logic: degradedByFloor c3 — the floor is what refused | `not(floorAdmits(p, r.applied)), ⏎     }` → `floorAdmits(p, r.applied), ⏎     }` | backlogEvictionForcesFullResyncTest (coverage.evictionForcedFull) | CAUGHT-T | backlogEvictionForcesFullResyncTest |
| F16 | logic: grantedViaSecondary c1 — the ids must actually differ | `p.replid != r.replid and optExists(p.replid2, g => g == r.replid)` → `optExists(p.replid2, g => g == r.replid)` | (pre-registered miss) equivalent: coupled to inv_replids_distinct | MISSED |  |
| F17 | logic: grantedViaSecondary c2 — served out of the inherited window | `p.replid != r.replid and optExists(p.replid2, g => g == r.replid)` → `p.replid != r.replid` | (pre-registered miss) coverage over-latch, no negative assertion | MISSED |  |
| F18 | logic: applyWriteOnPrimary — the write lands in the dataset | `data: p.data.append(w),` → `data: p.data,` | happyFullSyncTest + inv_applied_covered_by_data | CAUGHT-T | happyFullSyncTest,writeDuringTransferSurvivesTest,promotionMidSyncTest,uncleanRestartMint… |
| F19 | logic: applyWriteOnPrimary — the write advances the live head | `recv: p.recv + 1,` → `recv: p.recv,` | inv_offsets_ordered + happyFullSyncTest | CAUGHT-T | happyFullSyncTest,writeDuringTransferSurvivesTest,promotionMidSyncTest,partialResyncViaRe… |
| F20 | logic: applyWriteOnPrimary — the write advances the applied offset | `applied: p.applied + 1,` → `applied: p.applied,` | inv_identity_pair_monotone (a restart then rewinds at a fixed gen) | CAUGHT-T | uncleanRestartMintsFreshIdTest,restartRefusesAheadReplicaTest,overshippedTailMeetsFailove… |
| F21 | logic: applyWriteOnPrimary — the floor is armed once, at the first push | `floor: if (p.floor == UNARMED) p.recv else p.floor,` → `floor: p.recv,` | backlogEvictionForcesFullResyncTest (evict guard starves) | CAUGHT-T | backlogEvictionForcesFullResyncTest,spliceOverEvictedRangeFailsTest,truncatedSpliceIsAban… |
| F22 | logic: applyWriteOnPrimary — the floor opens at the command's start offset | `floor: if (p.floor == UNARMED) p.recv else p.floor,` → `floor: if (p.floor == UNARMED) p.recv + 1 else p.floor,` | backlogEvictionForcesFullResyncTest (floor == 2 check) | CAUGHT-T | backlogEvictionForcesFullResyncTest,overshippedTailMeetsFailoverTest |
| F23 | logic: canEvictBacklog c1 — only a primary evicts | `p.role == Primary and p.floor != UNARMED and p.recv - p.floor > BACKL…` → `p.floor != UNARMED and p.recv - p.floor > BACKLOG_CAP` | (pre-registered miss) equivalent: only primaries ever arm a floor | MISSED |  |
| F24 | logic: canEvictBacklog c2 — an unarmed buffer has nothing to evict | `p.role == Primary and p.floor != UNARMED and p.recv - p.floor > BACKL…` → `p.role == Primary and p.recv - p.floor > BACKLOG_CAP` | (pre-registered miss) arms a floor no push opened; model never discards data | MISSED → **CAUGHT-T** | unarmedBacklogHasNothingToEvictTest |
| F25 | logic: canEvictBacklog c3 — eviction only above capacity | `p.role == Primary and p.floor != UNARMED and p.recv - p.floor > BACKL…` → `p.role == Primary and p.floor != UNARMED and p.recv - p.floor >= BACK…` | (pre-registered miss) over-eviction only forces safe full resyncs | MISSED → **CAUGHT-T** | evictionNeedsAboveCapacityTest |
| F26 | logic: applyEvictBacklog — the floor rises to the evicted entry's end | `{ ...p, floor: p.floor + 1 }` → `{ ...p, floor: p.floor + 2 }` | backlogEvictionForcesFullResyncTest (floor == 2 check) | CAUGHT-T | backlogEvictionForcesFullResyncTest |
| F27 | logic: applyEvictBacklog — FIFO, not whole-buffer discard | `{ ...p, floor: p.floor + 1 }` → `{ ...p, floor: p.recv }` | backlogEvictionForcesFullResyncTest (floor == 2 check) | CAUGHT-T | backlogEvictionForcesFullResyncTest,spliceOverEvictedRangeFailsTest,truncatedSpliceIsAban… |
| F28 | logic: cutPayload — the cut drains the WALs before exporting | `pure def cutPayload(p: NodeState): List[WriteId] = p.data` → `pure def cutPayload(p: NodeState): List[WriteId] = p.data.slice(0, ma…` | happyFullSyncTest + inv_payload_covers_grant + inv_applied_covered_by_data | CAUGHT-T | happyFullSyncTest,writeDuringTransferSurvivesTest,overshippedTailMeetsFailoverTest |
| F29 | logic: cutBelowGrant — the acked-write-loss detector's direction | `payload.length() < grant` → `payload.length() > grant` | writeDuringTransferSurvivesTest (not(defects.cutBelowGrant) on an overship) | CAUGHT-T | writeDuringTransferSurvivesTest,overshippedTailMeetsFailoverTest |
| F30 | logic: cutAboveGrant — an exact cut is not an overship | `payload.length() > grant` → `payload.length() >= grant` | happyFullSyncTest (not(coverage.overshipped)) | CAUGHT-T | happyFullSyncTest |
| F31 | logic: applyInstallPayload — the payload becomes the dataset | `data: s.payload,` → `data: r.data,` | happyFullSyncTest + inv_applied_covered_by_data | CAUGHT-T | happyFullSyncTest,writeDuringTransferSurvivesTest,overshippedTailMeetsFailoverTest |
| F32 | logic: applyInstallPayload — the trailer identity is adopted | `replid: s.granted_gen,` → `replid: r.replid,` | happyFullSyncTest + inv_replid_offset_paired (tornIdentity) | CAUGHT-T | happyFullSyncTest,promotionMidSyncTest,partialResyncViaReplid2WindowTest,backlogEvictionF… |
| F33 | logic: applyInstallPayload — a full resync clears the failover window id | `replid2: None, ⏎     second_offset: NO_WINDOW,` → `replid2: r.replid2, ⏎     second_offset: NO_WINDOW,` | inv_replid_offset_paired (tornIdentity) + inv_failover_window_whole | MISSED |  |
| F34 | logic: applyInstallPayload — a full resync clears the frozen boundary | `replid2: None, ⏎     second_offset: NO_WINDOW,` → `replid2: None, ⏎     second_offset: r.second_offset,` | inv_replid_offset_paired (tornIdentity) + inv_failover_window_whole | MISSED |  |
| F35 | logic: applyInstallPayload — the absolute pair installs the live head | `recv: s.grant_offset, ⏎     applied: s.grant_offset,` → `recv: r.recv, ⏎     applied: s.grant_offset,` | inv_offsets_ordered + inv_replid_offset_paired (tornIdentity) | CAUGHT-T | writeDuringTransferSurvivesTest,promotionMidSyncTest |
| F36 | logic: applyInstallPayload — the absolute pair installs the applied offset | `recv: s.grant_offset, ⏎     applied: s.grant_offset,` → `recv: s.grant_offset, ⏎     applied: r.applied,` | happyFullSyncTest (applied == 2) | CAUGHT-T | happyFullSyncTest,writeDuringTransferSurvivesTest,promotionMidSyncTest,partialResyncViaRe… |
| F37 | logic: applyInstallPayload — the offsets come from the grant, not the payload end (disclosed reading) | `recv: s.grant_offset, ⏎     applied: s.grant_offset,` → `recv: s.payload.length(), ⏎     applied: s.payload.length(),` | writeDuringTransferSurvivesTest (applied == 1 with a 2-write payload) | CAUGHT-T | writeDuringTransferSurvivesTest,overshippedTailMeetsFailoverTest |
| F38 | logic: installPairedOk — detector conjunct (window cleared) | `post.replid2 == None, ⏎       post.second_offset == NO_WINDOW,` → `post.second_offset == NO_WINDOW,` | (pre-registered miss) detector self-mutation (A48/A49 class) | MISSED |  |
| F39 | logic: spliceRangeAvailable d1 — an empty replay range needs no history | `from >= p.recv or floorAdmits(p, from)` → `floorAdmits(p, from)` | (pre-registered miss) no test splices at from == recv over an unarmed floor | MISSED → **CAUGHT-T** | spliceAtHeadOverUnarmedBacklogTest |
| F40 | logic: spliceRangeAvailable d2 — the window is re-checked at extraction | `from >= p.recv or floorAdmits(p, from)` → `from >= p.recv` | writeDuringTransferSurvivesTest (splice at from < recv) | CAUGHT-T | writeDuringTransferSurvivesTest,overshippedTailMeetsFailoverTest |
| F41 | logic: classifyApply b1 — Appended is exactly the next position | `if (k == d.length() + 1) Appended ⏎     else if (k > d.length() + 1) …` → `if (k <= d.length() + 1) Appended ⏎     else if (k > d.length() + 1) …` | writeDuringTransferSurvivesTest (coverage.reapplied) | CAUGHT-T | writeDuringTransferSurvivesTest,overshippedTailMeetsFailoverTest |
| F42 | logic: classifyApply b2 — a landing past the end is a gap | `else if (k > d.length() + 1) Gapped` → `else if (k > d.length() + 1) Appended` | (pre-registered miss) Gapped unreachable under inv_applied_covered_by_data | MISSED |  |
| F43 | logic: classifyApply b3 — same content at a held position is a re-apply | `else if (d.nth(k - 1) == w) Reapplied` → `else if (d.nth(k - 1) != w) Reapplied` | writeDuringTransferSurvivesTest + overshippedTailMeetsFailoverTest | CAUGHT-T | writeDuringTransferSurvivesTest,overshippedTailMeetsFailoverTest |
| F44 | logic: classifyApply b4 — different content at a held position diverges | `else Diverged` → `else Reapplied` | overshippedTailMeetsFailoverTest (coverage.forkedTailReplaced) | CAUGHT-T | overshippedTailMeetsFailoverTest |
| F45 | logic: applyFrameToData — a landing at a held position overwrites (fidelity fix) | `if (k == d.length() + 1) d.append(w) else d.replaceAt(k - 1, w)` → `if (k == d.length() + 1) d.append(w) else d` | overshippedTailMeetsFailoverTest (final prefixAgrees) | CAUGHT-T | overshippedTailMeetsFailoverTest |
| F46 | logic: applyFrameToData — a landing at the next position appends | `if (k == d.length() + 1) d.append(w) else d.replaceAt(k - 1, w)` → `if (k == d.length() + 1) d else d.replaceAt(k - 1, w)` | inv_applied_covered_by_data | CAUGHT-P | seed 0x1: inv_no_acked_write_lost_across_fullsync,inv_applied_covered_by_data,inv_payload… |
| F47 | logic: ackIsAboveHead — strictly above the head, not at it | `pure def ackIsAboveHead(p: NodeState, wire: Offset): bool = wire > p.…` → `pure def ackIsAboveHead(p: NodeState, wire: Offset): bool = wire >= p…` | ackAboveLiveHeadIgnoredTest (recordAckAt(1,2) at head 2) | CAUGHT-T | ackAboveLiveHeadIgnoredTest |
| F48 | logic: ackIsAboveHead — the validation exists at all | `pure def ackIsAboveHead(p: NodeState, wire: Offset): bool = wire > p.…` → `pure def ackIsAboveHead(p: NodeState, wire: Offset): bool = false` | ackAboveLiveHeadIgnoredTest + inv_ack_never_above_live_head | CAUGHT-T | ackAboveLiveHeadIgnoredTest |
| F49 | logic: applyAck — AckedOffset never regresses | `{ ...s, acked: max2(s.acked, wire) }` → `{ ...s, acked: wire }` | (pre-registered miss) nothing pins ack monotonicity | MISSED → **CAUGHT-T** | ackAboveLiveHeadIgnoredTest |
| F50 | logic: applyPromote — the promoted node becomes a primary | `role: Primary, ⏎     replid: freshGen,` → `role: n.role, ⏎     replid: freshGen,` | promotionMidSyncTest (role == Primary) | CAUGHT-T | promotionMidSyncTest,partialResyncViaReplid2WindowTest |
| F51 | logic: applyPromote — promotion mints a fresh id | `role: Primary, ⏎     replid: freshGen,` → `role: Primary, ⏎     replid: n.replid,` | promotionMidSyncTest + inv_replid_offset_paired (promotePairedOk) | CAUGHT-T | promotionMidSyncTest,partialResyncViaReplid2WindowTest |
| F52 | logic: applyPromote — the outgoing id shifts into the window | `replid2: Some(n.replid),` → `replid2: n.replid2,` | promotionMidSyncTest + inv_replid_offset_paired | CAUGHT-T | promotionMidSyncTest,partialResyncViaReplid2WindowTest |
| F53 | logic: applyPromote — the window freezes at the applied offset, not the received head | `second_offset: n.applied, ⏎     recv: n.applied,` → `second_offset: n.recv, ⏎     recv: n.applied,` | promotionMidSyncTest + inv_second_offset_not_above_live | CAUGHT-T | promotionMidSyncTest |
| F54 | logic: applyPromote — settle_at_applied discards the unapplied tail | `second_offset: n.applied, ⏎     recv: n.applied,` → `second_offset: n.applied, ⏎     recv: n.recv,` | promotionMidSyncTest (recv == 2) | CAUGHT-T | promotionMidSyncTest |
| F55 | logic: applyPromote — the backlog floor re-arms at the same boundary | `floor: n.applied, ⏎     link: None,` → `floor: UNARMED, ⏎     link: None,` | promotionMidSyncTest (floor == 2) | CAUGHT-T | promotionMidSyncTest,partialResyncViaReplid2WindowTest |
| F56 | logic: applyPromote — the promoted node drops its own link | `floor: n.applied, ⏎     link: None,` → `floor: n.applied, ⏎     link: n.link,` | inv_link_points_at_a_live_session | CAUGHT-P | seed 0x1: inv_link_points_at_a_live_session |
| F57 | logic: promotePairedOk — detector conjunct (settle) | `post.recv == pre.applied, ⏎       post.applied == pre.applied,` → `post.applied == pre.applied,` | (pre-registered miss) detector self-mutation | MISSED |  |
| F58 | logic: restartKeepsIdentity c2 — identity survives only an intact recovery | `not(unclean) and keep == pre.applied` → `not(unclean)` | inv_identity_pair_monotone (a clean restart that lost data keeps its id) | CAUGHT-P | seed 0x1: inv_identity_pair_monotone,inv_second_offset_not_above_live |
| F59 | logic: restartKeepsIdentity c1 — an unclean restart always reminds | `not(unclean) and keep == pre.applied` → `keep == pre.applied` | (pre-registered miss) no test restarts unclean at a full recovery point | MISSED → **CAUGHT-T** | uncleanRestartAtFullRecoveryStillRemintsTest |
| F60 | logic: applyRestart (keep branch) — the dataset is truncated to what was recovered | `{ ...pre, role: Primary, data: pre.data.slice(0, keep), recv: keep, a…` → `{ ...pre, role: Primary, data: pre.data, recv: keep, applied: keep, ⏎…` | inv_restart_pairs_offset (restartOffsetTorn) | CAUGHT-P | seed 0x2: inv_restart_pairs_offset |
| F61 | logic: applyRestart (remint branch) — the dataset is truncated to what was recovered | `{ ...pre, role: Primary, data: pre.data.slice(0, keep), recv: keep, a…` → `{ ...pre, role: Primary, data: pre.data, recv: keep, applied: keep, ⏎…` | uncleanRestartMintsFreshIdTest + inv_restart_pairs_offset | CAUGHT-T | uncleanRestartMintsFreshIdTest,overshippedTailMeetsFailoverTest |
| F62 | logic: applyRestart (remint branch) — the live head commits with the data | `floor: UNARMED, link: None, last_departure: None, ⏎         replid: f…` → `floor: UNARMED, link: None, last_departure: None, recv: pre.recv, ⏎  …` | uncleanRestartMintsFreshIdTest + inv_restart_pairs_offset | CAUGHT-T | uncleanRestartMintsFreshIdTest,overshippedTailMeetsFailoverTest |
| F63 | logic: applyRestart (remint branch) — the applied offset commits with the data | `floor: UNARMED, link: None, last_departure: None, ⏎         replid: f…` → `floor: UNARMED, link: None, last_departure: None, applied: pre.applie…` | inv_applied_covered_by_data + inv_restart_pairs_offset | CAUGHT-T | uncleanRestartMintsFreshIdTest |
| F64 | logic: applyRestart (remint branch) — a reboot starts with an unarmed backlog | `floor: UNARMED, link: None, last_departure: None, ⏎         replid: f…` → `floor: keep, link: None, last_departure: None, ⏎         replid: fres…` | (pre-registered miss) the model's backlog is a floor abstraction; data is never discarded | MISSED → **CAUGHT-T** | uncleanRestartMintsFreshIdTest |
| F65 | logic: applyRestart (remint branch) — a reboot drops its link | `floor: UNARMED, link: None, last_departure: None, ⏎         replid: f…` → `floor: UNARMED, link: pre.link, last_departure: None, ⏎         repli…` | (pre-registered miss) equivalent: unlinkedFrom clears it at the call site | MISSED |  |
| F66 | logic: applyRestart (remint branch) — a reboot forgets the last departure | `floor: UNARMED, link: None, last_departure: None, ⏎         replid: f…` → `floor: UNARMED, link: None, last_departure: pre.last_departure, ⏎    …` | (pre-registered miss) last_departure is the feed-gate model's subject | MISSED → **CAUGHT-T** | restartRefusesAheadReplicaTest |
| F67 | logic: applyRestart (remint branch) — a lossy reboot mints a fresh id | `replid: freshGen, replid2: Some(pre.replid), second_offset: keep }` → `replid: pre.replid, replid2: Some(pre.replid), second_offset: keep }` | uncleanRestartMintsFreshIdTest + inv_identity_pair_monotone | CAUGHT-T | uncleanRestartMintsFreshIdTest,overshippedTailMeetsFailoverTest |
| F68 | logic: applyRestart (remint branch) — the outgoing id shifts into the window | `replid: freshGen, replid2: Some(pre.replid), second_offset: keep }` → `replid: freshGen, replid2: pre.replid2, second_offset: keep }` | uncleanRestartMintsFreshIdTest + inv_replid_offset_paired | CAUGHT-T | uncleanRestartMintsFreshIdTest,overshippedTailMeetsFailoverTest |
| F69 | logic: applyRestart (remint branch) — the window freezes at the proven offset | `replid: freshGen, replid2: Some(pre.replid), second_offset: keep }` → `replid: freshGen, replid2: Some(pre.replid), second_offset: pre.appli…` | uncleanRestartMintsFreshIdTest + inv_second_offset_not_above_live | CAUGHT-T | uncleanRestartMintsFreshIdTest |
| F70 | logic: restartOffsetPairedOk — detector conjunct (applied) | `post.recv == post.data.length() and post.applied == post.data.length()` → `post.recv == post.data.length()` | (pre-registered miss) detector self-mutation | MISSED |  |
| F71 | logic: identityPairOk — the lexicographic disjunct is load-bearing | `(curGen == prevGen and curOff == prevOff) or lexGt((curGen, curOff), …` → `(curGen == prevGen and curOff == prevOff)` | inv_identity_pair_monotone violated by the unmutated model (clause is tight) | CAUGHT-T | uncleanRestartMintsFreshIdTest |
| F72 | logic: identityPairOk — the offset half of the order is load-bearing | `(curGen == prevGen and curOff == prevOff) or lexGt((curGen, curOff), …` → `(curGen == prevGen and curOff == prevOff) or curGen > prevGen` | (pre-registered miss) weaker oracle; falsified only by F58's shape | CAUGHT-P | seed 0x1: inv_identity_pair_monotone |
| F73 | logic: prefixAgrees — content agreement, not just length | `else 0.to(n - 1).forall(i => d.nth(i) == hist.nth(i))` → `else true` | (pre-registered miss) weaker oracle; falsified only by a divergence row | MISSED → **CAUGHT-T** | overshippedTailMeetsFailoverTest |
| M01 | machine: linkIs — the link must name *this* session, not merely exist | `pure def linkIs(r: NodeState, sid: SessionId): bool = ⏎     optExists…` → `pure def linkIs(r: NodeState, sid: SessionId): bool = ⏎     r.link !=…` | inv_no_acked_write_lost_across_fullsync (a corpse session installs) | CAUGHT-P | 4000x40 seed 0x1: inv_no_acked_write_lost_across_fullsync,inv_splice_continuity |
| M02 | machine: pairSnapshot — the shadow tracks the live head | `off: nodes.get(n).recv,` → `off: nodes.get(n).applied,` | (pre-registered miss) equivalent while a primary's head is settled | MISSED → **CAUGHT-T** | promotionMidSyncTest |
| M03 | machine: pairSnapshot — the shadow records the role | `primary: nodes.get(n).role == Primary,` → `primary: true,` | inv_identity_pair_monotone (a promotion's predecessor was no primary) | MISSED → **CAUGHT-T** | promotionMidSyncTest |
| M04 | machine: ctlNextD — the shadow refreshes every step | `prev_pair: pairSnapshot,` → `prev_pair: ctl.prev_pair,` | (pre-registered miss) destroys the oracle rather than the model | MISSED → **CAUGHT-T** | promotionMidSyncTest |
| M05 | machine: freeSids — a free slot is a disconnected one | `SIDS.filter(k => sessions.get(k).phase == Disconnected)` → `SIDS.filter(k => sessions.get(k).phase != Disconnected)` | (pre-registered N/A) steering-only; psyncRequestAs re-checks the slot | MISSED → **N/A** | sizing/steering only — `freeSids` picks *which* slot, no guard reads the choice |
| M06 | machine: sessionsWithout — a restart clears the slots it was the replica of | `if (s.primary == n or s.replica == n) { ...s, phase: Disconnected } e…` → `if (s.primary == n) { ...s, phase: Disconnected } else s` | inv_link_points_at_a_live_session / inv_one_session_per_announced_identity | MISSED |  |
| M07 | machine: unlinkedFrom — a restart kills every link into its sessions | `if (dead) { ...nm, link: None } else nm` → `nm` | inv_link_points_at_a_live_session | CAUGHT-T | restartRefusesAheadReplicaTest,overshippedTailMeetsFailoverTest |
| M08 | machine: init — the initial primary heads a real generation | `{ role: Primary, data: List(), replid: FIRST_GEN, replid2: None,` → `{ role: Primary, data: List(), replid: NO_HISTORY, replid2: None,` | happyFullSyncTest (grant arm) / inv_partial_grant_sound | CAUGHT-T | promotionMidSyncTest,partialResyncViaReplid2WindowTest,backlogEvictionForcesFullResyncTes… |
| M09 | machine: init — non-primaries start with no history | `{ role: Replica, data: List(), replid: NO_HISTORY, replid2: None,` → `{ role: Replica, data: List(), replid: FIRST_GEN, replid2: None,` | happyFullSyncTest (a virgin replica gets a PartialGrant) | CAUGHT-T | happyFullSyncTest,writeDuringTransferSurvivesTest,promotionMidSyncTest,partialResyncViaRe… |
| M10 | machine: init — nothing starts with an armed backlog floor | `second_offset: NO_WINDOW, recv: 0, applied: 0, floor: UNARMED, ⏎     …` → `second_offset: NO_WINDOW, recv: 0, applied: 0, floor: 0, ⏎           …` | (pre-registered miss) an armed floor at 0 admits everything | MISSED → **CAUGHT-T** | initStateTest |
| M11 | machine: init — the generation counter starts past the initial id | `next_gen: FIRST_GEN + 1,` → `next_gen: FIRST_GEN,` | inv_replids_distinct (a promotion mints the id it already holds) | CAUGHT-T | promotionMidSyncTest,partialResyncViaReplid2WindowTest,uncleanRestartMintsFreshIdTest,ove… |
| M12 | machine: init — the pairing shadow starts from the initial state | `gen: n0.get(n).replid, off: n0.get(n).recv,` → `gen: n0.get(n).replid, off: 0,` | (pre-registered N/A) every initial recv is already 0 | MISSED |  |
| M13 | machine: writeOnPrimaryAs g1 — only a primary accepts writes | `nodes.get(p).role == Primary, ⏎     ctl.next_write <= MAX_WRITES,` → `ctl.next_write <= MAX_WRITES,` | inv_no_acked_write_lost_across_fullsync (a replica forks its own history) | MISSED → **CAUGHT-T** | replicaAcceptsNoWriteTest |
| M14 | machine: writeOnPrimaryAs — the write id is consumed | `ctl' = ctlNext(1, 0),` → `ctl' = ctlNext(0, 0),` | (pre-registered miss) duplicate ids only make histories agree more | CAUGHT-T | overshippedTailMeetsFailoverTest |
| M15 | machine: evictBacklogAs — eviction actually raises the floor | `nodes' = nodes.set(p, applyEvictBacklog(nodes.get(p))), ⏎     keepSes…` → `keepNodes, ⏎     keepSessions, ⏎     keepCtl,` | backlogEvictionForcesFullResyncTest (floor == 2) | CAUGHT-T | backlogEvictionForcesFullResyncTest,spliceOverEvictedRangeFailsTest,truncatedSpliceIsAban… |
| M16 | machine: psyncRequestAs g1 — a node never replicates from itself | `r != p, ⏎       rn.role == Replica,` → `rn.role == Replica,` | (pre-registered miss) role guards already exclude r == p | MISSED |  |
| M17 | machine: psyncRequestAs g2 — only a replica issues PSYNC | `rn.role == Replica, ⏎       pn.role == Primary,` → `pn.role == Primary,` | inv_identity_pair_monotone (a primary rewinds under PSYNC) | CAUGHT-P | seed 0x1: inv_no_acked_write_lost_across_fullsync,inv_applied_covered_by_data,inv_payload… |
| M18 | machine: psyncRequestAs g3 — PSYNC targets a primary | `pn.role == Primary, ⏎       rn.link == None,` → `rn.link == None,` | inv_no_acked_write_lost_across_fullsync (chained replica history) | MISSED → **CAUGHT-T** | psyncTargetsAPrimaryTest |
| M19 | machine: psyncRequestAs g4 — an already-linked replica does not re-PSYNC | `rn.link == None, ⏎       sessions.get(sid).phase == Disconnected,` → `sessions.get(sid).phase == Disconnected,` | inv_link_points_at_a_live_session / inv_one_session_per_announced_identity | MISSED → **CAUGHT-T** | linkedReplicaDoesNotRepsyncTest |
| M20 | machine: psyncRequestAs g5 — the target slot must be free | `sessions.get(sid).phase == Disconnected, ⏎       nodes' = nodesPost,` → `nodes' = nodesPost,` | inv_link_points_at_a_live_session (a live session is overwritten) | CAUGHT-P | 4000x40 seed 0x1: inv_link_points_at_a_live_session |
| M21 | machine: psyncRequestAs — an announced peer registers its identity | `identity: if (announced) Some(r) else None,` → `identity: None,` | supersededPredecessorTest (coverage.superseded) | CAUGHT-T | supersededPredecessorTest |
| M22 | machine: psyncRequestAs — an unannounced peer is dedup-exempt | `identity: if (announced) Some(r) else None,` → `identity: Some(r),` | (pre-registered miss) dedup only ever removes sessions | CAUGHT-P | seed 0x1: inv_one_session_per_announced_identity |
| M23 | machine: psyncRequestAs — a partial grant skips the checkpoint | `phase: if (arm == PartialGrant) Connecting else PreparingCheckpoint,` → `phase: PreparingCheckpoint,` | partialResyncViaReplid2WindowTest (phase == Connecting) | CAUGHT-T | truncatedSpliceIsAbandonedTest,overshippedTailMeetsFailoverTest |
| M24 | machine: psyncRequestAs — a full snapshot starts at checkpoint preparation | `phase: if (arm == PartialGrant) Connecting else PreparingCheckpoint,` → `phase: Connecting,` | happyFullSyncTest (cutCheckpointAt is disabled) | CAUGHT-T | happyFullSyncTest,writeDuringTransferSurvivesTest,promotionMidSyncTest,partialResyncViaRe… |
| M25 | machine: psyncRequestAs — the grant carries the primary's generation | `granted_gen: pn.replid,` → `granted_gen: rn.replid,` | happyFullSyncTest + inv_replid_offset_paired (tornIdentity) | CAUGHT-T | happyFullSyncTest,promotionMidSyncTest,partialResyncViaReplid2WindowTest,backlogEvictionF… |
| M26 | machine: psyncRequestAs — the full arm captures the offset before the cut (FM-004) | `grant_offset: if (arm == PartialGrant) rn.applied else pn.recv,` → `grant_offset: if (arm == PartialGrant) rn.applied else pn.recv + 1,` | inv_applied_covered_by_data + inv_payload_covers_grant | CAUGHT-T | happyFullSyncTest,writeDuringTransferSurvivesTest,promotionMidSyncTest,partialResyncViaRe… |
| M27 | machine: psyncRequestAs — the partial arm resumes at what the replica applied | `grant_offset: if (arm == PartialGrant) rn.applied else pn.recv,` → `grant_offset: if (arm == PartialGrant) rn.recv else pn.recv,` | partialResyncViaReplid2WindowTest (grant_offset == 1) | MISSED → **CAUGHT-T** | windowCheckedAtAppliedTest,psyncResetsReplicaPositionTest |
| M28 | machine: psyncRequestAs — a fresh session starts with no ack credited | `acked: 0, ⏎       resume_offset: 0, ⏎     } ⏎     val rPost = {` → `acked: pn.recv, ⏎       resume_offset: 0, ⏎     } ⏎     val rPost = {` | (pre-registered miss) nothing relates AckedOffset to replica progress | MISSED → **CAUGHT-T** | psyncResetsReplicaPositionTest |
| M29 | machine: psyncRequestAs — the replica links to the granted session | `link: Some(sid),` → `link: rn.link,` | happyFullSyncTest (deliverPayloadAt's linkIs guard disables) | CAUGHT-T | happyFullSyncTest,writeDuringTransferSurvivesTest,promotionMidSyncTest,partialResyncViaRe… |
| M30 | machine: psyncRequestAs — a +CONTINUE shifts the granted id in (TR-019) | `replid: if (arm == PartialGrant) pn.replid else rn.replid,` → `replid: rn.replid,` | partialResyncViaReplid2WindowTest (replid == 2) | CAUGHT-T | partialResyncViaReplid2WindowTest |
| M31 | machine: psyncRequestAs — the full arm adopts no id before the trailer | `replid: if (arm == PartialGrant) pn.replid else rn.replid,` → `replid: pn.replid,` | inv_no_acked_write_lost_across_fullsync (id adopted before the data) | MISSED → **CAUGHT-T** | restartRefusesAheadReplicaTest |
| M32 | machine: psyncRequestAs — the reconnect rewinds recv to what was applied | `recv: if (arm == PartialGrant) rn.applied else 0,` → `recv: if (arm == PartialGrant) rn.recv else 0,` | (pre-registered miss) the discarded-tail leg; needs a run test | MISSED → **CAUGHT-T** | psyncResetsReplicaPositionTest |
| M33 | machine: psyncRequestAs — the full arm rewinds to zero | `recv: if (arm == PartialGrant) rn.applied else 0,` → `recv: if (arm == PartialGrant) rn.applied else rn.recv,` | inv_offsets_ordered (recv above a zeroed applied after install) | MISSED → **CAUGHT-T** | restartRefusesAheadReplicaTest |
| M34 | machine: psyncRequestAs — the applied offset rewinds with recv | `applied: if (arm == PartialGrant) rn.applied else 0,` → `applied: if (arm == PartialGrant) rn.applied else rn.applied,` | happyFullSyncTest / inv_applied_covered_by_data | CAUGHT-P | seed 0x1: inv_offsets_ordered |
| M35 | machine: victims c1 — only an announced registration dedups | `announced, ⏎       k != sid,` → `k != sid,` | (pre-registered miss) unannounced dedup only removes more sessions | MISSED → **CAUGHT-T** | unannouncedRegistrationDoesNotDedupTest,corpseTeardownKeepsLiveLinkTest |
| M36 | machine: victims c2 — a registration never kicks itself | `k != sid, ⏎       sessions.get(k).phase != Disconnected,` → `sessions.get(k).phase != Disconnected,` | happyFullSyncTest (the granted slot is immediately disconnected) | MISSED |  |
| M37 | machine: victims c4 — dedup is per-primary | `sessions.get(k).primary == p, ⏎       sessions.get(k).identity == Som…` → `sessions.get(k).identity == Some(r),` | (pre-registered miss) cross-primary kick needs a run test | MISSED → **CAUGHT-T** | dedupIsPerPrimaryTest |
| M38 | machine: victims c5 — dedup is keyed on the node identity | `sessions.get(k).identity == Some(r),` → `sessions.get(k).identity != None,` | (pre-registered miss) over-kicking; needs a run test | CAUGHT-T | partialResyncViaReplid2WindowTest |
| M39 | machine: psyncRequestAs — the victims are actually disconnected | `else if (victims.contains(k)) { ...sessions.get(k), phase: Disconnect…` → `else if (false) { ...sessions.get(k), phase: Disconnected }` | supersededPredecessorTest + inv_one_session_per_announced_identity | CAUGHT-T | supersededPredecessorTest |
| M40 | machine: psyncRequestAs — the superseded-departure ghost reads the post-state | `victims.size() > 0 and nodesPost.get(p).last_departure != pn.last_dep…` → `false),` | (pre-registered miss) ghost self-mutation; the model never sets it | MISSED |  |
| M41 | machine: psyncRequestAs — the grant-below-floor ghost is armed | `arm == PartialGrant and not(floorAdmits(pn, rn.applied))),` → `false),` | (pre-registered miss) ghost self-mutation (F12 is the behaviour row) | MISSED |  |
| M42 | machine: psyncRequestAs — the foreign-history ghost is armed | `arm == PartialGrant and not(windowContains(pn, rn.replid, rn.applied)…` → `false),` | (pre-registered miss) ghost self-mutation (F11 is the behaviour row) | MISSED |  |
| M43 | machine: psyncRequestAs — the secondary-window coverage latch | `arm == PartialGrant and grantedViaSecondary(pn, rn)),` → `false),` | partialResyncViaReplid2WindowTest (coverage.partialViaSecondary) | CAUGHT-T | partialResyncViaReplid2WindowTest,overshippedTailMeetsFailoverTest |
| M44 | machine: psyncRequestAs — the eviction-degradation coverage latch | `arm == FullSnapshot and degradedByFloor(pn, rn)),` → `false),` | backlogEvictionForcesFullResyncTest (coverage.evictionForcedFull) | CAUGHT-T | backlogEvictionForcesFullResyncTest |
| M45 | machine: psyncRequestAs — the superseded coverage latch | `superseded: latch(coverage.superseded, victims.size() > 0),` → `superseded: latch(coverage.superseded, false),` | supersededPredecessorTest (coverage.superseded) | CAUGHT-T | supersededPredecessorTest |
| M46 | machine: cutCheckpointAt g1 — a cut happens once, at the preparing phase | `s.phase == PreparingCheckpoint, ⏎       keepNodes,` → `s.phase != Disconnected, ⏎       keepNodes,` | inv_no_acked_write_lost_across_fullsync / inv_applied_covered_by_data | MISSED → **CAUGHT-T** | cutHappensOnceTest |
| M47 | machine: cutCheckpointAt — the cut advances the session | `{ ...s, phase: StreamingCheckpoint, payload: payload, payload_cut: tr…` → `{ ...s, phase: PreparingCheckpoint, payload: payload, payload_cut: tr…` | happyFullSyncTest (deliverPayloadAt disabled) | CAUGHT-T | happyFullSyncTest,writeDuringTransferSurvivesTest,promotionMidSyncTest,partialResyncViaRe… |
| M48 | machine: cutCheckpointAt — the exported payload is the cut | `{ ...s, phase: StreamingCheckpoint, payload: payload, payload_cut: tr…` → `{ ...s, phase: StreamingCheckpoint, payload: s.payload, payload_cut: …` | happyFullSyncTest + inv_applied_covered_by_data | CAUGHT-T | happyFullSyncTest,writeDuringTransferSurvivesTest,overshippedTailMeetsFailoverTest |
| M49 | machine: cutCheckpointAt — payload_cut records that a cut happened | `{ ...s, phase: StreamingCheckpoint, payload: payload, payload_cut: tr…` → `{ ...s, phase: StreamingCheckpoint, payload: payload, payload_cut: fa…` | (pre-registered miss) payload_cut is read nowhere — dead field | MISSED → **CAUGHT-T** | happyFullSyncTest |
| M50 | machine: cutCheckpointAt — the acked-loss ghost is armed | `cutBelowGrant: latch(defects.cutBelowGrant, cutBelowGrant(payload, s.…` → `cutBelowGrant: latch(defects.cutBelowGrant, false) },` | (pre-registered miss) ghost self-mutation (F28 is the behaviour row) | MISSED |  |
| M51 | machine: cutCheckpointAt — the overship coverage latch | `overshipped: latch(coverage.overshipped, cutAboveGrant(payload, s.gra…` → `overshipped: latch(coverage.overshipped, false) },` | writeDuringTransferSurvivesTest (coverage.overshipped) | CAUGHT-T | writeDuringTransferSurvivesTest,overshippedTailMeetsFailoverTest |
| M52 | machine: deliverPayloadAt g1 — the trailer lands once, after the cut | `s.phase == StreamingCheckpoint, ⏎       // Only the replica's own liv…` → `s.phase != Disconnected, ⏎       // Only the replica's own live link …` | inv_applied_covered_by_data (an uncut empty payload installs a grant) | CAUGHT-P | seed 0x1: inv_no_acked_write_lost_across_fullsync,inv_applied_covered_by_data,inv_payload… |
| M53 | machine: deliverPayloadAt g2 — the corpse-session guard is load-bearing | `linkIs(rn, sid), ⏎       nodes' = nodes.set(s.replica, rPost),` → `nodes' = nodes.set(s.replica, rPost),` | inv_no_acked_write_lost_across_fullsync (a corpse overwrites a live dataset) | MISSED |  |
| M54 | machine: deliverPayloadAt — the install commits to the replica | `nodes' = nodes.set(s.replica, rPost), ⏎       sessions' = sessions.se…` → `keepNodes, ⏎       sessions' = sessions.set(sid, { ...s, phase: Paylo…` | happyFullSyncTest (data == payload) | CAUGHT-T | happyFullSyncTest,writeDuringTransferSurvivesTest,promotionMidSyncTest,partialResyncViaRe… |
| M55 | machine: deliverPayloadAt — the session advances to PayloadInstalled | `sessions' = sessions.set(sid, { ...s, phase: PayloadInstalled }),` → `sessions' = sessions.set(sid, { ...s, phase: Streaming }),` | happyFullSyncTest (phase == PayloadInstalled) / inv_splice_continuity | CAUGHT-T | happyFullSyncTest,writeDuringTransferSurvivesTest,promotionMidSyncTest,partialResyncViaRe… |
| M56 | machine: deliverPayloadAt — the torn-identity ghost is armed | `tornIdentity: latch(defects.tornIdentity, not(installPairedOk(rPost, …` → `tornIdentity: latch(defects.tornIdentity, false) },` | (pre-registered miss) ghost self-mutation (F32-F36 are the behaviour rows) | MISSED |  |
| M57 | machine: spliceStreamAt g1 — streaming starts from an installed or partial session | `s.phase == PayloadInstalled or s.phase == Connecting, ⏎       spliceR…` → `s.phase != Disconnected, ⏎       spliceRangeAvailable(pn, s.grant_off…` | inv_no_acked_write_lost_across_fullsync (streaming before the trailer) | CAUGHT-P | seed 0x1: inv_splice_continuity |
| M58 | machine: spliceStreamAt g2 — the replay range is re-checked at extraction | `spliceRangeAvailable(pn, s.grant_offset), ⏎       keepNodes,` → `keepNodes,` | spliceOverEvictedRangeFailsTest (.fail() test now succeeds) | CAUGHT-T | spliceOverEvictedRangeFailsTest |
| M59 | machine: spliceStreamAt — the resume point is seeded from the grant | `{ ...s, phase: Streaming, resume_offset: s.grant_offset }),` → `{ ...s, phase: Streaming, resume_offset: 0 }),` | (pre-registered miss) TR-002 resume seed is unasserted | MISSED → **CAUGHT-T** | happyFullSyncTest |
| M60 | machine: spliceStreamAt — the session reaches Streaming | `{ ...s, phase: Streaming, resume_offset: s.grant_offset }),` → `{ ...s, phase: PayloadInstalled, resume_offset: s.grant_offset }),` | happyFullSyncTest (phase == Streaming) | CAUGHT-T | happyFullSyncTest,writeDuringTransferSurvivesTest,promotionMidSyncTest,ackAboveLiveHeadIg… |
| M61 | machine: spliceStreamAt — the full-sync streaming coverage latch | `fullSyncStreaming: latch(coverage.fullSyncStreaming, s.arm == FullSna…` → `fullSyncStreaming: latch(coverage.fullSyncStreaming, true) },` | (pre-registered miss) over-latch with no negative assertion | MISSED |  |
| M62 | machine: abandonSpliceAt g2 — abandonment needs an unavailable range | `not(spliceRangeAvailable(pn, s.grant_offset)), ⏎       nodes' = nodes…` → `nodes' = nodes.set(s.replica, { ...rn, link: None }),` | happyFullSyncTest / truncatedSpliceIsAbandonedTest | MISSED → **CAUGHT-T** | abandonNeedsUnavailableRangeTest |
| M63 | machine: abandonSpliceAt — the replica's link dies with the attempt | `nodes' = nodes.set(s.replica, { ...rn, link: None }), ⏎       session…` → `keepNodes, ⏎       sessions' = sessions.set(sid, { ...s, phase: Disco…` | inv_link_points_at_a_live_session | CAUGHT-T | truncatedSpliceIsAbandonedTest |
| M64 | machine: abandonSpliceAt — the session is torn down, never streamed | `sessions' = sessions.set(sid, { ...s, phase: Disconnected }), ⏎      …` → `sessions' = sessions.set(sid, { ...s, phase: Streaming }), ⏎       ke…` | truncatedSpliceIsAbandonedTest (phase == Disconnected) | CAUGHT-T | truncatedSpliceIsAbandonedTest |
| M65 | machine: abandonSpliceAt — the abandonment coverage latch | `spliceAbandoned: latch(coverage.spliceAbandoned, true) },` → `spliceAbandoned: latch(coverage.spliceAbandoned, false) },` | truncatedSpliceIsAbandonedTest (coverage.spliceAbandoned) | CAUGHT-T | truncatedSpliceIsAbandonedTest |
| M66 | machine: receiveFrameAt g1 — frames only arrive on a streaming session | `s.phase == Streaming, ⏎       linkIs(rn, sid), ⏎       rn.recv < pn.r…` → `s.phase != Disconnected, ⏎       linkIs(rn, sid), ⏎       rn.recv < p…` | inv_no_acked_write_lost_across_fullsync (frames before the trailer) | MISSED → **CAUGHT-T** | framesOnlyOnStreamingSessionTest |
| M67 | machine: receiveFrameAt g2 — the corpse-session guard is load-bearing | `linkIs(rn, sid), ⏎       rn.recv < pn.recv,` → `rn.recv < pn.recv,` | inv_offsets_ordered / inv_no_acked_write_lost_across_fullsync | MISSED → **CAUGHT-T** | corpseCannotFeedReplicaTest |
| M68 | machine: receiveFrameAt g3 — a replica never receives past the primary's head | `rn.recv < pn.recv, ⏎       rn.recv < pn.data.length(),` → `rn.recv < pn.data.length(),` | (pre-registered miss) recv above the primary head is unasserted | MISSED → **CAUGHT-T** | promotedOvershipDoesNotServeForkedTailTest |
| M69 | machine: receiveFrameAt g4 — a frame must exist to be received | `rn.recv < pn.data.length(), ⏎       nodes' = nodes.set(s.replica, { .…` → `nodes' = nodes.set(s.replica, { ...rn, recv: rn.recv + 1 }),` | inv_applied_covered_by_data (applyFrameAt's sentinel read) | MISSED |  |
| M70 | machine: receiveFrameAt — reception advances the received offset | `nodes' = nodes.set(s.replica, { ...rn, recv: rn.recv + 1 }), ⏎       …` → `keepNodes, ⏎       keepSessions,` | happyFullSyncTest / writeDuringTransferSurvivesTest | CAUGHT-T | writeDuringTransferSurvivesTest,promotionMidSyncTest,overshippedTailMeetsFailoverTest |
| M71 | machine: applyFrameAt g1 — frames only land on a streaming session | `s.phase == Streaming, ⏎       linkIs(rn, sid), ⏎       rn.applied < r…` → `s.phase != Disconnected, ⏎       linkIs(rn, sid), ⏎       rn.applied …` | inv_no_acked_write_lost_across_fullsync | MISSED |  |
| M72 | machine: applyFrameAt g2 — the corpse-session guard is load-bearing | `linkIs(rn, sid), ⏎       rn.applied < rn.recv,` → `rn.applied < rn.recv,` | inv_no_acked_write_lost_across_fullsync (a corpse lands foreign frames) | CAUGHT-P | 4000x40 seed 0x1: inv_no_acked_write_lost_across_fullsync,inv_splice_continuity |
| M73 | machine: applyFrameAt g3 — only received frames are applied | `rn.applied < rn.recv, ⏎       k <= pn.data.length(),` → `k <= pn.data.length(),` | inv_offsets_ordered | CAUGHT-P | seed 0x1: inv_offsets_ordered |
| M74 | machine: applyFrameAt g4 — the sentinel read is never committed | `k <= pn.data.length(), ⏎       nodes' = nodes.set(s.replica,` → `nodes' = nodes.set(s.replica,` | inv_no_acked_write_lost_across_fullsync (NO_HISTORY sentinel lands) | MISSED |  |
| M75 | machine: applyFrameAt — the frame is written into the dataset | `{ ...rn, data: applyFrameToData(rn.data, k, w), applied: k }),` → `{ ...rn, data: rn.data, applied: k }),` | inv_applied_covered_by_data | CAUGHT-T | overshippedTailMeetsFailoverTest |
| M76 | machine: applyFrameAt — the applied offset advances with the frame | `{ ...rn, data: applyFrameToData(rn.data, k, w), applied: k }),` → `{ ...rn, data: applyFrameToData(rn.data, k, w), applied: rn.applied }…` | happyFullSyncTest (applied == 2) | CAUGHT-T | writeDuringTransferSurvivesTest |
| M77 | machine: applyFrameAt — the splice-gap ghost is armed | `spliceGap: latch(defects.spliceGap, outcome == Gapped),` → `spliceGap: latch(defects.spliceGap, false),` | (pre-registered miss) ghost self-mutation; Gapped is unreachable | MISSED |  |
| M78 | machine: applyFrameAt — the divergence ghost is armed | `spliceDiverged: latch(defects.spliceDiverged, ⏎           outcome == …` → `spliceDiverged: latch(defects.spliceDiverged, false) },` | (pre-registered miss) ghost self-mutation (F44 is the behaviour row) | MISSED |  |
| M79 | machine: applyFrameAt — divergence is a defect only inside the claimed window | `outcome == Diverged and k <= s.resume_offset) },` → `outcome == Diverged) },` | inv_splice_continuity violated by the unmutated model (disclosed residue) | CAUGHT-T | overshippedTailMeetsFailoverTest |
| M80 | machine: applyFrameAt — the reapplied coverage latch | `reapplied: latch(coverage.reapplied, outcome == Reapplied),` → `reapplied: latch(coverage.reapplied, false),` | writeDuringTransferSurvivesTest (coverage.reapplied) | CAUGHT-T | writeDuringTransferSurvivesTest |
| M81 | machine: applyFrameAt — the forked-tail coverage latch | `forkedTailReplaced: latch(coverage.forkedTailReplaced, ⏎           ou…` → `forkedTailReplaced: latch(coverage.forkedTailReplaced, false),` | overshippedTailMeetsFailoverTest (coverage.forkedTailReplaced) | CAUGHT-T | overshippedTailMeetsFailoverTest |
| M82 | machine: applyFrameAt — the tail-applied coverage latch | `tailApplied: latch(coverage.tailApplied, true) },` → `tailApplied: latch(coverage.tailApplied, false) },` | (pre-registered miss) witness-only latch with no test assertion | MISSED → **CAUGHT-T** | writeDuringTransferSurvivesTest |
| M83 | machine: recordAckAt g1 — acks are ingested only while streaming | `s.phase == Streaming, ⏎       not(ackIsAboveHead(pn, wire)),` → `s.phase != Disconnected, ⏎       not(ackIsAboveHead(pn, wire)),` | (pre-registered miss) AckedOffset outside Streaming is unconstrained | MISSED → **CAUGHT-T** | ackOnlyWhileStreamingTest |
| M84 | machine: recordAckAt g2 — the head validation gates the write path | `not(ackIsAboveHead(pn, wire)), ⏎       keepNodes,` → `keepNodes,` | inv_ack_never_above_live_head (defects.ackAboveHead) | CAUGHT-P | seed 0x1: inv_ack_never_above_live_head |
| M85 | machine: recordAckAt — the accepted ack is written | `sessions' = sessions.set(sid, sPost),` → `keepSessions,` | ackAboveLiveHeadIgnoredTest (acked == 2) | CAUGHT-T | ackAboveLiveHeadIgnoredTest |
| M86 | machine: recordAckAt — the above-head ghost reads the post-state | `ackAboveHead: latch(defects.ackAboveHead, sPost.acked > pn.recv) },` → `ackAboveHead: latch(defects.ackAboveHead, false) },` | (pre-registered miss) ghost self-mutation (M84/F47 are the behaviour rows) | MISSED |  |
| M87 | machine: ignoreAckAt g2 — the ignore branch takes only above-head acks | `ackIsAboveHead(pn, wire), ⏎       nodes' = nodes.set(s.primary, { ...…` → `nodes' = nodes.set(s.primary, { ...pn, acks_ignored: pn.acks_ignored …` | ackAboveLiveHeadIgnoredTest (acks_ignored == 1 after an honest ack) | MISSED → **CAUGHT-T** | honestAckIsNotIgnoredTest |
| M88 | machine: ignoreAckAt — the ruled outcome is ignore, not clamp (issue-21) | `val sessionsPost = sessions ⏎     all {` → `val sessionsPost = sessions.set(sid, { ...s, acked: pn.recv }) ⏎     …` | inv_ack_never_above_live_head (defects.ackWrittenOnIgnore) | CAUGHT-P | seed 0x1: inv_ack_never_above_live_head |
| M89 | machine: ignoreAckAt — the ignore is counted | `nodes' = nodes.set(s.primary, { ...pn, acks_ignored: pn.acks_ignored …` → `keepNodes,` | ackAboveLiveHeadIgnoredTest (acks_ignored == 1) | CAUGHT-T | ackAboveLiveHeadIgnoredTest |
| M90 | machine: ignoreAckAt — the ack-written ghost reads the post-state | `sessionsPost.get(sid).acked != s.acked) },` → `false) },` | (pre-registered miss) ghost self-mutation (M88 is the behaviour row) | MISSED |  |
| M91 | machine: ignoreAckAt — the ignored-ack coverage latch | `coverage' = { ...coverage, ackIgnored: latch(coverage.ackIgnored, tru…` → `coverage' = { ...coverage, ackIgnored: latch(coverage.ackIgnored, fal…` | ackAboveLiveHeadIgnoredTest (coverage.ackIgnored) | CAUGHT-T | ackAboveLiveHeadIgnoredTest |
| M92 | machine: promoteAs g1 — only a replica is promoted | `nn.role == Replica, ⏎       ctl.next_gen <= FIRST_GEN + MAX_GENS,` → `ctl.next_gen <= FIRST_GEN + MAX_GENS,` | inv_identity_pair_monotone (a primary re-promotes and rewinds) | MISSED → **CAUGHT-T** | primaryIsNotPromotedTest |
| M93 | machine: promoteAs — the promotion commits to the node | `nodes' = nodes.set(n, nPost),` → `keepNodes,` | promotionMidSyncTest (role == Primary) | CAUGHT-T | promotionMidSyncTest,partialResyncViaReplid2WindowTest |
| M94 | machine: promoteAs — the promoted node's replica-side sessions die | `if (s.replica == n) { ...s, phase: Disconnected } else s` → `s` | inv_link_points_at_a_live_session (promotionMidSyncTest) | MISSED → **CAUGHT-T** | promotionMidSyncTest |
| M95 | machine: promoteAs — the promotion consumes a generation | `ctl' = ctlNextD(0, 1, 1),` → `ctl' = ctlNextD(0, 0, 1),` | inv_replids_distinct (two promotions mint the same generation) | CAUGHT-P | seed 0x1: inv_no_acked_write_lost_across_fullsync,inv_replid_offset_paired,inv_identity_p… |
| M96 | machine: promoteAs — the torn-identity ghost is armed | `tornIdentity: latch(defects.tornIdentity, not(promotePairedOk(nn, nPo…` → `tornIdentity: latch(defects.tornIdentity, false) },` | (pre-registered miss) ghost self-mutation (F51-F53 are the behaviour rows) | MISSED |  |
| M97 | machine: promoteAs — the settle-discard coverage latch | `settleDiscardedFrames: latch(coverage.settleDiscardedFrames, nn.recv …` → `settleDiscardedFrames: latch(coverage.settleDiscardedFrames, true) },` | (pre-registered miss) over-latch with no negative assertion | MISSED → **CAUGHT-T** | windowCheckedAtAppliedTest |
| M98 | machine: restartNodeAs g1 — the restart subject is a primary | `nn.role == Primary, ⏎       keep <= nn.applied,` → `keep <= nn.applied,` | inv_identity_pair_monotone / restartRefusesAheadReplicaTest | MISSED → **CAUGHT-T** | replicaIsNotRestartedTest |
| M99 | machine: restartNodeAs g2 — recovery never invents data (TR-021) | `keep <= nn.applied, ⏎       keptId or ctl.next_gen <= FIRST_GEN + MAX…` → `keptId or ctl.next_gen <= FIRST_GEN + MAX_GENS,` | restartRefusesAheadReplicaTest (.fail() shape) / inv_applied_covered_by_data | MISSED → **CAUGHT-T** | restartCannotInventDataTest |
| M100 | machine: restartNodeAs — the recovered state commits | `nodes' = unlinkedFrom(nodes.set(n, nPost), n),` → `nodes' = unlinkedFrom(nodes, n),` | uncleanRestartMintsFreshIdTest (replid changed) | CAUGHT-T | uncleanRestartMintsFreshIdTest,restartRefusesAheadReplicaTest,overshippedTailMeetsFailove… |
| M101 | machine: restartNodeAs — a reboot drops the session registry it owned | `sessions' = sessionsWithout(n),` → `keepSessions,` | inv_link_points_at_a_live_session | CAUGHT-T | restartRefusesAheadReplicaTest |
| M102 | machine: restartNodeAs — only a reminting restart consumes a generation | `ctl' = ctlNextD(0, if (keptId) 0 else 1, 1),` → `ctl' = ctlNextD(0, 0, 1),` | inv_replids_distinct / inv_identity_pair_monotone | CAUGHT-P | seed 0x1: inv_no_acked_write_lost_across_fullsync,inv_replid_offset_paired,inv_identity_p… |
| M103 | machine: restartNodeAs — the offset-pairing ghost is armed | `restartOffsetTorn: latch(defects.restartOffsetTorn, not(restartOffset…` → `restartOffsetTorn: latch(defects.restartOffsetTorn, false),` | (pre-registered miss) ghost self-mutation (F60-F63 are the behaviour rows) | MISSED |  |
| M104 | machine: restartNodeAs — the remint-shift ghost is armed (issue-24 (a)) | `not(nPost.replid2 == Some(nn.replid) and nPost.second_offset == keep)…` → `false) },` | (pre-registered miss) ghost self-mutation (F68/F69 are the behaviour rows) | MISSED |  |
| M105 | machine: restartNodeAs — the remint coverage latch | `uncleanRestartReminted: latch(coverage.uncleanRestartReminted, not(ke…` → `uncleanRestartReminted: latch(coverage.uncleanRestartReminted, false)…` | uncleanRestartMintsFreshIdTest (coverage.uncleanRestartReminted) | CAUGHT-T | uncleanRestartMintsFreshIdTest |
| M106 | machine: dropLinkAt g2 — a link is dropped only by its own holder | `s.phase == Streaming, ⏎       linkIs(rn, sid), ⏎       nodes' = nodes…` → `s.phase == Streaming, ⏎       nodes' = nodes.set(s.replica, { ...rn, …` | (pre-registered miss) clearing a foreign link only removes behaviour | MISSED → **CAUGHT-T** | corpseLinkNotDroppableTest |
| M107 | machine: dropLinkAt — the corpse is created replica-side only | `nodes' = nodes.set(s.replica, { ...rn, link: None }), ⏎       keepSes…` → `nodes' = nodes.set(s.replica, { ...rn, link: None }), ⏎       session…` | supersededPredecessorTest (the corpse must survive to be superseded) | CAUGHT-T | supersededPredecessorTest |
| M108 | machine: dropLinkAt — the corpse coverage latch | `coverage' = { ...coverage, corpseSession: latch(coverage.corpseSessio…` → `coverage' = { ...coverage, corpseSession: latch(coverage.corpseSessio…` | (pre-registered miss) witness-only latch with no test assertion | CAUGHT-T | supersededPredecessorTest |
| M109 | machine: endSessionAt — the ending session clears only its own holder's link | `.set(s.replica, if (linkIs(rn, sid)) { ...rn, link: None } else rn)` → `.set(s.replica, { ...rn, link: None })` | (pre-registered miss) clearing a foreign link only removes behaviour | MISSED → **CAUGHT-T** | corpseTeardownKeepsLiveLinkTest |
| M110 | machine: endSessionAt — a streaming teardown records a graceful departure | `if (s.phase == Streaming) { ...pn, last_departure: Some(Graceful) } e…` → `pn),` | (pre-registered miss) last_departure is the feed-gate model's subject | MISSED → **CAUGHT-T** | backlogEvictionForcesFullResyncTest |
| M111 | machine: endSessionAt — the session slot is freed | `sessions' = sessions.set(sid, { ...s, phase: Disconnected }), ⏎      …` → `keepSessions, ⏎       keepCtl, ⏎       keepDefects, ⏎       keepCover…` | inv_link_points_at_a_live_session / supersededPredecessorTest | CAUGHT-T | partialResyncViaReplid2WindowTest,backlogEvictionForcesFullResyncTest,spliceOverEvictedRa… |
| M112 | machine: step — the apply arm is reachable | `applyStep, ⏎     ackStep,` → `ackStep,` | (pre-registered miss) witness-zero only; documented detection hole | MISSED |  |
| M113 | machine: step — the abandon arm is reachable | `abandonStep, ⏎     receiveStep,` → `receiveStep,` | (pre-registered miss) witness-zero only; documented detection hole | MISSED |  |
| M114 | machine: teardownStep — the restart arm is reachable | `any { restartStep, endSessionStep },` → `any { endSessionStep },` | (pre-registered miss) witness-zero only; documented detection hole | MISSED |  |
| T01 | types: NO_WINDOW — the absent-window sentinel is out of band | `pure val NO_WINDOW: Offset = -1` → `pure val NO_WINDOW: Offset = 0` | inv_failover_window_whole (offset 0 is a legal boundary) | CAUGHT-P | seed 0x1: inv_failover_window_whole |
| T02 | types: UNARMED — the unarmed-floor sentinel is out of band | `pure val UNARMED: Offset = -1` → `pure val UNARMED: Offset = 0` | backlogEvictionForcesFullResyncTest / inv_backlog_floor_sound | CAUGHT-T | backlogEvictionForcesFullResyncTest,spliceOverEvictedRangeFailsTest,truncatedSpliceIsAban… |
| T03 | types: NO_HISTORY — the no-history generation is distinct from a real one | `pure val NO_HISTORY: Gen = 0` → `pure val NO_HISTORY: Gen = 1` | happyFullSyncTest (a virgin replica matches FIRST_GEN) | CAUGHT-T | partialResyncViaReplid2WindowTest,backlogEvictionForcesFullResyncTest,spliceOverEvictedRa… |
| T04 | types: FIRST_GEN — the first real generation is not the no-history sentinel | `pure val FIRST_GEN: Gen = 1` → `pure val FIRST_GEN: Gen = 0` | happyFullSyncTest / inv_partial_grant_sound | CAUGHT-T | partialResyncViaReplid2WindowTest,backlogEvictionForcesFullResyncTest,spliceOverEvictedRa… |
| T05 | types: BACKLOG_CAP — the backlog is finite (sizing knob) | `pure val BACKLOG_CAP: int = 1` → `pure val BACKLOG_CAP: int = 4` | backlogEvictionForcesFullResyncTest (eviction unreachable) | CAUGHT-T | backlogEvictionForcesFullResyncTest,spliceOverEvictedRangeFailsTest,truncatedSpliceIsAban… |
| T06 | types: MAX_WRITES / MAX_GENS / MAX_DISRUPTS — bounded-exploration knobs | `pure val MAX_WRITES: int = 4` → `pure val MAX_WRITES: int = 8` | (pre-registered N/A) sizing knob, no semantics | MISSED → **N/A** | sizing knob — MAX_WRITES bounds the state space, it is not a semantic claim |
| I01 | main: inv_no_acked_write_lost — the antecedent restricts to a shared id | `(s.phase == Streaming and r.replid == p.replid)` → `(s.phase == Streaming)` | violated by the unmutated model (clause is load-bearing, not slack) | CAUGHT-P | seed 0x2: inv_no_acked_write_lost_across_fullsync |
| I02 | main: inv_no_acked_write_lost — agreement is checked to the applied offset | `implies prefixAgrees(r.data, p.data, r.applied)` → `implies prefixAgrees(r.data, p.data, r.data.length())` | violated by the unmutated model (the overshipped tail is legal) | CAUGHT-P | seed 0x2: inv_no_acked_write_lost_across_fullsync |
| I03 | main: inv_applied_covered_by_data — applied never exceeds the dataset | `NODES.forall(n => nodes.get(n).applied <= nodes.get(n).data.length())` → `NODES.forall(n => nodes.get(n).applied == nodes.get(n).data.length())` | violated by the unmutated model (the overshipped tail is legal) | CAUGHT-P | seed 0x1: inv_applied_covered_by_data |
| I04 | main: inv_offsets_ordered — applied never exceeds recv | `NODES.forall(n => nodes.get(n).applied <= nodes.get(n).recv)` → `NODES.forall(n => nodes.get(n).applied == nodes.get(n).recv)` | violated by the unmutated model (a received-but-unapplied tail is legal) | CAUGHT-P | seed 0x1: inv_offsets_ordered |
| I05 | main: inv_second_offset_not_above_live — the primary-role antecedent | `(s.role == Primary and s.second_offset != NO_WINDOW)` → `(s.second_offset != NO_WINDOW)` | (pre-registered miss) → strengthen permanently if green | MISSED |  |
| I06 | main: inv_second_offset_not_above_live — the boundary is inclusive | `implies s.second_offset <= s.recv` → `implies s.second_offset < s.recv` | violated by the unmutated model (promotion freezes at exactly recv) | CAUGHT-T | promotionMidSyncTest |
| I07 | main: inv_backlog_floor_sound — an armed floor is inside the history | `(s.floor != UNARMED) implies (s.floor >= 0 and s.floor <= s.recv)` → `(s.floor != UNARMED) implies (s.floor > 0 and s.floor <= s.recv)` | violated by the unmutated model (the first push arms the floor at 0) | CAUGHT-P | seed 0x1: inv_backlog_floor_sound |
| I08 | main: inv_one_session_per_announced_identity — dedup applies to announced peers only | `sa.identity != None,` → `true,` | violated by the unmutated model (unannounced peers are dedup-exempt) | CAUGHT-P | seed 0x1: inv_one_session_per_announced_identity |
| I09 | main: inv_identity_pair_monotone — the antecedent needs a primary predecessor | `(prev.primary and cur.role == Primary)` → `(cur.role == Primary)` | violated by the unmutated model (a promotion's predecessor was a replica) | MISSED |  |
| I10 | main: inv_identity_pair_monotone — the antecedent needs a primary successor | `(prev.primary and cur.role == Primary)` → `(prev.primary)` | (pre-registered miss) no demotion transition exists in the model | MISSED |  |
| I11 | main: inv_link_points_at_a_live_session — the session must belong to the holder | `s.phase != Disconnected and s.replica == n` → `s.phase != Disconnected` | (pre-registered miss) → keep as a tightness record if green | MISSED |  |


---

## Honest-miss analyses

### Class 1 — detector self-mutations (F38, F57, F70)

`installPairedOk`, `promotePairedOk` and `restartOffsetPairedOk` are not model behaviour: they are
the *predicates the defect ghosts are computed from*. Deleting a conjunct from one of them does not
change a single transition — it makes the corresponding detector blind to one shape of the defect it
watches for. Nothing in the model can observe that, because the ghost only latches when the model
*misbehaves*, and the unmutated model does not misbehave.

This is the A48/A49 class from the admission battery, and it has the same honest answer: the
detector is validated by the **behaviour rows that arm it**, not by mutating the detector. Each of
these three predicates is exercised by behaviour rows that flip the ghost and are caught:

| Detector | Behaviour rows that arm it (all CAUGHT) |
|---|---|
| `installPairedOk` (F38) | F32, F35, F36 — the install-time identity/offset field rows |
| `promotePairedOk` (F57) | F51–F53 — the promotion settle/freeze rows |
| `restartOffsetPairedOk` (F70) | F60–F63 — the restart offset-pairing rows |

Closing these would mean asserting a defect predicate *directly* on hand-built states — a unit test
of the detector. That is a real option, but it tests the oracle rather than the model, and the
campaign's standard is behaviour-level forcing. Recorded as a known, bounded detection hole: a
*narrowed* detector still fires on the shapes its behaviour rows produce.

### Class 2 — ghost-arming self-mutations (M40, M41, M42, M50, M56, M77, M78, M86, M90, M96, M103, M104)

Every one of these replaces a `latch(ghost, <condition>)` with `latch(ghost, false)`. In an
unmutated, non-misbehaving model the condition is **already false on every reachable state**, so the
mutation is a literal no-op on the state, not merely an unobservable one: the ghost was never going
to latch in the traces the battery samples.

They are still real rows, and they are not free: they say the *defect ghosts are only as good as the
rows that arm them*. The pairing is explicit — each ghost has behaviour rows elsewhere in this
battery that make the unsafe variant reachable, and every one of those rows is CAUGHT:

| Ghost (self-mutation row) | Behaviour rows that arm it |
|---|---|
| `defects.supersededDeparture` (M40) | M108 — the departure written on a superseded predecessor |
| `defects.grantBelowFloor` (M41) | F12 — a partial granted under the armed floor |
| `defects.grantForeignHistory` (M42) | F11 — a partial granted on a foreign history (CAUGHT after closure) |
| `defects.cutBelowGrant` (M50) | F28 — a cut that lands below the captured grant offset |
| `defects.tornIdentity` (M56, M96) | F32, F35, F36 (install), F51–F53 (promotion) |
| `defects.spliceGap` (M77) | M75/M76 — the frame-landing rows |
| `defects.spliceDiverged` (M78) | F44 — the divergence classification row |
| `defects.ackAboveHead` (M86) | M84, F47 — the above-head ack rows |
| `defects.ackWritten` (M90) | M88 — the ignore-branch write row |
| `defects.restartOffsetTorn` (M103) | F60–F63 |
| `defects.restartRemintShift` (M104) | F68, F69 — the issue-24 (a) replid2-shift rows |

The only mutation-proof alternative would be to make the model deliberately misbehave — i.e. to keep
a second, broken copy of the machine around purely to prove the detectors fire. The behaviour rows
already do that, one defect at a time, which is why they are the record here.

### Class 3 — the step-unwiring rows (M112, M113, M114)

Removing an action from `step` removes transitions. Safety invariants are closed under removing
transitions, so **no invariant can ever catch this** — the reachable set only shrinks. And the
model's `run` tests drive actions **directly**, not through `step`, so they cannot catch it either.
This is exactly the structural limit the admission battery recorded for A57/A58.

What *is* observable is a witness count collapsing. Measured at 1000×25, seed `0x1`, against the
baseline (only witnesses whose count changed are shown):

| Row | Unwired from `step` | Witnesses driven to **0 traces** | Collateral drop (largest) |
|---|---|---|---|
| M112 | `applyStep` | `witnessReappliedOverlap` (20 → 0), `witnessTailApplied` (63 → 0) | none — the rest move ±10 % on resampling noise |
| M113 | `abandonStep` | `witnessSpliceAbandoned` (51 → 0) | none |
| M114 | `restartStep` (inside `teardownStep`) | `witnessUncleanRestartReminted` (440 → 0) | the whole model narrows: `witnessFullSyncStreaming` 407 → 183, `witnessCorpseSession` 225 → 73, `witnessAckIgnored` 244 → 96, `witnessPartialGrant` 292 → 74, `witnessPartialViaSecondary` 25 → 5, `witnessEvictionForcedFullResync` 195 → 51 |

Baseline (unmutated, 1000×25, seed `0x1`): `witnessBacklogEvicted` 964, `witnessPromotedWithWindow`
966, `witnessUncleanRestartReminted` 440, `witnessFullSyncStreaming` 407, `witnessPartialGrant` 292,
`witnessWriteDuringTransfer` 273, `witnessAckIgnored` 244, `witnessCorpseSession` 225,
`witnessEvictionForcedFullResync` 195, `witnessAckRecorded` 165, `witnessTailApplied` 63,
`witnessSpliceAbandoned` 51, `witnessPartialViaSecondary` 25, `witnessReappliedOverlap` 20,
`witnessSuperseded` 18, `witnessSettleDiscardedFrames` 15.

M114 is the instructive one: restart is not merely its own witness's source, it is the model's main
*re-linking* driver — unwiring it halves or quarters six unrelated witnesses. A per-witness floor
check would catch M112 and M113 by a zeroed witness and M114 several times over.


The verdict stays MISSED because neither oracle in the battery's protocol (`quint test`,
`--invariants`) fails — but the hole is bounded and monitored: the witness table in the model's
header is the artefact that catches it, and a zeroed witness is exactly what the campaign's
"witnesses first" rule looks for. A future `just quint-witness-gate`-style check that fails on a
0-trace witness would convert this class to CAUGHT; that is the recorded follow-up.

### Class 4 — N/A rows

| Row | Why it is not a well-formed semantic edit |
|---|---|
| M05 | `freeSids` picks *which* free slot a PSYNC lands in. `psyncRequestAs` independently requires `sessions.get(sid).phase == Disconnected`, so the flipped filter changes only the steering of the nondeterministic choice, never what is admissible. |
| T06 | `MAX_WRITES` is a state-space bound, not a claim about the system. Raising it makes runs slower and reaches deeper states; it cannot be "wrong". |

### Class 5 — equivalent mutants, each backed by a tripwire

Every row here is behaviour-preserving *in this model* because the deleted conjunct is implied by
state the model maintains elsewhere. The "implied by" is not left as prose: each argument names the
invariant that would fail if its premise stopped being true. New tripwires are marked ★.

| Row | The deleted conjunct | Why the model cannot tell the difference | Tripwire |
|---|---|---|---|
| F01 | `off <= p.recv` on the primary arm of `windowContains` | the arm is decided at the replica's `applied`, and a replica carrying the primary's *own* generation never sits above that primary's head | ★ `inv_replica_not_ahead_of_matching_primary` |
| F04 | `p.second_offset != NO_WINDOW` on the secondary arm | doubly implied: the sibling conjunct already requires `replid2 == Some(id)`, which INV-REPLID-1 pairs with a present window; and `NO_WINDOW` is `-1`, so `off <= -1` refuses every non-negative offset anyway | `inv_failover_window_whole` |
| F16 | `p.replid != r.replid` in `grantedViaSecondary` | the sibling conjunct requires `p.replid2 == Some(r.replid)`, and a node's own id is never also its inherited id | `inv_replids_distinct` |
| F23 | `p.role == Primary` in `canEvictBacklog` | only primaries ever arm a floor, and the surviving `p.floor != UNARMED` conjunct therefore refuses every replica | ★ `inv_only_primaries_arm_a_floor` |
| F33 | clearing `replid2` at payload install | a replica holds no failover window before the trailer, so `replid2: r.replid2` copies `None` | ★ `inv_replica_holds_no_window` |
| F34 | clearing `second_offset` at payload install | same premise: `second_offset: r.second_offset` copies `NO_WINDOW` | ★ `inv_replica_holds_no_window` |
| F42 | `Gapped` for a landing past the end of the dataset | the branch is dead: the receive guard plus `applied <= data.length()` keep every landing at `k <= d.length() + 1`, so the `Gapped` arm is never evaluated | `inv_applied_covered_by_data` |
| F65 | `link: None` on the restart path | the restart subject is a primary, and a primary holds no replication link, so `link: pre.link` copies `None` | ★ `inv_primary_holds_no_link` |
| M06 | clearing the sessions a restarting node is the *replica* of | a restart targets a primary, and the replica side of a live session is a `Replica`, so the dropped disjunct never matches | ★ `inv_registered_session_replica_is_replica` |
| M12 | seeding the pairing shadow from `recv` rather than from `0` | `init` gives every node `recv: 0`, so the two expressions are equal by construction; `initStateTest` now pins that, so the equality cannot silently stop holding | `initStateTest` |
| M16 | `r != p` in `psyncRequestAs` | structural: `Role` is two-valued, and the sibling guards demand `rn.role == Replica` *and* `pn.role == Primary`, which no single node satisfies | (type-level) |
| M36 | `k != sid` in `victims` | `victims` is computed from the pre-state, in which the granting slot is `Disconnected` (a PSYNC guard), and the sibling conjunct excludes disconnected slots — a registration cannot kick itself | (guard-level) |
| M53 | the corpse guard `linkIs(rn, sid)` on `deliverPayloadAt` | before `Streaming` a session is still held by its own replica's link, so the guard is implied at every reachable call site. **The corpse hazard itself is not equivalent** — it is real on the streaming actions, where M67 (receive) is CAUGHT by `corpseCannotFeedReplicaTest` | ★ `inv_prestream_session_is_linked` |
| M69 | `rn.recv < pn.data.length()` on `receiveFrameAt` | on a primary `data.length() == recv`, so the surviving `rn.recv < pn.recv` conjunct is the same bound | ★ `inv_primary_applied_is_head` |
| M71 | `s.phase == Streaming` on `applyFrameAt` (widened to `!= Disconnected`) | on a pre-streaming link nothing is pending (`applied == recv`), so the surviving `rn.applied < rn.recv` conjunct is false and no transition is added | ★ `inv_prestream_link_has_nothing_pending` |
| M74 | the sentinel bound `k <= pn.data.length()` on `applyFrameAt` | a linked replica's `recv` never exceeds the primary's data length, so the frame being applied always exists | ★ `inv_linked_replica_recv_within_primary_data` |
| I05 | the `s.role == Primary` antecedent of `inv_second_offset_not_above_live` | the widened invariant holds, because a replica carries no window at all — the mutation asks for something already true | ★ `inv_replica_holds_no_window` |
| I10 | the `cur.role == Primary` antecedent of `inv_identity_pair_monotone` | the model has no demotion: a node the shadow recorded as a primary is still a primary, so the surviving `prev.primary` antecedent selects the same pairs | ★ `inv_primary_role_is_terminal` |

M53 deserves the explicit note it carries above, because the corpse-session shape is a **deliberate
hazard** this model keeps expressible (issue-22): `recordAckAt` has *no* `link` guard on purpose, so a
corpse can still ack. The battery's job there is not to add the missing guard but to show the guards
that *do* exist are load-bearing where they exist — and they are: M67 and M106/M109 are all CAUGHT,
and M01/M72 (splice continuity, apply) were CAUGHT-P on escalation in pass 1.

### Class 6 — residual honest misses

**F17 — `grantedViaSecondary` over-latches `coverage.partialViaSecondary`.**
Dropping the `optExists(p.replid2, …)` conjunct latches the ghost for *any* partial granted on an id
the primary does not itself hold. That set is empty: at the call site the arm is already
`PartialGrant`, so `windowContains` held, and its only two arms are "the primary's own id" and "the
inherited id" — a different id therefore *is* the inherited one. The mutation is equivalent, and
`inv_partial_grant_sound` is the standing check that a partial grant lies inside one of those two
arms. What is genuinely missing is a **negative** coverage assertion (a same-id partial that leaves
the ghost dark), which needs a trace this model cannot produce — see M61.

**M61 — `spliceStreamAt` over-latches `coverage.fullSyncStreaming`.**
Replacing `s.arm == FullSnapshot` with `true` latches the ghost on partial splices too. It is
undetectable because the ghost is a monotone latch and **every replica in this model bootstraps
through a full snapshot**: `init` gives replicas `replid: NO_HISTORY`, and `decideArm` sends a
no-history replica to `FullSnapshot` unconditionally. So `coverage.fullSyncStreaming` is already true
before any partial splice can occur, and no negative assertion can be written.
The one trace that would distinguish them is a replica that acquires a history *without* a full-arm
splice — install the payload, then abandon the splice before the trailer, then resume partially. Whether
that is reachable under this model's floor arithmetic is an open reachability question; it is
recorded as a **follow-up**, not claimed as impossible.

**I09 — `inv_identity_pair_monotone` is not tight (prediction was wrong).**
Pre-registered expectation: dropping the `prev.primary` antecedent *widens* the invariant to cover
replica→primary steps, and a promotion rewinds the live offset to `applied`, so the unmutated model
should violate it. It does not, at 500×20 or 4000×40 × 3 seeds. Reason: promotion always mints a
**strictly greater generation**, and the ordering is lexicographic on `(gen, offset)` — a greater
generation dominates any offset rewind. The invariant could therefore be permanently widened. It is
deliberately **not** widened: the antecedent encodes the issue-24 ruling's intent (compare a
primary's *successive* pairs), and adopting a strengthening justified only by sampling would lock in
a claim the ruling never made. Recorded here as a tightness observation.

**I11 — `inv_link_points_at_a_live_session` is weakened, not broken.**
Dropping `s.replica == n` weakens the invariant (a link would be allowed to point at a live session
belonging to someone else). A weakened invariant is only detectable if the model produces the
behaviour it stopped forbidding, and it does not. The conjunct is nonetheless not unguarded: the
behaviour it exists to forbid is now pinned deterministically by `corpseTeardownKeepsLiveLinkTest`
(M109 — a teardown clears only its own holder's link) and `corpseLinkNotDroppableTest` (M106 — a
corpse cannot drop a foreign link). The invariant is the standing statement; those two tests are the
forcing evidence.


---

## Gap closure

Every closure is an addition to `specs/quint/replication_fullsync.qnt` (the properties module).
`_types`, `_logic` and `_machine` are **byte-identical to `82a4ee22`** — the battery restored them
after every row and the model's *behaviour* was never edited to make a row catchable.

Three kinds of closure were used, in this order of preference:

1. **A `run` test that forces the behaviour** — deterministic, seed-independent, and it pins the
   exact conjunct. Used wherever the mutation changes a reachable behaviour.
2. **A `.fail()` `run` test** — for guard conjuncts whose removal *adds* behaviour. The scenario
   drives the model to the state the guard forbids and asserts the action is **disabled**; deleting
   the conjunct enables it and the test stops failing. Every `.fail()` closure below was validated
   with a *prefix probe*: the same trace minus the final action, asserted to pass, so the test is
   proven to be pinned on the intended action rather than failing earlier for an unrelated reason.
3. **An invariant** — for shapes with no single forcing trace, and for the *tripwires* described
   below.

### The tripwire discipline (equivalent mutants)

A row can be MISSED because the mutation is genuinely **behaviour-preserving in this model**: the
deleted conjunct is implied by state the model maintains elsewhere. Prose alone is a weak record of
that — it rots the moment the model changes. Following the admission battery's A40/A50 precedent,
each such argument is instead written down as a **falsifiable tripwire invariant**: the state fact
the equivalence argument depends on, checked on every sampled trace. If a future edit breaks the
premise, the tripwire fails and the equivalence argument fails with it, loudly, instead of silently
becoming false.

A tripwire is not expected to kill its row — it is expected to *hold*, and to stop holding if the
model drifts. Rows closed this way stay MISSED in the verdict table; what changed is that the
"unobservable" claim is now machine-checked.

### New invariants (11)

| Invariant | Rows it answers | Claim | Kind |
|---|---|---|---|
| `inv_primary_role_is_terminal` | I10, M03 | a node the shadow recorded as a primary is still a primary — the model has no demotion | tripwire |
| `inv_primary_applied_is_head` | M02, M69 | on a primary `applied == recv`; the shadow's choice of head is immaterial | tripwire |
| `inv_primary_holds_no_link` | F65 | a primary holds no replication link, so a restart's `link: None` is redundant | tripwire |
| `inv_replica_holds_no_window` | F33, F34, I05 | a replica carries no failover window, so clearing it at install is redundant | tripwire |
| `inv_only_primaries_arm_a_floor` | F23 | only primaries ever arm a backlog floor, so `canEvictBacklog`'s role conjunct is implied | tripwire |
| `inv_registered_session_replica_is_replica` | M06, M94 | the replica side of a live session is a `Replica` | tripwire |
| `inv_prestream_session_is_linked` | M53 | a session before `Streaming` is held by its replica's own link | tripwire |
| `inv_prestream_link_has_nothing_pending` | M71 | on such a link `applied == recv` — nothing is in flight before the splice | tripwire |
| `inv_linked_replica_recv_within_primary_data` | M74 | a linked replica's `recv` never exceeds the primary's data length | tripwire |
| `inv_replica_not_ahead_of_matching_primary` | F01 | a replica on the primary's own generation never sits above the primary's head | tripwire |
| `inv_installed_payload_was_cut` | M49 | a session at/after `StreamingCheckpoint` on the full arm has `payload_cut` set | killer |

`inv_installed_payload_was_cut` is the one that is not a tripwire: `payload_cut` was a **dead field**
(written, never read), which is why M49 was invisible. The invariant gives it meaning, and M49 is now
caught by it and by `happyFullSyncTest`.

### Strengthened existing tests (8 edits)

| Test | Added assertion | Closes |
|---|---|---|
| `happyFullSyncTest` | `resume_offset == 2`; `payload_cut` | M59, M49 |
| `writeDuringTransferSurvivesTest` | `coverage.tailApplied` | M82 |
| `promotionMidSyncTest` | promoted node's replica-side session is `Disconnected`; `ctl.prev_pair.get(2) == { gen: FIRST_GEN, off: 3, primary: false }` | M94, M02, M03, M04 |
| `backlogEvictionForcesFullResyncTest` | `last_departure == Some(Graceful)` after a streaming teardown | M110 |
| `ackAboveLiveHeadIgnoredTest` | a *lower* honest ack after a higher one leaves `acked` at the high-water mark | F49 |
| `uncleanRestartMintsFreshIdTest` | `floor == UNARMED` after the reboot | F64 |
| `restartRefusesAheadReplicaTest` | `last_departure == None` after an unclean restart; `not(coverage.evictionForcedFull)`; the reconnecting replica is rewound to `replid == FIRST_GEN, recv == 0, applied == 0` | F66, F14, M31, M33 |
| `overshippedTailMeetsFailoverTest` | the promoted node's forked tail **disagrees** with the old primary's history at the overshipped position | F73 |

The last one is the oracle row: `prefixAgrees` is what several invariants compare histories with, and
F73 replaced its content comparison with `true`. Nothing failed, because no assertion in the model
ever *depended on two histories disagreeing*. The carrier test now asserts a disagreement, so a
`prefixAgrees` that always says "agrees" fails it.

### New scenarios (26 `run` tests)

Plain scenarios — they force a behaviour and assert its result:

| Test | Closes | Forces |
|---|---|---|
| `initStateTest` | M10, M12 | the initial state: unarmed floors, zeroed pairing shadow |
| `noHistoryStillFullSyncsTest` | F09 | a `NO_HISTORY` replica resolves to `FullSnapshot` |
| `unarmedFloorRefusesPartialTest` | F07 | an unarmed floor refuses a partial that the window would have allowed |
| `spliceAtHeadOverUnarmedBacklogTest` | F39 | an empty replay range splices over an unarmed floor |
| `windowCheckedAtAppliedTest` | F11, M97 | the arm is decided at `applied`, not at `recv`, for a replica with a pending tail |
| `psyncResetsReplicaPositionTest` | M27, M28, M32 | reconnect seeds `grant_offset` from `applied`, `acked` from 0, and rewinds `recv` |
| `unannouncedRegistrationDoesNotDedupTest` | M35 | a `listening_port == 0` peer kicks nobody (TR-022 exemption) |
| `dedupIsPerPrimaryTest` | M37 | the same identity registered against a *different* primary kicks nobody |
| `corpseTeardownKeepsLiveLinkTest` | M109 | tearing a corpse session down leaves the replica's live link intact |
| `uncleanRestartAtFullRecoveryStillRemintsTest` | F59 | an unclean stop that loses *nothing* still re-mints: the re-mint is keyed on the unclean flag, not on how far recovery fell short |

`.fail()` scenarios — they drive the model to the state a guard forbids and assert the action is
**disabled** (each validated with a prefix probe):

| Test | Closes | Asserts disabled |
|---|---|---|
| `unarmedBacklogHasNothingToEvictTest` | F24 | eviction against an unarmed backlog |
| `evictionNeedsAboveCapacityTest` | F25 | eviction exactly *at* capacity |
| `cutHappensOnceTest` | M46 | a second checkpoint cut on an already-cut session |
| `abandonNeedsUnavailableRangeTest` | M62 | abandoning a splice whose range is available |
| `framesOnlyOnStreamingSessionTest` | M66 | frame delivery before the trailer |
| `corpseCannotFeedReplicaTest` | M67 | a corpse session feeding a re-linked replica |
| `ackOnlyWhileStreamingTest` | M83 | ACK ingest outside `Streaming` |
| `honestAckIsNotIgnoredTest` | M87 | the ignore branch taking an at-or-below-head ack |
| `replicaAcceptsNoWriteTest` | M13 | a replica accepting a client write |
| `psyncTargetsAPrimaryTest` | M18 | PSYNC against a replica (chained replication) |
| `linkedReplicaDoesNotRepsyncTest` | M19 | a second PSYNC from an already-linked replica |
| `primaryIsNotPromotedTest` | M92 | promoting a node that is already a primary |
| `replicaIsNotRestartedTest` | M98 | driving the restart action at a replica |
| `restartCannotInventDataTest` | M99 | recovering to a point above what was applied (TR-021) |
| `promotedOvershipDoesNotServeForkedTailTest` | M68 | receiving past the serving primary's head |
| `corpseLinkNotDroppableTest` | M106 | a corpse session clearing a foreign link |


---

## Coverage argument — why 204 rows is the whole surface

The battery table was authored **before any mutation was run** (the pre-registration snapshot is
`rows_prereg_snapshot.py`; `exp` is the prediction, and the prediction-vs-observation tally below is
computed against it). Enumeration, not sampling, is what makes the verdict table an exhaustiveness
claim, so the enumeration is stated explicitly.

### What was enumerated

| Surface | Rule applied | Rows |
|---|---|---|
| `_logic` — pure guards and updates | every conjunct of every `bool` guard, deleted or inverted one at a time; every field an `apply…` writes, redirected to the value it must *not* take; every branch of every `if`/`else if` chain | 73 |
| `_machine` — actions and state machine | every guard conjunct of every action, deleted one at a time or widened to its nearest weaker form (`== Streaming` → `!= Disconnected`); every field update in every action's effect; every ghost latch; every `init` field; the two `step` arms and `teardownStep` | 114 |
| `_types` — sentinels and sizing | every sentinel constant (`NO_WINDOW`, `UNARMED`, `NO_HISTORY`, `FIRST_GEN`, `BACKLOG_CAP`) and the sizing knobs | 6 |
| main — properties | every load-bearing clause of every invariant that has more than one (antecedents and conjuncts, one at a time) | 11 |
| | | **204** |

Distribution over definition sites — 58 distinct functions/actions/invariants carry at least one row.
The four largest are the four that carry the most semantics: `psyncRequestAs` (26 — arm selection,
grant seeding, dedup, ghost arming), `applyFrameAt` (12), `applyRestart` (10) and `restartNodeAs` (8).

### What was deliberately *not* mutated

- **The two disclosed discrepancies in the model header** (the checkpoint-cut reading, and the
  FM-REPLICATION-004 overshipped-tail residue) are pending user rulings. The battery **conforms** to
  them rather than fixing them: rows I01–I04 mutate the *clauses that encode the disclosed reading*
  and confirm those clauses are load-bearing (all CAUGHT-P), and the coverage ghosts that make the
  residue visible (`coverage.reapplied`, `coverage.forkedTailReplaced`) plus their carrier test
  `overshippedTailMeetsFailoverTest` have their own rows (F73, M79, M111 — all CAUGHT).
- **The absent `link` guard on `recordAckAt`** is a deliberate hazard (issue-22 corpse sessions), not
  an omission. No row "adds" it. What the battery does instead is check that the corpse guard is
  load-bearing everywhere it *does* appear: M67 (receive) and M106/M109 (link teardown) are CAUGHT-T,
  and M01/M72 (splice continuity, frame apply) are CAUGHT-P.

### Discrimination evidence

An enumeration is only worth what the oracles do with it. Two facts:

### Tests: rows killed

| `run` test | rows killed | ids |
|---|---:|---|
| `overshippedTailMeetsFailoverTest` | 47 | F06, F08, F10, F13, F18, F19, F20, F22, F28, F29, F31, F32, F36, F37, F40, F41, F43, F44, F45, F61, F62, F67, F68, F73, M07, M08, M09, M100, M11, M14, M23, M24, M25, M26, M29, M43, M47, M48, M51, M54, M60, M70, M75, M79, M81, T03, T04 |
| `promotionMidSyncTest` | 30 | F10, F18, F19, F32, F35, F36, F50, F51, F52, F53, F54, F55, I06, M02, M03, M04, M08, M09, M11, M24, M25, M26, M29, M47, M54, M55, M60, M70, M93, M94 |
| `partialResyncViaReplid2WindowTest` | 28 | F06, F08, F10, F13, F19, F32, F36, F50, F51, F52, F55, M08, M09, M11, M111, M24, M25, M26, M29, M30, M38, M43, M47, M54, M55, M93, T03, T04 |
| `writeDuringTransferSurvivesTest` | 26 | F10, F18, F19, F28, F29, F31, F35, F36, F37, F40, F41, F43, M09, M24, M26, M29, M47, M48, M51, M54, M55, M60, M70, M76, M80, M82 |
| `backlogEvictionForcesFullResyncTest` | 26 | F10, F15, F19, F21, F22, F26, F27, F32, F36, M08, M09, M110, M111, M15, M24, M25, M26, M29, M44, M47, M54, M55, T02, T03, T04, T05 |
| `truncatedSpliceIsAbandonedTest` | 25 | F10, F13, F19, F21, F27, F32, M08, M09, M111, M15, M23, M24, M25, M26, M29, M47, M54, M55, M63, M64, M65, T02, T03, T04, T05 |
| `spliceOverEvictedRangeFailsTest` | 23 | F10, F13, F19, F21, F27, F32, F36, M08, M09, M111, M15, M24, M25, M26, M29, M47, M54, M55, M58, T02, T03, T04, T05 |
| `happyFullSyncTest` | 20 | F10, F18, F19, F28, F30, F31, F32, F36, M09, M24, M25, M26, M29, M47, M48, M49, M54, M55, M59, M60 |
| `restartRefusesAheadReplicaTest` | 19 | F10, F14, F18, F19, F20, F36, F66, M07, M09, M100, M101, M24, M26, M29, M31, M33, M47, M54, M55 |
| `ackAboveLiveHeadIgnoredTest` | 14 | F10, F19, F47, F48, F49, M09, M24, M29, M47, M55, M60, M85, M89, M91 |
| `uncleanRestartMintsFreshIdTest` | 14 | F18, F20, F61, F62, F63, F64, F67, F68, F69, F71, M08, M100, M105, M11 |
| `supersededPredecessorTest` | 12 | F10, M09, M107, M108, M21, M24, M29, M39, M45, M47, M55, M60 |
| `windowCheckedAtAppliedTest` | 3 | F11, M27, M97 |
| `psyncResetsReplicaPositionTest` | 3 | M27, M28, M32 |
| `corpseTeardownKeepsLiveLinkTest` | 2 | M109, M35 |
| `unarmedFloorRefusesPartialTest` | 1 | F07 |
| `noHistoryStillFullSyncsTest` | 1 | F09 |
| `unarmedBacklogHasNothingToEvictTest` | 1 | F24 |
| `evictionNeedsAboveCapacityTest` | 1 | F25 |
| `spliceAtHeadOverUnarmedBacklogTest` | 1 | F39 |
| `uncleanRestartAtFullRecoveryStillRemintsTest` | 1 | F59 |
| `initStateTest` | 1 | M10 |
| `replicaAcceptsNoWriteTest` | 1 | M13 |
| `psyncTargetsAPrimaryTest` | 1 | M18 |
| `linkedReplicaDoesNotRepsyncTest` | 1 | M19 |
| `unannouncedRegistrationDoesNotDedupTest` | 1 | M35 |
| `dedupIsPerPrimaryTest` | 1 | M37 |
| `cutHappensOnceTest` | 1 | M46 |
| `abandonNeedsUnavailableRangeTest` | 1 | M62 |
| `framesOnlyOnStreamingSessionTest` | 1 | M66 |
| `corpseCannotFeedReplicaTest` | 1 | M67 |
| `promotedOvershipDoesNotServeForkedTailTest` | 1 | M68 |
| `ackOnlyWhileStreamingTest` | 1 | M83 |
| `honestAckIsNotIgnoredTest` | 1 | M87 |
| `primaryIsNotPromotedTest` | 1 | M92 |
| `replicaIsNotRestartedTest` | 1 | M98 |
| `restartCannotInventDataTest` | 1 | M99 |
| `corpseLinkNotDroppableTest` | 1 | M106 |

### Invariants: rows falsified

| invariant | rows falsified | ids |
|---|---:|---|
| `inv_no_acked_write_lost_across_fullsync` | 12 | F02, F03, F05, F46, I01, I02, M01, M102, M17, M52, M72, M95 |
| `inv_identity_pair_monotone` | 7 | F46, F58, F72, M102, M17, M52, M95 |
| `inv_splice_continuity` | 5 | F46, M01, M52, M57, M72 |
| `inv_offsets_ordered` | 5 | F46, I04, M34, M52, M73 |
| `inv_applied_covered_by_data` | 4 | F46, I03, M17, M52 |
| `inv_replid_offset_paired` | 4 | F46, M102, M52, M95 |
| `inv_replids_distinct` | 4 | F46, M102, M52, M95 |
| `inv_second_offset_not_above_live` | 4 | F46, F58, M17, M52 |
| `inv_backlog_floor_sound` | 4 | F46, I07, M17, M52 |
| `inv_ack_never_above_live_head` | 4 | F46, M52, M84, M88 |
| `inv_one_session_per_announced_identity` | 4 | F46, I08, M22, M52 |
| `inv_link_points_at_a_live_session` | 4 | F46, F56, M20, M52 |
| `inv_partial_grant_sound` | 3 | F12, F46, M52 |
| `inv_payload_covers_grant` | 3 | F46, M17, M52 |
| `inv_restart_pairs_offset` | 3 | F46, F60, M52 |
| `inv_failover_window_whole` | 3 | F46, M52, T01 |
| `inv_superseded_records_no_departure` | 2 | F46, M52 |


### Prediction vs. observation

Measured against the model **as committed in `82a4ee22`** (before gap closure), over all 204
pre-registered predictions:

| Outcome | Rows |
|---|---:|
| predicted a specific catcher, and that exact catcher fired | 86 |
| predicted a specific catcher, caught by a *different* oracle | 26 |
| predicted MISS/N-A, and it was missed | 59 |
| predicted MISS/N-A, but **caught anyway** (the prediction was too pessimistic) | 6 |
| predicted a specific catcher, but **MISSED** (the prediction was too optimistic) | 27 |

The last row is the battery's actual yield: **27 places where the model's author expected a property
to be load-bearing and it was not**. Seventeen of them are now closed by the gap-closure work
(F11, M03, M13, M18, M19, M27, M31, M33, M46, M62, M66, M67, M87, M92, M94, M98, M99); the remaining
ten — F01, F33, F34, M06, M36, M53, M69, M71, M74, I09 — are the equivalent mutants of class 5 plus
I09, each now carrying a tripwire or a structural argument.

The 6 pessimistic rows are worth naming because they say the existing surface is stronger than its
author believed: F08 (the inclusive `+CONTINUE` boundary — `partialResyncViaReplid2WindowTest`),
F72, M14, M22, M38, M108.

The 26 "different oracle" rows are not failures of prediction so much as evidence of **redundancy**:
a mutation predicted to trip an invariant instead broke a scenario test first (`quint test` runs
first and is deterministic, so it wins whenever both would fire).


---

## Final gates

Run against the closed model, after the last row was restored.

```
$ quint test specs/quint/replication_fullsync.qnt
  38 passing (2224ms)

$ quint run specs/quint/replication_fullsync.qnt --invariants <all 28> \
    --max-samples 4000 --max-steps 40 --seed 0x{1,2,3}
[ok] No violation found (6094ms at 656 traces/second).   # seed 0x1
[ok] No violation found (6078ms at 658 traces/second).   # seed 0x2
[ok] No violation found (6134ms at 652 traces/second).   # seed 0x3
```

Witnesses at the same budget (4000×40, seed `0x1`) — **none is at 0 traces**, which is the standing
check the class-3 rows depend on:

| Witness | traces | Witness | traces |
|---|---:|---|---:|
| `witnessPromotedWithWindow` | 3920 | `witnessAckRecorded` | 1390 |
| `witnessBacklogEvicted` | 3902 | `witnessWriteDuringTransfer` | 1206 |
| `witnessFullSyncStreaming` | 2249 | `witnessTailApplied` | 377 |
| `witnessPartialGrant` | 1992 | `witnessSpliceAbandoned` | 251 |
| `witnessUncleanRestartReminted` | 1818 | `witnessSuperseded` | 216 |
| `witnessCorpseSession` | 1794 | `witnessReappliedOverlap` | 171 |
| `witnessAckIgnored` | 1751 | `witnessPartialViaSecondary` | 125 |
| `witnessEvictionForcedFullResync` | 1552 | `witnessSettleDiscardedFrames` | 113 |

Working tree, scoped to this model:

```
$ git diff HEAD --stat -- specs/quint/replication_fullsync*
 specs/quint/replication_fullsync.qnt | 493 +++++++++++++++++++++++++++++++++-
 1 file changed, 491 insertions(+), 2 deletions(-)
```

Only the properties module changed, and only additively (the 2 deleted lines are the two existing
tests' closing assertions, replaced by longer ones). `_types`, `_logic` and `_machine` are
**byte-identical to `82a4ee22`** — verified by direct byte comparison against `git show
82a4ee22:specs/quint/<file>`, not by `git diff` (in this shared tree a concurrent lefthook stages
files mid-run, and `git diff` compares against the index).

### Reproducing

Drivers are committed at `.scratch/formal-spec/battery-drivers/t10b/` (`README.md` explains each
script and the paths that must be repointed):

```bash
eval "$(mise activate bash)"           # quint 0.32.0; bash, not zsh
python3 run_battery.py                 # pass 1: every row, 500x20, seeds 0x1/0x2
python3 escalate.py                    # every green row again at 4000x40, seeds 0x1/0x2/0x3
BATTERY_RESULTS=fullsync_results2.json BATTERY_ESC=escalation2.json \
  python3 run_battery.py $(cat missed_ids.txt) && python3 escalate.py   # pass 2, closed model
python3 witness_rows.py M112 M113 M114 # witness-count evidence for the step-unwiring rows
python3 assemble.py                    # regenerate this report
```

Single row by hand:

```bash
python3 run_battery.py M49             # mutate, run both oracles, restore, verify
```

### Follow-ups this battery leaves open

1. **A witness floor gate.** Class 3 (M112/M113/M114) is only observable as a witness collapsing to
   0 traces, and nothing in `just quint-*` fails on that today. A `--witnesses`-based gate asserting
   every witness stays above a floor would convert the whole class to CAUGHT and would also protect
   the 16 witnesses this model already carries.
2. **M61's distinguishing trace.** Whether a replica can acquire a history *without* a full-arm
   splice (install payload → abandon before the trailer → resume partially) is an open reachability
   question in this model. If it is reachable, `coverage.fullSyncStreaming` gains a negative
   assertion and M61 closes; if it is not, the arm conjunct in `spliceStreamAt` is provably
   redundant and deserves its own tripwire.
3. **I09's tightness.** `inv_identity_pair_monotone` holds without its `prev.primary` antecedent at
   4000×40 × 3 seeds, because promotion always mints a strictly greater generation and the ordering
   is lexicographic. Widening it is a *ruling* question (issue-24 wrote the antecedent deliberately),
   so it is raised, not taken.


---

## Addendum — 2026-08-20: rulings R4 / R5 / R6

**Authority**: `.scratch/formal-spec/2026-08-19-quint-completeness-campaign.md`, § "2026-08-20
post-execution rulings" — R4 (checkpoint-cut overlap), R5 (overshipped-tail residue), R6
(`inv_identity_pair_monotone` widening). Model + spec changes landed at commit `a2334b00`.

This section is **append-only**. Nothing recorded above is rewritten: row I09's original entry and
its honest-miss analysis stand as the record of what was observed *before* the ruling; the flip is
recorded here.

| | after gap closure | after R4/R5/R6 |
|---|---:|---:|
| invariants | 28 | 30 |
| witnesses | 16 | 17 |
| `run` tests | 38 | 38 |

New invariants: `inv_reapply_is_a_noop`, `inv_no_forked_tail_above_the_claim`.
New witness: `witnessOvershipTruncated`.
New coverage ghost: `coverage.overshipTruncated` (replaces `coverage.forkedTailReplaced`, whose
behaviour R5 forbids). New defect ghosts: `defects.forkedTailApplied`, `defects.reapplyChangedData`,
`defects.replayRegressedApplied`.
Widened invariant: `inv_identity_pair_monotone` — antecedent `(prev.primary and cur.role == Primary)`
→ `(cur.role == Primary)`.

### Pre-registered rows

Protocol identical to the passes above: exactly one textual site per row (replacement count asserted
== 1), `quint typecheck` → `quint test` → `quint run --max-samples=4000 --max-steps=40` over all 30
invariants for seeds `0x1`/`0x2`/`0x3`, per-invariant bisection only on the first red seed, restore
from a pristine copy and `cmp`-verify byte-identity before the next row. Final line of the driver:
`ALL RESTORED CLEAN`, re-verified afterwards against both `pristine/` and `HEAD`.

| id | site | mutation | expectation | verdict | caught by |
|---|---|---|---|---|---|
| R5-1 | `_logic`: `truncateAboveClaim` — the truncation itself | `if (claim < d.length()) d.slice(0, claim) else d` → `d` | R5's rule deleted: the forked tail survives the accept | **CAUGHT-T + CAUGHT-P** | `overshippedTailMeetsFailoverTest`; seed 0x1: `inv_no_forked_tail_above_the_claim` |
| R5-2 | `_logic`: `truncateAboveClaim` — the cut is at the claim, not one above | `d.slice(0, claim) else d` → `d.slice(0, claim + 1) else d` | off-by-one: one forked position is kept | **CAUGHT-T + CAUGHT-P** | `overshippedTailMeetsFailoverTest`; seed 0x1: `inv_no_forked_tail_above_the_claim` |
| R5-3 | `_machine`: `psyncRequestAs` — the accept calls the truncation | `data: if (arm == PartialGrant) truncateAboveClaim(rn.data, rn.applied) else rn.data,` → `data: rn.data,` | step-unwiring: the rule exists but the accept never applies it | **CAUGHT-T + CAUGHT-P** | `overshippedTailMeetsFailoverTest`; seed 0x1: `inv_no_forked_tail_above_the_claim` |
| R4-1 | `_logic`: `applyFrameToData` — a replayed frame writes its own id | `d.replaceAt(k - 1, w)` → `d.replaceAt(k - 1, NO_HISTORY)` | re-delivery stops being a no-op: replay corrupts the position it re-writes | **CAUGHT-T + CAUGHT-P** | `writeDuringTransferSurvivesTest`; seed 0x1: `inv_reapply_is_a_noop`, `inv_no_acked_write_lost_across_fullsync` |
| R4-2 | `_machine`: `applyFrameAt` — the next frame is one above applied | `val k = rn.applied + 1` → `val k = rn.applied` | replay regresses below the applied head | **CAUGHT-T + CAUGHT-P** | `writeDuringTransferSurvivesTest`, `overshippedTailMeetsFailoverTest`; seed 0x1: all 30 invariants |
| R6-1 | `_machine`: `promoteAs` — promotion mints a fresh generation | `applyPromote(nn, ctl.next_gen)` → `applyPromote(nn, nn.replid)` | promotion reuses its generation, so the offset rewind is no longer dominated | **CAUGHT-T + CAUGHT-P** | `promotionMidSyncTest`, `partialResyncViaReplid2WindowTest`, `windowCheckedAtAppliedTest`; seed 0x1: `inv_identity_pair_monotone` + 4 others |
| R6-2 | main: `inv_identity_pair_monotone` — restore the dropped antecedent | `(cur.role == Primary)` → `(prev.primary and cur.role == Primary)` | pre-registered N/A: re-narrowing an invariant is a weakening, so nothing can go red | **N/A** (green, as pre-registered) | — |

R4-2 going red on every invariant is expected and blunt — regressing the apply index breaks the
model's basic offset arithmetic. It is retained as the step-unwiring row for the R4 pair; R4-1 is the
one that isolates the no-op property, and it is the row that proves `inv_reapply_is_a_noop` is not
vacuous.

R6-2 carries no information on its own — it is the mirror of I09 and is recorded so the pair
R6-1/R6-2 stands as the tightness record: the property is enforced by R6-1, and re-narrowing it costs
nothing observable, which is exactly why the widening had to be a ruling rather than a sampling
result.

### I09 is superseded by the ruling

R6 rules that the `(generation, offset)` pair is an operator-facing total order across **any** step
into Primary, promotion included. The widened form is now the shipped invariant, so I09's mutation
(drop the `prev.primary` antecedent) *is* the model. Its pass-1 **MISSED** verdict was a correct
observation — the mutation is behaviour-preserving — and it is superseded here rather than rewritten.
Follow-up 3 ("I09's tightness") is **closed**: the ruling was taken, not the sampling result, and
R6-1 supplies the forcing test the original analysis said was missing.

Scope note: the widening does **not** claim that `ReplicationId` bytes carry an order. Rust's
`generate_replication_id()` (`frogdb-server/crates/replication/src/state.rs:485`) mints 40 random hex
characters with no ordering relation. R6's gen-domination is scoped in `specs/replication.md`
TR-REPLICATION-021 to the succession chain (`master_replid2`/`second_repl_offset` →
`master_replid`/`master_repl_offset`); the model's `Generation` is an ordered abstraction of that
chain, not of the id.

### R4 — what `inv_reapply_is_a_noop` does and does not carry

`inv_reapply_is_a_noop` is falsifiable in the model (R4-1 kills it) because a write's value is a
`WriteId`, and re-applying the same id at the same position is literally idempotent. The Rust system
does not have that property in general: post-execution propagation is verbatim
(`frogdb-server/crates/core/src/shard/post_execution.rs:87-111` — "everything else propagates
verbatim", asserted deterministic, *not* idempotent), so a re-executed `INCR`/`LPUSH`/`APPEND` frame
changes the keyspace. The offset-addressed skips R4 names are real but **sender-side**
(`RingBuffer::extract_backlog`'s `cmd.offset > start`, `FeedSequencer::buffer`'s `frame.sequence >
self.resume_offset`) and keyed on the offset the replica *claimed*; the checkpoint-cut overship sits
strictly *above* that claim, and `apply.rs::consume_frames` has no replica-side offset skip at all.

The invariant is therefore an **abstraction guard**, not a proof of the Rust behaviour: it pins that
the model never smuggles a mutation into the reapply path, and the header block in
`replication_fullsync.qnt` records what the write-log abstraction cannot carry. FM-REPLICATION-001's
"deliberate non-guarantees" bullet was corrected in the same commit to name the real mechanism
instead of asserting idempotence. **This is a partial counterexample to R4's premise and is raised
for re-ruling, not resolved here.**

### R5's boundaries

`truncateAboveClaim` and `inv_no_forked_tail_above_the_claim` are model-side only:
`replica/connection.rs:300-316` accepts `PsyncArm::Continue` by shifting the replication id and
streaming, discarding nothing. The spec row naming the model is **TR-REPLICATION-034**, marked
*Not implemented* with `Pending | R5`. Two boundaries are recorded rather than closed:

1. **The promotion path is outside R5.** `applyPromote` does not truncate;
   `promotedOvershipDoesNotServeForkedTailTest` (M68) drives a node to Primary holding a dataset of
   length 2 with `recv = applied = 1`, and that residue survives the ruling untouched. R5 covers the
   partial-resync *accept*, not promotion. Noted in the model header.
2. **"Truncate above the claim" has no in-place form for a keyspace.** A replica cannot undo applied
   commands whose inverses it does not hold. The honest implementations are to refuse the partial arm
   when the replica's received head is above its claim, or to pair the trailer offset with the data
   the payload actually carries — recorded in TR-REPLICATION-034's Rulings cell.

### Follow-ups this addendum adds

4. **TR-REPLICATION-034 has no implementation.** Landing R5 in Rust needs one of the two options in
   its Rulings cell plus a tagged forcing test in `frogdb-replication`; the row stays *Not
   implemented* until then.
5. **R4's premise needs re-ruling.** The overship range sits above the claimed offset, so the
   sender-side skips do not cover it, and verbatim propagation makes re-execution non-idempotent for
   counter/append commands. Either the skip must become replica-side and offset-addressed against
   the *applied* head, or the checkpoint cut must not overship.
6. **The promotion-path residue (boundary 1) is unruled.** Whether a promoted node should truncate
   above its own `recv` is the same question R5 answered for the accept path, and it is open.

---

## Addendum — 2026-08-20: follow-up 2 resolved (M61 is reachable)

**Append-only.** Nothing above is rewritten: row M61's `MISSED` verdict and its class-6 analysis
stand as the record of what pass 2 observed. This section records the answer to the reachability
question that analysis left open.

**Verdict: the distinguishing trace is reachable, and it is not the shape the follow-up guessed.**
M61 is therefore an ordinary missing-assertion row, not an equivalent mutant, and it closes with one
additive `run` test. No model defect is implied.

### Where the original reasoning went wrong

The class-6 note argued the mutation is undetectable because "every replica in this model bootstraps
through a full snapshot", so `coverage.fullSyncStreaming` is always already true before any partial
splice. That conflates two different events:

- a replica **acquires a history** in `deliverPayloadAt` — `applyInstallPayload` writes
  `replid: s.granted_gen` along with `recv`/`applied`/`data`;
- the ghost **latches** in `spliceStreamAt`, one transition later.

Anything that ends the session in between leaves a replica holding a history that never took a
full-arm splice. Its next `psyncRequestAs` then evaluates `decideArm` against a real `replid`, and
when the window and the floor both admit it the arm is `PartialGrant` — so the replica's *first ever*
splice is a partial one and the ghost is still dark. `endSessionAt` is the cheapest such gap: its
only guard is `phase != Disconnected`, so it fires at `PayloadInstalled` and simply clears the link.
(The follow-up's own suggestion — "install the payload, then abandon the splice before the trailer" —
is not quite coherent, since `deliverPayloadAt` *is* the trailer install; `abandonSpliceAt` also
works from `PayloadInstalled`, but it needs the floor to have climbed past the grant offset, which
then blocks the partial arm on the same primary. The plain session end has no such tension.)

### The trace

Verified on a scratch copy of `specs/quint/` (quint 0.32.0), pristine model otherwise:

```
init
  → writeOnPrimaryAs(1) ×2                  P1: recv=applied=2, floor=0
  → psyncRequestAs(2, 1, true, 1)           arm=FullSnapshot, grant_offset=2
  → cutCheckpointAt(1) → deliverPayloadAt(1)  R2: replid=FIRST_GEN, recv=applied=2
                                            coverage.fullSyncStreaming still false
  → endSessionAt(1)                          R2.link=None, session 1 Disconnected
  → psyncRequestAs(2, 1, true, 2)           windowContains(P1, FIRST_GEN, 2) ∧ floorAdmits(P1, 2)
                                            ⇒ arm=PartialGrant, grant_offset=2, phase=Connecting
  → spliceStreamAt(2)                        spliceRangeAvailable(P1, 2): 2 >= P1.recv ⇒ Streaming
                                            arm == PartialGrant ⇒ ghost stays dark
```

End state also asserted lawful: `defects.spliceGap/spliceDiverged/grantBelowFloor/
grantForeignHistory/tornIdentity/forkedTailApplied` all false, and `inv_splice_continuity`,
`inv_applied_covered_by_data`, `inv_no_acked_write_lost_across_fullsync` all hold.

### Evidence

| check | result |
| --- | --- |
| probe `run` test on the pristine model | passes; whole suite 40/40 (39 existing + probe) |
| same probe with M61's mutation (`latch(…, true)`) applied to `spliceStreamAt` | probe is the **only** failure — 39 pass, 1 fail, at the `not(coverage.fullSyncStreaming)` assertion |
| randomized reachability, `--max-samples=4000 --max-steps=40 --seed=0x1`, witness `SIDS.exists(k => phase == Streaming ∧ arm == PartialGrant) ∧ not(coverage.fullSyncStreaming)` | witnessed in **520 / 4000 traces (13.0%)** |

So the state is not a corner the walk barely touches — one trace in eight already reaches it. The
existing suite misses M61 purely because no scenario asserts the ghost is *dark* anywhere.

### The closing test (verified, not landed)

Landing this in `specs/quint/replication_fullsync.qnt` is additive only — one `run` test, no new
invariant, ghost, or state field — and flips M61 `MISSED → CAUGHT`. It is **not** landed here: the
brief for this investigation was to answer the question, not to edit the model. Verbatim, as run:

```quint
  run m61PartialSpliceWithoutFullSpliceTest =
    init
      .then(writeOnPrimaryAs(1))
      .then(writeOnPrimaryAs(1))
      .then(psyncRequestAs(2, 1, true, 1))
      .then(check(sessions.get(1).arm == FullSnapshot and sessions.get(1).grant_offset == 2))
      .then(cutCheckpointAt(1))
      .then(deliverPayloadAt(1))
      .then(check(and {
        nodes.get(2).replid == FIRST_GEN,
        nodes.get(2).applied == 2,
        not(coverage.fullSyncStreaming),
      }))
      .then(endSessionAt(1))
      .then(check(nodes.get(2).link == None and sessions.get(1).phase == Disconnected))
      .then(psyncRequestAs(2, 1, true, 2))
      .then(check(sessions.get(2).arm == PartialGrant and sessions.get(2).grant_offset == 2))
      .then(spliceStreamAt(2))
      .then(check(and {
        sessions.get(2).phase == Streaming,
        not(coverage.fullSyncStreaming),
      }))
```

### Knock-on for F17

Row F17 (`grantedViaSecondary` over-latches `coverage.partialViaSecondary`) was deferred to M61 for
the same reason — "a negative coverage assertion … needs a trace this model cannot produce". That
premise is now void: the trace above ends on a partial grant served from the primary's **own** id,
which is exactly the same-id partial F17 wanted. Its remaining obstacle (whether the dropped
`optExists(p.replid2, …)` conjunct is genuinely equivalent at the call site) is a separate argument
and is untouched by this note, but "no such trace exists" is no longer part of it.

Follow-up 2 above is **resolved**; follow-ups 1 and 3 stand.
