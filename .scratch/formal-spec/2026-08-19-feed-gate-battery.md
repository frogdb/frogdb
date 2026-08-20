# Feed-gate session model — mutation battery (lifecycle steps 4–5)

Base commit: `f7234770` (model landed by T9b, unmodified at battery start).

Model files:

- `specs/quint/replication_feed_gate_types.qnt`
- `specs/quint/replication_feed_gate_logic.qnt`
- `specs/quint/replication_feed_gate_machine.qnt`
- `specs/quint/replication_feed_gate.qnt` (witnesses, invariants, run tests)

Authority: `.scratch/formal-spec/2026-08-19-quint-completeness-campaign.md` — the model
lifecycle makes a documented battery plus honest-miss closure mandatory. *Exhaustiveness of a
model's checking is its battery verdict table, not its invariant count.*

Format follows `.scratch/formal-spec/2026-08-19-admission-battery.md` (task T9a) and the
issue-31 Q4 re-verification battery.

Base surface: **18 invariants, 17 witnesses, 11 run tests**.

## Verdict vocabulary

| Verdict | Meaning |
|---|---|
| `CAUGHT-T` | `quint test` fails — a run test pins the mutated behaviour deterministically |
| `CAUGHT-P` | an invariant is violated under sampled `quint run` |
| `CAUGHT-T+P` | both oracles fire |
| `MISSED` | both oracles stay green at the escalated budget — needs honest-miss analysis |
| `PAIRED-GREEN` | a *paired* row: the model mutation is applied together with a widening of the clause that is supposed to catch it, and the catch disappears — which is what proves the clause is load-bearing |
| `PAIRED-STILL-CAUGHT` | the same pairing, but a *different* oracle still catches the mutation — the clause under test is not the only guard on that behaviour (redundant coverage, reported rather than assumed) |
| `X → Y (closed)` | the row's pass-1 verdict, then its pass-2 verdict after the gap-closure test was added |
| `N/A` | not a well-formed single edit |

A witness firing is **not** a verdict. The CI gate (`scripts/quint-models.sh`, `just
quint-run`) runs `quint test` plus `quint run --max-samples=200 --max-steps=20 --invariants
<all inv_*>`; witnesses are reported but never gate. A row whose only signal is a witness
count is therefore `MISSED`, and its closure is a run test that asserts the coverage flag.

## Mechanics (discipline actually followed)

Rows are defined in `scratchpad/t9c/rows.py` and executed by `scratchpad/t9c/run_battery.py`.
Per row:

1. every file is restored from a pristine pre-battery copy, then the row's **single exact
   text replacement** is applied to that pristine copy (never on top of another mutation).
   The driver refuses any pattern that does not occur exactly once — all 129 rows validated
   before the first run.
2. `quint test specs/quint/replication_feed_gate.qnt`
3. `quint run specs/quint/replication_feed_gate.qnt --max-samples=500 --max-steps=20
   --seed=<0x1|0x2|0x3> --invariants <all 18>`
4. on a violation, the same configuration is re-run **per invariant** to name the catcher
   (quint does not report which of a list was violated).
5. no violation and no test failure → escalate to `--max-samples=4000 --max-steps=40` on
   seeds `0x1` and `0x2` before `MISSED` is recorded.
6. restore from the pristine copy; `git --no-optional-locks diff --stat -- specs/quint/replication_feed_gate*`
   must be empty (the driver aborts otherwise). `git checkout -- specs/quint/` is never used:
   other agents hold concurrent WIP in that directory.

Budget justification: the CI budget (200×20) is measurably thin for this model — the prime
row M17 (`val held = gateHeld(armed)` → `false`) shows no violation at 200×20 but violates
promptly at 500×20 and at 2000×25. The battery therefore runs 500×20 across three seeds as
its base and 4000×40 across two seeds before declaring a miss. Every quint invocation goes
through `bash -c 'eval "$(mise activate bash)"; …'`; the repo shell is zsh, which does not
word-split the `--invariants` list.

Runs are scoped to this model only. `just quint-run` / `just quint-check` (whole directory)
were never invoked.

**Where each row ran.** Rows G01–M40 were executed against the working tree by
`run_battery.py` under exactly the discipline above. The tail rows (M41–M50, I01–I08, P1–P8)
and the whole of pass 2 were executed by `run_battery_sb.py` / `run_battery_p2.py`, which run
the identical mutation and oracle protocol against a *copy* of the model
(`scratchpad/t9c/sandbox_p1` and `sandbox_p2`: the four model files plus the three modules they
import, nothing else). Same quint binary, same budgets, same per-row restore. Two reasons:
the working tree is shared with other agents mid-battery, and a copy lets the second half of
the battery run concurrently with the first. The one row this changes nothing for is the
`git diff` check — a sandbox row cannot dirty the tree at all.

**Pass 2.** After the gap-closure tests were added, every row that pass 1 recorded as `MISSED`
and that a closure targets was re-run from a pristine copy of the **closed** model. The table's
`Verdict` column shows both verdicts for those rows.

## Battery table

Rows are grouped: **G** = guard conjuncts, **E** = effect/field updates in the logic module,
**M** = machine wiring (action guards, var writes, ghost latches, `step`, `init`), **I** =
invariant clauses (mutating the property itself, to show the clause is load-bearing), **P** =
paired clause-load-bearing checks.

`Expected` is the prediction registered **before** any mutation was run; `Verdict` is what the
oracles actually returned.

### Guard conjuncts (`_logic`)

| Row | Where | Mutation | Expected | Verdict | Catcher |
|---|---|---|---|---|---|
| G01 | logic: canHandoff / stage conjunct | `ss.stage == Handoff, ⏎     not(held),` → `not(held),` | CAUGHT-P | CAUGHT-P | inv_ack_never_above_the_wire, inv_buffer_ahead_of_the_wire, inv_graceful_end_flushed_everything, inv_nothing_held_is_dropped, inv_stage_shape, inv_wire_is_a_prefix_of_accepted |
| G02 | logic: canHandoff / gate conjunct (FM-CLUSTER-097 PSYNC-into-window) | `ss.stage == Handoff, ⏎     not(held),` → `ss.stage == Handoff,` | CAUGHT-T | CAUGHT-T+P | handoffWaitsForReleaseTest / inv_no_ship_inside_barrier_window |
| G03 | logic: canReceiveFrame / stage conjunct | `ss.stage == Receiving or ss.stage == Holding, ⏎     ss.cursor < live,` → `ss.cursor < live,` | CAUGHT-P | CAUGHT-P | inv_departure_record_backed_by_an_ended_session, inv_nothing_held_is_dropped, inv_stage_shape |
| G04 | logic: canReceiveFrame / cursor < live | `ss.cursor < live, ⏎     live - ss.cursor <= CHANNEL_CAP,` → `live - ss.cursor <= CHANNEL_CAP,` | CAUGHT-P | CAUGHT-P | inv_no_ship_inside_barrier_window, inv_offsets_within_live |
| G05 | logic: canReceiveFrame / CHANNEL_CAP conjunct | `ss.cursor < live, ⏎     live - ss.cursor <= CHANNEL_CAP,` → `ss.cursor < live,` | MISSED | MISSED → **CAUGHT-T** (closed) | channelOverrunRefusesIntakeTest |
| G06 | logic: canConsult / stage conjunct | `pure def canConsult(ss: SessionState): bool = ss.stage == Consulting` → `pure def canConsult(ss: SessionState): bool = ss.stage != Ended` | CAUGHT-P | CAUGHT-P | inv_nothing_held_is_dropped, inv_stage_shape |
| G07 | logic: canObserveRelease / stage conjunct | `pure def canObserveRelease(ss: SessionState): bool = ss.stage == Holding` → `pure def canObserveRelease(ss: SessionState): bool = ss.stage != Ended` | CAUGHT-P | CAUGHT-P | inv_nothing_held_is_dropped, inv_stage_shape |
| G08 | logic: canSourceClosed / stage conjunct | `ss.stage == Receiving or ss.stage == Holding, ⏎     closed,` → `closed,` | CAUGHT-P | CAUGHT-P | inv_graceful_end_flushed_everything |
| G09 | logic: canSourceClosed / source actually closed | `closed, ⏎     ss.cursor == live,` → `ss.cursor == live,` | MISSED | MISSED → **CAUGHT-T** (closed) | closeAndLagGuardsRefuseAHealthySessionTest |
| G10 | logic: canSourceClosed / cursor caught up | `closed, ⏎     ss.cursor == live,` → `closed,` | MISSED | MISSED → **CAUGHT-T** (closed) | closeAndLagGuardsRefuseAHealthySessionTest |
| G11 | logic: canSourceLagged / stage conjunct | `ss.stage == Receiving or ss.stage == Holding, ⏎     live - ss.cursor > CHANNEL_CAP,` → `live - ss.cursor > CHANNEL_CAP,` | CAUGHT-P | MISSED → **CAUGHT-T** (closed) | midSendSessionIsNotClassifiedOutFromUnderItsFrameTest |
| G12 | logic: canSourceLagged / overrun comparison | `live - ss.cursor > CHANNEL_CAP,` → `true,` | MISSED | MISSED → **CAUGHT-T** (closed) | closeAndLagGuardsRefuseAHealthySessionTest |
| G13 | logic: canConfirmSent / in_flight conjunct | `ss.stage == Sending, ⏎     ss.in_flight != None,` → `ss.stage == Sending,` | MISSED | MISSED | — |
| G14 | logic: canSendFailed / stage conjunct | `pure def canSendFailed(ss: SessionState): bool = ss.stage == Sending` → `pure def canSendFailed(ss: SessionState): bool = ss.stage != Ended` | MISSED | MISSED → **CAUGHT-T** (closed) | midSendSessionIsNotClassifiedOutFromUnderItsFrameTest |
| G15 | logic: canSupersede / newer past handoff | `newer.stage != Handoff, newer.stage != Ended,` → `newer.stage != Ended,` | MISSED | MISSED | — |
| G16 | logic: canSupersede / newer still live | `newer.stage != Handoff, newer.stage != Ended,` → `newer.stage != Handoff,` | MISSED | MISSED → **CAUGHT-T** (closed) | supersessionAndAckGuardsTest |
| G17 | logic: canSupersede / older past handoff | `older.stage != Handoff, older.stage != Ended,` → `older.stage != Ended,` | MISSED | MISSED | — |
| G18 | logic: canSupersede / older still live | `older.stage != Handoff, older.stage != Ended,` → `older.stage != Handoff,` | CAUGHT-P | CAUGHT-P | inv_departure_record_backed_by_an_ended_session |
| G19 | logic: canSupersede / newest-wins ordering | `newer.stream_seq > older.stream_seq,` → `newer.stream_seq != older.stream_seq,` | MISSED | MISSED → **CAUGHT-T** (closed) | supersessionAndAckGuardsTest |
| G20 | logic: canSupersede / identity match | `optExists(newer.identity, i => announced(older, i)),` → `true,` | CAUGHT-P | CAUGHT-P | inv_unannounced_is_never_superseded |
| G21 | logic: announced / unannounced never matches (issue 22) | `pure def announced(ss: SessionState, i: int): bool = optExists(ss.identity, x => x == i)` → `pure def announced(ss: SessionState, i: int): bool = optExists(ss.identity, x => x == i) or ss.identity == None` | CAUGHT-P | CAUGHT-P | inv_duplicate_identity_is_always_kickable, inv_unannounced_is_never_superseded |
| G22 | logic: canAck / not in handoff | `ss.stage != Handoff, ⏎     ss.stage != Ended,` → `ss.stage != Ended,` | MISSED | MISSED → **CAUGHT-T** (closed) | supersessionAndAckGuardsTest |
| G23 | logic: canAck / not after end | `ss.stage != Handoff, ⏎     ss.stage != Ended,` → `ss.stage != Handoff,` | MISSED | MISSED → **CAUGHT-T** (closed) | supersessionAndAckGuardsTest |
| G24 | logic: gateHeld / any armed barrier holds (FM-CLUSTER-097 node-wide) | `BARRIERS.exists(b => armed.get(b) != None)` → `BARRIERS.forall(b => armed.get(b) != None)` | CAUGHT-P | CAUGHT-T+P | holdThenDrainTest, barrierWhileHeldTest, handoffWaitsForReleaseTest, closeInsideWindowDrainsThenEndsGracefulTest, sendFailedAbandonsBufferTest, nodeWideHoldTest / inv_no_ship_inside_barrier_window |

### Effects / field updates (`_logic`)

| Row | Where | Mutation | Expected | Verdict | Catcher |
|---|---|---|---|---|---|
| E01 | logic: applyEndWith / buffer dropped at end | `{ ...ss, stage: Ended, held: List(), in_flight: None, ending: Some(d) }` → `{ ...ss, stage: Ended, in_flight: None, ending: Some(d) }` | CAUGHT-T | CAUGHT-T+P | sendFailedAbandonsBufferTest / inv_stage_shape |
| E02 | logic: applyEndWith / in-flight cleared at end | `{ ...ss, stage: Ended, held: List(), in_flight: None, ending: Some(d) }` → `{ ...ss, stage: Ended, held: List(), ending: Some(d) }` | CAUGHT-P | CAUGHT-P | inv_stage_shape |
| E03 | logic: applyEndWith / departure recorded on the session | `{ ...ss, stage: Ended, held: List(), in_flight: None, ending: Some(d) }` → `{ ...ss, stage: Ended, held: List(), in_flight: None }` | CAUGHT-P | CAUGHT-T+P | supersededKickWritesNoDepartureTest, sendFailedAbandonsBufferTest, lagBreachEndsAtTheFrameTest / inv_departure_matches_its_cause |
| E04 | logic: applyEnd / cause recorded | `applyEndWith({ ...ss, cause: Some(c) }, d)` → `applyEndWith(ss, d)` | CAUGHT-P | CAUGHT-T+P | supersededKickWritesNoDepartureTest, lagBreachEndsAtTheFrameTest / inv_departure_matches_its_cause |
| E05 | logic: applyFlush / drain condition | `if (ss.held.length() > 0)` → `if (ss.held.length() > 1)` | CAUGHT-T | CAUGHT-T+P | holdThenDrainTest, barrierWhileHeldTest, handoffDedupTest, closeInsideWindowDrainsThenEndsGracefulTest, lagBreachEndsAtTheFrameTest / inv_graceful_end_flushed_everything |
| E06 | logic: applyFlush / frame consumed from the buffer | `{ ...ss, stage: Sending, in_flight: Some(ss.held.head()), held: ss.held.tail() }` → `{ ...ss, stage: Sending, in_flight: Some(ss.held.head()), held: ss.held }` | CAUGHT-P | CAUGHT-T+P | holdThenDrainTest, barrierWhileHeldTest, closeInsideWindowDrainsThenEndsGracefulTest, sendFailedAbandonsBufferTest / inv_buffer_ahead_of_the_wire, inv_nothing_held_is_dropped, inv_offsets_strictly_increase, inv_wire_is_a_prefix_of_accepted |
| E07 | logic: applyFlush / the frame shipped is the frame buffered | `{ ...ss, stage: Sending, in_flight: Some(ss.held.head()), held: ss.held.tail() }` → `{ ...ss, stage: Sending, in_flight: Some(ss.held.head() + 1), held: ss.held.tail() }` | CAUGHT-P | CAUGHT-T+P | holdThenDrainTest, barrierWhileHeldTest, handoffDedupTest, closeInsideWindowDrainsThenEndsGracefulTest, sendFailedAbandonsBufferTest, lagBreachEndsAtTheFrameTest / inv_no_ship_inside_barrier_window, inv_offsets_within_live, inv_wire_is_a_prefix_of_accepted |
| E08 | logic: applyFlush / pending departure honoured when empty | `\| Some(d) => applyEndWith(ss, d)` → `\| Some(d) => { ...ss, stage: Receiving, in_flight: None }` | CAUGHT-T | CAUGHT-T | laggedOverrunDisconnectTest, closeInsideWindowDrainsThenEndsGracefulTest |
| E09 | logic: applyFlush / idle when nothing pending | `\| None => { ...ss, stage: Receiving, in_flight: None }` → `\| None => { ...ss, stage: Holding, in_flight: None }` | CAUGHT-T | CAUGHT-T | holdThenDrainTest |
| E10 | logic: applyClassify / classify-then-DRAIN (flush before you end) | `applyFlush({ ...ss, ending: Some(d), cause: Some(c) })` → `applyEndWith({ ...ss, cause: Some(c) }, d)` | CAUGHT-T | CAUGHT-T+P | closeInsideWindowDrainsThenEndsGracefulTest / inv_graceful_end_flushed_everything |
| E11 | logic: recordFor / a record needs an ended session | `if (post.stage != Ended) prev else` → `if (false) prev else` | CAUGHT-P | CAUGHT-P | inv_departure_record_backed_by_an_ended_session |
| E12 | logic: recordFor / Superseded writes nothing (issue 22) | `\| Some(d) => if (d == Superseded) prev else Some(d)` → `\| Some(d) => Some(d)` | CAUGHT-T | CAUGHT-T+P | supersededKickWritesNoDepartureTest / inv_departure_record_is_never_superseded, inv_superseded_writes_no_departure |
| E13 | logic: recordFor / None arm | `\| None => prev ⏎       }` → `\| None => Some(Lost) ⏎       }` | MISSED | MISSED | — |
| E14 | logic: applyHandoff / resume_offset recorded (dedup basis) | `{ ...ss, stage: Receiving, resume_offset: r, identity: ident, stream_seq: seq,` → `{ ...ss, stage: Receiving, resume_offset: 0, identity: ident, stream_seq: seq,` | CAUGHT-T | CAUGHT-T+P | handoffDedupTest / inv_buffer_ahead_of_the_wire, inv_offsets_strictly_increase |
| E15 | logic: applyHandoff / announced identity stored | `{ ...ss, stage: Receiving, resume_offset: r, identity: ident, stream_seq: seq,` → `{ ...ss, stage: Receiving, resume_offset: r, identity: None, stream_seq: seq,` | CAUGHT-T | CAUGHT-T | supersededKickWritesNoDepartureTest |
| E16 | logic: applyHandoff / arrival order stored | `{ ...ss, stage: Receiving, resume_offset: r, identity: ident, stream_seq: seq,` → `{ ...ss, stage: Receiving, resume_offset: r, identity: ident, stream_seq: 0,` | CAUGHT-T | CAUGHT-T+P | supersededKickWritesNoDepartureTest / inv_duplicate_identity_is_always_kickable |
| E17 | logic: applyHandoff / stage after replay | `{ ...ss, stage: Receiving, resume_offset: r, identity: ident, stream_seq: seq,` → `{ ...ss, stage: Consulting, resume_offset: r, identity: ident, stream_seq: seq,` | CAUGHT-T | CAUGHT-T | holdThenDrainTest, barrierWhileHeldTest, laggedOverrunDisconnectTest, handoffDedupTest, closeInsideWindowDrainsThenEndsGracefulTest, sendFailedAbandonsBufferTest, lagBreachEndsAtTheFrameTest, nodeWideHoldTest |
| E18 | logic: applyHandoff / replay seeds the accepted history | `accepted: range(1, r + 1), sent: range(1, r + 1) }` → `accepted: List(), sent: range(1, r + 1) }` | CAUGHT-P | CAUGHT-P | inv_nothing_held_is_dropped, inv_wire_is_a_prefix_of_accepted |
| E19 | logic: applyHandoff / replay reaches the wire | `accepted: range(1, r + 1), sent: range(1, r + 1) }` → `accepted: range(1, r + 1), sent: List() }` | CAUGHT-T | CAUGHT-T+P | handoffDedupTest, handoffWaitsForReleaseTest, ackAboveLiveIsIgnoredTest / inv_nothing_held_is_dropped |
| E20 | logic: applyReceiveFrame / next frame is cursor+1 | `val f = ss.cursor + 1` → `val f = ss.cursor + 2` | CAUGHT-P | CAUGHT-T+P | holdThenDrainTest, barrierWhileHeldTest, handoffDedupTest, closeInsideWindowDrainsThenEndsGracefulTest, sendFailedAbandonsBufferTest, lagBreachEndsAtTheFrameTest / inv_no_ship_inside_barrier_window, inv_offsets_within_live |
| E21 | logic: applyReceiveFrame / cursor advances | `val advanced = { ...ss, cursor: f, stage: Consulting }` → `val advanced = { ...ss, stage: Consulting }` | CAUGHT-P | CAUGHT-T+P | barrierWhileHeldTest, handoffDedupTest, closeInsideWindowDrainsThenEndsGracefulTest, sendFailedAbandonsBufferTest / inv_buffer_ahead_of_the_wire, inv_offsets_strictly_increase |
| E22 | logic: applyReceiveFrame / intake goes to the gate | `val advanced = { ...ss, cursor: f, stage: Consulting }` → `val advanced = { ...ss, cursor: f, stage: Receiving }` | CAUGHT-T | CAUGHT-T | holdThenDrainTest, barrierWhileHeldTest, handoffDedupTest, closeInsideWindowDrainsThenEndsGracefulTest, sendFailedAbandonsBufferTest, lagBreachEndsAtTheFrameTest, nodeWideHoldTest |
| E23 | logic: applyReceiveFrame / replay dedup filter (FM-REPLICATION-015) | `if (f > ss.resume_offset)` → `if (true)` | CAUGHT-T | CAUGHT-T+P | handoffDedupTest / inv_buffer_ahead_of_the_wire, inv_no_resend_below_resume, inv_offsets_strictly_increase |
| E24 | logic: applyReceiveFrame / accepted frame is buffered | `{ ...advanced, held: ss.held.append(f), accepted: ss.accepted.append(f) }` → `{ ...advanced, accepted: ss.accepted.append(f) }` | CAUGHT-P | CAUGHT-T+P | holdThenDrainTest, barrierWhileHeldTest, handoffDedupTest, closeInsideWindowDrainsThenEndsGracefulTest, sendFailedAbandonsBufferTest, lagBreachEndsAtTheFrameTest / inv_nothing_held_is_dropped |
| E25 | logic: applyReceiveFrame / accepted history recorded | `{ ...advanced, held: ss.held.append(f), accepted: ss.accepted.append(f) }` → `{ ...advanced, held: ss.held.append(f) }` | CAUGHT-P | CAUGHT-P | inv_nothing_held_is_dropped, inv_wire_is_a_prefix_of_accepted |
| E26 | logic: dedupsFrame / coverage predicate | `pure def dedupsFrame(ss: SessionState): bool = ss.cursor + 1 <= ss.resume_offset` → `pure def dedupsFrame(ss: SessionState): bool = false` | CAUGHT-T | CAUGHT-T | handoffDedupTest |
| E27 | logic: applyHold / a held session parks | `pure def applyHold(ss: SessionState): SessionState = { ...ss, stage: Holding }` → `pure def applyHold(ss: SessionState): SessionState = { ...ss, stage: Receiving }` | CAUGHT-T | CAUGHT-T | holdThenDrainTest, barrierWhileHeldTest, closeInsideWindowDrainsThenEndsGracefulTest, sendFailedAbandonsBufferTest, nodeWideHoldTest |
| E28 | logic: applySent / the landed frame enters the wire history | `\| Some(o) => ss.sent.append(o)` → `\| Some(o) => ss.sent` | CAUGHT-P | CAUGHT-T+P | holdThenDrainTest, barrierWhileHeldTest, handoffDedupTest, closeInsideWindowDrainsThenEndsGracefulTest, lagBreachEndsAtTheFrameTest / inv_graceful_end_flushed_everything, inv_nothing_held_is_dropped |
| E29 | logic: applySent / in-flight cleared on landing | `val after = { ...ss, sent: landed, in_flight: None }` → `val after = { ...ss, sent: landed }` | CAUGHT-P | MISSED | — |
| E30 | logic: applySent / a lag breach ends the link | `if (breach) applyEnd(after, Lost, LagBreach) else applyFlush(after)` → `applyFlush(after)` | CAUGHT-T | CAUGHT-T | lagBreachEndsAtTheFrameTest |
| E31 | logic: applySent / a lag breach is Lost, not Graceful | `if (breach) applyEnd(after, Lost, LagBreach) else applyFlush(after)` → `if (breach) applyEnd(after, Graceful, LagBreach) else applyFlush(after)` | CAUGHT-P | CAUGHT-T+P | lagBreachEndsAtTheFrameTest / inv_departure_matches_its_cause |
| E32 | logic: admitAck / ignore, never clamp (issue 21 AMENDED) | `if (ackIgnored(reported, live)) ss.acked else max2(ss.acked, reported)` → `if (ackIgnored(reported, live)) live else max2(ss.acked, reported)` | CAUGHT-T | CAUGHT-T+P | ackAboveLiveIsIgnoredTest / inv_ack_ignored_not_clamped, inv_ack_never_above_the_wire |
| E33 | logic: admitAck / watermark never retreats (TR-REPLICATION-033) | `if (ackIgnored(reported, live)) ss.acked else max2(ss.acked, reported)` → `if (ackIgnored(reported, live)) ss.acked else reported` | MISSED | MISSED → **CAUGHT-T** (closed) | ackWatermarkNeverRetreatsTest |
| E34 | logic: ackIgnored / above-live boundary | `pure def ackIgnored(reported: Offset, live: Offset): bool = reported > live` → `pure def ackIgnored(reported: Offset, live: Offset): bool = reported > live + 1` | CAUGHT-T | CAUGHT-T+P | ackAboveLiveIsIgnoredTest / inv_ack_never_above_live, inv_ack_never_above_the_wire |
| E35 | logic: gracefulLoss / Ended clause | `post.stage == Ended, ⏎     endsGracefully(post),` → `endsGracefully(post),` | MISSED | CAUGHT-T+P | closeInsideWindowDrainsThenEndsGracefulTest / inv_graceful_end_flushed_everything |
| E36 | logic: gracefulLoss / graceful clause | `post.stage == Ended, ⏎     endsGracefully(post),` → `post.stage == Ended,` | CAUGHT-P | CAUGHT-P | inv_graceful_end_flushed_everything |
| E37 | logic: gracefulLoss / in-flight-landed exemption | `pre.in_flight != None and post.sent.length() == pre.sent.length(),` → `pre.in_flight != None,` | CAUGHT-P | CAUGHT-T+P | closeInsideWindowDrainsThenEndsGracefulTest / inv_graceful_end_flushed_everything |
| E38 | logic: isPrefix / length clause | `short.length() <= long.length(), ⏎     short.indices().forall(i => short.nth(i) == long.nth(i)),` → `short.indices().forall(i => short.nth(i) == long.nth(i)),` | MISSED | MISSED | — |
| E39 | logic: wireMax / fold over the whole wire | `pure def wireMax(ss: SessionState): Offset = ss.sent.foldl(0, (a, x) => max2(a, x))` → `pure def wireMax(ss: SessionState): Offset = 0` | CAUGHT-P | CAUGHT-T | ackAboveLiveIsIgnoredTest |

### Machine wiring (`_machine`)

| Row | Where | Mutation | Expected | Verdict | Catcher |
|---|---|---|---|---|---|
| M01 | machine: armBarrier / floor is the live offset at arm time | `armed' = armed.set(b, Some(live)),` → `armed' = armed.set(b, Some(0)),` | CAUGHT-P | CAUGHT-P → **CAUGHT-T+P** (closed) | barrierFloorIsTheLiveOffsetAtArmTimeTest, drainAtTheFloorIsNotADivergenceTest / inv_no_ship_inside_barrier_window |
| M02 | machine: armBarrier / floor not inflated | `armed' = armed.set(b, Some(live)),` → `armed' = armed.set(b, Some(MAX_WRITES)),` | MISSED | CAUGHT-T | closeInsideWindowDrainsThenEndsGracefulTest, barrierFloorIsTheLiveOffsetAtArmTimeTest, drainAtTheFloorIsNotADivergenceTest |
| M03 | machine: armBarrier / re-arm refused | `armed.get(b) == None,` → `true,` | MISSED | MISSED → **CAUGHT-T** (closed) | barrierFloorIsTheLiveOffsetAtArmTimeTest |
| M04 | machine: releaseBarrier / actually disarms | `action releaseBarrier(b: BarrierId): bool = all { ⏎     armed.get(b) != None, ⏎     armed' = armed.set(b, None),` → `action releaseBarrier(b: BarrierId): bool = all { ⏎     armed.get(b) != None, ⏎     armed' = armed,` | CAUGHT-T | CAUGHT-T | holdThenDrainTest, barrierWhileHeldTest, handoffWaitsForReleaseTest, sendFailedAbandonsBufferTest |
| M05 | machine: releaseBarrier / coverage latch | `coverage' = { ...coverage, barrierReleased: latch(coverage.barrierReleased, true) },` → `coverage' = { ...coverage, barrierReleased: coverage.barrierReleased },` | CAUGHT-T | CAUGHT-T | barrierWhileHeldTest, handoffWaitsForReleaseTest |
| M06 | machine: lapseBarrier / actually disarms | `action lapseBarrier(b: BarrierId): bool = all { ⏎     armed.get(b) != None, ⏎     armed' = armed.set(b, None),` → `action lapseBarrier(b: BarrierId): bool = all { ⏎     armed.get(b) != None, ⏎     armed' = armed,` | CAUGHT-T | CAUGHT-T | barrierWhileHeldTest |
| M07 | machine: lapseBarrier / coverage latch | `coverage' = { ...coverage, barrierLapsed: latch(coverage.barrierLapsed, true) },` → `coverage' = { ...coverage, barrierLapsed: coverage.barrierLapsed },` | CAUGHT-T | CAUGHT-T | barrierWhileHeldTest |
| M08 | machine: armBarrier / overlapping-hold coverage condition | `overlappingHold: latch(coverage.overlappingHold, gateHeld(armed)),` → `overlappingHold: latch(coverage.overlappingHold, true),` | MISSED | MISSED → **CAUGHT-T** (closed) | barrierFloorIsTheLiveOffsetAtArmTimeTest |
| M09 | machine: writeFrame / no writes after the source closed | `live < MAX_WRITES, ⏎     not(source_closed),` → `live < MAX_WRITES,` | MISSED | MISSED → **CAUGHT-T** (closed) | closedSourceAcceptsNoMoreWritesTest |
| M10 | machine: writeFrame / live offset advances (TR-REPLICATION-030) | `live' = live + 1,` → `live' = live,` | CAUGHT-T | CAUGHT-T | holdThenDrainTest, barrierWhileHeldTest, laggedOverrunDisconnectTest, handoffDedupTest, handoffWaitsForReleaseTest, closeInsideWindowDrainsThenEndsGracefulTest, ackAboveLiveIsIgnoredTest, sendFailedAbandonsBufferTest, lagBreachEndsAtTheFrameTest, nodeWideHoldTest |
| M11 | machine: closeSource / latches the close | `source_closed' = true,` → `source_closed' = false,` | CAUGHT-T | CAUGHT-T | closeInsideWindowDrainsThenEndsGracefulTest |
| M12 | machine: handoffReplayedAs / replay bounded by live | `r <= live,` → `true,` | CAUGHT-P | MISSED → **CAUGHT-T** (closed) | handoffCannotReplayPastTheLiveHeadTest |
| M13 | machine: handoffReplayedAs / entering streaming clears the fence cell | `departure_record' = None,` → `departure_record' = departure_record,` | MISSED | MISSED → **CAUGHT-T** (closed) | enteringStreamingClearsTheFenceCellTest |
| M14 | machine: handoffReplayedAs / arrival counter advances | `next_seq' = next_seq + 1,` → `next_seq' = next_seq,` | CAUGHT-T | CAUGHT-T+P | supersededKickWritesNoDepartureTest / inv_duplicate_identity_is_always_kickable |
| M15 | machine: handoffReplayedAs / handoff-after-release coverage | `handoffAfterRelease: latch(coverage.handoffAfterRelease, ⏎           coverage.barrierReleased or coverage.barrierLapsed),` → `handoffAfterRelease: coverage.handoffAfterRelease,` | CAUGHT-T | CAUGHT-T | handoffWaitsForReleaseTest |
| M16 | machine: receiveFrame / dedup coverage latch | `handoffDeduped: latch(coverage.handoffDeduped, dedupsFrame(ss)),` → `handoffDeduped: coverage.handoffDeduped,` | CAUGHT-T | CAUGHT-T | handoffDedupTest |
| M17 | machine: consultGate / the gate is actually read | `val held = gateHeld(armed)` → `val held = false` | CAUGHT-P | CAUGHT-T | holdThenDrainTest, barrierWhileHeldTest, closeInsideWindowDrainsThenEndsGracefulTest, sendFailedAbandonsBufferTest, nodeWideHoldTest |
| M18 | machine: consultGate / the gate can open | `val held = gateHeld(armed)` → `val held = true` | CAUGHT-T | CAUGHT-T | holdThenDrainTest, barrierWhileHeldTest, handoffDedupTest, sendFailedAbandonsBufferTest, lagBreachEndsAtTheFrameTest |
| M19 | machine: consultGate / departure record on the flush-to-end path | `canConsult(ss), ⏎       sessions' = newSessions, ⏎       departure_record' = recordFor(departure_record, post),` → `canConsult(ss), ⏎       sessions' = newSessions, ⏎       departure_record' = departure_record,` | MISSED | MISSED | — |
| M20 | machine: consultGate / graceful-loss defect recompute | `gracefulEndDroppedFrames: ⏎           latch(defects.gracefulEndDroppedFrames, gracefulLoss(ss, post)), ⏎       }, ⏎       coverage' = { ...coverage, ⏎         heldFrame:` → `gracefulEndDroppedFrames: defects.gracefulEndDroppedFrames, ⏎       }, ⏎       coverage' = { ...coverage, ⏎         heldFrame:` | MISSED | MISSED | — |
| M21 | machine: consultGate / held-frame coverage condition | `heldFrame: latch(coverage.heldFrame, held and ss.held.length() > 0),` → `heldFrame: latch(coverage.heldFrame, held),` | MISSED | MISSED → **CAUGHT-T** (closed) | emptyBufferHoldLatchesNoCoverageTest |
| M22 | machine: consultGate / node-wide both-held coverage | `bothSessionsHeld: latch(coverage.bothSessionsHeld, ⏎           SESSIONS.forall(x => newSessions.get(x).stage == Holding)),` → `bothSessionsHeld: latch(coverage.bothSessionsHeld, ⏎           SESSIONS.exists(x => newSessions.get(x).stage == Holding)),` | MISSED | MISSED → **CAUGHT-T** (closed) | emptyBufferHoldLatchesNoCoverageTest |
| M23 | machine: observeRelease / re-reads the gate instead of trusting the wakeup | `sessions' = sessions.set(s, { ...ss, stage: Consulting }),` → `sessions' = sessions.set(s, applyFlush(ss)),` | CAUGHT-P | CAUGHT-T | holdThenDrainTest, barrierWhileHeldTest, sendFailedAbandonsBufferTest |
| M24 | machine: observeRelease / drain coverage condition | `drainedOnRelease: latch(coverage.drainedOnRelease, ⏎           ss.held.length() > 0 and not(gateHeld(armed))),` → `drainedOnRelease: latch(coverage.drainedOnRelease, true),` | MISSED | MISSED → **CAUGHT-T** (closed) | emptyBufferHoldLatchesNoCoverageTest |
| M25 | machine: sourceClosed / classified Graceful (TR-REPLICATION-007) | `val post = applyClassify(ss, Graceful, ClosedByPeer)` → `val post = applyClassify(ss, Lost, ClosedByPeer)` | CAUGHT-P | CAUGHT-T+P | closeInsideWindowDrainsThenEndsGracefulTest / inv_departure_matches_its_cause |
| M26 | machine: sourceClosed / writes the fence cell | `canSourceClosed(ss, source_closed, live), ⏎       sessions' = sessions.set(s, post), ⏎       departure_record' = recordFor(departure_record, post),` → `canSourceClosed(ss, source_closed, live), ⏎       sessions' = sessions.set(s, post), ⏎       departure_record' = departure_record,` | MISSED | MISSED → **CAUGHT-T** (closed) | gracefulCloseRecordsItsDepartureTest |
| M27 | machine: sourceLagged / classified Lost | `val post = applyClassify(ss, Lost, ReceiverLagged)` → `val post = applyClassify(ss, Graceful, ReceiverLagged)` | CAUGHT-T | CAUGHT-T+P | laggedOverrunDisconnectTest / inv_departure_matches_its_cause |
| M28 | machine: sourceLagged / writes the fence cell | `canSourceLagged(ss, live), ⏎       sessions' = sessions.set(s, post), ⏎       departure_record' = recordFor(departure_record, post),` → `canSourceLagged(ss, live), ⏎       sessions' = sessions.set(s, post), ⏎       departure_record' = departure_record,` | CAUGHT-T | CAUGHT-T | laggedOverrunDisconnectTest |
| M29 | machine: sourceLagged / lagged coverage latch | `laggedDisconnect: latch(coverage.laggedDisconnect, true),` → `laggedDisconnect: coverage.laggedDisconnect,` | CAUGHT-T | CAUGHT-T | laggedOverrunDisconnectTest |
| M30 | machine: sourceLagged / lost-end coverage latch | `lostEnd: latch(coverage.lostEnd, post.stage == Ended),` → `lostEnd: coverage.lostEnd,` | MISSED | MISSED → **CAUGHT-T** (closed) | laggedOverrunDisconnectTest |
| M31 | machine: confirmSentAs / writes the fence cell | `canConfirmSent(ss), ⏎       sessions' = sessions.set(s, post), ⏎       departure_record' = recordFor(departure_record, post),` → `canConfirmSent(ss), ⏎       sessions' = sessions.set(s, post), ⏎       departure_record' = departure_record,` | CAUGHT-T | CAUGHT-T | closeInsideWindowDrainsThenEndsGracefulTest |
| M32 | machine: confirmSentAs / ending-drain coverage latch (the disclosed carve-out) | `endingDrainedPastBarrierFloor: latch(coverage.endingDrainedPastBarrierFloor, ⏎           isEnding(ss) and BARRIERS.exists(b => ⏎             optExists(armed.get(b), f => optExists(shipped, o => o > f)))),` → `endingDrainedPastBarrierFloor: coverage.endingDrainedPastBarrierFloor,` | CAUGHT-T | CAUGHT-T | closeInsideWindowDrainsThenEndsGracefulTest |
| M33 | machine: confirmSentAs / ending-drain coverage: isEnding conjunct | `isEnding(ss) and BARRIERS.exists(b =>` → `BARRIERS.exists(b =>` | MISSED | MISSED | — |
| M34 | machine: confirmSentAs / ending-drain coverage: strict past-the-floor | `optExists(armed.get(b), f => optExists(shipped, o => o > f)))),` → `optExists(armed.get(b), f => optExists(shipped, o => o >= f)))),` | MISSED | MISSED → **CAUGHT-T** (closed) | drainAtTheFloorIsNotADivergenceTest |
| M35 | machine: confirmSentAs / lag-breach coverage latch | `lagBreach: latch(coverage.lagBreach, breach),` → `lagBreach: coverage.lagBreach,` | CAUGHT-T | CAUGHT-T | lagBreachEndsAtTheFrameTest |
| M36 | machine: sendFailed / classified Lost | `val post = applyEnd(ss, Lost, SendFailure)` → `val post = applyEnd(ss, Graceful, SendFailure)` | CAUGHT-T | CAUGHT-T+P | sendFailedAbandonsBufferTest / inv_departure_matches_its_cause |
| M37 | machine: sendFailed / send-failed coverage latch | `sendFailed: latch(coverage.sendFailed, true),` → `sendFailed: coverage.sendFailed,` | CAUGHT-T | CAUGHT-T | sendFailedAbandonsBufferTest |
| M38 | machine: supersede / the older session is the one kicked | `sessions' = sessions.set(older, post),` → `sessions' = sessions.set(newer, post),` | CAUGHT-T | CAUGHT-T | supersededKickWritesNoDepartureTest |
| M39 | machine: supersede / classified Superseded (issue 22) | `val post = applyEnd(oss, Superseded, Kicked)` → `val post = applyEnd(oss, Lost, Kicked)` | CAUGHT-T | CAUGHT-T+P | supersededKickWritesNoDepartureTest / inv_departure_matches_its_cause, inv_superseded_writes_no_departure |
| M40 | machine: supersede / a session cannot kick itself | `newer != older,` → `true,` | MISSED | MISSED | — |
| M41 | machine: supersede / superseded coverage latch | `supersededEnd: latch(coverage.supersededEnd, true),` → `supersededEnd: coverage.supersededEnd,` | CAUGHT-T | CAUGHT-T | supersededKickWritesNoDepartureTest |
| M42 | machine: replicaAckAs / the watermark takes the admitted value | `sessions' = sessions.set(s, { ...ss, acked: admitted }),` → `sessions' = sessions.set(s, { ...ss, acked: reported }),` | CAUGHT-T | CAUGHT-T+P | ackAboveLiveIsIgnoredTest / inv_ack_never_above_live, inv_ack_never_above_the_wire |
| M43 | machine: replicaAckAs / ignored ACKs are counted (issue 21) | `ignored_acks' = if (ackIgnored(reported, live)) ignored_acks + 1 else ignored_acks,` → `ignored_acks' = ignored_acks,` | CAUGHT-T | CAUGHT-T | ackAboveLiveIsIgnoredTest |
| M44 | machine: replicaAckAs / clamp detector precision | `clampedAck: latch(defects.clampedAck, ⏎           admitted != ss.acked and admitted != reported),` → `clampedAck: latch(defects.clampedAck, ⏎           admitted != ss.acked),` | CAUGHT-T | CAUGHT-T+P | ackAboveLiveIsIgnoredTest / inv_ack_ignored_not_clamped |
| M45 | machine: replicaAckAs / ignored-ack coverage latch | `ackIgnored: latch(coverage.ackIgnored, ackIgnored(reported, live)),` → `ackIgnored: coverage.ackIgnored,` | CAUGHT-T | CAUGHT-T | ackAboveLiveIsIgnoredTest |
| M46 | machine: replicaAck / a replica ACKs what it landed (TR-REPLICATION-033) | `action replicaAck(s: SessionId): bool = replicaAckAs(s, wireMax(sessions.get(s)))` → `action replicaAck(s: SessionId): bool = replicaAckAs(s, live)` | CAUGHT-P | CAUGHT-P | inv_ack_never_above_the_wire |
| M47 | machine: replicaAckAbove / the hostile ACK is above live | `action replicaAckAbove(s: SessionId): bool = replicaAckAs(s, live + 1)` → `action replicaAckAbove(s: SessionId): bool = replicaAckAs(s, live)` | CAUGHT-T | CAUGHT-T+P | ackAboveLiveIsIgnoredTest / inv_ack_never_above_the_wire |
| M48 | machine: step / supersede unwired | `supersede(s, t), ⏎` → `` | MISSED | MISSED | — |
| M49 | machine: step / lapseBarrier unwired | `lapseBarrier(b), ⏎` → `` | MISSED | MISSED | — |
| M50 | machine: init / defect ghosts start clear | `clampedAck: false, supersededRecordedDeparture: false,` → `clampedAck: true, supersededRecordedDeparture: false,` | CAUGHT-T | CAUGHT-T+P | ackAboveLiveIsIgnoredTest / inv_ack_ignored_not_clamped |

### Invariant clauses (main)

| Row | Where | Mutation | Expected | Verdict | Catcher |
|---|---|---|---|---|---|
| I01 | main: inv_no_ship_inside_barrier_window / the isEnding carve-out | `isEnding(ss) or optForall(armed.get(b), floor => wireMax(ss) <= floor)` → `optForall(armed.get(b), floor => wireMax(ss) <= floor)` | CAUGHT-P | MISSED → **CAUGHT-T** (closed) | closeInsideWindowDrainsThenEndsGracefulTest |
| I02 | main: inv_no_ship_inside_barrier_window / unarmed barriers are vacuous | `isEnding(ss) or optForall(armed.get(b), floor => wireMax(ss) <= floor)` → `isEnding(ss) or optExists(armed.get(b), floor => wireMax(ss) <= floor)` | CAUGHT-P | CAUGHT-P | inv_no_ship_inside_barrier_window |
| I03 | main: inv_no_ship_inside_barrier_window / the floor is inclusive | `isEnding(ss) or optForall(armed.get(b), floor => wireMax(ss) <= floor)` → `isEnding(ss) or optForall(armed.get(b), floor => wireMax(ss) < floor)` | CAUGHT-P | CAUGHT-P | inv_no_ship_inside_barrier_window |
| I04 | main: inv_nothing_held_is_dropped / the Ended exclusion | `ss.stage == Ended or ⏎         ss.accepted.length() == ss.sent.length() + ss.held.length() + inFlight` → `ss.accepted.length() == ss.sent.length() + ss.held.length() + inFlight` | CAUGHT-P | CAUGHT-P | inv_nothing_held_is_dropped |
| I05 | main: inv_duplicate_identity_is_always_kickable / both directions | `canSupersede(sessions.get(a), sessions.get(b)) or ⏎         canSupersede(sessions.get(b), sessions.get(a))))` → `canSupersede(sessions.get(a), sessions.get(b))))` | CAUGHT-P | CAUGHT-P | inv_duplicate_identity_is_always_kickable |
| I06 | main: inv_ack_never_above_the_wire / distinct from ack-vs-live | `ss.acked <= wireMax(ss)` → `ss.acked <= live` | MISSED | MISSED | — |
| I07 | main: inv_stage_shape / Sending <-> in_flight is an equivalence | `(ss.stage == Sending) == (ss.in_flight != None),` → `(ss.stage == Sending) implies (ss.in_flight != None),` | MISSED | MISSED | — |
| I08 | main: inv_offsets_within_live / the wire clause | `wireMax(ss) <= live,` → `true,` | MISSED | MISSED | — |

### Paired clause-load-bearing checks

| Row | Where | Mutation | Expected | Verdict | Catcher |
|---|---|---|---|---|---|
| P1 | logic: gracefulLoss held-clause is what catches E10 (end-without-drain) | `applyFlush({ ...ss, ending: Some(d), cause: Some(c) })` → `applyEndWith({ ...ss, cause: Some(c) }, d)` **+** `pre.held.length() > 0,` → `false,` | PAIRED | PAIRED-GREEN | — |
| P2 | logic: defects.resentBelowResume latch is what catches E23 (dedup off) | `if (f > ss.resume_offset)` → `if (true)` **+** `resentBelowResume: latch(defects.resentBelowResume, resentBelowResume(post)),` → `resentBelowResume: defects.resentBelowResume,` | PAIRED | PAIRED-STILL-CAUGHT | inv_buffer_ahead_of_the_wire, inv_offsets_strictly_increase |
| P3 | logic: defects.clampedAck latch is what catches E32 (clamp) | `if (ackIgnored(reported, live)) ss.acked else max2(ss.acked, reported)` → `if (ackIgnored(reported, live)) live else max2(ss.acked, reported)` **+** `clampedAck: latch(defects.clampedAck, ⏎           admitted != ss.acked and admitted != reported),` → `clampedAck: defects.clampedAck,` | PAIRED | PAIRED-STILL-CAUGHT | inv_ack_never_above_the_wire |
| P4 | logic: defects.supersededRecordedDeparture latch is what catches E12 | `\| Some(d) => if (d == Superseded) prev else Some(d)` → `\| Some(d) => Some(d)` **+** `supersededRecordedDeparture: ⏎           latch(defects.supersededRecordedDeparture, newRecord != departure_record),` → `supersededRecordedDeparture: defects.supersededRecordedDeparture,` | PAIRED | PAIRED-STILL-CAUGHT | inv_departure_record_is_never_superseded |
| P5 | machine: the isEnding carve-out does not swallow a live session (M17 still caught) | `val held = gateHeld(armed)` → `val held = false` **+** `isEnding(ss) or optForall(armed.get(b), floor => wireMax(ss) <= floor)` → `true or optForall(armed.get(b), floor => wireMax(ss) <= floor)` | PAIRED | PAIRED-GREEN | — |
| P6 | machine: inv_ack_never_above_the_wire is what catches M46 (ack the live head) | `action replicaAck(s: SessionId): bool = replicaAckAs(s, wireMax(sessions.get(s)))` → `action replicaAck(s: SessionId): bool = replicaAckAs(s, live)` **+** `ss.acked <= wireMax(ss)` → `ss.acked <= live` | PAIRED | PAIRED-GREEN | — |
| P7 | logic: inv_stage_shape's equivalence is what catches E29 (in-flight retained) | `val after = { ...ss, sent: landed, in_flight: None }` → `val after = { ...ss, sent: landed }` **+** `(ss.stage == Sending) == (ss.in_flight != None),` → `(ss.stage == Sending) implies (ss.in_flight != None),` | PAIRED | PAIRED-GREEN | — |
| P8 | machine: inv_offsets_within_live's wire clause is what catches M12 (replay past live) | `r <= live,` → `true,` **+** `wireMax(ss) <= live,` → `true,` | PAIRED | PAIRED-GREEN | — |


## Coverage argument

The battery is an *exhaustiveness* claim, not a sample: every conjunct, every field update and
every invariant clause in the four files has a row.

| Surface | Population | Rows |
|---|---|---|
| Guard conjuncts (`_logic` `can*` + `gateHeld`/`announced`) | every conjunct of every guard | G01–G24 |
| Effect field updates (`_logic` `apply*`, `recordFor`, `admitAck`, the shape predicates and the two defect predicates) | every field written and every branch condition | E01–E39 |
| Machine wiring (`_machine`) | every action guard, every `var` write that is not a frame-keeper, every ghost latch, `step` membership, `init` | M01–M50 |
| Invariant clauses (main) | every clause of the invariants whose statement carries a decision (carve-outs, `optForall` vs `optExists`, boundaries, exclusions) | I01–I08 |
| Paired load-bearing checks | one per oracle whose contribution a single mutation cannot show | P1–P8 |

Two surfaces are deliberately *not* one-row-per-line:

- **Frame keepers** (`keepEnv`, `keepSessions`, …). Mutating one makes the action assign a
  variable twice or leave it unassigned, which Quint rejects statically — a compile error, not
  an oracle result. They are covered instead by the `var`-write rows that read them (M04, M06,
  M10, M13, M31 …).
- **Comment and header text.** Not executable.

Invariant rows (I) and paired rows (P) answer the question a plain model-mutation battery cannot:
*is each oracle clause carrying weight, or is it decoration?* An I-row mutates the property and
expects a violation **on the unmutated model** (proving the clause is not vacuous); a P-row
widens an oracle and re-runs a model mutation the intact oracle caught, expecting the catch to
**disappear** (proving that oracle, and not some other one, was the catcher).

## Rows still MISSED — per-row honest-miss analysis

Every row still MISSED after gap closure is one of four kinds. None is a blanket skip: each says
either "the mutated model has the same behaviour as the original" or "the mutation makes an
oracle weaker, which by construction cannot fail".

### 1. Equivalent mutants — the mutation cannot change any reachable behaviour

- **E29** — `applySent`: `in_flight` not cleared on landing. Every continuation of `after`
  rewrites `in_flight` before anything reads it: the drain branch of `applyFlush` writes
  `Some(held.head())`, the depart branch (`applyEndWith`) and the idle branch both write `None`,
  and the breach branch goes through `applyEndWith` too. The stale value does not survive the
  step it is created in. *(Pre-registered CAUGHT-P — the prediction missed this masking.)*
  Consequence for **P7**: the pairing meant to show that `inv_stage_shape`'s equivalence is what
  catches E29 is vacuous, because nothing catches E29. What the equivalence buys is shown by
  I07 instead.
- **E13** — `recordFor`'s `None` arm. Guarded by `post.stage == Ended`, and every path to
  `Ended` runs through `applyEndWith`, which writes `ending: Some(d)`. An `Ended` session with
  `ending == None` does not exist; `inv_departure_matches_its_cause` is the standing statement
  of that.
- **M19 / M20** — `consultGate`'s departure-record write and its graceful-loss recompute. A
  `Consulting` session never carries a pending departure: `ending` is written only by
  `applyClassify` (whose own `applyFlush` lands on `Sending` or `Ended`) and by `applyEnd`
  (terminal), while the two ways into `Consulting` — `applyReceiveFrame` and `observeRelease` —
  start from `Receiving`/`Holding`. So `post.stage == Ended` is unreachable inside `consultGate`
  and both mutated expressions are no-ops.
- **M33** — the `isEnding` conjunct of the `endingDrainedPastBarrierFloor` latch. For a session
  that is *not* ending, `shipped > floor` is unreachable, so the conjunct never changes the
  latch. Proof, from floor monotonicity: a live session reaches `Sending` only through
  `consultGate` with the gate open, i.e. with no barrier armed; any barrier armed after that
  records `floor = live` at its own arm time, and `live` only advances, so `floor >= o` for the
  frame `o` in flight. The row that *is* killable at this boundary is M34 — and it is now
  closed (`drainAtTheFloorIsNotADivergenceTest`).
- **M40** — `supersede`'s `newer != older`. Redundant: `canSupersede` demands
  `newer.stream_seq > older.stream_seq`, which no session satisfies against itself.
- **G13** — `canConfirmSent`'s `in_flight != None`. Redundant against `inv_stage_shape`, which
  states `(stage == Sending) == (in_flight != None)` as an equivalence, so the surviving
  conjunct already implies the dropped one in every reachable state. I07 is the row that proves
  that equivalence is itself load-bearing.
- **G15 / G17** — `canSupersede`'s two `!= Handoff` conjuncts. Redundant against the identity
  conjunct: `inv_stage_shape` pins `identity == None` for a `Handoff` session, and issue 22's
  `announced` refuses `None` on both sides of the dedup, so a session still replaying can
  neither supersede nor be superseded with or without the stage conjunct.

### 2. Oracle-widening rows — the mutation makes a *predicate* more eager, not the model wrong

- **E38** — `isPrefix`'s length clause. Dropping it does not make the predicate `true` on a
  short list: `nth` past the end is **undefined** in Quint, not `false`. On the only inputs that
  distinguish mutant from original the result is unspecified, so no oracle can report a
  difference. The clause exists to keep undefined behaviour out of the model, which is a
  property of the *statement*, not of any reachable state.
  (Its sibling **E35**, widening `gracefulLoss` by dropping the `Ended` clause, was
  pre-registered MISSED and in fact came back CAUGHT-T+P — the widened predicate does fire on a
  still-draining graceful session.)

### 3. Weakened-invariant rows — answered by their paired check, by construction

**I06, I07, I08** each weaken an invariant clause. A weakened invariant accepts strictly more
states, so it cannot fail on the unmutated model: `MISSED` is the only verdict such a row can
have on its own. The question it asks — *does this clause carry any catch?* — is answered by the
paired rows **P6, P7, P8**, which re-run a model mutation with the clause weakened and check
whether the catch disappears. Read each I-row with its P-row; the P verdicts are in the table.

### 4. Structural limits — `step` wiring

- **M48 / M49** — `supersede` / `lapseBarrier` removed from `step`'s `any { … }`. `quint test`
  drives actions directly, so no run test notices, and the sampled run's only signal is witness
  coverage — which is not part of the CI gate (`quint test` + `quint run --invariants`). Same
  class as the Q4 migration battery's M22. Closing it needs a gated assertion over *sampled*
  coverage, which is a harness property (the campaign's CI-gate design), not something this file
  can state. Recorded rather than papered over.

## Gap closure

Twelve `run` tests were added to `specs/quint/replication_feed_gate.qnt`, and two existing tests
each gained one expectation — 11 run tests before, 23 after. They are gated: `quint test` is part
of the CI gate, so every row below is now killed by the *gate*, not merely by a witness.

**Pass-1 → pass-2: 38 `MISSED` → 15.** Twenty-three rows flipped to `CAUGHT-T` (two of them to
`CAUGHT-T+P`), and one further row (M30) that pass 1 caught only through a witness is now caught
by the gate. Nothing was weakened to make a row pass.

Every closure was written against a sandbox copy of the model (`scratchpad/t9c/sandbox`, seeded
from the `f7234770` files plus the shared `lib_*`/`cluster_common_types` modules) so the working
tree was never mutated for closure development while the battery was still running. Each closure
was then re-checked by re-applying its target mutation to the *closed* model and confirming the
new test fails: 23 of 23 targeted rows flipped from MISSED to killed.

| Closure test | Rows closed | What it pins |
|---|---|---|
| `closeAndLagGuardsRefuseAHealthySessionTest` | G09, G10, G12 | a clean end needs the source to have actually closed **and** the cursor caught up; a `Lost` disconnect needs an actual overrun |
| `channelOverrunRefusesIntakeTest` | G05 | past `CHANNEL_CAP` the frames are gone: the intake is refused, and the lagged transition is the only one enabled |
| `midSendSessionIsNotClassifiedOutFromUnderItsFrameTest` | G11, G14 | a session with a frame on the wire is not classified out from under it (the in-flight frame would vanish); a session that has sent nothing cannot report a failed send |
| `supersessionAndAckGuardsTest` | G16, G19, G22, G23 | newest-wins and only newest; an ended session displaces nobody; a session ACKs only while streaming |
| `enteringStreamingClearsTheFenceCellTest` | M13 | FM-REPLICATION-062: entering streaming clears the node's last-departure cell |
| `gracefulCloseRecordsItsDepartureTest` | M26 | the graceful path writes its record too, in the classify-and-end-in-one-step case |
| `barrierFloorIsTheLiveOffsetAtArmTimeTest` | M01, M02, M03, M08 | the floor is `live` at arm time (not 0, not the end of the backlog); an armed barrier is not re-armed; overlapping-hold coverage means what it says |
| `emptyBufferHoldLatchesNoCoverageTest` | M21, M22, M24 | three coverage ghosts pinned **false** where a widened version would latch: a hold that held nothing back, one session holding, a wakeup with an empty buffer |
| `drainAtTheFloorIsNotADivergenceTest` | M34 | the carve-out's boundary is strict — a frame written *before* the barrier armed sits at the floor, and draining it out is not a drain past the window |
| `ackWatermarkNeverRetreatsTest` | E33 | TR-REPLICATION-033: the acked offset is a high-water mark, not the last number reported |
| `closedSourceAcceptsNoMoreWritesTest` | M09 | a closed source produces no more frames |
| `laggedOverrunDisconnectTest` (+1 expectation) | M30 | `coverage.lostEnd` is asserted by a gated test rather than only by an ungated witness |
| `closeInsideWindowDrainsThenEndsGracefulTest` (+1 expectation) | I01 | the FM-CLUSTER-097 carve-out in `inv_no_ship_inside_barrier_window` is *reached*, and the invariant holds there |

Two of these are worth calling out beyond their row:

- **`drainAtTheFloorIsNotADivergenceTest`** narrows the disclosed FM-CLUSTER-097 carve-out from
  the outside. The battery's M34 row asked whether shipping *exactly at* the floor is counted as
  a divergence; it is not, and now a gated test says so. The carve-out therefore covers only
  frames written **after** the barrier armed, which is the smallest set that reproduces the
  seam's behaviour.
- **`enteringStreamingClearsTheFenceCellTest`** is the only gated statement in this model that a
  successor's stream clears a predecessor's `Lost` record — the fence-clearing half of
  FM-REPLICATION-062. Before it, `departure_record' = None` in `handoffReplayedAs` could be
  deleted with no oracle noticing.

- **I01** is the one closure that names an invariant inside a run test:
  `.expect(inv_no_ship_inside_barrier_window)` at the state where an ending session has drained
  past the floor. This is the row the battery found most worth closing. `quint test` does not
  evaluate invariants, and sampled runs never reach the drained-past-floor state
  (`witnessEndingDrainedPastFloor` sits at 0 traces even at 2000×30) — so before this line, the
  carve-out `isEnding(ss) or …` could be **deleted** and every oracle stayed green. Both halves
  now hold: the state is reached by a gated test, and the invariant is asserted true there. If
  the FM-CLUSTER-097 ruling deletes the carve-out, this expectation is the alarm the file header
  promises.

### Paired checks — what the P rows returned

Five of the eight paired rows came back `PAIRED-GREEN`: widening the clause under test made the
catch disappear, so that clause — and not some other oracle — is what was catching the mutation
(P1 `gracefulLoss`'s held clause, P5 the `isEnding` carve-out's non-swallowing of a live session,
P6 the ack-vs-wire clause, P7 `inv_stage_shape`'s equivalence, P8 `inv_offsets_within_live`'s wire
clause).

Three came back `PAIRED-STILL-CAUGHT`, which is a finding rather than a failure — the behaviour
has redundant coverage:

| Row | Clause removed | Still caught by |
|---|---|---|
| P2 | the `resentBelowResume` defect latch | `inv_buffer_ahead_of_the_wire`, `inv_offsets_strictly_increase` |
| P3 | the `clampedAck` defect latch | `inv_ack_never_above_the_wire` |
| P4 | the `supersededRecordedDeparture` defect latch | `inv_departure_record_is_never_superseded` |

Each of the three is a *defect ghost* pinning an issue-21/22 ruling. The ghost is still worth
keeping: the structural invariant that also fires says "some offset rule broke", while the ghost
says *which ruling* was violated, which is the difference between a diagnosable failure and a
puzzle. Reported here so the redundancy is a known property, not an assumption.

No invariant was weakened, no row was quarantined, and no witness was added in place of a gated
test. The gap-closure additions are confined to the `run` test section of
`specs/quint/replication_feed_gate.qnt`; the other three files are byte-identical to `f7234770`.

## Final gate results

### Verdict counts

| | Pass 1 (base model) | Pass 2 (after gap closure) |
|---|---|---|
| `CAUGHT-T` | 31 | 54 |
| `CAUGHT-T+P` | 32 | 33 |
| `CAUGHT-P` | 20 | 19 |
| `MISSED` | 38 | **15** |
| `PAIRED-GREEN` | 5 | 5 |
| `PAIRED-STILL-CAUGHT` | 3 | 3 |
| **Total rows** | **129** | **129** |

Killed by an oracle: 83/129 → **106/129**. Of the 129 rows, 8 are paired checks that are not
kill/miss questions at all, and 15 of the remaining 121 are the documented-unobservable set
above: 106 killed + 15 explained = 121.

### Gate, on the closed model

```
$ quint test specs/quint/replication_feed_gate.qnt
  23 passing (834ms)

$ quint run specs/quint/replication_feed_gate.qnt \
    --max-samples=200 --max-steps=20 --invariants <all 18>     # the CI budget
[ok] No violation found

$ quint run … --max-samples=500 --max-steps=20 --seed=0x1|0x2|0x3 --invariants <all 18>
[ok] No violation found   (×3)
```

Witnesses (ungated, reported for reachability): **16 of 17** fire at 2000×30.
`witnessEndingDrainedPastFloor` remains at 0 traces — unchanged from T9b, and the reason its
carrier is a run test. Two closures now depend on that fact rather than merely noting it
(I01's expectation, and `drainAtTheFloorIsNotADivergenceTest` on the other side of the
boundary).

### Surface, before and after

| | T9b landing (`f7234770`) | after T9c |
|---|---|---|
| invariants | 18 | 18 (**unchanged — none weakened, none added**) |
| witnesses | 17 | 17 (unchanged) |
| run tests | 11 | **23** |

### Tree state

`specs/quint/replication_feed_gate_types.qnt`, `_logic.qnt` and `_machine.qnt` are
byte-identical to `f7234770` — `git diff HEAD --stat` reports nothing for them. The only
changed file is `specs/quint/replication_feed_gate.qnt`, and the whole of that change is the
gap-closure additions (12 new `run` tests plus two expectations inside existing tests). No
mutation survives anywhere in the tree; every one of the 129 rows was reverted from a pristine
copy immediately after its verdict was recorded.
