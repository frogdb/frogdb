# T9c feed-gate mutation battery — row definitions.
# Each row: one exact single text replacement. `expect` is the PRE-REGISTERED prediction,
# authored before any mutation was run.
#
# file keys: L = replication_feed_gate_logic.qnt
#            M = replication_feed_gate_machine.qnt
#            X = replication_feed_gate.qnt (main: invariants/witnesses/tests)
#            T = replication_feed_gate_types.qnt

ROWS = []


def row(rid, f, target, old, new, expect, why, extra=None):
    ROWS.append(
        dict(
            id=rid,
            file=f,
            target=target,
            old=old,
            new=new,
            expect=expect,
            why=why,
            extra=extra or [],
        )
    )


# ---------------------------------------------------------------------------
# A. Guard conjuncts (logic module `can*` + `gateHeld`/`announced` reads)
# ---------------------------------------------------------------------------

row(
    "G01",
    "L",
    "canHandoff / stage conjunct",
    "    ss.stage == Handoff,\n    not(held),",
    "    not(held),",
    "CAUGHT-P",
    "re-handoff from a streaming session re-seeds accepted/sent under a live buffer",
)

row(
    "G02",
    "L",
    "canHandoff / gate conjunct (FM-CLUSTER-097 PSYNC-into-window)",
    "    ss.stage == Handoff,\n    not(held),",
    "    ss.stage == Handoff,",
    "CAUGHT-T",
    "handoffWaitsForReleaseTest asserts not(canHandoff(..., gateHeld(armed)))",
)

row(
    "G03",
    "L",
    "canReceiveFrame / stage conjunct",
    "    ss.stage == Receiving or ss.stage == Holding,\n    ss.cursor < live,",
    "    ss.cursor < live,",
    "CAUGHT-P",
    "intake while Sending overwrites the stage and strands the in-flight frame",
)

row(
    "G04",
    "L",
    "canReceiveFrame / cursor < live",
    "    ss.cursor < live,\n    live - ss.cursor <= CHANNEL_CAP,",
    "    live - ss.cursor <= CHANNEL_CAP,",
    "CAUGHT-P",
    "cursor/accepted run past the primary's live head",
)

row(
    "G05",
    "L",
    "canReceiveFrame / CHANNEL_CAP conjunct",
    "    ss.cursor < live,\n    live - ss.cursor <= CHANNEL_CAP,",
    "    ss.cursor < live,",
    "MISSED",
    "the cap only decides which of receive/lagged is enabled; no safety claim rides on it",
)

row(
    "G06",
    "L",
    "canConsult / stage conjunct",
    "  pure def canConsult(ss: SessionState): bool = ss.stage == Consulting",
    "  pure def canConsult(ss: SessionState): bool = ss.stage != Ended",
    "CAUGHT-P",
    "flushing straight out of Holding ships inside the window without a gate read",
)

row(
    "G07",
    "L",
    "canObserveRelease / stage conjunct",
    "  pure def canObserveRelease(ss: SessionState): bool = ss.stage == Holding",
    "  pure def canObserveRelease(ss: SessionState): bool = ss.stage != Ended",
    "CAUGHT-P",
    "a wakeup delivered to a Sending session moves it to Consulting with a frame in flight",
)

row(
    "G08",
    "L",
    "canSourceClosed / stage conjunct",
    "    ss.stage == Receiving or ss.stage == Holding,\n    closed,",
    "    closed,",
    "CAUGHT-P",
    "classify from Sending replaces the in-flight frame -> a frame vanishes",
)

row(
    "G09",
    "L",
    "canSourceClosed / source actually closed",
    "    closed,\n    ss.cursor == live,",
    "    ss.cursor == live,",
    "MISSED",
    "no property ties a Graceful departure to the upstream source having closed",
)

row(
    "G10",
    "L",
    "canSourceClosed / cursor caught up",
    "    closed,\n    ss.cursor == live,",
    "    closed,",
    "MISSED",
    "channel frames never consumed are invisible: they are not in `accepted`",
)

row(
    "G11",
    "L",
    "canSourceLagged / stage conjunct",
    "    ss.stage == Receiving or ss.stage == Holding,\n    live - ss.cursor > CHANNEL_CAP,",
    "    live - ss.cursor > CHANNEL_CAP,",
    "CAUGHT-P",
    "same in-flight replacement as G08, on the Lost path",
)

row(
    "G12",
    "L",
    "canSourceLagged / overrun comparison",
    "    live - ss.cursor > CHANNEL_CAP,",
    "    true,",
    "MISSED",
    "a spurious Lost disconnect is the safe direction; nothing forbids a link dying",
)

row(
    "G13",
    "L",
    "canConfirmSent / in_flight conjunct",
    "    ss.stage == Sending,\n    ss.in_flight != None,",
    "    ss.stage == Sending,",
    "MISSED",
    "redundant: inv_stage_shape makes Sending <-> in_flight != None an equivalence",
)

row(
    "G14",
    "L",
    "canSendFailed / stage conjunct",
    "  pure def canSendFailed(ss: SessionState): bool = ss.stage == Sending",
    "  pure def canSendFailed(ss: SessionState): bool = ss.stage != Ended",
    "MISSED",
    "a spurious Lost end is safe-direction; applyEnd keeps the shape legal",
)

row(
    "G15",
    "L",
    "canSupersede / newer past handoff",
    "    newer.stage != Handoff, newer.stage != Ended,",
    "    newer.stage != Ended,",
    "MISSED",
    "redundant: a Handoff session has identity None, so the identity conjunct refuses",
)

row(
    "G16",
    "L",
    "canSupersede / newer still live",
    "    newer.stage != Handoff, newer.stage != Ended,",
    "    newer.stage != Handoff,",
    "MISSED",
    "nothing observes that the successor was alive when it kicked",
)

row(
    "G17",
    "L",
    "canSupersede / older past handoff",
    "    older.stage != Handoff, older.stage != Ended,",
    "    older.stage != Ended,",
    "MISSED",
    "redundant: a Handoff session announced nothing to be deduped on",
)

row(
    "G18",
    "L",
    "canSupersede / older still live",
    "    older.stage != Handoff, older.stage != Ended,",
    "    older.stage != Handoff,",
    "CAUGHT-P",
    "re-kicking an ended session rewrites its Lost ending, orphaning the record",
)

row(
    "G19",
    "L",
    "canSupersede / newest-wins ordering",
    "    newer.stream_seq > older.stream_seq,",
    "    newer.stream_seq != older.stream_seq,",
    "MISSED",
    "an older session kicking a newer one has no observable in this model",
)

row(
    "G20",
    "L",
    "canSupersede / identity match",
    "    optExists(newer.identity, i => announced(older, i)),",
    "    true,",
    "CAUGHT-P",
    "cross-identity kicks reach an unannounced session -> inv_unannounced_is_never_superseded",
)

row(
    "G21",
    "L",
    "announced / unannounced never matches (issue 22)",
    "  pure def announced(ss: SessionState, i: int): bool = optExists(ss.identity, x => x == i)",
    "  pure def announced(ss: SessionState, i: int): bool = optExists(ss.identity, x => x == i) or ss.identity == None",
    "CAUGHT-P",
    "an unannounced session becomes dedupable -> inv_unannounced_is_never_superseded",
)

row(
    "G22",
    "L",
    "canAck / not in handoff",
    "    ss.stage != Handoff,\n    ss.stage != Ended,",
    "    ss.stage != Ended,",
    "MISSED",
    "the model's ACK alphabet (wireMax / live+1) is a no-op during handoff",
)

row(
    "G23",
    "L",
    "canAck / not after end",
    "    ss.stage != Handoff,\n    ss.stage != Ended,",
    "    ss.stage != Handoff,",
    "MISSED",
    "acking an ended session's own final wireMax is idempotent",
)

row(
    "G24",
    "L",
    "gateHeld / any armed barrier holds (FM-CLUSTER-097 node-wide)",
    "    BARRIERS.exists(b => armed.get(b) != None)",
    "    BARRIERS.forall(b => armed.get(b) != None)",
    "CAUGHT-P",
    "one armed barrier stops holding the feed -> ships inside that barrier's window",
)

# ---------------------------------------------------------------------------
# B. Effect field-updates (logic module `apply*`)
# ---------------------------------------------------------------------------

row(
    "E01",
    "L",
    "applyEndWith / buffer dropped at end",
    "    { ...ss, stage: Ended, held: List(), in_flight: None, ending: Some(d) }",
    "    { ...ss, stage: Ended, in_flight: None, ending: Some(d) }",
    "CAUGHT-T",
    "sendFailedAbandonsBufferTest asserts held == List() at Ended",
)

row(
    "E02",
    "L",
    "applyEndWith / in-flight cleared at end",
    "    { ...ss, stage: Ended, held: List(), in_flight: None, ending: Some(d) }",
    "    { ...ss, stage: Ended, held: List(), ending: Some(d) }",
    "CAUGHT-P",
    "Ended with a frame in flight breaks inv_stage_shape",
)

row(
    "E03",
    "L",
    "applyEndWith / departure recorded on the session",
    "    { ...ss, stage: Ended, held: List(), in_flight: None, ending: Some(d) }",
    "    { ...ss, stage: Ended, held: List(), in_flight: None }",
    "CAUGHT-P",
    "an Ended session with no classification breaks inv_departure_matches_its_cause",
)

row(
    "E04",
    "L",
    "applyEnd / cause recorded",
    "    applyEndWith({ ...ss, cause: Some(c) }, d)",
    "    applyEndWith(ss, d)",
    "CAUGHT-P",
    "Ended with cause None breaks inv_departure_matches_its_cause",
)

row(
    "E05",
    "L",
    "applyFlush / drain condition",
    "    if (ss.held.length() > 0)",
    "    if (ss.held.length() > 1)",
    "CAUGHT-T",
    "the last held frame never ships -> holdThenDrainTest in_flight expectation",
)

row(
    "E06",
    "L",
    "applyFlush / frame consumed from the buffer",
    "      { ...ss, stage: Sending, in_flight: Some(ss.held.head()), held: ss.held.tail() }",
    "      { ...ss, stage: Sending, in_flight: Some(ss.held.head()), held: ss.held }",
    "CAUGHT-P",
    "the head stays buffered after shipping -> duplicate send / buffer behind the wire",
)

row(
    "E07",
    "L",
    "applyFlush / the frame shipped is the frame buffered",
    "      { ...ss, stage: Sending, in_flight: Some(ss.held.head()), held: ss.held.tail() }",
    "      { ...ss, stage: Sending, in_flight: Some(ss.held.head() + 1), held: ss.held.tail() }",
    "CAUGHT-P",
    "the wire stops being a prefix of accepted",
)

row(
    "E08",
    "L",
    "applyFlush / pending departure honoured when empty",
    "        | Some(d) => applyEndWith(ss, d)",
    "        | Some(d) => { ...ss, stage: Receiving, in_flight: None }",
    "CAUGHT-T",
    "laggedOverrunDisconnectTest expects Ended after classify-then-drain",
)

row(
    "E09",
    "L",
    "applyFlush / idle when nothing pending",
    "        | None => { ...ss, stage: Receiving, in_flight: None }",
    "        | None => { ...ss, stage: Holding, in_flight: None }",
    "CAUGHT-T",
    "holdThenDrainTest expects Receiving after the drain completes",
)

row(
    "E10",
    "L",
    "applyClassify / classify-then-DRAIN (flush before you end)",
    "    applyFlush({ ...ss, ending: Some(d), cause: Some(c) })",
    "    applyEndWith({ ...ss, cause: Some(c) }, d)",
    "CAUGHT-T",
    "closeInsideWindowDrainsThenEndsGracefulTest expects in_flight Some(1), not Ended",
)

row(
    "E11",
    "L",
    "recordFor / a record needs an ended session",
    "    if (post.stage != Ended) prev else",
    "    if (false) prev else",
    "CAUGHT-P",
    "classification alone writes the fence cell -> record not backed by an ended session",
)

row(
    "E12",
    "L",
    "recordFor / Superseded writes nothing (issue 22)",
    "        | Some(d) => if (d == Superseded) prev else Some(d)",
    "        | Some(d) => Some(d)",
    "CAUGHT-T",
    "supersededKickWritesNoDepartureTest asserts departure_record == None",
)

row(
    "E13",
    "L",
    "recordFor / None arm",
    "        | None => prev\n      }",
    "        | None => Some(Lost)\n      }",
    "MISSED",
    "arm unreachable: an Ended session always carries a classification",
)

row(
    "E14",
    "L",
    "applyHandoff / resume_offset recorded (dedup basis)",
    "    { ...ss, stage: Receiving, resume_offset: r, identity: ident, stream_seq: seq,",
    "    { ...ss, stage: Receiving, resume_offset: 0, identity: ident, stream_seq: seq,",
    "CAUGHT-T",
    "handoffDedupTest: replayed frames get re-sent on the live path",
)

row(
    "E15",
    "L",
    "applyHandoff / announced identity stored",
    "    { ...ss, stage: Receiving, resume_offset: r, identity: ident, stream_seq: seq,",
    "    { ...ss, stage: Receiving, resume_offset: r, identity: None, stream_seq: seq,",
    "CAUGHT-T",
    "supersededKickWritesNoDepartureTest: the kick needs a matching identity",
)

row(
    "E16",
    "L",
    "applyHandoff / arrival order stored",
    "    { ...ss, stage: Receiving, resume_offset: r, identity: ident, stream_seq: seq,",
    "    { ...ss, stage: Receiving, resume_offset: r, identity: ident, stream_seq: 0,",
    "CAUGHT-T",
    "equal stream_seq disables the kick -> supersededKickWritesNoDepartureTest",
)

row(
    "E17",
    "L",
    "applyHandoff / stage after replay",
    "    { ...ss, stage: Receiving, resume_offset: r, identity: ident, stream_seq: seq,",
    "    { ...ss, stage: Consulting, resume_offset: r, identity: ident, stream_seq: seq,",
    "CAUGHT-T",
    "holdThenDrainTest: receiveFrame is disabled out of Consulting",
)

row(
    "E18",
    "L",
    "applyHandoff / replay seeds the accepted history",
    "      accepted: range(1, r + 1), sent: range(1, r + 1) }",
    "      accepted: List(), sent: range(1, r + 1) }",
    "CAUGHT-P",
    "wire longer than accepted -> inv_wire_is_a_prefix_of_accepted",
)

row(
    "E19",
    "L",
    "applyHandoff / replay reaches the wire",
    "      accepted: range(1, r + 1), sent: range(1, r + 1) }",
    "      accepted: range(1, r + 1), sent: List() }",
    "CAUGHT-T",
    "handoffDedupTest asserts sent == List(1, 2) right after the replay",
)

row(
    "E20",
    "L",
    "applyReceiveFrame / next frame is cursor+1",
    "    val f = ss.cursor + 1",
    "    val f = ss.cursor + 2",
    "CAUGHT-P",
    "frames beyond the live head enter the session -> inv_offsets_within_live",
)

row(
    "E21",
    "L",
    "applyReceiveFrame / cursor advances",
    "    val advanced = { ...ss, cursor: f, stage: Consulting }",
    "    val advanced = { ...ss, stage: Consulting }",
    "CAUGHT-P",
    "the same frame is taken repeatedly -> accepted stops strictly increasing",
)

row(
    "E22",
    "L",
    "applyReceiveFrame / intake goes to the gate",
    "    val advanced = { ...ss, cursor: f, stage: Consulting }",
    "    val advanced = { ...ss, cursor: f, stage: Receiving }",
    "CAUGHT-T",
    "holdThenDrainTest: consultGate is never enabled, nothing ever ships",
)

row(
    "E23",
    "L",
    "applyReceiveFrame / replay dedup filter (FM-REPLICATION-015)",
    "    if (f > ss.resume_offset)",
    "    if (true)",
    "CAUGHT-T",
    "handoffDedupTest: an already-replayed frame is buffered and re-sent",
)

row(
    "E24",
    "L",
    "applyReceiveFrame / accepted frame is buffered",
    "      { ...advanced, held: ss.held.append(f), accepted: ss.accepted.append(f) }",
    "      { ...advanced, accepted: ss.accepted.append(f) }",
    "CAUGHT-P",
    "an accepted frame that is neither sent, in flight nor held -> dropped",
)

row(
    "E25",
    "L",
    "applyReceiveFrame / accepted history recorded",
    "      { ...advanced, held: ss.held.append(f), accepted: ss.accepted.append(f) }",
    "      { ...advanced, held: ss.held.append(f) }",
    "CAUGHT-P",
    "held frames outrun the accepted history -> nothing-dropped accounting breaks",
)

row(
    "E26",
    "L",
    "dedupsFrame / coverage predicate",
    "  pure def dedupsFrame(ss: SessionState): bool = ss.cursor + 1 <= ss.resume_offset",
    "  pure def dedupsFrame(ss: SessionState): bool = false",
    "CAUGHT-T",
    "handoffDedupTest asserts coverage.handoffDeduped",
)

row(
    "E27",
    "L",
    "applyHold / a held session parks",
    "  pure def applyHold(ss: SessionState): SessionState = { ...ss, stage: Holding }",
    "  pure def applyHold(ss: SessionState): SessionState = { ...ss, stage: Receiving }",
    "CAUGHT-T",
    "holdThenDrainTest asserts stage == Holding under an armed barrier",
)

row(
    "E28",
    "L",
    "applySent / the landed frame enters the wire history",
    "      | Some(o) => ss.sent.append(o)",
    "      | Some(o) => ss.sent",
    "CAUGHT-P",
    "a delivered frame disappears -> nothing-dropped accounting breaks",
)

row(
    "E29",
    "L",
    "applySent / in-flight cleared on landing",
    "    val after = { ...ss, sent: landed, in_flight: None }",
    "    val after = { ...ss, sent: landed }",
    "CAUGHT-P",
    "a non-Sending session with a frame in flight -> inv_stage_shape",
)

row(
    "E30",
    "L",
    "applySent / a lag breach ends the link",
    "    if (breach) applyEnd(after, Lost, LagBreach) else applyFlush(after)",
    "    applyFlush(after)",
    "CAUGHT-T",
    "lagBreachEndsAtTheFrameTest expects Ended/Lost at that frame",
)

row(
    "E31",
    "L",
    "applySent / a lag breach is Lost, not Graceful",
    "    if (breach) applyEnd(after, Lost, LagBreach) else applyFlush(after)",
    "    if (breach) applyEnd(after, Graceful, LagBreach) else applyFlush(after)",
    "CAUGHT-P",
    "classification stops matching its cause",
)

row(
    "E32",
    "L",
    "admitAck / ignore, never clamp (issue 21 AMENDED)",
    "    if (ackIgnored(reported, live)) ss.acked else max2(ss.acked, reported)",
    "    if (ackIgnored(reported, live)) live else max2(ss.acked, reported)",
    "CAUGHT-T",
    "ackAboveLiveIsIgnoredTest asserts acked stays 1 and defects.clampedAck is clear",
)

row(
    "E33",
    "L",
    "admitAck / watermark never retreats (TR-REPLICATION-033)",
    "    if (ackIgnored(reported, live)) ss.acked else max2(ss.acked, reported)",
    "    if (ackIgnored(reported, live)) ss.acked else reported",
    "MISSED",
    "the model's honest ACK is wireMax, which is monotone by construction",
)

row(
    "E34",
    "L",
    "ackIgnored / above-live boundary",
    "  pure def ackIgnored(reported: Offset, live: Offset): bool = reported > live",
    "  pure def ackIgnored(reported: Offset, live: Offset): bool = reported > live + 1",
    "CAUGHT-T",
    "ackAboveLiveIsIgnoredTest: live+1 is admitted -> acked above live",
)

row(
    "E35",
    "L",
    "gracefulLoss / Ended clause",
    "    post.stage == Ended,\n    endsGracefully(post),",
    "    endsGracefully(post),",
    "MISSED",
    "oracle-widening: latches on a still-draining graceful session (no invariant gets weaker)",
)

row(
    "E36",
    "L",
    "gracefulLoss / graceful clause",
    "    post.stage == Ended,\n    endsGracefully(post),",
    "    post.stage == Ended,",
    "CAUGHT-P",
    "every Lost end with a buffer becomes a 'graceful loss' -> spurious defect fires",
)

row(
    "E37",
    "L",
    "gracefulLoss / in-flight-landed exemption",
    "      pre.in_flight != None and post.sent.length() == pre.sent.length(),",
    "      pre.in_flight != None,",
    "CAUGHT-P",
    "the ordinary Sent-then-end path is misread as a drop -> spurious defect fires",
)

row(
    "E38",
    "L",
    "isPrefix / length clause",
    "    short.length() <= long.length(),\n    short.indices().forall(i => short.nth(i) == long.nth(i)),",
    "    short.indices().forall(i => short.nth(i) == long.nth(i)),",
    "MISSED",
    "oracle-widening: nth beyond the end is undefined rather than false",
)

row(
    "E39",
    "L",
    "wireMax / fold over the whole wire",
    "  pure def wireMax(ss: SessionState): Offset = ss.sent.foldl(0, (a, x) => max2(a, x))",
    "  pure def wireMax(ss: SessionState): Offset = 0",
    "CAUGHT-P",
    "the barrier floor and ack-vs-wire checks both read wireMax; replicaAck degenerates",
)

# ---------------------------------------------------------------------------
# C. Machine wiring: guards, var writes, ghost latches
# ---------------------------------------------------------------------------

row(
    "M01",
    "M",
    "armBarrier / floor is the live offset at arm time",
    "    armed' = armed.set(b, Some(live)),",
    "    armed' = armed.set(b, Some(0)),",
    "CAUGHT-P",
    "a floor of 0 makes every prior send a violation: proves the comparison is live",
)

row(
    "M02",
    "M",
    "armBarrier / floor not inflated",
    "    armed' = armed.set(b, Some(live)),",
    "    armed' = armed.set(b, Some(MAX_WRITES)),",
    "MISSED",
    "inflating the floor only weakens the obligation the invariant states",
)

row(
    "M03",
    "M",
    "armBarrier / re-arm refused",
    "    armed.get(b) == None,",
    "    true,",
    "MISSED",
    "re-arming only raises the floor, i.e. weakens the window",
)

row(
    "M04",
    "M",
    "releaseBarrier / actually disarms",
    "  action releaseBarrier(b: BarrierId): bool = all {\n    armed.get(b) != None,\n    armed' = armed.set(b, None),",
    "  action releaseBarrier(b: BarrierId): bool = all {\n    armed.get(b) != None,\n    armed' = armed,",
    "CAUGHT-T",
    "holdThenDrainTest: the gate never opens, the drain never happens",
)

row(
    "M05",
    "M",
    "releaseBarrier / coverage latch",
    "    coverage' = { ...coverage, barrierReleased: latch(coverage.barrierReleased, true) },",
    "    coverage' = { ...coverage, barrierReleased: coverage.barrierReleased },",
    "CAUGHT-T",
    "barrierWhileHeldTest asserts coverage.barrierReleased",
)

row(
    "M06",
    "M",
    "lapseBarrier / actually disarms",
    "  action lapseBarrier(b: BarrierId): bool = all {\n    armed.get(b) != None,\n    armed' = armed.set(b, None),",
    "  action lapseBarrier(b: BarrierId): bool = all {\n    armed.get(b) != None,\n    armed' = armed,",
    "CAUGHT-T",
    "barrierWhileHeldTest drains only after the second barrier lapses",
)

row(
    "M07",
    "M",
    "lapseBarrier / coverage latch",
    "    coverage' = { ...coverage, barrierLapsed: latch(coverage.barrierLapsed, true) },",
    "    coverage' = { ...coverage, barrierLapsed: coverage.barrierLapsed },",
    "CAUGHT-T",
    "barrierWhileHeldTest asserts coverage.barrierLapsed",
)

row(
    "M08",
    "M",
    "armBarrier / overlapping-hold coverage condition",
    "      overlappingHold: latch(coverage.overlappingHold, gateHeld(armed)),",
    "      overlappingHold: latch(coverage.overlappingHold, true),",
    "MISSED",
    "a spuriously-true coverage ghost: no test pins it false before the second arm",
)

row(
    "M09",
    "M",
    "writeFrame / no writes after the source closed",
    "    live < MAX_WRITES,\n    not(source_closed),",
    "    live < MAX_WRITES,",
    "MISSED",
    "post-close writes break no stated obligation in this model",
)

row(
    "M10",
    "M",
    "writeFrame / live offset advances (TR-REPLICATION-030)",
    "    live' = live + 1,",
    "    live' = live,",
    "CAUGHT-T",
    "no frames ever exist -> holdThenDrainTest's receiveFrame is disabled",
)

row(
    "M11",
    "M",
    "closeSource / latches the close",
    "    source_closed' = true,",
    "    source_closed' = false,",
    "CAUGHT-T",
    "closeInsideWindowDrainsThenEndsGracefulTest: sourceClosed never becomes enabled",
)

row(
    "M12",
    "M",
    "handoffReplayedAs / replay bounded by live",
    "      r <= live,",
    "      true,",
    "CAUGHT-P",
    "a replay past the live head puts unwritten offsets on the wire",
)

row(
    "M13",
    "M",
    "handoffReplayedAs / entering streaming clears the fence cell",
    "      departure_record' = None,",
    "      departure_record' = departure_record,",
    "MISSED",
    "no property observes the stale record while the successor streams",
)

row(
    "M14",
    "M",
    "handoffReplayedAs / arrival counter advances",
    "      next_seq' = next_seq + 1,",
    "      next_seq' = next_seq,",
    "CAUGHT-T",
    "equal stream_seq disables the kick -> supersededKickWritesNoDepartureTest",
)

row(
    "M15",
    "M",
    "handoffReplayedAs / handoff-after-release coverage",
    "        handoffAfterRelease: latch(coverage.handoffAfterRelease,\n          coverage.barrierReleased or coverage.barrierLapsed),",
    "        handoffAfterRelease: coverage.handoffAfterRelease,",
    "CAUGHT-T",
    "handoffWaitsForReleaseTest asserts coverage.handoffAfterRelease",
)

row(
    "M16",
    "M",
    "receiveFrame / dedup coverage latch",
    "        handoffDeduped: latch(coverage.handoffDeduped, dedupsFrame(ss)),",
    "        handoffDeduped: coverage.handoffDeduped,",
    "CAUGHT-T",
    "handoffDedupTest asserts coverage.handoffDeduped",
)

row(
    "M17",
    "M",
    "consultGate / the gate is actually read",
    "    val held = gateHeld(armed)",
    "    val held = false",
    "CAUGHT-P",
    "the prime row: a live session ships inside an armed window",
)

row(
    "M18",
    "M",
    "consultGate / the gate can open",
    "    val held = gateHeld(armed)",
    "    val held = true",
    "CAUGHT-T",
    "holdThenDrainTest: nothing ever drains",
)

row(
    "M19",
    "M",
    "consultGate / departure record on the flush-to-end path",
    "      canConsult(ss),\n      sessions' = newSessions,\n      departure_record' = recordFor(departure_record, post),",
    "      canConsult(ss),\n      sessions' = newSessions,\n      departure_record' = departure_record,",
    "MISSED",
    "unreachable: a Consulting session never carries a pending departure",
)

row(
    "M20",
    "M",
    "consultGate / graceful-loss defect recompute",
    "        gracefulEndDroppedFrames:\n          latch(defects.gracefulEndDroppedFrames, gracefulLoss(ss, post)),\n      },\n      coverage' = { ...coverage,\n        heldFrame:",
    "        gracefulEndDroppedFrames: defects.gracefulEndDroppedFrames,\n      },\n      coverage' = { ...coverage,\n        heldFrame:",
    "MISSED",
    "same unreachable path as M19: consultGate never ends a graceful session",
)

row(
    "M21",
    "M",
    "consultGate / held-frame coverage condition",
    "        heldFrame: latch(coverage.heldFrame, held and ss.held.length() > 0),",
    "        heldFrame: latch(coverage.heldFrame, held),",
    "MISSED",
    "spuriously-true coverage ghost; no test pins it false for an empty-buffer hold",
)

row(
    "M22",
    "M",
    "consultGate / node-wide both-held coverage",
    "        bothSessionsHeld: latch(coverage.bothSessionsHeld,\n          SESSIONS.forall(x => newSessions.get(x).stage == Holding)),",
    "        bothSessionsHeld: latch(coverage.bothSessionsHeld,\n          SESSIONS.exists(x => newSessions.get(x).stage == Holding)),",
    "MISSED",
    "nodeWideHoldTest asserts it only at the end, where both really are held",
)

row(
    "M23",
    "M",
    "observeRelease / re-reads the gate instead of trusting the wakeup",
    "      sessions' = sessions.set(s, { ...ss, stage: Consulting }),",
    "      sessions' = sessions.set(s, applyFlush(ss)),",
    "CAUGHT-P",
    "a wakeup while another barrier is armed ships inside that window",
)

row(
    "M24",
    "M",
    "observeRelease / drain coverage condition",
    "        drainedOnRelease: latch(coverage.drainedOnRelease,\n          ss.held.length() > 0 and not(gateHeld(armed))),",
    "        drainedOnRelease: latch(coverage.drainedOnRelease, true),",
    "MISSED",
    "spuriously-true coverage ghost; no test pins it false under a still-held gate",
)

row(
    "M25",
    "M",
    "sourceClosed / classified Graceful (TR-REPLICATION-007)",
    "    val post = applyClassify(ss, Graceful, ClosedByPeer)",
    "    val post = applyClassify(ss, Lost, ClosedByPeer)",
    "CAUGHT-P",
    "classification stops matching its cause",
)

row(
    "M26",
    "M",
    "sourceClosed / writes the fence cell",
    "      canSourceClosed(ss, source_closed, live),\n      sessions' = sessions.set(s, post),\n      departure_record' = recordFor(departure_record, post),",
    "      canSourceClosed(ss, source_closed, live),\n      sessions' = sessions.set(s, post),\n      departure_record' = departure_record,",
    "MISSED",
    "no test ends a graceful session with an empty buffer and inspects the record",
)

row(
    "M27",
    "M",
    "sourceLagged / classified Lost",
    "    val post = applyClassify(ss, Lost, ReceiverLagged)",
    "    val post = applyClassify(ss, Graceful, ReceiverLagged)",
    "CAUGHT-T",
    "laggedOverrunDisconnectTest asserts ending == Some(Lost)",
)

row(
    "M28",
    "M",
    "sourceLagged / writes the fence cell",
    "      canSourceLagged(ss, live),\n      sessions' = sessions.set(s, post),\n      departure_record' = recordFor(departure_record, post),",
    "      canSourceLagged(ss, live),\n      sessions' = sessions.set(s, post),\n      departure_record' = departure_record,",
    "CAUGHT-T",
    "laggedOverrunDisconnectTest asserts departure_record == Some(Lost)",
)

row(
    "M29",
    "M",
    "sourceLagged / lagged coverage latch",
    "        laggedDisconnect: latch(coverage.laggedDisconnect, true),",
    "        laggedDisconnect: coverage.laggedDisconnect,",
    "CAUGHT-T",
    "laggedOverrunDisconnectTest asserts coverage.laggedDisconnect",
)

row(
    "M30",
    "M",
    "sourceLagged / lost-end coverage latch",
    "        lostEnd: latch(coverage.lostEnd, post.stage == Ended),",
    "        lostEnd: coverage.lostEnd,",
    "MISSED",
    "no run test asserts coverage.lostEnd; the witness is not a gated oracle",
)

row(
    "M31",
    "M",
    "confirmSentAs / writes the fence cell",
    "      canConfirmSent(ss),\n      sessions' = sessions.set(s, post),\n      departure_record' = recordFor(departure_record, post),",
    "      canConfirmSent(ss),\n      sessions' = sessions.set(s, post),\n      departure_record' = departure_record,",
    "CAUGHT-T",
    "closeInsideWindow... asserts departure_record == Some(Graceful) after the drain",
)

row(
    "M32",
    "M",
    "confirmSentAs / ending-drain coverage latch (the disclosed carve-out)",
    "        endingDrainedPastBarrierFloor: latch(coverage.endingDrainedPastBarrierFloor,\n          isEnding(ss) and BARRIERS.exists(b =>\n            optExists(armed.get(b), f => optExists(shipped, o => o > f)))),",
    "        endingDrainedPastBarrierFloor: coverage.endingDrainedPastBarrierFloor,",
    "CAUGHT-T",
    "closeInsideWindow... asserts the carve-out was actually taken",
)

row(
    "M33",
    "M",
    "confirmSentAs / ending-drain coverage: isEnding conjunct",
    "          isEnding(ss) and BARRIERS.exists(b =>",
    "          BARRIERS.exists(b =>",
    "MISSED",
    "equivalent by the invariant: only an ending session can ship past a floor",
)

row(
    "M34",
    "M",
    "confirmSentAs / ending-drain coverage: strict past-the-floor",
    "            optExists(armed.get(b), f => optExists(shipped, o => o > f)))),",
    "            optExists(armed.get(b), f => optExists(shipped, o => o >= f)))),",
    "MISSED",
    "shipping exactly the floor latches spuriously; no test pins the boundary",
)

row(
    "M35",
    "M",
    "confirmSentAs / lag-breach coverage latch",
    "        lagBreach: latch(coverage.lagBreach, breach),",
    "        lagBreach: coverage.lagBreach,",
    "CAUGHT-T",
    "lagBreachEndsAtTheFrameTest asserts coverage.lagBreach",
)

row(
    "M36",
    "M",
    "sendFailed / classified Lost",
    "    val post = applyEnd(ss, Lost, SendFailure)",
    "    val post = applyEnd(ss, Graceful, SendFailure)",
    "CAUGHT-T",
    "sendFailedAbandonsBufferTest asserts ending == Some(Lost)",
)

row(
    "M37",
    "M",
    "sendFailed / send-failed coverage latch",
    "        sendFailed: latch(coverage.sendFailed, true),",
    "        sendFailed: coverage.sendFailed,",
    "CAUGHT-T",
    "sendFailedAbandonsBufferTest asserts coverage.sendFailed",
)

row(
    "M38",
    "M",
    "supersede / the older session is the one kicked",
    "      sessions' = sessions.set(older, post),",
    "      sessions' = sessions.set(newer, post),",
    "CAUGHT-T",
    "supersededKickWritesNoDepartureTest asserts session 1 ended",
)

row(
    "M39",
    "M",
    "supersede / classified Superseded (issue 22)",
    "    val post = applyEnd(oss, Superseded, Kicked)",
    "    val post = applyEnd(oss, Lost, Kicked)",
    "CAUGHT-T",
    "supersededKickWritesNoDepartureTest asserts ending == Some(Superseded)",
)

row(
    "M40",
    "M",
    "supersede / a session cannot kick itself",
    "      newer != older,",
    "      true,",
    "MISSED",
    "redundant: canSupersede needs a strictly greater stream_seq",
)

row(
    "M41",
    "M",
    "supersede / superseded coverage latch",
    "        supersededEnd: latch(coverage.supersededEnd, true),",
    "        supersededEnd: coverage.supersededEnd,",
    "CAUGHT-T",
    "supersededKickWritesNoDepartureTest asserts coverage.supersededEnd",
)

row(
    "M42",
    "M",
    "replicaAckAs / the watermark takes the admitted value",
    "      sessions' = sessions.set(s, { ...ss, acked: admitted }),",
    "      sessions' = sessions.set(s, { ...ss, acked: reported }),",
    "CAUGHT-T",
    "ackAboveLiveIsIgnoredTest asserts acked stays 1",
)

row(
    "M43",
    "M",
    "replicaAckAs / ignored ACKs are counted (issue 21)",
    "      ignored_acks' = if (ackIgnored(reported, live)) ignored_acks + 1 else ignored_acks,",
    "      ignored_acks' = ignored_acks,",
    "CAUGHT-T",
    "ackAboveLiveIsIgnoredTest asserts ignored_acks == 1",
)

row(
    "M44",
    "M",
    "replicaAckAs / clamp detector precision",
    "        clampedAck: latch(defects.clampedAck,\n          admitted != ss.acked and admitted != reported),",
    "        clampedAck: latch(defects.clampedAck,\n          admitted != ss.acked),",
    "CAUGHT-T",
    "an honest advancing ACK trips the detector -> ackAboveLiveIsIgnoredTest",
)

row(
    "M45",
    "M",
    "replicaAckAs / ignored-ack coverage latch",
    "        ackIgnored: latch(coverage.ackIgnored, ackIgnored(reported, live)),",
    "        ackIgnored: coverage.ackIgnored,",
    "CAUGHT-T",
    "ackAboveLiveIsIgnoredTest asserts coverage.ackIgnored",
)

row(
    "M46",
    "M",
    "replicaAck / a replica ACKs what it landed (TR-REPLICATION-033)",
    "  action replicaAck(s: SessionId): bool = replicaAckAs(s, wireMax(sessions.get(s)))",
    "  action replicaAck(s: SessionId): bool = replicaAckAs(s, live)",
    "CAUGHT-P",
    "acking the live head credits offsets the replica never received",
)

row(
    "M47",
    "M",
    "replicaAckAbove / the hostile ACK is above live",
    "  action replicaAckAbove(s: SessionId): bool = replicaAckAs(s, live + 1)",
    "  action replicaAckAbove(s: SessionId): bool = replicaAckAs(s, live)",
    "CAUGHT-T",
    "ackAboveLiveIsIgnoredTest asserts ignored_acks == 1",
)

row(
    "M48",
    "M",
    "step / supersede unwired",
    "      supersede(s, t),\n",
    "",
    "MISSED",
    "structural limit (the Q4 M22 class): tests call actions directly",
)

row(
    "M49",
    "M",
    "step / lapseBarrier unwired",
    "      lapseBarrier(b),\n",
    "",
    "MISSED",
    "structural limit: the witness is the only signal and it is not gated",
)

row(
    "M50",
    "M",
    "init / defect ghosts start clear",
    "      clampedAck: false, supersededRecordedDeparture: false,",
    "      clampedAck: true, supersededRecordedDeparture: false,",
    "CAUGHT-T",
    "the ghost-backed invariants read defects; every ack test asserts it clear",
)

# ---------------------------------------------------------------------------
# D. Invariant clauses (mutating the property: a violation on the UNMUTATED model
#    proves the clause is load-bearing rather than decorative)
# ---------------------------------------------------------------------------

row(
    "I01",
    "X",
    "inv_no_ship_inside_barrier_window / the isEnding carve-out",
    "        isEnding(ss) or optForall(armed.get(b), floor => wireMax(ss) <= floor)",
    "        optForall(armed.get(b), floor => wireMax(ss) <= floor)",
    "CAUGHT-P",
    "the disclosed FM-CLUSTER-097 divergence: an ending session does drain past the floor",
)

row(
    "I02",
    "X",
    "inv_no_ship_inside_barrier_window / unarmed barriers are vacuous",
    "        isEnding(ss) or optForall(armed.get(b), floor => wireMax(ss) <= floor)",
    "        isEnding(ss) or optExists(armed.get(b), floor => wireMax(ss) <= floor)",
    "CAUGHT-P",
    "optExists turns 'not armed' into a violation: proves the None handling is load-bearing",
)

row(
    "I03",
    "X",
    "inv_no_ship_inside_barrier_window / the floor is inclusive",
    "        isEnding(ss) or optForall(armed.get(b), floor => wireMax(ss) <= floor)",
    "        isEnding(ss) or optForall(armed.get(b), floor => wireMax(ss) < floor)",
    "CAUGHT-P",
    "a wire sitting exactly at the floor is lawful: proves the boundary is at the floor",
)

row(
    "I04",
    "X",
    "inv_nothing_held_is_dropped / the Ended exclusion",
    "      ss.stage == Ended or\n        ss.accepted.length() == ss.sent.length() + ss.held.length() + inFlight",
    "      ss.accepted.length() == ss.sent.length() + ss.held.length() + inFlight",
    "CAUGHT-P",
    "ending IS the licence to drop the buffer: proves the exclusion is load-bearing",
)

row(
    "I05",
    "X",
    "inv_duplicate_identity_is_always_kickable / both directions",
    "        canSupersede(sessions.get(a), sessions.get(b)) or\n        canSupersede(sessions.get(b), sessions.get(a))))",
    "        canSupersede(sessions.get(a), sessions.get(b))))",
    "CAUGHT-P",
    "the overlap is resolvable in whichever direction is newer: proves symmetry is needed",
)

row(
    "I06",
    "X",
    "inv_ack_never_above_the_wire / distinct from ack-vs-live",
    "      ss.acked <= wireMax(ss)",
    "      ss.acked <= live",
    "MISSED",
    "weakening: it collapses onto inv_ack_never_above_live (paired check P6 shows the loss)",
)

row(
    "I07",
    "X",
    "inv_stage_shape / Sending <-> in_flight is an equivalence",
    "        (ss.stage == Sending) == (ss.in_flight != None),",
    "        (ss.stage == Sending) implies (ss.in_flight != None),",
    "MISSED",
    "weakening: paired check P7 shows which rows it costs",
)

row(
    "I08",
    "X",
    "inv_offsets_within_live / the wire clause",
    "        wireMax(ss) <= live,",
    "        true,",
    "MISSED",
    "weakening: paired check P8 shows which rows it costs",
)

# ---------------------------------------------------------------------------
# E. Paired clause-load-bearing checks: an oracle-widening edit applied TOGETHER
#    with a model mutation that the intact oracle catches. Expectation is that
#    the catch DISAPPEARS, which is what proves the clause is load-bearing.
#    `expect` here means: the paired run is green (i.e. the clause was carrying it).
# ---------------------------------------------------------------------------

row(
    "P1",
    "L",
    "gracefulLoss held-clause is what catches E10 (end-without-drain)",
    "    applyFlush({ ...ss, ending: Some(d), cause: Some(c) })",
    "    applyEndWith({ ...ss, cause: Some(c) }, d)",
    "PAIRED",
    "with the pre.held clause blanked, inv_graceful_end_flushed_everything must stop firing",
    extra=[("L", "      pre.held.length() > 0,", "      false,")],
)

row(
    "P2",
    "L",
    "defects.resentBelowResume latch is what catches E23 (dedup off)",
    "    if (f > ss.resume_offset)",
    "    if (true)",
    "PAIRED",
    "with the latch removed, inv_no_resend_below_resume must stop firing",
    extra=[
        (
            "M",
            "        resentBelowResume: latch(defects.resentBelowResume, resentBelowResume(post)),",
            "        resentBelowResume: defects.resentBelowResume,",
        )
    ],
)

row(
    "P3",
    "L",
    "defects.clampedAck latch is what catches E32 (clamp)",
    "    if (ackIgnored(reported, live)) ss.acked else max2(ss.acked, reported)",
    "    if (ackIgnored(reported, live)) live else max2(ss.acked, reported)",
    "PAIRED",
    "with the latch removed, inv_ack_ignored_not_clamped must stop firing",
    extra=[
        (
            "M",
            "        clampedAck: latch(defects.clampedAck,\n          admitted != ss.acked and admitted != reported),",
            "        clampedAck: defects.clampedAck,",
        )
    ],
)

row(
    "P4",
    "L",
    "defects.supersededRecordedDeparture latch is what catches E12",
    "        | Some(d) => if (d == Superseded) prev else Some(d)",
    "        | Some(d) => Some(d)",
    "PAIRED",
    "with the latch removed, inv_superseded_writes_no_departure must stop firing",
    extra=[
        (
            "M",
            "        supersededRecordedDeparture:\n          latch(defects.supersededRecordedDeparture, newRecord != departure_record),",
            "        supersededRecordedDeparture: defects.supersededRecordedDeparture,",
        )
    ],
)

row(
    "P5",
    "M",
    "the isEnding carve-out does not swallow a live session (M17 still caught)",
    "    val held = gateHeld(armed)",
    "    val held = false",
    "PAIRED",
    "with the carve-out widened to every session, the invariant must stop catching M17",
    extra=[
        (
            "X",
            "        isEnding(ss) or optForall(armed.get(b), floor => wireMax(ss) <= floor)",
            "        true or optForall(armed.get(b), floor => wireMax(ss) <= floor)",
        )
    ],
)

row(
    "P6",
    "M",
    "inv_ack_never_above_the_wire is what catches M46 (ack the live head)",
    "  action replicaAck(s: SessionId): bool = replicaAckAs(s, wireMax(sessions.get(s)))",
    "  action replicaAck(s: SessionId): bool = replicaAckAs(s, live)",
    "PAIRED",
    "with the wire clause weakened to live, the ack-vs-wire catch must disappear",
    extra=[("X", "      ss.acked <= wireMax(ss)", "      ss.acked <= live")],
)

row(
    "P7",
    "L",
    "inv_stage_shape's equivalence is what catches E29 (in-flight retained)",
    "    val after = { ...ss, sent: landed, in_flight: None }",
    "    val after = { ...ss, sent: landed }",
    "PAIRED",
    "with the equivalence weakened to an implication, the shape catch must disappear",
    extra=[
        (
            "X",
            "        (ss.stage == Sending) == (ss.in_flight != None),",
            "        (ss.stage == Sending) implies (ss.in_flight != None),",
        )
    ],
)

row(
    "P8",
    "M",
    "inv_offsets_within_live's wire clause is what catches M12 (replay past live)",
    "      r <= live,",
    "      true,",
    "PAIRED",
    "with the wire clause blanked, the replay-past-live catch must weaken",
    extra=[("X", "        wireMax(ss) <= live,", "        true,")],
)
