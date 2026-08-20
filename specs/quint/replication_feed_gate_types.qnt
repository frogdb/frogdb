// replication_feed_gate_types — types and constants for the replica feed-gate / barrier
// session model (formal-spec phase 3, campaign task T9b; the issue-26 class). The
// state-shape half of the model: the per-session sequencing `Stage` behind
// TR-REPLICATION-013's hold/release, the `Departure` classification of
// TR-REPLICATION-007 / FM-REPLICATION-062 (including the issue-22 `Superseded` outcome),
// the `EndCause` that says *which* input class ended the link, and the per-session record
// the FM-REPLICATION-015 replay-dedup and the FM-CLUSTER-097 node-wide hold are judged on.
//
// Single responsibility: the *shape* of this model's state and the constants that bound
// it — nothing that reads or writes state.
//   Belongs here:     type declarations, sizing constants (`SESSIONS`, `BARRIERS`,
//                     `MAX_WRITES`, `CHANNEL_CAP`, `IDENTITIES`).
//   Does NOT belong:  `var`s, guards, updates, invariants, witnesses, tests. Guards and
//                     updates live in replication_feed_gate_logic.qnt; the `var`s and the
//                     actions in replication_feed_gate_machine.qnt; everything checkable
//                     in replication_feed_gate.qnt (the runnable main model, which also
//                     carries the full "Rows modeled" header).
//   Shared vocabulary: `Option` comes from cluster_common_types.qnt — the type is declared
//                     exactly once in this directory because Quint's sum-type constructors
//                     are nominal (see that file's header and lib_option.qnt's). The
//                     module's *name* says cluster only because the cluster models were
//                     its first consumers; `Option` itself is area-neutral, and this
//                     replication model is deliberately its third consumer rather than
//                     declaring a second, incompatible `Some`/`None`.
module replication_feed_gate_types {
  import cluster_common_types.* from "./cluster_common_types"

  // One replica-facing streaming session (the thing `FeedSequencer` is one instance of).
  type SessionId = int

  // A replication-stream offset. Frames carry the offset of the byte range they end at,
  // so "offset" and "frame sequence number" are the same number in this model
  // (TR-REPLICATION-030: the live offset advances as the backlog is appended).
  type Offset = int

  // One barrier arm/release cycle (TR-REPLICATION-013). Two of them so the model can
  // exercise overlapping holds — the case where one barrier releases while another is
  // still in force, which is the reason the gate publishes a *deadline* rather than a
  // boolean latch (FM-CLUSTER-097).
  type BarrierId = int

  // The session's sequencing stage — the model's copy of `FeedSequencer::Stage`.
  //
  //   Handoff    — the session has not yet replayed the backlog tail it was granted
  //                (`+CONTINUE` / full-sync trailer). It has put nothing on the wire.
  //   Receiving  — idle, waiting for the next frame from the upstream broadcast channel.
  //   Consulting — a decision point: the driver must read the feed gate before the
  //                session may put anything on the wire. Every path to the wire goes
  //                through here, which is the whole point of the seam.
  //   Holding    — the gate said "held". Frames keep arriving and keep being buffered;
  //                nothing goes out until a release is observed.
  //   Sending    — a frame is on the wire and its outcome (delivered / delivered but
  //                over the lag budget / failed) has not come back yet.
  //   Ended      — terminal. The link is gone and the buffer is dropped.
  type Stage = Handoff | Receiving | Consulting | Holding | Sending | Ended

  // How a session left, per TR-REPLICATION-007 and FM-REPLICATION-062 — classified
  // *before* the session leaves the registry, because only a graceful departure disarms
  // the self-fence.
  //
  //   Graceful   — the upstream source closed cleanly (the primary is shutting the stream
  //                down, not losing it).
  //   Lost       — anything else the link did: the receiver fell behind the broadcast
  //                channel, the send failed, or the frame that landed breached the lag
  //                budget. All three mean "this replica may have missed writes".
  //   Superseded — the ruled issue-22 outcome: this session did not fail, it was replaced
  //                by a newer session announcing the same replica identity. A superseded
  //                session writes **no** departure record (see `departure_record` in the
  //                machine): its record would otherwise land *after* the successor's
  //                clear and silently un-fence a primary whose replica is in fact fine.
  type Departure = Graceful | Lost | Superseded

  // Which input class ended the link. Ghost detail: no guard reads it; the invariants use
  // it to check that the classification in `Departure` matches the input that produced it
  // (TR-REPLICATION-007's "classified before leaving"), which a `Departure` alone cannot
  // express because three distinct causes all map to `Lost`.
  type EndCause = ClosedByPeer | ReceiverLagged | SendFailure | LagBreach | Kicked

  type SessionState = {
    stage: Stage,
    // The high-water mark the handoff replayed up to. Frames at or below it are already
    // on the wire and must NOT be sent again — FM-REPLICATION-015's "exactly
    // `(replay_from, current]`, once, in order".
    resume_offset: Offset,
    // Frames buffered but not yet on the wire, oldest first. Unbounded by construction:
    // there is deliberately no hold-overflow disconnect in this model, because the real
    // system has none either — the only overrun that disconnects a replica is the
    // upstream broadcast channel's (`SourceLagged`), not the sequencer's held buffer.
    held: List[Offset],
    // The frame handed to the transport whose outcome has not come back yet.
    in_flight: Option[Offset],
    // The departure this session will report once its buffer has drained. Set at
    // classification time, consumed at end time — the gap between the two is exactly the
    // "flush before you end" obligation.
    ending: Option[Departure],
    cause: Option[EndCause],
    // The replica identity this session announced at handoff. `None` models an
    // *unannounced* identity, which the issue-22 amendment says is never deduped: an
    // unannounced session can neither supersede nor be superseded.
    identity: Option[int],
    // Arrival order among sessions that reached streaming, from the machine's monotone
    // counter. Newest-wins is what makes "supersede" well-defined without a wall clock
    // (global no-wall-clock ruling).
    stream_seq: int,
    // How far this session has consumed the upstream broadcast channel. The gap between
    // `cursor` and the live offset is the channel backlog this receiver is behind by —
    // the quantity that trips `SourceLagged`.
    cursor: Offset,
    // Ghost histories, in order, never read by a guard:
    //   `accepted` — every frame this session took into its buffer (i.e. survived the
    //     dedup filter). The FIFO invariant is stated as "`sent` is a prefix of
    //     `accepted`", which says no-skip, no-reorder and no-duplicate in one claim.
    //   `sent` — every frame that actually reached the wire, in wire order.
    accepted: List[Offset],
    sent: List[Offset],
    // The offset this replica has ACKed. TR-REPLICATION-033: a replica ACKs the offset it
    // *landed*, never the one it claimed or merely received; TR-REPLICATION-018 as
    // amended by issue 21 refuses (and counts) an ACK above the live offset instead of
    // clamping it.
    acked: Offset,
  }

  // ---------------------------------------------------------------------------
  // Sizing constants — bounds on the state space a sampled run explores, not facts about
  // the system. Each is the smallest value that still reaches the behaviour it exists for.
  // ---------------------------------------------------------------------------

  // Two sessions: the minimum that can exercise supersession (issue 22) and a node-wide
  // hold covering more than one session at once (FM-CLUSTER-097 is a *node* property, so
  // a one-session model could not distinguish it from a per-session one).
  pure val SESSIONS: Set[SessionId] = Set(1, 2)

  // Two barriers: the minimum that can overlap, so a release can arrive while the gate is
  // still held by the other arm.
  pure val BARRIERS: Set[BarrierId] = Set(1, 2)

  // How far the primary's live offset may advance. Four writes is enough for a handoff to
  // dedup a non-trivial prefix and still leave frames to buffer, hold and drain.
  pure val MAX_WRITES: Offset = 4

  // How far a session's cursor may fall behind the live offset before the upstream
  // broadcast channel overruns it. This is the *only* overflow that disconnects.
  pure val CHANNEL_CAP: int = 2

  // The announceable replica identities. Two so that two sessions can announce the same
  // identity (the supersession case) or different ones (no supersession); `None` — an
  // unannounced identity — is the third case and is not a member of this set.
  pure val IDENTITIES: Set[int] = Set(7, 8)
}
