// Full-sync / PSYNC handoff — type vocabulary and model sizing.
//
// Types and sizing constants for the phase-3 replication handoff model. No state,
// no logic: the state machine lives in `replication_fullsync_machine`, the pure
// guards/updates in `replication_fullsync_logic`, and the properties in the main
// module `replication_fullsync`.
//
// Rows modelled by the family (see the main module for the per-property mapping):
//   TR-REPLICATION-001  PSYNC arm selection (partial grant vs. full snapshot)
//   TR-REPLICATION-002  granted partial: phase, backlog tail, ResumeOffset seed
//   TR-REPLICATION-003  full resync cuts a checkpoint at a captured offset
//   TR-REPLICATION-005  payload finish -> Streaming at the same generation
//   TR-REPLICATION-006  sync failure before Streaming -> Disconnecting, no stream
//   TR-REPLICATION-014  frame claim/apply/land on the replica
//   TR-REPLICATION-015  full resync installs an absolute offset pair
//   TR-REPLICATION-016  promotion freezes the window at the applied offset
//   TR-REPLICATION-018  ACK ingest (issue-21 ruling: ignore + count)
//   TR-REPLICATION-019  granted +CONTINUE shifts the inherited id
//   TR-REPLICATION-020  full-sync trailer adoption clears the failover window
//   TR-REPLICATION-021  identity recovery gated on intact dataset recovery
//   TR-REPLICATION-022  registration against an announced identity (issue-22)
//   FM-REPLICATION-004  the acked-write-loss row: offset captured before the cut
//   FM-REPLICATION-012  replay is contiguous or it does not happen
//   FM-REPLICATION-013  every PSYNC resolves to exactly one arm
//   FM-REPLICATION-014  the armed backlog floor is the only lower bound
//   FM-REPLICATION-016  backlog eviction raises the floor to the evicted end
//   FM-REPLICATION-019  promotion mints a new history, freezes the inherited one
//   FM-REPLICATION-020  no reader observes a half-updated identity pair
//   INV-OFFSET-1        landed <= applied <= live
//   INV-REPLID-1        the failover window is whole or absent
//
// Deliberate abstractions, each documented at its use site:
//   * a write is one unit of offset, identified by a unique `WriteId`, so "the
//     same offset carrying different bytes" (the issue-24 divergence class) is
//     expressible without modelling command payloads;
//   * ClaimedOffset and LandedOffset are collapsed into one `applied` field. The
//     claim/land split is the ACK-vs-apply race of TR-REPLICATION-014, not the
//     handoff window this model is about. The split that *is* load-bearing here
//     — received vs. applied — is kept (`recv` vs `applied`), because
//     TR-REPLICATION-016 freezes the failover window at the applied offset and
//     FM-REPLICATION-019 names "frozen at the received head" as not-observable;
//   * a replication id is an abstract generation counter, not 40 hex chars. The
//     malformed-id leg of TR-REPLICATION-019 is a parsing concern, out of scope.
//
// `NodeId` and `Option` come from `cluster_common_types`, the single nominal
// declaration site for both (its name predates the replication models; the types
// it declares are not cluster-specific).
module replication_fullsync_types {
  import cluster_common_types.* from "./cluster_common_types"

  // A replication history identity (`replication_id`). Abstract: two nodes
  // carrying the same generation claim the same history, so equality is the only
  // operation the model needs. Minted from a monotone counter so a *fresh* id is
  // always numerically greater than every id ever issued.
  type Gen = int

  // One replicated write. Unique across the run, so a divergent history is
  // detectable by comparing contents at the same position.
  type WriteId = int

  // A byte offset in the replication stream. One write == one unit.
  type Offset = int

  // A primary-side replication session (`ReplicaSession`).
  type SessionId = int

  type Role =
    | Primary
    | Replica

  // Primary-side record of how the last streaming session ended
  // (`LastStreamingDeparture`). `Superseded` is the issue-22 amendment: a
  // predecessor kicked by a duplicate registration records *no departure at all*,
  // so this constructor exists only to make "the code wrote one" observable.
  type Departure =
    | Graceful
    | Lost
    | Superseded

  // `SessionPhase`, with one refinement: the spec's StreamingCheckpoint ->
  // Streaming edge is split by `PayloadInstalled`, so the window between "the
  // replica has installed the payload" and "the primary has spliced the stream"
  // is a state the model can stop in and check. That window is the whole subject
  // of FM-REPLICATION-004/-005.
  type Phase =
    | Connecting
    | PreparingCheckpoint
    | StreamingCheckpoint
    | PayloadInstalled
    | Streaming
    | Disconnected

  // The arm TR-REPLICATION-001 / FM-REPLICATION-013 resolves a PSYNC to.
  type Arm =
    | PartialGrant
    | FullSnapshot

  // How a streamed frame landed on the replica's dataset. `Appended` is the
  // ordinary case; `Reapplied` is the lawful overlap FM-REPLICATION-001 names as
  // a deliberate non-guarantee; `Gapped` and `Diverged` are the two shapes that
  // must never occur.
  type ApplyOutcome =
    | Appended
    | Reapplied
    | Gapped
    | Diverged

  type NodeState = {
    role: Role,
    // The dataset, as the ordered sequence of replicated writes it holds. For a
    // primary this is also the stream content it serves.
    data: List[WriteId],
    // `replication_id` / `replid2` + `second_replid_offset`. INV-REPLID-1: the
    // window is whole or absent, so `replid2 == None` iff `second_offset` is
    // NO_WINDOW.
    replid: Gen,
    replid2: Option[Gen],
    second_offset: Offset,
    // LiveOffset: the primary's head, or the replica's received/decoded head.
    recv: Offset,
    // ClaimedOffset == LandedOffset (collapsed, see the header). INV-OFFSET-1.
    applied: Offset,
    // The armed backlog floor (`BacklogStart`), UNARMED until the first push.
    floor: Offset,
    // The replica's own current link, if any. A session may outlive this pointer
    // — that corpse is the issue-22 shape.
    link: Option[SessionId],
    last_departure: Option[Departure],
    // Issue-21 ruling: acks above the live head are counted, not applied.
    acks_ignored: int,
  }

  type Session = {
    primary: NodeId,
    replica: NodeId,
    // The replica identity announced at registration (`replconf listening-port`
    // + node id). `None` models the `listening_port == 0` case that
    // TR-REPLICATION-022 exempts from dedup.
    identity: Option[NodeId],
    phase: Phase,
    arm: Arm,
    // The generation the grant/trailer carries.
    granted_gen: Gen,
    // The offset the arm was granted at: `snapshot_offset` for a full snapshot
    // (captured *before* the cut, FM-REPLICATION-004), `replay_from` for a
    // partial grant.
    grant_offset: Offset,
    // What the checkpoint cut actually materialised. Empty until the cut.
    payload: List[WriteId],
    payload_cut: bool,
    // AckedOffset for this session (TR-REPLICATION-018).
    acked: Offset,
    // ResumeOffset, seeded at the splice (TR-REPLICATION-002).
    resume_offset: Offset,
  }

  // Bookkeeping that is not any node's state: the mints and the previous-step
  // shadow the identity-pairing invariant compares against.
  type Ctl = {
    next_write: WriteId,
    next_gen: Gen,
    // Per node, the (generation, live offset) pair observed in the previous
    // state, plus whether the node was a primary then. Feeds the
    // lexicographic-monotonicity invariant (issue-24 atomic pairing).
    prev_pair: NodeId -> { gen: Gen, off: Offset, primary: bool },
    // Remaining budget for role-destroying events (promotion, restart). See
    // MAX_DISRUPTS.
    disrupts_left: int,
  }

  // --- Sizing ---

  // Three nodes: one primary, two replicas. Two replicas are the minimum for
  // "promotion mid-sync" (one promotes while the other is still syncing).
  pure val NODES: Set[NodeId] = Set(1, 2, 3)

  pure val INITIAL_PRIMARY: NodeId = 1

  // Three session slots: two live links plus one corpse (the issue-22 shape).
  pure val SIDS: Set[SessionId] = Set(1, 2, 3)

  // Backlog capacity in offset units. Two is enough to make eviction reachable
  // inside the write budget while leaving a resumable window.
  pure val BACKLOG_CAP: int = 1

  // Total writes admitted across the run, bounding the state space.
  pure val MAX_WRITES: int = 4

  // Total fresh generations admitted (promotions + unclean restarts).
  pure val MAX_GENS: int = 3

  // Total role-destroying events admitted across the run: promotions and
  // restarts, whether or not they mint a generation. A bound in the same family
  // as MAX_WRITES and MAX_GENS, and needed for the same reason plus one more:
  // both actions are enabled in nearly every state and clear the whole session
  // registry, so an unbounded supply of them lets a random walk restart the
  // primary before any handoff finishes, and the deep states this model exists
  // to check are never reached. Two is enough for "promotion mid-sync" and for
  // "restart, then re-register".
  pure val MAX_DISRUPTS: int = 2

  pure val FIRST_GEN: Gen = 1

  // `Option`-free sentinels, matching the code's own: `second_replid_offset` and
  // `BacklogStart` are absolute values with an out-of-band "unset".
  pure val NO_WINDOW: Offset = -1
  pure val UNARMED: Offset = -1

  // The id a replica that has never held a history presents (`PSYNC ? -1`).
  pure val NO_HISTORY: Gen = 0
}
