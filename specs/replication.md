# Replication — failure modes

Status: LOCKED (2026-08-04) — Phase 3 mutation gate passed (frogdb-replication 98.7% on 1180
mutants, frogdb-replication-runtime 100% of viable, vs an 85% gate; the 15 surviving mutants are
all documented equivalents at the code). Behavior changes to this area are spec-first: edit the
row, update the forcing test, then the code. See CLAUDE.md "Locked core areas".

Every way FrogDB's replication link can fail, refuse, or succeed, one table per mode. This is the
reference the mutation run is measured against: a mutant that survives is a row nothing forces.

Scope, part one — the full-sync payload path (FM-REPLICATION-001..005): what a primary puts on the
wire when it grants a `+FULLRESYNC` (`frogdb-replication/src/replica_session.rs`), the envelope and
its markers (`frogdb-replication/src/fullsync.rs`), what the replica accepts, verifies and installs
(`frogdb-replication/src/replica/connection.rs`), the shard-level export/install seam
(`frogdb-core/src/shard/dispatch_replication.rs`), the dataset framing they share
(`frogdb-persistence/src/serialization/dataset.rs`), and the server-side wiring that connects them
(`frogdb-replication-runtime/src/{export,install}.rs`). Rows stop at what a *client of the replica*
can observe once the sync reports done.

Scope, part two — the steady-state link (FM-REPLICATION-006..012): what the replica does with the
frames a sync hands it. The decode loop and the ACK it answers with
(`frogdb-replication/src/replica/streaming.rs`), the received/applied offset pair and the gate that
guards it (`frogdb-replication/src/replica/offset.rs`), the frame consumer that applies them
(`frogdb-replication/src/apply.rs`) through the executor seam
(`frogdb-replication-runtime/src/executor.rs`), and the primary-side backlog the link resumes from
(`frogdb-replication/src/primary/{ring_buffer,replay}.rs`). Rows stop at what a client of *either*
node can observe: the replica's keyspace and `INFO replication`, and the primary's `WAIT`.

Scope, part three — the rest of the replication runtime (FM-REPLICATION-013..043), added by the
Phase 3 hardening pass so that no source file in the area is unspecced. In id order:

* **Handshake and resume arms (013-018)** — how a `PSYNC` resolves to `+FULLRESYNC` or `+CONTINUE`
  and how honestly the fallback is classified, the armed floor that is the sole lower bound on a
  resume, what a granted `+CONTINUE` actually replays, the backlog's two retention caps, and the
  two edges: a `PSYNC` behind the shutdown drain, and an unknown `REPLCONF` option
  (`frogdb-replication/src/primary/{replay,ring_buffer}.rs`, `replica_session.rs`).
* **Identity and promotion (019-024)** — minting a new history, freezing the inherited one at the
  applied offset, what a promotion that cannot persist must not adopt, what survives a restart,
  what a demotion closes, and the split-brain audit
  (`frogdb-replication/src/identity.rs`, `frogdb-server/crates/server/src/role_manager.rs`).
* **Replica lifecycle (025-030)** — `REPLICAOF`/`REPLICAOF NO ONE`, the reconnect loop, the
  read-only refusal, serving reads while behind, and independent expiry
  (`frogdb-replication/src/replica/mod.rs`, `frogdb-server/crates/server/src/commands/replication.rs`,
  `frogdb-server/crates/server/src/connection/guards.rs`).
* **Framing and codec (031-036)** — what the offset counts, whole-frame decode, the ACK grammar,
  the shard-tagged transaction group, and the checkpoint envelope's bounds
  (`frogdb-replication/src/frame.rs`, `apply.rs`, `fullsync.rs`, `offset_coordinator.rs`).
* **WAIT and the replica registry (037-043)** — what `WAIT` may answer, when it solicits a
  `GETACK`, what the count counts, what a `WAIT` parked across a demotion returns, the two write
  fences, and the one registry `INFO`/`ROLE` render from
  (`frogdb-replication/src/tracker.rs`, `frogdb-server/crates/server/src/commands/{wait,info}.rs`).

Scope, part four — rows added by closing a filed bug (044-050), each naming its issue in `Bug refs`.
These are not a separate area; they are the rows a fix had to write because the behavior it
established was not previously specced anywhere: what a checkpoint file *name* may be (044), the
ceiling on a reconstructed replicated `MULTI` (045), the round trip of the `min-replicas-to-write`
freshness window (046), which config keys own the backlog's two caps (047), what split-brain
observability may claim when the audit write fails (048), the replica identity announced at the
handshake and rendered by `INFO`/`ROLE` (049), and the three resync counters (050).

Scope, part five — the `frogdb-replication-runtime` seams themselves (051-053), added by the Phase 3
mutation round because the crate's own unit tests reached almost none of them: the applier that turns
a replicated frame into exactly one shard message (051), and the two install paths a full-sync payload
lands through — a live dataset re-partitioned onto this node's shards (052) and a staged RocksDB
checkpoint installed shard-for-shard with its warm tier materialized (053).

Scope, part six — the control lane (FM-REPLICATION-054..057): process-wide state that lives *beside*
the keyspace and therefore has no shard to be tagged with. Today that is the function registry
(`frogdb-server/crates/server/src/function_store.rs`, `frogdb-core/src/scripting/functions`), carried as
`CONTROL_SHARD`-tagged frames the replica applies through `ControlApplier`
(`frogdb-replication/src/apply.rs`), plus the full-resync hook that seeds a fresh replica
(`frogdb-replication/src/replica_session.rs`). Rows stop at what a client of either node can call.

Adjacent specs: the boot-time half of a full sync — installing a staged checkpoint and adopting the
replication id/offset it carries — lives in
[Persistence — failure modes](persistence.md) (FM-PERSISTENCE-027, -038, -039).

## State space

Extracted from the structs that back this area.

| Variable | Authoritative field | Writer(s) | Persistence | Survives restart |
|---|---|---|---|---|
| ReplicationId | `ReplicationState.replication_id` (`state.rs`) | `ReplicationState::new` (fresh mint); `shift_replication_id`/`_inner` (promotion mint, `mem::replace`s the old value into `SecondaryId`); `adopt_replication_history`/`_inner` (demotion/resync adopts upstream's); `load_or_create` (regenerate on missing/corrupt/invalid file) | `replication_state.json`, via `ReplicationState::save` (temp file + `fs::rename`, **no fsync**) | Yes today — `load_or_create` reads the file unconditionally, independent of dataset recovery. Pending, see TR-REPLICATION-021: issue 24's ruled-but-unimplemented semantics make this conditional on an *intact* dataset recovery; this row will change once that lands. |
| SecondaryId | `ReplicationState.secondary_id: Option<String>` | `shift_replication_id_inner` (sets); `clear_secondary_window_inner` (clears to `None`); `adopt_replication_history_inner` (clears via the same call) | Same file, `serde(default)` | Yes |
| SecondaryOffset | `ReplicationState.secondary_offset: i64` | Same three writers as SecondaryId, kept in lockstep (`shift_replication_id_inner` sets it to the boundary; `clear_secondary_window_inner` resets it to the `-1` sentinel) | Same file | Yes |
| OffsetAtSave | `ReplicationState.offset_at_save: u64` (serde alias `replication_offset`) | `apply_staged_metadata` (`state.rs:474`, absolute set from `StagedReplicationMetadata.replication_offset` on a checkpoint install — the only non-raise-only writer; that transition is owned by the persistence area, see TR-PERSISTENCE-043, and deliberately has no row here); `OffsetCoordinator::reconcile_for_persist` (`offset_coordinator.rs:316`, raise-only `max` against the applied offset, primary side — see TR-REPLICATION-031); `ReplicaOffset::reconcile_for_persist` (`replica/offset.rs:597`, same raise-only shape, replica side); `plan_primary_stint` (`primary/promotion.rs:70`, raise-only `max` against the promotion boundary, committed via `save_snapshot(&plan.minted)` at `primary/mod.rs:646-648` — see TR-REPLICATION-008). The two `reconcile_for_persist` paths are reached through `save_state()`: the primary's (`primary/mod.rs:640-642`) is called from the periodic snapshot hook (`server/init.rs:385-387`, fired by `persistence/src/snapshot/rocks_coordinator.rs:241`) and the shutdown hook (`server/subsystems.rs:806-814`), both guarded `!is_replica`; the replica's (`replica/mod.rs:325`) has **no production caller** today — test/model harness only. | Itself the persisted field | Yes — seeds `ReplicationIdentity::adopting`'s `live.fetch_max(offset_at_save)` (raise-only) |
| LiveOffset (received) | `ReplicationIdentity.live: Arc<AtomicU64>` (`identity.rs`) | `OffsetCoordinator::advance` (primary write path, `fetch_add`); `OffsetCoordinator::settle_at_applied_inner` (promotion boundary, `store(applied)` — a rewind); `ReplicationIdentity::adopting` (boot seed, `fetch_max(offset_at_save)`) | Not persisted directly | No — rebuilt from OffsetAtSave at boot |
| ClaimedOffset | `AppliedOffset.applied: Arc<AtomicU64>` (`replica/offset.rs`) — the struct is also named `AppliedOffset`; this row is its `applied` field specifically, not the type | `advance_by` (primary path, ungated `fetch_add`); `claim` (replica path, gated `fetch_add` on `Claim::Granted`); `reset_pair` (full-resync install, absolute set, bumps Epoch) | Not persisted | No |
| LandedOffset | `AppliedOffset.landed: Arc<AtomicU64>` | `advance_by` (moves with ClaimedOffset via `fetch_max`); `land` (`fetch_max(applied.load())`, called after each shard apply); `reset_pair` (the **only** path that can move it backward) | Not persisted — this is a different atomic from the primary-side AckedOffset it is eventually certified against over the wire | No |
| ApplyGateFrozen / ApplyGateStint | `AppliedOffset.gate: Arc<Mutex<ApplyGate{frozen, stint}>>` | `freeze()` (promotion, sets `frozen=true`, returns the boundary); `begin_replica_stint()` (unfreezes, `stint += 1`); `retire_replica_applies()` (demotion, `stint += 1` **without** unfreezing) | Not persisted | No. Pending, see TR-REPLICATION-009: a promotion-persist failure (issue 16) leaves this frozen with no code path back to unfrozen short of a fresh `begin_replica_stint` — the stranded state issue 16 rules must become *observable*, not undone. |
| Epoch | `AppliedOffset.epoch: Arc<AtomicU64>` | `reset_pair` only (every full-resync install) | Not persisted | No. Pending, see TR-REPLICATION-015: issue 24's amendment (point 3) proposes keying INV-OFFSET-2's monotonicity claim on `(ReplicationId, Epoch)`; Epoch has no persisted counterpart today, so a restart cannot distinguish "same epoch" from "different epoch" at all. |
| DivergedEpoch | `AppliedOffset.diverged: Arc<AtomicU64>` (`NO_DIVERGENCE` sentinel, or the diverged epoch) | `admit_divergence` (latches, only if Epoch is still current); `reset_pair` (clears on fresh install) | Not persisted | No |
| BacklogStart | `ReplicationRingBuffer.start: AtomicI64` (`UNARMED = -1`) (`primary/ring_buffer.rs`) | `arm_backlog_floor`/`arm_start` (promotion/demotion re-anchor); `reset_backlog` (back to `UNARMED`); the eviction path (raises with each dropped entry) | Not persisted | No |
| BacklogEntries | `ReplicationRingBuffer.entries: Mutex<VecDeque<BufferedCommand>>` | `record` (push); eviction on `max_entries`/`max_bytes` | Not persisted | No |
| SessionPhase | Per-`ReplicaSession` `Phase` (`Connecting \| PreparingCheckpoint \| StreamingCheckpoint \| Streaming \| Disconnecting`) (`replica_session.rs`) | `session_machine::step`'s returned `Transition::phase`, applied by `ReplicaSession::run`'s driver loop (monotonic; `Disconnecting` terminal by construction) | Not persisted (session-lifetime only) | No — the session itself does not survive a restart |
| LastStreamingDeparture | `ReplicationTrackerImpl.last_streaming_departure: AtomicU8` (`0`=None/unknown, `1`=Graceful, `2`=Lost) (`tracker.rs`) | `record_streaming_departure` (the departing session's own exit handler only, via `Effect::RecordDeparture`); `clear_streaming_departure` (a new session entering `Streaming`, via `Effect::ClearDeparture`) | Not persisted | No. Pending, see TR-REPLICATION-010: issue 23's ruling adds a third writer — demotion clearing this to the `None` sentinel alongside FenceArmed's `reset_arming()` — not yet implemented. |
| FenceArmed | `ReplicationQuorumChecker.armed: AtomicBool` (`frogdb-replication-runtime/src/quorum.rs`) | `arm_if_streaming` (write-path lazy latch — today's only arming path); `reset_arming` (demotion, clears); `disarm_if_departed_cleanly` (clears when unarmed conditions + Graceful LastStreamingDeparture hold) | Not persisted | No. Pending, see TR-REPLICATION-023: issue 19's ruling adds session-Streaming-transition arming; not yet implemented. |
| SelfFenceEnabled | `ReplicationQuorumChecker.self_fence_enabled: AtomicBool` | `set_self_fence_enabled` (`CONFIG SET self-fence-on-replica-loss`; param registered at `params.rs:1174-1187`, TOML section `replication`, the `name` field itself carries no `replication-` prefix — see TR-REPLICATION-029) | Not persisted by `CONFIG SET` itself (atomics-only, no persist step — `runtime_config.rs:3388-3438`); persisted only by an explicit `CONFIG REWRITE`, which requires a config file (errors `"ERR The server is running without a config file"` at `runtime_config.rs:1315` if none) and writes via temp file + `sync_all()` fsync + rename (`config_persister.rs:115`) | Yes, but only after a successful `CONFIG REWRITE`; boot reads it back at `server/replication_init.rs:324-328` from `config/replication.rs:170-179`; round-trip test-pinned at `runtime_config.rs:5615-5616`/`:5638-5639` |
| FreshnessTimeoutMs | `ReplicationQuorumChecker.freshness_timeout_ms: AtomicU64` | `set_freshness_timeout_ms` (`CONFIG SET replica-freshness-timeout-ms`; same registry shape as SelfFenceEnabled, `params.rs:1174-1187` — see TR-REPLICATION-029) | Same as SelfFenceEnabled: not persisted by `CONFIG SET`, only by `CONFIG REWRITE` (`config_persister.rs:115`, fsynced) | Same as SelfFenceEnabled: yes after `CONFIG REWRITE`, reseeded at `server/replication_init.rs:324-328` |
| FeedGateHeldUntil | `ReplicaFeedGate.held_until: Mutex<Option<Instant>>` (`feed_gate.rs`) | `publish(held_until)`, driven by the pure `decide_publish`/`decide_feed_hold_until` over the set of armed cluster-side slot-handoff barrier deadlines | Not persisted | No |
| IsReplica | `RoleManager.is_replica` (`frogdb-server/crates/server/src/role_manager.rs`) | `promote` (Release, set false only after `begin_primary_stint()?` returns `Ok`); `demote` (Release, set true **first**, before `end_primary_stint`) | Not persisted. `Arc<AtomicBool>` field on `RoleManager`, seeded at boot from static config (`server/init.rs:344-346`, `config.replication.is_replica()`). A runtime `REPLICAOF` never touches disk — `commands/replication.rs:99-174` calls `request_demote`/`request_promote` only, and `replication.role` is `#[param(skip)]` (`config/replication.rs:18-20`). | Mode-dependent. Standalone: **No** — a runtime `REPLICAOF` is lost across restart unless the operator hand-edits the TOML. Cluster: **Yes, re-derived** — `reconcile_self_role` (`cluster_init.rs:225-226`) compares against the persisted Raft/cluster state (`state.get_node(self_node_id).role`, `cluster/state.rs:852-864`) at boot; `REPLICAOF` is rejected outright in cluster mode (`commands/replication.rs:111-115`). |
| PrimaryTarget | `RoleManager.primary_target: Option<SocketAddr>` | `promote` (cleared to `None` unconditionally, before the fallible `begin_primary_stint()` call — see TR-REPLICATION-011); `demote` (set to `Some(primary)`, its last step — see TR-REPLICATION-012); the constructor (`boot_target` seeds it) | Not persisted. Plain `Option<SocketAddr>` field on `RoleManager`, seeded only by the `boot_target` constructor argument, itself computed from static config at `server/replication_init.rs:206-214`; `primary-host`/`primary-port` are `#[param(skip)]` (`config/replication.rs:24-30`). | Mode-dependent, same split as IsReplica. Standalone: **No**. Cluster: **Yes, indirectly** — a demotion re-resolves the address from live cluster state (`cluster_init.rs:976-999`) and `demote()` sets it (`role_manager.rs`, immediately after the replica stream starts). |
| ReplicaRegistry | `ReplicationTrackerImpl.replicas: RwLock<HashMap<u64, Arc<ReplicaSession>>>` (`tracker.rs`) | `register_replica`/`register_announced_replica` (insert; id via `next_replica_id.fetch_add`; unconditional today — no identity dedup); `unregister_replica` (remove) | Not persisted | No |
| AckedOffset | `ReplicaSession.acked_offset: AtomicU64` (`replica_session.rs`) | `record_ack` (wire `REPLCONF ACK` only; monotonic, stores only when `sequence > prev`) | Not persisted | No |
| ResumeOffset | `ReplicaSession.resume_offset: AtomicU64` | `seed_resume_position` / `OffsetCoordinator::seed_replica_position` (the primary's own bookkeeping of where a replica resumed; never a value the replica sent) | Not persisted | No |
| RoleFenceCounter | `WaitCoordinator`'s `tokio::sync::watch::Sender<u64>` (`wait_coordinator.rs`) | `fence_role_change`, called once from `end_primary_stint` | Not persisted | No |

---

## Transitions

One `TR-REPLICATION-NNN` per action source, numbered from 001. Pre/postconditions are stated over
the State-space variables above. A row carries `Pending` when the ruled semantics (the issues are
the authority) differ from what the cited `Source` does today.

---

## TR-REPLICATION-001 — a PSYNC resolves to a partial or full resync arm

| Field | Value |
| --- | --- |
| Precondition | SessionPhase = Connecting; a `PSYNC <id> <offset>` line has been read |
| Postcondition | SessionPhase = Connecting (the reply is decided but not yet sent — see TR-REPLICATION-002); the chosen `ResumeSource` is `PartialGrant` iff ReplicationId/SecondaryId+SecondaryOffset admit the requested `(id, offset)` under `window_contains`'s two-bound check, else `FullSnapshot` |
| Source | `frogdb-replication/src/session_machine.rs:410` (`step`, `Phase::Connecting`/`SessionEvent::Begin` arm) and `:512` (`begin`); `frogdb-replication/src/replica/psync.rs:87` (`select_psync_arm`); `frogdb-replication/src/state.rs:434` (`window_contains`) |
| Rulings | — |
| Model | `replication_fullsync.qnt::inv_partial_grant_sound` |

---

## TR-REPLICATION-002 — a granted partial resync starts streaming from the backlog tail

| Field | Value |
| --- | --- |
| Precondition | SessionPhase = Connecting; the handshake reply (`+CONTINUE`) has been written to the wire (`SessionEvent::ReplySent`) after a `PartialGrant` |
| Postcondition | SessionPhase = Streaming; LastStreamingDeparture is cleared to None for this replica generation (`Effect::ClearDeparture`); BacklogEntries from `replay_from` are queued for send; once that tail has been written to the wire, ResumeOffset for this session is seeded to the last offset actually streamed (the final tail entry's offset, or `replay_from` unchanged if the tail was empty) — not the grant point itself, which the tail may have moved well past on a busy primary |
| Source | `frogdb-replication/src/session_machine.rs:542` (`reply_sent`); `frogdb-replication/src/replica_session.rs:941` (`start_streaming`), `:1030-1032` (the `seed_replica_position` call, after the tail loop) |
| Rulings | — |

---

## TR-REPLICATION-003 — a full resync cuts a checkpoint and streams it

| Field | Value |
| --- | --- |
| Precondition | SessionPhase = PreparingCheckpoint; `SessionEvent::CheckpointCut(StepOutcome::Ok)` |
| Postcondition | SessionPhase = StreamingCheckpoint; the session owns the checkpoint directory (`Effect::OwnCheckpointDir`) and begins sending it (`Effect::SendCheckpoint{path, replication_id: ReplicationId, offset: OffsetAtSave-equivalent snapshot offset}`) |
| Source | `frogdb-replication/src/session_machine.rs:450` |
| Rulings | — |
| Model | `replication_fullsync.qnt::inv_payload_covers_grant` |

---

## TR-REPLICATION-004 — a full resync without a checkpoint store exports the live dataset

| Field | Value |
| --- | --- |
| Precondition | SessionPhase = Connecting; `SessionEvent::Begin` resolves to `BeginSync::Full`, and the handler has no `rocks_store` (`live_snapshot_source` path) |
| Postcondition | SessionPhase = PreparingCheckpoint, then on `SessionEvent::DatasetExported`: the live dataset is queued (`Effect::SendLiveDataset{replication_id: ReplicationId, offset: LiveOffset at export}`) |
| Source | `frogdb-replication/src/session_machine.rs:465` (`DatasetExported` arm); `frogdb-replication/src/primary/mod.rs` (`live_snapshot_source` field, ~line 200) |
| Rulings | — |

---

## TR-REPLICATION-005 — the full-resync payload finishing streams the primary into the same generation a partial grant would

| Field | Value |
| --- | --- |
| Precondition | SessionPhase = StreamingCheckpoint; `SessionEvent::PayloadSent` |
| Postcondition | SessionPhase = Streaming; LastStreamingDeparture cleared to None for this generation (`Effect::ClearDeparture`); streaming begins from the offset the payload carried; ResumeOffset for this session is seeded the same way as TR-REPLICATION-002 — to the last offset actually streamed from any backlog tail replayed after the payload's offset, or to the payload's own offset unchanged if that tail was empty — not unconditionally to the payload's offset |
| Source | `frogdb-replication/src/session_machine.rs:474`; `frogdb-replication/src/replica_session.rs:941` (`start_streaming`), `:1030-1032` (the `seed_replica_position` call) |
| Rulings | — |
| Model | `replication_fullsync.qnt::inv_no_acked_write_lost_across_fullsync` |

---

## TR-REPLICATION-006 — a sync failure at any phase before Streaming fails the session without a partial reply

| Field | Value |
| --- | --- |
| Precondition | SessionPhase ∈ {Connecting, PreparingCheckpoint}; a `Drained`/`CheckpointCut` event carries `StepOutcome::Failed(cause)` |
| Postcondition | SessionPhase = Disconnecting; `Effect::FailSync{failure, cause}` is emitted; no `Effect::Stream` is ever reached for this generation |
| Source | `frogdb-replication/src/session_machine.rs:424`, `:439` |
| Rulings | — |

---

## TR-REPLICATION-007 — a session ending classifies its departure before it leaves the registry

| Field | Value |
| --- | --- |
| Precondition | any SessionPhase; `SessionEvent::Ended(outcome)` |
| Postcondition | SessionPhase = Disconnecting (terminal); if the phase this session was on *before* the event was Streaming: LastStreamingDeparture is set (Graceful for `LinkOutcome::Ended(Graceful)`/orderly EOF/primary-initiated teardown/broadcaster closed, Lost for `LinkOutcome::Errored`/read-write errors/lag disconnect/`RecvError::Lagged`) via `Effect::RecordDeparture`, strictly before `Effect::Unregister` removes it from ReplicaRegistry; if the phase before the event was not Streaming, LastStreamingDeparture is left untouched (no record for a session that never streamed) |
| Source | `frogdb-replication/src/session_machine.rs:492` (`Ended` arm), `:601-610` (effect ordering: `RecordDeparture` before `Unregister`) |
| Rulings | — (already locked behavior, FM-REPLICATION-062) — this row transcribes the classification the FM row specifies; issue 23 changes what happens to LastStreamingDeparture on a subsequent *demotion*, not on this transition — see TR-REPLICATION-010 |
| Model | `replication_feed_gate.qnt::inv_departure_matches_its_cause`, `replication_feed_gate.qnt::inv_departure_record_backed_by_an_ended_session` |

---

## TR-REPLICATION-008 — a promotion mints a fresh primary identity and freezes the inherited one at the applied boundary (success path)

| Field | Value |
| --- | --- |
| Precondition | IsReplica = true; `begin_primary_stint()` called (from `RoleManager::promote`); `discard_staged_full_sync` succeeds |
| Postcondition | ClaimedOffset is frozen (ApplyGateFrozen = true) at the value `settle_at_applied_inner()` returns; ReplicationId = a freshly minted id; SecondaryId = the pre-promotion ReplicationId; SecondaryOffset = the frozen boundary; OffsetAtSave is raised (`max`) to the same frozen boundary by `plan_primary_stint` (`primary/promotion.rs:70`); ReplicationId/SecondaryId/SecondaryOffset/OffsetAtSave are persisted together to `replication_state.json` via `save_snapshot(&plan.minted)` (`primary/mod.rs:646-648`) before the caller observes success; BacklogStart is re-armed at the same boundary (`arm_backlog_floor`) |
| Source | `frogdb-replication/src/primary/mod.rs:449` (`begin_primary_stint_inner`), `:646` (`save_snapshot`); `frogdb-replication/src/primary/promotion.rs:37` (`StintPlan`), `:63` (`plan_primary_stint`), `:70` (OffsetAtSave raise); `frogdb-replication/src/state.rs:363` (`shift_replication_id`) |
| Rulings | — (already locked behavior, FM-REPLICATION-019) |

---

## TR-REPLICATION-009 — a promotion that cannot persist rolls the identity back but leaves the applied gate frozen (failure path)

| Field | Value |
| --- | --- |
| Precondition | Same as TR-REPLICATION-008 up to the point ClaimedOffset is frozen (ApplyGateFrozen = true); the subsequent `save_snapshot(&plan.minted)` fails |
| Postcondition | ReplicationId/SecondaryId/SecondaryOffset are restored bit-for-bit to their pre-promotion values (`plan.rollback`); ApplyGateFrozen remains true (the freeze is **not** rolled back); IsReplica is left unset by `RoleManager::promote` (an `Err` return leaves the flag as it was — still a replica); no metric or `INFO` field distinguishes this state from "never pointed at a primary" today |
| Source | `frogdb-replication/src/primary/mod.rs:449` (`begin_primary_stint_inner`, the `save_snapshot` error branch); `frogdb-server/crates/server/src/role_manager.rs:322` (`promote`, `# Failure`) |
| Rulings | [issue 16](../.scratch/replication-correctness/issues/open/16-failed-promotion-strands-the-applied-gate.md) — ruled "keep freeze, make it observable": this postcondition's freeze is the accepted, permanent behavior; what is missing is the observability half (a level-triggered `INFO`/metric field distinguishing this state) and the two witnesses the review addendum requires (a retried `begin_primary_stint` minting from the same boundary; a cluster-mode recovery integration test) |
| Pending | [issue 16](../.scratch/replication-correctness/issues/open/16-failed-promotion-strands-the-applied-gate.md) |

---

## TR-REPLICATION-010 — a demotion ends the primary stint in a fixed order

| Field | Value |
| --- | --- |
| Precondition | IsReplica set to true (Release) by the caller, *before* this runs; `end_primary_stint()` called (from `RoleManager::demote`) |
| Postcondition | RoleFenceCounter is bumped (`fence_role_change`, releasing any parked WAIT — see TR-REPLICATION-027); BacklogStart is reset to UNARMED (`reset_backlog`); ApplyGateStint is incremented **without** clearing ApplyGateFrozen (`retire_replica_applies`); every entry in ReplicaRegistry is disconnected (`disconnect_all_replicas`) |
| Source | `frogdb-replication/src/primary/mod.rs:517` (`end_primary_stint`) |
| Rulings | [issue 23](../.scratch/replication-correctness/issues/open/23-demotion-keeps-the-departure-it-disarmed.md) — ruled "clear with the latch": LastStreamingDeparture must also be reset to the `None`/unknown sentinel here (never a synthesized Graceful — the review addendum is explicit that writing Graceful would pre-disarm a re-promoted node's fence), alongside FenceArmed's `reset_arming()`. Neither `end_primary_stint` nor the surrounding `RoleManager::demote` clears LastStreamingDeparture today — the four steps above are the complete, unchanged postcondition; the fifth step this ruling adds is not yet in the code. |
| Pending | [issue 23](../.scratch/replication-correctness/issues/open/23-demotion-keeps-the-departure-it-disarmed.md) |

---

## TR-REPLICATION-011 — `RoleManager::promote` orders the mint before the role flip

| Field | Value |
| --- | --- |
| Precondition | `promote()` called |
| Postcondition | The stream is dropped, `stop_boot_replica()` stops the boot-spawned reconnect loop (a separate teardown that lives outside `stream`), and PrimaryTarget is cleared to `None` — **unconditionally and first**, before `begin_primary_stint()` is ever attempted, and even on the no-op arm where the node was already primary; these three steps are irreversible and happen regardless of what follows. Only if IsReplica was true does `begin_primary_stint()` then run (TR-REPLICATION-008/009). On `Ok`: IsReplica = false. On `Err`: IsReplica alone is left unchanged (still true) — leaving a node flagged as a replica with PrimaryTarget already cleared and no stream, i.e. no primary to point at and no link to one (the stranded state TR-REPLICATION-009/issue 16 describes). |
| Source | `frogdb-server/crates/server/src/role_manager.rs:322` (`promote`) |
| Rulings | — |

---

## TR-REPLICATION-012 — `RoleManager::demote` orders the role flip before the stint teardown

| Field | Value |
| --- | --- |
| Precondition | any role state; `demote(primary)` called. The no-op guard is three conjuncts: IsReplica already true, PrimaryTarget already `Some(primary)` (same target), and no sync refusal latched (`sync_refusal().is_none()`); only when all three hold is the call a no-op (the stint is not ended twice). A latched sync refusal deliberately makes a same-target re-`demote` re-run the full teardown below — it is the operator's only recovery path short of a restart. |
| Postcondition | IsReplica = true (written first, so every write-gate read after this point sees the new role before the stint teardown below completes); `end_primary_stint()` runs (TR-REPLICATION-010); the old `ReplicaStream` (if any) is dropped; `stop_boot_replica()` clears any pending boot-replica handler; FenceArmed is reset (`reset_arming`), when a self-fence checker is configured; a new replica stream toward `primary` starts; PrimaryTarget = Some(primary), written last |
| Source | `frogdb-server/crates/server/src/role_manager.rs:362` (`demote`) |
| Rulings | — |

---

## TR-REPLICATION-013 — a slot-handoff barrier arming or releasing publishes a new feed-gate deadline

| Field | Value |
| --- | --- |
| Precondition | a cluster-side barrier arms or releases, changing the set of armed deadlines `frogdb_core::PauseState::feed_hold_until` supplies (that set is not itself a tracked State-space variable here — it lives in `frogdb-core`, outside this crate's structs — but every entry in it feeds directly into FeedGateHeldUntil below) |
| Postcondition | FeedGateHeldUntil = `decide_feed_hold_until(armed_deadlines)` (the max of the armed set, or None if empty), published via `ReplicaFeedGate::publish`; any consumer awaiting `released()` or polling `is_held()` observes the new value |
| Source | `frogdb-replication/src/feed_gate.rs` (`decide_publish`, `decide_feed_hold_until`, `ReplicaFeedGate::publish`) |
| Rulings | [issue 26](../.scratch/replication-correctness/issues/open/26-the-feed-gate-model-proves-a-transcription-not-the-session.md) — the derivation this row states is correct and already witnessed (the stateright model catches a reintroduced defect here in under a second). What issue 26 found is a gap in what *consumes* this value: the streaming path in `replica_session.rs` can silently stop consulting the gate with no model or layer noticing except FM-CLUSTER-097's own integration test. Since `e31fee92` (issue 26 option 1) that path no longer carries the sequencing inline — `FeedSequencer::step` decides it and the driver performs the consultations it asks for: `FeedAction::AwaitRelease` → `feed_gate.released().await` (once before the backlog handoff tail, once in the live write task), `FeedAction::ReceiveOrRelease` → a `select!` racing `wal_rx.recv()` against `feed_gate.released()`, and `FeedAction::ConsultGate` → `feed_gate.is_held()` fed back as `FeedInput::GateHeld`. There is no `while feed_gate.is_held()` buffering loop any more; a bypass is now a change to those call sites rather than to a hand-written loop. Ruled fix (Option 2 now): a slot-handoff-barrier fault family in the replication seeded sweep, plus an interim seam lint requiring the streaming path to perform the gate consultations — neither is landed yet. |
| Pending | [issue 26](../.scratch/replication-correctness/issues/open/26-the-feed-gate-model-proves-a-transcription-not-the-session.md) |
| Model | `replication_feed_gate.qnt::inv_no_ship_inside_barrier_window` |

---

## TR-REPLICATION-014 — the frame consumer claims, applies and lands a replicated frame

| Field | Value |
| --- | --- |
| Precondition | SessionPhase = Streaming; ApplyGateFrozen = false; ApplyGateStint matches the consumer's own stint; Epoch matches the frame's epoch; DivergedEpoch is not the current Epoch; a frame is available |
| Postcondition | A frame whose whole span ClaimedOffset already covers (`frame.sequence <= ClaimedOffset`) never reaches the claim at all: it is skipped and counted, both offsets unchanged, the consumer running (FM-REPLICATION-065). Otherwise the claim decides. On `Claim::Granted`: ClaimedOffset advances by the frame's byte length; the shard apply runs; LandedOffset is raised to ClaimedOffset (`land()`). On `Claim::Stale` (epoch mismatch or diverged): the frame is dropped, ClaimedOffset/LandedOffset unchanged, the consumer keeps running. On `Claim::Retired` (ApplyGateFrozen or stint mismatch): the frame is dropped and the consumer stops. |
| Source | `frogdb-replication/src/replica/offset.rs:388` (`claim`), `:420` (`land`), `covers`/`record_skip` (the pre-claim skip); `frogdb-replication/src/apply.rs` (`consume_frames`, the one site that consults them) |
| Rulings | — |
| Model | `replication_fullsync.qnt::inv_offsets_ordered` |

---

## TR-REPLICATION-015 — a full resync installs an absolute offset pair, moving the landed offset backward

| Field | Value |
| --- | --- |
| Precondition | ApplyGateFrozen = false; ApplyGateStint matches the caller's stint; a full resync or checkpoint install has a new `(live, offset)` pair to adopt |
| Postcondition | LiveOffset = the installed value; ClaimedOffset = the installed value; LandedOffset = the installed value (the only transition in this table that can lower LandedOffset); Epoch is incremented; DivergedEpoch is cleared |
| Source | `frogdb-replication/src/replica/offset.rs:268` (`reset_pair`, private — reached only through the resync install path) |
| Rulings | [issue 17](../.scratch/replication-correctness/issues/open/17-save-point-above-the-live-head.md) — ruled "save point follows the history": `OffsetAtSave` (the *persisted* save point, a separate field from LandedOffset) must be allowed to follow a `reset_to`/staged-checkpoint install downward together with LiveOffset/ClaimedOffset/LandedOffset, rather than only ever rising. INV-OFFSET-2 becomes Hard-tier and scoped "monotone within a replication history" rather than globally monotone. Per the anti-pattern review (finding H5) and issue 24's amendment (point 3), that scoping needs `(ReplicationId, Epoch)` keying to be checkable, and issue 24 is ruled to land its identity/offset-pairing changes **before** this issue, since both relocate `OffsetAtSave` and neither issue originally cited the other. Neither ordering nor the OffsetAtSave-follows-history behavior is implemented yet. |
| Pending | [issue 17](../.scratch/replication-correctness/issues/open/17-save-point-above-the-live-head.md), [issue 24](../.scratch/replication-correctness/issues/open/24-a-restart-keeps-the-replication-id-it-lost-the-history-for.md) |
| Model | `replication_fullsync.qnt::inv_applied_covered_by_data` |

---

## TR-REPLICATION-016 — a promotion freezes the applied gate at the claimed offset, never the received offset

| Field | Value |
| --- | --- |
| Precondition | `settle_at_applied()`/`settle_at_applied_inner()` called (the first step of TR-REPLICATION-008/009, before the fallible persist) |
| Postcondition | ApplyGateFrozen = true; the returned boundary = ClaimedOffset at the instant of the call (never LiveOffset, which may be ahead of what the applier has actually claimed) |
| Source | `frogdb-replication/src/offset_coordinator.rs:205` (`settle_at_applied`), `:223` (`settle_at_applied_inner`) |
| Rulings | — (already locked behavior, FM-REPLICATION-019's Invariant cell) |
| Model | `replication_fullsync.qnt::inv_second_offset_not_above_live` |

---

## TR-REPLICATION-017 — a demotion retires the replica applier without unfreezing it

| Field | Value |
| --- | --- |
| Precondition | `retire_replica_applies()` called (part of TR-REPLICATION-010) |
| Postcondition | ApplyGateStint is incremented (so any consumer holding the previous stint's claims stops at its next frame); ApplyGateFrozen is left exactly as it was — this call never clears a freeze, only ever compounds one |
| Source | `frogdb-replication/src/replica/offset.rs:243` (`retire_replica_applies`); `frogdb-replication/src/offset_coordinator.rs:247` (the coordinator's thin wrapper) |
| Rulings | — |

---

## TR-REPLICATION-018 — an ACK is ingested and notifies WAIT waiters

| Field | Value |
| --- | --- |
| Precondition | a `REPLCONF ACK <offset>` arrives from a streaming replica; `offset > AckedOffset`'s current value for that session (record_ack is a no-op otherwise, though it still refreshes the session's liveness clock) |
| Postcondition | AckedOffset for that replica session = `offset`; WAIT waiters subscribed to the ack broadcast channel are notified |
| Source | `frogdb-replication/src/tracker.rs:612` (`record_ack`); `frogdb-replication/src/replica_session.rs:539` (`ReplicaSession::record_ack`); `frogdb-replication/src/offset_coordinator.rs:260` (`ingest_replica_ack`, the entry point) |
| Rulings | [issue 21](../.scratch/replication-correctness/issues/open/21-ack-above-live-head.md) — AMENDED ruling "ignore + count": today neither `ingest_replica_ack` nor `record_ack` checks the wire ack against LiveOffset at all (`record_ack` only checks monotonicity against the session's own prior AckedOffset), so a replica that acks past the primary's own live head is credited in full — inflating WAIT's count, reporting negative INFO lag, and shrinking the divergence window used elsewhere. The ruled postcondition adds a precondition clause: `offset <= LiveOffset` — an ack strictly above LiveOffset is **ignored entirely** (no AckedOffset write, the session's next valid ack still updates normally), counted via a metric, with no disconnect. The originally-ruled alternative (clamp AckedOffset to LiveOffset) was reversed by the anti-pattern review because it makes the primary a writer of AckedOffset, contradicting this same row's Source-level guarantee that only the wire ack writes it. Not implemented yet — the ceiling check itself is absent from both cited functions. |
| Pending | [issue 21](../.scratch/replication-correctness/issues/open/21-ack-above-live-head.md) |
| Model | `replication_feed_gate.qnt::inv_ack_never_above_live`, `replication_feed_gate.qnt::inv_ack_ignored_not_clamped`, `replication_fullsync.qnt::inv_ack_never_above_live_head` |

---

## TR-REPLICATION-019 — a granted `+CONTINUE` shifts the inherited id into the failover window, at a validated id

| Field | Value |
| --- | --- |
| Precondition | SessionPhase = Connecting; a partial resync is granted (TR-REPLICATION-001, PartialGrant arm); the wire supplies `granted_id` |
| Postcondition | if `granted_id` is well-formed (40 lowercase-hex chars): ReplicationId = `granted_id`; SecondaryId/SecondaryOffset unchanged by this call (this is the *replica*-side adoption of what the primary granted, not the primary-side promotion shift — TR-REPLICATION-008 is the mint side). If malformed: the call is rejected, ReplicationId is left untouched, the link is dropped rather than continuing under an unvalidated id. |
| Source | `frogdb-replication/src/replica/connection.rs:309` (`psync`, `state.shift_replication_id(granted_id.clone(), resumed_at)`); `frogdb-replication/src/state.rs:497` (`is_valid_replication_id`) |
| Rulings | [issue 18](../.scratch/replication-correctness/issues/open/18-unvalidated-wire-replication-id.md) — ruled "confirmed: validation at the single chokepoint" (already wired into `shift_replication_id`/`adopt_replication_history` as the validation point for all three wire-adoption call sites, including this one). Review addendum (finding A1): the guarantee is well-formedness only, not anti-impersonation — a malformed id cannot be adopted, but a well-formed id from an unauthenticated or unauthorized peer is not caught here; that depends on masterauth/TLS (and, in cluster mode, Raft-carried node identity). No FM row states this scope limit explicitly today. |
| Pending | — (the validation chokepoint itself is implemented; the addendum's explicit scope note in the FM catalog is not, but this task does not touch FM rows) |

---

## TR-REPLICATION-020 — a received full-sync payload's trailer is adopted at the same validated chokepoint

| Field | Value |
| --- | --- |
| Precondition | SessionPhase = Connecting; a full resync payload (live-dataset trailer or checkpoint trailer) has been received and its dataset installed; the trailer carries a `replication_id` |
| Postcondition | if well-formed: ReplicationId = the trailer's id; SecondaryId/SecondaryOffset are cleared (`clear_secondary_window`, folded into `adopt_replication_history`) — a full resync closes any prior failover window rather than merging with it. If malformed: rejected, prior ReplicationId/SecondaryId/SecondaryOffset untouched. |
| Source | `frogdb-replication/src/replica/connection.rs:388` (`receive_snapshot`, `.adopt_replication_history(metadata.replication_id.clone())`), `:462` (`receive_checkpoint`, same call); `frogdb-replication/src/state.rs:407` (`adopt_replication_history`), `:413` (`_inner`) |
| Rulings | same as TR-REPLICATION-019 (issue 18) |
| Model | `replication_fullsync.qnt::inv_failover_window_whole` |

---

## TR-REPLICATION-021 — identity recovery at boot is gated on an intact dataset recovery

| Field | Value |
| --- | --- |
| Precondition | node boot; `<data_dir>/replication_state.json` is read via `load_or_create` |
| Postcondition | Identity recovery is gated on dataset recovery. If no dataset was restored, or the restored dataset was not recovered *intact* (`keys_failed == 0` under FM-PERSISTENCE-033's continue policy, and `frogdb_wal_recovery_dropped_records_total == 0`): (a) ReplicationId = a freshly minted id; (b) simultaneously the recovered-but-untrusted id shifts into SecondaryId at boundary 0 (the Redis RDB-aux shape, both halves together). The persisted offset commits atomically with the write it names (not stamped at manifest time, which biases it low and makes replay non-idempotent for INCR/LPUSH/APPEND). Identity is written inside the FM-PERSISTENCE-019 quiesce window. **Gen-domination on promotion.** The `(history generation, offset)` pair is an operator-facing total order: a later observation is never lexicographically below an earlier one, across *any* step that leaves the node a primary — a promotion included, not exempted. Promotion rewinds the live offset to the applied offset (TR-REPLICATION-016/FM-REPLICATION-019), so what carries the pair forward is that the same step opens a **strictly later** history generation; a rewound offset under the *same* generation would regress the order and is forbidden. This is why the ordering is lexicographic on the pair rather than monotone on the offset alone. Scope: "generation" is the position of a history in this node's succession chain, not a comparable field on the wire — `ReplicationId` is 40 random hex characters (`state.rs`, `generate_replication_id`) with no order on its bytes. What the implementation provides is the chain itself: a promotion mints a fresh id and shifts its predecessor into `secondary_id`/`secondary_offset` in the same step (FM-REPLICATION-019), so successive generations are distinguishable and orderable *by that chain*. An operator comparing two `INFO replication` samples by id bytes alone is not reading the order this states; comparing `(master_replid, master_repl_offset)` against a remembered `(master_replid2, second_repl_offset)` is. — **Not implemented.** Today, `load_or_create` sets ReplicationId/SecondaryId/SecondaryOffset/OffsetAtSave to the file's contents (or mints fresh only if the file is missing/corrupt/fails `validate()`) **regardless of whether a dataset was recovered** — a node restarted with `persistence.enabled=false`, or one whose store is empty, still adopts the prior ReplicationId even though LiveOffset seeds at 0. `replication_state.json` also still stands as an independent identity authority rather than the ruled `database_id`-keyed cache/retirement. |
| Source | `frogdb-replication/src/state.rs:235` (`load_or_create`); `frogdb-replication/src/identity.rs:~90-98` (`adopting`, `live.fetch_max(state.offset_at_save)`) |
| Rulings | [issue 24](../.scratch/replication-correctness/issues/open/24-a-restart-keeps-the-replication-id-it-lost-the-history-for.md) — ruled (a) structural + AMENDED (b) as well as (a), after the anti-pattern review found (a) alone insufficient (R-C1 CRITICAL: an unclean restart *with* a dataset can still resume `(id, offset)` below what replicas already acked and re-issue those offsets under the same id, reachable into a `+CONTINUE` over divergent bytes). Cross-references (not yet folded into `specs/persistence.md` by this task): FM-PERSISTENCE-019 (quiesce window), FM-PERSISTENCE-027/-028/-033/-038/-039/-049. |
| Pending | [issue 24](../.scratch/replication-correctness/issues/open/24-a-restart-keeps-the-replication-id-it-lost-the-history-for.md) |
| Model | `replication_fullsync.qnt::inv_replid_offset_paired`, `replication_fullsync.qnt::inv_restart_pairs_offset`, `replication_fullsync.qnt::inv_identity_pair_monotone` |

---

## TR-REPLICATION-022 — a replica session registers against an announced identity

| Field | Value |
| --- | --- |
| Precondition | a session completes its handshake and announces `(addr, port)` |
| Postcondition | if an existing streaming session already carries the same node/replica identity (not IP:port — a shared/NAT'd address must not dedupe, and `listening_port == 0` stays exempt as unknown): the predecessor session is disconnected with a `Superseded` outcome — recording **no** departure at all (neither Graceful nor Lost) for the predecessor, subject to a cooldown to prevent a kick loop. The new session then proceeds to register normally. — **Not implemented.** Today, ReplicaRegistry gains a new entry unconditionally — id minted by `next_replica_id.fetch_add` — with no check against any existing entry announcing the same identity. |
| Source | `frogdb-replication/src/tracker.rs:173` (`register_replica`), `:183` (`register_announced_replica`) |
| Rulings | [issue 22](../.scratch/replication-correctness/issues/open/22-duplicate-streaming-identity.md) — AMENDED ruling. Original ruling ("kick predecessor", classified as Graceful) was corrected by the anti-pattern review (finding H2): a Graceful record for the kicked predecessor can land *after* the successor's own `ClearDeparture`, silently un-fencing the primary — exactly the shape FM-REPLICATION-062's NOT-observable clause forbids. The corrected ruling adds the `Superseded` outcome (no departure record at all) and changes the dedup key from (peer IP, announced port) to node/replica id + cooldown (finding A1: an IP:port key would kick-loop forever on a shared/NAT'd address). Not implemented — today's registration path has no dedup of any kind. |
| Pending | [issue 22](../.scratch/replication-correctness/issues/open/22-duplicate-streaming-identity.md) |
| Model | `replication_feed_gate.qnt::inv_superseded_writes_no_departure`, `replication_feed_gate.qnt::inv_departure_record_is_never_superseded`, `replication_feed_gate.qnt::inv_unannounced_is_never_superseded`, `replication_feed_gate.qnt::inv_duplicate_identity_is_always_kickable`, `replication_fullsync.qnt::inv_one_session_per_announced_identity`, `replication_fullsync.qnt::inv_superseded_records_no_departure` |

---

## TR-REPLICATION-023 — the self-fence latch arms

| Field | Value |
| --- | --- |
| Precondition | a replica session's own transition into SessionPhase = Streaming (registration path) |
| Postcondition | FenceArmed is set to true at that transition, independent of any write ever occurring. The write-path `arm_if_streaming` call (run from `has_quorum()` on every write command) stays in place as a belt-and-braces catch-up, now redundant with the new arming point rather than the sole one. — **Not implemented.** Today, arming happens exclusively from the write path: FenceArmed = true iff `tracker.has_streaming_replica()` is true at the moment a write calls `has_quorum()`. A replica that reaches Streaming but is lost before any write is served leaves FenceArmed = false forever (until some future write arms it), and the fence never engages for that loss. |
| Source | `frogdb-replication-runtime/src/quorum.rs` (`ReplicationQuorumChecker::arm_if_streaming`, `has_quorum`) |
| Rulings | [issue 19](../.scratch/replication-correctness/issues/open/19-self-fence-arms-only-on-the-write-path.md) — ruled "arm on Streaming transition". Not implemented — arming today happens exclusively through the write-path call cited above; nothing on the session-lifecycle path (session_machine.rs's transition into Streaming, TR-REPLICATION-002/005) touches FenceArmed. Review addendum (finding A6): FM-REPLICATION-041's existing Invariant cell already *claims* "armed is a latch set by any replica reaching Phase::Streaming" while the spec is LOCKED and mutation-gated — this row's Pending link is the tracking mechanism for that gap; the FM row's text itself is not altered by this task. |
| Pending | [issue 19](../.scratch/replication-correctness/issues/open/19-self-fence-arms-only-on-the-write-path.md) |

---

## TR-REPLICATION-024 — the self-fence latch clears

| Field | Value |
| --- | --- |
| Precondition | either: (a) `reset_arming()` called (demotion, TR-REPLICATION-012); or (b) `disarm_if_departed_cleanly()` runs and finds FenceArmed = true, no session in ReplicaRegistry with SessionPhase = Streaming, and LastStreamingDeparture = Graceful |
| Postcondition | FenceArmed = false |
| Source | `frogdb-replication-runtime/src/quorum.rs` (`reset_arming`, `disarm_if_departed_cleanly`); called from `frogdb-server/crates/server/src/role_manager.rs:362` (`demote`) for (a) |
| Rulings | — (already locked behavior, FM-REPLICATION-041's Invariant cell). Note: arm (b)'s read of LastStreamingDeparture is exactly what issue 23's TR-REPLICATION-010 amendment protects — if a re-promoted node's demotion had left LastStreamingDeparture at a stale Graceful (or, worse, a synthesized one), this clear could fire on the wrong generation's record. |

---

## TR-REPLICATION-025 — a replica-side read is served regardless of link health

| Field | Value |
| --- | --- |
| Precondition | a read command arrives at a replica; unconditional on this table's State-space variables — notably not gated on SessionPhase (the replica's own upstream session may be in any phase, including no session at all) or on ApplyGateFrozen (TR-REPLICATION-009's stranded state) |
| Postcondition | a `replica-serve-stale-data yes\|no` config knob governs the answer, with Redis semantics/spelling. When `no` and the replica's upstream link is down (including the stranded-promotion state TR-REPLICATION-009 describes): the replica answers errors to data commands (INFO/auth-class commands excepted). When `yes` (the default): the read is answered from the local keyspace unconditionally. Client-side detection is `master_link_status` in INFO. — **Not implemented.** FrogDB has no `replica-serve-stale-data` config gate at all today: every read is answered from the local keyspace unconditionally, matching Redis's `yes` default but with no `no` option. |
| Source | `frogdb-replication/src/replica/mod.rs` (the read path has no stale-gate check to cite — this is the absence the ruling fills) |
| Rulings | [issue 28](../.scratch/replication-correctness/issues/open/28-replica-serve-stale-data-knob.md) — raised by the anti-pattern review (finding A4) against FM-REPLICATION-029's existing "deliberately no stale-read gate" Invariant text and issue 16's ruled stranded-promotion state (TR-REPLICATION-009), which is an unbounded-staleness source with no knob to bound it. Not implemented — no such config key exists yet. |
| Pending | [issue 28](../.scratch/replication-correctness/issues/open/28-replica-serve-stale-data-knob.md) |

---

## TR-REPLICATION-026 — WAIT snapshots its target once and resolves via fast path, solicitation, or deadline

| Field | Value |
| --- | --- |
| Precondition | `WAIT numreplicas timeout` on a primary (IsReplica = false) |
| Postcondition | `target = LiveOffset` at arrival, snapshotted exactly once; if the count of ReplicaRegistry entries with SessionPhase = Streaming and AckedOffset >= target already meets `numreplicas`: returns immediately, no GETACK sent, no state changed. Otherwise: if any entry has SessionPhase = Streaming, exactly one GETACK round is solicited (TR-REPLICATION-018 is what subsequently advances AckedOffset past target for waiters); then the call blocks until quorum is reached (unblocking on the next AckedOffset write that satisfies target) or `timeout` elapses (or RoleFenceCounter changes — TR-REPLICATION-027) |
| Source | `frogdb-replication/src/wait_coordinator.rs` (`WaitCoordinator::wait_for_replicas`, `count_acked`); `frogdb-server/crates/server/src/connection/blocking.rs:254-282` (the connection-handler orchestration: the one-time `target` snapshot, the fast-path check, the `IsReplica` gate, and the CLIENT-UNBLOCK race — this is where the "snapshotted exactly once" claim is actually enforced, not solely in `wait_coordinator.rs`) |
| Rulings | — (already locked behavior, FM-REPLICATION-037/038/039) |

---

## TR-REPLICATION-027 — a role change parked under a WAIT releases it with an error, never a count

| Field | Value |
| --- | --- |
| Precondition | a `WAIT` (TR-REPLICATION-026) is blocked (fast path did not satisfy it) on a node that then runs `end_primary_stint()` (TR-REPLICATION-010), bumping RoleFenceCounter |
| Postcondition | the blocked WAIT resolves to `WaitVerdict::RoleChanged`, discarding whatever count had accumulated; the caller replies with the role-change error rather than an integer. An exact tie between quorum being reached and RoleFenceCounter changing on the same poll resolves to RoleChanged (the `select!` is `biased;` with the fence arm listed first, not left to an unseeded/non-reproducible-under-turmoil random tie-break). |
| Source | `frogdb-replication/src/wait_coordinator.rs` (`fence_role_change`, the `select!` arms producing `WaitVerdict::RoleChanged`); the counter itself is RoleFenceCounter (`tokio::sync::watch::Sender<u64>`) |
| Rulings | — (already locked behavior, FM-REPLICATION-040) |

---

## TR-REPLICATION-028 — a future quorum-durability write mode (roadmap)

| Field | Value |
| --- | --- |
| Precondition | undesigned |
| Postcondition | undesigned |
| Source | — (no code exists for this transition) |
| Rulings | [issue 29](../.scratch/replication-correctness/issues/open/29-replication-durability-quorum-write-mode.md) — filed as the roadmap path toward lossless *unplanned*-failover durability (a `replication-durability = quorum` mode, CRDB/etcd Raft-commit shaped), explicitly `needs-triage` and **not** ready-for-agent: quorum definition, WAIT interaction, timeout/degraded behavior, scope, and durability-sync interplay are all deferred to a future design round. This row exists only to mark the roadmap slot; it invents no mechanism the issue itself does not state. [cluster-correctness issue 26](../.scratch/cluster-correctness/issues/open/26-planned-failover-gets-a-drain-and-offset-parity-barrier.md) — a distinct issue from the replication-correctness issue 26 that TR-REPLICATION-013 cites — is the boundary-adjacent, already-ruled mechanism for the *planned*-failover path (issue 29 cross-references it explicitly, at lines 15/27/66 of its own body); this row is only the *unplanned* path issue 29 covers. |
| Pending | [issue 29](../.scratch/replication-correctness/issues/open/29-replication-durability-quorum-write-mode.md) |

---

## TR-REPLICATION-029 — a `CONFIG SET` updates the self-fence knobs directly, with no persistence step

| Field | Value |
| --- | --- |
| Precondition | `CONFIG SET self-fence-on-replica-loss <yes\|no>` or `CONFIG SET replica-freshness-timeout-ms <ms>` is accepted by `runtime_config.rs`'s validation (param registered at `params.rs:1174-1187`, TOML section `replication`, `name` field carrying no `replication-` prefix) |
| Postcondition | SelfFenceEnabled or FreshnessTimeoutMs (respectively) is set to the new value via `ReplicationQuorumChecker::set_self_fence_enabled`/`set_freshness_timeout_ms`; no persistence occurs as part of this call — `CONFIG SET` is atomics-only (see the State-space rows for the separate `CONFIG REWRITE` path that does persist). The same setter also runs once at boot, applying the value parsed from the static config (`runtime_config.rs:1135-1136`), before any `CONFIG SET` has run. |
| Source | `frogdb-server/crates/server/src/runtime_config.rs:1135-1136` (boot apply), `:3146` (self-fence `CONFIG SET` arm), `:3177` (freshness `CONFIG SET` arm); `frogdb-replication-runtime/src/quorum.rs:96` (`set_self_fence_enabled`), `:116` (`set_freshness_timeout_ms`) |
| Rulings | — |

---

## TR-REPLICATION-030 — the primary write path advances the live offset and appends to the backlog

| Field | Value |
| --- | --- |
| Precondition | a write command completes locally on the primary (IsReplica = false) and its RESP bytes are handed to the replication write path |
| Postcondition | LiveOffset advances by the frame's byte length (`OffsetCoordinator::advance`, Release-ordered `fetch_add`, live bumped before claimed within the same call); ClaimedOffset advances in lockstep via `advance_by`, called from inside `advance` itself — not a second, separate path, which is exactly why "live first, then applied" is a single ordering guarantee rather than two; LandedOffset is raised to the same value in the same `advance_by` call (`fetch_max`) — on the primary the two heads never separate, which is what lets a later demotion ACK from its true landed position (TR-REPLICATION-033); the frame is recorded into BacklogEntries (`record`/`push`), first arming BacklogStart at the entry's start offset if it was still UNARMED (`fetch_max`); if the push exceeds `max_entries`/`max_bytes`, the oldest entries are evicted in a loop, each eviction raising BacklogStart to just past the evicted entry (BacklogStart only ever rises here) |
| Source | `frogdb-replication/src/offset_coordinator.rs:108-121` (`advance`); `frogdb-replication/src/replica/offset.rs:128-134` (`advance_by`, the LandedOffset `fetch_max`); `frogdb-replication/src/primary/mod.rs:862`, `:883` (call sites); `frogdb-replication/src/primary/replay.rs:232` (`record`); `frogdb-replication/src/primary/ring_buffer.rs:204` (`push`), `:216` (`push_inner`, arm-on-first-push and the eviction loop) |
| Rulings | — |

---

## TR-REPLICATION-031 — `reconcile_for_persist` raises the save point toward the applied offset before it is written to disk

| Field | Value |
| --- | --- |
| Precondition | `save_state()` runs — the periodic snapshot hook (`server/init.rs:385-387`) or the shutdown hook (`server/subsystems.rs:806-814`), both `!is_replica`-guarded. Promotion reaches `ReplicationState::save` by a different route entirely (`save_snapshot`, TR-REPLICATION-008): `save_snapshot` is a sibling of `save_state`, not a caller of it, and never touches `reconcile_for_persist` — promotion's own OffsetAtSave raise is `plan_primary_stint`'s `max` (`primary/promotion.rs:70`), a distinct writer covered by TR-REPLICATION-008, not this row. |
| Postcondition | OffsetAtSave is raised via `max` to the caller's currently-applied offset (`reconcile_for_persist`, identical shape in both the primary and replica implementations), and the resulting `ReplicationState` is written to `replication_state.json` (`ReplicationState::save`: a fixed `.tmp` name — no pid suffix, unlike `config_persister.rs`'s `CONFIG REWRITE` path — then `fs::rename`, with **no fsync** anywhere in the call, so the write is not durable against a power loss and a concurrent save from two writers could collide on the same temp path). The raise is one-directional: a full-resync install that moves LandedOffset backward (TR-REPLICATION-015) does **not** lower OffsetAtSave to match — this is exactly the gap issue 17 rules against. |
| Source | `frogdb-replication/src/offset_coordinator.rs:306` (`OffsetCoordinator::reconcile_for_persist`); `frogdb-replication/src/replica/offset.rs:591` (`ReplicaOffset::reconcile_for_persist`); `frogdb-replication/src/state.rs:279` (`ReplicationState::save`); `frogdb-replication/src/primary/mod.rs:640` (primary `save_state`, the only side with a production caller); `frogdb-replication/src/replica/mod.rs:325` (replica `save_state`, no production caller today) |
| Rulings | [issue 17](../.scratch/replication-correctness/issues/open/17-save-point-above-the-live-head.md) — ruled "save point follows the history" (see TR-REPLICATION-015 for the fuller statement, including issue 24's ordering amendment). This row is the persist half of that gap: `reconcile_for_persist`'s raise-only `max` is precisely the behavior issue 17 rules must change to allow OffsetAtSave to follow a `reset_pair` install downward. Not implemented. |
| Pending | [issue 17](../.scratch/replication-correctness/issues/open/17-save-point-above-the-live-head.md) |

---

## TR-REPLICATION-032 — an unrecoverable apply failure latches the diverged epoch

| Field | Value |
| --- | --- |
| Precondition | ApplyGateStint matches the caller's own stint; a frame apply (or claim) fails in a way the applier cannot recover from at the current Epoch |
| Postcondition | if Epoch is still the epoch the divergence was detected under: DivergedEpoch is latched to that Epoch, and waiters on the divergence notifier are woken (`notify_waiters()`) — this is what fires the streaming loop's `applied.divergence()` `select!` arm and abandons the link, rather than the divergence being discovered only on the next inter-frame check. Every subsequent `claim` at the same Epoch then resolves `Claim::Stale` (TR-REPLICATION-014). A divergence report against an already-superseded Epoch is a no-op; `reset_pair`'s Epoch bump (TR-REPLICATION-015) is what clears DivergedEpoch on the next full-resync install. |
| Source | `frogdb-replication/src/replica/offset.rs:444` (`admit_divergence`), `:452` (`notify_waiters`); `frogdb-replication/src/apply.rs:439`, `:614` (the two production callers that decide when a divergence is admitted); `frogdb-replication/src/replica/streaming.rs:129` (the `select!` arm it wakes) |
| Rulings | — |

---

## TR-REPLICATION-033 — a replica ACKs the landed offset, never the claimed or received one

| Field | Value |
| --- | --- |
| Precondition | a replica's streaming loop is running (`stream_replication`); either the spontaneous ack-interval ticks, or a solicited-ack target is pending (a `GETACK` was seen — consumed by TR-REPLICATION-026's WAIT solicitation) |
| Postcondition | the value sent as `REPLCONF ACK <offset>` is LandedOffset (`applied.landed()`) at the moment of sending, in both arms — never ClaimedOffset and never LiveOffset. A solicited ack additionally waits until LandedOffset reaches the solicited target before sending (`wait_until_applied`) rather than sending immediately at whatever LandedOffset already holds. This is the wire counterpart of AckedOffset's own row: the acked value is always a durability claim about applied data, never about bytes merely received or claimed. |
| Source | `frogdb-replication/src/replica/streaming.rs:139-142` (spontaneous tick, `applied.landed()`), `:135` (solicited send), `:36` (`solicited_ack`/`wait_until_applied`), `:247` (`send_ack`) |
| Rulings | — |
| Model | `replication_feed_gate.qnt::inv_ack_never_above_the_wire` |

---

## TR-REPLICATION-034 — a partial-resync accept truncates everything above the offset it claimed

| Field | Value |
| --- | --- |
| Precondition | SessionPhase = Connecting; the replica is granted `+CONTINUE` (TR-REPLICATION-001, PartialGrant arm) and holds keyspace state produced by writes *above* the offset it claimed. Reached whenever an earlier full sync overshipped — `snapshot_offset` is captured before the cut, so the payload may cover more than the granted offset (FM-REPLICATION-004) — and the link then breaks before the handoff replay has reconciled that tail. Sharpened when the grant comes through the failover window (TR-REPLICATION-019, `granted_id` present): the history that produced the tail has been replaced, so nothing in the granting history need ever write those positions again. |
| Postcondition | no replica ever holds an unaccounted effect above the offset a window grant resumes from. The intent — "discard the forked tail" — is met by **prevent-plus-refuse** rather than by in-place truncation, in two halves. *Prevent*: the tail an overshipped full sync creates is described exactly, per shard, by the payload's coverage vector, and the replica skips the frames it already holds instead of re-executing them (FM-REPLICATION-066), so the handoff replay leaves no forked position behind. *Refuse*: while any part of that tail is still unreached (`ClaimedOffset < max(Y_s)`), or the offsets came back from a restart with nothing describing what sits above them, a `+CONTINUE` carrying `granted_id` is refused — the replica rewinds to 0 and takes a full resync, which replaces the keyspace and the residue with it. The claimed prefix is never discarded: that is what a grant asserts agreement on, and a **bare** `+CONTINUE` (same history) is never refused, because the stream about to arrive is the one that wrote the tail. Both consequences the truncation was wanted for follow: the granting history's frames never land on a position the replaced history wrote, and no forked position survives a grant that could reach it. In-place truncation is deliberately *not* implemented — a replica cannot invert a verbatim `INCR`/`LPUSH`, so "discard above the claim" is only realizable as "never create it, or replace the whole dataset". |
| Source | `frogdb-replication/src/replica/psync.rs` (`window_grant_verdict`, the pure decision); `frogdb-replication/src/replica/connection.rs` (`psync`, `PsyncArm::Continue` arm — the refusal rewinds to 0 and drops the link before `shift_replication_id`); `frogdb-replication/src/replica/offset.rs` (`AppliedOffset::floor_ceiling`, `provenance`); `frogdb-replication/src/apply.rs` (`consume_frames`, the floor skip that prevents the tail) |
| Rulings | R5 of the quint-completeness campaign's 2026-08-20 post-execution rulings (`.scratch/formal-spec/2026-08-19-quint-completeness-campaign.md`) — ruled a new replica-side rule so the residue class is removed by construction rather than argued about case by case. The follow-up choice that ruling left open was settled 2026-08-21 (ledger R12–R16): the "truncate" intent is implemented as prevent-plus-refuse — per-shard coverage watermarks in the full-sync trailer become receiver-side skip floors, and a window grant while `applied < max(Y_s)` is refused (full resync wipes the residue). The same investigation found the healthy handoff already re-executes the overshipped range (no break needed) — tracked with this row's fix as replication-correctness issue 35; the restart-flavored sibling (offset stamped low against RocksDB contents) is issue 36. |
| Pending | — the residue class is closed by FM-REPLICATION-066, whose `Forced by` tests force both halves of this postcondition. What remains is the restart-flavored sibling: a node whose offsets came back from the persisted state refuses window grants *unconditionally* (ruling R16), because the pair "offset, what sits above it" is not recovered atomically. Pairing them — and with it the same-history restart bias — is issue 36. |
| Model | `replication_fullsync.qnt::inv_no_forked_tail_above_the_claim` |

---

## How to read a row

| Field | Meaning |
|---|---|
| Trigger | The concrete precondition or interleaving that puts the link in this mode. |
| Observable | What a client of the replica sees: its keyspace, its `INFO replication`, the outcome of the sync. |
| NOT observable | What must never appear in this mode. This is the half mutation testing attacks. |
| Invariant | The internal guarantee, named at the mechanism that provides it. |
| Catalog | *Optional.* The invariant-catalog entries that check this row's guarantee **universally** — see below. Absent on rows nothing in the catalog generalizes. |
| Outcome variant | The enum variant / metric label the mode reports through, or `n/a` for wire-level invariants with no client-visible outcome. |
| Forced by | The test(s) that fail if the behavior changes. Every one carries a `// FM-REPLICATION-NNN` tag at its definition site; `just lint-spec` enforces both directions. |
| Bug refs | Known issues that touch this mode. |

Test names are bare function names, resolved against the crate list in
`scripts/spec-lint.py` (`NEXTEST_CRATES`).

### The `Catalog` field

A row states its guarantee for *one* transition; the invariant catalog
(`frogdb-server/crates/replication/src/invariants.rs`) states a well-formedness claim about the
`ReplicationView` projection (`replication/src/view.rs`) for *every* state, and
`debug_assert_view_clean` evaluates the HARD tier at each seam that produces one. Every test that
drives such a seam is therefore also an invariant test, at no authoring cost — which is the point:
a row is forced by the tests it names, an entry is forced by every test in the crate.

A row carries a `Catalog` field when an entry generalizes the guarantee the row states point-wise:
deleting the code the row names would make that entry fire. The cross-reference runs both ways —
each entry's `check_*` function names the rows it generalizes — and `just lint-spec`
checks that every `INV-*` id this spec cites is defined in the **replication** catalog. The check
is per-area (`INVARIANT_CATALOGS`), so citing one of the cluster catalog's entries here is an error
rather than a pass — an invariant is a claim about one area's state projection, and a claim about
the slot map says nothing about this one. (Naming a cluster id in this paragraph would itself fail
the lint, which is how the rule was checked.)

Two entries are deliberately not cited by any row above:

* `INV-GATE-1` (the replica feed gate's held answer, its remaining hold, and the barrier budget)
  generalizes a *cluster* row — FM-CLUSTER-097, the node-wide feed hold that closes the slot-handoff
  window — because the gate lives in this crate but the transition that drives it does not. The
  per-area rule that makes a cross-area citation an error is what keeps that from being written
  down as `Catalog` on FM-CLUSTER-097 instead; the citation therefore lives at the code, in
  `inv_gate_1`'s doc comment.
* `INV-ROLE-1` is cited by FM-REPLICATION-022 as a documented *non*-guarantee, not a guarantee:
  chained replication is neither supported nor refused (issue 48), which is what its
  `DocumentedException` tier records.

---

## FM-REPLICATION-001 — a granted full resync always carries the primary's dataset

| Field | Value |
|---|---|
| Trigger | A replica sends `PSYNC ? -1` (first attach, or a reconnect whose offset the primary cannot serve from the backlog) and the primary grants `+FULLRESYNC`. The mode that matters is the primary running with `persistence.enabled = false`: there is no RocksDB to checkpoint, which is the configuration the turmoil sims and every embedded/cache deployment use. Sharpened by a replica that already holds a *divergent* keyspace — its own forked keys, or stale values for keys the primary has since changed. |
| Observable | The replica's keyspace after the sync is exactly the primary's: the primary's keys and values, its TTLs, and **none** of the replica's own. `DBSIZE` matches, keys that exist only on the replica are gone, and post-sync writes stream on top. On the wire the payload is a `$FROGDB_SNAPSHOT` envelope — one `shard-<n>.dataset` blob per primary shard, then the `FullSyncMetadata` trailer — the same shape as the `$FROGDB_CHECKPOINT` envelope, differing only in the marker and in what the blobs contain. |
| NOT observable | **A replica reporting `master_link_status:up` with a stale keyspace** — the whole bug (issue 67): the old minimal-RDB branch sent an empty envelope carrying no dataset, and the replica adopted the new replid/offset, flipped to `Streaming`, and kept serving its pre-sync keys forever. Nor any of its near misses: a payload marker other than `FROGDB_CHECKPOINT`/`FROGDB_SNAPSHOT` being accepted; a corrupted or truncated dataset being installed; a dataset arriving with no installer wired and the sync still reporting success; the granted *identity* — replid or offset — being adopted before the dataset is installed, which would leave a node that never received the payload advertising a history it cannot serve and having discarded the failover window (`secondary_id`/`secondary_offset`) describing the keyspace it is still holding. |
| Invariant | The primary has exactly two honest payloads and no third: with RocksDB it checkpoints, without it `stream_live_dataset` serializes the *live* keyspace (Redis' `repl-diskless-sync` parity — "no persistence configured" never means "no dataset on the wire"). Both are refused rather than faked when they cannot be produced: a failed checkpoint cut errors the sync, and an unwired `live_snapshot_source` errors it too, because dropping the connection costs one reconnect backoff while a data-less payload is silently permanent. The replica enforces the same rule from its side — `select_full_resync_payload` (`replica/psync.rs`, the pure decision `ReplicaConnection::psync` calls before reading the count line) rejects any marker it cannot install, so an old primary's minimal RDB fails the sync instead of being mistaken for one. Ordering: on `+FULLRESYNC` `psync` adopts *neither* half of the granted pair — it rewinds the offset to 0 and leaves the replication id and failover window alone, so a promise that goes unkept costs nothing; the id is taken from the payload's own trailer, `install_payload` runs before either is adopted, and an install failure rewinds to 0 again — so every failure lands on "ask for a full resync again", never on "stream deltas onto a keyspace that never took the base snapshot". The trailer's combined checksum is folded blob-by-blob in wire order under positional names (`shard-<n>.dataset`), so a reordered, dropped, truncated or corrupted blob fails verification before it reaches the installer. |
| Catalog | `INV-OFFSET-4`, `INV-REPLID-1` — adopting neither half of the granted pair leaves the failover window whole and standing over the keyspace this node still holds; the offset entry skips a live head of 0 for exactly the in-flight resync this row describes. |
| Outcome variant | `SyncType::{FullSyncCheckpoint, FullSyncSnapshot}`; `INFO replication` `master_link_status` |
| Forced by | `run_full_sync_without_rocks_streams_the_live_dataset`, `full_sync_without_a_live_snapshot_source_fails_the_sync`, `receive_snapshot_installs_the_dataset_before_adopting_offset`, `receive_snapshot_without_an_installer_fails_the_sync`, `receive_snapshot_rejects_a_corrupted_dataset`, `psync_rejects_a_payload_that_carries_no_dataset`, `a_full_resync_grant_carries_the_id_and_the_offset`, `a_full_resync_grant_that_is_not_a_pair_is_rejected`, `the_envelope_names_a_checkpoint_a_dataset_or_nothing_installable`, `a_full_sync_that_never_delivers_a_dataset_leaves_the_old_history_alone`, `a_checkpoint_that_dies_mid_transfer_leaves_the_old_history_alone`, `test_full_resync_from_a_persistence_disabled_primary_transfers_the_dataset`, `every_shard_contributes_its_blob_in_shard_order`, `a_shard_that_cannot_export_fails_the_whole_sync`, `a_shard_that_vanishes_mid_export_fails_the_sync`, `a_full_resync_drains_before_it_cuts`, `a_full_resync_without_a_drain_hook_cuts_straight_away`, `a_failed_drain_abandons_the_sync_and_stages_nothing`, `a_failed_cut_owns_no_checkpoint_directory`, `a_cut_that_succeeded_is_owned_before_it_is_shipped`, `a_primary_without_rocksdb_exports_the_live_keyspace_instead`, `a_primary_with_no_dataset_source_at_all_abandons_the_sync`, `a_session_that_staged_no_checkpoint_cleans_no_directory` |
| Bug refs | `.scratch/testing-improvements/issues/67` (fixed — this row is its outcome); `.scratch/testing-improvements/issues/61` (the live-install seam this reuses); `.scratch/testing-improvements-round2/issues/51` (the granted identity adopted before the payload — fixed) |
| Model | `replication_fullsync.qnt::inv_reapply_is_a_noop` |

Deliberate non-guarantees, so a future reader does not mistake them for gaps:

* **The live dataset is read shard-by-shard, not from one instant.** Each shard serializes its own
  keyspace when its `ExportSnapshot` message reaches it, so the blobs are not a single atomic cut
  across shards. This does not lose writes: `snapshot_offset` is captured *before* the export
  (FM-REPLICATION-004), so anything written during the export is either already in a blob or is
  replayed from the backlog at the streaming handoff. Cross-shard *visibility* during the export is
  the same granularity every other cross-shard operation has.
* **The shipped offset is durable only on the replica's next state save.** A crash between install
  and that save leaves the state file naming the previous offset; the replica reconnects from there
  and is replayed the range above it. Same window the checkpoint path has between install and
  metadata consumption (FM-PERSISTENCE-039). What bounds the damage is that **replay is
  offset-addressed**: the replica's claim is the address, `extract_backlog` returns only
  `offset > claimed` (`primary/ring_buffer.rs`), and `FeedSequencer::buffer` drops any live frame at
  or below the same `resume_offset` (`feed_sequencer.rs`), so nothing the claim already covers is
  re-delivered at all (FM-REPLICATION-012, FM-REPLICATION-015). That sender-side skip is no longer
  the only thing standing between a mis-computed resume and a double-applied write: the replica
  enforces the same rule from its own side, against the head it actually holds rather than the
  offset it once named, and ignores any frame whose whole span that head already covers
  (FM-REPLICATION-065). The re-delivered tail is therefore exactly the range
  between the claim and what the keyspace holds, and that range is closed from the other side by
  issue-24's amendment point 2 (TR-REPLICATION-021): the persisted offset commits atomically with
  the write it names, so the claim does not sit below the keyspace. Re-execution of that range is
  not a no-op in general — propagation is verbatim (`core/src/shard/post_execution.rs`,
  `replication_forms`), which is why the offset must name the write rather than be stamped behind
  it.
* **A live-dataset sync is not resumable.** There is nothing staged on disk, so a connection that
  dies mid-payload starts over. The checkpoint path is no different in practice — the staged dir is
  discarded unless it committed.

## FM-REPLICATION-002 — a shard exports its live keyspace, and an install replaces it wholesale

| Field | Value |
|---|---|
| Trigger | `ReplicationMsg::ExportSnapshot` on a shard serving a live-dataset full resync, and `ReplicationMsg::InstallSnapshot` on the receiving side — including the edge shapes: an empty shard, a shard holding logically-expired-but-not-yet-reaped keys, and an install whose routed slice for some shard is empty. |
| Observable | The blob round-trips: installing a shard's export into another shard reproduces its keys, values and TTLs. An empty shard exports an empty blob. Installing into a shard clears whatever it held first, so keys absent from the payload are gone; the `WATCH` version bumps so in-flight watchers abort. |
| NOT observable | Expired keys resurfacing on the replica as live data — a logically-dead key that the reaper has not reached is not part of the keyspace and must not be shipped. A partial export presented as complete: a value that has been spilled out of memory has nowhere to be read back from on a node with no RocksDB, so it errors the export rather than being skipped. An install that merges into the existing keyspace instead of replacing it (which would leave the replica's forked keys behind — the FM-REPLICATION-001 bug arriving by another route). A no-op install that still bumps the watch version. |
| Invariant | `export_snapshot` walks `all_keys()` and drops anything whose metadata is missing or `is_expired()`, and returns `Err` — never a short blob — when a key's value is not hot. `install_snapshot` clears then restores inside the shard's own task with no `.await` between, so the swap is atomic *per shard*: a client sees the whole old keyspace or the whole new one for that shard, never a mixture. The clear routes through the canonical write-effect pipeline as a synthetic `FLUSHDB`, inheriting its WATCH bump, tracking invalidation and WAL range tombstone while staying silent on keyspace notifications (adopting a dataset is not a stream of user writes). A dirty delta of `-1` is reserved for the genuinely empty install so it does not bump watchers. |
| Outcome variant | `ReplicationMsg::{ExportSnapshot, InstallSnapshot}` |
| Forced by | `export_snapshot_round_trips_through_install`, `export_of_an_empty_shard_is_an_empty_blob`, `export_snapshot_drops_expired_keys`, `install_snapshot_replaces_the_live_keyspace`, `install_snapshot_bumps_the_watch_version`, `install_empty_snapshot_clears_the_shard`, `install_empty_snapshot_into_empty_shard_is_a_no_op` |
| Bug refs | `.scratch/testing-improvements/issues/67`, `.scratch/testing-improvements/issues/61` |

## FM-REPLICATION-003 — a dataset blob decodes to exactly what was encoded, or fails whole

| Field | Value |
|---|---|
| Trigger | Decoding a `shard-<n>.dataset` blob on the receiving node: a well-formed blob, an empty one, a blob truncated by a short read, and a blob whose entry payload is corrupt but whose length prefixes still parse. |
| Observable | Round-trip equality for keys, values and expiry, including a zero-length key. An empty blob decodes to no entries — the legitimate encoding of an empty shard, not an error. Anything malformed is a `SerializationError`. |
| NOT observable | A truncated blob decoding to the entries that happened to fit — a short read would silently drop the tail of a shard's keyspace, and the replica would install a subset while reporting a completed sync. A corrupt entry being skipped so the rest of the blob still installs: partial success here is indistinguishable from data loss. |
| Invariant | The framing is `[key_len u32 LE][key][entry_len u32 LE][entry]` repeated to the end of the blob, with no entry count to disagree with the bytes — the blob's own length is the terminator, which is why an empty blob is unambiguous. `take_chunk` treats a length prefix that overruns the remaining bytes as `Truncated`, and `read_entries` propagates the first error rather than accumulating what parsed, so a blob is all-or-nothing. Verification runs earlier still: the envelope's combined checksum (FM-REPLICATION-001) rejects a damaged blob before decoding is attempted, making this layer the second line rather than the only one. |
| Outcome variant | `SerializationError::{Truncated, …}` |
| Forced by | `blob_round_trips_keys_values_and_expiry`, `empty_blob_decodes_to_no_entries`, `truncated_blob_is_an_error`, `corrupt_entry_payload_fails_the_blob` |
| Bug refs | `.scratch/testing-improvements/issues/67` |

## FM-REPLICATION-004 — the granted offset never runs ahead of the payload

| Field | Value |
|---|---|
| Trigger | Writes landing on the primary while a full sync is being prepared and streamed — the window between granting `+FULLRESYNC <replid> <offset>` and the replica switching to the live tail. |
| Observable | The replica ends the sync holding the payload's dataset *and* every write made during the handoff: the writes in `(snapshot_offset, current]` are replayed from the backlog before the live tail. |
| NOT observable | An acknowledged write that is in neither the payload nor the replayed range — the shape that appears when the offset is captured *after* the cut (`offset > data`), so the replica believes it holds writes that were never shipped and no reconnect will ever ask for them again. |
| Invariant | `snapshot_offset` is read from the tracker **before** the checkpoint is cut or the live dataset exported, and the same value is used for both the FULLRESYNC reply and the trailer, so the safe direction is structural: writes landing after the capture only *add* data, giving `offset <= data`. That inequality is one-sided by design, and its slack is the overship: the payload holds writes the granted offset does not name. The trailer's per-shard coverage vector is this row's **exact-pairing companion** — where `snapshot_offset` is the lower bound on what the payload holds, `Y_s` is the exact upper bound for each shard, so the pair brackets the payload instead of merely bounding it below and the replay of `(snapshot_offset, current]` becomes dedupable (FM-REPLICATION-066). The vector does not weaken this row: the offset stays the prefix bound, the replay still covers every handoff write, and the no-loss direction is unchanged. On the checkpoint path the pre-checkpoint hook drains the shard WALs into RocksDB first, so an acknowledged-but-unflushed write cannot be missed; the live-dataset path needs no drain because the shards *are* the source it reads. `start_streaming` subscribes to the broadcast before replaying `(snapshot_offset, current]`, so the replayed range and the live tail cannot leave a gap between them. Redis parity: the FULLRESYNC offset is the `master_repl_offset` captured at fork time. |
| Outcome variant | n/a (wire-level invariant; surfaces as a missing write on the replica) |
| Forced by | `full_sync_replays_writes_made_during_handoff`, `the_granted_offset_is_the_one_the_payload_and_the_replay_both_use` |
| Bug refs | `.scratch/replication-cluster-rework/issues` (the WAL-drain half — a full-resync checkpoint cut before the shard WALs were drained lost acknowledged writes) |
| Model | `replication_fullsync.qnt::inv_applied_covered_by_data`, `replication_fullsync.qnt::inv_payload_covers_grant`, `replication_fullsync.qnt::inv_coverage_brackets_the_payload` |

## FM-REPLICATION-005 — the receive→stream handoff loses no byte of the live tail

| Field | Value |
|---|---|
| Trigger | The primary streaming live WAL frames while the replica is still reading the full-sync payload — the normal case, because `start_streaming` begins the moment the payload is written, so the trailer and the first frames routinely arrive in the same TCP segment. Sharpened by load: the slower the transfer (a large checkpoint, a contended host), the more frames are already queued behind the trailer. Both payload shapes are exposed, checkpoint and live dataset. |
| Observable | Streaming resumes on the byte after the trailer: every frame the primary sent during the transfer is decoded, applied and ACKed, so the replica's offset converges on the primary's and `WAIT` is satisfiable without a reconnect. A sync whose payload was followed by nothing on the wire starts streaming from an empty decoder. |
| NOT observable | **A replica whose offset is permanently short of the primary's stream position while it reports `master_link_status:up`** — the whole bug (issue 01): the frames that shared the trailer's read were dropped with the payload reader, so they were never decoded, never applied and never ACKed. Nor its consequences: `WAIT` structurally unsatisfiable against a replica the primary considers online; a fixed byte-count divergence that only a reconnect (which re-syncs from the replica's short offset) can heal; frames re-decoded or double-counted because the residual was handed over more than once. |
| Invariant | The payload paths never wrap the socket themselves: [`PayloadReader`] owns the `BufReader`, and its `Drop` moves whatever the buffering read past the payload into the connection's `pending_stream_bytes` — on every exit path, including the `?` returns of a failed sync. `stream_replication` *seeds* its decode buffer with `take_pending_stream_bytes()` instead of starting empty, and drains the seeded bytes **before** its first socket read, because they can already hold whole frames the primary is waiting to see ACKed. The hand-back is take-once (`mem::take`), so a residual cannot be replayed. A third payload shape inherits all of this by construction: `payload_reader()` is the only sanctioned way to buffer this socket (`stream` says so at its declaration), and it carries the hand-back with it. |
| Outcome variant | n/a (wire-level invariant; surfaces as a stalled replica offset and an unsatisfiable `WAIT`) |
| Forced by | `receive_checkpoint_streams_the_frames_that_trailed_the_payload`, `receive_snapshot_streams_the_frames_that_trailed_the_payload`, `a_payload_with_no_trailing_frames_leaves_the_stream_empty`, `dropping_the_reader_hands_back_what_it_read_past_the_payload`, `a_fully_consumed_reader_leaves_no_residual` |
| Bug refs | `.scratch/hardening/issues` (issue 01 — surfaced as a load-dependent `test_broadcast_lag_disconnect_and_resync` flake; the seed write was acked by neither replica because the ACK never arrived) |
| Model | `replication_fullsync.qnt::inv_splice_continuity` |

[`PayloadReader`]: ../frogdb-server/crates/replication/src/replica/payload_reader.rs

## FM-REPLICATION-006 — owing an ACK never stops the replica reading its link

| Field | Value |
|---|---|
| Trigger | A `REPLCONF GETACK` (what `WAIT` sends) reaching a replica whose applier is behind the received head — frames still queued on the 10k frame channel, a shard slow to apply, or a consumer that has stopped for good because a promotion froze its stint. Sharpened by the primary continuing to stream behind the GETACK, which is the normal case: a solicitation is not a barrier, and the frames after it are already in flight. |
| Observable | The frames that arrive behind the GETACK are decoded and queued while the answer is still owed, with no wait for the applier: the received head keeps advancing at socket speed. The spontaneous ACK tick keeps firing on cadence throughout, so a primary counting ACKs keeps seeing this replica's true applied head. The solicited ACK goes out the moment the applier reaches the solicited offset — which covers the GETACK frame itself, as in Redis. |
| NOT observable | **A replica that stops reading its socket because it owes an ACK.** Its consequences: TCP backpressure to the primary, which stalls the shared broadcast and slows *other* replicas' streams; the spontaneous ACK cadence skipping a tick, so the link goes quiet exactly when `WAIT` made it interesting; the solicited answer itself arriving a whole cadence late because the loop that must notice the applier caught up was the loop that was blocked. Nor the opposite escape: an ACK reporting the *received* head to answer without waiting (FM-REPLICATION-008's rule — an ACK is a durability claim). |
| Invariant | The wait is a `select!` branch, not an inline `await`. `drain_frames` only *records* the solicitation (`pending_ack: Option<u64>`, the offset it covers) and returns to the loop, so the socket-read branch and the ACK-tick branch stay pollable for exactly as long as the answer is owed. A GETACK arriving while one is already pending raises the target to the newer, higher offset rather than queueing a second answer: ACKs are cumulative, so one ACK at the newer target answers both. An unanswerable solicitation (the applier stopped for good — a frozen gate, a retired stint) needs no timeout to stay harmless: it parks one branch of the loop and nothing else, while the spontaneous cadence keeps reporting the same applied head the timeout would have made it report. The loop's `select!` is `biased;` (determinism audit R7/A54), fixed order: divergence latch, then the solicited ACK, then the spontaneous tick, then the socket read — so a seeded run replays identically instead of depending on tokio's random tie-break, and an exact tie between the solicited answer becoming computable and a divergence landing in the same instant resolves to divergence, never to sending one more ACK over a link about to be abandoned (FM-REPLICATION-010's ordering, not this row's). None of the four arms but the socket read can be perpetually ready, so biasing costs no arm a fair share of the loop. |
| Outcome variant | n/a (wire-level invariant; surfaces as a stalled replica read and a hiccuping ACK cadence) |
| Forced by | `a_solicited_ack_does_not_stall_the_decode_loop`, `a_solicited_ack_is_sent_as_soon_as_the_applier_catches_up`, `a_second_getack_raises_the_target_to_the_newer_offset`, `the_ack_cadence_survives_a_solicitation_that_can_never_be_answered`, `wait_until_applied_returns_as_soon_as_the_applier_catches_up`, `wait_until_applied_parks_when_the_applier_can_no_longer_advance`, `a_tie_between_divergence_and_a_solicited_ack_favors_divergence` |
| Bug refs | `.scratch/replication-cluster-rework/issues` (issue 09) |

---

## FM-REPLICATION-007 — frames outlive their connection, never their history

| Field | Value |
|---|---|
| Trigger | A replica link that drops mid-stream, possibly mid-`MULTI`, with frames already decoded onto the 10k-deep frame channel — which, with its consumer, outlives the connection (`ReplicaReplicationHandler::start` reconnects in a loop into the same channel). The retry is then granted `+FULLRESYNC`: a dataset is installed and both heads are reset to the payload's offset. The leftovers now describe a keyspace that no longer exists. |
| Observable | The queued frames from the replaced history are dropped without reaching a shard and without claiming a byte, counted on the consumer's `discarded` shutdown field; an open group among them is abandoned with them. The applied head after the resync is the payload's offset plus exactly the new history's frames. A `+CONTINUE` resume is the opposite case and is left alone: it installs no dataset and resets no head, so its leftovers — including a `MULTI` group split across the reconnect — still apply, and the group closes normally on the `EXEC` that arrives after it. |
| NOT observable | **A frame from a replaced history applied to the installed dataset**, or its bytes credited to the new history's offset (which would make this node ACK — and, once promoted, vouch for — an offset covering data it never held, the FM-REPLICATION-004 hazard arriving from the replica side). **A `MULTI` group straddling a resync**: an old history's half-transaction continued by the new history's commands, or closed by an `EXEC` from the other side of the install, applied on the *old* group's tagged shard. Nor the blunt fixes for either: retiring the stint on resync (which stops the long-lived consumer for good, so the replica silently applies nothing after its first reconnect) or draining/rebuilding the channel per connection. |
| Invariant | Every frame the decode loop queues is stamped with the **history epoch** it was decoded under (`StreamedFrame`), and the epoch is bumped by `reset_pair` only — i.e. exactly when a full resync adopts a new dataset — under the same gate lock that moves the heads. The consumer checks it twice: a cheap pre-check at the top of the loop that drops a stale frame and the group it belonged to, and the authoritative re-check inside `ReplicaApplyStint::claim`, taken under that same lock, which is what makes the check race-free (`Claim::Stale`) against a resync landing on the connection task mid-group. A group is additionally abandoned when the frame in hand is current but the group was opened under an older epoch. Because both the decode loop and the reset run on the connection task, a channel never interleaves epochs: all of one history's frames precede all of the next's. |
| Outcome variant | n/a (internal invariant; surfaces as a diverged replica keyspace and an over-claimed applied offset) |
| Forced by | `a_full_resync_discards_the_frames_queued_from_the_previous_history`, `a_multi_group_left_open_by_a_dropped_link_is_never_closed_by_the_next_history`, `a_continue_resume_still_applies_the_frames_it_left_queued`, `a_claim_stamped_before_a_resync_is_refused_after_it` |
| Bug refs | `.scratch/replication-cluster-rework/issues` (issue 06) |

---

## FM-REPLICATION-008 — an ACK is a durability claim, not a receipt

| Field | Value |
|---|---|
| Trigger | `WAIT N t` on the primary, which sends `REPLCONF GETACK` and counts the ACKs that come back, against a replica whose applier is behind its socket: frames queued on the 10k-deep frame channel, or one group in flight between the applier's claim and the shard's reply. Sharpened by killing the replica the instant `WAIT` returns, which is the whole point of the primitive. |
| Observable | An acked offset implies **every frame at or below it has been applied to its shard**. The replica ACKs its *landed* head — moved as each `apply_group` returns, and immediately for frames that reach no shard (`REPLCONF`, `FROGDB.FINALIZE`, an unparseable payload) — on both branches: the spontaneous cadence tick and the solicited answer. A group in flight to a shard is inside the promotion boundary (claimed) and outside the ACK (not landed), which is the one-group gap between the two heads. A full resync levels all three heads at the adopted offset: the installed dataset is applied. |
| NOT observable | **`WAIT` satisfied by a replica that has not applied the write** — neither one still holding it in the frame channel (the received head, which a promotion discards down to the applied offset) nor one that has merely claimed it (the claimed head, which is what the promotion boundary needs and is a byte count, not an apply). A `WAIT 1` that returns 1 and is then followed by killing and restarting that replica must not lose the key. Nor the reverse escape: a landed head that runs ahead of the claimed head, or that keeps reporting a previous history's offset after a resync adopted a lower one. |
| Invariant | Three heads, one direction: `landed <= claimed <= received`. `claimed` is `AppliedOffset::current` — taken before the group is dispatched, under the gate, which is what makes the promotion boundary exact (FM-REPLICATION-007's claim path). `landed` is `AppliedOffset::landed`, advanced by `ReplicaApplyStint::land` at every point where nothing is in flight; because the consume loop applies one group at a time and awaits each, "nothing in flight" means `landed` may simply be re-read from `claimed` (a `fetch_max`, so a resync that stored a lower offset in between wins). Only `landed` is read by `send_ack` and by `wait_until_applied`, the two ACK branches. The primary's own writes (`advance_by`) move both, because that path applies first and counts after. |
| Catalog | `INV-OFFSET-1` — the three heads never invert, checked wherever the pair moves rather than only on the two ACK branches this row names. |
| Outcome variant | n/a (wire-level invariant; surfaces as `WAIT` returning a count that overstates durability) |
| Forced by | `an_ack_reports_the_landed_head_not_the_claimed_one`, `a_claim_alone_does_not_move_the_offset_the_replica_acks`, `a_group_in_flight_to_its_shard_is_claimed_but_not_yet_ackable`, `a_frame_that_touches_no_shard_lands_as_it_is_claimed`, `a_full_resync_levels_the_landed_head_with_the_adopted_offset`, `test_spop_replication_convergence_random_workload` |
| Bug refs | `.scratch/testing-improvements-round2/issues` (issue 76) |
| Model | `replication_fullsync.qnt::inv_offsets_ordered` |

**Latency.** Moving the ACK behind the apply does not add a round trip, but it does mean a `WAIT`
is answered on the *applier's* schedule rather than the decoder's: the solicited answer is parked as
a `select!` branch and fires the moment the landed head reaches the solicited offset
(FM-REPLICATION-006), while the fallback is the spontaneous cadence
(`replication.ack-interval-ms`, Redis `repl-ping-replica-period`). A replica whose applier is
wedged therefore keeps ACKing its true, lower landed head on cadence instead of a higher receipt —
`WAIT` times out rather than lying. This is the Redis behaviour and the reason its replicas ack
after executing the command stream.

**Not covered here.** ACK means applied, not *persisted*: `ReplicationState::save()` is
`write`+`rename` with no fsync, so a power cut can still lose a landed offset that was acked. That
is a separate contract (issue 76 item 2) and is not claimed by this row.

---

## FM-REPLICATION-009 — the backlog outlives its last replica by a bounded time

| Field | Value |
|---|---|
| Trigger | A primary whose backlog window is armed — by boot recovery at a non-zero offset, or by `begin_primary_stint` at a promotion boundary — and whose replica count is zero and stays zero: a standalone node restarted after it once had a replica, or a primary whose only replica went away for good. Every write is still stamped and buffered, for resume history nobody is waiting for. |
| Observable | After `repl-backlog-ttl` seconds with zero connected replicas the buffer is emptied and the window closed, so the next `PSYNC` is answered `+FULLRESYNC` (`FullResyncReason::BacklogEvicted`). A replica that reconnects *before* the TTL elapses still gets its `+CONTINUE`, and a replica connected the whole time never lets the clock start — a reconnect restarts the window rather than resuming it. `repl-backlog-ttl 0` parks the timer entirely (Redis's disable value), and the knob is live: a `CONFIG SET` retunes an idle window that is already running. |
| NOT observable | **A `+CONTINUE` over history that was freed** — the floor is disarmed in the same call that empties the buffer, so no resume can be granted over the hole. **Any offset or identity movement**: freeing the backlog is not a stint change, so `master_replid`, `master_replid2`, `second_repl_offset`, `master_repl_offset` and the applied head are all exactly what they were; `INFO` reports the same history, only without a resume window. Nor the timer firing repeatedly (once per tick for the rest of the process) once the window has already been freed, nor starting at all on a node that has no window armed. |
| Invariant | The TTL is an idle clock, not a countdown from arming: `BacklogTtl::due` clears its start whenever the replica count is non-zero or the TTL is `0`, sets it on the first tick that finds zero replicas, and fires — clearing itself in the same breath — on the first tick at or past the deadline. `PartialSyncReplay::expire_backlog_if_idle` refuses to start the clock while nothing is armed and, when the clock fires, calls exactly `reset_backlog()`: the ring buffer's `reset` empties the entries and returns `start` to `UNARMED`, which is the same disarm both ends of a primary stint use. The tick itself is the server's 1 Hz maintenance task (Redis frees the backlog from `replicationCron` on the same cadence and for the same reason). |
| Catalog | `INV-BACKLOG-1` — the floor is disarmed in the same call that empties the buffer, and the entry fires on any floor left standing over history that was freed. |
| Outcome variant | `FullResyncReason::BacklogEvicted` on the next `PSYNC` after a free |
| Forced by | `an_idle_backlog_is_freed_once_its_ttl_elapses`, `a_replica_reconnecting_before_the_ttl_still_resumes`, `a_connected_replica_never_starts_the_ttl_clock`, `a_ttl_of_zero_never_frees_the_backlog`, `a_freed_backlog_full_resyncs_the_next_psync`, `freeing_the_backlog_moves_no_offset_and_no_replication_id`, `an_unarmed_backlog_never_starts_the_ttl_clock`, `the_ttl_fires_once_per_idle_window_not_once_per_tick` |
| Bug refs | `.scratch/replication-cluster-rework/issues` (issue 07) |

---

## FM-REPLICATION-010 — an admitted divergence ends the history it happened on

| Field | Value |
|---|---|
| Trigger | `apply_group` returning `Err` on a replica: a command the primary accepted that this node's shard refuses (a version skew, a shard-side resource failure, a bug), for a bare command or for a `MULTI/EXEC` group. The claim is already taken and cannot be given back without desynchronising every later frame's stream position, so the node now holds a keyspace that does not match the offset it counts. |
| Observable | The failure is **latched on the history it happened on**: every further `claim` on that epoch returns `Claim::Stale`, so later frames are dropped without reaching a shard and the applied head stops moving. `land()` is never reached either, so the ACK keeps reporting the last truly-applied offset and `WAIT` times out rather than lying (FM-REPLICATION-008). The connection task wakes on the latch, logs at `error`, rewinds the received head to 0 and drops the link with an `Err`, so the reconnect asks `PSYNC ? -1` and is answered `+FULLRESYNC` — on the exponential-backoff path, not a 100 ms hot loop. A divergence outstanding when a link is *established* abandons that link before it decodes a byte. Installing the resync payload (`reset_pair`) clears the latch in the same critical section that bumps the epoch, and the applier resumes on the new history. |
| NOT observable | **A diverged replica quietly continuing to apply** the rest of the stream onto a keyspace it has already broken, or ACKing offsets it never applied, or being handed `+CONTINUE` over the hole after a promotion. **Retiring the long-lived frame consumer** — FM-REPLICATION-007 forbids it: the consumer outlives connections, so retiring it makes the replica silently apply nothing for the rest of the process. Nor the reverse escapes: a latch surviving the full resync that replaced the keyspace (which would force a second, pointless resync), a latch admitted against an epoch a resync has *already* replaced, or an unparseable payload being treated as a divergence (it reaches no shard, breaks no keyspace, and is counted and skipped as before). |
| Invariant | The latch is an epoch-keyed cell on `AppliedOffset` (`diverged: AtomicU64`, sentinel `u64::MAX`) plus a `Notify`. `ReplicaApplyStint::admit_divergence(epoch)` takes the gate, compares `epoch` to the live epoch and stores it only if they match — so a resync that landed between the claim and the `Err` wins and the doomed history is simply forgotten. `claim` checks the cell under the same gate, immediately after its epoch check, and returns `Claim::Stale` (not `Retired`: the consumer must stay alive for the next history). `reset_pair` stores the sentinel back under that gate, so the latch clears if and only if a fresh dataset was installed — it survives reconnects, `+CONTINUE`s and promotion/demotion round trips. `AppliedOffset::divergence()` is a re-checking `Notify` wait, safe against a latch that fires before the waiter parks, consumed as a `select!` branch in `stream_replication` and re-checked once before the pre-loop drain. That `select!` is `biased;` with the divergence branch listed first of its four arms (determinism audit R7/A54): a divergence that lands in the same instant as a now-answerable solicited ACK, or a fresh read, wins the tie and ends the link — the newly-diverged history never gets one more frame decoded onto it or one more ACK sent over it. |
| Outcome variant | n/a (internal invariant; surfaces as a dropped link, a forced `+FULLRESYNC`, and a stalled ACK head) |
| Forced by | `a_failed_apply_stops_the_history_it_happened_on`, `a_diverged_applier_resumes_on_the_history_a_resync_installs`, `a_diverged_history_is_refused_until_a_resync_replaces_it`, `a_divergence_on_a_history_a_resync_already_replaced_is_ignored`, `the_divergence_wait_resolves_however_it_races_the_latch`, `an_admitted_divergence_drops_the_link_and_rewinds_for_a_full_resync`, `a_divergence_outstanding_at_connect_abandons_the_new_link_at_once`, `a_tie_between_divergence_and_a_solicited_ack_favors_divergence` |
| Bug refs | `.scratch/replication-cluster-rework/issues` (issue 08) |

**Reads during the window.** A diverged replica keeps serving reads until the resync payload lands.
This matches Redis's `replica-serve-stale-data yes` default, and the window here is much tighter than
Redis's: the link is dropped the instant the latch is seen, so the node is already in a forced full
resync rather than waiting out a link timeout. Refusing reads would need a `-MASTERDOWN`-style gate
on the read path and a knob to go with it; that is deliberately not in this row.

**Not covered here.** The remaining over-claim window is a **crash between a claim and its shard
write**, which leaves the persisted offset one group ahead of the data with no `Err` to latch. This
row only covers failures the applier is alive to see. Closing the crash window means persisting the
applied offset from the shard's own write path, and it stays tracked in
`.scratch/replication-cluster-rework/issues` (issue 08 follow-up).

---

## FM-REPLICATION-011 — the link carries every command the connection layer accepted

| Field | Value |
|---|---|
| Trigger | A write whose replicated encoding is larger than one internal frame's old ceiling: a `SET` of a bulk value between 64 MB and 512 MB — inside the limit FrogDB advertises to clients (`proto-max-bulk-len`) and therefore committed on the primary before any replication code sees it. The link is healthy; the only thing wrong is the size. |
| Observable | The write replicates: the replica serves the whole value (not a prefix), `WAIT 1` acks it, and both nodes still report the link — `master_link_status:up` on the replica, `connected_slaves:1` on the primary — after the frame has crossed. |
| NOT observable | **A committed write that cannot be replicated** — the whole bug (issue 69): `encode` cast the payload length with an unchecked `as u32` while `MAX_FRAME_SIZE` (64 MB) was checked on **decode only**, so the primary emitted a frame its replica refused, the link dropped, the backlog re-sent the identical frame on reconnect, and it never recovered. Nor its near misses: a payload silently **truncated** by the length cast and installed as a short value; a frame that encodes but cannot decode (the two ceilings disagreeing in either direction); a ceiling raised past `u32::MAX`, which would make the length field itself lossy and the decode-side sanity check meaningless. |
| Invariant | One derivation, not three coincidences. `frogdb_protocol::PROTO_MAX_BULK_LEN` is the ceiling on user data; `MAX_INTERNAL_FRAME_LEN = 2 * PROTO_MAX_BULK_LEN` is what every internal transport must carry (one accepted command: a maximal bulk plus its command name, key and framing, with one further maximal bulk of allowance — 1 GiB, the number Redis caps a client's accumulated request at with `PROTO_MAX_QUERYBUF_LEN`). Both `frogdb_replication::frame::MAX_FRAME_SIZE` and `frogdb_cluster::network::MAX_FRAME_SIZE` are *defined as* that constant rather than copied, and the cluster bus is in scope because it carries user bytes too (`BusRpc::PubSubBroadcast` / `PubSubForward`). Enforcement is symmetric: `ReplicationFrame::encode` and the tokio `Encoder` both refuse over-sized payloads with `FrameEncodeError::PayloadTooLarge` **before** reserving or casting, so the bound is checked on the side that can still report it, and the decoder's check becomes a sanity check on a peer rather than the only guard. A frame that cannot be encoded fails the replica link loudly (logged at `error`, link dropped) instead of going out short. |
| Outcome variant | `FrameEncodeError::PayloadTooLarge` (surfaced as `io::ErrorKind::InvalidInput`) |
| Forced by | `the_frame_ceiling_is_derived_from_the_resp_bulk_ceiling`, `encode_refuses_a_payload_larger_than_the_frame_ceiling`, `a_payload_over_the_old_ceiling_round_trips_across_the_link`, `a_value_over_the_old_frame_ceiling_replicates_without_wedging_the_link` |
| Bug refs | `.scratch/testing-improvements-round2/issues` (issue 69 — this row is its outcome) |

Deliberate non-guarantees:

* **A replicated encoding above 1 GiB still fails, but loudly.** The ceiling bounds one *frame*, not
  one user value, so a command whose replicated form exceeds it — an `MSET` of several maximal
  bulks, or a synthesized effect such as a `SORT ... STORE` result — is refused at encode instead of
  being truncated. That is a link failure, not silent divergence, and it is the deliberate trade:
  fragmenting frames would put reassembly state on both sides of a path whose whole job is to be
  hard to desynchronise. Redis has the same shape of limit (`PROTO_MAX_QUERYBUF_LEN`) and the same
  answer to exceeding it — kill the connection.
* **The boundary is asserted arithmetically, not by allocating it.** `payload_fits` is checked at
  `0`, `MAX - 1`, `MAX` and `MAX + 1`, and a real encode→decode round trip is done at 64 MiB + 1 —
  above the *old* ceiling, which is the regression that matters. Round-tripping a literal 1 GiB
  payload would allocate several GiB in a test process for no additional coverage of the branch.
* **The connection layer is not tightened to match.** Issue 69's third option — refuse at the client
  so the write is never accepted — was rejected: it would drop FrogDB below the `proto-max-bulk-len`
  it advertises, and Redis parity on what a client may store is the stronger contract. The internal
  transports move up to meet it instead.

---

## FM-REPLICATION-012 — a replay is contiguous from the resume point or it does not happen

| Field | Value |
|---|---|
| Trigger | The backlog window closing between a resume being *granted* and the same resume being *streamed*. The two are far apart: `+CONTINUE` (or `+FULLRESYNC <offset>`) is written first, and for a full sync the entire checkpoint cut and file transfer sits in between — on a busy primary with the default 10 000-entry backlog that is routinely long enough to evict the whole window. Also reached by the backlog TTL firing (FM-REPLICATION-009) or a stint boundary resetting the buffer inside that gap. |
| Observable | The resume is abandoned, not shortened: the link drops, and the replica's reconnect is answered `+FULLRESYNC` (`FullResyncReason::BacklogEvicted`) because the same floor check fails at grant time too. A full sync whose window closed mid-transfer therefore costs a second full sync rather than a silently incomplete keyspace. |
| NOT observable | **A replica streaming from a hole** — the whole bug (issue 52): the eviction check ran only at grant time, `extract_backlog` returned a shorter vector, the session seeded `resume_offset` from the last frame it *did* send, and the live tail deduped against that — so the replica was permanently missing the evicted range while its offset looked contiguous, `master_link_status` stayed `up`, and `WAIT` converged. Nor the near misses: a truncated range reported as an empty tail (indistinguishable from a caught-up replica); a `+CONTINUE` granted from the window as it was *before* the extraction; the ordering invariant being the only thing asserted about the returned tail. |
| Invariant | The window check lives on the extraction, not just on the grant. `ReplicationRingBuffer::extract_backlog` re-reads the floor **under the entries lock** — the same lock `push` holds while it evicts and raises that floor — so the window it checks is the window whose contents it returns, and it answers `Err(BacklogTruncated { requested, floor })` rather than a short `Vec`. Both callers must then decide rather than assume: `handle_partial_sync_request` degrades a grant to `FullResyncReason::BacklogEvicted`, and `ReplicaSession::start_streaming` fails the link with `io::ErrorKind::InvalidData` before writing a single replayed frame. Two ranges need no history and are served regardless: an empty range (`start >= end` — a caught-up replica, and the fresh-primary full sync whose snapshot offset is the head), and a backlog disabled by config, where every reconnect already full-resyncs. |
| Catalog | `INV-BACKLOG-1`, `INV-BACKLOG-2` — the retained entries cover their range without a hole and a granted `+CONTINUE` never resumes below the floor, checked after every push and eviction rather than only at the extraction this row guards. |
| Outcome variant | `BacklogTruncated`; `FullResyncReason::BacklogEvicted` on the next `PSYNC` |
| Forced by | `an_evicted_resume_point_is_refused_not_truncated`, `a_closed_window_refuses_every_extraction`, `a_resume_evicted_after_the_grant_is_abandoned_not_truncated`, `a_full_sync_whose_handoff_window_is_evicted_abandons_the_link` |
| Bug refs | `.scratch/testing-improvements-round2/issues` (issue 52 — this row is its outcome) |

**Not covered here.** This row makes a truncated replay *fail*; it does not make the window wider.
A primary whose backlog cannot span its own checkpoint transfer will now full-resync twice instead
of once, and the fix for that is capacity (`repl-backlog-size`), not the streamer. Nor does the row
claim byte-level contiguity *within* the retained range: the floor is the authority, because
eviction raises it to exactly the end offset of the entry it dropped, so "floor `<=` resume point"
is equivalent to "no entry covering the replayed range was dropped".

---

## FM-REPLICATION-013 — every PSYNC resolves to exactly one arm, and the fallback reason is classified honestly

| Field | Value |
|---|---|
| Trigger | Any `PSYNC <replid> <offset>` reaching a primary. Five distinguishable inputs: the `?`/`-1` sentinel of a first attach; a replid the primary has never issued and does not hold as its failover window; an offset ahead of the primary's own head (a replica that saw writes this primary lost — the post-failover rejoin); a request inside the window but below the retained floor; and a primary whose backlog is switched off entirely. Plus the malformed cases: wrong arity, an unparseable offset. |
| Observable | Exactly one of two first-line replies, always, on every input: `+CONTINUE <replid>` or `+FULLRESYNC <replid> <offset>` — and a malformed request gets an error reply and no stream, not a half-opened session. `PSYNC ? -1` always full-resyncs. An unknown replid always full-resyncs *even when the offset would fit the window numerically* — the arms are checked replid-first, so a replica that guesses an in-range offset under someone else's history never gets a tail. An offset ahead of the head always full-resyncs rather than being served an empty or clamped tail. The `+FULLRESYNC` reply always carries the primary's *current* replid and an offset the payload covers (FM-REPLICATION-004), and the `+CONTINUE` reply carries the primary's live replid, not the one the replica asked with — so a sibling continued through the failover window learns the new history's id from the grant itself. |
| NOT observable | **A `+CONTINUE` granted on a history this primary does not head.** The two shapes that would produce it: `window_contains` degraded to an offset-only comparison (so `PSYNC <any-id> <in-range>` is continued), and the `ReplidMismatch`/`OffsetAhead` arms collapsed into one another so an ahead-of-head request is treated as an unknown-history full resync — same wire reply today, but the reason is what the operator log and any future `sync_partial_err` breakdown report, and conflating them hides a real split-brain from the operator. Also never observable: a request the primary neither continues nor full-resyncs (a hang, a dropped connection with no reply, or `+OK`); a negative offset other than `-1` being read as a huge unsigned offset instead of clamping to 0; a malformed `PSYNC` leaving a registered replica behind. |
| Invariant | `PartialSyncReplay::can_replay` is a single ordered decision with no fallthrough: `enabled` → `Disabled`; literal `"?"` → `InitialSync`; `ReplicationState::window_contains` (`state.rs:381`, replid-equality *and* `requested_offset <= current_offset`, or the secondary window) → on failure re-inspect the id to split `ReplidMismatch` from `OffsetAhead`; then the floor check. The secondary-window arm of `window_contains` — the promotion-minted `secondary_id`/`secondary_offset` pair and its inclusive boundary — is specced by FM-REPLICATION-019; this row owns only the fact that the arm is consulted after the primary's own id and before the floor. The caller `handle_psync` (`primary/mod.rs:518`) reads `offsets.current()` once, up front, so the upper bound the decision uses is the same head the `+FULLRESYNC` offset is minted from, and `offset.max(0) as u64` clamps every negative to 0 before the u64 domain is entered. `ReplayDecision` is a two-variant enum, so "neither arm" is not representable; `PsyncHandoff::from_args` rejects bad arity/offset before the handler is ever reached. The replica reads the same two arms and no third through `select_psync_arm` (`replica/psync.rs`), the pure twin of this decision: `PsyncArm` is likewise two variants, a `-…` line is surfaced as the primary's own text and anything else is `InvalidData`, so `ReplicaConnection::psync` selects nothing itself. |
| Catalog | `INV-REPLID-1`, `INV-REPLID-3` — the two malformed-window shapes that would make `window_contains` grant a `+CONTINUE` over a history this primary does not head: a half-cleared window, and an id that claims the same history twice. |
| Outcome variant | `ReplayDecision::{Continue, FullResync}`; `FullResyncReason::{Disabled, InitialSync, ReplidMismatch, OffsetAhead, BacklogEvicted}` |
| Forced by | `initial_sync_sentinel_falls_back_to_full`, `unknown_replid_falls_back_to_full`, `offset_ahead_falls_back_to_full`, `disabled_backlog_always_full_resyncs`, `window_fit_continues_with_ordered_tail`, `partial_falls_back_to_full_when_backlog_disabled`, `fullresync_offset_and_metadata_come_from_live_tracker`, `test_partial_sync_falls_back_to_full`, `test_partial_resync_unknown_replid_falls_back_to_full`, `test_psync_initial_request`, `test_psync_invalid_args`, `a_continue_carries_an_optional_id`, `a_refusal_is_passed_through_verbatim`, `anything_that_is_not_a_psync_answer_is_rejected`, `promotion_model_full_two_primary` |
| Bug refs | `.scratch/testing-improvements/issues/34` (the loose `FULLRESYNC-or-OK` assertions that would have masked a bogus grant — fixed; this row is the tightened contract) |
| Model | `replication_fullsync.qnt::inv_partial_grant_sound`, `replication_fullsync.qnt::inv_replids_distinct` |

Deliberate non-guarantees, so a future reader does not mistake them for gaps:

* **The reason is not on the wire, but the fork is now counted.** `FullResyncReason` still reaches
  only a `tracing::info!` at `primary/mod.rs:575` — a replica is never told *why* it was refused.
  The three-way split Redis reports through `sync_full` / `sync_partial_ok` / `sync_partial_err`
  does exist now (FM-REPLICATION-050), and deliberately does not key on `FullResyncReason`:
  `can_replay` returns `Disabled` before it inspects the `?` sentinel, so classifying on the reason
  would charge a first attach against a disabled backlog to `sync_partial_err`. The classification
  keys on the requested replid instead, which is what Redis's `master_replid[0] != '?'` test does.
* **`PSYNC ? <n>` for any `n` is a first attach.** The sentinel is the id, not the offset, matching
  Redis's `master_replid[0] == '?'` check; an offset sent alongside `?` is ignored, not validated.

---

## FM-REPLICATION-014 — the armed floor is the only lower bound, so a `+CONTINUE` is never served over a hole

| Field | Value |
|---|---|
| Trigger | A reconnecting replica presents an in-window offset whose commands the backlog may no longer hold: it fell behind past the retention caps and its bytes were evicted; or the primary armed its window at a non-zero recovered offset after a restart and has buffered nothing since; or the buffer was reset at a stint boundary and the replica's offset predates the re-arming. Applies identically to a request admitted through the primary's own replid and to one admitted through the post-promotion failover window. |
| Observable | The replica is given a clean `+FULLRESYNC` and a dataset, not a `+CONTINUE` followed by a tail that starts somewhere after where it asked. A replica sitting exactly at the retained floor — one command behind the oldest buffered entry — *is* continued: the boundary is inclusive on the resumable side, so the common "reconnected one write behind" case does not pay for a full sync. A replica that is fully caught up (`req == current`) is continued with an empty tail and then follows the live stream. A primary that has restarted at a non-zero offset and buffered nothing full-resyncs every replica until it has produced history of its own; it never claims to be able to replay the range it recovered but never held. |
| NOT observable | **A truncated `+CONTINUE`:** the replica adopting `+CONTINUE`, receiving frames that begin above its offset, and applying them onto a keyspace missing every command in the gap — a permanently divergent replica reporting `master_link_status:up`. The mutations that produce it: `>=` weakened to `>` (or the reverse) at the floor comparison, so the exact-floor replica is either refused or a below-floor replica is granted; the `None` (unarmed) branch answering `Ok` instead of `BacklogEvicted`, which is the restart case; eviction dropping bytes without raising the floor; the floor check skipped for requests admitted through the failover window, which would let a promoted node grant a sibling a tail it no longer retains; and the tempting-but-wrong shortcut "the buffer is empty, so grant when `req == current`", which is only sound at offset 0 and is refused by name in the code. |
| Invariant | The lower bound is `ReplicationRingBuffer::start` (`ring_buffer.rs:56`, Redis `repl_backlog_off`), an `AtomicI64` with an `UNARMED = -1` sentinel — never `oldest_offset()`, which is the *end* offset of the oldest entry and sits one entry too high. It is only ever raised: `arm_start` and every eviction use `fetch_max`, so it cannot be walked backwards by a race between a stint arming it and a concurrent `push`. `can_replay` consults it **last and unconditionally**, after both arms of `window_contains` have been tried, so passing the upper-bound window (own replid or secondary) is necessary but never sufficient; `None` (unarmed) and "below the floor" reach the same `BacklogEvicted` refusal, so "no claimed history" and "history I no longer hold" are not distinguished. A first push into an unarmed buffer opens the window at that command's *start* (`offset - len`, saturating) so the pushed entry is itself replayable. `reset()` takes the entries lock for the whole clear-and-disarm, so no push can interleave an entry into a window that has just been closed. |
| Catalog | `INV-BACKLOG-1`, `INV-BACKLOG-2` — the floor sits inside the oldest retained entry and no grant is served from below it, in every state rather than on the `can_replay` path alone. |
| Outcome variant | `FullResyncReason::BacklogEvicted` |
| Forced by | `request_below_armed_floor_falls_back_to_full`, `unarmed_backlog_at_nonzero_offset_falls_back_to_full`, `evicted_offset_falls_back_to_full_not_truncated`, `boundary_req_equals_oldest_continues`, `boundary_req_equals_current_grants_empty_tail`, `secondary_id_within_window_continues`, `ring_buffer_push_into_an_unarmed_buffer_opens_the_window_at_the_entry_start`, `ring_buffer_reset_closes_the_window_and_lets_the_floor_move_down`, `partial_falls_back_to_full_when_offset_evicted` |
| Bug refs | — |

Deliberate non-guarantees:

* **The floor is a byte position, not a command boundary the replica can verify.** Nothing on the
  wire lets a replica detect a truncated tail on its own; the guarantee is entirely the primary's.
  That is why the check is refusal-biased: an unnecessary full resync costs a checkpoint, a wrongly
  granted `+CONTINUE` costs silent divergence.
* **This row is the grant-time half only.** It pins the floor a `PSYNC` is judged against;
  [FM-REPLICATION-012](#fm-replication-012--a-replay-is-contiguous-from-the-resume-point-or-it-does-not-happen)
  pins the *extraction*-time re-check that catches a window closing between the grant and the
  stream. Both are needed: passing this row's floor and then losing the range before it is written
  is exactly the shape issue 52 shipped.

---

## FM-REPLICATION-015 — a granted `+CONTINUE` replays exactly `(replay_from, current]`, once, in order

| Field | Value |
|---|---|
| Trigger | A partial resync is granted while the primary is still taking writes: the backlog tail is extracted and written to the replica at the same time new commands are being broadcast to live subscribers. Sharpened by a multi-shard primary, where the tail interleaves commands that executed on different shards. |
| Observable | The replica's keyspace after the resume equals the primary's: every command in the gap is applied exactly once, in offset order, and the live stream continues from the tail's end with no repeat of the last replayed command and no skipped one. Replayed commands carry the same origin shard tag the live frames carried, so a replica applying by shard routes them to the same place. Repeated reconnects at the same offset are all servable — extraction does not consume the buffer, so N replicas resuming from the same point each get the full tail. `WAIT` on the primary counts the resumed replica once it acks past the tail. |
| NOT observable | **A gap or a double-apply at the replay/live seam.** The concrete shapes: reading the backlog head *before* subscribing to the broadcast, so a command published in between belongs to neither the tail nor the live stream — a silently missing write; the live tail's dedup dropped or its comparison flipped from `<=` to `<`, so the boundary command is applied twice (for a non-idempotent replicated command that is corruption, not a no-op); `extract_backlog` losing its upper bound and streaming past the offset the grant promised; the tail arriving out of offset order; the shard tag defaulting to 0 on the replayed frames so a multi-shard replica misroutes them; the extraction draining the buffer so the second replica to reconnect at the same offset gets a short tail. |
| Invariant | `start_streaming` (`replica_session.rs`) subscribes to `wal_broadcast` **before** it reads `offsets.current()` and calls `replay.extract_backlog(replay_from, current)` — the overlap is deliberate, and the duplicate it creates is removed on the write side by `frame.sequence <= resume_offset`, never by narrowing the read. `extract_backlog` filters `offset > start && offset <= end` under the entries lock and is non-destructive (it clones `Bytes`), with a `debug_assert` that the extracted offsets are strictly ascending. Each replayed command is re-encoded through `ReplicationFrame::new_on_shard(offset, shard_id, payload)` from the `shard_id` stored on `BufferedCommand`, so the replayed frame is byte-identical in routing to the live one. `seed_replica_position(self.id, resume_offset)` seeds the tracker at the resume point so `WAIT` accounting starts from the granted offset, not from 0. |
| Catalog | `INV-BACKLOG-2`, `INV-OFFSET-3` — a grant never names a range above the offset it replays to, and seeding the tracker at the resume point never credits a replica past the live head. |
| Outcome variant | `ReplayDecision::Continue(ReplayGrant { replay_from, frames, resume_offset })` |
| Forced by | `handle_partial_replays_backlog_then_live_tail`, `partial_window_with_backlog_grants_continue`, `window_fit_continues_with_ordered_tail`, `boundary_req_equals_current_grants_empty_tail`, `test_ring_buffer_extract_backlog_is_contiguous_and_bounded`, `test_ring_buffer_extract_is_nondestructive`, `test_partial_resync_after_brief_disconnect_grants_continue`, `test_partial_sync_continue_response`, `a_partial_grant_replies_then_streams_from_the_offset_the_replica_named`, `the_handshake_replies_render_the_lines_the_replica_parses_back` |
| Bug refs | `.scratch/testing-improvements/issues/34` (`test_partial_sync_continue_response` previously accepted `CONTINUE`, `FULLRESYNC` *or* `OK`) |
| Model | `replication_feed_gate.qnt::inv_wire_is_a_prefix_of_accepted`, `replication_feed_gate.qnt::inv_offsets_strictly_increase`, `replication_feed_gate.qnt::inv_buffer_ahead_of_the_wire`, `replication_feed_gate.qnt::inv_no_resend_below_resume`, `replication_fullsync.qnt::inv_splice_continuity` |

Deliberate non-guarantees:

* **The overlap window is bounded by the tail, not by time.** A replica that reconnects while a very
  large tail is being written sees the duplicates suppressed by sequence, so an unbounded stall in
  the replay does not corrupt anything — it only delays the live stream behind the same socket.

---

## FM-REPLICATION-016 — the backlog is bounded on both axes, and every eviction raises the floor with it

| Field | Value |
|---|---|
| Trigger | A primary under sustained write load with the backlog armed: the retained set crosses the entry cap, then the byte cap, and old commands are dropped while replicas are attached and reconnecting. |
| Observable | Memory is bounded — a primary streaming indefinitely does not grow its backlog without limit under either cap — and the retention window is honest about what it can still serve: after eviction, resuming from an offset inside the *retained* range still yields the full tail, and resuming from an evicted offset is refused (FM-REPLICATION-014) rather than short-served. Eviction is FIFO, so the retained range is always a contiguous suffix of the stream and never a hole in the middle. |
| NOT observable | **A retained range that lies about its lower edge.** The mutations: an eviction that drops the entry but leaves the floor where it was, which turns the very next reconnect at that offset into a truncated `+CONTINUE`; the floor raised to the *new* front entry's end offset instead of the evicted entry's end, which skips a servable command; eviction stopping at one of the two caps so the other is unbounded (an entry-cap-only buffer holding gigabytes of large values, or a byte-cap-only buffer holding unbounded entry count); eviction running on the wrong side (LIFO), which would strand the newest commands and hand back a hole. |
| Invariant | `ReplicationRingBuffer::push` (`ring_buffer.rs:115-156`) holds the entries lock across arm-check, eviction loop and insert. The loop is `!entries.is_empty() && (`both caps`)`: the guard is outside the disjunction, so an empty deque exits whatever the caps say, and the disjunction tests both caps (`entries.len() >= max_entries` and `current_bytes + entry_size > max_bytes`). Every `pop_front` pairs its `current_bytes.fetch_sub` with a `start.fetch_max(evicted.offset)` — the evicted command's *end* offset is exactly where the new front entry begins, so the floor lands on a real resume point rather than mid-command. `VecDeque` push-back/pop-front makes the retained set a contiguous suffix by construction. |
| Catalog | `INV-BACKLOG-1`, `INV-BACKLOG-3` — every eviction leaves the floor on a real resume point and the ring covering its range within both caps, after every push rather than the pushes this row exercises. |
| Outcome variant | n/a (wire-level; surfaces through `FullResyncReason::BacklogEvicted`) |
| Forced by | `test_ring_buffer_entry_limit_eviction`, `test_ring_buffer_byte_limit_eviction`, `test_ring_buffer_oldest_offset_tracks_eviction`, `test_ring_buffer_push_and_extract`, `test_ring_buffer_empty`, `evicted_offset_falls_back_to_full_not_truncated` |
| Bug refs | `.scratch/hardening/issues/done/14-the-replication-backlog-is-wired-to-split-brain-config.md` (fixed — the backlog now has its own config keys, both caps are validated, and the eviction loop's empty-deque guard covers both axes; see FM-REPLICATION-047) |
| Model | `replication_fullsync.qnt::inv_backlog_floor_sound` |

Deliberate non-guarantees:

* **The byte cap is a target, not a hard ceiling.** The `!entries.is_empty()` guard means a single
  command larger than `max_bytes` drains the buffer and is then retained anyway, so the buffer
  transiently holds one oversized entry. This is intentional — the alternative (refusing to buffer
  it) would advance the stream past a command the floor still claims — but no test pins it, and
  nothing pins where the floor lands after a byte-driven eviction either.
* **Both caps share one empty-deque guard.** The eviction loop leads with `!entries.is_empty() &&`,
  so neither cap can spin it: `max_entries == 0` used to make `entries.len() >= max_entries`
  permanently true on an empty deque and hang the loop *under the entries lock*, parking every
  later write behind it. It is no longer reachable from config either — `validate()` rejects `0` on
  both caps — but the guard, not the validation, is what makes it unreachable from every caller.
  See [FM-REPLICATION-047](#fm-replication-047--the-backlog-is-configured-by-its-own-keys-and-no-configuration-of-it-can-hang-a-write).

---

## FM-REPLICATION-017 — a PSYNC that arrives behind the shutdown drain is refused, not half-served

| Field | Value |
|---|---|
| Trigger | A replica's PSYNC lands during primary shutdown, after the drain latch is set: either racing the latch, or reconnecting into a node that has already begun disconnecting its downstreams. |
| Observable | The connection is closed with a connection-aborted error and no handshake reply — the replica sees a failed attach and retries against whoever is primary next. Already-streaming sessions are ended by the same shutdown, so `connected_slaves` goes to zero and stays there; a late attach does not push it back up. The shutting-down node never becomes the source of a checkpoint it will not finish streaming. |
| NOT observable | **A replica registered against a node that is going away**: a `+FULLRESYNC` granted and a checkpoint cut during shutdown (expensive, and abandoned mid-transfer, leaving the replica with a partial payload); a session registered in the tracker that shutdown has already walked past, so it is never disconnected and `connected_slaves` reports a phantom; a `+CONTINUE` granted off a backlog that is about to be reset by `end_primary_stint`. |
| Invariant | `PrimaryReplicationHandler::draining` (`AtomicBool`) is set before downstream sessions are torn down, and `handle_psync` (`primary/mod.rs:518`) tests it as its **first** action — ahead of reading the offset, ahead of the replay decision, ahead of `register_replica` — returning `io::ErrorKind::ConnectionAborted`. The ordering (latch, then disconnect) is what makes the two paths jointly exhaustive: a PSYNC either registers before the latch and is torn down by the shutdown walk, or sees the latch and never registers. |
| Outcome variant | `io::ErrorKind::ConnectionAborted` |
| Forced by | `psync_after_the_shutdown_drain_is_refused`, `shutdown_downstream_sessions_ends_a_streaming_session` |
| Bug refs | — |

---

## FM-REPLICATION-018 — an unrecognized `REPLCONF` option never breaks the handshake

| Field | Value |
|---|---|
| Trigger | A replica (or a Redis-ecosystem tool) sends the pre-PSYNC `REPLCONF` options: `listening-port`, `capa eof psync2`, `ip-address`, `rdb-only`, `rdb-filter-only`, `capa-eof`, and options a future or foreign version knows that this primary does not. Also bare `REPLCONF` with no arguments. |
| Observable | Every one is answered `+OK`, so the handshake proceeds to PSYNC regardless of which options the peer chose to announce — a newer replica talking to an older primary, or `redis-cli --replica`, gets to the sync rather than stalling on an unknown-option error. `REPLCONF ACK <offset>` and `REPLCONF GETACK *` are answered on the same connection without disturbing it. |
| NOT observable | **A handshake that dies on an option.** The shapes: an unknown subcommand returning an error instead of `+OK`, which turns a forward-compatible option into a hard sync failure for every replica that sends it; a malformed `listening-port` (non-numeric, out of `u16` range) erroring the connection rather than being rejected locally; bare `REPLCONF` erroring on arity. The permissiveness has a floor, though: `+OK` must not be extended to *`PSYNC`* itself — a `PSYNC` that cannot be handled is an error, never a cheerful `+OK` that leaves the replica waiting for a stream that never starts. |
| Invariant | `ReplconfCommand::execute` (`commands/replication.rs:217`) matches the known subcommands and falls through to `+OK` on anything else, explicitly "for forward compatibility"; only `ACK`/`GETACK` have side effects, and a `listening-port` parse failure is contained by the shared `AnnouncedOption::parse` / `announcement_error` pair that both this path and the connection stage use, rather than escalated. `PsyncCommand::execute` deliberately returns `CommandError::Internal` because the real path is owned by `DispatchStage::ReplicationHandshake`/`PsyncHandoff::from_args`, so a `PSYNC` that reaches the ordinary command path is loud rather than silently OK. |
| Outcome variant | n/a (RESP `+OK`) |
| Forced by | `test_replconf_listening_port`, `test_replconf_capa`, `test_replconf_ack`, `test_replconf_subcommands`, `announcement_errors_keep_their_wire_shape` |
| Bug refs | — |

Deliberate non-guarantees:

* **`+OK` is an acknowledgement of receipt, not of support.** `listening-port` and `capa` *are* now
  recorded and rendered (FM-REPLICATION-049), but the rest still are not: `ip-address`, `rdb-only`,
  `rdb-filter-only` and every unknown option are answered `+OK` and dropped, so a replica cannot
  infer from `+OK` that the primary will act on the option it sent. An unknown `capa` token is
  dropped from the recorded set for the same reason — the `+OK` is about the handshake continuing,
  not about the capability being honoured.

## FM-REPLICATION-019 — a promotion mints a new history and freezes the inherited one at the applied offset

| Field | Value |
|---|---|
| Trigger | A replica is promoted — `REPLICAOF NO ONE`, or a cluster-committed `PromotionEvent` routed through `RoleManager::promote`. The node has been following someone else's `replication_id` and must now head a history of its own, while the siblings that followed the same upstream still hold everything up to the point this node reached. |
| Observable | `INFO replication` on the promoted node reports `role:master`, a **new** 40-hex-char `master_replid` distinct from the one it followed, the inherited id verbatim as `master_replid2`, and `second_repl_offset` equal to the node's own applied offset at the instant of promotion — **inclusive**, no Redis `+1`. `master_repl_offset` is continuous across the identity change (it does not reset, jump, or rewind). A sibling that presents `(inherited_replid, offset)` for any `offset <= second_repl_offset` is answered `+CONTINUE` and resumes; the same sibling one byte past the boundary, or any unrelated replid, is answered `+FULLRESYNC`. The backlog floor is re-armed from the boundary, so a re-promotion at a *lower* offset opens a fresh window rather than claiming continuity over a gap. |
| NOT observable | **A window frozen at the received head instead of the applied head.** A replica's decode loop runs ahead of its applier; freezing at `live` would let this node grant `+CONTINUE` over frames it decoded but never applied, handing a sibling a silent hole in the keyspace. Freezing low only costs a full resync — freezing high corrupts. **An exclusive boundary** (`+1`, Redis's convention): FrogDB replicas request their *applied* offset and `ReplicationState::window_contains` compares with `<=`, so a `+1` would grant one byte of history the node never had. **A stale window carried in from a previous role** — the mint replaces `replication_id` and overwrites `secondary_id`/`secondary_offset` in one shot, never merges. **The old id still being advertised as `master_replid`** (the pre-`ReplicationIdentity` bug: two role handlers each minting their own state, so a promoted node advertised the id it had already replaced). |
| Invariant | `PrimaryReplicationHandler::begin_primary_stint` (`frogdb-server/crates/replication/src/primary/mod.rs`) reads the boundary from `OffsetCoordinator::settle_at_applied()` — never `current()` — which freezes the `ApplyGate` and the applied counter together, so the boundary it returns is exactly what the applier has *claimed*, and the applier claims a group before, never after, handing it to a shard. What the boundary then *does* is the pure decision `plan_primary_stint(previous, minted_id, boundary) -> StintPlan` (`primary/promotion.rs`), which the method applies: `ReplicationState::shift_replication_id` moves the old id into `secondary_id` via `mem::replace` and stores the boundary unshifted into `secondary_offset`, and `StintPlan::backlog_floor` is that same boundary. The mint's entropy (`generate_replication_id`) is drawn in the method, not the decision, which is what makes the decision a function of its inputs. `OffsetCoordinator::can_serve_partial_sync` delegates the two-bound check to `window_contains`, whose secondary arm requires `secondary_offset >= 0` and `requested_offset <= secondary_offset`. `PartialSyncReplay::reset_backlog()` + `arm_backlog_floor(boundary)` re-anchor the resume window to the new history. `RoleManager::promote` runs the mint **before** clearing `is_replica`, so no PSYNC can be served against a half-built identity. |
| Catalog | `INV-REPLID-2`, `INV-REPLID-1`, `INV-REPLID-3`, `INV-OFFSET-4` — the promotion pair is the entry's whole subject: the id this node headed becomes the secondary id frozen at the boundary, both ids stay well-formed and distinct, the window is whole, and the frozen offset never exceeds the live head. |
| Outcome variant | `+CONTINUE` at or below the boundary under the inherited id; `+FULLRESYNC` above it |
| Forced by | `test_secondary_replication_id_failover`, `test_promoted_node_via_replicaof_no_one_serves_downstream_psync`, `test_psync2_failover_partial_sync`, `test_failover_chain_survivor_reattaches_to_promoted_node`, `promotion_freezes_the_window_at_the_applied_offset_not_the_received_head`, `a_re_promotion_at_a_lower_offset_re_arms_the_floor_from_scratch`, `shift_replication_id_freezes_window_inclusively`, `test_window_contains`, `can_serve_partial_sync_honours_secondary_failover_window`, `secondary_id_within_window_continues`, `a_stint_heads_the_minted_id_and_freezes_the_inherited_one_at_the_boundary`, `the_save_offset_rises_to_the_boundary`, `the_save_offset_never_follows_a_boundary_backwards`, `a_boundary_equal_to_the_save_offset_changes_nothing_about_it`, `a_stint_at_the_zero_boundary_still_mints_and_arms`, `a_second_stint_shifts_the_first_stints_id_into_the_window`, `the_decision_is_a_function_of_its_inputs_alone`, `promotion_model_smoke`, `promotion_model_full_deep` |
| Bug refs | `.scratch/replication-cluster-rework/promotion-replid-psync.md` §6.1, §12 finding 1 (CRITICAL: window frozen at the received head, not the applied head), §12 finding 2 (MAJOR: backlog floor survived a role change) |
| Model | `replication_fullsync.qnt::inv_failover_window_whole`, `replication_fullsync.qnt::inv_replids_distinct`, `replication_fullsync.qnt::inv_second_offset_not_above_live` |

---

## FM-REPLICATION-020 — a promotion that cannot be persisted adopts nothing

| Field | Value |
|---|---|
| Trigger | The mint half of a promotion fails: `replication_state.json` cannot be written (read-only or full data dir, a directory in the way), or the inherited staged full-sync checkpoint cannot be disarmed. Also the benign case — a promotion arriving at a node that is *already* primary (a repeated `REPLICAOF NO ONE`, a re-delivered `PromotionEvent` from the 1 Hz role reconciler, or a node that booted primary). |
| Observable | On failure the node stays a replica: `INFO replication` still reports `role:slave` with its `master_host`/`master_port`, and `master_replid`, `master_replid2`, `second_repl_offset` are byte-identical to their pre-attempt values. `REPLICAOF NO ONE` returns the error rather than `+OK`; writes are still refused. A later retry can still promote it cleanly. On the already-primary path the node is a no-op: no new id is minted, `master_replid` does not change, `master_replid2`/`second_repl_offset` keep whatever the *first* promotion armed, and downstream replicas are not disturbed. |
| NOT observable | **A half-promoted node** — an in-memory identity that advertises a new `master_replid` no disk knows about, so a restart silently reverts to the inherited id while siblings have already resumed against the new one. **A node that flipped `is_replica` to false but never minted**, i.e. writable under the *deposed* primary's replication id — the exact double-primary shape the window exists to prevent. **A second mint on a repeated promotion**, which would rotate `master_replid2` to the id this node minted a moment ago and orphan every sibling still resuming against the real inherited history. **A window observable mid-flight**: no reader ever sees `replication_id` updated while `secondary_id` is not, or vice versa. |
| Invariant | `begin_primary_stint` performs mint → persist → rollback under **one** `state.write()` guard: it computes `plan_primary_stint(&state, minted_id, boundary)` (`primary/promotion.rs`), publishes `plan.minted`, and on a `save_snapshot` error restores `*state = plan.rollback` — the state the node was already on, carried by the plan itself — and returns `Err` before the lock is released, so the failed identity is never visible to `INFO`, PSYNC, or the cluster bus. `crate::discard_staged_full_sync(&self.data_dir)?` runs **first** and propagates its error, so a promotion that cannot disarm the inherited checkpoint aborts rather than proceeding. `RoleManager::promote` early-returns before touching the target when `!is_replica`, giving idempotence structurally rather than by comparison, and stores `is_replica = false` (Release) only **after** `begin_primary_stint()?` returns `Ok` — an `Err` leaves the flag set. |
| Catalog | `INV-REPLID-1`, `INV-REPLID-2` — a rollback that restored only half the pair would leave a window with an id and no offset, or a secondary id that is not the history this node was on. |
| Outcome variant | Error to the caller; role and identity unchanged |
| Forced by | `a_promotion_that_cannot_persist_leaves_the_identity_untouched`, `a_promotion_that_cannot_mint_leaves_the_node_a_replica`, `promote_mints_identity_before_clearing_the_replica_flag`, `promote_stops_the_inbound_stream_before_minting`, `promote_is_idempotent_and_does_not_remint`, `promoting_a_node_that_booted_primary_mints_nothing`, `discard_staged_full_sync_keeps_the_metadata_when_disarming_fails`, `the_rollback_is_the_state_the_node_was_already_on`, `a_failed_promotion_strands_the_node`, `a_failed_promotion_leaves_the_node_unable_to_replicate` |
| Bug refs | `.scratch/replication-cluster-rework/promotion-replid-psync.md` §12 finding 5 (MEDIUM: a promotion that could not persist flipped the role flag anyway), §13 finding 3 (MEDIUM: mint/persist/rollback made atomic under one lock) |
| Model | `replication_fullsync.qnt::inv_replid_offset_paired` |

**Deliberate non-guarantee — "adopts nothing" is about the identity, not the link.** The rollback
this row owns is `StintPlan`'s: the `ReplicationState`, restored bit for bit. It is *not* a
rollback of the promotion attempt as a whole. `RoleManager::promote` has already dropped the
inbound stream and cleared `primary_target` (irreversibly — the socket is gone), and
`begin_primary_stint` has already frozen the applied gate via `settle_at_applied()`; neither is
undone, so the node comes to rest as a replica that follows nobody, applies nothing
(`Claim::Retired`) and cannot complete a full resync either (`reset_to` refused under the frozen
gate). Only a fresh replica stint — `REPLICAOF`, or the cluster role reconciler — reopens it. The
end state is the one documented at `RoleManager::promote`'s `# Failure`, and whether it is the one
we want is open as replication-correctness issue 16; the model witness
`a_failed_promotion_strands_the_node` and the replay
`a_failed_promotion_leaves_the_node_unable_to_replicate` characterize it so a fix fails loudly.

---

## FM-REPLICATION-021 — a promoted identity survives a restart, and the checkpoint it inherited cannot resurrect

| Field | Value |
|---|---|
| Trigger | A node is promoted and then restarted on its own data directory — the ordinary end of a failover. The node may be carrying a staged full-sync checkpoint from the upstream it was following when the promotion landed (a full resync stages a dataset that boot recovery installs on the next start). Also covers a restart of a plain primary, and a restart whose `replication_state.json` is unreadable or fails validation. |
| Observable | After the reboot `INFO replication` reports the **same** `master_replid`, the same `master_replid2`, and the same `second_repl_offset` the node advertised before shutdown, so a sibling that was mid-resume against the failover window can still resume against it. Keys written *after* the promotion are still present, alongside the pre-promotion data the node inherited. `master_repl_offset` resumes from the saved position, not from 0. When the state file is corrupt or invalid the node boots with a **freshly minted** id and **no** window (`master_replid2` all-zero, `second_repl_offset:-1`), so every downstream is answered `+FULLRESYNC` instead of being handed a resume over history the node cannot prove. A state file written by an older build (`replication_offset` key) still restores its offset. |
| NOT observable | **The deposed primary's checkpoint reinstalling over the promoted node's database** — a boot-time full-sync install would replace the keyspace *and* re-adopt the upstream's `replication_id`, silently discarding every write the node accepted since it was promoted while continuing to advertise the failover window. **A staged directory deleted but not disarmed**: if the delete fails partway, a directory that is still *named* as live is a live checkpoint; the rename must happen first so a crash mid-cleanup leaves the promotion intact. **Metadata surviving a failed disarm** — the carried staged metadata is consumed unconditionally so no later code path can adopt it. **A restart that rewinds the live head below `offset_at_save`.** **A corrupt state file being partially trusted** (id kept, window dropped, or vice versa) — a regenerated identity is all-or-nothing. |
| Invariant | `discard_staged_full_sync(data_dir)` (`frogdb-server/crates/replication/src/state.rs`) renames the staging directory to `*.discarded` **before** attempting the delete — the rename is the atomic disarm, the delete is best-effort cleanup — always consumes the carried `StagedReplicationMetadata`, and returns `Err` on a rename failure so the caller (`begin_primary_stint`) aborts the promotion. `ReplicationState::save` writes a temp file and `fs::rename`s it into place, so a reader never sees a partial record. `load_or_create` regenerates a fresh state whenever parse or `validate()` fails, and `offset_at_save` carries a serde alias for the legacy `replication_offset` key. `ReplicationIdentity::adopting` seeds the live head with `fetch_max(state.offset_at_save)` — raise-only, so a tracker already positioned is never rewound. |
| Catalog | `INV-REPLID-1`, `INV-REPLID-3`, `INV-OFFSET-2` — what a reload may reconstitute is a whole window over two well-formed, distinct ids. `INV-OFFSET-2` is the ruled exception (issue 17) rather than a guarantee: the raise-only `offset_at_save` this row describes is precisely what can leave the file claiming more than the live head. |
| Outcome variant | Identity + window restored verbatim; on a corrupt file, fresh identity → `+FULLRESYNC` for every downstream |
| Forced by | `test_promoted_identity_survives_restart`, `test_info_master_replid_survives_restart`, `discard_staged_full_sync_disarms_the_staging_area`, `discard_staged_full_sync_disarms_before_deleting`, `test_consume_staged_replication_metadata`, `offset_at_save_loads_from_legacy_replication_offset_key`, `test_replication_state_load_corrupted`, `test_replication_state_persistence`, `test_replication_state_load_missing` |
| Bug refs | `.scratch/replication-cluster-rework/promotion-replid-psync.md` §11 "Two fixes beyond the plan" (the staged full-sync checkpoint survived promotion), §12 finding 3 (MAJOR: `discard_staged_full_sync` deleted before disarming) |

**Deliberate non-guarantee.** `ReplicationState::save` does *not* `fsync` the file or its parent
directory. A power cut between the rename and the OS flush can lose the most recent identity
update, and `offset_at_save` lags the live head by construction (it is refreshed on the persistence
path, not on every write). A node that loses its state file this way boots with a fresh id and
full-resyncs its downstreams — safe, but not free.

---

## FM-REPLICATION-022 — a demotion stops the node being a replication source, and adopting a new history closes its window

| Field | Value |
|---|---|
| Trigger | A writable node is demoted — `REPLICAOF <host> <port>` on a live primary, or a cluster-committed `DemotionEvent`. It may have downstream replicas attached, blocked `WAIT` calls in flight, and an armed failover window from an earlier promotion. It then full-resyncs from its new upstream and adopts that node's history. |
| Observable | `INFO replication` flips to `role:slave` reporting the **real** new `master_host`/`master_port` (and `ROLE` agrees). Downstream replicas are disconnected, and their next `PSYNC` against this node is rejected — there is no interval in which a node is a replica of one primary while still serving a resync to another. Blocked `WAIT` calls return rather than hanging on a quorum that can no longer be reached. Once the node links up and adopts the new upstream's history, `master_replid` is the upstream's id, `master_replid2` returns to the all-zero sentinel and `second_repl_offset` to `-1` — the node no longer claims any resumable history of its own. A later re-promotion mints a *fresh* id again rather than reusing the one it minted before. A repeated demotion to the same primary is a no-op (the stint is not ended twice). |
| NOT observable | **A stale failover window surviving a role round-trip.** A node that promoted (arming `replid2` = old-primary-id) and then demoted onto an unrelated primary would, if the window persisted, grant `+CONTINUE` under an id whose data it has just thrown away in a full resync — a sibling resumes over a keyspace that was replaced underneath it. **A backlog that outlives the stint**, letting a `PSYNC` land a `+CONTINUE` against a history the node abandoned. **`WAIT` blocked forever** across the role change. **`INFO` still rendering `master`/`connected_slaves`** after the flag flipped. |
| Invariant | `RoleManager::demote` stores `is_replica = true` (Release) **first** — so every gate that reads the flag rejects immediately — then `end_primary_stint()`, then tears down the old stream, then `replication_self_fence.reset_arming()`, then starts the new stream; the ordering is the mirror of `promote`. `PrimaryReplicationHandler::end_primary_stint` (`primary/mod.rs:382`) does exactly four things: `wait.fence_role_change()`, `replay.reset_backlog()`, `offsets.retire_replica_applies()`, `tracker.disconnect_all_replicas()`. `ReplicationState::adopt_replication_history` sets the new id and calls `clear_secondary_window()` (`secondary_id = None`, `secondary_offset = -1`) in the same call, so adoption cannot leave a half-open window; `apply_staged_metadata` routes through the same function so the checkpoint path cannot diverge from the live path. The `ReplicationHandshake` gate reads the same live role flag. |
| Catalog | `INV-REPLID-1`, `INV-ROLE-1` — `clear_secondary_window` clears both halves or neither, in every state and not only on the adoption path. `INV-ROLE-1` states this row's headline claim universally — a node that follows an upstream serves no downstream session — and is tiered as the documented non-guarantee it is: chained replication is not refused (issue 48). |
| Outcome variant | Downstream `PSYNC` rejected; window cleared to the `-1` / all-zero sentinels after resync |
| Forced by | `test_demoted_primary_stops_serving_psync_to_its_downstream`, `test_promotion_window_cleared_when_node_adopts_a_new_history`, `test_wait_unblocked_on_demotion`, `ending_a_stint_disconnects_downstream_replicas`, `demote_ends_the_primary_stint_while_the_node_is_already_fenced`, `a_no_op_demotion_does_not_end_the_stint_again`, `re_promotion_after_a_demotion_mints_again`, `adopt_replication_history_drops_a_stale_window`, `apply_staged_metadata_drops_a_stale_window`, `clear_secondary_window_closes_the_old_history` |
| Bug refs | `.scratch/replication-cluster-rework/promotion-replid-psync.md` §6.3 (stale secondary window across a role round-trip), §12 finding 2 (MAJOR: backlog and floor survived a demotion) |

---

## FM-REPLICATION-023 — `INFO replication` reports one identity for the whole node, at every point in its life

| Field | Value |
|---|---|
| Trigger | Any `INFO replication` — on a standalone node that never replicated, on a replica following a primary, on a promoted node with an armed window, and on a node that has been demoted. The identity is read by operators, by `frogctl`, and by tooling that decides whether a resume is possible. |
| Observable | Exactly one `master_replid` per node, shared by both role handlers: a replica reports the **same** id as the primary it follows once `+FULLRESYNC` has landed. A node with no failover history reports `master_replid2:0000000000000000000000000000000000000000` and `second_repl_offset:-1`. A node with a window reports the inherited id and its inclusive boundary in the same pair, and does so **in both role arms** — the fields are not a primary-only decoration, so a promoted-then-demoted node still shows the window until it adopts a new history. A boundary of `0` renders as `second_repl_offset:0` and is a real window, distinct from the `-1` no-failover sentinel. A standalone node with no replication state renders its node id as `master_replid` rather than a blank or a zero id. |
| NOT observable | **`0` conflated with "no window."** `second_repl_offset:0` means "everything up to and including offset 0 is resumable under `master_replid2`"; `-1` means there is no window at all. Collapsing the two (rendering `0` for an absent window, or treating a `0` boundary as absent) either invites a `+CONTINUE` that must not be granted or refuses one that should be. **A window rendered from a `secondary_id` with a negative `secondary_offset`**, i.e. a half-cleared state leaking into `INFO`. **A replica hard-coding `master_repl_offset:0`** while its stream is at a real position — INFO that lies about how far a node has got is worse than INFO that omits the field. **Two different ids from the two role handlers**, the pre-`ReplicationIdentity` shape where promotion made `INFO` and PSYNC disagree. |
| Invariant | `ReplicationIdentity` is one cell per process (`state: SharedReplicationState`, `live: Arc<AtomicU64>`, `applied: AppliedOffset`), handed by clone to the primary handler, the replica handler, the cluster bus health probe, and INFO — `adopting()` takes the tracker's *existing* offset atomic rather than allocating a second one, which is what makes all four report the same number. `connection/info_handler.rs` builds `secondary_window: Option<(String, u64)>` only when `secondary_id.is_some() && secondary_offset >= 0`, so the `-1` sentinel can never render as a window, and gates the primary branch on an Acquire load of `is_replica`. `info/sections.rs` renders the `Option` with a single `match` — `Some` → the pair verbatim, `None` → the all-zero id and `-1` — so both role arms share one formatting decision. |
| Catalog | `INV-REPLID-1`, `INV-REPLID-3` — the `None`/`-1` pair `info_handler` refuses to render as a window is the same well-formedness the entries hold at every transition, so the render cannot be the only thing standing between a half-open window and a client. |
| Outcome variant | `-1` / all-zero sentinels when no window; the pair verbatim when there is one |
| Forced by | `replication_primary_renders_secondary_window_after_failover`, `replication_replica_renders_secondary_window_after_failover`, `replication_standalone_renders_node_id_replid`, `replication_live_replid_overrides_node_id`, `test_info_master_replid_replica_matches_primary` |
| Bug refs | `.scratch/replication-cluster-rework/promotion-replid-psync.md` §11 "Two fixes beyond the plan" (INFO `master_repl_offset` on a replica was hard-coded to 0) |

---

## FM-REPLICATION-024 — a split-brain demotion audits exactly the writes it is about to discard, once

| Field | Value |
|---|---|
| Trigger | The metadata plane commits a `DemotionEvent` against a node that has been accepting writes as a primary while the cluster elected another — two nodes believing they head the same history. The node's backlog holds writes past the point its last streaming replica acknowledged; those writes are about to be thrown away by the resync that follows. The self-role reconciler re-emits the same event at 1 Hz until the node converges. |
| Observable | A `split_brain_discarded_<TS>.log` appears in the data directory before the demotion is issued, carrying `seq_diverge_start` = the acked floor, `seq_diverge_end` = the live head, `ops_discarded` = the write count, `old_primary`/`new_primary` as **hex** node ids (`unknown` when the event names no successor), `epoch_old` = the event's epoch and `epoch_new` = that epoch `+ 1`, followed by the raw RESP of each divergent write. Writes at or below the acked floor are **absent** — the new primary already holds them. `frogdb_split_brain_ops_discarded_total` equals the discarded count exactly, `frogdb_split_brain_events_total` counts one, and `frogdb_split_brain_recovery_pending` is raised while a log awaits an operator. `has_pending_logs()` reports true. The demotion to the new primary fires either way, so the node resyncs and the divergent writes are gone from the keyspace. A re-delivery of the same `(node, epoch)` writes no second file and does not inflate the counters. A node that did **not** diverge writes nothing and bumps nothing, and the demotion is byte-identical whether or not split-brain logging is enabled. What the telemetry reports when the audit *cannot* be written is FM-REPLICATION-048. |
| NOT observable | **Acknowledged writes in the discard audit.** The floor is the acked offset, not `0` and not the last checkpoint; surrendering acked writes into a "discarded" file tells an operator that data the cluster still holds was lost. **A window whose end is below its start**, or an empty write set dressed up as a divergence — an audit file with `ops_discarded=0` is noise that hides the real events. **A second file per re-emission**: the reconciler retries at 1 Hz, so an undeduped logger would produce a file per second and a discard counter that grows without any data being discarded. **Silent discard** — the demotion must never proceed without the audit having been attempted first. **Discarding without demoting**, or demoting without the counters moving. |
| Invariant | `PrimaryReplicationHandler::divergence_record` (`primary/mod.rs:619`) is the single owner of the divergence predicate: `start = offsets.min_acked().unwrap_or(0)`, `end = offsets.current()`, `None` when `end <= start` **or** when `extract_divergent_writes(start)` (filter `offset > start`, non-destructive) comes back empty. Both offsets come from the one `OffsetCoordinator`, so a concurrent `advance` between the two reads only widens `end`. `split_brain_header` is extracted precisely so the field mapping is pinned by a unit test. `split_brain_log::write_log` writes the header then the raw entries and calls `file.sync_all()` before returning the path. `RoleChangeConsumer::logged_demotion: Mutex<Option<(NodeId, u64)>>` dedupes on the `(node, epoch)` pair. The logger is `Option` at the type level — `None` when `split_brain_log_enabled` is off — so the demotion path is structurally independent of it. |
| Outcome variant | `Some(DivergenceRecord)` → audit + telemetry + demote; `None` → demote only |
| Forced by | `split_brain_lifecycle_captures_audit_and_initiates_discard`, `split_brain_header_maps_record_and_event_fields`, `split_brain_header_unknown_new_primary`, `demotion_identical_whether_or_not_log_enabled`, `demotion_fires_when_split_brain_log_disabled`, `divergence_record_window_and_writes`, `divergence_record_none_when_caught_up`, `divergence_record_none_when_backlog_empty_past_start`, `divergence_record_no_streaming_replicas_uses_zero_floor`, `test_write_and_read_log`, `test_write_log_empty_entries`, `test_has_pending_logs`, `test_has_pending_logs_ignores_other_files`, `every_discarded_entry_is_newline_separated_exactly_once` |
| Bug refs | `.scratch/replication-cluster-rework/issues/done/08` (a divergence retires nothing) |

---

## FM-REPLICATION-025 — `REPLICAOF <host> <port>` fences the node before it opens a stream

| Field | Value |
|---|---|
| Trigger | A live, writable primary is told `REPLICAOF <host> <port>` at runtime — the demoted-old-primary shape after a failover, or an operator re-pointing a replica at a different primary mid-stream. Sharpened by the node holding its own divergent keyspace and by clients writing to it concurrently with the command. |
| Observable | The `+OK` is not a promise about the *link*, it is a promise about the *role*: by the time it returns, `ROLE` reports `slave`, `INFO replication` reports `role:slave` with `master_host`/`master_port` set to the resolved target, and every write is answered `-READONLY` ([FM-REPLICATION-028](#fm-replication-028--a-replica-refuses-every-write-command-by-flag-and-not-by-list)) — all of this before any byte of the new stream arrives, and regardless of whether the target is reachable at all. Re-issuing `REPLICAOF` against the *same* target is a no-op that does not tear down a healthy link; issuing it against a *different* target switches primaries: the old stream is dropped and the reported `master_host`/`master_port` become the new target. Bad arguments are refused with the role untouched: cluster mode answers `ERR REPLICAOF not allowed in cluster mode.`, port `0` and an unresolvable host are argument errors, and the node stays a writable primary. |
| NOT observable | **A window in which the node reports `slave` but still accepts writes**, or reports `master` while a primary's frames are already landing in its keyspace — either one lets a client's acknowledged write be silently overwritten by the incoming stream with no error ever returned. Nor: a second `REPLICAOF` at the same target restarting a healthy stream (a gratuitous full resync); a switch to a new target leaving the *old* primary's stream still applying, so two histories interleave into one keyspace; a refused `REPLICAOF` (cluster mode, port 0, bad host) leaving the node half-demoted. |
| Invariant | `RoleManager::demote(addr)` (`frogdb-server/crates/server/src/role_manager.rs:332`) orders the teardown so the flag is the *first* thing that moves: set `is_replica` (`Release`) → end the primary stint → drop the existing `ReplicaStream` → stop the boot handler → `checker.reset_arming()`. The read path loads the same flag under `Acquire` in `run_pre_checks` (`frogdb-server/crates/server/src/connection/guards.rs:275`), so a `-READONLY` is guaranteed for every command that starts after `+OK`. Idempotence is per-target: `demote` compares the recorded `SocketAddr` and returns without touching the stream when it matches, and tears down and rebuilds when it does not. Argument validation runs entirely before `controller.request_demote(addr)` (`frogdb-server/crates/server/src/commands/replication.rs:62`, `:103`, `:133-148`), so a rejected command never reaches the role manager. `RealReplicaStream::Drop` (`role_manager.rs:688`) stops the handler and aborts the connection but deliberately leaves the frame consumer alive (FM-REPLICATION-007). |
| Outcome variant | `ROLE`/`INFO replication` `role`, `master_host`, `master_port`; `CommandError::InvalidArgument` |
| Forced by | `test_replicaof_host_port_demotes`, `test_role_and_info_report_real_primary_after_demotion`, `demote_sets_flag_records_target_and_starts_stream`, `demote_is_idempotent_per_target_but_switches_primaries`, `demote_resets_replication_self_fence_arming` |
| Bug refs | `.scratch/testing-improvements/issues/61` (runtime `REPLICAOF` full resync must install into the live store) |

**Where the stint teardown is pinned.** Ending the primary stint — so the node stops counting a
replication offset on a history it no longer owns, and so a demotion that arrives while the node is
already fenced still closes the stint — belongs to
[FM-REPLICATION-022](#fm-replication-022--a-demotion-stops-the-node-being-a-replication-source-and-adopting-a-new-history-closes-its-window)
and is forced there. This row covers only the *ordering* guarantee that the fence precedes it.

**Not covered here.** What the new link then does with the target — handshake, `+FULLRESYNC` and
installing the payload into the live keyspace — is FM-REPLICATION-001. This row stops at the role
flip.

---

## FM-REPLICATION-026 — `REPLICAOF NO ONE` promotes once, and the old primary's stream never lands again

| Field | Value |
|---|---|
| Trigger | `REPLICAOF NO ONE` on a node that is currently following a primary — the manual-failover path, and the same path automatic promotion drives. Sharpened by the old primary *still being alive and still writing*, and by the command being issued twice. |
| Observable | The node becomes a writable primary: `ROLE` reports `master`, every write command that was answered `-READONLY` a moment ago now succeeds, and it serves `PSYNC` to its own downstream. Writes the old primary performs **after** the promotion never appear in the promoted node's keyspace, however long a client waits — the link is gone, not merely idle. A node that boot-configured a primary target keeps that promise across the promotion too: the reconnect loop its `--replicaof` started does not survive to re-attach. A demote/promote round trip returns the node to exactly the state it started in. |
| NOT observable | **A promoted node still applying its old primary's frames** — split brain with no error surface: the new primary accepts client writes while the demoted one silently overwrites them, and both sides report `master`. Nor: a boot-configured replica's registered handler surviving promotion and quietly reconnecting to the old primary, so the node oscillates between roles with no command having been issued; a round trip leaving residual state that makes the second demotion behave differently from the first. |
| Invariant | `RoleManager::promote()` (`frogdb-server/crates/server/src/role_manager.rs:298`) is ordered stream-first, flag-last, and `promote()` additionally stops any registered boot-replica handler — the reconnect loop is owned by the role manager, not by the boot path, precisely so a promotion can end it. The flag is what the write path reads (`guards.rs:275`), so clearing it last means "writable" implies "no stream". |
| Outcome variant | `ROLE` `master`; `INFO replication` `role:master`, `master_replid` |
| Forced by | `test_replica_of_no_one_stops_accepting_primary_writes`, `test_promoted_replica_serves_all_writes_after_promotion`, `promote_stops_registered_boot_replica_handler`, `demote_promote_round_trip` |
| Bug refs | `.scratch/replication-cluster-rework/issues` (promotion-replid rework) |

**Where the mint ordering is pinned.** The rule that the replication identity exists — and the
inbound stream is stopped — *before* the replica flag is cleared, that promotion is idempotent and
does not re-mint, and that a node which booted primary mints nothing, is
[FM-REPLICATION-019](#fm-replication-019--a-promotion-mints-a-new-history-and-freezes-the-inherited-one-at-the-applied-offset) /
[FM-REPLICATION-020](#fm-replication-020--a-promotion-that-cannot-be-persisted-adopts-nothing) and is
forced there. This row depends on that ordering and does not restate it: what it adds is the
*client-visible* consequence — the old primary's post-promotion writes are unreachable, and the boot
handler cannot resurrect the link.

---

## FM-REPLICATION-027 — the reconnect loop retries forever and never reports a link it does not have

| Field | Value |
|---|---|
| Trigger | Any way the link ends: the primary is unreachable at boot, the TCP connection is refused or reset mid-handshake, the primary closes cleanly, the sync fails, or the replica is shut down while parked in backoff. Sharpened by repeated start/stop cycles in quick succession. |
| Observable | The replica keeps trying and eventually converges once the primary is reachable — repeated attach/detach cycles each end in a synced replica, never in a node stuck detached. Throughout, `INFO replication` on the replica is honest: `master_link_status` reads `up` only while frames are actually streaming, and reads `down` from boot until the first successful handshake, across every failed attempt, and again the moment a link drops. A replica pointed at a dead address reports `role:slave` with `master_link_status` never `up`, indefinitely, while continuing to serve commands. Shutdown is prompt from every blocking point, including from inside a backoff sleep. |
| NOT observable | **`master_link_status:up` on a replica with no live stream** — the field is what operators and orchestration page on, so a stale `true` turns "my replica is fine" into silent data loss at failover time (the same class as FM-REPLICATION-001's data-less full sync). Nor: a hot reconnect loop after a clean close (a primary that closes politely must not be hammered at full speed); a failed connect that gives up permanently, leaving a replica that never reattaches after a primary restart; a `stop()` racing ahead of `start()` being lost, so a shutdown leaves the reconnect task running past process teardown. |
| Invariant | `link_up: Arc<AtomicBool>` is single-writer-per-transition and fail-closed: only `ReplicaConnection` sets it `true`, and only once it reaches `ConnectionState::Streaming`; `connect_and_sync` stores `false` (`Release`) after the inner block on **every** exit path (`frogdb-replication/src/replica/mod.rs:388`), so a stale `true` cannot survive the function that set it. `INFO` reads the same flag, so the field cannot disagree with the loop. The retry loop (`replica/mod.rs:286-341`) is unbounded, with exponential backoff from 100 ms doubling to a 30 s cap on the error path; a *clean* close resets the backoff to 100 ms and still sleeps once, so a politely-closing primary cannot be hot-looped. Every blocking point sits in a `biased` `tokio::select!` whose first branch is the `watch` shutdown channel, and `stop()` uses `send_replace`, so a stop that arrives before `start()` parks is observed by the pre-loop `borrow()` check rather than lost. |
| Outcome variant | `INFO replication` `master_link_status` |
| Forced by | `test_replica_handles_rapid_reconnect`, `test_info_replication_master_link_status_tracks_connection`, `test_info_replication_master_link_status_down_before_connected` |
| Bug refs | `.scratch/hardening` — "lying INFO" (fixed; this row is its outcome) |

**Not covered here.** The backoff *schedule* — the 100 ms floor, the doubling, the 30 s cap, and the
single sleep after a clean close — is asserted by nothing today. The tests above force convergence
and the honesty of `master_link_status`, not the timing. See the gaps list.

---

## FM-REPLICATION-028 — a replica refuses every write command, by flag and not by list

| Field | Value |
|---|---|
| Trigger | Any client command on a node whose replica flag is set — by boot config, by runtime `REPLICAOF` ([FM-REPLICATION-025](#fm-replication-025--replicaof-host-port-fences-the-node-before-it-opens-a-stream)), or by cluster-driven demotion. Sharpened by walking the *entire* command registry rather than a hand-picked sample, and by commands added after this row was written. |
| Observable | Every command carrying `CommandFlags::WRITE` is answered `READONLY You can't write against a read only replica.` — over 100 commands today, with no exceptions and no partial application: `SET`, `DEL`, `ZADD` and the long tail all fail identically. Everything else still works: `GET`, `PING`, `INFO`, `ROLE` and the rest of the read/admin surface are served normally. In cluster mode a write for a slot this node does not own is answered `-MOVED` rather than `-READONLY`, so a client's redirect cache is not poisoned by a role error. |
| NOT observable | **A write command that slips through the gate** and mutates a replica's keyspace — a divergence with no error, no log and no offset movement, which the next full resync silently erases along with the client's acknowledged data. The bug shape being ruled out is specifically a *hand-maintained list* of blocked commands: any list drifts the moment a command is added, so the gate must be derived from the registry. Nor the inverse: a replica refusing reads or `INFO` (which would make it useless and would break the very tooling used to diagnose it); replica *apply* traffic being caught by the gate (that would stop replication dead — it is internal, not a client); a `-READONLY` shadowing a `-MOVED` in cluster mode. |
| Invariant | The gate is four lines in `run_pre_checks` (`frogdb-server/crates/server/src/connection/guards.rs:306-314`): `is_replica` (`Acquire`) → registry lookup → `flags().contains(CommandFlags::WRITE)` → `!write_defers_to_cluster_redirect(..)` (`guards.rs:449`). Membership is a property of the command's own registry entry, so a new write command is covered the day it is registered and cannot be forgotten. The ladder order is fixed and documented: auth → READONLY → MISCONF → CLUSTERDOWN (self-fence) → NOREPLICAS. Replica apply traffic never reaches this code at all — it runs under `REPLICA_INTERNAL_CONN_ID` with no `PreDispatchView`, so the gate is structurally inapplicable rather than conditionally skipped. |
| Outcome variant | `-READONLY` error reply; `-MOVED` when the cluster redirect takes precedence |
| Forced by | `test_replica_rejects_every_write_command`, `test_replica_readonly_enforcement`, `test_replica_read_only`, `test_replica_readonly_error` |
| Bug refs | — |

**Where the role transitions are pinned.** That the gate engages the instant `REPLICAOF <host>
<port>` returns, and disengages the instant `REPLICAOF NO ONE` returns, is
[FM-REPLICATION-025](#fm-replication-025--replicaof-host-port-fences-the-node-before-it-opens-a-stream)
and [FM-REPLICATION-026](#fm-replication-026--replicaof-no-one-promotes-once-and-the-old-primarys-stream-never-lands-again).
This row covers only *which* commands the gate catches once it is engaged.

**Not covered here.** The `-MOVED`-beats-`-READONLY` precedence is stated above because it is the
reason `write_defers_to_cluster_redirect` exists, but nothing forces it today — see the gaps list.

---

## FM-REPLICATION-029 — a replica keeps serving, and its reads never go backwards

| Field | Value |
|---|---|
| Trigger | A client reading from a replica while the primary is still writing — a replica lagging behind an in-flight write burst, and (by the same mechanism) one whose link has dropped or has never connected. |
| Observable | The replica answers reads and `INFO` throughout, whatever the link is doing; it never returns an error because it is behind. What it returns is *stale, not wrong*: a monotonic-read guarantee holds per key — once a client has observed value N from a replica, no later read from that replica returns anything older, even while later writes are still in flight. After `WAIT 1 <ms>` reports the replica acked, a read observes at least that write (read-your-writes through the ack), and the replica converges to the primary's final value. |
| NOT observable | **A read that goes backwards** — the shape that breaks caches and read-scaled workloads far more insidiously than staleness: a client that saw `100` seeing `97` again means frames applied out of order, a partial group made visible, or a resync exposing a half-installed dataset mid-flight. Nor: a replica refusing reads with a `-MASTERDOWN`-style error while behind or disconnected (FrogDB has no such gate and no knob for one); a replica returning a value the primary never wrote; a post-`WAIT`-ack read failing to observe the acked write. |
| Invariant | Frames are applied in stream order by the single long-lived frame consumer, and the applied head only advances via `land()` after the shard write completes (FM-REPLICATION-008), so no read can observe an offset regression. A full resync installs the dataset *before* adopting the offset and rewinds to 0 on failure (FM-REPLICATION-001), so a failed install never exposes a partially-replaced keyspace. There is deliberately **no** stale-read gate on the read path: FrogDB has no `replica-serve-stale-data` config knob at all and behaves unconditionally like Redis's `yes` default — the same choice FM-REPLICATION-010 records for the divergence window. |
| Outcome variant | n/a (read-path guarantee; surfaces only as reply values) |
| Forced by | `test_replica_read_monotonic_after_primary_writes` |
| Bug refs | — |

**Scope, and what is only inspected.** The forcing test covers the *lagging but connected* replica.
Two neighbouring cases are asserted nowhere:

* **A replica whose link drops after it already holds data** — the operationally common one (primary
  dies, client keeps reading). That the previously-synced keys stay readable is the
  `replica-serve-stale-data yes` behaviour this row claims, and it is currently held by inspection
  only. See the gaps list.
* **A replica that has never connected at all** still serving `INFO` is forced by
  [FM-REPLICATION-027](#fm-replication-027--the-reconnect-loop-retries-forever-and-never-reports-a-link-it-does-not-have)'s
  dead-address test, but that test reads no keys, so it says nothing about the read path.

---

## FM-REPLICATION-030 — a replica expires on its own clock, and no `DEL` is ever propagated

| Field | Value |
|---|---|
| Trigger | A key with a TTL on a primary that has a connected replica, and the TTL elapsing. Also: `EXPIRE`/`PEXPIRE`/`SET ... EX` being replicated at all. This is a **deliberate deviation from Redis** and the row exists to pin it. |
| Observable | The key disappears from the replica without the primary ever sending a `DEL`: the replication offset is byte-identical before and after the expiry fires, proving nothing crossed the wire. TTL commands replicate **verbatim and relative** — the primary does not rewrite `EXPIRE k 10` into an absolute `PEXPIREAT` — so the replica re-anchors the TTL against its own clock at apply time and a bounded drift window opens between the two nodes. Inside that window the replica may still serve a value the primary has already dropped; `PTTL` on the replica is bounded by the replication lag rather than exactly equal to the primary's. Outside it, both nodes agree the key is gone. |
| NOT observable | **Unbounded drift** — a replica whose TTL is anchored so much later than the primary's that a key outlives the primary's copy indefinitely, or a `PTTL` that grows rather than shrinks. Nor: a synthetic expiry `DEL` appearing on the wire and moving the replication offset (which would make every expiry a replicated write and make the offset a function of wall-clock timing); an expired key remaining readable on the replica after the drift window closes; a promoted replica inheriting TTLs it cannot expire because it was waiting for a `DEL` that will now never come. |
| Invariant | `RemovalPropagation { wal, replicate }` (`frogdb-core/src/shard/post_execution.rs:189`) splits WAL durability from replica broadcast; organic expiry uses `replicate: false`, so `EffectScope::InternalRemoval` with `RemovalReason::Expired` never reaches the broadcaster. Expiry is role-agnostic: the same active-cycle and lazy-on-read paths run on primary and replica alike, which is what makes independent expiry *correct* rather than merely tolerated — there is no `DEL` to wait for. The drift bound is the replication lag itself: the replica anchors the relative TTL when it applies the command, so its deadline is later than the primary's by at most the propagation delay. |
| Outcome variant | `RemovalReason::Expired` with `RemovalPropagation { replicate: false }`; `INFO replication` `master_repl_offset` (unchanged across an expiry) |
| Forced by | `test_replica_expires_independently_not_via_del`, `test_replica_pttl_bounded_by_replication_lag` |
| Bug refs | `.scratch/hardening` — "immortal expired keys" (fixed) |

**Prose already in the tree.** The full contrast with Redis's primary-authoritative expiry is
written out in the Tier 9 header comment at
`frogdb-server/crates/server/tests/integration_replication.rs:3961-3988`. Link to it rather than
duplicating it — the deviation table below is the short form.

---

## FM-REPLICATION-031 — the offset advances by RESP payload bytes, never by transport framing

| Field | Value |
|---|---|
| Trigger | Every broadcast on the primary and every frame ingested on the replica. Sharpened by the two cases where the two ends could most easily drift: a `REPLCONF GETACK` (a control frame the primary emits itself — it must be counted like any other, or `WAIT`'s solicitation silently shifts the stream), and a payload carrying binary data (`\r\n`, NUL) that a length-blind encoder would mis-measure. |
| Observable | The replica's `master_repl_offset` in `INFO replication` converges on exactly the primary's, and the `offset=` field the primary reports for `slave0` reaches the same number — not a value 20 bytes per frame lower. A `+CONTINUE` resume starts at exactly the byte the replica stopped at, so no write is replayed and none is skipped. `WAIT` is satisfiable at all: the primary compares an ACK to its own live head, and the comparison is only meaningful because both were counted with the same unit. |
| NOT observable | **An offset that includes the 20-byte `FRPL` header.** The bug shape is a fixed per-frame drift that nothing local detects: the replica's ACK reads low forever so `WAIT` never reaches quorum, and the reconnect's `PSYNC <id> <offset>` names a byte position inside the backlog that is not a frame boundary, so a granted `+CONTINUE` replays from the middle of a command. Near misses the mutation run will try: counting `frame.encoded_size()` instead of `payload.len()`; counting only the argument bytes and not the RESP framing around them; counting a `char` length instead of a byte length (which mangles any non-ASCII value); skipping the advance for the `GETACK` frame the primary injects (stamping it `sequence 0` and not recording it), which desynchronises everything after the first `WAIT`. |
| Invariant | One definition, one gate. `ReplicationFrame::stream_advance()` (`frame.rs:345`) is `self.payload.len()`, and it is the *only* place the unit is stated; `OffsetCoordinator::advance_unit` (`offset_coordinator.rs:94`) and `OffsetCoordinator::frame_advance` (`offset_coordinator.rs:124`) both route to it, so the primary's advance gate and the replica's ingest path cannot be changed independently. The frame's `sequence` field is the write's *end* offset, stamped by `broadcast_tagged` from the value `advance()` returned, so the number on the wire and the number in the backlog are the same number. `request_acks` pushes its `GETACK` through the identical advance-then-record path. The payload itself is produced by `serialize_command_to_resp`, which length-prefixes every argument with its byte count, making `payload.len()` a well-defined and binary-safe quantity. |
| Outcome variant | n/a (wire-level invariant; surfaces as `INFO replication` `master_repl_offset` divergence and a `WAIT` that can never be satisfied) |
| Forced by | `advance_and_frame_advance_agree_on_the_single_unit`, `broadcast_and_ingest_count_the_same_unit`, `offset_coordinator::tests::frame_advance_counts_payload_not_header`, `advance_is_cumulative_and_visible_via_current`, `test_serialize_command_to_resp`, `test_serialize_command_to_resp_binary_data`, `test_serialize_command_to_resp_no_args`, `test_serialize_command_to_resp_long_command_name`, `encoded_size_is_the_header_plus_the_payload_and_matches_the_encoding` |
| Bug refs | `.scratch/replication-cluster-rework/issues` (issue 03 — the epoch/offset fold rework this unit underpins) |

**Why one name is module-qualified.** `frame_advance_counts_payload_not_header` exists twice —
`replication/src/offset_coordinator.rs:265` and `replication/src/replica/offset.rs:606` — asserting
the same property on the two coordinators. This row means the former; the qualified form keeps that
unambiguous for a reader, while `just lint-spec` resolves on the leaf name either way.

---

## FM-REPLICATION-032 — a frame is decoded whole or not at all

| Field | Value |
|---|---|
| Trigger | TCP segmentation: a socket read that ends mid-header (fewer than 20 bytes of `FRPL` present) or mid-payload (header complete, payload short). Guaranteed for any value larger than the read reservation — a 1 MB `SET` spans many reads — and also arrives from the full-sync handoff, where the decode buffer is *seeded* with the residual bytes the payload reader over-read (FM-REPLICATION-005), so the first frame of a stream routinely starts mid-buffer. |
| Observable | The value arrives on the replica byte-identical, however the primary's writes were split across segments. The received head advances exactly once per frame, at the moment the last byte of that frame is in hand — never in installments. A buffer holding one and a half frames yields exactly one frame and keeps the half. |
| NOT observable | **A short payload handed to the applier as if it were complete** — the shape is a truncated RESP command that then fails to parse, gets counted and stepped over (`apply.rs:328`), and so is *silently dropped while the offset still advances*: a lost write on a replica that keeps ACKing and reports itself in sync. **The header re-parsed from bytes that were already consumed**: `src.advance(FRAME_HEADER_SIZE)` happens inside the `ReadingHeader` arm (`frame.rs:442`), so a state machine that forgot its position would read the *payload's* first four bytes as magic — an immediate `invalid frame magic` and a dropped link on every large value. **The offset advancing for a frame that has not fully arrived**, i.e. `frame_advance` called anywhere but on a `Some(frame)`. |
| Invariant | `ReplicationFrameCodec` is a two-state machine held across `decode` calls (`DecodeState::{ReadingHeader, ReadingPayload}`, `frame.rs:382`). The header is consumed exactly once and its five fields are carried *in the state*, so resumption never re-reads it; the payload arm returns `Ok(None)` without consuming while `src.len() < *length` (`frame.rs:462`), so the parked bytes stay in the buffer for the next read. `drain_frames` (`replica/streaming.rs:173`) calls `self.offsets.frame_advance(&frame)` only on the `Some(frame)` branch of `while let`, so an incomplete frame cannot move a head. Because the length is in the header, the frame boundary is known before the payload is trusted — the decoder never has to scan for a delimiter inside attacker- or value-controlled bytes. |
| Outcome variant | n/a (wire-level invariant; surfaces as truncated values on the replica or a link that drops on every large write) |
| Forced by | `test_codec_partial_decode`, `test_codec_decode`, `test_frame_shard_id_round_trips`, `test_frame_decode_insufficient_data`, `decode_accepts_only_this_builds_frame_version`, `decode_refuses_a_claimed_length_above_the_ceiling_but_not_at_it`, `decode_takes_exactly_the_payload_its_header_claims`, `codec_yields_a_frame_the_moment_its_last_byte_arrives`, `codec_accepts_only_this_builds_frame_version`, `codec_refuses_a_claimed_length_above_the_ceiling_but_not_at_it`, `codec_round_trips_a_payload_longer_than_the_header`, `test_frame_decode_invalid_magic`, `codec_refuses_a_header_whose_magic_is_not_frpl` |
| Bug refs | none |

**Now covered, and how.** The decoder's *rejection* ladder — bad magic, unsupported version,
oversized length — was originally filed as a gap on the streaming path: the one-shot
`ReplicationFrame::decode` had tests, `ReplicationFrameCodec` had none, and the two are separate
implementations of the same three checks that could therefore disagree about which frames a link
carries. All three rungs are now forced on **both** paths, deliberately in mirrored pairs
(`decode_*` against `codec_*`), so a change to one that the other does not follow
fails a test rather than splitting the wire contract in half. The length gate is
additionally pinned at its boundary — accepted *at* the ceiling, refused one past it — because an
off-by-one in either direction is the mutation this row exists to catch.

**The version gate is equality, not a ceiling.** A build has exactly one header layout, so a frame
stamped with any other version cannot be parsed by it — older is not "compatible", it is a
different layout. Version 1 carried no `shard_id`, so a v1 frame read under today's 20-byte header
yields a bogus shard id *and* a bogus sequence, and the sequence is the replication offset: a
silently mis-parsed frame is a mis-routed write at a fabricated offset, which is strictly worse
than a refused link. Both decoders refuse `FRAME_VERSION - 1` and `FRAME_VERSION + 1` and accept
only their own. The cost is that two builds with different frame versions cannot replicate to each
other at all, which is the honest statement of what the wire format guarantees today.

---

## FM-REPLICATION-033 — the ACK grammar is bound to its parser, and a parse consumes exactly its frame

| Field | Value |
|---|---|
| Trigger | Every ACK the replica sends (spontaneous cadence tick and solicited answer alike) and every `REPLCONF GETACK *` the primary sends for `WAIT`. Sharpened by the primary's read side, which parses ACKs out of a *streaming* buffer that may hold zero, one, a partial, or several concatenated ACKs; and by `u64::MAX`, whose 20-digit decimal form is the boundary a hand-rolled length prefix gets wrong. |
| Observable | `WAIT` returns the true number of replicas at or past the offset, promptly, over an arbitrarily long-lived link. An offset acked at any magnitude up to `u64::MAX` is read back as the same number. A primary that receives two ACKs in one read counts the *later* one and does not stall holding the second. A `GETACK` is never mistaken for an `ACK` (which would credit the replica with offset 0) and an `ACK` is never mistaken for a `GETACK` (which would make the primary answer its own solicitation). |
| NOT observable | **A `consumed` count that is not exactly the parsed frame's length.** Under-report and the primary re-parses the tail of the frame it just handled — a garbage ACK or a permanently stuck buffer; over-report and it eats the head of the next ACK, so `WAIT` stops seeing acknowledgements from a live replica and times out. Also: a partial ACK treated as complete (yielding a truncated, therefore *lower* offset, which makes `WAIT` under-count and a promoted node under-vouch); a `$<len>` prefix computed from anything but the byte length of the decimal offset, which fails first at 20 digits; case-sensitive subcommand matching, which drops ACKs from any peer that lowercases. |
| Invariant | `ReplconfCodec` (`frame.rs:87`) owns both directions of the control grammar in one place — `encode_ack`/`parse_ack`, `encode_getack`/`is_getack` — so an encoder cannot drift from its inverse. `parse_ack` returns `(offset, consumed)` rather than just the offset, making the byte count the caller advances by a value the parser computes, not one the caller re-derives; it returns `None` (never a partial parse) for any buffer that does not hold a whole frame. The tests bind each encoder to its own output rather than to a re-typed literal, and additionally pin the literal wire bytes, so agreeing-with-yourself is not enough to pass. |
| Outcome variant | n/a (wire-level invariant; surfaces as `WAIT` timing out against a healthy replica) |
| Forced by | `replconf_ack_round_trips`, `replconf_getack_round_trips`, `replconf_cross_discriminator_rejection`, `replconf_parse_ack_streaming_invariants`, `replconf_parse_ack_rejects_wrong_command`, `replconf_is_getack_recognizes_variants_and_rejects_others`, `replconf_parse_ack_checks_the_command_and_the_subcommand`, `replconf_is_getack_decides_on_the_command_and_subcommand_tokens_alone`, `replconf_is_getack_rejects_a_frame_cut_inside_the_command_name`, `replconf_is_getack_rejects_a_six_byte_subcommand_that_is_not_getack` |
| Bug refs | none |

---

## FM-REPLICATION-034 — a replicated transaction is one shard-tagged group, closed only by its own EXEC

| Field | Value |
|---|---|
| Trigger | A `MULTI/EXEC` executed on the primary and framed by `broadcast_transaction_on_shard` (`lib.rs:123`). Sharpened by a keyless command inside the group (nothing in `args[0]` to route on), by a group whose `EXEC` never arrives because the link dropped, and by the malformed shapes a stream can present after a reconnect: a `MULTI` while one is already open, an `EXEC` with none open. |
| Observable | The replica applies the inner commands as **one** `apply_group` on the shard the primary executed them on — a client reading the replica never sees a half-applied transaction, and never sees the group land on a shard chosen by re-deriving routing from the first argument. The literal `MULTI` and `EXEC` frames are never themselves routed to a shard. A group whose `EXEC` never arrives applies nothing *and* claims nothing until it does, so the applied head does not credit an uncommitted group. |
| NOT observable | **The group split into per-command applies**, which makes intermediate transaction state readable on the replica and breaks the atomicity the primary already granted. **A frame routed by `args[0]` instead of by its tag** — the pre-tag bug: keyless commands and the `MULTI`/`EXEC` frames themselves all collapse onto shard 0, so a multi-shard replica diverges permanently while reporting itself in sync. **An interrupted group's bytes credited to the applied head**, which makes the node ACK — and once promoted, vouch for — writes no shard ever saw. **A group closed by the wrong `EXEC`**: an `EXEC` arriving with no open group must not close some earlier group, and the malformed shapes must still claim their bytes, because bytes that reach no shard are still bytes the primary counted. |
| Invariant | The origin shard travels **in the frame** (`ReplicationFrame::new_on_shard`, `shard_id: u16`, `frame.rs`), stamped by `broadcast_tagged` from the shard that executed the write and recorded into the backlog alongside it so a `+CONTINUE` replay re-tags identically; `CONTROL_SHARD` (`u16::MAX`) marks frames that are never routed at all. `ReplicationBroadcaster::broadcast_transaction_on_shard` is the single definition of the framing — `MULTI`, each command, `EXEC`, all on one shard id — so there is no second place a group's shape can be constructed. On the replica, `PendingTxn` (`apply.rs:137`) captures `shard_id` and `epoch` at the `MULTI` and accumulates `bytes`; the whole span is claimed in one `claim_or_stop!(txn.bytes + frame_bytes)` at the `EXEC` (`apply.rs:408`), before the dispatch and never after, which is what makes the promotion boundary exact for a group in flight. |
| Outcome variant | n/a (internal invariant; surfaces as a diverged replica keyspace and readable half-transactions) |
| Forced by | `transaction_group_applied_atomically_on_tagged_shard`, `an_interrupted_transaction_credits_nothing`, `replconf_is_skipped_and_not_routed`, `test_frame_shard_id_round_trips`, `a_reconstructed_transaction_is_one_atomic_shard_message` |
| Bug refs | `.scratch/replication-cluster-rework/issues` (issue 06 — the origin-shard tag this row pins) |

**Not covered here.** The malformed shapes named in "NOT observable" — a nested `MULTI`
(`apply.rs:381-389`) and an `EXEC` with no open group (`apply.rs:416-421`) — are implemented and
correct but have no forcing test; they are filed as gaps rather than claimed here. The growth of an
open group is bounded by FM-REPLICATION-045, which also owns the disposition a breach takes.

---

## FM-REPLICATION-035 — the checkpoint envelope's length prefixes are bounded before they allocate, and truncation is always an error

| Field | Value |
|---|---|
| Trigger | Reading any part of a `$FROGDB_CHECKPOINT` / `$FROGDB_SNAPSHOT` envelope from a primary that is buggy, mid-crash, or hostile: a non-numeric or absurd file count, a `$<len>` name or metadata prefix beyond the sane bound, a name or metadata body cut short by a closed connection, a trailer with the wrong number of `:`-separated fields. |
| Observable | Every malformed shape is a clean `io::Error` that fails the sync — `InvalidData` for a length or shape that cannot be trusted, `UnexpectedEof` for one that overruns the bytes that actually arrived — after which the replica retries on its normal reconnect backoff, still serving its previous keyspace. A well-formed envelope round-trips field-for-field, including the zero-file case, and its byte shape is fixed: `$<MARKER>\r\n<count>\r\n`, then per file `$<name_len>\r\n<name>\r\n$<size>\r\n<size bytes>`, then `$<len>\r\n<rdb_size>:<checksum_hex>:<replication_id>:<replication_offset>\r\n`. |
| NOT observable | **A length prefix driving an allocation before it is bounded** — `$99999999` on the name field reserving hundreds of megabytes, or a claimed file count pre-reserving a million entries: a one-line remote OOM from a peer that has not authenticated anything beyond reaching the port. The bound is checked *before* the `vec![0u8; n + 2]`, and moving the check after it is exactly the mutation this row exists to kill. **A truncated header or trailer accepted with whatever bytes arrived**, which stages a checkpoint whose file list or replid/offset disagrees with the primary's — the worst case, since the stage then verifies and installs. **Encoder and decoder drifting together**: a round-trip test alone passes by agreeing with itself, which is why the golden-bytes assertion pins the literal wire form. |
| Invariant | `CheckpointStreamCodec` owns both directions of the grammar, so an encoder change that is not matched by its inverse fails the round-trip. `parse_dollar_len(line, ctx, max)` (`fullsync.rs:351`) is the one length parser — `MAX_CHECKPOINT_NAME_LEN` and `MAX_CHECKPOINT_METADATA_LEN`, both 64 KiB — and `parse_file_count` the one count parser (`MAX_CHECKPOINT_FILE_COUNT`, 1e6); both reject non-numeric input and over-bound values before any buffer is sized from them. Every body read is `read_exact(n + 2)`, so a stream that ends early is `UnexpectedEof` rather than a short body silently accepted. The trailer is split on `:` and rejected unless it yields exactly four fields with a 32-byte checksum. Markers themselves are FM-REPLICATION-001's; this row owns everything downstream of them. |
| Outcome variant | `io::ErrorKind::{InvalidData, UnexpectedEof}` on the sync |
| Forced by | `test_checkpoint_codec_round_trip`, `test_checkpoint_codec_golden_bytes`, `test_checkpoint_codec_zero_file_prelude`, `test_parse_file_count_rejects_garbage`, `test_read_prelude_rejects_bad_marker`, `test_read_file_header_non_numeric_len`, `test_read_file_header_oversized_name_len`, `test_read_file_header_truncated_name`, `test_read_metadata_wrong_field_count`, `test_read_metadata_oversized_len`, `test_read_metadata_truncated_body`, `prop_file_header_sequence_round_trips` |
| Bug refs | — (the name's *contents* are FM-REPLICATION-044's; this row bounds only its length) |

**Bounded is not validated.** This row covers the length prefixes and the truncation behaviour. It
deliberately does **not** claim that a file name is safe to use as a path — that is
FM-REPLICATION-044, which validates the name's shape at the same codec boundary, immediately after
the length bound this row pins.

---

## FM-REPLICATION-036 — a partially received checkpoint is never staged, and a staged one is never partial

| Field | Value |
|---|---|
| Trigger | A full-sync checkpoint transfer that ends before its last byte — the primary crashes, the connection is cut, a file is short — or one whose combined checksum does not match the trailer, or one whose commit rename fails (a cross-device staging dir, a permissions change). Also the ordinary case of a stale staged dir left behind by an earlier sync that was never consumed. |
| Observable | Nothing appears under the staged-checkpoint dir: the scratch dir is scrubbed, the sync errors, and the replica retries on its reconnect backoff while still serving its previous keyspace. A stage that *does* commit is complete — every file the primary framed, plus a `replication_metadata.json` naming the replid and offset it corresponds to — so writes the primary had acknowledged but not yet flushed are present on the replica after the sync. An envelope carrying zero files still produces the scratch dir, so a legitimately empty checkpoint commits rather than failing on a missing rename source. |
| NOT observable | **A staged dir holding a prefix of the primary's files.** This is the whole row: the boot-time installer (FM-PERSISTENCE-027, -038, -039) opens whatever is staged as a complete database and adopts the replid/offset stamped beside it, so a torn stage is a silently truncated keyspace that reports itself fully in sync at the primary's offset — undetectable from either node. Its near misses: the previous staged checkpoint deleted *before* the new one is known to verify (a mismatch then leaves the node with neither); a checksum mismatch still returning a `StagedOutcome`; files written directly into the staged dir instead of the scratch dir, which makes every partial transfer a partial stage; `receive_to_file` treating a zero-length read as end-of-file and returning the bytes it happened to get. |
| Invariant | Ordering, and a single commit point. Files land in `<data-dir>/staging.incoming` (`DataDirLayout::staging_incoming_dir`, inside the data directory like every other install scratch path — FM-PERSISTENCE-057), never in the staged dir. `CheckpointStager::commit` verifies `computed == meta.checksum` **first**; on mismatch it `remove_dir_all`s the scratch dir and returns `InvalidData` without having touched the staged dir at all. Only after verification does it remove any stale staged dir and perform `fs::rename(incoming -> staged.dir())`, which is *the writer's commit point* in the `StagedCheckpoint` contract and is atomic on one filesystem — there is no window in which the staged dir holds a partial set. A failed rename scrubs the scratch dir and errors. `receive_to_file` loops to the header's `expected_size` and converts a `read` returning 0 into `UnexpectedEof`, so a connection that dies mid-file fails the whole receive rather than producing a short file that would then be checksummed as-is. The module contract is explicit that `commit` *stages* and does not install — the boot-time half is the adjacent spec's. Two failures are deliberately non-fatal and must stay that way: removing a stale staged dir, and stamping `replication_metadata.json` — the stage is already durable at the rename, and the offset is adopted from the sync regardless. |
| Outcome variant | `io::ErrorKind::InvalidData` (checksum), `io::ErrorKind::UnexpectedEof` (truncation), `io::Error::other` (rename) — all failing the sync |
| Forced by | `stager_commit_stages_and_stamps_metadata`, `stager_commit_checksum_mismatch_cleans_up`, `stager_commit_removes_stale_staged`, `stager_commit_metadata_write_failure_is_non_fatal`, `receiver_reads_framed_files_and_metadata`, `receiver_creates_incoming_for_zero_files`, `receiver_truncated_stream_yields_unexpected_eof`, `test_full_resync_checkpoint_carries_writes_still_pending_in_the_wal` |
| Bug refs | none |

**Deliberately not resumable.** An interrupted transfer starts over rather than continuing from the
bytes already staged. Redis does not resume an RDB transfer either, and resuming would require the
scratch dir to be trusted across a reconnect — exactly the state this row exists to forbid.

---

## FM-REPLICATION-037 — WAIT answers with a count, blocks, or refuses; it never invents a number

| Field | Value |
|---|---|
| Trigger | `WAIT numreplicas timeout` on a primary, across every shape the reply branches on: a quorum already satisfied at arrival (including the degenerate `numreplicas 0`); a quorum reachable only after ACKs come back; a quorum that is *unreachable* (`numreplicas` above the number of attached replicas, or zero replicas at all); `timeout 0`; a malformed argument pair; and the same command on a replica, standalone or cluster. |
| Observable | An integer: the number of streaming replicas that have acknowledged at or past the offset the primary was at when `WAIT` arrived. An already-satisfied quorum returns it with **no round trip and no `GETACK` on the wire** — including `WAIT 0 t`, which reports the live acked count rather than a constant 0. A quorum that is not yet met blocks and returns the moment it is. `timeout 0` has no deadline: it blocks until the quorum is reached or the client is released (`CLIENT UNBLOCK`, or a demotion — FM-REPLICATION-040). A non-zero timeout returns the count acked **at the deadline**, which may be less than `numreplicas` and may be 0. An unreachable `numreplicas` blocks for the whole timeout and then answers with the replicas that did ack. `WAIT` on a replica is `ERR WAIT cannot be used with replica instances` *before* argument parsing, so a replica rejects `WAIT garbage garbage` with the role error, not a parse error. `WAIT` takes no key, so in cluster mode it never `MOVED`s — the same connection that gets redirected for a keyed command gets an integer for `WAIT`. Inside `MULTI`/Lua (deny-blocking) it returns the count for the *current* live offset immediately, never `nil` and never a block. |
| NOT observable | **A `WAIT` that returns before its deadline with fewer than `numreplicas`** — Dragonfly's "every currently-tracked replica has acked, give up early" shortcut, explicitly rejected: a replica may be mid-attach, so early-exiting answers a question the client did not ask. **`WAIT n 0` returning instead of blocking** — the shape that appears when `timeout == 0` is folded into the deadline arm and converts to an already-elapsed instant; the sibling bug is deriving the deadline from `std::time::Instant` instead of `tokio::time::Instant`, which returns instantly under `tokio::time::pause()` and under turmoil (the simulated clock runs ahead of real time) while looking correct in production. **`WAIT 0 t` blocking, or answering a hardcoded 0** rather than the live count. **A `-MOVED` for `WAIT`**, or a cluster-mode special case that fans out to other shards and reports somebody else's replicas. **A count reported after a timeout that was recomputed against a *newer* offset** — re-snapshotting the target on the timeout path would count replicas as caught-up on writes that landed after the `WAIT` started. **A count that includes a replica which never acknowledged the target from the wire** — the primary's own record of where a replica *resumed* (the backlog-tail seed at the top of streaming, or a checkpoint's `snapshot_offset`) describes bytes the primary sent, not bytes the replica decoded and applied, so crediting it makes `WAIT 1` answer 1 against a replica whose keyspace is still empty; the credit has to come off the wire or `WAIT` is reporting the sender's optimism as the receiver's durability (issue 28, and FM-REPLICATION-039's invariant is where the two positions are kept apart). |
| Invariant | One decision, one place: `WaitCoordinator::wait_for_replicas` mirrors Redis's `waitCommand` as snapshot -> immediate check -> solicit-once -> quorum-or-deadline, and the target is snapshotted exactly once (`target_offset()` = `OffsetCoordinator::current()`) and threaded through every arm, including `WaitVerdict::TimedOut(self.count_acked(target))`. `deadline` is `Option<Instant>` built at the call site as `(timeout_ms > 0).then(...)`, so `timeout 0` is structurally the no-deadline `select!` arm rather than a zero duration; the instant is `tokio::time::Instant` and is handed to `tokio::time::timeout_at`, which is what makes the turmoil sims a real clock test rather than a no-op (the import carries that reasoning at its declaration). The fast path `count >= num_replicas` covers `numreplicas 0` by arithmetic instead of by a special case, and returns `Reached(count)` — the *actual* count, which Redis also lets exceed `numreplicas`. The unreachable-quorum case needs no code: nothing in the blocking arm can complete it, so only the deadline or the fence can. The replica refusal is duplicated deliberately at both entry points — `handle_wait_command` checks `is_replica` before `parse_wait_args`, and `WaitCommand::execute` repeats it for the shard/deny-blocking path — because Redis rejects before parsing and a client must not learn its arguments are bad from a node that would have refused anyway. Keylessness is `KeySpec::None` + `Arity::Fixed(2)` on the spec, which is what the redirect layer consults, so "never redirects" is a property of the spec rather than of the handler. |
| Catalog | `INV-OFFSET-3` — the count `WAIT` may answer is bounded by what a replica can have acked: never past the live head, and never at all before its session reached the wire. |
| Outcome variant | `WaitVerdict::{Reached, TimedOut}`; `Response::Integer`; `ERR WAIT cannot be used with replica instances` |
| Forced by | `quorum_already_met_returns_without_soliciting`, `numreplicas_zero_returns_actual_acked_count`, `timeout_returns_count_acked_at_target`, `no_deadline_blocks_until_quorum`, `target_offset_reads_the_offset_coordinator`, `a_replica_credited_only_by_a_resume_seed_does_not_satisfy_the_quorum`, `test_wait_during_replica_resync`, `test_wait_no_replicas`, `test_wait_zero_numreplicas`, `test_wait_zero_timeout_blocks_without_quorum`, `test_wait_zero_timeout_blocks_until_ack`, `test_wait_invalid_args`, `test_wait_on_replica_is_an_error`, `test_client_unblock_releases_wait`, `test_wait_numreplicas_exceeds_actual_blocks_to_timeout`, `test_wait_never_redirects_in_cluster`, `test_cluster_wait_degrades_under_partition`, `test_wait_blocks_until_ack` |
| Bug refs | `.scratch/replication-cluster-rework/wait-cluster-mode.md` (the contract table Q1-Q9 and risks R1-R9 this row is the outcome of); `.scratch/testing-improvements/issues/37` (its origin — the INFO/WAIT probe that found cluster WAIT unspecified) |
| Model | `replication_feed_gate.qnt::inv_ack_never_above_live` |

**Deliberate divergence: the target offset.** Redis snapshots `c->woff`, the offset right after
*this connection's* last write, so one client's `WAIT` is never delayed by another client's later
writes. FrogDB has no per-connection write offset (writes are stamped on the shard tasks, not on the
connection), so `target_offset()` is the global live offset — the same call Dragonfly made, for the
same thread-per-shard reason. It is strictly conservative: the target is always >= the client's own
last write, so `WAIT` can over-wait but can never report a replica as caught up on a write it has
not acked. The cost is latency under a mixed write load, not correctness.

---

## FM-REPLICATION-038 — WAIT solicits exactly one GETACK round, and only when one would be answered

| Field | Value |
|---|---|
| Trigger | A `WAIT` that must block: the quorum was not already met. Two shapes matter — at least one replica is in the streaming phase (solicitation would be answered), and none is (the primary is replica-less, or every session is still handshaking/syncing). Sharpened by a long block: nothing re-solicits during it. |
| Observable | A blocking `WAIT` with a live replica resolves in milliseconds, not on the replica's spontaneous ACK cadence (`replication.ack-interval-ms`, default 1000 ms — Redis's `repl-ping-replica-period` analogue): eight consecutive write+`WAIT 1 2000` rounds complete well inside two seconds, where the cadence alone would average ~4 s. On a primary with no streaming replica, `WAIT` blocks to its deadline **without `master_repl_offset` moving**: `INFO replication`'s offset is unchanged by the wait. |
| NOT observable | **`master_repl_offset` growing on a primary that took no writes** — `REPLCONF GETACK *` is part of the offset-stamped command stream, so soliciting on a replica-less primary inflates the replication offset for nothing; the visible damage is a replica that later attaches being granted a `+FULLRESYNC` at an offset covering bytes that were never data, and `INFO`/`WAIT` on a cache-mode node drifting upward while idle. **A GETACK per waiter, or a re-solicit loop while parked** — N blocked clients producing N broadcast writes, which is the same offset inflation multiplied by the wait fan-out, plus stream bandwidth stolen from the replicas the wait is about. **A GETACK sent on the fast path**, i.e. before the already-satisfied check: it would turn every satisfied `WAIT` into a wire round trip and defeat the whole point of the immediate check. |
| Invariant | `wait_for_replicas` calls `solicitor.solicit_acks()` exactly once, *after* the fast-path return and *before* subscribing to the quorum future, gated on `self.tracker.replica_count() > 0` — and `replica_count()` counts sessions in `Phase::Streaming` only, so a replica still in `Connecting`/`Syncing` does not authorize the write. There is no periodic re-solicit by design: replicas answer `GETACK` immediately (FM-REPLICATION-006 keeps the answer off the decode loop's critical path) and spontaneously ACK on cadence, which also bounds the ACK latency of a replica that attaches mid-wait. The solicitation edge is a trait (`AckSolicitor`) with `PrimaryReplicationHandler` as the only production impl, so the "how many rounds" property is unit-testable without a socket — a mock counts invocations. Redis parity: `replicationRequestAckFromSlaves` sets a flag that `beforeSleep` turns into one broadcast per event-loop iteration, and Redis likewise skips the stream write when no replica would consume it. |
| Outcome variant | n/a (wire-level invariant; surfaces as `WAIT` latency and as an inflated `master_repl_offset`) |
| Forced by | `blocking_wait_solicits_exactly_once_then_reaches_quorum`, `no_streaming_replicas_means_no_solicitation`, `quorum_already_met_returns_without_soliciting`, `test_wait_returns_promptly_via_getack` |
| Bug refs | `.scratch/replication-cluster-rework/wait-cluster-mode.md` §7 (solicitation policy: Redis parity chosen, Dragonfly early-exit rejected) |

---

## FM-REPLICATION-039 — the count is a set of distinct streaming replicas at or past the target

| Field | Value |
|---|---|
| Trigger | Any `count_acked(target)` read — the fast path, the timeout path, the deny-blocking `MULTI`/Lua path, and every wake of the blocked quorum loop. The interleavings that matter: a replica ACKing the same offset twice; a replica ACKing an *older* offset than one it already claimed; a replica that dies mid-wait; a replica that attaches and reaches streaming mid-wait; a replica that reconnects via partial resync already at or past the target; a replica still in `Syncing` while another streams; and, in cluster mode, replicas attached to a *different* shard's primary. |
| Observable | The returned count never exceeds the number of replicas currently in the streaming phase, and every replica it counts has acknowledged an offset >= the target. Three live replicas ACKing give 3; kill one and the next `WAIT 3` returns <= 2. A replica that dies mid-wait stops counting, and a `WAIT 1 t` issued after the loss returns exactly 0. A replica that attaches mid-wait can *satisfy* the wait once it reaches streaming and acks. A replica that has only just entered the streaming phase counts for nothing until it ACKs from the wire — whether it resumed by partial resync or by installing a checkpoint, the primary's record of where it resumed does not answer a `WAIT`, so a `WAIT 1` issued against a replica whose keyspace is still empty returns 0 and converges to 1 on that replica's first ACK at or past the target (one spontaneous-ACK cadence at worst, per FM-REPLICATION-038). In cluster mode with two shards of one replica each, `WAIT 1` on either primary answers 1 — never 2. |
| NOT observable | **A count larger than the number of connected replicas.** The concrete shapes: counting the same session twice because the acked-offset projection was rebuilt from a `Vec` that a reconnect appended to instead of a keyed registry; counting a `Syncing` session that holds no data yet (which would report `WAIT 1 = 1` against a replica whose keyspace is still the payload it has not installed — FM-REPLICATION-001's bug arriving from the primary side); counting a session whose socket is gone because unregistration is deferred to a reaper; **crediting a replica with an offset the primary supplied rather than one the replica sent** — the resume seed written when a session enters streaming, folded into the same monotonic atomic the wire ACKs advance, which made every freshly-streaming replica count at the primary's live offset the instant it was handed the backlog tail (issue 28: `WAIT 1` answered 1 against an empty replica keyspace, and `state=online` at `offset=<the primary's own offset>` said the same thing to `INFO`); or an ACK regressing the count by moving a replica's stored offset *backwards* (a duplicate/reordered `REPLCONF ACK` on the wire), which would make an already-returned `WAIT` unrepeatable. **`WAIT` and `ROLE`/`INFO` disagreeing about the replica set** — two projections computed from the same registry with different phase filters. **A cluster fan-out**: counting the whole cluster's replicas would tell a client its write is durable on nodes that never saw it. |
| Invariant | One projection: `ReplicationTrackerImpl::get_streaming_replicas()` filters the registry to `Phase::Streaming` and snapshots each session, and `count_acked`, `count_good_replicas`, `min_acked_offset` and ROLE's replica listing are all derived from it — "which replicas count and what have they acknowledged" has exactly one definition. The registry is `RwLock<HashMap<u64, Arc<ReplicaSession>>>` keyed by a monotonically allocated id, so iteration yields one entry per live session by construction; a reconnect gets a **new** id and the old session removes itself through its own exit handler (`unregister_replica`), which is why double-counting is a data-structure property rather than a check. Per-replica monotonicity is `ReplicaSession::record_ack`: it stores only when `sequence > prev`, returns whether it advanced, and always refreshes `last_ack_time` (any ACK proves liveness, even on an idle primary) — so a stale or duplicate ACK is liveness-only and cannot move a count. **The wire ACK is the only writer of `acked_offset`.** Where a replica *resumed* is the primary's own bookkeeping and lives in a second monotonic atomic, `resume_offset`, written only by `seed_resume_position`: the two are different facts (what the primary sent vs. what the replica applied and said so) and folding them into one field is what let the sender's optimism answer a durability question (issue 28). Only the byte-lag measure reads them together, as `stream_position() = max(acked_offset, resume_offset)` — "how far behind the stream is this link", where a resume genuinely closes the gap — while `count_acked`, `min_acked_offset`, `INFO`'s `slaveN:offset=` and `ROLE`'s replica offset read `acked_offset` alone, which is exactly Redis's `repl_ack_off` (moved only by `replconfCommand`'s ACK branch, and the only thing `replicationCountAcksByOffset` compares). Seeding therefore notifies no waiter — it cannot change a count, so a notification would only spin the wait loop — but it still refreshes `last_ack_time`, because the lag *clock* must restart where streaming restarted (FM-REPLICATION-043). Waking is `broadcast::Sender<(u64, u64)>` (capacity 1024): `wait_for_acks` re-reads `count_acked` before every `recv`, treats `Lagged` as "continue" (the count is re-derived from the registry, never accumulated from the messages) and `Closed` as "return the current count", so no notification is load-bearing. Cluster containment is free: a node's PSYNC replicas *are* its own shard's replica set (Raft carries metadata only, ADR-0001), so there is no cluster branch to get wrong. |
| Catalog | `INV-SESSION-2`, `INV-OFFSET-3` — distinctness is what makes the count a set. Two live sessions for one announced identity would count a single follower twice (GAP-5), and no session may be credited past the live head or before it streamed. |
| Outcome variant | n/a (surfaces as `WAIT`'s integer and as `INFO replication`'s `connected_slaves`) |
| Forced by | `acks_below_the_target_do_not_count`, `a_replica_that_attaches_mid_wait_can_satisfy_the_quorum`, `a_replica_that_detaches_mid_wait_stops_counting`, `record_ack_is_monotonic_and_refreshes_liveness`, `test_get_streaming_replicas`, `test_record_ack`, `test_wait_for_acks_immediate`, `test_wait_for_acks_with_timeout`, `a_resume_seed_never_moves_the_wire_acked_offset`, `seed_resume_position_advances_only_strictly_forward`, `wait_does_not_count_a_seeded_position_that_was_never_acked`, `a_resume_seed_does_not_wake_a_blocked_wait`, `min_acked_offset_ignores_a_resume_seed`, `seed_replica_position_does_not_satisfy_wait`, `test_min_acked_offset`, `test_wait_with_disconnected_replica`, `test_wait_returns_correct_count_with_partial_ack`, `test_wait_multiple_replicas`, `test_wait_never_exceeds_connected_slaves`, `test_wait_in_cluster_counts_shard_replicas`, `test_wait_does_not_count_other_shards_replicas`, `test_wait_ignores_slot_migration_target` |
| Bug refs | `.scratch/replication-cluster-rework/wait-cluster-mode.md` §4.0 Q3/Q5 (per-node, per-shard counting; no server-side fan-out); `.scratch/testing-improvements-round2/issues/76` (what an ACK means, the other half of this count — FM-REPLICATION-008) |
| Model | `replication_feed_gate.qnt::inv_duplicate_identity_is_always_kickable`, `replication_fullsync.qnt::inv_one_session_per_announced_identity` |

---

## FM-REPLICATION-040 — a WAIT parked across a demotion returns an error, never a count

| Field | Value |
|---|---|
| Trigger | A node that stops being a primary while a `WAIT` is parked on it: `REPLICAOF host port`, a cluster `CLUSTER FAILOVER` takeover, or any promotion bridge that ends the primary stint. Four interleavings: the demotion lands while the wait is blocked; it lands in the window *between* the handler's role check and the coordinator's subscription; it landed on an *earlier* stint, before this wait started; and it lands in the *same poll* that also reaches quorum — e.g. the GETACK round the solicitation sends is itself what satisfies the target, racing a demotion landing at the same instant. |
| Observable | The parked client gets `-UNBLOCKED force unblock from blocking operation, instance state changed (master -> replica?)` — an error naming the cause the way Redis does — not an integer. Every later `WAIT` on that node is refused with the replica error (FM-REPLICATION-037). The node that was promoted serves `WAIT` as the shard's new primary, counting the replicas that reattach to it. A wait started *after* an earlier demotion/promotion round trip is not released by that old fence. An exact tie between quorum and the fence — both ready on the same `select!` poll — also resolves to the error, never the count. |
| NOT observable | **A count returned across a role change.** It would describe acknowledgments on a replication stream the node no longer heads: the client reads "2 replicas have my write" about a history that a new primary has already forked away from, which is exactly the false-durability claim `WAIT` exists to prevent — including the degenerate case where quorum and the fence become ready in the same poll, which an unbiased `select!` would resolve to `Reached` roughly half the time. **A wait parked forever on a demoted node** — the shape that appears when the role fence is subscribed *after* the role check: the demotion publishes the replica flag first and bumps the fence second, so a subscriber created in between sees neither, and the client hangs until it disconnects while every other `WAIT` on the node is refused. **A stale fence releasing a fresh wait**: a `watch` sender reset to a sentinel, or a boolean flag instead of a counter, makes a node that was demoted-and-repromoted release the next stint's waits immediately with a spurious error. |
| Invariant | The fence is a `tokio::sync::watch::Sender<u64>` **counter**, not a flag — a node can be demoted, promoted and demoted again while one wait is parked, and a subscriber only ever needs "did it change since I started", which a counter answers and a boolean does not. `RoleFence` is taken by the caller *before* it concludes the node is still a primary (`blocking.rs`: `role_fence()` then a second `is_replica` read), so one of the two observations must see a racing demotion; the ordering requirement is documented at `RoleFence` itself. `fence_role_change()` is called from `PrimaryReplicationHandler::end_primary_stint`, next to the downstream `disconnect_all_replicas` it belongs with, so the release and the teardown cannot drift apart. In `wait_for_replicas` the fence is a `select!` arm in *both* the deadline and the no-deadline shapes, producing `WaitVerdict::RoleChanged(count)`; the count travels with the verdict for observability but the caller is required to discard it and reply with the error — which is why `RoleChanged` is a distinct variant rather than a `TimedOut` with a flag. Both `select!`s are `biased;` with the fence arm listed first, so an exact tie is not left to `tokio::select!`'s (unseeded, and therefore also non-reproducible under `turmoil`) random tie-break — determinism-audit R7/A53. A dropped sender resolves the fence too: the coordinator that owned the stream is gone. |
| Outcome variant | `WaitVerdict::RoleChanged`; `-UNBLOCKED ... (master -> replica?)` |
| Forced by | `role_change_releases_a_wait_parked_forever`, `role_change_releases_a_wait_with_a_deadline`, `a_fence_from_an_earlier_stint_does_not_release_a_later_wait`, `a_demotion_racing_the_role_check_still_releases_the_wait`, `a_tie_between_quorum_and_role_change_favors_role_changed_no_deadline`, `a_tie_between_quorum_and_role_change_favors_role_changed_with_deadline`, `wait_released_by_a_demotion_reports_the_role_change_even_if_client_unblock_races`, `test_wait_unblocked_on_demotion`, `test_wait_unblocked_on_cluster_demotion`, `test_wait_rejected_on_cluster_replica`, `test_wait_is_served_by_a_promoted_cluster_primary`, `test_failover_chain_survivor_reattaches_to_promoted_node`, `test_cluster_wait_unblocked_across_failover` |
| Bug refs | `.scratch/replication-cluster-rework/wait-cluster-mode.md` §5.1 R4/R7 and §7 (the `-UNBLOCKED` wording and the fence-before-role-check ordering are both recorded decisions) |

---

## FM-REPLICATION-041 — the replica-loss self-fence refuses writes only after a replica has actually streamed, and only while one was actually lost

| Field | Value |
|---|---|
| Trigger | `replication.self-fence-on-replica-loss` enabled on a primary, and the replica set going empty or stale — a replica that streamed and then died, a replica that stopped ACKing within `replication.replica-freshness-timeout-ms`, or a primary that never had a replica at all. Also the two ways a streaming replica can leave (FM-REPLICATION-062): **lost** — a read/write error, a write timeout, a lag disconnect, a receiver that fell behind the WAL broadcast, or a link that simply stopped ACKing — and **graceful** — an orderly EOF from the replica's own half of the link, or a teardown the primary itself asked for. Also the toggles: enabling the fence on a primary that already served replicas, retuning the freshness window live, and a demotion followed by a re-promotion. |
| Observable | Once armed and out of fresh replicas, every WRITE-flagged command is refused with exactly `SELFFENCE writes rejected: no fresh streaming replica (self-fence-on-replica-loss)` — an error code of its own, naming the mechanism and the knob that turns it off, on a node that may have nothing to do with cluster mode; **reads keep working**, and the refusal clears on its own as soon as a fresh replica reconnects and streams — no restart, no config change. A **cluster** primary fenced by lost Raft quorum still answers `CLUSTERDOWN The cluster is down (quorum lost, writes rejected)` (FM-CLUSTER-059): the two fences are two checkers behind one write gate, and each names itself. A primary whose last streaming replica left **gracefully** (FM-REPLICATION-062) does not fence at all — the latch is dropped on that departure, so a deliberately decommissioned replica leaves the primary writable, while a replica that was *lost* leaves it fenced. A primary that never had a replica accepts writes indefinitely with the fence enabled (a cluster primary with zero replicas is the common case and must behave exactly as before). A `MULTI` that queues a write while fenced is rejected at *queue* time and the transaction is flagged dirty, so `EXEC` answers `EXECABORT` rather than running the surviving subset. The fence reason is also reported out-of-band on the status endpoint (`cluster.write_fence: "replica quorum lost"`), and the field is **absent** rather than null while writes flow. |
| NOT observable | **A fresh primary fencing itself before it ever had a replica** — the shape that appears when arming is dropped or is inferred from config instead of latched from the tracker: a standalone node with the flag on refuses every write from boot, which is a total outage caused by a safety feature. **Arming inherited across a demotion** — a node demoted and later re-promoted would fence the fresh primary on its predecessor's history; `reset_arming` on demotion is what prevents it. **A fence that grants a grace period when the toggle is flipped on**: arming is tracked even while fencing is *disabled*, so enabling it on a primary that has already served replicas fences immediately rather than waiting to re-arm — the opposite behavior gives the operator a window in which the flag is on and does nothing. **A stale replica counted as quorum**: a session still in `Streaming` whose last ACK is older than the freshness window is a dead link that has not been reaped yet, and counting it is indistinguishable from having no replica at all. **A per-write `Vec` allocation** on this path — it runs on every write command, and the existence-only check exists for that reason. **`-CLUSTERDOWN` from this fence**: it is the *cluster* fence's code (FM-CLUSTER-059), and answering it on a standalone primary tells the operator to go looking at a cluster that does not exist, with no mention of the knob that produced the refusal. **A fence still engaged after the last streaming replica left cleanly**: the operator who decommissioned the replica performed the departure, and holding writes hostage to a replica nobody expects back is an outage produced by the safety feature. **A fence dropped while a replica is still registered but silent**: a session in `Streaming` that has stopped ACKing has *not* departed — that is the partition this fence exists for, and the disarm must not read it as a clean exit. |
| Invariant | `ReplicationQuorumChecker::has_quorum()` is ordered cheapest-first and short-circuits: one relaxed load for the toggle, then `arm_if_streaming()` (a single relaxed load once latched; it touches the tracker only while still unarmed, and then through the allocation-free `has_streaming_replica()`), then `!fencing \|\| !armed -> true`, and only an armed+enabled checker pays `count_fresh_streaming_replicas() >= 1`; only when *that* is 0 does it pay the last step, `disarm_if_departed_cleanly()`. Freshness is `r.last_ack_time.elapsed() < freshness_timeout`, with the window **loaded per check** rather than captured at construction — that is what makes `CONFIG SET` live. `armed` is a latch set by any replica reaching `Phase::Streaming` and cleared by exactly two things: `reset_arming()` on demotion, and `disarm_if_departed_cleanly()` — which fires only when the checker is armed, no fresh streaming replica remains, `has_streaming_replica()` is false (nothing is even registered as streaming) **and** the tracker's last streaming departure was `ReplicaDeparture::Graceful` (FM-REPLICATION-062). An unknown departure — the tracker was never told, which is what a bare `unregister_replica()` leaves behind — keeps the fence armed, so the safe verdict is the default rather than the exception. `write_fence_reason()` shares the same predicate as the write gate (`fence_engaged()` is `has_quorum()`'s negation under an enabled toggle, disarm step included), so the status report cannot claim a fence state the write path is not in. The refusal itself lives in `ConnectionHandler::run_pre_checks` (`guards.rs`), third in the ladder — READONLY -> MISCONF -> **self-fence** -> NOREPLICAS -> NOADMIN -> ACL — and is gated on `CommandFlags::WRITE` read from `get_entry` (all registered commands, not just shard commands). The rung's *wording* is the checker's, not the gate's: `QuorumChecker::quorum_lost_error()` defaults to `CLUSTER_DOWN_QUORUM_LOST` (what the cluster checker and its `SelfFenceGate` wrapper report, delegating to the inner checker) and is overridden by `ReplicationQuorumChecker` with the `SELFFENCE` string, so one gate serves both fences without either having to know which is installed. Config validation requires `replica-freshness-timeout-ms >= 3 x ack-interval-ms`, so the window cannot be tuned below the cadence it measures. |
| Catalog | `INV-FENCE-1`, `INV-SESSION-3` — the checker is armed whenever a session is streaming and only a graceful departure disarms it, plus the arming latch as the independent witness that a recorded `Lost` departure describes a generation that actually streamed. |
| Outcome variant | `SELFFENCE writes rejected: no fresh streaming replica (self-fence-on-replica-loss)`; `WriteFenceReporter::write_fence_reason() == Some("replica quorum lost")` |
| Forced by | `unarmed_allows_writes`, `armed_with_fresh_replica_allows_writes`, `armed_with_stale_replica_rejects_writes`, `armed_with_no_replicas_rejects_writes`, `arming_transition`, `self_fence_toggle_is_live`, `freshness_timeout_is_live`, `empty_tracker_never_fences`, `the_self_fence_names_itself_and_its_knob`, `test_self_fence_engages_on_replica_loss`, `test_self_fence_recovers_after_replica_reconnect`, `test_self_fence_unarmed_allows_writes`, `test_cluster_primary_writes_without_replicas`, `write_fence_reason_is_reported_only_while_fenced`, `enabling_the_fence_onto_a_lost_quorum_warns_once`, `enabling_the_fence_on_a_healthy_primary_is_silent`, `disabling_the_fence_is_silent` |
| Bug refs | — (no open issue; the Lua/EXEC bound is tracked under `.scratch/replication-cluster-rework/issues/open/03-lua-internal-write-validation.md`) |

**Redis has no equivalent, and the wording says so.** `-SELFFENCE` is a FrogDB-only error code:
Redis has no fence in this configuration and would refuse the write via `min-replicas-to-write`
(`-NOREPLICAS`) or not at all. It replaced `-CLUSTERDOWN`, which this fence used to borrow from
cluster mode (issue 30) — an error that named a subsystem the fenced node need not even be running,
and named neither the mechanism nor the knob that clears it. The string is pinned by
`test_self_fence_engages_on_replica_loss` with a comment saying exactly that, so any reword is
deliberate. A client that treats every unknown `-` code as a plain error is unaffected; one that
matched on `CLUSTERDOWN` to detect this fence must now match `SELFFENCE`, and that is the intended
break — pre-production, and the old code was a misdiagnosis.

**The status-endpoint claim in the Observable cell is forced from `frogdb-telemetry`**, by
`write_fence_reason_is_reported_only_while_fenced` — the crate became eligible when GAP-7 closed.

**Gate bounds.** `run_pre_checks` fires both fences for direct writes and at `MULTI` *queue* time.
The two producers it structurally cannot see — a write issued from inside Lua (`EVAL` carries no
WRITE flag, so the `SET` never appears in any command text the connection reads) and a `MULTI`
queued while replicas were healthy then `EXEC`'d after they dropped — are fenced at the shard write
seam instead, where the mutation actually happens (`specs/txn.md` FM-TXN-051, forced by
`test_self_fence_gates_lua_writes` and `test_min_replicas_to_write_multi_and_lua_paths`).

---

## FM-REPLICATION-042 — min-replicas-to-write refuses from boot, on live config, and never arms

| Field | Value |
|---|---|
| Trigger | `min-replicas-to-write N` (N > 0) on a primary whose count of *good* replicas — streaming **and** last-ACKing within `min-replicas-max-lag` — is below N. Three shapes: a primary with zero replicas from boot; a primary that had a good replica and lost it; and the knob being raised or lowered at runtime with `CONFIG SET` while the write path is hot. Also the promoted-primary case: the value is set before a promotion and must govern the node afterwards. |
| Observable | Writes are refused with exactly `NOREPLICAS Not enough good replicas to write.`, **from boot** — unlike the self-fence there is no arming, so a primary that never had a replica refuses immediately, as Redis does. Reads stay allowed. The gate tracks health live in both directions: writes flow while a good replica streams and start failing within the detection window once it drops. `CONFIG SET min-replicas-to-write 1` on a replica-less primary makes the very next `SET` fail, and setting it back to 0 makes the next one succeed — no restart. A `MULTI` that queues a write while ungated-but-unhealthy is rejected at queue time and `EXEC` answers `EXECABORT`. |
| NOT observable | **Writes accepted with `min-replicas-to-write N` set and no replicas**, i.e. the gate inheriting the self-fence's arming latch: that would make the knob a no-op on exactly the node it exists to protect (a primary that has never yet had a replica) and is the single most damaging way to get this wrong, because the config reads as enabled. **A stale replica counted as good**: `min-replicas-max-lag` exists to exclude a session whose link is dead but whose teardown has not run, and ignoring it turns the gate into a bare connected-count check. **The value read once at boot** — a `CONFIG SET` that logs success and changes nothing is worse than a rejected `CONFIG SET`. **The tracker read on non-write commands**, or the config read before the WRITE-flag check: this is the hot path, and a `GET` must not pay for a replication policy. **`min-replicas-max-lag 0` excluding everybody** rather than disabling the freshness filter — Redis's 0 means "no lag check", and inverting it fences a healthy primary. |
| Invariant | The gate is the fourth rung of `run_pre_checks`, immediately after the self-fence (Redis runs `writeCommandsDeniedByDiskError` then its `NOREPLICAS` check in the same relative order). It is entered only for commands whose `get_entry` flags contain `CommandFlags::WRITE`, and only then is `min_replicas_to_write()` read; the tracker walk happens only when that value is `> 0`. "Good" is `ReplicationTrackerImpl::count_good_replicas(max_lag)`, derived from the same `get_streaming_replicas()` projection `WAIT` counts (FM-REPLICATION-039), filtered by `max_lag.is_zero() \|\| r.last_ack_time.elapsed() < max_lag` — the `is_zero()` disjunct *is* Redis's `min-replicas-max-lag 0` semantics, expressed as a disabled filter rather than a zero-length window. Both values come from `ConfigManager` on every check, which is what makes the knobs live. A missing tracker (`replication_tracker == None`, a build with no replication wiring) yields 0 good replicas — the safe direction: refuse rather than assume. Replica apply traffic can never reach this ladder: the replication executor sends `CoreMsg::Execute` straight to the shards under `REPLICA_INTERNAL_CONN_ID` and never builds a `PreDispatchView`, the same carve-out Redis makes with its "unless coming from our master" clause. |
| Outcome variant | `NOREPLICAS Not enough good replicas to write.` |
| Forced by | `test_min_replicas_to_write_rejects_without_replicas`, `test_min_replicas_to_write_gate_tracks_replica_health`, `test_min_replicas_to_write_config_set_live`, `test_get_streaming_replicas`, `count_good_replicas_excludes_a_stale_replica_but_zero_disables_the_check`, `count_good_replicas_ignores_non_streaming_replicas`, `noreplicas_still_fires_after_a_replica_goes_silent`, `has_streaming_replica_tracks_the_streaming_projection` |
| Bug refs | `.scratch/replication-cluster-rework/issues/open/03-lua-internal-write-validation.md` (the shared Lua bypass — closed at the shard write seam, `specs/txn.md` FM-TXN-051) |

---

## FM-REPLICATION-043 — the primary's replica view is one registry, rendered once

| Field | Value |
|---|---|
| Trigger | `INFO replication` on a node that owns a replication tracker, in every role it can be in: a primary with N streaming replicas, a primary whose replicas are still handshaking, a node demoted at runtime by `REPLICAOF`, and a node promoted into a chain that a survivor then reattaches to. Also the lag machinery that acts on the same registry: a replica whose byte-lag or seconds-since-ACK crosses `replication-lag-threshold-bytes` / `replication-lag-threshold-secs`. |
| Observable | `connected_slaves:N` equals the number of `slaveN:` lines rendered, and there is exactly one `slaveN:ip=<addr>,port=<port>,state=<state>,offset=<acked>,lag=<secs>` line per replica in a phase Redis names — handshaking, receiving its checkpoint, or streaming — in registry (attach) order, with `offset` being that replica's **wire-acknowledged** offset — Redis's `repl_ack_off`, which is `0` until the replica's first `REPLCONF ACK` lands and never carries anything the primary told itself. For a streaming replica that is the same number `WAIT` counts against (FM-REPLICATION-039), including the freshly-online one that reads `state=online,offset=0` for a beat; for one that is still syncing it is what it has acknowledged so far, which is exactly why its `state` is not `online` (FM-REPLICATION-060). Every field is projected off that replica's own registry entry: `port` is its announced `REPLCONF listening-port` rather than its ephemeral source port, `state` is its lifecycle phase in Redis's vocabulary — `wait_bgsave`, `send_bulk` or `online`, each observable by a client (FM-REPLICATION-049, FM-REPLICATION-060) — and `lag` is the whole-second age of its last ACK (FM-REPLICATION-049). A demoted node renders the *replica* branch (`role:slave`, `master_host`/`master_port`/`master_link_status`) with `connected_slaves:0`, even though it still owns the tracker. A promoted node counts the survivor that reattaches to it. `master_repl_offset` is the node's one offset counter in either role. Separately: a replica that exceeds a lag threshold is proactively disconnected, and an address-keyed cooldown stops the primary from re-disconnecting it in a tight reconnect loop; both thresholds are live-tunable and default to 0 (disabled). |
| NOT observable | **A `slaveN:` line that claims `state=online` for a replica that is not streaming** — a session still installing its full-sync payload has none of the primary's data, and reporting it as an online slave is how an operator concludes a failover target is ready when it is not (FM-REPLICATION-001's failure from the primary's side). The line itself is rendered (that is Redis's behaviour and FM-REPLICATION-060's), but it states the phase it is in. **An `offset=` on that line that the replica never sent** — the phase is a *sender-side* fact (`Phase::Streaming` is published at the top of `start_streaming`, before the backlog tail has even been written to the socket, let alone decoded), so `state=online` alone never proves the replica holds anything; pairing it with an offset the primary seeded made the pair read as a caught-up replica and is what let issue 28's `WAIT` through. `offset=` is wire-only for that reason, and a `state=online,offset=0` line is the honest rendering of a link that has been handed the stream and not yet answered. **A syncing replica that is invisible**: absent from both the count and the lines is not the safe reading — it is the state an operator opens `INFO replication` during a resync specifically to see, and its absence is indistinguishable from a replica that never dialled (issue 21). **`connected_slaves` and the `slaveN:` line count disagreeing**: they are one projection — the count *is* the number of lines rendered — or there is no contract. **`connected_slaves` read as the number of replicas that can serve a read or take a failover**: it is not, and never was, what `WAIT` counts; `WAIT` counts acknowledged offsets over the streaming set, which is deliberately narrower. **A demoted node still rendering the `master` branch** — it would report `role:master` with `master_host` absent, so an orchestrator cannot tell which node heads the history; the tracker outliving a demotion is exactly why the render is gated on the live role flag rather than on the tracker's presence. **A hardcoded `master_repl_offset:0` on the replica branch**: the applied offset is the value the node would resume from and freeze as the failover boundary if promoted, and reporting 0 made a promoted node's `second_repl_offset` look like it came from nowhere. **A lag disconnect loop**: without the address-keyed cooldown (replica ids change on reconnect, so id-keyed would not survive) a lagging replica is disconnected on its first post-reconnect check, forever. |
| Invariant | One source: both renderers call `info::rendered_replicas(tracker)`, which maps `tracker.get_all_replicas()` — id-sorted, so the `slaveN:` indices are stable between two renders of an unchanged registry — through `ReplicaLine::from_replica`, mapping `address.ip()`, `listening_port`, `ReplicaState::from_phase(phase)`, `acked_offset` and `lag_secs()` into a `ReplicaLine` and yielding `None` for a phase Redis has no state for; `connected_slaves` is that vector's length, so the count cannot be computed from a different set than the lines — one `ReplicaLine::from_replica` / `ReplicaLine::render` pair that both INFO renderers call (FM-REPLICATION-049), and the whole `PrimarySnapshot` is `.filter(\|_\| !is_replica)`'d on the live role flag, which is what makes a demotion flip the render even though `replication_tracker` is still wired. `repl_offset` is `tracker.current_offset()` in both branches — one counter per node, advanced by the primary stream when this node stamps writes and by the replica ingest loop when it applies them. `ReplicationSection::render` is pure over that snapshot (no clocks, no locks), so the rendering is unit-testable without a server. Lag has two independent measures on the same registry: `replica_lag(id)` = `current_offset.saturating_sub(stream_position())` (bytes) and `replica_lag_secs(id)` = `last_ack_time.elapsed()`. `stream_position()` is `max(acked_offset, resume_offset)` — the byte measure is the *only* consumer that folds the resume seed back in, and it must, because "how far behind the stream is this link" is answered by where the replica resumed just as much as by what it acked: reading `acked_offset` alone would make every full-resync replica look maximally lagged at the moment it went online and hand it straight to `LagPolicy` for a disconnect, in a loop. It is the same reason `last_ack_time` is reset by the seed. Neither reset lends the replica any durability credit — `count_acked` and `INFO`'s `offset=` read `acked_offset` alone (FM-REPLICATION-039). The `saturating_` is still load-bearing: a seed lands at the offset read a moment earlier, so it can briefly equal or exceed a re-read `current_offset`. `LagPolicy::should_disconnect` loads both thresholds *at the point of use* (live retune), returns early when both are 0 (a disabled policy does not even count the frame, so arming mid-session starts the cadence from the next frame), samples only every `LAG_CHECK_INTERVAL` = 100 forwarded frames, and suppresses a re-fire while `is_in_lag_cooldown` — which is keyed by `SocketAddr`, not replica id, precisely because ids change on reconnect. |
| Catalog | `INV-SESSION-2`, `INV-OFFSET-3` — the registry both renderers project is one entry per live session, each with an ack no larger than the live head, so neither a doubled identity nor a credited-before-the-wire session can reach a render. |
| Outcome variant | `INFO replication` fields `connected_slaves`, `slaveN:`, `master_repl_offset`, `master_link_status` |
| Forced by | `replication_primary_renders_slave_lines`, `replication_replica_renders_its_applied_offset`, `replication_replica_renders_master_link_up_when_streaming`, `replication_replica_renders_master_link_down_when_not_streaming`, `test_info_replication_shows_all_replicas`, `test_info_replication_primary_format`, `test_failover_chain_survivor_reattaches_to_promoted_node`, `test_replica_lag`, `replica_lag_measures_from_the_resume_seed_before_the_first_ack`, `stream_position_is_the_max_of_the_wire_ack_and_the_resume_seed`, `test_replica_lag_secs`, `test_lag_disconnect_cooldown`, `test_lag_cooldown_address_based`, `lag_policy_disabled_never_fires`, `lag_policy_byte_threshold_triggers`, `lag_policy_byte_threshold_not_exceeded_does_not_fire`, `lag_policy_time_threshold_triggers`, `lag_policy_cooldown_suppresses_retrigger`, `lag_policy_byte_threshold_retunes_live`, `lag_policy_time_threshold_retunes_live_via_handler`, `a_slave_line_is_projected_from_the_replica_not_from_literals`, `a_slave_line_reports_the_real_ack_age`, `test_replica_repl_offset_catches_up_to_primary`, `connected_slaves_counts_every_rendered_line` |
| Bug refs | `.scratch/replication-cluster-rework/wait-cluster-mode.md` §7.7 (CLUSTER SHARDS peer offsets still report 0 — the cluster-side twin of this rendering) |

**The literals in this section are gone.** `state=online` and `lag=0` were string constants in the
format call and are now projected per replica — GAP-2 and GAP-3 are closed by FM-REPLICATION-049.
`repl_backlog_size:1048576` and `repl_backlog_first_byte_offset:0` were the last two, and are now
projected off the live ring buffer through one shared field list — FM-REPLICATION-059, which also
took `repl_backlog_active` and `repl_backlog_histlen` (both were derived from the wrong quantity)
and the shard-local replica branch's `master_repl_offset:0`, the last instance of the literal this
row's NOT-observable clause already forbade. No field in either `INFO replication` renderer is a
constant standing in for data.

---

## FM-REPLICATION-044 — a checkpoint file name is one path component, or the frame is refused

| Field | Value |
|---|---|
| Trigger | Reading a per-file header out of a `$FROGDB_CHECKPOINT` / `$FROGDB_SNAPSHOT` envelope whose name is not a bare file name: an absolute path (`/etc/authorized_keys`), a traversal (`../../frogdb.conf`, `a/../../b`), a nested name (`a/b`), the empty string, `.` or `..`, a form that normalizes to another (`CURRENT/`, `CURRENT/.`), or bytes that are not valid UTF-8. The primary is remote input on this path — a replica does a full sync against whatever answered its `PSYNC`, and the name is used *before* any checksum has been verified. |
| Observable | The sync fails with `io::ErrorKind::InvalidData` at the header, and the replica retries on its normal reconnect backoff still serving its previous keyspace. Nothing is written: not at the escape target, not under the staging directory, not even the staging directory's own creation for that file. Legal names — `CURRENT`, `000042.sst`, `MANIFEST-000005`, and awkward-but-single-component ones like `..sneaky` — decode unchanged and land directly in the staging dir. |
| NOT observable | **A name that resolves outside the staging directory.** `Path::join` discards its receiver entirely when the argument is absolute and climbs out of it on `..`, so a single hostile header turned a replica's checkpoint receive into an arbitrary file write as the server user — the replica dials the primary, so nothing but the address is authenticated. **A name that is written before it is validated.** The transport loop writes each file to disk as it is framed and only checks the combined checksum after the whole envelope has landed, so a refusal that happens at verification time happens one filesystem too late; the check has to be at the codec boundary or it is not a check. **Two distinct wire names folding to the same checksum input.** The name is part of the combined-checksum coverage (`CheckpointChecksum::update_file` hashes the name bytes as sent), so any decode that is not injective breaks the coverage: `String::from_utf8_lossy` mapped every invalid byte to U+FFFD, and `Path::components()` normalizes `CURRENT`, `CURRENT/` and `CURRENT/.` to one component — either way two different wire names land on one staged file while hashing different bytes, which is a checksum the sender and receiver can disagree on by construction. |
| Invariant | `checkpoint_file_name(name)` (`fullsync.rs`) is the single rule: the name must decompose to exactly one `std::path::Component::Normal` **and** re-encode to itself, the second half being what rejects the normalizing forms the first half would let through. `CheckpointStreamCodec::read_file_header` decodes the name with `std::str::from_utf8` — not lossily — and passes it through that rule before returning a `CheckpointFileHeader`, so an escaping name never reaches a caller. `receive_to_file(reader, dir, name, …)` takes the directory and the name separately and re-applies the same rule before joining, which makes containment a property of the function rather than of every caller that builds a path; `receive_checkpoint_files` therefore no longer joins anything itself. The `MAX_CHECKPOINT_NAME_LEN` bound of FM-REPLICATION-035 still runs first, before the buffer is sized. |
| Outcome variant | `io::ErrorKind::InvalidData` on the sync |
| Forced by | `read_file_header_refuses_names_that_are_not_one_component`, `receive_to_file_refuses_a_name_that_escapes_its_directory`, `receiver_refuses_a_file_name_that_escapes_the_staging_dir` |
| Bug refs | `.scratch/hardening/issues/done/12-checkpoint-file-names-are-not-validated.md` (Bug A in FM-REPLICATION-035's tail) |

**Names, not paths.** The checkpoint envelope has no directory concept: RocksDB checkpoint files are
flat, and the receiver stages them flat. Nothing is lost by refusing every shape that is not a bare
name, and admitting even one of them would put the burden of containment back on each call site.

---

## FM-REPLICATION-045 — an unterminated replicated MULTI is bounded and abandoned, never accumulated

| Field | Value |
|---|---|
| Trigger | A replicated `MULTI` whose `EXEC` never arrives on the same history: a primary bug, a corrupted or truncated frame stream, or a peer deliberately opening a group and streaming commands into it forever. The replica cannot decline to read the stream, so the group grows with every frame. Sharpened by the two shapes that breach different axes — millions of tiny commands, and a handful of very large values. |
| Observable | The group is dropped once it outgrows either ceiling, an `error`-level line names the group's size, both limits and the running abandoned count, `ReplicaTxnBound::abandoned` moves, and the link is ended: every later claim on that history is refused, nothing after the breach reaches a shard, and the connection rewinds so its reconnect can only be answered `+FULLRESYNC`. The abandoned group's bytes are never claimed, so the applied head still describes only data this node holds. A large but legal transaction — up to and including one sitting exactly on both ceilings — still applies as one atomic group on its tagged shard and claims its whole byte span. |
| NOT observable | **Unbounded growth of the buffered group.** `PendingTxn::commands` had no ceiling, so a `MULTI` with no `EXEC` pinned every subsequent frame for the life of the link until the replica OOMed — taking its read traffic and its failover candidacy with it, from a peer that authenticated nothing beyond answering the `PSYNC`. **Only one axis bounded.** A command count alone leaves a few `proto-max-bulk-len`-sized values unbounded in bytes; a byte total alone leaves millions of tiny commands unbounded in the per-command bookkeeping that dwarfs their payloads — either half on its own is a bound an attacker picks around. **A breach resumed with `+CONTINUE`.** The group's frames were consumed from the stream but never claimed, so the applied head no longer names a position the primary can resume from: a partial resync would either redeliver the same unterminated group forever or splice the surviving half of a transaction into the keyspace. **A breach that only logs.** The pre-existing nested-`MULTI` and stray-`EXEC` guards discard a group and carry on, which is right for a malformed *shape*; a group too large to hold is a link that cannot be trusted to close anything, so it takes the same disposition an admitted divergence takes. **A count that depends on log level** — the increment lives outside the `tracing::error!` argument list, because a disabled event does not evaluate its fields. |
| Invariant | `ReplicaTxnBound` (`apply.rs`) holds both ceilings and the abandoned counter, shared behind an `Arc` so the count is the node's rather than one link's. `exceeded(commands, bytes)` is checked immediately after each frame is pushed onto the open group, and is a strict `>` on both axes — the limits name the largest group that still applies. On breach the consume loop drops the group, counts it, and calls `ReplicaApplyStint::admit_divergence(epoch)`, the same latch a failed apply uses (FM-REPLICATION-010): further claims return `Claim::Stale`, and the connection woken through `AppliedOffset::divergence` runs `abandon_diverged_link`, which resets the received head to 0 so `psync_request_args` sends `PSYNC ? -1`. Both ceilings come from config (`replication.replica_txn_max_commands`, default 1e6; `replication.replica_txn_max_bytes`, default 1 GiB), and `ReplicationConfigSection::validate` rejects 0 on either — there is no "0 = unlimited" reading, since an unlimited bound is the bug itself. |
| Outcome variant | n/a (internal; surfaces as an `error` log, a moved abandoned counter, and a forced full resync) |
| Forced by | `an_unterminated_multi_is_abandoned_at_the_command_ceiling`, `an_unterminated_multi_is_abandoned_at_the_byte_ceiling`, `a_large_transaction_under_the_bound_still_applies_atomically` , `zero_replicated_txn_ceilings_are_rejected` |
| Bug refs | `.scratch/hardening/issues/done/13-an-unterminated-multi-grows-without-bound.md` (Bug B in FM-REPLICATION-034's tail) |

**Why full and not partial.** Reusing the divergence latch buys the forced `+FULLRESYNC` for free
rather than inventing a second teardown, and it is the only correct answer: an abandoned group's
bytes were consumed but never claimed, so there is no offset a `+CONTINUE` could legitimately resume
from. Redis bounds the analogous replica-side accumulation with `client-query-buffer-limit` and
kills the link on breach; this is the same instinct applied to the reconstructed group rather than
the socket buffer.

---

---

## FM-REPLICATION-046 — the freshness window survives a CONFIG round trip, and `0` disables by decision

| Field | Value |
|---|---|
| Trigger | Any read-back-and-reapply of the `min-replicas-to-write` freshness window: `CONFIG GET min-replicas-max-lag` followed by `CONFIG SET min-replicas-max-lag <that value>` — the shape every config dump/restore, every desired-state reconciler diffing GET output, and every operator copying a value from `CONFIG GET` performs. Sharpest when the stored window is sub-second, because the TOML field (`replication.min-replicas-timeout-ms`) is milliseconds while Redis's parameter name is seconds; `min_replicas_config()` in the fence tests uses 500 ms, so the project's own test deployments sit inside the sharp case. Also triggered by a seconds value large enough that its millisecond form overflows `u64`, and by an operator setting the window to `0` on purpose. |
| Observable | Both spellings reach the same live window and both are honoured immediately. `min-replicas-max-lag-ms` is the native unit and round-trips exactly at every magnitude. `min-replicas-max-lag` is Redis's seconds-valued spelling and rounds **up**: a 500 ms window reports `1`, and every non-zero sub-second window reports `1` rather than `0`, so the round trip may widen the window but never switches the filter off. Reapplying a reported value leaves the `NOREPLICAS` gate armed — a replica that has gone silent past the window is still refused entry to the good count afterwards. A seconds value whose millisecond form would overflow is rejected at validation with the live window untouched, while the largest expressible window (`u64::MAX / 1000` seconds) is accepted. `0` on either spelling stores `0`, reads back `0`, and disables the freshness check so that every streaming replica counts however long it has been silent — Redis's documented `min-replicas-max-lag 0`. Only the millisecond row participates in `CONFIG REWRITE`, so the file keeps the exact window regardless of which name was used to set it. |
| NOT observable | **A round trip that silently widens the window to "off"**: reporting a 500 ms window as `0` and then accepting that `0` back is indistinguishable from an explicit disable, so the `NOREPLICAS` gate degrades from "N replicas ACKed recently" to "N replicas are attached" without a log line, a warning, or a changed config value — the operator's dump matches the running config exactly, and the guarantee is gone. **A stale session counted as good**: the window exists precisely to exclude a replica whose link is dead but whose teardown has not run, and any path that reaches a zero window by accident (truncation, an unchecked wrap, a default) collapses that filter. **An overflowing multiplication**: `secs * 1000` unchecked wraps an absurd-but-legal seconds value into an arbitrary small window — and lands on exactly `0`, i.e. disabled, for any multiple of 2^64/1000 — so the most extreme value an operator can type produces the least protective behaviour. **Two registry rows writing the same TOML key**: an alias that named `replication.min-replicas-timeout-ms` alongside the millisecond row would make `CONFIG REWRITE` emit two values for one key, and since the seconds view rounds, the losing writer would silently retune the operator's file. **`0` excluding everybody** rather than disabling the filter — inverting the sentinel fences a healthy primary (shared with FM-REPLICATION-042). **A boundary that only a wall clock can see**: the freshness comparison must be assertable without racing `Instant::elapsed()`, or `<` versus `<=` is untestable by construction. |
| Invariant | The window is stored once, in milliseconds, as `replication.min-replicas-timeout-ms`, and is served under two CONFIG names with two different lifecycles. `min-replicas-max-lag-ms` is the derived row: it owns the TOML field, so it is the only spelling `CONFIG REWRITE` persists, and its `get`/`apply` are the identity on milliseconds. `min-replicas-max-lag` is a **virtual** registry row (`section: None, field: None`), which is what keeps it out of `ConfigManager::config_updates()` and therefore out of the rewrite path; its value type is `MinReplicasMaxLagSecs`, whose `from_millis` is `div_ceil(1000)` (round up, so no non-zero window can report as disabled) and whose `to_millis` is `checked_mul(1000)` (so overflow is a validation error, not a wrap). `validate` is `to_millis().map(|_| ())`, which means the rejection happens before `apply` touches the runtime cell. The freshness comparison itself is `frogdb_replication::ack_is_fresh(ack_age, window)` — a pure predicate over an age, not over a clock — and it is the single spelling of the boundary for both write gates: `count_good_replicas` (this row) and `ReplicationQuorumChecker::count_fresh_streaming_replicas` (FM-REPLICATION-041). The boundary is strict: an ACK exactly `window` old is stale. The zero sentinel lives at exactly one place, the `max_lag.is_zero()` disjunct in `count_good_replicas`, ordered first so the disable short-circuits the clock read; `ack_is_fresh` itself never encodes it, and `replica-freshness-timeout-ms` (which rejects `0` at validation) does not have it. |
| Outcome variant | No new wire outcome. `CONFIG SET min-replicas-max-lag <overflowing>` answers `ERR Invalid value for 'min-replicas-max-lag': too large: the window is stored in milliseconds and would overflow`; the gate's own outcome remains `NOREPLICAS Not enough good replicas to write.` |
| Forced by | `min_replicas_max_lag_round_trips_without_losing_a_sub_second_window`, `min_replicas_max_lag_zero_is_an_explicit_disable_on_both_spellings`, `min_replicas_max_lag_rejects_a_seconds_value_that_overflows_millis`, `noreplicas_still_fires_after_a_replica_goes_silent`, `count_good_replicas_excludes_a_stale_replica_but_zero_disables_the_check`, `count_good_replicas_ignores_non_streaming_replicas`, `ack_is_fresh_excludes_an_ack_exactly_at_the_window` |
| Bug refs | `.scratch/hardening/issues/done/18-min-replicas-max-lag-cannot-round-trip-a-sub-second-window.md` (fixed); closes GAP-1 and GAP-4 |

---

## FM-REPLICATION-047 — the backlog is configured by its own keys, and no configuration of it can hang a write

| Field | Value |
|---|---|
| Trigger | Any boot that tunes replication from a config file. Three shapes reach this row. An operator turns split-brain audit logging off (`split-brain-log-enabled = false`) to stop divergence files accumulating in the data directory — a flag whose own doc comment says it is log-only. An operator sizes the backlog, on either axis, and reaches `0` on the way — the natural spelling of "I do not want this buffer", and the one a templating layer produces when a value is unset. And an operator sizes the byte cap large enough that its megabyte-to-byte conversion leaves the `usize` domain. All three arrive through `ReplicationConfigSection` and are resolved once, at construction of the `PrimaryReplicationHandler`, before any replica has attached. |
| Observable | Each backlog property is governed by the key that names it. `backlog-enabled` decides whether the ring is populated at all; `backlog-size` and `backlog-max-mb` are its two caps; `repl-backlog-ttl` is its idle-free window. `split-brain-log-enabled` governs the divergence audit file and nothing else, so switching it off leaves a reconnecting in-window replica answered `+CONTINUE` exactly as before — the audit and the resume window are independently operable. Both caps are refused at `0` with an error that names the field *and* names `backlog_enabled` as the switch the operator actually wanted, and a byte cap that cannot be expressed is a boot error rather than a silently re-sized buffer. Whatever caps a buffer is built with, `ReplicationRingBuffer::push` returns: the newest command is always retained, the buffer never grows past its caps, and no writer is left parked behind the entries lock. |
| NOT observable | **A config key that silently disables partial resync.** `enabled`, `max_entries` and `max_bytes` were read from `split-brain-log-enabled`, `split-brain-buffer-size` and `split-brain-buffer-max-mb`; only `ttl_secs` came from a backlog key. Turning off a flag documented as "does not affect cluster behavior" therefore switched off the backlog, and every replica reconnect became a full checkpoint transfer — with no error, no log line, and a config file that reads exactly as intended. The backlog itself behaved correctly throughout, which is why no replication test could see it: the defect was entirely in the mapping, so the mapping is now a named function with a test that pins each field to its own key and sets the split-brain flag to the opposite of `backlog_enabled` so the old wiring cannot pass. **An eviction loop that cannot terminate on an empty deque.** `while entries.len() >= self.max_entries` is `0 >= 0` when `max_entries` is `0`, `pop_front()` on an empty deque returns `None`, and the `if let` body never ran — so the loop spun forever *holding `self.entries.lock()`*, and every subsequent write that touched the backlog blocked behind it. A whole-server hang, reachable from a config file that `validate()` accepted without a word. Validation now refuses `0`, but validation guards one path and the loop guards all of them, so the empty-deque guard is the load-bearing half and is what the forcing test asserts. **A byte cap that wraps.** `split_brain_buffer_max_mb * 1024 * 1024` was unchecked on a `usize`: the largest number an operator can type wraps to an arbitrarily *small* cap, and lands on exactly `0` for any multiple of 2^64 / 2^20 — so the config written to make the backlog enormous produced a backlog that retains one command. The conversion is now `checked_mul`, rejected at validation, and saturating (never wrapping) at the wiring site. **A cap of `0` read as "unlimited" or as "disabled"** — the first is the bug that hangs, the second already has a spelling, so neither reading is available. |
| Invariant | `ReplicationConfigSection` owns four backlog keys — `backlog_enabled`, `backlog_size`, `backlog_max_mb`, `backlog_ttl_secs` — and `split_brain_log_enabled` owns only the audit file (`SplitBrainLogger` is `Option` at the type level, `None` when it is off, per FM-REPLICATION-024). The three new keys are `#[param(skip)]`: the ring's capacity is fixed by `ReplicationRingBuffer::new` at construction, so a `CONFIG SET` could only report a change the running buffer never made — the same lying-observability class FM-REPLICATION-046 closed. (`repl-backlog-ttl` stays `mutable` because `BacklogTtl` is a live `Arc` seam that a retune actually reaches.) `ReplicationConfigSection::validate` rejects `0` on both caps and rejects a `backlog_max_mb` whose byte form overflows; `backlog_max_bytes()` is the single `checked_mul(1024 * 1024)` spelling that `validate` and the wiring share. `replication_init::backlog_config(&ReplicationConfigSection) -> BacklogConfig` is the whole mapping, extracted as a pure function precisely so a unit test can hold it — it saturates rather than wraps on the unreachable overflow. `ReplicationRingBuffer::push`'s eviction loop leads with `!entries.is_empty() &&`, so the guard covers **both** caps rather than only the byte one: an empty deque always exits, whatever the caps say. |
| Catalog | `INV-BACKLOG-3` — the caps bind the live ring whatever configuration produced them, and the entry allows exactly the one entry the eviction loop must always leave standing. |
| Outcome variant | n/a (boot-time; a rejected config is an `anyhow` error out of `validate`, and the wire outcome it protects is `ReplayDecision::Continue` remaining reachable) |
| Forced by | `the_backlog_is_configured_by_backlog_keys_only`, `an_overflowing_backlog_mb_saturates_rather_than_wrapping`, `ring_buffer_push_terminates_under_a_degenerate_cap`, `zero_backlog_caps_are_rejected_and_the_mb_conversion_is_checked`, `partial_resync_survives_split_brain_logging_disabled` |
| Bug refs | `.scratch/hardening/issues/done/14-the-replication-backlog-is-wired-to-split-brain-config.md` (named as a live bug in FM-REPLICATION-016's tail) |

**Why the keys were renamed rather than aliased.** There is one ring buffer, not two: it serves the
partial-resync replay and the split-brain divergence capture from the same entries. So
`split-brain-buffer-size` did not describe a buffer that exists independently of the backlog — it
described the backlog under a name that hid what it did. FrogDB is pre-release, so the keys were
renamed outright; an alias would have kept the misleading spelling in the operator's vocabulary for
the one benefit of not breaking a config file nobody has yet written.

**Why `0` is refused rather than clamped.** The loop no longer hangs on it, so `0` is now merely a
cap that retains one command — survivable, but not a backlog, and indistinguishable at runtime from
a correctly sized one until a replica reconnects and is refused. Rejecting it at boot is the only
point where the operator is still present to be told.

---

## FM-REPLICATION-048 — split-brain observability reports the record that exists, not the event that was attempted

| Field | Value |
|---|---|
| Trigger | A demotion whose divergence record cannot be written: the data directory is read-only, the volume is full, the path was removed underneath the process, or the file system refuses the create for any other reason. `write_log` does a `File::create` under `data_dir` and `sync_all`s before returning, so every one of those surfaces as an `io::Error` out of the same call. The demotion itself is already committed by the metadata plane at this point — the writes are being discarded whether or not their record lands. |
| Observable | The three surfaces say three different things, and each says only what it can support. `frogdb_split_brain_recovery_pending` is raised **only** when a file exists — it is the same claim `has_pending_logs()` makes at the next boot, so the two agree about which directory contents mean "an operator has something to reconcile". `frogdb_split_brain_log_write_failures_total` moves on exactly the failures, giving the lost record its own signal instead of leaving it to be inferred from silence. `frogdb_split_brain_events_total` and `frogdb_split_brain_ops_discarded_total` count the demotion and the writes it threw away, both of which happened regardless of the write, so they stay unconditional — a failure that also stopped counting the discarded data would hide the loss twice over. The `error`-level line naming the I/O error is still emitted, and the demotion still fires (FM-REPLICATION-024). |
| NOT observable | **A recovery-pending gauge asserting a file that was never written.** All three metric calls sat *outside* the match on `write_log`, so a failed write produced metric-for-metric the state of a successful one. `frogdb_split_brain_recovery_pending = 1` is a specific claim — "a divergence record is on disk waiting for an operator to reconcile it" — and after a failed write there is no such file: the operator is dispatched to find something that does not exist, while the divergent writes are gone with no durable trace at all. The alerting consequence is worse than the missing file, because a raised gauge reads as a *handled* incident. **A failure indistinguishable from a success.** With the gauge moved and nothing added, the failure would report as a clean demotion; the write-failure counter exists so the difference is visible without reading logs. **Discard accounting made conditional on the write.** Moving `SplitBrainEventsTotal` / `SplitBrainOpsDiscardedTotal` into the `Ok` arm alongside the gauge is the symmetric mistake: those two describe the event, which happened, and suppressing them on failure would mean the case where the audit is lost is also the case where nothing counts the data lost with it. **A counter that only moves when the log level is on** — the increment is a statement, not a `tracing!` field. |
| Invariant | `SplitBrainLogger::log` (`cluster_init.rs`) matches on `frogdb_replication::split_brain_log::write_log(...)` and each arm carries the telemetry its outcome supports: `Ok(path)` logs the path and sets `SplitBrainRecoveryPending` to `1.0`; `Err(e)` logs the error and increments `SplitBrainLogWriteFailuresTotal`. `SplitBrainEventsTotal::inc` and `SplitBrainOpsDiscardedTotal::inc_by(record.writes.len())` follow the match, unconditionally, because they describe the demotion rather than the file. The gauge has exactly two writers and they agree: this `Ok` arm, and `Server::check_split_brain_logs` at boot, which raises it from `has_pending_logs(data_dir)` — both are statements about directory contents. Nothing before the match touches telemetry, so a `divergence_record()` of `None` (this node did not diverge) still moves nothing at all. |
| Outcome variant | n/a (observability; `io::Result` from `write_log` is the discriminant) |
| Forced by | `split_brain_telemetry_follows_the_log_write_outcome` |
| Bug refs | `.scratch/hardening/issues/done/15-split-brain-metrics-move-when-the-log-write-fails.md` |

**Same class as the lying-INFO bugs.** The rule this row encodes is the campaign's: an observability
surface must not report a stronger outcome than the code achieved. What is new here is the
corollary — the remedy for a failed operation is not to fall silent, because silence is also
indistinguishable from success. The gauge moved *and* a counter was added; either alone would have
left a real failure unreportable.

**Not covered here.** Whether a failed audit should also block or retry the demotion. It does not:
the demotion is a metadata-plane decision the data path is reflecting, and refusing to reflect it
because a diagnostic file could not be written would leave two nodes claiming one history — a strictly
worse failure than a lost audit. The counter is the signal that the trade was made.

---

## FM-REPLICATION-049 — a replica's announced identity is recorded at the handshake and is what the primary renders

| Field | Value |
|---|---|
| Trigger | The pre-`PSYNC` `REPLCONF` options a replica sends to describe *itself*: `listening-port <p>` (the port it accepts connections on, which is not the ephemeral source port of the link it dialled out on) , `capa <c>...` (`eof`, `psync2`, and capabilities this primary has never heard of) and `frogdb-version <v>` (the replica's `CARGO_PKG_VERSION`, sent on every handshake) — sent by a boot-configured replica and by a runtime-demoted one alike, including a node whose configured `server.port` is `0` and whose serving port therefore exists only on the bound listener. Then the render surfaces that must report what was announced: `INFO replication`'s `slaveN:` lines through both renderers, and `ROLE` on a primary. Also every non-`Streaming` lifecycle phase a registered session can be in when one of those surfaces is read, and any replica whose ACKs have gone quiet. |
| Observable | A replica that announced `listening-port 7001` is rendered `port=7001` — by `INFO replication`, by `ROLE`, and by both INFO renderers — from the moment its `PSYNC` registers it, on a link whose source port is something else entirely. The port a replica announces is the port it actually *serves* on, read off its bound RESP listener: a node started with `port 0` announces the port the OS assigned it, so the primary renders a dialable address for every replica regardless of how the replica's port was chosen. A replica that announced `capa eof psync2` has both capabilities recorded; one that announced `capa eof nonsense-v9` has `eof` recorded, the unknown one dropped, and still reaches `PSYNC` and syncs (the recording half of FM-REPLICATION-018's `+OK`). A replica that announced `frogdb-version 0.1.0` is recorded with that exact string in `ReplicaInfo::replica_version`, verbatim and unparsed — it is a record of what the peer *said*. A peer that announced no version is recorded as `None`, which means **unknown**: it is what a replica predating the option and a non-FrogDB peer both produce, so it is deliberately distinguishable from every real version and no consumer may collapse it into one (a gate that must prove compatibility before it *refuses* admits it and says so out loud; a gate that must prove compatibility before it *finalizes* blocks on it — FM-REPLICATION-064 draws that line and explains why the two differ). Re-announcing an option overwrites only its own kind: a second `listening-port` does not clear the capabilities or the version, a second `capa` does not reset the port, and a second `frogdb-version` clears neither of the other two. `state` is projected from that replica's lifecycle phase rather than written as a constant — `wait_bgsave` before the checkpoint is cut, `send_bulk` while it transfers, `online` once it streams — and every one of those values reaches a client, because both renderers feed the whole registry through that projection (FM-REPLICATION-060). A replica being torn down renders no line, which is Redis's own behaviour. `lag` is the whole-second age of its last `REPLCONF ACK`, the same measure `LagPolicy` disconnects on, so a replica that stops ACKing renders a growing number. A malformed announcement is still refused locally with the wire errors it always raised — `ERR wrong number of arguments for 'replconf listening-port' command`, `ERR invalid port number` for a UTF-8 argument that is not a `u16`, `ERR invalid port encoding` for an argument that is not UTF-8 at all, `ERR wrong number of arguments for 'replconf frogdb-version' command` for a valueless version, and `ERR invalid version encoding` for a version that is not UTF-8 — and does not kill the connection. Token matching is ASCII-case-insensitive, as it is everywhere else on this dispatch path: `capa EOF` records `eof`, not nothing. |
| NOT observable | **`port=0` on a replica that announced a port.** The announcement was parsed, logged, answered `+OK` and dropped on the floor: `SessionInner.listening_port` was initialised to `0` and had no writer anywhere in the crate, under a comment claiming the server would store it. `port` is the field an operator or an orchestrator dials to reach the replica from the primary's view, so `0` is not a degraded reading — it is an address that cannot be used, reported with the same confidence as a correct one. **`port=0` because the replica announced its *configured* port rather than its bound one.** The primary can only render what it was told, so recording the announcement faithfully (above) closes nothing if the replica announces `config.server.port` — `0` under an OS-assigned port — and the same unusable address reaches the wire from the other end of the handshake (issue 25). The announced value is a property of the running listener, not of the config file. **`replica_version: None` for a replica that announced one.** The third option had the same shape as the first two and one worse consequence: it was parsed, logged and dropped under a comment saying "the replication tracker stores this so the primary can check all replica versions during finalization", and `SessionInner.replica_version` had no writer anywhere in the workspace, so the field read `None` for every replica that had ever connected. A gate written as "every known replica is at version >= X" is *vacuously true over an empty set*, so the missing writer makes such a gate fail **open** during exactly the rolling upgrade it exists to protect — worse than no gate, because the operator believes one is there (issue 22). **A version defaulted rather than left unknown**: recording an absent announcement as `"0.0.0"`, as the empty string, or as this primary's own version collapses "the peer did not say" into "the peer said something", which fails the gate open a second way. **A capability recorded by refusing the ones next to it**: dropping an unknown `capa` token must not drop the known tokens in the same command, and must not fail the handshake (that is FM-REPLICATION-018, and closing this row must not weaken it). **`state=online` for a replica that is not streaming.** The literal it replaced was safe only because `info_handler` pre-filtered to `get_streaming_replicas()` — two facts in two files with nothing tying them, so the constant became a lie the moment the feed widened, labelling a replica that holds none of the primary's data as a ready failover target (GAP-2, and FM-REPLICATION-001's failure from the primary's side). The feed *has* since widened (FM-REPLICATION-060) and the projection is now the only thing standing between a syncing replica and that lie, so it is load-bearing rather than defensive. **`state` claimed to be per-replica while the feed is streaming-only**: a projection no client can observe the outputs of is a spec asserting behaviour it does not have — which is why the feed was widened rather than the claim softened. **`lag=0` for a replica that has gone silent**: the primary computes the real seconds-since-ACK and acts on it, so rendering `0` tells the operator a replica is perfectly caught up right up to the moment the primary disconnects it for lag — misleading data, not missing data (GAP-3). **A `slaveN:` line dropped while `connected_slaves` still counts it**: the two are one projection (FM-REPLICATION-043), so a phase with no Redis spelling must drop out of *both* — `connected_slaves` is the length of the rendered vector, never of the registry. **The announcement travelling on a shared/global side channel**: it belongs to one connection and must not leak between concurrent handshakes. |
| Invariant | The announcement is folded on the connection, because at `REPLCONF` time there is no session to write it to — `ReplicaSession` is constructed by `tracker.register_replica`, which runs inside `PrimaryReplicationHandler::handle_psync`, i.e. one command later. `AnnouncedOption::parse(&args) -> Result<Option<AnnouncedOption>, AnnouncementError>` (`replica_session.rs`) is the single parser: `Ok(None)` for a `REPLCONF` that announces nothing (`ACK`, `GETACK`, anything unknown), `Ok(Some(_))` for `listening-port`/`capa`/`frogdb-version`, `Err` only for a `listening-port` that is absent or not a `u16` or a `frogdb-version` that is absent or not UTF-8 (there is no "invalid version *number*" twin — the value is never parsed, so the only way it can fail is by not being text). `DispatchStage::ReplicationHandshake` (renamed from `PsyncIntercept`, because it now owns both commands of the handshake) calls it, folds the result into `ConnectionHandler.replica_announcement: ReplicaAnnouncement` via `ReplicaAnnouncement::absorb`, and answers `+OK`; the buffer is a plain connection field, so it is per-link by construction. The same stage's `PSYNC` arm passes that `ReplicaAnnouncement` by value into `handle_psync`, which hands it to `tracker.register_announced_replica(addr, announcement)` → `ReplicaSession::announced(id, address, announcement)` — so the session is *born* with the identity rather than being mutated into it, and there is no window in which a registered session reports a placeholder. All three fields are seeded in that one constructor — `SessionInner` has no setter for any of them. The shard-path `ReplconfCommand::execute` arm for `listening-port`/`capa`/`frogdb-version` calls the same parser and the same `announcement_error` mapping (it cannot record — a `MULTI`-queued `REPLCONF` never reaches the connection stage), so both paths answer identically on the wire. On the render side there is one spelling of a `slaveN:` line: `ReplicaLine::from_replica(&ReplicaInfo) -> Option<Self>` projects `address.ip()`, `listening_port`, `ReplicaState::from_phase(phase)?`, `acked_offset` and `ReplicaInfo::lag_secs()`, and `ReplicaLine::render(index)` formats it; `info/sections.rs` and `commands/info.rs` both reach them through the one shared feed `info::rendered_replicas(tracker)` and nothing else. `lag_secs` is `ack_age_secs(last_ack_time)` truncated to whole seconds — the same function `replica_lag_secs` returns to `LagPolicy`, so INFO and the disconnect policy cannot drift. `ReplicaState::from_phase` is total over `Phase` and returns `Option`: `Connecting`/`PreparingCheckpoint` → `Some(wait_bgsave)`, `StreamingCheckpoint` → `Some(send_bulk)`, `Streaming` → `Some(online)`, `Disconnecting` → `None` (no line, matching Redis, which emits a `slaveN:` line only for its three states). The `Option` is what makes "has no Redis spelling" a value the type system carries to the one `filter_map` in `rendered_replicas`, instead of a state name invented at the render site. On the announcing side there is one source for the port: `Server::with_listeners` reads `infra.listener.local_addr()?.port()` — the bound listener, which resolves an OS-assigned `0` — and threads that single value into both roles' handshakes, `init_replication` (the boot-configured replica's `ReplicaReplicationHandler`) and `RealReplicaStreamer::new` (every stream a runtime demotion starts). Neither constructor reads `config.server.port` any more, so the configured value cannot re-enter on one path only. |
| Catalog | `INV-SESSION-2` — the announced identity this row records is the key the entry compares sessions on; a session that announced nothing is unknown rather than port 0, and is never compared. |
| Outcome variant | n/a (RESP `+OK` on the announcement; `INFO replication` fields `slaveN:ip/port/state/offset/lag`; `ROLE`'s replica triplet). Rejections reuse `CommandError::WrongArity { command: "replconf listening-port" \| "replconf frogdb-version" }` and `CommandError::InvalidArgument`. `ReplicaInfo::replica_version: Option<String>` is internal — no surface *renders* it, but it is no longer inert: FM-REPLICATION-064 reads the announcement at the `PSYNC` and can turn it into a refusal on the wire. |
| Forced by | `an_announced_session_reports_the_port_and_capabilities_it_was_told`, `an_unknown_capability_is_recorded_as_absent_not_rejected`, `a_repeated_option_overwrites_only_its_own_kind`, `a_replica_that_announced_its_version_is_recorded_with_it`, `a_replica_that_announced_no_version_is_recorded_as_unknown`, `re_announcing_the_version_does_not_clear_the_port_or_capabilities`, `an_unreadable_frogdb_version_is_refused`, `lag_secs_is_the_age_of_the_last_ack`, `a_psync_carries_the_announcement_into_the_registry`, `a_slave_line_is_projected_from_the_replica_not_from_literals`, `a_slave_line_reports_the_real_ack_age`, `role_reports_the_announced_port`, `parse_capa_matches_case_insensitively`, `the_handshake_announces_the_port_it_was_given`, `runtime_stream_announces_the_streamers_listening_port`, `test_info_replication_shows_all_replicas` |
| Bug refs | `.scratch/hardening/issues/done/16-replconf-listening-port-and-capa-are-parsed-then-discarded.md`, `.scratch/hardening/issues/done/22-replconf-frogdb-version-is-parsed-then-discarded.md`, `.scratch/hardening/issues/done/25-a-replica-announces-its-configured-port-not-its-bound-one.md`; closes GAP-2 and GAP-3 |
| Model | `replication_feed_gate.qnt::inv_unannounced_is_never_superseded` |

**The issue's suggested remedy was not available.** Issue 16 proposed "adding setters called from the
connection-coupled path where `PSYNC` already reaches the session". There is no session to set: the
`ReplicaSession` does not exist until `PSYNC` creates it, so a setter would have had nothing to be
called on at `REPLCONF` time. The announcement is therefore buffered on the connection and passed
into construction — which is the stronger shape anyway, because a session can never be observed in a
placeholder state between registration and a later setter call.

**Why `Disconnecting` renders no line.** Redis's `genInfoSectionDict` emits a `slaveN:` line only for
`wait_bgsave`, `send_bulk` and `online`, and a replica being torn down is on its way out of the
registry — naming it in a fourth state would put a word on the wire no Redis client parses, for a
peer that will be gone by the next render. Dropping it does *not* desynchronize `connected_slaves`
from the line count (which FM-REPLICATION-043 forbids outright), because the count is the length of
the rendered vector: the two fall out of one `filter_map`. Forced by
`a_disconnecting_replica_is_not_rendered` and `connected_slaves_counts_every_rendered_line`.

**The announced version's consumer lives in FM-REPLICATION-064.** Recording it closes the *writer*
half of issue 22; the reader half was missing when this row was written, and issue 27 filed it.
`ClusterCommand::FinalizeUpgrade` (`cluster/src/commands.rs`) validates the versions of the **raft
topology's nodes**, not of the replicas attached over `PSYNC`, and it fails *closed* on an empty one
— so the finalization check the deleted comment named was never this field's consumer, and a
primary-replica pair outside a raft cluster had no gate at all. `version_gate::is_gate_active`
likewise keys on the cluster's finalized `active_version` and never reads a replica's announcement.
The handshake gate that does read it — refusing a different major at the `PSYNC`, warning on a minor
skew or an unreadable version — is FM-REPLICATION-064. This row still stops at "the primary records
what the replica said, and unknown stays unknown": the record is verbatim and the gate parses a copy.

---

## FM-REPLICATION-050 — the resync counters count the fork they are named after

| Field | Value |
|---|---|
| Trigger | Any `PSYNC` that resolves through `handle_psync`, in each of the three shapes the counters exist to tell apart: a first attach (`PSYNC ? -1`), a reconnect inside the backlog window that is granted `+CONTINUE`, and a reconnect that presents a real replid but cannot be served — evicted past the floor, an unknown history, an offset ahead of the head, or a backlog switched off. Then `INFO stats` read through either renderer: the connection-level `StatsSection` and the shard-local `build_stats_info` that `INFO` inside a script reaches. |
| Observable | `sync_full` counts full resyncs served, `sync_partial_ok` counts `+CONTINUE` grants **that went on to serve their backlog tail**, and `sync_partial_err` counts partials that were asked for and refused. A first attach moves `sync_full` only. A reconnect inside the window moves `sync_partial_ok` only, leaving `sync_full` where it was. A reconnect that overran the backlog moves **both** `sync_partial_err` and `sync_full`, because the refusal falls through to a full transfer the primary actually paid for — Redis's rule, matched exactly. A primary that has served no `PSYNC` reports all three as `0`, and that zero now means "nothing has synced", which is a different statement from the one the field used to make — and a primary whose statistics were reset reports `0` again, the counters being lifetime totals *since the last* `CONFIG RESETSTAT` (FM-REPLICATION-058), which is the only thing that moves them backwards. Both renderers report the same three values for the same tracker state, always. |
| NOT observable | **Three hardcoded zeros.** Every field was the literal `0` in *both* `INFO` renderers, so a primary full-resyncing every replica on every reconnect — the exact symptom of a mis-sized or disabled backlog — was indistinguishable from a primary whose replicas had never dropped. These are the documented signal for backlog sizing; a constant makes the sizing decision unmakeable while looking like data. **A refused partial that moves only `sync_partial_err`.** Redis increments `stat_sync_full` on that path too; counting only the refusal hides the transfer, and makes `sync_full` disagree with the number of checkpoints cut. **A first attach counted as a refused partial.** `PSYNC ? -1` did not ask for a partial, so charging it to `sync_partial_err` would make every healthy cold start look like a backlog miss — the classification keys on whether the replica named a history, exactly as Redis's `master_replid[0] != '?'` does. **The two renderers disagreeing.** They are separate code paths over separate data sources, which is precisely how both came to hardcode zeros independently; the fields are now one list (`sync_counter_fields`) that both consume, so a field cannot be added to one and forgotten in the other. **Counters that live on the handler rather than the tracker**: the tracker is the one object both renderers can already reach in either role. **A grant counted before its tail exists.** `+CONTINUE` is written before the backlog is extracted, and the window can close in between (FM-REPLICATION-012) — the link then drops without a byte of the resume being streamed, the replica reconnects, and the reconnect is refused. Counting the grant at the fork reports a partial resync that served no data, and then counts the same replica's failure a second time; `sync_partial_ok` climbing while replicas are in fact full-resyncing on every reconnect is the original bug wearing a different mask. |
| Invariant | `SyncOutcome::classify(requested_id, granted_partial)` (`sync_counters.rs`) is the whole rule and is pure: a grant is `PartialOk` whatever id was sent; otherwise the literal `"?"` sentinel is `FullResyncRequested`; any other id is `PartialRefused`. It deliberately does **not** key on `FullResyncReason`, because `PartialSyncReplay::can_replay` returns `Disabled` before it inspects the sentinel, so a `PSYNC ? -1` against a disabled backlog would be misclassified as a refused partial. `SyncCounters` is three `AtomicU64`s with one writer, `record`, whose `PartialRefused` arm increments `partial_err` **and** `full` — the one place Redis's fall-through rule is expressed. Recording is split across the two moments the two arms become true. `handle_psync` records the **refusals** — `SyncOutcome::classify(replication_id, false)` — immediately after the decision and before registration, so they move on the same decision the wire reply is minted from (FM-REPLICATION-013's two arms) and the full transfer the primary has committed to is counted whether or not the session survives it. A **grant** is recorded by `start_streaming`, once `extract_backlog` has returned the tail and before it is written: that is the instant the promise in `+CONTINUE` is backed by data, and it is where Redis counts it too (after `addReplyReplicationBacklog`). `ResumeSource::{PartialGrant, FullSnapshot}` is what tells the streamer which fork reached it; the full-sync fork records nothing there, having been counted at the decision. An abandoned resume is recorded nowhere — the reconnect it forces is a real second `PSYNC` and is counted on its own. `ReplicationTrackerImpl` owns the counters and exposes `sync_counters() -> SyncCountersSnapshot`; `info_handler` reads it role-independently into `ReplicationSnapshot.sync`, and `info::sync_counter_fields(snapshot) -> [(&'static str, u64); 3]` is the single name-to-value list that `StatsSection::render` and `build_stats_info` both render, in the same order, in the Redis-documented position in the `stats` section. |
| Outcome variant | n/a (`INFO stats` fields `sync_full`, `sync_partial_ok`, `sync_partial_err`); `SyncOutcome::{PartialOk, FullResyncRequested, PartialRefused}` |
| Forced by | `each_psync_fork_moves_the_counter_it_is_named_after`, `a_granted_continue_is_partial_ok_whatever_id_was_sent`, `the_question_mark_sentinel_is_a_request_not_a_refusal`, `a_full_resync_after_a_named_id_is_a_refused_partial`, `counters_start_at_zero_and_each_outcome_moves_only_its_own_fields`, `a_refused_partial_advances_both_partial_err_and_full`, `both_info_renderers_report_the_same_sync_counters`, `an_untouched_primary_reports_zero_sync_counters`, `a_resume_evicted_after_the_grant_is_abandoned_not_truncated` |
| Bug refs | `.scratch/hardening/issues/done/17-sync-counters-in-info-are-hardcoded-to-zero.md` (named as a live deviation in FM-REPLICATION-013's non-guarantees and in the deviations table) |

**Two renderers is the hazard, not the zeros.** Both were independently wrong in the same way, which
is what a duplicated surface produces. They were not collapsed into one — `build_stats_info` serves
the shard-local `INFO` a script sees and has no access to the connection's `InfoSources` — but the
*fields* were: `sync_counter_fields` is the only place the three names are spelled, and
`both_info_renderers_report_the_same_sync_counters` asserts the two agree for one state, so the next
field added to one and not the other fails a test rather than shipping.

---

## FM-REPLICATION-051 — the replica applier delivers a group to its tagged shard, or fails loudly

| Field | Value |
|---|---|
| Trigger | The replica's consume loop hands a reconstructed group to the [`ReplicaApplier`] seam (`replication-runtime/src/executor.rs`), which is the only place a replicated frame becomes a shard message. Sharpened by the four shapes the seam must tell apart: a bare command (`len == 1`), a reconstructed `MULTI … EXEC` (`len > 1`), an empty group (`len == 0`, e.g. `MULTI` immediately followed by `EXEC`), and a frame whose origin-shard tag names a shard this node does not have — the shape a primary with a larger `cluster.shard_count` produces. |
| Observable | A bare command arrives at the tagged shard as exactly one `CoreMsg::Execute`, and a group of N arrives as exactly one `CoreMsg::ExecTransaction` carrying all N commands in order — one shard message either way, never N. Both carry `REPLICA_INTERNAL_CONN_ID`, so a replicated write is not re-broadcast out of the replica. Every failure the shard can produce is classified and returned to the consume loop, which latches divergence: a `-ERR`/blob-error response becomes `ApplyError::Rejected` carrying the shard's own text, an aborted or errored transaction becomes `Rejected` too, a closed or silent shard channel becomes `ShardUnavailable`, and an out-of-range tag becomes `ShardOutOfRange` **before** anything is sent. |
| NOT observable | **A group silently doing nothing.** Every whole-function mutant of this seam returns `Ok(())`, which is exactly a replica that consumes the stream, ACKs the offset, reports `master_link_status:up`, and applies none of it — an empty replica that vouches for the primary's history and, once promoted, serves it. **A group split into per-command applies** (see FM-REPLICATION-034), which makes intermediate transaction state readable on the replica. **A failure swallowed into `Ok`**: a rejected command, an aborted transaction, or a dead shard that does not reach the divergence latch (FM-REPLICATION-010) leaves the replica permanently forked while claiming to be in sync. **An out-of-range tag routed anyway** — clamping or wrapping onto a shard this node does have would apply the write to the wrong keyspace rather than refusing the stream. **An empty group that blocks**: a `MULTI`/`EXEC` pair with nothing between it must reach no shard *and* not wait on one, or the consume loop stalls behind a reply that will never come. |
| Invariant | Routing is by the frame's origin-shard tag only, validated in `sender_for` against `num_shards` *and* against the wired sender vector (`get(idx)`), so a `num_shards` that over-states the senders still refuses rather than panicking. `apply_group` dispatches on `commands.len()` with three arms and no fallthrough: `0 => Ok(())` (no send), `1 => apply_single`, `_ => apply_transaction`. Both apply paths follow the same shape — send, then *check the reply*: `Response::Error | Response::BlobError` and `TransactionResult::{Error, WatchAborted}` are the only rejection shapes, and each is converted to `ApplyError::Rejected { shard, detail }` with the shard's own reason preserved for the operator. A send error or a dropped oneshot on either path is `ApplyError::ShardUnavailable(shard_id)`. |
| Outcome variant | `ApplyError::{ShardOutOfRange, ShardUnavailable, Rejected}`; surfaces to the operator as the latched divergence of FM-REPLICATION-010 (`INFO replication`), never as a silent no-op |
| Forced by | `a_single_replicated_command_executes_directly_on_its_tagged_shard`, `a_reconstructed_transaction_is_one_atomic_shard_message`, `an_empty_group_reaches_no_shard`, `a_refused_command_is_reported_as_a_divergence_with_its_reason`, `a_failed_transaction_is_reported_as_a_divergence`, `an_origin_shard_tag_outside_the_shard_count_is_refused_before_any_send`, `a_shard_that_is_gone_or_silent_is_reported_as_unavailable` |
| Bug refs | none open; row added by the `frogdb-replication-runtime` mutation round (the seam was previously exercised only end-to-end, so every whole-function `-> Ok(())` mutant survived) |

---

## FM-REPLICATION-052 — a received live dataset is re-partitioned onto this node's shards, whole or not at all

| Field | Value |
|---|---|
| Trigger | A replica installs a `FullSyncPayload::LiveDataset` — the payload a primary running with `persistence.enabled = false` sends (FM-REPLICATION-001). Sharpened by the two nodes disagreeing on `cluster.shard_count`, by a dataset that leaves one of this node's shards with no keys at all, by a corrupted or truncated blob, and by a shard that dies mid-install. |
| Observable | After the install this node's keyspace is the primary's, partitioned by **this** node's hash space: every key lands on `shard_for_key(key, local_shard_count)` regardless of which of the primary's blobs carried it, so the two shard counts need not agree. Every local shard receives exactly one `ReplicationMsg::InstallSnapshot` — including shards the dataset has no keys for, which receive an empty entry list. Anything that goes wrong fails the whole install: the replica does not adopt the offset, and retries the full resync. |
| NOT observable | **A shard skipped because the dataset had no keys for it.** An install is a *replace*; a shard that is never sent an `InstallSnapshot` keeps its own forked keys, and the node then serves a keyspace that is part primary and part its own history — the precise bug the live-install seam exists to close. **Keys routed by the primary's partitioning**, i.e. blob index used as the destination shard: with mismatched shard counts every subsequent read of a re-hashed key misses on a node that holds it. **A partially decoded dataset installed anyway** — a bad blob must fail the install rather than silently drop the keys it carried, because the replica would then claim to hold them. **An install with no shards wired reporting success**, which is a full resync into nothing. **A missing ack treated as done**: a shard that took the message and died has not installed it, and continuing would leave that shard forked. |
| Invariant | `install_dataset` refuses a zero-shard node up front, then decodes on a blocking task (whole-keyspace CPU) via `route_dataset`, which allocates `vec![Vec::new(); num_shards]` — the vector is sized by *this* node's shard count, so the per-shard fan-out exists before any key is placed and empty shards keep their slot. Each decoded key is bucketed by `shard_for_key(&entry.key, num_shards)`; a blob that fails `read_entries` aborts the whole function (`?`) with the blob index named. `install_per_shard` then walks `0..num_shards` in order, awaiting each shard's ack before the next, and converts a closed channel into `shard <n> is gone` and a dropped oneshot into `shard <n> dropped the install ack`. Expired keys are dropped at the *source* (the exporting shard), so nothing here second-guesses a TTL against the local clock. |
| Outcome variant | n/a (internal; a failed install surfaces as the full resync being retried, and as `master_link_status` never reaching `up` for that attempt) |
| Forced by | `a_received_dataset_is_repartitioned_onto_this_nodes_shards`, `every_shard_is_installed_including_the_ones_with_no_keys`, `a_dataset_install_with_no_shards_wired_is_refused`, `a_blob_that_does_not_decode_fails_the_whole_install`, `a_shard_that_never_acks_the_install_fails_it` |
| Bug refs | `.scratch/testing-improvements/issues/61` (the live-install seam this row pins); `.scratch/testing-improvements/issues/67` (the payload it installs) |

---

## FM-REPLICATION-053 — a received checkpoint installs every shard of the staged DB, warm tier materialized

| Field | Value |
|---|---|
| Trigger | A replica installs a `FullSyncPayload::StagedCheckpoint` — the payload a primary with RocksDB sends. Sharpened by a primary running the warm tier (`tiered_storage.enabled = yes`), by a key present in both tiers, by a warm key whose TTL has already passed, and by a staged directory this node cannot open at all. |
| Observable | The staged database is scanned **shard by shard** and each shard's entries are installed into the *same-numbered* local shard: the checkpoint's partitioning is adopted as-is, not re-derived, so a key the primary placed in CF 3 arrives on local shard 3 even where `shard_for_key` would say otherwise. Warm-tier keys are materialized as ordinary hot keys (their values live in the staged DB, which is discarded after the install, so they cannot stay warm); the receiving node re-demotes them under its own tiering policy. Where the same key exists in both tiers the **hot** copy wins, matching boot recovery. A warm key already past its TTL is not resurrected. A staged directory that cannot be opened fails the install loudly and leaves every shard untouched — and the *reason* it could not be opened decides whether the sync is retried at all: a layout this node can never adopt is refused terminally (FM-REPLICATION-061), anything else stays retryable. |
| NOT observable | **A checkpoint install that touches no shard and still reports success** — the replica adopts the offset with its old keyspace intact and is then permanently forked while reporting `master_link_status:up`. **Warm keys dropped**: the warm tier is part of the primary's keyspace, and a checkpoint that installs only the hot tier is silent data loss that `DBSIZE` on the replica under-reports. **A warm copy shadowing a newer hot one** for the same key, which would roll that key back to its pre-demotion value. **An expired warm key resurrected** with a deadline already in the past — it would be readable on the replica until the next expiry pass and would diverge from the primary, which does not hold it. **A shard of the staged DB skipped**: an install is a replace, so an unvisited shard keeps this node's own keys. **A half-read checkpoint installed**, which is `FLUSHALL` plus a subset. |
| Invariant | `read_snapshot` opens the staged directory with `RocksStore::open_with_warm(dir, num_shards, config, warm_enabled)` and routes any failure through `classify_open_failure`, so nothing is installed unless the whole DB opened; a shard-count or warm-tier disagreement becomes a terminal `InstallError::Incompatible` naming both sides and every other failure a retryable `InstallError::Transient(failed to open staged checkpoint: …)` (FM-REPLICATION-061). It then loops `0..num_shards`, driving `recover_shard_into` per shard into a fresh `SnapshotSink` — the same recovery path boot uses — and pushes each sink's entries in shard order, so the returned vector is positionally the install plan. `SnapshotSink::absorb_warm` runs only when `warm_enabled`, skips entries that fail `deserialize` (warning, not aborting — a single unreadable warm value must not fail a whole resync), skips `metadata.expires_at.is_some_and(\|at\| at <= now)`, and skips keys already present via `RestoreSink::contains`, which is what makes hot win over warm. `install_per_shard` is shared with the live-dataset path, so ordering, acks and failure wording are identical (see FM-REPLICATION-052). |
| Outcome variant | n/a (internal; a *transient* failed install surfaces as the full resync being retried, a structural one as `INFO`'s `master_sync_error` — FM-REPLICATION-061) |
| Forced by | `a_staged_checkpoint_is_read_shard_by_shard_and_installed_into_each`, `warm_tier_keys_are_materialized_and_a_hot_copy_wins`, `an_expired_warm_key_is_not_resurrected`, `a_checkpoint_this_node_cannot_read_is_refused_and_touches_no_shard` |
| Bug refs | none open. **Structural constraint, now handled rather than merely known:** the checkpoint path requires the two nodes to agree on `cluster.shard_count` and on `tiered_storage.enabled`. The staged DB is opened with *this* node's values, and `ColumnFamilyManifest::reconcile` (`persistence/src/rocks/manifest.rs:76-103`) rejects the mismatch with `ShardCountMismatch` / `WarmTierMismatch`. Such a replica still cannot complete a full resync from a persistent primary — that part is unchanged — but it no longer *retries forever* while saying nothing an operator can read: the refusal is terminal and named in `INFO` (FM-REPLICATION-061, issue 23). The live-dataset path (FM-REPLICATION-052) has no such constraint. |

---

# GAPS — behavior nothing forces (do not name a test; these need one written)

**GAP-1 — CLOSED** by FM-REPLICATION-046. The freshness filter is forced by
`count_good_replicas_excludes_a_stale_replica_but_zero_disables_the_check`,
`count_good_replicas_ignores_non_streaming_replicas` and
`ack_is_fresh_excludes_an_ack_exactly_at_the_window`. The gap predicted the `<`→`<=` mutant would
survive every unit test in the crate, and it was right for a reason worth keeping: the comparison
sat on a live `Instant::elapsed()` line, so **no** test could separate `<` from `<=` without landing
exactly on the window. The boundary was extracted into the pure `ack_is_fresh(ack_age, window)`
predicate, which both write gates now share, and is asserted on an age rather than a clock.

**GAP-2 — CLOSED** by FM-REPLICATION-049 and FM-REPLICATION-060 together. FM-REPLICATION-049 removed
the constant: `ReplicaLine` carries a `ReplicaState` projected from the replica's `Phase`, so a
rendered line states that replica's own phase instead of a literal
(`a_slave_line_is_projected_from_the_replica_not_from_literals`). FM-REPLICATION-060 then made the
projection observable: both renderers feed `info::rendered_replicas(tracker)` over
`get_all_replicas()`, so `wait_bgsave` and `send_bulk` reach a client rather than living only in the
projection's unit tests, and `connected_slaves` counts the lines actually rendered
(`a_replica_being_fed_a_checkpoint_is_rendered_as_send_bulk`,
`connected_slaves_counts_every_rendered_line`). The two halves had to land in that order: widening
the feed while the state was a literal would have printed `online` for a replica holding none of the
primary's data, which is precisely the mislabelling the gap named.

**What `get_streaming_replicas()` is still for**: the acked-offset projection — `WAIT`, `ROLE`,
quorum — which is deliberately narrower than what `INFO` renders and must stay so. `INFO
replication` answers "what is attached and what is it doing"; `WAIT` answers "how many replicas hold
this offset". Reading `connected_slaves` as the second question is the misreading FM-REPLICATION-043
now names outright.

**GAP-3 — CLOSED** by FM-REPLICATION-049. `ReplicaLine.lag_secs` is `ReplicaInfo::lag_secs()`, the
whole-second age of the replica's last ACK, which is the same `ack_age_secs(last_ack_time)` measure
`replica_lag_secs` hands to `LagPolicy` — so `INFO` and the proactive-disconnect policy read one
number and cannot drift. Forced by `a_slave_line_reports_the_real_ack_age` and
`lag_secs_is_the_age_of_the_last_ack`. The integration assertion the gap also asked for (a stalled
replica reporting a non-zero `lag` in `test_info_replication_shows_all_replicas`) was **not** added:
it needs a replica held ACK-silent for a wall-clock second, which is a sleep in the integration
suite for a value the unit tests pin exactly.

**GAP-4 — CLOSED** by FM-REPLICATION-046. Of the three remedies this gap offered, the fix takes two
together: the millisecond field is exposed under its own CONFIG name (`min-replicas-max-lag-ms`,
exact at every magnitude) and the Redis-spelled seconds view rounds **up**, so a round trip may
widen the window but can never report a live window as `0`. Rejecting sub-second values — the third
remedy — was **not** taken: it would break the project's own `min_replicas_config()` fence tests and
every deployment with a sub-second TOML value. The original write-up follows.

<details><summary>Original GAP-4 (kept for the reasoning)</summary>

**A sub-second `min-replicas-timeout-ms` round-trips through `CONFIG` as 0, which silently *disables* the lag filter.**
`frogdb-server/crates/server/src/runtime_config.rs:2028-2046` — `get` is
`min_replicas_timeout_ms / 1000` and `apply` is `secs * 1000`, integer arithmetic in both directions.
A config file with `min-replicas-timeout-ms = 500` (the value `min_replicas_config()` uses in the
fence tests, `integration_replication.rs:6611`) makes `CONFIG GET min-replicas-max-lag` answer `0`;
feeding that value straight back with `CONFIG SET` — which is what every config-dump/restore and
`CONFIG REWRITE` round trip does — turns the freshness filter **off**, so a dead-but-not-yet-reaped
session counts as a good replica and the `NOREPLICAS` gate stops protecting anything. The TOML field
is milliseconds, the `CONFIG` name is Redis seconds, and nothing rejects a sub-second value.
Full write-up under `<!-- BUGS -->` at the top of this fragment.
*Test that should exist:* `min_replicas_max_lag_round_trips_without_losing_a_sub_second_window` in
`runtime_config.rs` — set the TOML field to 500 ms, `CONFIG GET`, `CONFIG SET` the result back,
assert the effective window is not 0 (either by rejecting sub-second values, by rounding up to 1 s,
or by exposing the ms field directly).

</details>

**GAP-5 — CLOSED** by `INV-SESSION-2` and `test_wait_never_exceeds_connected_slaves`, both named in
FM-REPLICATION-039. The gap was that every existing test asserts a specific expected count against a
known-stable replica set, and none asserts the *bound* across a reconnect — where a double-count
would come from (a session lingering in `Streaming` while its replacement registers under a new id;
the registry is a `HashMap`, but `unregister_replica` runs from the old session's exit handler, so
the overlap window is real). It is now covered from both directions. `INV-SESSION-2`
(`frogdb-server/crates/replication/src/invariants.rs`) is the universal half: at most one live
session per announced identity, checked over the whole `ReplicationView` at every seam that takes
one, so the overlap cannot exist unobserved anywhere rather than only where a test looks.
`test_wait_never_exceeds_connected_slaves` (`integration_replication.rs`) is the end-to-end half:
`WAIT 5 200` through eight rounds of link churn — `REPLICAOF NO ONE` then `REPLICAOF` back at the
same primary, so the replica re-announces the *same* identity from the same port rather than
arriving as a new follower — asserting the returned count never exceeds `connected_slaves` sampled
either side of the call, and that the set reconverges afterwards so a bound held by simply losing
the replica does not pass. The seeded-sweep cross-check `XREPL-3` will add a third witness over
generated schedules when its issue lands; the bound does not depend on it.

**GAP-6 — CLOSED** by `wait_released_by_a_demotion_reports_the_role_change_even_if_client_unblock_races`,
named in FM-REPLICATION-040's `Forced by`. The race the gap describes — a `CLIENT UNBLOCK ERROR`
landing in the same poll as the demotion, where the `biased;` ordering makes the role change win and
the client gets `-UNBLOCKED ... (master -> replica?)` rather than the CLIENT UNBLOCK message — is now
a contract rather than an accident of arm order. Pinning it needed the same treatment `reconcile_ack`
got: the `select!` moved out of `ConnectionHandler::handle_wait_command` into the free function
`resolve_wait_race` (`frogdb-server/crates/server/src/connection/blocking.rs`), which takes the wait
future, an `UnblockSignal` and a count closure, so a test can present *both* arms ready in one poll —
an interleaving a live socket reaches only by luck. The test asserts the reply is the role-change
error and that the acked count is never even computed (the closure panics if called), and its
siblings pin the other three corners: a reached quorum also beats a same-poll unblock, ERROR mode
still releases a genuinely parked wait, and TIMEOUT mode still answers with the count. The mock
unblock source is shared with the blocking-command coordinator's tests
(`blocking/coordinator.rs::test_support`), so both races read the CLIENT UNBLOCK edge through one
seam. The `MULTI`/deny-blocking half of the gap was already covered: WAIT inside `MULTI`/Lua returns
the live count immediately and never blocks, so there is no parked wait for a
demotion to release (FM-REPLICATION-037).

**GAP-7 — CLOSED.** `write_fence_reason`'s only test,
`write_fence_reason_is_reported_only_while_fenced`
(`frogdb-server/crates/telemetry/src/status.rs`), was uncitable because `frogdb-telemetry` was not
in `NEXTEST_CRATES`, leaving FM-REPLICATION-041's status-endpoint claim unforced. `frogdb-telemetry`
and `frogdb-config` are now both eligible (`scripts/spec-lint.py`), and the test is named in
FM-REPLICATION-041's `Forced by`. The second crate was added for the same reason: config
`validate()` tests are the forcing tests for every "rejected at boot" clause, and they all live in
`frogdb-config`.

---

## Tagging notes for whoever lands these

- Three candidate tests already carry other-area tags and were **kept out** of the `Forced by` cells
  to avoid a second tag: `test_self_fence_multi_rejected_at_queue_time` (`FM-TXN-007`, :6735),
  `test_self_fence_multi_partial_queue_aborts_whole_transaction` (`FM-TXN-008`, :6788),
  `test_min_replicas_to_write_multi_and_lua_paths` (`FM-TXN-007`, :6932). The `MULTI`-queue-time
  behavior each row describes is still true; if you want it forced here, add a second
  `// FM-REPLICATION-NNN` line — the lint accepts multiple tags per definition. Same applies to the
  `test_wait_inside_multi_*` family (`FM-TXN-044`, :630/:719/:814), which is why
  FM-REPLICATION-037's deny-blocking sentence names no test of its own.
- Two tests are named by two rows each and so take two tags: `test_get_streaming_replicas`
  (037 + 040) and `test_failover_chain_survivor_reattaches_to_promoted_node` (038 + 041).
- `test_wait_during_replica_resync` was **rejected** as a forcing test when this spec was written:
  it early-`return`ed with `eprintln!("Replica did not connect, skipping test")` when
  `connected_slaves:1` was absent, so it could pass without asserting anything. Issue 19 removed the
  skip and issue 28 gave it the durability assertion it was missing — a counted ack now has to be a
  replica that serves the write — so it is named by FM-REPLICATION-037.
- `test_wait_blocks_until_ack` (:484) asserts only `acked >= 0` — it forces nothing about WAIT's
  count and is listed nowhere above.

---

## FM-REPLICATION-054 — a state-changing FUNCTION subcommand crosses the link, and only those

| Field | Value |
|---|---|
| Trigger | A client runs `FUNCTION LOAD`, `DELETE`, `FLUSH` or `RESTORE` on a primary that already has a replica attached. The function registry is process-wide state that lives beside the keyspace — one `SharedFunctionRegistry` per node, persisted to its own `functions.fdb` — so nothing about the write path that carries keys touches it. |
| Observable | The replica ends up with the same libraries: `FCALL_RO` of a replicated function answers on the replica, `FUNCTION LIST` shows the library, and a `FUNCTION DELETE` of one library removes exactly that one and leaves the others callable. |
| NOT observable | **A primary and its replica disagreeing about which libraries exist** — the whole bug (issue 48): no `FUNCTION` subcommand was ever propagated, so every replica answered "Function not found" for every library its primary held, and `FCALL` after a failover failed permanently. Nor the near misses: a read-only subcommand (`LIST`, `DUMP`, `STATS`, `HELP`, `KILL`) generating a frame, which would put a client's `FUNCTION LIST` into the offset stream and make `WAIT` account for it; a failed mutation (bad shebang, duplicate library without `REPLACE`) propagating anyway; a `DELETE` applied as a flush. |
| Invariant | One owner, one gate, one lane. `FunctionStore` is the sole mutator of the registry — the connection handlers and the replica's apply loop call the same four methods — so "what replicates" and "what a replica applies" cannot drift into two lists; `MUTATING_SUBCOMMANDS` is that list, and it is the same constant the propagation check reads. Propagation is *post-hoc*: `mutate_functions` runs the mutation first and returns early on `Response::Error`/`BlobError`, so only an effect that actually happened is broadcast (Redis's verbatim propagation of `FUNCTION` has the same shape). The frame is tagged `CONTROL_SHARD` (`u16::MAX`) rather than a real shard id, because the registry is per-process: shard-tagging would make the frame undeliverable the moment the two nodes' shard counts differ. The replica applies it through `ControlApplier`, a *synchronous* trait invoked inline in the consume loop, so a control frame can neither be reordered against the surrounding write stream nor block it on someone else's I/O; a rejected apply is a divergence (`ApplyError::ControlRejected`), not a skipped frame. |
| Outcome variant | `ApplyError::ControlRejected`; frames tagged `CONTROL_SHARD` |
| Forced by | `a_function_loaded_on_the_primary_reaches_an_attached_replica`, `function_delete_removes_one_library_on_the_replica_and_keeps_the_rest`, `only_the_four_state_changing_subcommands_replicate` |
| Bug refs | `.scratch/testing-improvements-round2/issues` (issue 48 — this row is its outcome) |

**Not covered here.** `FCALL` effects are *not* propagated as function calls: a script's writes
already replicate as their individual effects through the ordinary write path, which is what makes a
non-deterministic function safe. This row covers the library definitions only.

---

## FM-REPLICATION-055 — a full resync carries the primary's whole function registry

| Field | Value |
|---|---|
| Trigger | A replica attaches to a primary that already holds libraries — every library loaded before the link existed is invisible to the steady-state stream, and a restart of an existing replica reaches the same state. Sharpened by a replica that booted with its *own* `functions.fdb` holding libraries the primary does not have. |
| Observable | After the sync the replica can call the primary's pre-existing libraries, and holds exactly the primary's set — a library only the replica had is gone, the same wholesale-replacement rule the keyspace half of a full sync follows (FM-REPLICATION-002). |
| NOT observable | A replica reporting `master_link_status:up` while missing libraries the primary has held since before it attached; a half-installed registry (some libraries replaced, others not) visible to an `FCALL` racing the install; the registry state depending on which of a concurrent client mutation and a full sync happened to reach the wire first. |
| Invariant | The registry rides the link as a replicated command, not as part of the payload envelope: a `function_snapshot_hook` on the primary broadcasts one whole-registry `FUNCTION RESTORE <dump> FLUSH` frame, invoked *after* the session captures `snapshot_offset` (`SessionDriver::new`) and before the `+FULLRESYNC` reply goes out, therefore inside the `(snapshot_offset, current]` range `start_streaming` replays before the live tail — the one window a post-capture broadcast is guaranteed to land in. The policy is `FLUSH` (not `APPEND`/`REPLACE`) so the replica converges on the primary's exact set rather than the union; `FunctionStore::restore` performs the flush and the loads under a single write lock, so no intermediate set is observable. Ordering against concurrent client mutations is a process-global `PROPAGATION_ORDER` mutex held across *mutate-then-broadcast* on the client path and across *snapshot-then-broadcast* on the sync path, which is what stops a snapshot read before a `LOAD` from being broadcast after that `LOAD`'s own frame and silently erasing it. |
| Outcome variant | n/a (rides the replayed backlog window; surfaces as the replica's `FUNCTION LIST`) |
| Forced by | `a_replica_that_full_syncs_receives_the_primarys_existing_libraries`, `a_full_grant_publishes_the_function_registry_ahead_of_its_reply`, `a_full_grant_without_a_function_hook_sends_only_the_reply` |
| Bug refs | `.scratch/testing-improvements-round2/issues` (issue 48 — this row is its outcome) |

**Why not in the payload.** `FullSyncMetadata` is a strict four-part colon-joined trailer, and the
file list in the `$FROGDB_CHECKPOINT` envelope is staged for RocksDB to open — neither has room for
an unrelated blob, and the `LiveDataset` branch has no file list at all. Carrying the registry as a
replicated command instead means one mechanism serves both sync flavours and the steady-state
stream, at the cost of the dump being re-sent on every resync (bounded by the library set, which is
operator-authored and small).

---

## FM-REPLICATION-056 — a promoted replica keeps the libraries it replicated

| Field | Value |
|---|---|
| Trigger | `REPLICAOF NO ONE` (or an automatic failover) on a replica that received libraries over the link. |
| Observable | The promoted node serves them as a primary: `FCALL` — the write-capable entry point, not only `FCALL_RO` — of a replicated function answers on the new primary. |
| NOT observable | Libraries vanishing at promotion, which is what made the missing propagation dangerous rather than merely incomplete: a failover would silently drop every library and every `FCALL` against them. Nor libraries that only work while the node is a replica. |
| Invariant | A replicated `FUNCTION` frame is applied into the node's *own* `SharedFunctionRegistry` and persisted to its own `functions.fdb` by the same `FunctionStore` a client mutation would use — there is no replica-only shadow copy and no borrowed state on the link, so promotion is a role change with nothing to migrate. |
| Outcome variant | n/a |
| Forced by | `a_promoted_replica_keeps_the_libraries_it_replicated` |
| Bug refs | `.scratch/testing-improvements-round2/issues` (issue 48 — this row is its outcome) |

---

## FM-REPLICATION-057 — a replica refuses client-driven registry mutations

| Field | Value |
|---|---|
| Trigger | A client sends `FUNCTION LOAD` / `DELETE` / `FLUSH` / `RESTORE` directly to a replica. |
| Observable | `-READONLY You can't write against a read only replica.`, the same reply a write to a replica's keyspace gets. Read subcommands (`LIST`, `DUMP`, `STATS`, `HELP`) are still served. |
| NOT observable | A library existing on a replica that its primary has never seen — invisible upstream, surviving until some later `FUNCTION` frame happened to overwrite it, and promotable into the authoritative set by a failover. Nor the opposite over-correction: the whole `FUNCTION` container being refused on a replica, which would break `FUNCTION LIST` on the node an operator inspects most. |
| Invariant | The gate is per-subcommand, checked in `mutate_functions` against the same `is_replica` flag the keyspace write path uses, and it sits *before* the `PROPAGATION_ORDER` lock and the mutation, so a refused call touches nothing. It cannot be expressed as a command flag: `FUNCTION_SPEC` carries only `CommandFlags::NOSCRIPT` (the container has read subcommands), so the generic `WRITE`-flag gate in `guards.rs` never fires for it — which is exactly why the hole existed. Redis draws the same line at the same place, flagging `function|load` as a write while `function|list` is not. |
| Outcome variant | `-READONLY` |
| Forced by | `a_client_can_not_load_a_function_on_a_replica` |
| Bug refs | `.scratch/testing-improvements-round2/issues` (issue 48 — found while fixing it) |

---

## FM-REPLICATION-058 — `CONFIG RESETSTAT` zeroes the resync counters, and nothing else replication owns

| Field | Value |
|---|---|
| Trigger | `CONFIG RESETSTAT` on a node whose replication tracker has recorded `PSYNC` outcomes — the reset an operator issues *before* watching a maintenance window, so that what they read afterwards is the window and not the lifetime. Sharpened by a node with no tracker at all (standalone), and by resyncs that happen after the reset. |
| Observable | All three of `sync_full`, `sync_partial_ok` and `sync_partial_err` read `0` afterwards, through **both** `INFO stats` renderers, and resume counting from that zero on the next `PSYNC`. A node with no replication tracker still answers `+OK`. The reset does not disturb the replication *state* it sits next to: the registered replicas, `connected_slaves`, `master_repl_offset` and per-replica lag are unchanged, so `INFO replication` reads the same before and after. |
| NOT observable | **A RESETSTAT that leaves the three counters standing.** This was the shipped behaviour: `config_resetstat` reset the shard stats, call counts, error stats, latency histograms and keyspace counters and could not reach the counters at all, so an operator who reset before a window kept reading lifetime totals and had to diff by hand against a reading taken earlier — while every *other* field in the same `INFO stats` section had been rebased, which is the part that misleads. **A partial reset** — zeroing `sync_full` but not the two partial counters, or either renderer disagreeing about which are zero — makes the three mutually inconsistent (`full < partial_err` is impossible by FM-REPLICATION-050's fall-through rule) and would read as corruption. **Counters that stop counting after a reset**: RESETSTAT rebases a statistic, it does not retire it. **A "reset the replication stats" that also clears live state** — dropping the replica registry, zeroing `master_repl_offset` or forgetting the lag-disconnect history would make an observability command a replication event, and `master_repl_offset` is the stream position, not a statistic; zeroing it would desynchronize every attached replica's `WAIT` accounting. |
| Invariant | `SyncCounters::reset()` stores `0` into all three atomics and is the only operation that moves them backwards (absent it they are monotone); it is `Relaxed`, so it tears against a concurrent `record` exactly as `snapshot` does, which is acceptable for the same reason — an operator issuing RESETSTAT has declared the previous values uninteresting, so which side of the boundary a racing `PSYNC` lands on cannot mislead. `ReplicationTrackerImpl::reset_sync_counters()` forwards to it and touches nothing else the tracker owns. The command reaches the tracker through `ConnCtx::replication_tracker`, an ambient `Option<&ReplicationTrackerImpl>` layered by `base_ctx` from `ClusterDeps` — the same concrete handle `CommandContext` already carries, not a bespoke provider — and `config_resetstat` is `if let Some(tracker) = ctx.replication_tracker`, so an absent tracker is a skipped reset and never an error. The reset is role-independent, matching the read side (FM-REPLICATION-050 reports the counters whatever role is running). |
| Outcome variant | n/a (`+OK`; `INFO stats` fields `sync_full`, `sync_partial_ok`, `sync_partial_err`) |
| Forced by | `reset_zeroes_all_three_counters`, `counters_keep_counting_after_a_reset`, `resetting_the_sync_counters_leaves_the_replica_registry_and_offset_alone`, `config_resetstat_zeroes_the_sync_counters`, `config_resetstat_without_a_replication_tracker_is_ok`, `both_info_renderers_report_zeros_after_the_counters_are_reset` |
| Bug refs | `.scratch/hardening/issues/done/24-config-resetstat-does-not-reset-the-sync-counters.md` |

**The issue's suggested remedy was not taken as written.** Issue 24 asked for "a `&dyn` provider with a
`reset` verb … `ConnCtx` is a `core` type and must not depend on the replication crate". That premise
is false in the current tree: `frogdb-core` already depends on `frogdb-replication`, re-exports
`ReplicationTrackerImpl` from its root, and `CommandContext` already carries
`Option<&Arc<ReplicationTrackerImpl>>` for exactly this reason. A provider trait would have added a
third seam (trait + no-op + layering method) to express a dependency that already exists, so `ConnCtx`
carries the handle directly and matches its sibling context type.

**Audit of the other replication-owned `INFO stats` fields** (the issue's third remedy).
`sync_counter_fields` and, since issue 29, `net_byte_fields` are the only live replication data in
either `stats` renderer. `total_net_repl_input_bytes` / `total_net_repl_output_bytes` were the literal
`0` in both renderers with no source behind them at all — a hardcoded literal shaped like data, filed
as `.scratch/hardening/issues/open/29-net-repl-byte-counters-are-hardcoded-zero.md` when closing issue
20 did not reach them (the backlog geometry had a live source to project from and those two had none).
They are real counters now (FM-REPLICATION-063) and `CONFIG RESETSTAT` zeroes them exactly as it
zeroes the three counters this row owns, so nothing in `stats` is replication-owned and unresettable.

---

## FM-REPLICATION-059 — the backlog geometry both renderers report is the live ring buffer's

| Field | Value |
|---|---|
| Trigger | `INFO replication` on a node whose `replication.backlog-max-mb` is not the default, in each state the buffer can be in: never written to (no window armed), written to (armed at the first retained offset), evicted past (floor raised), reset by a `PSYNC` epoch change, and on a replica, which owns the same buffer unarmed. Sharpened by reading the section the *other* way a client can — `redis.call('INFO', 'replication')` from inside a script, which renders on the shard rather than on the connection. |
| Observable | `repl_backlog_size` is the node's configured byte cap, in both roles and through both renderers, so an operator who tuned it can confirm the change landed. `repl_backlog_active` is `1` exactly when a resume window is armed and `0` otherwise — a disabled backlog reports `0` beside its configured size, never a size that implies a buffer that is filling. `repl_backlog_first_byte_offset` is the armed eviction floor: the same offset `PartialSyncReplay` refuses a `PSYNC` below (FM-REPLICATION-014), so an operator diagnosing "why does every reconnect full-resync" reads the boundary that is actually deciding it. `repl_backlog_histlen` is `master_repl_offset − first_byte_offset` — the servable window in stream bytes — and is `0` while unarmed. The four are a set: `floor + histlen` never exceeds the head. Both renderers report the same four values for the same tracker state, and a scripted `INFO` reports the *node's* replication facts rather than the "replication is not configured" defaults — which also makes a scripted `ROLE`, `WAIT` and `CLUSTER INFO` read the live offset and replica set. |
| NOT observable | **`repl_backlog_size:1048576` on a node configured for anything else.** It was a literal in both renderers, printing Redis's default back at an operator who had tuned FrogDB's — the field's only use is confirming a config change, so a constant makes it worse than absent. **`repl_backlog_first_byte_offset:0` beside an armed floor.** `0` asserts the backlog can serve from the beginning of history, which is the exact claim the floor exists to deny (FM-REPLICATION-014), so the operator diagnosing repeated full resyncs concludes the backlog is not the cause. **`repl_backlog_active` derived from the replica count** — replicas attached and a window armed are different facts, and the pre-fix render could print `active:0` beside a non-zero `first_byte_offset`, which is self-contradictory on its face. **`repl_backlog_histlen` equal to `master_repl_offset`** — that is the whole history, not the retained window, and it overstates what a reconnecting replica can resume from by everything already evicted. **A window that claims to extend past the head** (`floor + histlen > master_repl_offset`) or a `histlen` that underflowed a floor above the head, which a seeded position can transiently produce. **The two renderers disagreeing**: this is the failure that produced issues 17 and 20 twice over, so the four fields are emitted from one list or there is no contract. **A scripted `INFO replication` reporting `connected_slaves:0`, `master_repl_offset:0`, zeroed sync counters and an all-zero backlog on a primary with live replicas** — the shard-local sub-command context dropped the tracker, so every tracker-backed reader inside a script rendered the no-replication defaults with full confidence. |
| Invariant | `ReplicationRingBuffer::geometry(current_offset)` is the only producer of the four values: `active`/`first_byte_offset` come from `start_offset()` (the `AtomicI64` that `fetch_max` arms and eviction raises — the same one `can_replay` consults), `size_bytes` is the buffer's own `max_bytes`, and `histlen` is a `saturating_sub` so a floor momentarily above the head yields `0` rather than a wrapped `u64`. The buffer is an `Arc` owned by `PartialSyncReplay`; `PrimaryReplicationHandler::new` publishes that handle into `ReplicationTrackerImpl::publish_backlog` immediately after constructing the replay, so both renderers reach the *live* buffer through the tracker — the FM-REPLICATION-050 pattern, and the only object both INFO renderers hold in either role. `backlog_geometry_fields(geometry)` is the single `[(name, value); 4]` list: `sections.rs` writes it through the `StatsSection` writer and `commands/info.rs` formats the same list into its string, in both role branches, so a fix cannot land in one renderer only. The shard-local renderer sees a tracker at all because `ScriptInvoker::from_context` clones `ctx.replication_tracker` and `run_local` re-attaches it to the sub-command context, alongside the replica identity it already propagated. |
| Catalog | `INV-BACKLOG-1`, `INV-BACKLOG-3` — the geometry both renderers report is the live ring's own floor and byte count, and the entries hold those to a hole-free range inside both caps, so a rendered geometry cannot describe a buffer that could not exist. |
| Outcome variant | `INFO replication` fields `repl_backlog_active`, `repl_backlog_size`, `repl_backlog_first_byte_offset`, `repl_backlog_histlen` |
| Forced by | `the_handler_publishes_its_backlog_to_the_tracker`, `backlog_geometry_reports_the_configured_cap_and_the_armed_floor`, `backlog_geometry_first_byte_offset_follows_eviction`, `backlog_geometry_after_a_reset_claims_no_window`, `backlog_geometry_histlen_never_underflows`, `the_backlog_size_reported_is_the_configured_one`, `the_first_byte_offset_reported_is_the_armed_floor`, `both_info_renderers_report_the_same_backlog_geometry`, `a_replica_reports_its_configured_capacity_with_no_window`, `info_reports_the_configured_backlog_geometry_through_both_renderers`, `run_local_propagates_the_replication_tracker`, `run_local_without_a_tracker_sees_none` |
| Bug refs | `.scratch/hardening/issues/done/20-backlog-geometry-in-info-is-hardcoded.md` |

**Two fixes the issue did not ask for, taken deliberately.** Issue 20 scoped itself to
`repl_backlog_size` and `repl_backlog_first_byte_offset` and explicitly excluded the other two as
"projected from real state already". They were not. `repl_backlog_active` was `!replicas.is_empty()`
— a fact about attachments, not about the window — and pairing a projected floor with it would have
let INFO print `repl_backlog_active:0` beside `repl_backlog_first_byte_offset:9000`. `repl_backlog_histlen`
was `repl_offset`, the entire history rather than the retained window, which is the same overstatement
`first_byte_offset:0` made in the other coordinate. Both now derive from the same
`BacklogGeometry`, so the four fields cannot disagree with each other. While there, the shard-local
replica branch's hardcoded `master_repl_offset:0` was replaced with the node's real offset — the
literal FM-REPLICATION-043's NOT-observable clause already forbade, still present in the renderer
that clause did not name.

**The scripted-INFO gap was found by the issue's own test, not by the issue.** The renderer fix
passed at the unit level and the end-to-end assertion through `redis.call('INFO')` still read
`repl_backlog_size:0`: `ScriptInvoker` carried the store, the shard routing and the replica identity
across the seam but not the replication tracker, so every tracker-backed reader inside a script had
been rendering the no-replication defaults since the seam was introduced. That is a wider bug than
this row's four fields — a scripted `WAIT` returned `0` acks on a primary with acked replicas, and a
scripted `ROLE` reported no replicas and offset `0` — and it is fixed here rather than filed because
the issue's acceptance test cannot pass without it.

---

## FM-REPLICATION-060 — every attached replica INFO can name is rendered, in the state it is actually in

| Field | Value |
|---|---|
| Trigger | `INFO replication` on a primary while a replica is mid-attach — handshaking, being fed a checkpoint, or streaming — and while one is being torn down; and two renders of an unchanged registry, to pin the `slaveN:` indices. |
| Observable | A replica that is dialling or waiting for its checkpoint renders `state=wait_bgsave`, one receiving the payload renders `state=send_bulk`, one caught up renders `state=online` — so an operator watching a resync sees it progress rather than seeing nothing until it completes. `connected_slaves` equals the number of lines rendered, always, because it is that vector's length. Lines are ordered by attach: replica ids come from a monotonic counter, so `slave0:` is the longest-attached peer and two renders that observed no registry change produce the same indices. A replica being torn down (`Phase::Disconnecting`) renders no line and is not counted, matching Redis. Both renderers agree, including the shard-local one a script reads. |
| NOT observable | **A syncing replica rendered as nothing at all.** Both renderers fed `get_streaming_replicas()`, so a replica that had dialled, announced itself, and was being fed a multi-gigabyte checkpoint was indistinguishable in `INFO` from a replica that did not exist — during exactly the window an operator opens `INFO replication` to watch. `connected_slaves:0` on a primary actively serving a full sync is missing data reported as a fact, and the operator's next move (assume the replica is misconfigured, restart it, restart the sync) is worse than doing nothing. **`state=online` for a replica that is not streaming**: the widened feed must not reach the wire without the FM-REPLICATION-049 projection in front of it, or it prints a ready failover target that holds none of the primary's data. **`connected_slaves` counted from `get_all_replicas()` while the lines are filtered** — the count and the lines must fall out of the same `filter_map`, not out of two traversals that a later filter change can separate (this is FM-REPLICATION-043's rule, and the shape that makes it structurally true). **`slaveN:` indices that reshuffle between two renders of an unchanged registry**: the registry is a `HashMap`, whose iteration order is not stable, so an operator (or a diff of two INFO captures) would see replicas swap positions with nothing having happened. The same reshuffling reaches `ROLE`, whose replica array is positional on the wire. **A `Phase` with no Redis spelling rendered under an invented state word**, or dropped from the lines while still counted. |
| Invariant | One feed: `info::rendered_replicas(tracker)` maps `tracker.get_all_replicas()` through `ReplicaLine::from_replica`, and both `connection/info_handler.rs` (via `PrimarySnapshot.replicas`) and `commands/info.rs` call it and nothing else — `connected_slaves` is `replicas.len()` in both. `ReplicaState::from_phase(phase) -> Option<Self>` is total over `Phase` and returns `None` only for `Disconnecting`; `from_replica` propagates that `None` with `?`, so "no Redis spelling" removes a replica from the count and the lines in one step. `get_all_replicas` and `get_streaming_replicas` both `sort_unstable_by_key(|r| r.id)` before returning: ids come from the tracker's monotonic `next_replica_id`, so id order *is* attach order, and the sort is what converts `HashMap` iteration into a stable projection for `INFO` and for `ROLE`'s positional array alike. `get_streaming_replicas` keeps its `Phase::Streaming` filter and stays the acked-offset projection for `WAIT`, `ROLE` and quorum — the two feeds are deliberately different questions. |
| Catalog | `INV-SESSION-1` — the phases this row renders only move forward, and nothing leaves `Disconnecting`, so a rendered state is never a session's second pass through one. |
| Outcome variant | `INFO replication` fields `connected_slaves`, `slaveN:...,state=...` |
| Forced by | `a_replica_being_fed_a_checkpoint_is_rendered_as_send_bulk`, `connected_slaves_counts_every_rendered_line`, `a_disconnecting_replica_is_not_rendered`, `rendered_replicas_are_ordered_by_attach` |
| Bug refs | `.scratch/hardening/issues/done/21-info-renders-only-streaming-replicas.md` |
| Model | `replication_fullsync.qnt::inv_link_points_at_a_live_session` |

**This is a wire-visible change, taken on the accuracy rule.** `connected_slaves` grows to include
syncing replicas, and `state` values other than `online` now reach clients. A client that treats
`connected_slaves` as "replicas that can serve a read or take a failover" reads a larger number than
before — but it never had that meaning: that question is `WAIT`'s, over `get_streaming_replicas()`,
which is unchanged. The alternative, keeping the narrow feed, means a primary that is actively
serving a full sync reports `connected_slaves:0`, which is the misleading-data failure the repo's
observability rule ranks below missing data. Redis renders all three states, so this also moves
toward parity rather than away.

**Ordering was not in the issue.** Rendering the registry unfiltered exposed that neither getter
sorted, so `slaveN:` indices came out in `HashMap` order. The sort was added to *both* getters, not
only the INFO feed, because `ROLE`'s replica array is positional too and would reshuffle between two
calls that observed no change. Forced by `rendered_replicas_are_ordered_by_attach`.

---

## FM-REPLICATION-061 — a full resync this node can never install is refused once, not retried forever

| Field | Value |
|---|---|
| Trigger | A replica receives a `FullSyncPayload::StagedCheckpoint` whose on-disk layout it cannot adopt — the primary's checkpoint carries a different `cluster.shard_count`, or a warm tier this node has disabled (`ColumnFamilyManifest::reconcile` rejects it). Contrasted throughout with an install that merely failed (a shard gone, a staged directory that is not a database), and with the operator's recovery: the same `REPLICAOF host port` re-issued after the configuration is corrected. |
| Observable | The install is refused, no shard is touched, the offset is rewound to 0 — and the **reconnect loop stops**. The primary is asked for exactly one full resync, not one per backoff tick. The replica's `INFO replication` holds `master_link_status:down` and adds `master_sync_error:<detail>`, which names **both** sides of the disagreement (`written with 2 shard(s)` / `configured for 1`; `this node has tiered-storage.enabled = false`) — neither node's own config shows the mismatch, so a one-sided message sends the operator to the wrong host. Both INFO renderers report it, including the shard-local one a script reads. An ordinary install failure keeps the old behaviour exactly: reconnect, retry, latch nothing, render no `master_sync_error`. Re-issuing `REPLICAOF` at the **same** primary after a refusal opens a fresh stream, where it is still a no-op for a link that has not given up. `promote()` clears the refusal with the stream that held it. |
| NOT observable | **A misconfigured pair retrying forever.** Every attempt made the *primary* cut and ship a whole checkpoint, which the replica then threw away — unbounded primary disk and network burned on a condition no retry can change, for as long as the two configs disagree (issue 23). **`master_link_status:down` as the only signal.** Down means "no data arriving right now", which is usually a restarting primary; it cannot distinguish "reconnecting" from "will never connect again", and only the second one needs a human. That is why the refusal is a *separate* field whose mere presence is the alert, rather than a new `master_link_status` value that would break every existing `up`/`down` consumer. **A refusal reported in the log only.** The pre-fix code did log the mismatch — once per attempt, scrolling past — and an operator diagnosing a replica reads `INFO`. **`master_sync_error` rendered empty, or rendered for a transient failure**: a field that is present on every disconnect carries no information; absence is what makes presence actionable. **A transient failure classified as terminal**, which strands a replica that would have recovered on its own — the classification defaults to `Transient` (`From<io::Error>`) so an unclassified failure keeps retrying. **A refused node that cannot be re-armed**: `demote()`'s same-target no-op answered "already replicating" to the one command the operator has to fix this with, leaving the node down forever with its configuration now correct. **The refusal surviving in only one INFO renderer**, the split this path has already produced three times (FM-REPLICATION-043, -059, -060). |
| Invariant | The installer seam returns `Result<(), InstallError>` with two variants: `Incompatible { detail }` (terminal) and `Transient(io::Error)` (retry), and `impl From<io::Error> for InstallError` yields `Transient`, so the *default* is the old behaviour. `install::classify_open_failure` maps `RocksError::ShardCountMismatch` / `WarmTierMismatch` to `Incompatible` with a detail naming both sides, and every other open failure to `Transient`. `ReplicaConnection::install_payload` latches an `Incompatible` detail into the handler's shared `sync_refusal: Arc<RwLock<Option<String>>>` before converting the error for the wire, so it outlives the connection that learned it. The reconnect loop's `Err(e) if self.sync_refusal().is_some()` arm logs and **returns** instead of backing off. `ReplicaReplicationHandler::sync_refusal()` reaches INFO through `ReplicaStream::sync_refusal` → `RoleManager::sync_refusal` (same active-stream selection as `link_up`, `None` when not a replica) → `RoleController::sync_refusal` → `ShardIdentity::master_sync_error()` → `ShardDiagnostics` / `CommandContext`, and both renderers emit it through the single `info::replica_link_fields(link_up, sync_error)` list. `RoleManager::demote` skips its idempotence no-op when `sync_refusal().is_some()`. |
| Outcome variant | `INFO replication` field `master_sync_error` (FrogDB-specific); the wire error `full resync refused: <detail>` |
| Forced by | `an_incompatible_install_latches_the_refusal_and_a_transient_one_does_not`, `a_geometry_mismatch_is_refused_once_and_not_retried`, `a_transient_install_failure_is_still_retried`, `a_checkpoint_this_node_cannot_read_is_refused_and_touches_no_shard`, `master_sync_error_derives_from_role_controller`, `sync_refusal_reads_through_the_active_stream`, `re_demoting_to_the_same_primary_retries_a_refused_stream`, `re_demoting_to_the_same_primary_is_still_a_no_op_when_nothing_was_refused`, `a_replica_that_gave_up_names_the_mismatch_in_info`, `a_link_that_is_merely_down_renders_no_sync_error`, `both_renderers_report_the_same_replica_link_block` |
| Bug refs | `.scratch/hardening/issues/done/23-geometry-mismatched-replica-full-resyncs-forever.md` |

**What this does not do: make the mismatch work.** Repartitioning a staged checkpoint into a
different shard count is a real option (the live-dataset path already routes every key through
`shard_for_key`, so the machinery exists), and it is deliberately out of scope here: it would turn a
configuration error into a silent adoption of a layout the operator did not ask for, and the warm-tier
case has no equivalent answer at all. The contract is fail-fast with a named cause; correcting the
configuration stays the operator's move. See the issue's "Open question" for the alternatives weighed.

**Why the loop returns rather than idling.** `start()` returning `Err` is what a boot-configured
replica's supervisor already treats as "this subsystem stopped"; keeping the task alive with a
paused loop would need a wake path that only `demote()` can trigger, and `demote()` already builds a
*fresh* stream. Stopping is the smaller mechanism, and the state is fully readable from INFO after
the task is gone because the refusal is latched in the handler, not in the loop.

---

## FM-REPLICATION-062 — a streaming replica's departure is classified, and only a graceful one disarms the self-fence

| Field | Value |
|---|---|
| Trigger | A `ReplicaSession` that reached `Phase::Streaming` ending its `run()`. Every exit: an orderly EOF on the read half, a read error, a write error or write timeout, a lag-threshold disconnect, a WAL-broadcast `Lagged` overrun, the broadcaster closing (primary shutdown), a frame that cannot be encoded, a primary-initiated `request_disconnect()` (demotion / decommission), and an error propagated out of the partial/full sync phases. Also the sessions that end *before* streaming — a handshake that dies mid-checkpoint has no departure to classify. |
| Observable | Only through the self-fence (FM-REPLICATION-041), which is the single consumer: after the last streaming replica leaves **gracefully** the primary keeps accepting writes, and after it is **lost** the primary refuses them with `SELFFENCE …` until a fresh replica streams and ACKs. Two operator-visible shapes follow. A replica decommissioned on purpose — `REPLICAOF NO ONE`, an orderly shutdown, a primary-side teardown — leaves a writable primary. A replica that was killed mid-stream, partitioned away, or disconnected for lag leaves a fenced one. |
| NOT observable | **A departure recorded for a session that never streamed**: a link that died during its checkpoint transfer never armed the fence (FM-REPLICATION-041) and must not be able to disarm it either — sampling the phase *after* the exit handler sets `Disconnecting` would record every dead handshake as a departure. **An unknown departure read as graceful**: a bare `unregister_replica()` (every caller that is not the session's own exit handler, including tests and `disconnect_all_replicas`) records nothing, and nothing must mean *keep fencing* — the permissive default is the one failure of this seam that silently un-fences a primary. **A stale classification outliving a newer one**: the record is last-writer-wins over streaming departures, so the most recent one decides; a graceful departure from an hour ago must not disarm a fence armed by the replica that was lost since. **A departure answering for a replica that arrived after it**: a record left by the previous replica is cleared the moment the next one starts streaming, so it can never be read as *this* replica's ending — without the clear, a predecessor's clean departure would still be on record while the current replica's link dies, and the fence would read the death as a decommission. **The record consulted while a replica is still registered as streaming**: a silent-but-attached session is the partition case, and FM-REPLICATION-041's disarm is gated on `has_streaming_replica()` being false precisely so a stale link cannot be mistaken for a departed one. |
| Invariant | `ReplicaDeparture::{Graceful, Lost}` lives in `frogdb-replication` beside the session that produces it. `SessionDriver::drive` returns `io::Result<ReplicaDeparture>` and every `?` failure maps to `Lost` (`LinkOutcome::Errored` → `Lost`), so a new error path is classified conservatively without being touched. The exit transition — `session_machine::step` on `SessionEvent::Ended` — reads the phase the sync left off on from the view it is handed, i.e. **before** it returns `Phase::Disconnecting`, and emits `Effect::RecordDeparture(kind)` only if that phase was `Phase::Streaming` — **ahead of** `Effect::Unregister` in the same effect list, because the fence's disarm reads "nothing is streaming" and "the last departure was graceful" as two separate loads: unregistering first would open a window in which the session is gone and its own record has not landed, so a predecessor's graceful departure would answer for a link that actually died. Both orderings are properties of the returned list rather than of the async exit handler, so a test asserts them without a socket. The record is also **cleared as a replica enters `Phase::Streaming`** (`clear_streaming_departure()`, driven by the `Effect::ClearDeparture` the transition that publishes that phase emits), so a departure never outlives the replica generation it describes and a new replica always begins from the unknown — i.e. fencing — state. The tracker stores it in an `AtomicU8` (`0` = none/unknown, `1` = graceful, `2` = lost) alongside its other cross-session state (`sync_counters`, `lag_disconnect_times`, `backlog`), because it must outlive the session map it describes; `last_streaming_departure() -> Option<ReplicaDeparture>` is the only reader. Classification, exhaustively: read half `Ok(0)` → `Graceful`; read half `Err` → `Lost`; `forward_frame` returning `Break` (write error or write timeout) → `Lost`; a lag-threshold breach → `Lost`; `RecvError::Lagged` → `Lost`; a frame that fails `encode()` → `Lost`; `RecvError::Closed` (the primary's own broadcaster went away) → `Graceful`; `disconnect_requested()` → `Graceful`. |
| Catalog | `INV-SESSION-3`, `INV-FENCE-1` — a recorded streaming departure implies a session that actually reached `Streaming`, and only a graceful one disarms the checker. |
| Outcome variant | n/a (internal; the only wire-visible consequence is FM-REPLICATION-041's `SELFFENCE` refusal appearing or not appearing) |
| Forced by | `an_orderly_eof_is_a_graceful_departure`, `a_read_error_is_a_lost_departure`, `a_lag_disconnect_is_a_lost_departure`, `a_primary_initiated_disconnect_is_graceful`, `a_session_that_never_streamed_records_no_departure`, `a_new_streaming_generation_clears_the_previous_departure`, `only_a_session_that_reached_streaming_reports_a_departure`, `the_departure_is_recorded_before_the_session_leaves_the_registry`, `an_error_out_of_any_phase_is_a_lost_departure`, `the_last_streaming_departure_wins`, `a_graceful_departure_disarms_the_fence`, `a_lost_departure_keeps_the_fence_armed`, `an_unrecorded_departure_keeps_the_fence_armed`, `a_registered_but_silent_replica_still_fences`, `a_disarmed_fence_re_arms_on_the_next_streaming_replica`, `test_self_fence_disarms_after_a_clean_replica_departure` |
| Bug refs | `.scratch/hardening/issues/done/30-self-fence-flake-and-misleading-clusterdown.md` |
| Model | `replication_feed_gate.qnt::inv_departure_matches_its_cause`, `replication_feed_gate.qnt::inv_graceful_end_flushed_everything`, `replication_feed_gate.qnt::inv_superseded_writes_no_departure`, `replication_feed_gate.qnt::inv_departure_record_backed_by_an_ended_session` |

**What this seam deliberately does not claim.** A replica that is killed outright still closes its
socket the way an orderly one does — the kernel sends the same FIN — so "graceful" here means
*the link ended the way a link that is finished ends*, not *a human intended it*. The distinction
this row buys is therefore between **silence** (a replica that stopped answering: timeouts, lag,
errors, a partition — the case the fence exists for, where the missing replica may be alive and
promotable elsewhere) and **closure** (a replica that is demonstrably gone from this primary's
point of view). Making the stronger claim would need an in-band farewell the replica sends before
it closes, which the replica's teardown path cannot await today (`RealReplicaStream::drop` is
synchronous and aborts the connection task). That is a real follow-up, not a pretence this row
makes.

---

## FM-REPLICATION-063 — `total_net_repl_*_bytes` count bytes that actually crossed the wire

| Field | Value |
|---|---|
| Trigger | Any replication transfer, in each of the two lanes a node's transfer total is made of: the full-sync payload (a `FullSyncPayload::LiveDataset` or `StagedCheckpoint`, whichever a primary sends per FM-REPLICATION-001) and the steady-state frame stream, on both sides — a primary forwarding a live write or replaying a resumed replica's backlog tail, and a replica decoding what it received. Then `INFO stats` read through either renderer, exactly as FM-REPLICATION-050's `sync_*` counters are. |
| Observable | `total_net_repl_output_bytes` on a primary grows by the real encoded size of every frame actually written to the wire — a live-forwarded write, a backlog tail replayed to a resuming replica, or a full-sync payload — the instant that write lands, never before. `total_net_repl_input_bytes` on a replica grows by the same measure on the receiving side: the checksum-verified size of an installed full-sync payload, and the exact byte span `drain_frames` consumes from its read buffer per decode pass. A primary only ever moves `output`; a replica only ever moves `input` — nothing here double-counts a byte as both a primary's send and its own receipt within one process. Both renderers report the same pair for the same tracker state, always, and `CONFIG RESETSTAT` zeroes both (FM-REPLICATION-058) without disturbing the replica registry, the offset, or the sync counters next to them. |
| NOT observable | **Two hardcoded zeros.** Both fields were the literal `0` in *both* `INFO` renderers with nothing in the codebase counting a byte — a primary saturating a slow link and a primary that has replicated nothing were indistinguishable, which is exactly the plausible-looking-but-fake shape the counters exist to rule out. **Bytes counted as sent before the write lands.** `forward_frame` returning `Break` means the write did not land, or landed partially; counting `encoded.len()` unconditionally (rather than only on `Forward::Continue`) would report transfer that never happened. **The backlog-replay tail left uninstrumented.** A resumed replica's tail is written directly via `stream.write_all`, a second output path distinct from the live-forward one — a build that counts only the live path undercounts every partial resync by the size of the tail it replayed. **Bytes derived from the offset coordinator instead of the wire.** The offset advances once per *stamped* write, not once per *sent* byte, and never accounts for a full-sync payload at all; deriving the counters from it would report a number that moves independently of anything a socket did. **A replica counting its own full-sync payload before the checksum is verified**, which would let a corrupted transfer that gets rejected and retried still inflate the tally with bytes that were discarded. |
| Invariant | `NetByteCounters` (`net_bytes.rs`) is two `AtomicU64`s (`output`, `input`) with `record_output`/`record_input` no-oping on `bytes == 0`, `Relaxed` for the same reason `SyncCounters` is (FM-REPLICATION-050): a monotone diagnostic tally nothing derives a decision from tolerates benign tearing. `ReplicationTrackerImpl` owns one `Arc<NetByteCounters>`, vended cross-crate by `net_bytes_handle()` (`pub`, unlike the crate-local `offset_handle()`, because the counters are recorded from `frogdb-server`'s handler-construction call sites as well as from within `frogdb-replication`) and read back by `net_bytes() -> NetByteCountersSnapshot`; `reset_net_bytes()` is `SyncCounters::reset`'s sibling, zeroing only this pair. Four call sites record output, all on the primary: `stream_checkpoint` and `stream_live_dataset` each record `total_size` — the same checksum-verified size already stamped into `FullSyncMetadata.rdb_size` — once, after `write_metadata` succeeds; the live-forward path in `start_streaming`'s write task records `encoded.len()` only on `Forward::Continue`, never on `Break`; the backlog-replay tail loop in the same function records `encoded.len()` per frame immediately after its `write_all` succeeds, independently of the live-forward path (a replayed frame is never also live-forwarded, so the two sites cannot double-count one frame). Two call sites record input, both on the replica: `receive_snapshot` and `receive_checkpoint` each record `metadata.rdb_size` after their checksum/commit step confirms it real; `replica/streaming.rs`'s `drain_frames` records `starting_len - buf.len()` — the exact span `ReplicationFrameCodec::decode`'s loop consumed from the read buffer — at both of its exits (channel closed early, and loop completion), so a partial drain still counts what it actually decoded. `info::net_byte_fields(snapshot) -> [(&'static str, u64); 2]` is the single name-to-value list both `StatsSection::render` and `build_stats_info` render, in Redis's field position (immediately after `total_net_output_bytes`, before the `instantaneous_*` fields), the same pattern `sync_counter_fields` established. |
| Outcome variant | n/a (`INFO stats` fields `total_net_repl_input_bytes`, `total_net_repl_output_bytes`) |
| Forced by | `net_bytes_handle_is_shared_with_the_tracker_snapshot`, `resetting_the_net_bytes_leaves_the_replica_registry_and_offset_alone`, `repl_output_bytes_grow_when_a_write_is_forwarded`, `repl_output_bytes_include_the_full_sync_payload`, `repl_input_bytes_grow_on_the_replica_as_it_applies`, `repl_input_bytes_count_only_whole_frames_leaving_partial_carryover_uncounted`, `repl_input_bytes_count_only_the_frame_actually_sent_when_the_channel_closes_mid_drain`, `set_net_bytes_counters_wires_the_handlers_own_connections_to_it`, `both_info_renderers_report_the_same_repl_byte_counters`, `both_info_renderers_report_zeros_after_the_repl_byte_counters_are_reset`, `config_resetstat_zeroes_the_repl_byte_counters` |
| Bug refs | `.scratch/hardening/issues/done/29-net-repl-byte-counters-are-hardcoded-zero.md` |

**Why the offset coordinator was rejected as the source** (considered and dropped, not merely
unmentioned). The tracker's offset advances once per write *stamped* into the stream, which is a
count of writes, not of bytes on a socket: it cannot see a full-sync payload at all (no frame, no
offset movement), and it advances even for a write whose frame later fails to reach a given replica —
the offset is "how far the primary's history has moved", not "how much this link has transferred".
Counting bytes at the write sites that already know the real, on-the-wire (or checksum-verified) size
is the only shape that cannot be fooled by a write that was stamped but never sent.

---

## FM-REPLICATION-064 — the announced replica version is read at the handshake, and a different major is refused there

| Field | Value |
|---|---|
| Trigger | A `PSYNC` arriving on a connection that announced `REPLCONF frogdb-version <v>` (FM-REPLICATION-049) — in every shape a peer can produce it: the same version as this primary, a patch-only difference, a minor difference, a different major in *either* direction (a replica ahead of the primary and one behind it), a version carrying a semver pre-release/build suffix (`2.0.0-rc1`, `1.5.0+build7`), a version with no readable minor (`1`, `1.x`), a string with no readable major at all (`banana`, `v1.2.3`, `""`), and no announcement whatsoever — plus the total-function edge where *this primary's* own version string is unreadable. Then the primary-side surfaces a refusal must leave untouched: the replica registry `INFO replication` / `ROLE` render from, the `sync_*` counters, and the checkpoint a `+FULLRESYNC` would cut. |
| Observable | A replica whose announced major differs from this primary's is **refused at the `PSYNC`**: it is sent the RESP error `-ERR PSYNC refused - replica announced FrogDB version <theirs> (major <M>) but this primary is FrogDB version <ours> (major <N>); replication requires both ends on the same major version. Run the replica on a <N>.x build, or move this primary to <M>.x, then let it reconnect.` — one line, naming both versions verbatim, both majors, the rule, and both remedies — and the handshake ends with `io::ErrorKind::InvalidData`. A FrogDB replica surfaces that same text in its own log (`PSYNC error: ERR PSYNC refused - …`), so either operator can act from the node they are watching. The refusal leaves **no trace on the primary but its log line**: no session in the registry, so `INFO replication` / `ROLE` report no such replica and `connected_slaves` does not move; no `sync_full` / `sync_partial_err` (the refusal is not a resync this primary served); no checkpoint cut. A replica on the same major but a different **minor** is *served* — the stream is granted exactly as it would be for a matched pair, the session registers with the announced version on it — and the primary logs one `WARN` for that session naming `replica_addr`, `replica_version` and `primary_version`. A peer whose version is **unreadable or absent** is likewise served, with one `WARN` carrying the raw announced string (or `<unannounced>` when it announced nothing), so the case an operator most needs to see is the one the log spells out. A patch-only difference and an exact match are served silently. The warnings are once per *session*, at the `PSYNC` that creates it, so a steady stream never repeats them and a reconnect re-reports. |
| NOT observable | **A replica on an incompatible major that streams anyway.** This is the state issue 27 filed: `ReplicaInfo::replica_version` was recorded and *nothing in the workspace read it*, so the only surfaces that looked like a version gate — `ClusterCommand::FinalizeUpgrade`, which validates raft topology members rather than attached replicas, and the `frogdb_version_gate_active` gauge, which keys on the cluster's finalized `active_version` — said nothing about the replicas attached to a primary over `PSYNC`. A standalone primary with two replicas had no gate at all, while an operator watching those surfaces believed one was there. **A refusal for a peer that announced nothing.** Unknown is *unknown*, not old: refusing it would drop every pre-option replica and every non-FrogDB peer on a suspicion, and would do it to exactly the peers that cannot be told why. The gate refuses only what it can prove — both majors readable and different — which is why an unreadable *primary* version refuses nobody either. **A refusal issued after the session was registered, after a counter moved, or after a checkpoint was cut**: an operator would then see a phantom replica in `INFO`, `sync_full` climbing for streams that were never served, and disk spent on a payload nobody reads. The gate is the first thing after the shutdown-drain check for that reason. **A refusal enforced only at the `REPLCONF`.** The announcing option is the *record* step; a peer is free to log an error there and carry on to `PSYNC` (Redis replicas do exactly that for options a primary rejects), so a gate that lives only there fails open against the peer it exists to stop. **A refusal delivered as a bare socket close.** FM-REPLICATION-017's drain refusal is deliberately silent because a shutting-down primary has nothing actionable to say; this one is the opposite — the whole value is the message, and a closed socket with no error would leave the replica's operator with a reconnect loop and no reason. **An error naming only one side.** "Incompatible version" without both numbers makes the operator go read two `INFO`s to learn what the primary already knew. **A warning that reports a placeholder** — `0.0.0`, the empty string, or this primary's own version — in place of what the peer actually said, which is the same collapse of "did not say" into "said something" FM-REPLICATION-049 forbids on the recording side. **Per-frame or per-reconnect-attempt warning spam**, which would bury the refusal it sits next to. |
| Invariant | One rule, one site. `VersionVerdict::evaluate(primary, announced) -> VersionVerdict` (`frogdb-replication/src/version_compat.rs`) is the whole rule and is *total* over both operands: `parse_version` cuts the string at the first `-` or `+` (semver pre-release/build), takes the first dot-segment as the major — required, `u64`, or the string is unreadable — and the second as the minor when it parses, so `1.2.0-rc1` is `(1, Some(2))`, `1` is `(1, None)`, and `banana` / `""` / `v1` / ` 1.2.3` are `None`. The verdict is `IncompatibleMajor(MajorMismatch)` **only** when both sides parse and the majors differ; `MinorSkew` only when both minors parse and differ; `Unproven` whenever either side is unreadable or the replica announced nothing; `Compatible` otherwise. `MajorMismatch::wire_error()` is the single spelling of the refusal text (and its `Display`), so the string sent on the wire, the string in the primary's log, and the string the unit test asserts cannot drift. The primary half of the comparison is `PRIMARY_VERSION = env!("CARGO_PKG_VERSION")` of `frogdb-replication` — the same crate, and therefore the same workspace version, whose `replica/connection.rs` announces it from the other end, so a primary and a replica built from one tree cannot disagree about what "this version" is. The gate is called from exactly one place: `PrimaryReplicationHandler::handle_psync`, immediately after the `draining` check and **before** `record_sync_outcome` and `register_announced_replica`, which is what makes a refusal leave no registry, counter or checkpoint trace. It runs once per session because `handle_psync` runs once per session — that is the whole of the "once per replica session" property; there is no separate suppression state to get wrong. The refusal writes `-<message>\r\n` to the `BoxedStream` best-effort (a peer that will not read it is precisely the peer this gate exists for) and returns `io::Error::new(InvalidData, message)`; the two warning arms only log. `ReplicaAnnouncement::version` remains the verbatim record FM-REPLICATION-049 defines — this gate parses a *copy* at the moment of decision and never rewrites what the session stores. |
| Outcome variant | RESP `-ERR PSYNC refused - …` on the refusal, then the connection ends; `io::Result` error `InvalidData` out of `handle_psync`. Admissions produce no wire surface: `+FULLRESYNC` / `+CONTINUE` as usual (FM-REPLICATION-013). Log-only otherwise: one `ERROR` on the refusal, one `WARN` per session for `MinorSkew` / `Unproven`, nothing for `Compatible`. |
| Forced by | `an_identical_version_is_compatible`, `a_patch_only_difference_is_compatible_and_silent`, `a_minor_difference_is_admitted_and_reported`, `a_different_major_is_refused_in_either_direction`, `the_refusal_error_names_both_versions`, `an_unannounced_version_is_unproven_not_incompatible`, `an_unreadable_version_is_unproven_and_keeps_the_raw_string`, `an_unreadable_primary_version_refuses_nobody`, `a_pre_release_or_build_suffix_compares_on_its_numeric_core`, `a_version_without_a_readable_minor_compares_on_its_major`, `a_peer_built_from_this_tree_is_compatible_with_this_primary`, `psync_from_an_incompatible_major_is_refused_before_anything_is_registered`, `psync_from_a_minor_skewed_replica_is_served`, `psync_from_a_replica_with_an_unreadable_version_is_served`, `test_psync_is_refused_for_an_incompatible_replica_version` |
| Bug refs | `.scratch/hardening/issues/done/27-no-gate-reads-the-announced-replica-version.md` (filed by `.scratch/hardening/issues/done/22-replconf-frogdb-version-is-parsed-then-discarded.md`, whose remedy step 4 asked whether the consumer existed — it did not) |

**Why the gate is at `PSYNC` and not at the announcing `REPLCONF`.** The two commands are handled in
the same dispatch stage (`DispatchStage::ReplicationHandshake`), so either was reachable. `REPLCONF`
is the earlier of the two, but it is the step that *records* — a peer that gets an error there may
log it and send `PSYNC` anyway, which is what Redis's own replica does for options its primary
rejects, so a gate living only there is advisory. `PSYNC` is the moment this primary would commit
resources, and a refusal there is the only one the peer cannot proceed past.

**Why unknown is admitted here and must block at finalization.** FM-REPLICATION-049 says a consumer
gating on `replica_version` must treat `None` as blocking, and this row admits it. Both are right,
because the two gates take opposite actions: refusing to *finalize an upgrade* is reversible and
costs an operator a retry, so failing closed is free and failing open is a silent lie; refusing to
*replicate* costs availability and durability, so failing closed on an unprovable case would take
data paths down over peers that never had the option to announce. This row therefore refuses only
what it can prove, and makes everything it cannot prove loud. The finalization-side gate — a
readiness check an upgrade tool can query for a standalone primary's attached replicas — remains
unbuilt; it is the second half of issue 27's open question and is out of this row's scope.

---

## FM-REPLICATION-065 — a replica ignores a frame its applied head already covers

| Field | Value |
|---|---|
| Trigger | A frame reaches the replica's frame consumer whose whole byte span is already covered by what this node has applied — `frame.sequence <= ClaimedOffset`, the frame spanning `(sequence - stream_advance(), sequence]` (FM-REPLICATION-031). Every way the stream can produce one: a primary that replays a range the replica's claim already named (a resume computed against the wrong offset, an off-by-one in `extract_backlog`'s `offset > start` filter or `FeedSequencer::buffer`'s `sequence > resume_offset` drop, a backlog entry recorded twice), a re-delivery of a whole `MULTI … EXEC` group, and the boundary that decides the rule's shape: a frame ending *exactly* at the applied head. Sharpened by the frames this must **not** touch: a frame ending above the head, including one whose span *starts* below it, and a frame stamped with a history this node has replaced. |
| Observable | The re-delivered frame is ignored: it is not parsed, not routed to a shard, and not applied — a replica re-sent `INCR k` for a write it already holds still reads `k = 1`, and the same for every other verbatim-propagated command (`LPUSH`, `APPEND`, `SETRANGE`; `core/src/shard/post_execution.rs`, `replication_forms`). Neither offset moves: ClaimedOffset and LandedOffset are exactly where they were, so the replica does not credit the same bytes twice and its next `REPLCONF ACK` still names data it holds. The skip is **counted, never silent**: `ConsumeStats.skipped` tallies it for the stint's owner (reported in the consumer's shutdown line beside `frames_processed`/`errors`/`discarded`), a node-wide cumulative counter (`AppliedOffset::skipped`) makes it readable while the stint is still running, and each skip logs one `WARN` naming the frame's sequence, its shard, the applied head it fell under and the running total — because a skip is always evidence of a sender-side accounting bug, and a receiver that quietly absorbs one leaves nothing for an operator to act on. |
| NOT observable | **A re-delivered frame re-executing.** This is the hole the row closes: dedup was sender-side only (`primary/ring_buffer.rs` `offset > start`, `feed_sequencer.rs` `sequence > resume_offset`), keyed on the offset the replica *claimed*, so every path that computes that claim wrongly re-executed a non-idempotent command on the replica and left a keyspace that silently disagrees with the primary's while both nodes report the same offset. **A skipped frame moving ClaimedOffset or LandedOffset** — its bytes are inside the head by definition, so claiming them would push the replica's offset past the primary's and make it vouch for history that does not exist. **A silent skip**: no counter, no total, no log. **A frame ending above the head being skipped**, in whole or in part — the rule is whole-span coverage, not "starts below". A frame straddling the head is applied verbatim, sub-range and all; that residue belongs to TR-REPLICATION-034, which removes it at its source rather than here. **A skip decided before the history check**, which would let a frame from a replaced history be counted as a duplicate of the new history's data rather than discarded with the history it belongs to. **A skip that abandons an open `MULTI` group**, or one that arrives inside one: the applied head only moves at `EXEC`, by the whole group's byte total, so it never falls strictly inside a group's span — a re-delivered group is skipped whole, frame by frame, and `pending` is left untouched. **A skip that stops the consumer.** Ignoring is not divergence and not retirement: the link is healthy, the next frame above the head applies normally. |
| Invariant | Receiver-authoritative dedup, Raft-style: the replica's own applied head is the address, not the offset it once told a primary about. One predicate, one site — `ReplicaApplyStint::covers(end_offset)` (`replica/offset.rs`) is `end_offset <= self.current()`, and `consume_frames` (`apply.rs`) consults it exactly once per frame, immediately after the two history checks (the `epoch != stint.epoch()` drop and the open-group epoch abandon) and **before** `stream_advance()`, the payload parse and every `claim_or_stop!` site. Placing it there is what makes the dispositions total and ordered: replaced history → `discarded`, already covered → `skipped`, otherwise → the claim decides (TR-REPLICATION-014). The skip is a `continue` with no other state touched: no claim, no `land()`, no `admit_divergence`, `pending` untouched. `ReplicaApplyStint::record_skip()` returns the node-wide running total and is called *outside* the `warn!` (a `tracing` macro does not evaluate its fields when the event is disabled, which would make the counter depend on log level — the same rule `ReplicaTxnBound::record_abandoned` follows). The node-wide counter lives on `AppliedOffset` beside the heads it guards and is deliberately **not** cleared by `reset_pair`: a full resync replaces the history, not the evidence that this node was re-sent data. Because the check reads the head rather than the epoch, it holds against a sender bug that has not been imagined yet — which is the whole reason the guarantee is stated on the receiver. |
| Outcome variant | `ConsumeStats.skipped` (per stint); `AppliedOffset::skipped()` (node-wide, cumulative); one `WARN` per skipped frame. No wire surface — a skip changes nothing the primary can see except that the replica's ACK does not move. |
| Forced by | `a_frame_at_or_below_the_applied_head_is_skipped`, `a_frame_ending_exactly_at_the_applied_head_is_skipped`, `a_frame_ending_above_the_applied_head_is_applied`, `a_skipped_frame_neither_claims_nor_lands`, `a_re_delivered_group_is_skipped_frame_by_frame`, `skipped_frames_are_counted_on_the_stats_and_the_node`, `a_replaced_history_is_discarded_before_the_skip_is_considered` |
| Rulings | R7 of the quint-completeness campaign's 2026-08-20 post-execution rulings (`.scratch/formal-spec/2026-08-19-quint-completeness-campaign.md`) — ruled receiver-side skip over tightening the sender, so the guarantee covers any future sender-side accounting bug rather than the one instance found. Skipped frames are counted rather than silently dropped, following the R-21 precedent (an out-of-range ACK is ignored *and* counted, FM-REPLICATION-012). |
| Bug refs | `.scratch/replication-correctness/issues/done/34-replica-side-skip-below-applied-head.md` |
| Model | `replication_fullsync.qnt::inv_reapply_is_a_noop` |

**What this row does not cover, and where that class went.** The full-sync overship
(FM-REPLICATION-004: `snapshot_offset` is captured *before* the checkpoint is cut, so the payload
carries data the granted offset does not name) produces re-delivered frames that sit *above* the
replica's applied head, not at or below it. This row's rule correctly applies them — the replica
cannot tell *from an offset* that a checkpoint already contains a write the offset does not claim.
That class is now closed by FM-REPLICATION-066, which gives the replica the one thing the offset
could not carry: the payload's exact per-shard upper bound. The two rules are one dispatch and are
ordered — the head is consulted first, the floor second (`FrameDisposition`), because a frame
covered by both is a duplicate of data the head already counts and claiming its bytes again would
push this node's offset past the primary's. This row remains the safety net under the *accounting*:
whatever the sender believes about what the replica claimed, the replica answers with what it
actually holds.

---

## FM-REPLICATION-066 — a full sync ships its exact per-shard reach, and the replica skips what it already holds

| Field | Value |
|---|---|
| Trigger | Any full sync, healthy, on either payload path. `snapshot_offset` is captured before the drain and the cut, and the offset `fetch_add` lives in `ReplicationBroadcast` — the *last* write effect (`core/src/shard/post_execution.rs`, `WRITE_EFFECT_ORDER`) — so a write is drainable into the payload before it is even counted. The handoff then replays `(snapshot_offset, current]` over a keyspace that already holds part of that range. The live-dataset path has the same shape from its sequential per-shard export (`replication-runtime/src/export.rs`): no drain, no pause, each shard captured at a different instant. Sharpened by the two boundaries the rule turns on: a frame ending *exactly* at its shard's watermark, and a cross-shard transaction the capture fell between (shard A drained before it, shard B after). |
| Observable | The trailer carries a per-shard coverage vector alongside the single offset: `Y_s` is the offset of the last write shard `s` broadcast at the moment that shard's contribution was captured — its drain-ack on the checkpoint path, its export message on the live-dataset path. The replica installs the vector as per-shard **skip floors** with the offset it came with, and a shard-`s` frame whose span ends at or below `Y_s` is skipped instead of re-applied: a non-idempotent write pinned inside the capture→cut window (`INCR`, `LPUSH`, `APPEND`) is applied exactly once end to end, not twice. Unlike an FM-REPLICATION-065 skip, a floor skip **claims its bytes** — the effect really is held, and a node that skipped without claiming would stall below its own data and re-ask for a range it has. The skip is counted, never silent: `ConsumeStats.floor_skipped` per stint, `AppliedOffset::floor_skipped()` node-wide, one `DEBUG` per frame — `DEBUG` and not `WARN` because this is the expected outcome of a healthy sync, not evidence of a sender bug. Floors are replaced wholesale at each install and retire once ClaimedOffset reaches `max(Y_s)`. They are persisted with the offset they qualify — in the staged `replication_metadata.json` (FM-PERSISTENCE-039) and in the replication state file — so a crash between the install and the reconcile recovers both halves. While they are in force, a `+CONTINUE` carrying `granted_id` is refused (TR-REPLICATION-034): the replica rewinds to 0, drops the link, and the reconnect asks `PSYNC ? -1`. |
| NOT observable | **The overshipped range re-executing.** This is the defect the row closes, and it needed no link break, no failover and no fault: a healthy full sync into a `INCR`-shaped stream left the replica's keyspace silently disagreeing with the primary's while both nodes reported the same offset. **A floor skip that does not claim its bytes** — the frame is *above* the head, so not claiming would leave ClaimedOffset below data this node holds. **A frame skipped against another shard's watermark**: the vector is indexed by the *sender's* shard id, which every frame carries. **A watermark inferred for a shard the vector does not describe** — an index past the end is *absent*, and absent means "apply it", never "skip it" (a zero would mean the same here, but only by accident, and a trailer that lost a field must not read as a positive claim of no coverage: a four-field trailer is refused outright rather than decoded as an empty vector). **The floor consulted before the head**, which would claim a duplicate's bytes twice. **The floor consulted inside an open `MULTI` group**: every frame of a replicated group carries its origin shard and a shard broadcasts a whole transaction in one step, so `Y_s` always falls on one of that shard's group boundaries and never strictly inside a group's span. **A torn cross-shard transaction skipped whole or applied whole** — the payload holds exactly the per-shard halves the floors describe, so the mixed skip/apply across its *sibling* groups is the mend, and it must happen exactly once. **Floors surviving the install that replaced their dataset**, or an offset adopted without them: the pair describes one payload, and separating them is the bug they exist to prevent. **A write above `Y_s` reaching the payload** — on the checkpoint path the WAL flush engine is held between a shard's drain-ack and the cut, so nothing above the watermark slips into RocksDB by background or inline flush; a hold that breaches its deadline flushes, and the full sync that armed it is **abandoned** — the payload is never sent, the link drops, the replica retries from scratch (amended 2026-08-21, user ruling; the landed behaviour degraded the breached shard's watermark to `0` instead, which is not a weaker claim but *no* claim: no floor means that shard's overshipped range re-executes, which is this row's own defect back again in the slow-cut shape and invisible to both nodes. One reconnect backoff is the cheaper half of the trade, and it is the same fail-loudly stance as FM-REPLICATION-001). **A shipped payload whose trailer carries a `0` a breach produced** — `0` stays representable so `serde(default)` metadata still reads back as no floors, but no sender ever writes one for this reason. **A directory leaked by that abort** — the cut succeeded, so the staged directory is owned before the sync is failed and the exit handler deletes it, unlike a cut that failed and staged nothing. **A same-history `+CONTINUE` refused**, which would turn every healthy reconnect during a floor window into a full resync. |
| Invariant | `Y_s` is exact, not approximate, and the reason is structural: a shard task is single-threaded, both capture points are messages that same task processes, and `ReplicationBroadcast` is the terminal entry of `WRITE_EFFECT_ORDER`. So at the moment shard `s` handles its `FlushWal` / `ExportSnapshot` message, every write it has executed has already been broadcast, and none it has not executed has. Both soundness directions follow: everything shard `s` contributed to the payload sits at or below `Y_s` (`WalPersistence` precedes `ReplicationBroadcast`, and `flush_async` pushes its flush down the same FIFO), and nothing above `Y_s` is in it (the flush hold — and a hold that lapsed before the cut fails the sync at the `CoverageBreached` seam rather than degrading the claim it could not keep, so the exactness is a precondition of shipping at all and not a best effort). Receiver-side, one predicate at one site: `frame_disposition(applied_head, floor, end_offset)` (`replica/offset.rs`) is pure and total over its three inputs and orders the two rules — head, then floor — and `consume_frames` consults it once per frame, only when no group is pending. The floors live on `AppliedOffset` beside the heads they qualify and are installed by `reset_pair` under the same gate as the offset pair, so an applier that reads a floor is reading the floors of the dataset whose offset it is claiming into. Retirement is a compare-exchange on read, not a store, so an install racing the retirement is not clobbered. The window-grant refusal is a second pure seam (`window_grant_verdict`) over four inputs — the granted id, the offsets' provenance, ClaimedOffset and `max(Y_s)` — and the connection is the I/O half that acts on it, rewinding to 0 exactly as the FULLRESYNC arm does so the refusal cannot loop. |
| Outcome variant | `ConsumeStats.floor_skipped` (per stint); `AppliedOffset::floor_skipped()` (node-wide, cumulative); one `DEBUG` per skipped frame; the refusal logs one `WARN` and surfaces on the wire only as a full resync (`sync_full`). A breached hold logs one `WARN` naming the shard (the release) plus the `SyncFailure::CoverageHoldBreached` `ERROR` (the abandonment), advances the `INFO stats` counter `full_sync_hold_breaches` by exactly one — one count per abandoned *sync*, not per breached shard, taken where the abort is taken (the session's `FailSync` handler) rather than where the breach is reported, so a breach the machine did not act on cannot move it — and surfaces on the wire as a link dropped after `+FULLRESYNC` with no payload — the replica reconnects and asks again. The counter is what makes a chronically slow cut alertable: the retry loop is otherwise visible only in the logs. It is a FrogDB field with no Redis counterpart: it is rendered beside the `sync_*` triple in both `INFO stats` renderers, from its own shared field list and deliberately *outside* Redis's `sync_*` prefix, so a client enumerating that prefix still sees exactly Redis's three. It is monotone for the life of the process — `CONFIG RESETSTAT` deliberately does not reach it (FM-REPLICATION-058 still zeroes exactly the resync triple and the net-byte pair), so an alert reading `increase()` over it cannot be silently rebased. |
| Forced by | `a_coverage_vector_round_trips_through_the_trailer`, `a_trailer_without_the_coverage_field_is_refused`, `a_malformed_watermark_fails_the_whole_vector`, `an_empty_coverage_field_parses_as_no_watermarks`, `a_watermark_past_the_end_of_the_vector_is_absent_not_zero`, `a_frame_at_or_below_its_floor_is_covered_by_it`, `an_absent_floor_never_covers_anything`, `the_head_outranks_the_floor`, `an_install_adopts_the_payloads_floors_with_its_offset`, `a_shard_the_vector_does_not_describe_has_no_floor`, `floors_retire_once_the_head_reaches_the_ceiling`, `a_rewind_clears_the_floors`, `a_frame_the_installed_payload_already_holds_is_not_re_applied`, `a_floor_skipped_frame_claims_its_bytes`, `the_floor_boundary_is_inclusive`, `a_frame_is_measured_against_its_own_shards_floor`, `a_torn_cross_shard_transaction_is_mended_exactly_once`, `the_head_is_consulted_before_the_floor`, `floors_are_replaced_at_the_next_install`, `floor_skips_are_counted_on_the_stats_and_the_node`, `a_same_history_grant_is_never_refused`, `a_window_grant_under_uncovered_floors_is_refused`, `the_refusal_ends_exactly_at_the_ceiling`, `a_window_grant_with_no_floors_in_force_is_accepted`, `a_recovered_stint_refuses_every_window_grant`, `a_window_grant_over_an_uncovered_tail_is_refused_and_rewinds`, `a_same_history_continue_is_granted_over_the_very_same_floors`, `a_restarted_node_refuses_a_window_grant_it_cannot_account_for`, `offsets_seeded_from_the_persisted_state_are_recovered`, `a_node_that_has_never_synced_is_not_recovered`, `an_install_clears_the_recovered_provenance`, `a_save_point_persists_the_floors_in_force`, `the_stage_carries_the_payloads_coverage_vector`, `a_recovered_identity_comes_back_with_its_floors`, `floors_the_head_has_passed_do_not_come_back`, `the_wal_drain_ack_reports_the_shards_max_broadcast_offset`, `a_fullresync_drain_arms_the_flush_hold_and_hands_back_the_handle`, `full_sync_quiesce_reports_one_watermark_per_shard_and_holds_the_engines`, `a_breached_hold_names_its_shard_and_downgrades_no_claim`, `a_breached_coverage_hold_abandons_the_sync_and_owns_the_directory`, `a_breached_hold_aborts_the_sync`, `both_info_renderers_report_the_same_full_sync_hold_breaches`, `the_bgsave_entry_point_never_arms_a_hold`, `the_drains_coverage_reaches_the_trailer_and_the_hold_is_released_after_the_cut`, `a_failed_cut_still_releases_the_hold`, `a_write_pinned_in_the_capture_to_cut_window_lands_exactly_once` |
| Rulings | R12–R16 of the quint-completeness campaign (`.scratch/formal-spec/2026-08-19-quint-completeness-campaign.md`, 2026-08-21). R13 chose one receiver-authoritative mechanism for both payload paths over per-path fixes; R14 scoped the refusal to window grants only and gave the safety argument for never refusing a same-history grant; R15 put the vector in the staged metadata so the floors survive a crash in the window; R16 made a crash-recovered stint refuse window grants unconditionally until issue 36 pairs the offset with what sits above it. **Amended 2026-08-21 (user ruling)**: a breached flush hold aborts the full sync instead of degrading that shard's watermark to `0`. The degradation was correct only about the payload and wrong about the consequence — `0` is not a weaker floor, it is no floor, so the shard's overshipped range re-executes and the row's own defect returns in the slow-cut shape. Correct-by-construction beats a rare, invisible corruption; the cost is one reconnect backoff. The wire break is deliberate and loud in both directions — a trailer with four fields is refused at the parse rather than read as "no coverage" — which FrogDB can take because it is unreleased. |
| Bug refs | `.scratch/replication-correctness/issues/done/35-fullsync-overship-per-shard-coverage-vector.md`; the restart-flavored sibling is issue 36 |
| Model | `replication_fullsync.qnt::inv_no_forked_tail_above_the_claim`, `replication_fullsync.qnt::inv_coverage_brackets_the_payload`, `replication_fullsync.qnt::inv_overship_is_skipped_not_reapplied`, `replication_fullsync.qnt::inv_no_hole_below_the_claim`, `replication_fullsync.qnt::inv_reapply_is_a_noop` |

---

## Redis deviations

Deliberate or known differences from Redis 8.x replication semantics on this path. Each is pinned
by the tests named above, so a change here is a visible spec edit rather than a silent drift.

| Mode | FrogDB | Redis | Rationale |
|---|---|---|---|
| FM-REPLICATION-001 | The full-sync payload is a FrogDB envelope (`$FROGDB_CHECKPOINT` / `$FROGDB_SNAPSHOT`, per-file headers, a `FullSyncMetadata` trailer), never an RDB | `$<length>\r\n<RDB payload>`, or the diskless `$EOF:<40-byte delimiter>` framing | FrogDB has no RDB writer, and its on-disk form is RocksDB. The trailer carries the replid/offset that Redis puts in RDB aux fields, which is what couples offset durability to snapshot durability (FM-PERSISTENCE-039). The cost is that only FrogDB replicas can sync from a FrogDB primary — `redis-cli --rdb` and a real Redis replica cannot. |
| FM-REPLICATION-001 | A primary that cannot produce its dataset (failed checkpoint cut, no live-snapshot source) drops the connection and lets the replica retry | Redis retries the RDB save and, in diskless mode, will kill the transfer, but a `bgsave` failure still leaves the replica waiting rather than the link failing outright | Failing loudly is the point of this row: the alternative FrogDB actually shipped — putting *something* on the wire — is issue 67. One reconnect backoff is cheap; a silently stale replica is not. |
| FM-REPLICATION-001 | A persistence-disabled primary serializes its live keyspace from the shard tasks, with no fork and no child process | `repl-diskless-sync` forks and serializes the fork's memory to the socket | Same guarantee, different mechanism: FrogDB's shards are the source of truth and are single-threaded, so an export is a message, not a memory snapshot. The absence of a fork is why the export is per-shard rather than one instant (see the non-guarantees above). |
| FM-REPLICATION-002 | Installing a dataset emits no keyspace notifications | Redis' replica-side `FLUSHALL` before loading an RDB is likewise silent | Matched deliberately; noted because the install *does* route through the `FLUSHDB` effect pipeline, and the suppression is that command's `EventSpec`, not a special case. |
| FM-REPLICATION-013 | The reason a partial resync was refused (`FullResyncReason`) reaches only the primary's log; the replica sees only `+FULLRESYNC` | Same — Redis's replica is likewise not told why | Parity. Listed because the *counters* used to deviate here too and no longer do: `sync_full`/`sync_partial_ok`/`sync_partial_err` are real as of FM-REPLICATION-050, including Redis's rule that a refused partial advances `sync_full` as well. |
| FM-REPLICATION-014 | The lower-bound floor is inclusive: `req_offset >= backlog_start` grants | `psync_offset < server.repl_backlog->offset` refuses; the backlog offset is `master_repl_offset + 1` at creation | Same boundary, shifted by one because of the offset convention: FrogDB stores the start of the retained range, Redis stores the first offset it can serve. |
| FM-REPLICATION-014 | An unarmed backlog refuses every PSYNC, including from a replica that is exactly caught up | Redis creates the backlog on the first replica attach and can partial-resync a caught-up replica off a zero-length window | Deliberate. FrogDB's floor is `fetch_max`-monotone and can be armed at a non-zero recovered offset after a restart, where "empty buffer, `req == current`" is only sound at offset 0 — the code refuses the shortcut by name. Cost: one extra full resync on the first reconnect after a restart. |
| FM-REPLICATION-015 | The replay/live seam is closed by an *overlap* (subscribe first, replay second) with duplicate suppression on `frame.sequence <= resume_offset` | Redis copies the backlog into the replica's output buffer under the global lock, so there is no window to overlap | Consequence of FrogDB's broadcast-based fanout: there is no lock spanning "read the head" and "start receiving". The overlap is the safe direction — a duplicate is removable, a gap is not. |
| FM-REPLICATION-016 | Two independent caps: entry count *and* bytes, with a single oversized entry allowed to exceed the byte cap | One cap, `repl-backlog-size` in bytes, over a fixed circular buffer | FrogDB buffers whole RESP commands with their offsets and shard tags rather than a raw byte ring, so an entry count bounds the per-entry overhead the byte cap cannot see. Neither cap is an operator-facing parameter today. |
| FM-REPLICATION-017 | A PSYNC arriving during shutdown is refused with a connection abort before any reply | Redis serves the sync and lets the shutdown tear it down | Deliberate: cutting a checkpoint for a node that will not finish streaming it wastes the primary's disk and hands the replica a partial payload. One reconnect backoff against the next primary is cheaper. |
| FM-REPLICATION-019 | **Inclusive** `second_repl_offset`: `shift_replication_id` stores the boundary unshifted and `window_contains` tests `requested_offset <= secondary_offset` | Exclusive: `shiftReplicationId()` sets `server.second_replid_offset = server.master_repl_offset + 1`, and `masterTryPartialResynchronization` grants when `psync_offset <= server.second_replid_offset` against a replica reporting the offset it *wants next* (`replication.c:1698-1710`, `:718-763`) | FrogDB replicas request their **applied** offset, not their next-wanted offset. Adding `+1` on top of an already-inclusive request would grant one byte of history the promoted node never applied. Same guarantee, one fewer off-by-one. |
| FM-REPLICATION-019 | The promotion boundary comes from `OffsetCoordinator::settle_at_applied()` — the applied head, explicitly **not** the live/received head, taken under a frozen `ApplyGate` | `master_repl_offset` — on a Redis replica the offset advances only as the stream is *processed*, so received and applied coincide | FrogDB decodes frames on one task and applies them on another, so a replica's received head runs ahead of the data it holds. Freezing at the received head would hand a sibling `+CONTINUE` over a hole. |
| FM-REPLICATION-022 | `adopt_replication_history` clears the failover window in the same function that installs the new id; there is no way to set one without clearing the other, and `apply_staged_metadata` routes through it | `clearReplicationId2()` (`replication.c:2270`) is called from `readSyncBulkPayload` after a full resync, and separately when the ids are found equal | One code path instead of several call sites means a new adoption path cannot forget the clear — the stale-window shape from `.scratch/replication-cluster-rework/promotion-replid-psync.md` §6.3. |
| FM-REPLICATION-022 | FrogDB shifts the id on **promotion** only (`begin_primary_stint`); demotion calls `end_primary_stint`, which resets the backlog, retires replica applies, fences `WAIT` and disconnects downstream replicas — but mints nothing. The window is cleared later, when the node adopts its new upstream's history | `replicationSetMaster` caches the master and disconnects replicas; the reverse (`replicationUnsetMaster`, `:3053-3098`) shifts the id and calls `disconnectSlaves`. Redis shifts on **un**-setting a master | A demoted node has no history of its own to head, so there is nothing to mint. Keeping the inherited window until adoption lets a sibling that was mid-resume finish; clearing it at adoption is what stops the node vouching for a keyspace it just replaced. |
| FM-REPLICATION-024 | A `split_brain_discarded_<TS>.log` with the window, the op count and the raw RESP of every discarded write, plus three metrics and a pending gauge | No equivalent. Redis silently discards the divergent tail on `readSyncBulkPayload`; `replicationCacheMasterUsingMyself` only preserves the *link*, not the lost writes | Silent data loss during a partition is the failure operators most need evidence of. The writes are still discarded — FrogDB does not try to reconcile — but they are recoverable by hand. |
| FM-REPLICATION-030 | A replica expires keys **independently**, on its own clock. The primary never broadcasts a synthetic `DEL` for an organic expiry (`RemovalPropagation { replicate: false }`), and expiry runs role-agnostically on both nodes. | The primary is the sole authority: it broadcasts an explicit `DEL`/`UNLINK` on expiry, and a replica never deletes an expired key on its own — it only hides it from reads until the `DEL` arrives. | FrogDB's shards are single-threaded and own their own expiry cycle, so making expiry primary-authoritative would mean routing every expiry through the broadcaster and making the replication offset a function of wall-clock timing. The cost is a bounded drift window, pinned by `test_replica_pttl_bounded_by_replication_lag`; the benefit is that expiry is not a replicated write and a promoted replica inherits no backlog of un-expired keys. |
| FM-REPLICATION-030 | TTL commands replicate **verbatim and relative** — `EXPIRE k 10` goes on the wire as-is and the replica re-anchors against its own clock. | Redis rewrites relative TTL commands into absolute `PEXPIREAT` before propagating, so primary and replica share one deadline. | Direct consequence of independent expiry: with no authoritative `DEL`, the absolute-rewrite buys nothing the drift bound does not already give, and verbatim propagation keeps the replication stream a faithful command log. This is the deviation most likely to surprise someone porting Redis knowledge — it is why replica `PTTL` is *bounded by* rather than *equal to* the primary's. |
| FM-REPLICATION-029 | A replica always serves reads while behind or disconnected. There is **no `replica-serve-stale-data` knob** — the `yes` behaviour is unconditional. | `replica-serve-stale-data` defaults to `yes` but can be set to `no`, in which case a stale replica answers `-MASTERDOWN Link with MASTER is down and replica-serve-stale-data is set to 'no'` for every command except a small allowlist. | Matching the default without the knob. Adding the knob means a gate on the read path plus a definition of "stale" (link-down? lag threshold?), and no FrogDB deployment has asked for it. Recorded here so its absence is a decision, not an oversight — the same reasoning already given for the divergence window in FM-REPLICATION-010. |
| FM-REPLICATION-028 | `-READONLY` is derived from `CommandFlags::WRITE` on the registry entry, and defers to `-MOVED` in cluster mode. | Redis uses the same command-table `write` flag, and likewise resolves cluster redirection before the read-only check. | Parity, noted because the *mechanism* is the row's real content: a hand-maintained block list would pass every test written against today's command set and fail silently on the next command added. |
| FM-REPLICATION-025 | `REPLICAOF` is refused outright in cluster mode (`ERR REPLICAOF not allowed in cluster mode.`); role changes there go through the cluster's own failover path. | Redis likewise rejects `REPLICAOF` on a cluster node, directing operators to `CLUSTER REPLICATE`. | Parity. Noted because FrogDB has no `CLUSTER REPLICATE` equivalent surfaced yet, so on a cluster node the command is a dead end rather than a redirect to another command. |
| FM-REPLICATION-031 / -030 | Each replicated command is wrapped in a 20-byte binary header — `FRPL` magic, version, flags, `shard_id`, `sequence`, `length` | The replication link is an **inline RESP command stream** with no framing at all; the replica parses commands straight out of the socket | The header buys the origin-shard tag (FM-REPLICATION-034) and a self-delimiting length, so the decoder never scans value bytes for a boundary and a frame's extent is known before its payload is trusted. Cost: the link is FrogDB-only — a real Redis replica cannot `PSYNC` from a FrogDB primary. |
| FM-REPLICATION-031 | The offset counts **only the RESP payload bytes**; the 20-byte header is excluded on both ends | The offset is literally the byte count of the stream the primary wrote | Deliberately keeps the offset numerically identical to Redis's for the same command sequence, so `WAIT`, `REPLCONF ACK`, backlog arithmetic and `INFO` all stay Redis-shaped, while the socket carries 20 more bytes per frame than the offset accounts for. The header is transport, not history. |
| FM-REPLICATION-031 | The frame's `sequence` field is the write's **end offset**, not a monotonic frame counter | No per-frame sequence exists | Makes the frame self-describing about its stream position, so the backlog and the wire carry the same number and a `+CONTINUE` resume point can be validated against a frame rather than inferred. |
| FM-REPLICATION-034 | A `MULTI/EXEC` group is tagged with the shard it executed on and applied as one `apply_group` on that shard | Redis wraps replicated effects in `MULTI/EXEC` and the (single-keyspace) replica executes them in order; routing is not a concept | FrogDB's replica has N shards, so the group needs a routing decision Redis never makes. Re-deriving it from `args[0]` is wrong for keyless commands and for the `MULTI`/`EXEC` frames themselves, which is why the tag rides on the wire. |
| FM-REPLICATION-032 | A structurally valid frame whose payload does not parse is **counted and stepped over**, not treated as an error | A protocol error in the replication stream is fatal — Redis logs and drops the link | The length prefix delimits the frame regardless of whether this build understands its payload, so an unknown command from a newer primary costs one skipped write instead of an unrecoverable link. The offset stays aligned either way. The trade is deliberate but currently unforced (see gaps). |
| FM-REPLICATION-036 | A full-sync payload transfer is not resumable: an interrupted one is scrubbed and started over | Neither is Redis's RDB transfer (diskless or disk-backed) | Matched; noted only because the staged-directory machinery might suggest otherwise. |
| FM-REPLICATION-037 | `WAIT`'s target is the node's **global live offset** (`OffsetCoordinator::current()`) | `c->woff`, the offset right after *this connection's* last write | FrogDB stamps writes on the shard tasks and has no per-connection write offset. Dragonfly made the same call for the same thread-per-shard reason. Strictly conservative — the target is always >= the client's own last write, so `WAIT` can over-wait but never under-wait. Cost is latency under a mixed write load. |
| FM-REPLICATION-037 | An unreachable `numreplicas` blocks to the deadline | Same (Redis blocks) | Matched deliberately, and recorded because Dragonfly diverges: it early-exits once every currently-tracked replica has acked. Rejected — a replica may be mid-attach, so the early answer is to a different question. Pinned by an *elapsed-time lower bound* assertion, not just the return value. |
| FM-REPLICATION-037 | Cluster mode has no `WAIT` special case: per-node, never redirects, no server-side fan-out | Per-shard, keyless, no redirect | Same contract. Noted because the cluster-wide guarantee a client usually wants (`ALL_SHARDS` + `AGG_MIN`) is deliberately left client-side; FrogDB will not fan a `WAIT` out across shards. `WAITAOF` is a stub. |
| FM-REPLICATION-041 | A replica-loss fence that answers `-SELFFENCE writes rejected: no fresh streaming replica (self-fence-on-replica-loss)`, armed by the first streaming replica and disarmed when the last one departs cleanly | No equivalent; Redis would answer `-NOREPLICAS` via `min-replicas-to-write`, or accept the write | A FrogDB-only safety net for the "was replicated, now is not" case that `min-replicas-to-write` handles only if the operator set it in advance. It carries its own error code rather than borrowing cluster mode's `-CLUSTERDOWN` (issue 30): the code names the mechanism and the knob that clears it, which is the only reason an operator on a standalone primary can act on it. Pinned by an exact-string assertion so a reword is intentional. |
| FM-REPLICATION-062 | A streaming replica's departure is classified graceful vs lost, and only a lost one holds the self-fence | No equivalent — Redis has no fence to hold, and `freeClient` draws no such distinction | Falls out of having the fence at all: a fence that cannot tell "my replica left" from "my replica stopped answering" either fences a deliberate decommission forever or fences nothing. The classification is deliberately conservative — anything unclassified counts as lost. |
| FM-REPLICATION-042 | The ACK-freshness window is stored as `replication.min-replicas-timeout-ms` in TOML, default **5 s** | `min-replicas-max-lag`, seconds, default **10** | The halved default remains a live divergence (a tighter window refuses writes sooner than Redis would); the lossy sub-second round trip that used to accompany it is fixed — see FM-REPLICATION-046. |
| FM-REPLICATION-046 | Two CONFIG names for one window: `min-replicas-max-lag-ms` (native, exact) and `min-replicas-max-lag` (Redis parity, seconds, rounds **up**). Only the `-ms` name is persisted by `CONFIG REWRITE` | One name, `min-replicas-max-lag`, seconds only; sub-second windows are inexpressible | Redis's unit is a historical wart that cannot express the windows FrogDB's own deployments use, and a seconds-only surface makes `CONFIG GET`/`SET` lossy for them. Keeping the Redis spelling preserves compatibility for tooling that knows only that name; adding the `-ms` spelling makes the value round-trippable. Rounding up rather than truncating is what keeps the lossy direction *safe*: a widened window still filters, a truncated one (`0`) disables the gate. |
| FM-REPLICATION-042 | Both write fences run twice: at `run_pre_checks` for direct writes and `MULTI` queue time, and at the shard write seam for the producers that never reach it (Lua-internal writes, a `MULTI` whose replicas dropped between `+QUEUED` and `EXEC`) | Redis's `NOREPLICAS` check runs in `processCommand`, so scripts are gated through their own `scriptPrepareForRun` deny path | Same coverage, reached differently: FrogDB's script writes are refused where they are produced rather than pre-declared, so an *undeclared* runtime write is gated too (`specs/txn.md` FM-TXN-051). |
| FM-REPLICATION-059 | `repl_backlog_size` is the configured cap even while the buffer holds less, and `repl_backlog_active` means "a resume window is armed" | Redis reports `repl_backlog_size` as the configured `repl-backlog-size` too, and sets `repl_backlog_active` when the backlog has been created (which happens on the first replica attach) | Parity on the size; a deliberate sharpening on `active`. Redis's flag answers "does a backlog exist", which on FrogDB is always yes — the buffer is constructed unconditionally so the accessors are infallible — so the only non-vacuous reading is "is there a window a reconnecting replica could resume from", which is also the question `first_byte_offset` and `histlen` answer. The three therefore agree by construction; keeping Redis's meaning would have let `active:0` print beside a non-zero floor. |
| FM-REPLICATION-060 | A replica in the `Disconnecting` phase renders no `slaveN:` line and is not counted by `connected_slaves` | Same: `genInfoSectionDict` emits a line only for `wait_bgsave`, `send_bulk` and `online`, skipping any other state | **Not a deviation** — this row records where FrogDB deliberately converged. An earlier design rendered `state=offline` for the fourth phase, on the reasoning that `connected_slaves` had to equal the line count and dropping a line would break that; making `connected_slaves` the *length of the rendered vector* removes the constraint and lets FrogDB match Redis exactly, without inventing a state word no Redis client parses. |
| FM-REPLICATION-043 | Proactive lag disconnect on `replication-lag-threshold-bytes` / `-secs`, with an address-keyed reconnect cooldown; both default 0 (off) | `client-output-buffer-limit slave <hard> <soft> <soft-seconds>` — a buffer-size limit, not an offset/time lag limit | Different mechanism, same goal. FrogDB's broadcast fan-out is not a per-client output buffer, so the natural measure is the replica's acked-offset lag and its ACK silence. The cooldown is keyed by `SocketAddr` because replica ids change on every reconnect. |
| FM-REPLICATION-061 | `INFO replication` on a replica may carry a **`master_sync_error`** field, present only when the inbound stream has given up on a full resync it can never install, and the stream then stops retrying | No such field. Redis's replica has no structural refusal to report — an RDB is partitioning-agnostic and tier-agnostic, so any failure to load one is transient by nature, and `replicationHandleMasterDisconnection` always schedules another attempt | FrogDB-specific because the failure is FrogDB-specific: a checkpoint carries the primary's column-family layout, and two nodes can disagree about it in a way no retry resolves. Redis's "always retry" is right for Redis's failure set and wrong for this one. The field is additive — `master_link_status` keeps its `up`/`down` meaning for every existing consumer — and its absence is the normal case, so tooling that does not know it is unaffected. |
| FM-REPLICATION-055 | A syncing replica gets the function registry as a replicated `FUNCTION RESTORE <dump> FLUSH` command inside the replayed backlog window | The libraries ride the RDB itself (`function` aux payload), so they arrive with the dataset | FrogDB's registry is not in the keyspace and not in RocksDB — it is its own `functions.fdb` beside the data dir — and the full-sync envelope has no slot for an unrelated blob. Using the frame lane instead means one mechanism covers both sync flavours and the steady-state stream. Cost: the dump is re-sent on every resync, and it lands *just after* the dataset rather than atomically with it, so a window of a few milliseconds exists where a synced replica has the keys but not the libraries. |
| FM-REPLICATION-064 | A `PSYNC` from a replica on a different **major** version is refused with an error naming both versions; a minor skew is served with a warning | No version gate on the replica side of the handshake at all. Redis's replica checks the *primary's* `capa`-negotiated features and its RDB version, and a primary accepts a `PSYNC` from any peer that speaks the protocol; incompatibility surfaces later as an RDB the replica cannot load | FrogDB's checkpoint is a RocksDB column-family layout rather than a self-describing RDB, and its frame encoding is versioned by the build, so "incompatible" is a property of the pair that is knowable at the handshake and expensive to discover afterwards — a full sync transferred and then rejected, or worse, silently misapplied. Redis's late failure is acceptable for Redis because its RDB carries an explicit version byte the loader checks; FrogDB's equivalent check is this gate. Deliberately narrow: only a *proven* major difference refuses, so a peer that announces nothing (a pre-option replica, or a non-FrogDB client speaking `PSYNC`) is still served exactly as Redis would serve it — the deviation adds a refusal, never a new way to be dropped for silence. |
