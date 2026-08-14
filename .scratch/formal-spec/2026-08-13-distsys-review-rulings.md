# Rulings on the independent distsys review

Source: [2026-08-13-independent-distsys-review.md](2026-08-13-independent-distsys-review.md).
User rulings, recorded as issued. Findings not listed here are not yet ruled.

## Global principle (ruled with CRIT-2)

**No wall-clock time in anything state-related.** Wall-clock timestamps must not gate,
order, or admit state-machine transitions anywhere in the system. Time-derived values may
exist as observability data only. This generalizes issue 17's "log-ordered fence, no
wall-clock" ruling from a per-issue decision to a campaign-wide design rule; apply it when
triaging the remaining findings and when writing phase-3 specs/models.

## CRIT-2 — proposer-minted handoff deadlines

Ruling: **logical admission token** (reviewer's suggested resolution (b), option 1).

- The source's drain confirmation carries the `handoff_seq` it drained.
- `CompleteSlotMigration` is admitted iff that seq matches the migration's current seq.
- `prepared_at_ms` / `barrier_ms` / `lease_ms` comparisons are removed from admission
  logic; timestamp fields are demoted to observability-only (may remain in replicated
  payloads for operator visibility, never consulted by `admits_*` / `live_handoff_at`).
- Spec work: amend TR-CLUSTER-013 + FM-CLUSTER-089; add an FM row for the
  stale-seq completion attempt (target proposes completion with an outdated seq → rejected).
- Note: Task 2's quint model already encodes these semantics (`drained` flag,
  seq match, `inv_complete_requires_drained`) — code converges on the model.
- **MAJ-4 folded in** (ruled 2026-08-14): the preamble's directional clock-skew
  argument is backwards for the applying node (forward skew at apply makes
  `!barrier_expired` *refuse more*, not admit more; the apply-side hazard is backward
  skew). CRIT-2's spec amendment must replace that preamble paragraph with the
  role-split statement: proposer-forward-skew was the admission hazard (now moot under
  the logical token); applier skew is irrelevant once the value is replicated data.

## MAJ-4 — preamble's directional clock-skew argument is backwards

Ruling: **fold into CRIT-2's spec work** — no separate issue. CRIT-2's amendment
deletes wall-clock admission reasoning from this area; the requirement that it replace
the preamble with the role-split statement is recorded on the CRIT-2 entry above.

## MAJ-1 — split-brain audit floor collapses to 0

Ruling: **accept, file issue** (reviewer's resolution). Filed as
[replication issue 31](../replication-correctness/issues/open/31-divergence-audit-floor-from-successor-offset.md)
(ready-for-agent); rides the implementation wave. Floor from the `DemotionEvent`'s
successor offset; `unknown-floor` variant when absent — never a silent 0; invert the
zero-floor pinning test.

## MAJ-2 — TR-REPLICATION-022 node-id dedup key does not exist on the wire

Ruling: **fix the protocol** (reviewer's resolution (a), option 1).

- Replica mints and persists a replica run id; transmits it during the replication
  handshake as a `REPLCONF replica-id <id>` capability.
- Session dedup keys on that id, making the AMENDED TR-REPLICATION-022 ruling
  implementable as written; the NAT case (two replicas behind one NAT announcing the
  same `listening-port`) becomes genuinely distinguishable.
- Spec work: amend FM-REPLICATION-049 (announced identity grows the replica id);
  re-key locked INV-SESSION-2 on the new identity — locked-invariant edit goes through
  the normal spec-first flow (failure-mode row → forcing test → change).
- Explicitly ruled: **no backward-compatibility constraint** — pre-alpha software, the
  handshake may require the new capability outright rather than gate on it. Get the
  spec/protocol correct; do not carry a compat shim.

## MAJ-3 — FM-REPLICATION-019 contradicts the State-space table on offset rewind

Ruling: **accept, file issue** — the rewind is the truth (a node must never claim
bytes it did not apply); FM-019's continuity cell is the lie. Amend FM-019, add a
downstream-ahead-of-new-primary FM row (full resync forced) with forcing tests. Filed
as [replication issue 32](../replication-correctness/issues/open/32-fm-019-offset-rewind-truth-reconciliation.md)
(ready-for-agent); rides the implementation wave. Coordinate with issue 24's atomic
replid/offset pairing.

## MAJ-5 — source write pause unfenced after `barrier_ms` deletion

Ruling: **redesign, option B — source-authoritative-until-commit** (filed as
[cluster issue 31](../cluster-correctness/issues/open/31-slot-migration-redesign-source-authoritative-until-commit.md)).

Triage found MAJ-5's premise partially resolved already (feed byte-cap = issue 17
amendment; abort mechanism = issue 15 amendment; FM-CLUSTER-084/085 re-derivation =
issue 29 sweep) but exposed a dangling reference: issue 17's amendment assigns orphaned-
handoff liveness to an "issue 18 reconcile abort" that issue 18 never defines. Rather than
patch (amend 18 + repatriation-when-possible), the user ruled the structural fix: drop the
Redis-style delete-as-you-copy bulk phase; source retains keys and serves all traffic
until `Complete` applies; target catches up via slot-scoped mutation stream; abort =
target-discard, safe at any time including dead target; reconcile orphan-abort on the
FAIL-flag criterion. Supersedes issue 15's repatriation. Design is HITL — brainstorm
before phase-3 models encode migration semantics. Details in issue 31.

## MAJ-6 — `CLUSTER RESET` is replicated and therefore cluster-wide destructive

Ruling: **node-local** (reviewer's first option; destructive scope = invocation
scope, Redis parity). Reset departs and clears only the issuing node; surviving
topology untouched; cluster-wide wipe stays achievable node-by-node. Filed as
[cluster issue 34](../cluster-correctness/issues/open/34-cluster-reset-becomes-node-local.md)
(ready-for-agent); rides the implementation wave. Departure uses issue 20's
demote-don't-remove shape.

## MAJ-7 — node ids: 16 bits/ms, regenerated every boot, upsert-merged collisions

Ruling: **accept, file issue** (reviewer's resolution, plus no-timestamp component per
the no-wall-clock principle and an etcd-style boot guard). Mint once ≥128 random bits,
persist beside `database_id`, read back on boot; `AddNode` rejects conflicting ids;
boot refuses id/data-dir mismatch. Filed as
[cluster issue 35](../cluster-correctness/issues/open/35-node-identity-outlives-the-process.md)
(ready-for-agent); rides the implementation wave. Retires Task 1's node-id-stability
deferred minor; aligns with MAJ-2's replica run id family.

## MAJ-8 — no inequality between self-fence window and successor promotion

Ruling: **accept, file issue**, with framing correction: the timing inequality is
defense-in-depth — the hard backstop stays epoch fencing at write admission (no clock
inequality survives a process stall). Named precondition + FM row; `validate()`
enforces detection < election on load and live mutation; defaults fixed. Filed as
[cluster issue 36](../cluster-correctness/issues/open/36-self-fence-window-precedes-successor-promotion.md)
(ready-for-agent); rides the implementation wave. Coordinates with issue 27's
raft-liveness detection redefinition.

## MAJ-9 — no replica-validity bound; offset-unknown stays promotion-eligible

Ruling: **accept, logical bound** (no wall-clock: staleness in offset-lag
bytes/entries, never disconnection seconds). Offset-unknown candidates rank strictly
last — promotable only when no known-offset candidate exists (availability floor
kept); optional lag disqualifier scoped to automatic promotion (forced failover
unaffected, issue 19 owns that path). Filed as
[cluster issue 37](../cluster-correctness/issues/open/37-promotion-validity-bound-offset-unknown-last.md)
(ready-for-agent); rides the implementation wave.

## MAJ-10 — `config_epoch` arbitrates nothing

Ruling: **wire-compat honesty** (reviewer's second option). Ownership authority is
raft consensus; `config_epoch` is wire rendering only. Amend FM-CLUSTER-010/011/012 to
say so, add the deviations-table row, delete the dead (and wrong-direction)
newcomer-renumber arbitration machinery. Explicitly rejected: wiring epoch into
arbitration — a second conflict resolver beside raft, redundant when raft is healthy
and dangerous where they'd disagree. Filed as
[cluster issue 38](../cluster-correctness/issues/open/38-config-epoch-is-wire-compat-only.md)
(ready-for-agent); rides the implementation wave.

## MAJ-11 — biased select falsifies TR-BLOCKING-007; H5 unsound as ordered

Ruling: **eliminate sender-drops as a signaling mechanism** (reviewer's resolution,
option 1). The deadline fast-path (`shard/blocking.rs:320-322`) sends
`entry.op.timeout_reply()` + increments `BlockedTimeoutTotal` instead of dropping;
`Satisfaction::Retry` and the admission refusal likewise send real replies (H7 already
covers the latter). After that, `Err(_)` at the coordinator uniquely means channel death
and spec-gaps issue 08's H5 (`-ERR shard unavailable`) is sound as written. Amend
TR-BLOCKING-007 to remove the "server's reply normally wins" race claim (it is
deterministically false — `biased;` select, `response_rx` first). Ordering dependency
recorded as an amendment on
[spec-gaps issue 08](../spec-gaps/issues/open/08-blocking-command-rows.md).

## CRIT-1 — restarted source never re-arms its slot write barrier

Ruling: **fix the spec now; code fix with the implementation wave** (clarified by the
user after the initial "fix now"). Filed as
[cluster issue 32](../cluster-correctness/issues/open/32-restarted-source-never-re-arms-its-slot-write-barrier.md)
(ready-for-agent). Spec half landed same day: FM-CLUSTER-104 (gap-cited to issue 32 per
the `MISSING ([gap: …])` mechanism) plus the `PauseState.slots` State-space row restated
to stop laundering "reconstructible in principle" as a property. Code half (reconcile +
snapshot-delta emission + forcing test) rides the ruled-issues implementation wave.
Survives issue 31's redesign (finalization drain keeps the barrier; restart mid-drain
still needs re-arm).

## CRIT-3 — `wal_watermark` can lead the durable point

Ruling: **accept, file issue** (reviewer's resolution). Filed as
[spec-gaps issue 12](../spec-gaps/issues/open/12-wal-watermark-carries-covered-sequence.md)
(ready-for-agent); fix rides the implementation wave. Covered sequence carried through
the write path into `record_wal_watermark(covered_seq)` as a `fetch_max`; amend
FM-PERSISTENCE-035; rewrite the pinning test
(`only_a_synced_rocks_sink_commit_records_the_durable_watermark` asserts the buggy
equality); add the two-shard forcing test.

## CRIT-4 — disconnect-while-parked undetectable; push consumed and delivered to nobody

Ruling: **restructure** (reviewer's "better" option). The blocking wait becomes a
run-loop state instead of an inline await, matching the Redis/Valkey/Dragonfly shape
(blocked client = state on a still-readable connection). Filed bundled with CRIT-5 as
[spec-gaps issue 13](../spec-gaps/issues/open/13-blocking-wait-becomes-a-run-loop-state.md);
rides the implementation wave.

## CRIT-5 — CLIENT KILL cannot terminate a parked client

Ruling: **accept, bundle with CRIT-4's restructure** (one issue, one pass over the
machinery). The restructure keeps `killed()` polled during the wait; CRIT-5-specific
remainder is the CLIENT KILL TR row + forcing test. Same
[spec-gaps issue 13](../spec-gaps/issues/open/13-blocking-wait-becomes-a-run-loop-state.md).

## CRIT-7 — cross-shard SCA admits hold-and-wait cycles

Ruling: **wound-wait on the SCA path** (reviewer's suggested resolution). The user
probed the alternative (Calvin-style atomic globally-ordered multi-shard declaration,
incl. whether ignoring the crates' locked status changes the call — it does not: lock
status is process cost paid equally by both options; the driver is architectural).
Wound-wait is a local lock-table change preserving shared-nothing async scatter; the
sequencer adds an ordering point to the multi-shard hot path. Filed as
[spec-gaps issue 14](../spec-gaps/issues/open/14-sca-wound-wait-restores-acyclicity.md)
(ready-for-agent); rides the implementation wave. Liveness proviso recorded in the
issue: a wounded txn's retry keeps its original txid (age-based priority), else
starvation. Partially moots MAJ-20's timeout-only exit for this cycle class.

## CRIT-6 — `replication_state.json`: shared temp path, two writers, no fsync

Ruling: **accept, file issue** (reviewer's resolution: `stamp_with` reuse + unique temp
+ serialized writers + FM row). Filed as
[replication issue 30](../replication-correctness/issues/open/30-replication-state-file-atomic-durable-single-writer.md)
(ready-for-agent); rides the implementation wave.

## MAJ-12 — TR-BLOCKING-019 misstates the drain scope; plain XREAD waiters parked forever

Ruling: **accept-plus — fix the semantics, not just the row** (goes beyond the
reviewer's split-row honesty). A `BLOCK 0` XREAD waiter on a key that became a
non-stream is unsatisfiable forever — nothing will ever re-signal that key as a
stream — so leaving it parked violates the every-blocked-state-is-leavable principle
underlying spec-gaps issue 13. Ruled shape: the wrong-type drain covers **all** stream
waiters on the key (XREAD and XREADGROUP), replied with a pinned error;
`DrainNoGroup` stays XREADGROUP-only (correct — plain XREAD needs no group; its wait
remains satisfiable). TR-BLOCKING-019 splits into per-arm postconditions with error
texts pinned; the divergence from Redis's park-forever behavior (and from its
`-UNBLOCKED the stream key no longer exists` text) is documented as a
deviation-as-improvement. Filed as
[spec-gaps issue 15](../spec-gaps/issues/open/15-wrong-type-drain-covers-plain-xread-waiters.md)
(ready-for-agent); rides the implementation wave. Sequence with
[spec-gaps issue 13](../spec-gaps/issues/open/13-blocking-wait-becomes-a-run-loop-state.md)
(same machinery — land 13's restructure first or coordinate).

## MAJ-13 — deny-blocking contexts return the wrong nil shape; unrowed

Ruling: **accept, file issue** (reviewer's resolution). The conversion at
`execution.rs:623-630` collapses `Response::BlockingNeeded` to `Response::Null` (`$-1`)
discarding the op, so `MULTI; BLPOP k 1; EXEC` returns `$-1` where Redis returns `*-1`
— a third instance of the wrong-shape family FM-BLOCKING-002 polices, on a path the
spec never rows. Fix is one line (`op.timeout_reply()` at the conversion site — the op
is already in scope and the FM-BLOCKING-002 machinery exists); plus a TR row
("a blocking command in a deny-blocking context resolves immediately with the op-aware
nil, registers no `WaitEntry`, sets no blocked flag") and a forcing test per op family.
The Lua path (`scripting/bindings.rs:184`) is confirmed correct — Lua flattens both
nils. Filed as
[spec-gaps issue 16](../spec-gaps/issues/open/16-deny-blocking-context-returns-op-aware-nil.md)
(ready-for-agent); rides the implementation wave. Cross-ref spec-gaps issue 13: the
conversion site may move in the run-loop restructure — the TR row must survive it.

## MAJ-14 — CLIENT PAUSE × blocking commands unrowed; diverges from Redis

Ruling: **adopt Redis semantics** (beyond the reviewer's either/or). Two ruled campaign
principles decide it: observability accuracy (`blocked_clients` = 0 while clients sit
parked at dispatch during a failover pause is exactly "misleading data not ok"), and
deviations-must-be-improvements (a client asking for a 1s timeout waiting 61s is not
one). Ruled shape: a blocking command issued during CLIENT PAUSE enters the blocked
state and its deadline runs during the pause (Redis's "Blocking timeout following
PAUSE should honor the timeout"); `blocked_clients`/`CLIENT LIST` reflect it; pause
gates execution, not parking. Implementation rides issue 13's run-loop restructure
(blocked-client-as-state makes pause-compatible parking natural). The regression
assertion (`resp.is_none() || matches!(Bulk(None))` — forces nothing) is tightened to
require the reply. Filed as
[spec-gaps issue 17](../spec-gaps/issues/open/17-client-pause-honors-blocking-deadlines.md)
(ready-for-agent, sequenced after issue 13); rides the implementation wave.

## MAJ-15 — registration-ordinal row asserts a FIFO mechanism the code does not use

Ruling: **restate the row + give the ordinal its claimed reader** (combines the
reviewer's two halves). Per-key FIFO: the row is restated — the per-key `VecDeque`
push order is the FIFO authority (making pop paths consult the ordinal would be churn
for zero behavior change). Slot-scoped drain: `drain_waiters_for_slot` stops iterating
HashMap keys and orders by the registration ordinal — `seq_by_slot` gains the real
reader the row claims, cross-key drain becomes fair (oldest waiter first) and
deterministic, and ordinal-mutants become killable instead of silently surviving the
mutation gate. TR-BLOCKING-001/013 citations re-derived against the restated row.
Filed as
[spec-gaps issue 18](../spec-gaps/issues/open/18-slot-drain-orders-by-registration-ordinal.md)
(ready-for-agent); rides the implementation wave.

## MAJ-16 — `Satisfaction::Retry` silently drops a live waiter; unrowed

Ruling: **accept, investigate-first** (reviewer's resolution with the reachability
question made step 1). `Retry => continue` drops the entry with its `response_tx` —
in a release build the two `debug_assert!(false)` arms become a silent nil for a
client that should have stayed parked, and the documented cause ("lost a race to an
earlier waiter") is not reachable as written (`check_key` runs immediately before
every `satisfy` on the same serial thread). Issue step 1 settles reachability
(targeted construction attempt + mutants evidence over `blocking.rs`). If dead:
`unreachable!()` — fail-stop over a silent wrong answer — and the row states the
resolution taxonomy is total without Retry. If reachable: re-register the entry
(Redis shape — the client stays blocked), never a silent nil; row + forcing test.
Either branch kills the `Err(_) → bare Response::Null` path (wrong-shape family).
Filed as
[spec-gaps issue 19](../spec-gaps/issues/open/19-satisfaction-retry-resolved-dead-or-reregister.md)
(ready-for-agent); rides the implementation wave. Coordinate with spec-gaps issue 13
(same machinery — the arms may move or vanish in the run-loop restructure).

## MAJ-17 — crash between the install renames re-mints `database_id`, wedges the boot

Ruling: **accept, file issue** (reviewer's resolution). Power loss between
`install_staged`'s two renames leaves no marker and no `<db>`; the next boot's
Phase 0 runs *before* the install, so it mints a fresh `database_id` (contradicting
FM-PERSISTENCE-049) or bails under `require-existing-data` while a complete
`checkpoint_ready` sits in the parent — the opposite of FM-PERSISTENCE-025's "next
boot finishes the install cleanly". Fix per the reviewer: Phase 0 becomes
mid-install-aware (probe `StagedCheckpoint::for_db_dir` + `<db>_backup_*` before
concluding "empty"), `database_id` carried forward from the backup (CRDB rule:
identity is the last thing you re-derive, never the first — same
identity-outlives-process family as cluster issue 35 / MAJ-2), install runs before
the `require-existing-data` gate; FM row for "crash between the two install renames"
+ crash-point forcing test. The timestamped backup name (`<db>_backup_<ts>`) is
flagged for a sequence-based rename while touching (wall-clock in a path — cosmetic,
not state-bearing). Filed as
[spec-gaps issue 20](../spec-gaps/issues/open/20-mid-install-crash-recovery-preserves-database-id.md)
(ready-for-agent); rides the implementation wave. Persistence locked, gate 0.85.

## MAJ-18 — staging/backup live in the data dir's *parent*: undeclared deployment constraint

Ruling: **restage inside the data dir** (the real fix, beyond the reviewer's
probe-and-document minimum). Kubernetes is FrogDB's primary deployment target
(operator, helm); the standard layout mounts the PVC *at* `persistence.data-dir`, so
`rename(<db>, <db>_backup_*)` is an EBUSY on a mount point — full resync permanently
impossible — and the staged download lands in the container's ephemeral rootfs where
it can fill the node's disk. Probe-and-document would only convert silent failure to
fast failure of something that should work. Ruled layout (etcd/CRDB shape): the mount
point is never renamed — RocksDB dir becomes `<data-dir>/db`, staging
`<data-dir>/staging`, backup `<data-dir>/backup`; every install rename happens inside
one filesystem inside the mount; the spec row states the only filesystem requirement
is the data dir itself. Pre-alpha: no layout compat shim. Filed as
[spec-gaps issue 21](../spec-gaps/issues/open/21-staging-and-backup-live-inside-the-data-dir.md)
(ready-for-agent); rides the implementation wave. Reworks the same rename machinery
as issue 20 (MAJ-17) — coordinate; ideally one implementer takes both.
