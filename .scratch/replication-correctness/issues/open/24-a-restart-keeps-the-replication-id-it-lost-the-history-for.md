# 24 — a restart keeps the replication id whose history it just lost

Status: ready-for-agent

Sequenced after issue 37 — see the Amendment (2026-08-22) below.

## Parent

[PRD](../../PRD.md) §3 W3. Found by the seeded replication sweep
([issue 12](../done/12-replication-seeded-sweep.md)), seed 81 — family `mixed`, a
crash-restart of the primary overlapping an isolate of the same node.

## What was found

Replication identity is recovered independently of the dataset it names.
`recovery::recover` says so explicitly:

> Replication state (phase 5) is role-gated, not persistence-gated: replication
> runs without RocksDB persistence, so this phase runs in both cases

`replication::restore_state` reads `<data_dir>/<replication.state_file>`
(`replication_state.json`) through `ReplicationState::load_or_create`, which
also *writes* the file on first boot. Nothing consults whether a dataset was
recovered. So a node that is SIGKILLed and restarted with
`persistence.enabled = false`:

- comes back with an **empty keyspace** and `master_repl_offset = 0` (nothing
  raised `offset_at_save`, so the `live.fetch_max(offset_at_save, …)` seed in
  `ReplicationIdentity::recovering` contributes nothing), and
- comes back advertising the **same `master_replid`** it headed before the
  crash.

The id outlived the history it names. Sweep seed 81 sees exactly this — the
three nodes are on replid `4e13…` at offset 240, the primary is killed and
restarted, and for four observation rounds it reports `role=master
replid=4e13… offset=0 connected_slaves=0` while both replicas still report
`replid=4e13… offset=240`. Every replica is trivially "ahead of its primary on
the same lineage".

The transient state alone is an operability defect (`INFO` on three nodes of one
lineage cannot be read as one history, and any external staleness monitor that
subtracts offsets sees a negative lag). The reachable damage is one step
further: the rebooted primary now re-issues offsets `0…N` **under the id the old
history used**, so those offsets name different bytes on the two sides. A
replica that reconnects while its own offset is above the new head is refused
and full-resyncs — that is what usually happens, and it is what heals seed 81 —
but a replica held away from the primary long enough for the new stream to pass
its offset presents `PSYNC 4e13… 241` into a window that now *contains* 241,
and `PartialSyncReplay::handle_partial_sync_request` grants `+CONTINUE`: the
replid matches and the offset is inside the backlog. Nothing in the grant
distinguishes the pre-crash lineage from the post-crash one. Seed 81's shape (a
crash overlapping an isolate of the same node) is the shape that produces that
window, and the sweep should reach it with a bigger backlog.

[Issue 21](./21-ack-above-live-head.md) is one of the reachable consequences,
and the sweep reaches it independently of the proptest that filed it: a replica
that followed the previous incarnation acks its own higher offset to the
rebooted primary, which credits it past its own live head and trips the
Hard-tier `INV-OFFSET-3`. Fixing *this* issue closes that path into 21, but not
21 itself — a promotion that settles at its applied offset
(`settle_at_applied` stores the lower value into `live`) puts a replica above
the head with no restart involved, and the ingest seam still has no ceiling.

This is distinct from [issue 17](./17-save-point-above-the-live-head.md), which
is about the save point being *above* the live head after a backwards full
resync. Here the save point and the head agree at 0 and it is the *identity*
that is stale.

## Precedent

Redis persists the replication id and the offset **together**, as RDB aux fields
`repl-id`/`repl-offset`, and restores them together in `loadDataFromDisk` — the
id is only inherited by a node that also loaded the dataset that id describes.
A Redis master started with no RDB to load keeps the id it minted in
`initServerConfig`/`changeReplicationId`, i.e. a fresh random one, so every
replica of the previous incarnation full-resyncs. Valkey inherits this. FrogDB
splits the pair across two files with different lifetimes, and the identity file
is the one that always survives.

## Ruling needed

- (a) **Identity recovery follows dataset recovery.** If no dataset was restored
  (persistence disabled, or an absent/empty store), mint a fresh replication id
  at boot rather than adopting the file. Matches Redis; costs one guaranteed
  full resync per replica after a restart that had nothing to restore anyway.
- (b) **Keep the file, but demote what it holds.** Boot without a recovered
  dataset shifts the loaded id into the failover window (`secondary_id`) with
  boundary 0 and mints a new primary id, so a replica presenting the old id is
  recognised as being on a history this node used to head and is always
  full-resynced instead of silently continued.
- (c) The pairing is wrong at the source: persist the offset in the same file as
  the id and treat a mismatch with the recovered dataset as corruption.

## Acceptance criteria

- [ ] Ruling recorded here with its reasoning
- [ ] Behaviour implemented, with a forcing test in the owning locked crate
      (`frogdb-replication` — `cargo mutants -p <crate>` only runs that
      package's own tests)
- [ ] End-to-end coverage in `integration_replication.rs`: a primary restarted
      with an empty dataset never grants `+CONTINUE` to a replica that followed
      its previous incarnation
- [ ] Failure-mode row added to
      `specs/replication.md` naming its forcing
      test
- [ ] The named-gap exemption in the sweep's `XREPL-2` (see below) removed, so
      the arm witnesses the fixed behaviour instead of tolerating it

## Witness

`frogdb-server/crates/server/tests/simulation/replication_scheduler.rs` —
`restart_tainted_replids` computes the set of replication ids a node the
schedule restarted was *observed rewinding under*, and `check_cross_node`
exempts those ids from both `XREPL-2a` (a replica ahead of its primary) and
`XREPL-2b` (an offset going backwards). Keyed on the observed rewind rather than
on "this node was restarted", so a fix that mints a fresh id empties the set and
re-arms both checks with no edit; pinned both ways by
`test_xrepl_2_exempts_a_replid_a_restart_rewound` and
`test_xrepl_2_gap_does_not_cover_a_primary_that_never_rewound`. At the 500-seed
budget this gap covers seeds 122, 171 and 211. Replay the witness with:

```
REPLICATION_SEED_TRACE=1 REPLICATION_SEEDS_START=81 just replication-seeds 1
```

## Ruling (2026-08-13)

**Option a: structural.** Replication identity + offset move INTO the checkpoint/dataset metadata (Redis RDB-aux shape) so identity cannot outlive the dataset by construction. A boot that recovers no dataset naturally mints a fresh replid (covers persistence-disabled). Subsumes option c. Closes issue 21's restart-reuse path. New FM row; remove the `restart_tainted_replids`/XREPL-2 sweep exemption (seeds 122/171/211).

## Amendment (2026-08-13)

The anti-pattern review found option (a) alone insufficient (R-C1 CRITICAL): a primary restarting *with* a dataset resumes `(id, offset)` below what replicas acked and re-issues those offsets under the same id → `+CONTINUE` over divergent bytes. Redis's RDB-aux path does both halves. Four additions, all accepted:

1. **(b) as well as (a):** every unclean primary restart shifts the old id into `replid2` at a frozen boundary and mints a fresh primary id. (a)+(b) together are the Redis shape.
2. **Atomic pairing:** the persisted offset must commit in the same atomic unit as the write it names. Manifest-time stamping biases the offset low → non-idempotent replay (INCR/LPUSH/APPEND applied twice).
3. **Sequencing:** this issue lands **before** issue 17 (both relocate `offset_at_save`; neither cited the other). FM-REPLICATION-021's "same `master_replid` after reboot" Observable inverts under this ruling and is rewritten in this issue's change set. INV-OFFSET-2's "monotone within a history" is keyed `(replication_id, epoch)`.
4. **Persistence constraints:** identity is written inside the FM-PERSISTENCE-019 quiesce window (the current post-cut sidecar write can tear the pair); a fresh id is also minted when `frogdb_wal_recovery_dropped_records_total > 0` (truncation rolled the dataset back under an unchanged identity); "recovered a dataset" means recovered it **intact** — `keys_failed == 0` under FM-PERSISTENCE-033's `continue` policy.

## Addendum (2026-08-13, anti-pattern review)

Persistence-report R4–R6 fold-in
(`.scratch/formal-spec/reviews/2026-08-13-antipattern/spec-review-persistence.md`), scoped to
this issue's change set:

- **R4 — retire `replication_state.json` as identity authority.** Once the dataset is
  authoritative, a second durable copy of identity with a different lifetime is how this bug
  comes back (FM-PERSISTENCE-038 currently regenerates and writes it back). Either delete the
  file outright, or demote it to a cache keyed on `database_id` (FM-PERSISTENCE-049) plus the
  recovered sequence number, ignored whenever either mismatches the opened database.
- **R5 — phase-order edits, three rows.** `FM-PERSISTENCE-027` gains a data dependency on phase
  2 (`OpenRocks`) and phase 3 (`RestoreShards`, for the intactness verdict), not just the
  install — extend its NOT-observable clause accordingly. `FM-PERSISTENCE-028` inverts: a
  persistence-disabled node mints a **fresh identity every boot**, not "restores its identity."
  `FM-PERSISTENCE-038`/`-039` restate their regeneration/precedence semantics against a
  dataset-authoritative source.
- **R6 — validate against the existing sequence anchor.** `SnapshotMetadataFile.sequence_number`
  is the RocksDB sequence at the checkpoint cut; whatever carries the restored `repl_offset`
  must be validated against it (same cut, same window) so a mismatch is detectable as
  corruption rather than silently adopted.

## Amendment (2026-08-22, issue-36 redesign grill — rulings R18/R20/R23)

This issue is now a sub-issue of [PRD 36](./36-offset-stamped-batches-restart-bias.md)
and **depends on [issue 37](./37-mint-at-persist-and-primary-stamps.md)**, which supplies
the mechanism the 2026-08-13 amendment's point 2 demanded:

1. **The atomic pairing IS the per-shard stamps (R18/R23).** "The persisted offset must
   commit in the same atomic unit as the write it names" is implemented by issue 37: the
   mint moves to the persist point and each shard's `WriteBatch` carries a "max offset in
   this batch" stamp key in a reserved per-shard metadata CF. This *replaces* the
   manifest/sidecar-time offset write this amendment warned against — do not build a
   second pairing mechanism here. The recovered head is `max` over the per-shard stamps;
   the recovered coverage vector is the stamps themselves.
2. **R20 sharpens the rotation rule: rotate unless *clean* shutdown.** The 2026-08-13
   ruling rotated on "no intact dataset recovered"; R20 rotates on every **unclean** boot
   even with an intact dataset, because exact stamps still cannot close the
   shipped-but-unflushed tail under relaxed durability (frames broadcast, offsets
   consumed, batch never committed — unknowable at recovery, and those offsets are on
   the wire). Unclean boot: loaded id → `secondary_id` bounded at the recovered head
   (max over stamps), fresh primary id minted — cheap, because a restarted primary's
   backlog is empty so it could never serve `+CONTINUE` anyway. **Clean-shutdown
   carve-out:** a shutdown that drained the feed, flushed every shard, and wrote a marker
   proving head == max(stamps) keeps its identity, so rolling restarts don't force
   fleet-wide resyncs. The marker is single-shot (consumed at boot).
3. **R4's state-file demotion stands** and gets easier: with identity and coverage both
   living in the dataset (stamps + dataset-metadata identity), `replication_state.json`
   has nothing authoritative left to hold. Delete or demote per R4 in this issue's
   change set.
4. **Model (R24):** this issue owns the primary-restart transitions in
   `replication_fullsync*.qnt` — lose unflushed tail, recover stamps, rotate-unless-clean
   — and the `no offset reuse within a history` invariant. Issue 38 owns the
   replica-side restart transitions. 37 lays the stamp state.

Sequencing unchanged: still lands **before** issue 17; now also **after** issue 37.
