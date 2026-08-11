# 21 — a restart keeps the replication id whose history it just lost

Status: needs-triage

## Parent

[PRD](../../PRD.md) §3 W3. Found by the seeded replication sweep
([issue 12](./12-replication-seeded-sweep.md)), seed 81 — family `mixed`, a
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
      `.scratch/hardening/specs/replication-failure-modes.md` naming its forcing
      test
- [ ] The named-gap exemption in the sweep's `XREPL-2` (see below) removed, so
      the arm witnesses the fixed behaviour instead of tolerating it

## Witness

`frogdb-server/crates/server/tests/simulation/replication_scheduler.rs` —
`check_cross_node`'s `XREPL-2a` carries a narrow named-gap exemption citing this
issue: a primary the schedule restarted, observed back at offset 0, is not
reported. The exemption is pinned both ways by
`test_xrepl_2_exempts_only_a_restarted_primary_that_is_back_at_zero`, so it
cannot widen to a live primary a replica has genuinely overtaken. Replay the
witness with:

```
REPLICATION_SEED_TRACE=1 REPLICATION_SEEDS_START=81 just replication-seeds 1
```
