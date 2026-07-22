# Round-10 follow-ups (batch)

Status: ready-for-agent

Deferred items surfaced during round-10 implementation (proposals 14–37) and its whole-branch
adversarial review. Each is independently actionable; split into separate issues if picked up.

## 1. Spill effects: spurious WATCH-abort + misleading `evicted` notification

`spill_for_eviction` (core/src/shard/eviction.rs) bumps the key's version — a WATCH on a key
whose value merely spilled to the Warm Tier aborts EXEC though the value is unchanged — and
emits an `evicted` keyspace notification for a key that is still readable. Both flagged by
proposal 14's adversarial review as orthogonal follow-ups (spill is tiering, not removal).

## 2. `init_cluster` dead parameters

Proposal 29 removed the `broadcaster`/`tracker` fields from `SplitBrainLogger`; the
`_replication_broadcaster`/`_replication_tracker` params of `init_cluster`
(server/src/server/cluster_init.rs) are now dead but couldn't be dropped without touching
`server/mod.rs` mid-round. Drop the params and their call-site args.

## 3. READONLY lazy-purge reindex gap (documented in proposal 15)

Field lazy-purges on READONLY commands (e.g. HGETEX without options purging expired fields)
mutate hash state without a WRITE spec, so `ReindexSpec` cannot fire. Documented divergence;
needs either a purge-effect hook or acceptance ADR.

## 4. Pre-existing: FT.SEARCH silent-swallow under Continuation Lock conflict

`connection/search/merge.rs:364,535` (`_ => {}`) + `dispatch_core.rs` sending a Keyed error
for scatter ops: an FT.SEARCH colliding with a MULTI/EXEC/Lua Continuation Lock drops the
shard error and returns an incomplete result as success. Predates round 10.

## 5. Pre-existing: reindex holes for cross-type overwrites

`SET` overwriting an indexed hash key leaves a stale search doc; `COPY`/`RESTORE` into an
index-prefix key doesn't index the destination. Never covered by the old string match either;
out of proposal-15 scope.

## 6. RocksStore `put`/`get` still `pub` (proposal 20 deviation)

Test-only-pending-removal: cross-crate callers in frogdb-replication tests and benchmarks
blocked gating. Migrate those callers to test-support constructors, then gate `put`/`get`.

## 7. Demotion-path shared-offset stamp (informational, lane-B review)

`ReplicaReplicationHandler::set_shared_offset` now stamps its `live` (0 on a fresh demotion)
into the shared HealthProbe atomic before adopting it — transient 0 until FULLRESYNC
`reset_to`. Judged an improvement over the stale-high old value; add a demotion-path test
pinning the intended sequence if it matters operationally.
