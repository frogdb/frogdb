# Round-10 follow-ups (batch)

Status: done (landed 2026-07-22, branch arch-deepening/impl)

Resolution summary — all 7 items implemented and merged:
1. Spill: no version bump / no `evicted` notification / no USDT probe (fc121930); observability
   stays on TieredSpills metrics. Red-green tests pin both behaviors.
2. `init_cluster` dead params dropped (1b30991d).
3. Lazy-purge hook BUILT (no ADR): `lazily_shrunk` store buffer drained by both the lazy-read
   seam and the active-expiry sweep via shared `reindex_shrunk_hash_keys` (fac157f3). Note:
   FrogDB's HGETEX is a WRITE — the real gap was the pure-READONLY hash readers. Bonus: fixed
   the same latent hole in the active-expiry sweep.
4. FT.SEARCH conflict swallow fixed (4601be5f) + whole-branch review found the fix incomplete
   for keyless scatter ops (FT.DROPINDEX returned OK while a locked shard skipped the drop) —
   closed with `PartialResult::ShardError` + central `FatalReply` abort in `ScatterGather::run`
   (0d322ee1); silent truncation now impossible by construction for all broadcast merges.
5. Cross-type overwrite holes fixed via `ReindexAction::Refresh` +
   `RefreshFirstKey`/`RefreshSecondKey` on SET/SETEX/PSETEX/RESTORE/COPY/RENAME/RENAMENX
   (fac157f3). Full clobber audit in the proposal-15 conformance test. Remaining narrow gap
   (pre-existing, out of scope): COPY/RESTORE of a *JSON* doc into a JSON-index prefix is not
   indexed — see issue 16.
6. `put`/`get` gated `#[cfg(any(test, feature = "test-support"))]` (20dc6c6d); no production
   feature leak (verified `cargo tree -e features,no-dev`).
7. Demotion-path test added (e65255d0); code behaves exactly as described.

Review-fix extras (0d322ee1): EsAllMerge surfaces embedded errors; no-op SET NX/XX miss and
no-op COPY now set `write_was_noop` (stops phantom replication/reindex/notifications —
matches Redis, follows the RENAMENX precedent).

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
