# 02 — Unstub debug web UI shard stats + latency; collapse the provider traits

Status: ready-for-human

## What to build

The Debug Bundle web UI's `get_shard_stats` and `get_latency` providers return `Vec::new()`
(debug crate `web_ui/state.rs` + `server/src/debug_providers.rs`) — the UI renders empty panels.
End-to-end: the debug UI shows real per-Internal-Shard stats and latency data for a running
node.

Per proposal 02 step 3, also collapse the three parallel provider traits feeding the debug UI
into a coherent surface (they exist because each consumer hand-assembled node state; round-8
landed the shared `WalLagAggregate` — extend the same direction).

## Acceptance criteria

- [x] `get_shard_stats` returns real per-shard rows (key counts, memory, warm-tier counts as available)
- [x] `get_latency` returns real latency data (source from the existing latency tracking used by LATENCY commands/INFO)
- [x] Provider-trait count reduced; no stub-returning impls remain
- [x] Debug UI integration/unit test asserts non-empty panels against a live node fixture
- [x] `cargo clippy --all-targets -- -D warnings` clean on debug + server crates

## Blocked by

None - can start immediately

## Source

Round-8 P02 agent report; `.scratch/arch-deepening/proposals/02-node-state-snapshot.md` (step 3).

## Comments

### 2026-07-20 — implemented

**Commits** (branch `worktree-agent-a5640d27881c41c43`):
- `f1666500` feat(debug): unstub shard-stats/latency providers; collapse debug UI provider traits
- `6caddb2d` feat(debug): surface per-shard stats panel in debug UI

**Data sources wired (real ShardMessage scatter over the live shard senders — the
same ones INFO/LATENCY/SLOWLOG use):**
- `get_shard_stats` → `ShardMessage::InfoSnapshot` per shard → `ShardStats {
  shard_id, keys, memory_bytes, peak_memory_bytes, hot_keys, warm_keys }`
  (key count + data/peak memory from `InfoShardSnapshot.memory`, hot/warm tier
  counts from `InfoShardSnapshot.tiered`).
- `get_latency` → `ShardMessage::LatencyLatest` (newest sample per event across
  shards) + `ShardMessage::LatencyHistory` (merged per-event history) → min /
  max / avg / sample-count, exactly mirroring the `LATENCY` command gather.
- `get_slowlog` (also unstubbed as a bonus) → `ShardMessage::SlowlogGet`.

**Provider-trait collapse (proposal 02 step 3):** the three parallel traits
`ReplicationInfoProvider` + `ClientInfoProvider` + `ClusterInfoProvider` (3) are
replaced by a single `NodeStateProvider` (1), implemented once by a server-side
`ServerDebugProvider` composite (`debug_providers.rs`) that reads replication
identity, connected clients, and cluster topology from one place. The dead
`DebugShardMessage`/`DebugQuery`/`DebugQueryResponse` stub protocol and the unused
second shard-sender field were deleted. All three `Vec::new()` stub methods now
return real data. Cluster slot-range compaction still delegates to the canonical
`frogdb-cluster::get_node_slots` (no re-implementation). Replication identity now
reflects real role/replicas/offset instead of always reporting `standalone`.
Shapes are plain data values a future single `NodeStateSnapshot::collect()`
(issue 03) can populate — issue 03 itself is NOT implemented here.

**Surfaced end-to-end:** added `/debug/api/shard-stats` + `/debug/partials/shard-stats`
routes and folded the shard-stats table into the performance partial (previously
`get_shard_stats` had no route at all, so unstubbing alone would not have shown
anything).

**Tests** (`frogdb-debug`, `web_ui::state::tests`, all passing):
- `shard_stats_returns_real_rows` — populates 2 mock shards (3 + 7 keys) over the
  real `ShardMessage` protocol; asserts one row per shard, correct per-shard key
  counts / memory / hot / warm, and total keys = 10.
- `latency_returns_real_data` — asserts aggregated command-event row with
  max=5000us, min=3000us, samples=4.
- `slowlog_returns_real_entries` — asserts one entry per shard with reconstructed
  `GET key` command + client addr.
- `shard_stats_partial_renders_non_empty_panel` — end-to-end: calls the partial
  handler against a live shard fixture, asserts the rendered HTML is a non-empty
  table (no empty-state placeholder) with real key counts.
- `unwired_state_returns_empty_not_panic` — guards the None-senders path.
- Pre-existing `debug_providers.rs` flag-conversion tests retained.

**Verification:** `just check` (full workspace) green; pre-commit hook ran
`cargo clippy --all-targets -- -D warnings` + `cargo fmt --all --check` clean on
both commits (workspace-wide, covers debug + server).

**Note / follow-up:** `with_bundle_support` and the diagnostic-bundle path are
still not wired into `subsystems.rs` (they were already unwired before this
change — `bundle_shard_senders` was always `None`). The bundle path now shares
the single `shard_senders` field, so wiring bundles later is just a
`with_bundle_support(config, tracer)` call. Out of scope for this issue.
