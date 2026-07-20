# 02 — Unstub debug web UI shard stats + latency; collapse the provider traits

Status: needs-triage

## What to build

The Debug Bundle web UI's `get_shard_stats` and `get_latency` providers return `Vec::new()`
(debug crate `web_ui/state.rs` + `server/src/debug_providers.rs`) — the UI renders empty panels.
End-to-end: the debug UI shows real per-Internal-Shard stats and latency data for a running
node.

Per proposal 02 step 3, also collapse the three parallel provider traits feeding the debug UI
into a coherent surface (they exist because each consumer hand-assembled node state; round-8
landed the shared `WalLagAggregate` — extend the same direction).

## Acceptance criteria

- [ ] `get_shard_stats` returns real per-shard rows (key counts, memory, warm-tier counts as available)
- [ ] `get_latency` returns real latency data (source from the existing latency tracking used by LATENCY commands/INFO)
- [ ] Provider-trait count reduced; no stub-returning impls remain
- [ ] Debug UI integration/unit test asserts non-empty panels against a live node fixture
- [ ] `cargo clippy --all-targets -- -D warnings` clean on debug + server crates

## Blocked by

None - can start immediately

## Source

Round-8 P02 agent report; `.scratch/arch-deepening/proposals/02-node-state-snapshot.md` (step 3).
