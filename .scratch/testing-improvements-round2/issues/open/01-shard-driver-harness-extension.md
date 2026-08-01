# `shard_driver` harness cannot reach eviction, persistence, WAL, RESP2 or blocking paths

Status: ready-for-agent
Type: AFK
Origin: round-2 testing audit 2026-07-28 — 15 parallel area audits, `.scratch/testing-improvements-round2/`
Source: INFRASTRUCTURE.md I1
LOE: 1–2 days (measured)
Tier: A
Area: frogdb-core / `core/tests/shard_driver/` harness
Asked by: 01 (F1, F2 — "should be built once, first"), 02, 06 (F7, F13), 07 (item 1)
Unblocks: 01/F2, F5, F6, F8, F12, F14 · 02/F1, F5, F13(b) · 06/F7, F13 · most of 07

## Context

`shard_driver` is the level-3 boundary most round-2 findings want to be written at: real
command dispatch, real shard worker, real WAL seam, no socket. Four independent area audits
asked for the same extension, and 01 asked for it to be "built once, first". The bulk of the
work is not new machinery — the capabilities already exist on `ShardWorkerBuilder` and are
simply not forwarded by the harness. Only three items below are genuinely new.

## Evidence

- `ShardDriver::new(n)` (`core/tests/shard_driver/harness.rs:55`) hardcodes a 5-call builder
  chain — `with_message_rx`, `with_new_conn_rx`, `with_shard_senders`, `with_registry`,
  `build`.
- Everything the findings need already exists on `ShardWorkerBuilder` and is simply not
  forwarded: `with_eviction` (`builder.rs:207`), `with_persistence` (`:225`),
  `with_replication` (`:201`), `with_wal_mode` (`:219`), `with_fake_wal_failure` (`:286`),
  `with_scripting` (`:213`).
- Existing `drive_*` seams in `core/src/shard/event_loop.rs`: `:350`, `:360`, `:376`, `:410`.
- `ShardDriver::execute` hardcodes `Resp3`, so no RESP2 shape assertion is possible (06).
- `block_wait` (`harness.rs:202`) already exists but enters at the waiter layer and skips
  argument parsing entirely, which is what 06/F7 and F13 need to cover.

## What to build

1. Forward the six existing `ShardWorkerBuilder` options listed above through `ShardDriver`
   construction, so a test can opt into eviction, persistence, replication, a WAL mode, fake
   WAL failure and scripting.
2. `drive_register_tracking(conn_id, mode, prefixes) -> InvalidationReceiver` — a fifth
   `drive_*` seam in `core/src/shard/event_loop.rs` beside the existing four, mirroring
   `drive_capture_keyspace`. Requested by 02.
3. A `ProtocolVersion` parameter on `ShardDriver::execute`, replacing the hardcoded `Resp3`.
4. A wrapper driving a *blocking* command through `blocking.rs::execute()`, entering above
   argument parsing rather than at the waiter layer.

## Acceptance criteria

- [ ] A test can construct a driver with eviction + a warm store, run a command that spills,
      and assert on both the store and the invalidation stream, without a socket.
- [ ] Each of the six builder options above is settable from a `core/tests/shard_driver/`
      test, and at least one test per option exercises it.
- [ ] A test drives the same command under RESP2 and RESP3 and asserts the two reply shapes
      differ where the spec says they should.
- [ ] A blocking command driven through the new wrapper rejects a bad-arity / wrong-type
      argument *before* any waiter is registered.

## Test boundary

Level 3 — this **is** the level-3 harness. Everything it unblocks (eviction, spill, WAL
failure, tracking invalidation, RESP2 shape, blocking argument parsing) is shard-worker
behaviour that needs no socket, connection layer or routing; forcing it to level 4 is the
anti-pattern `BRIEF.md` calls out.

## Depends on

Nothing.
