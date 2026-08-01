# No workload profile emits cross-shard ops — the lock-leak checker never sees a scatter

Status: ready-for-agent
Type: AFK
Origin: round-2 testing audit 2026-07-28 — 15 parallel area audits, `.scratch/testing-improvements-round2/`
Source: INFRASTRUCTURE.md I7
LOE: ~0.5 day (measured)
Tier: A
Area: crates/testing / workload profiles (VLL lock table)
Asked by: 12 (F1) — "the single cheapest high-severity item in the audit"

## Context

The VLL lock table's multi-key path is the one most likely to leak a lock, and no generated
workload ever exercises it: the profile generator emits no multi-key command at all. Every
piece of machinery needed to catch a leak — the quiescence checker, the lock-table probe, the
runner — already exists and is pointed at workloads that cannot trigger the bug. Adding one
profile is roughly half a day.

## Evidence

- `Profile` enum at `testing/src/workload.rs:20`, generation at `:150`.
- The file emits **zero** MGET / MSET / DEL — verified by grep.
- The quiescence checker, the lock-table probe and the runner all already exist and need no
  changes.

## What to build

1. A `ScatterHeavy` variant on the `Profile` enum (`testing/src/workload.rs:20`).
2. Generation for it at `:150`, emitting multi-key cross-shard operations — MGET, MSET, DEL
   over key sets that deliberately span shards.
3. Register the profile with the existing runner so the existing lock-leak / quiescence
   checker runs against it. No changes to the checker, the probe or the runner.

## Acceptance criteria

- [ ] A profile emitting cross-shard ops exists; the existing lock-leak checker runs against
      it and passes (or fails, which is the point).
- [ ] Generated histories under the new profile contain MGET, MSET and DEL, and at least one
      generated key set spans more than one shard.
- [ ] The new profile is reachable from the runner without any change to
      `conservation`/quiescence code.
- [ ] If the checker fails against the new profile, the failure is filed as a defect issue
      rather than the profile being weakened.

## Test boundary

Level 4 — the workload runner drives a real multi-shard server and the lock-table probe reads
live state; the behaviour is cross-shard routing plus lock lifetime, which no single-shard
level-3 driver can express. It does not need multiple nodes, so not level 5.

## Depends on

Nothing.
