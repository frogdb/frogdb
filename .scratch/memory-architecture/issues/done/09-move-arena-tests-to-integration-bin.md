# 09: move real-thread arena tests to an integration test binary

Status: done
Type: AFK
Origin: post-wave review grill, 2026-09-01 — human reopened issue 07's test-only global_allocator flag
Area: frogdb-server/crates/server tests

## Why

Issue 07 added `#[cfg(test)] #[global_allocator] tikv_jemallocator::Jemalloc` to
`frogdb-server/crates/server/src/lib.rs` so the lib test binary's allocations hit jemalloc
arenas — without it the two real-thread arena tests read zero. It works, but it swaps the
allocator under *every* lib unit test in the crate, and it is surprising to find in lib.rs.

Human ruling: move the allocator-dependent tests to an integration test binary that declares
the requirement itself.

## What to build

1. New `frogdb-server/crates/server/tests/arena_reading.rs` with `#[global_allocator]
   tikv_jemallocator::Jemalloc` at the top and the two real-thread tests from
   `shard_arena_reading.rs` moved into it:
   `a_bound_shards_broker_reports_the_sampled_upper_bound` and
   `a_brokers_reading_is_its_own_shards_and_no_ones_else`.
2. Remove the `#[cfg(test)] #[global_allocator]` from lib.rs. Lib unit tests go back to the
   default allocator; the remaining `shard_arena_reading` unit tests (the six that don't
   need real arenas) stay where they are.
3. Whatever the moved tests need from the crate must be reachable from an integration test
   (`shard_arena_reading` is already `pub mod`; check the helpers the tests use).
4. A short comment at the integration bin's allocator declaration saying why (bytes must hit
   jemalloc arenas; mirrors main.rs and frogdb-telemetry's test setup).

## Out of scope

Changing what the tests assert. Touching frogdb-telemetry's own test allocator.

## Depends on

Nothing — issue 07 is merged. Coordinate with issue 08 if one agent takes both (same code
area).

## Resolution (2026-09-01)

Landed with issue 08 in one combined commit. New
`frogdb-server/crates/server/tests/arena_reading.rs` (own `[[test]]` entry — the crate has
`autotests = false`) declares `#[global_allocator] tikv_jemallocator::Jemalloc` with a
why-comment mirroring `main.rs` and the `frogdb-telemetry` arena tests; both real-thread
tests moved verbatim, no assertion changed. `#[cfg(test)] #[global_allocator]` removed from
`lib.rs`, so lib unit tests run on the default allocator again. The allocator-independent
unit tests stay in `shard_arena_reading.rs` — four, not the six this issue guessed — and
the module doc gained a "Where the tests are" pointer. Both moved tests pass in the new
binary; frogdb-server suite 2116/2116.
