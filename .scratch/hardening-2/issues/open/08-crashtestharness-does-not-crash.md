# `CrashTestHarness` does not crash, and its durability modes are decorative

Status: ready-for-agent
Type: bug (test infrastructure) — invalidates evidence, not production behavior
Severity: likelihood 3/3 (every crash-recovery test in `frogdb-core` uses it), consequence 3/3
(ten FM-PERSISTENCE tags claim crash durability is witnessed and none of them is) — score 9
Area: core / persistence test harness

## Problem

`frogdb-server/crates/core/src/persistence/test_harness.rs` is the harness behind
`crash_recovery_tests.rs`, which supplies **10 of the 27 weak persistence witnesses** found by the
campaign-2 witness audit. Three independent defects make it unable to witness what it is cited for.

**1. `crash()` is a clean shutdown** (`:183-186`):

```rust
pub fn crash(&mut self) {
    // Drop RocksStore without sync - simulates crash
    self.rocks = None;
}
```

Dropping a `RocksStore` runs RocksDB's destructor, which flushes memtables and closes the WAL
cleanly. Nothing unsynced is ever lost, so **no test built on this harness can distinguish a
durable write from a non-durable one**. `simulate_crash()` (`:190`) is an alias, so the more
convincing name has the same body.

**2. The three durability-mode constructors set a `WalConfig` nothing reads.**
`with_sync_mode()`/`with_periodic_mode()`/`with_async_mode()` (`:74`, `:85`, `:96`) each build a
`WalConfig` and stash it at `:65`. The only consumer is `create_wal_writer()` (`:120-127`).

**3. Every write helper bypasses the WAL entirely.** `put_direct` (`:130`), `put_with_expiry`
(`:150`), `put_with_lfu` (`:160`) call `rocks().put()`; `put_with_sync` (`:139`) sets
`WriteOptions::set_sync` directly. None routes through `RocksWalWriter`, so a test that constructs
the harness `with_async_mode()` and then calls `put_direct` is not exercising async WAL durability —
it is exercising a default-options RocksDB put. The mode selection has no effect on anything the
test then does.

Net: a test can say "crash in async mode, reopen, assert the key is gone", and it will fail —
because the key is still there, because there was no crash and no async mode. So instead the tests
assert the weaker thing that passes. `crash_recovery_tests.rs:702` is the endpoint of that
pressure: `assert!(result.is_ok() || result.is_err())`.

## Fix

This is a W2 prerequisite — the crash rows cannot be re-witnessed until the harness crashes.

1. Replace `crash()` with a real crash primitive. Two options, and the choice should be recorded:
   an out-of-process child killed with `SIGKILL` (matches W2's cluster harness, most faithful), or
   an in-process `PageCacheSink`-style layer that discards unsynced pages on drop (cheaper, keeps
   the tests fast, already has precedent in this repo). Do not keep a method named `crash` that
   performs a clean drop under any circumstances.
2. Route the write helpers through `create_wal_writer()` so `WalConfig` is load-bearing, or delete
   the three mode constructors. A constructor that selects a durability mode nothing honours is
   worse than no constructor.
3. Re-witness the ten weak FM-PERSISTENCE tags against the fixed harness, and use
   `MISSING ([gap: …](…))` for any row the new primitive still cannot force.

## Verification

The definitive check: a test that writes in async mode, crashes, reopens, and **asserts the key is
absent**. On today's harness that assertion fails. On a correct one it passes, and it fails again if
the durability mode is forced back to sync — which is the property the whole file claims to test
and none of it does.

## Comments

Found by the campaign-2 witness audit, 2026-08-07. Same root cause as round-2 infra issue 02
(`ClusterNode::kill()` is a graceful shutdown): both harnesses named a crash primitive after the
thing it was supposed to simulate, and neither simulates it.
