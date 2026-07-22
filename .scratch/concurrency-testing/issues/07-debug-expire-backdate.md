# 07 — DEBUG command to backdate key expiry (kill the real-clock sleep in sim tests)

Status: done
Type: AFK
Origin: lazy-expiry plan review 2026-07-21

## What to build

A DEBUG subcommand (e.g. `DEBUG EXPIRE-BACKDATE <key> <ms>`) that rewrites a key's expiry timestamp to lie `<ms>` in the past, making the key already-expired without any wall-clock wait.

Motivation: TTL expiry is evaluated against real-clock `std::time::Instant` while turmoil advances a virtual clock, so every sim test that needs an elapsed TTL carries a `std::thread::sleep(50ms)` (the stage-1 lazy-expiry repros in `frogdb-server/crates/server/tests/simulation.rs`, and the F3 pin) — a real wall-clock stall inside a simulation that is otherwise deterministic and virtual-time-driven. This cross-clock hazard is also why the S7 (`client_pause_write_vs_exec`) expiry sub-assertion was dropped entirely rather than pinned (see [issue 03](./03-turmoil-clippy-lint-gap.md), which corrected the doc comment's rationale for the drop to cite this real-clock-TTL blocker specifically, not a client-observability argument). A backdate command makes "key is now expired" deterministic and instant under turmoil: set the expiry timestamp directly into the past instead of waiting for real time to catch up to it.

Note: Redis has no equivalent DEBUG subcommand for this. DEBUG is an unstable, non-parity-constrained interface (no client-visible protocol guarantee), so this is a pure test-infrastructure addition — there is no cross-engine parity obligation to reconcile.

## Acceptance criteria

- [x] `DEBUG EXPIRE-BACKDATE <key> <ms>` implemented, mirroring `DEBUG SET-ACTIVE-EXPIRE`. Wiring: parse/dispatch in `debug_conn_command.rs` (`b"EXPIRE-BACKDATE"` arm + `dynamic_keys`); `DebugProvider::expire_backdate` in `conn_command.rs`; handler round-trip in `debug_handler.rs` via `query_one` to the key's owning shard; `DebugIntrospectionMsg::ExpireBackdate` in `shard/message.rs`; dispatch in `shard/dispatch_debug_introspection.rs` (now `&mut self`); store method `HashMapStore::backdate_expiry` + `BackdateExpiryResult` in `store/hashmap.rs`. No-TTL key → `ERR key has no expiry to backdate`; missing key → `ERR no such key`. The method rewrites only the deadline (metadata `expires_at` + expiry index) — it never purges, never pushes to `lazily_purged`, never bumps the version. Direct tests: `backdate_expiry_*` (store) and `expire_backdate_*` (executor).
- [x] Sim-test `std::thread::sleep` TTL-elapse waits replaced with `DEBUG EXPIRE-BACKDATE` in all four sites in `tests/simulation.rs`: `watch_lazy_expiry_false_negative_realpath` (F3 pin), `xreadgroup_ttl_no_nogroup_realpath` (F1 pin, backdate + kept the active-expiry sweep window), `regression_watch_read_lazy_purge_aborts_realpath` (gap 3), `regression_watch_second_watcher_aborts_realpath` (gap 4). PEXPIRE-before-WATCH ordering preserved in each (backdate doesn't bump the version). All doc comments updated; zero `std::thread::sleep` left in the file. Fail-for-right-reason spot-checked by temporarily neutering `backdate_expiry` (tests flip to failing).
- [x] S7 (`client_pause_write_vs_exec`) expiry sub-assertion pinned. Backdate removes the cited real-clock-TTL blocker, so the sub-assertion is now pinned in a dedicated focused test `client_pause_write_expiry_suppression_realpath` (kept out of S7's seed-looped 3-connection body to avoid conflating concerns): under `CLIENT PAUSE WRITE`, a backdated key reads gone to `GET` (nil) yet `OBJECT ENCODING` still sees it physically present (client-visible suppression), and after `UNPAUSE` the next sweep reaps it (`no such key`). S7's doc comment updated to point at the pin.
- [x] Doc note added to `website/src/content/docs/architecture/debugging.md` (DEBUG subcommand table, alongside the `SET-ACTIVE-EXPIRE` entry) and to the `DEBUG HELP` output.

## Blocked by

None, but this should land **after** the lazy-expiry fix-stage plan (`docs/superpowers/plans/2026-07-21-lazy-expiry-parity-fix.md`) completes, to avoid churning the very `std::thread::sleep`-based tests that plan is actively flipping from `#[ignore]`d repros to active `regression_` pins.
