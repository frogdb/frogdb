# Proposal: Lazy-expiry effect parity (per-key expired-watch tracking)

Status: needs-triage
Origin: phase-4b final review + task-9/task-12 reviews (2026-07-21, merge `252b39af`)

## Problem

The store-layer lazy purge (`check_and_delete_expired`, `frogdb-server/crates/core/src/store/hashmap.rs`; reached via `get_with_expiry_check` and `purge_if_expired`) is **version- and wait-queue-ignorant**. Active expiry (`apply_expiry_effects`, `event_loop.rs`) bumps the shard version and — since F1 (`63eff9a2`) — drains blocked XREADGROUP waiters to NOGROUP. The lazy path does neither. Phase 4b closed the EXEC-time and WATCH-time validation seams (F3, `673a52c9`) and the active-sweep seam (F1), but four gaps share this one root cause:

1. **Lazy path doesn't drain XREADGROUP waiters.** A blocked XREADGROUP waiter on a TTL-expired stream stays parked until BLOCK timeout when the key is lazily purged by any read (`TYPE`/`XLEN`/`EXISTS`/another XREADGROUP) instead of the active sweep.
2. **Racing lazy read nullifies the F1 drain.** Lazy purge removes the key from the store, so the later active sweep's `deleted_keys` never contains it and the F1 drain never fires — a read racing ahead of the sweep silently disables the fix.
3. **Read-path lazy purge doesn't bump the WATCH version.** A watched key lazily purged by a third party's read leaves the per-shard version untouched → WATCH false negative (under-abort), same class as F3 but on the read path.
4. **Concurrent lazy-purge under-abort for a second watcher.** Watcher B watches `k` live; `k` expires; watcher A's WATCH-time purge (dispatch_core.rs, deliberately no-bump per F3 design) removes `k`; B's later EXEC finds nothing to purge, doesn't bump, commits over a live→gone transition.

## Direction

Redis solves this with per-key state: `signalModifiedKey`/`keyModified` → `touchWatchedKey` fires on expiry-at-lookup (redis/redis PR #7920, issue #7918), plus `wk->expired` recorded at WATCH time. Point-fixing the four gaps separately at the coarse per-shard version is the wrong seam (gap 4 is unfixable that way). Proposed shape:

- Per-key expired-watch tracking (`wk->expired`-style) in the watch registry, replacing reliance on the coarse `shard_version` for expiry-driven invalidation.
- A lazy-purge effects seam: when the store reports a lazy removal, the worker (not the store — store stays version-ignorant, F3 encapsulation constraint) applies the same effects as active expiry: version/watch invalidation + stream-waiter drain.
- Discipline: each gap needs a **real-path (turmoil) repro before its fix lands** (D8 rule). Gap 2 and 3 repros likely mirror `watch_lazy_expiry_false_negative_realpath` (note the turmoil clock trap: TTL is real-clock `Instant`; use `std::thread::sleep`).

## Acceptance criteria

- [ ] Turmoil real-path repro (or reasoned unreachability note) for each of gaps 1-4
- [ ] Lazy purge produces the same externally observable effects as active expiry (WATCH abort, XREADGROUP NOGROUP drain) on all paths
- [ ] Store remains version- and wait-queue-ignorant (worker-seam only)
- [ ] Shard-driver scenarios extended to cover the closed gaps (S2 lazy arm, S5 lazy arm)
- [ ] `regression_`-pinned tests for each confirmed gap

## References

- Ledger: `.superpowers/sdd/phase4b/progress.md` (tasks 9, 12; follow-up header)
- F3 two-seam fix: `673a52c9`; F1 drain: `63eff9a2`
- Spec: `docs/superpowers/specs/2026-07-17-concurrency-invariant-testing-design.md`
