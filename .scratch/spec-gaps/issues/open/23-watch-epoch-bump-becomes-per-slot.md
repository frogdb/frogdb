# 23: WATCH epoch bump becomes per-slot — field expiry stops starving unrelated CAS loops

Status: ready-for-agent

## Origin

Distsys-review MAJ-21 (`.scratch/formal-spec/2026-08-13-independent-distsys-review.md`),
ruled **accept-plus** by the user 2026-08-14
([rulings ledger](../../../formal-spec/2026-08-13-distsys-review-rulings.md)):
fix the epoch starvation, document the slot aliasing as a deviation.

## What is wrong

Two undocumented divergences from `specs/txn.md`'s per-key WATCH language
(FM-TXN-033 `:707-710`; deviations table `:905-909` has no row for either):

1. **Global epoch starvation (the fix target)** — `shard/event_loop.rs:365`:
   `if result.fields_expired > 0 { self.bump_version_global(); }`. Every
   active-expiry cycle that reaps at least one hash *field* bumps the shard-wide
   epoch (`SlotVersions::version_for` = per-slot stamp + `global_epoch`,
   `worker.rs:56-103`), invalidating **every** outstanding WATCH on the shard. A
   tenant running `HEXPIRE` field TTLs on a continuously-expiring hash starves
   every other connection's WATCH/MULTI/EXEC CAS loop on that shard — nil abort on
   every attempt, forever, no error, no metric. Unbounded liveness violation; the
   "safe over-abort" comment covers safety only.
2. **Slot aliasing (document, keep)** — `get_key_version` =
   `version_for(slot_for_key(key))` (`worker.rs:633`): a write to *any* key sharing
   the watched key's hash slot aborts the CAS. Redis is key-granular
   (`CLIENT_DIRTY_CAS` per watched key). Bounded over-abort noise (~70 unrelated
   aborts/s per watched slot on a hot shard) — the CAS retry succeeds, progress is
   made. Not worth per-key version tracking today; must be *documented*, not
   silent (FoundationDB analogy: conflict-range widening is always declared).

## What to build (spec-first; txn locked, gate 0.90)

1. Spec rows first:
   - Amend FM-TXN-033 to the honest slot-granular trigger ("any write to a key in
     a watched key's hash slot"), and add the bounded-epoch postcondition: field
     expiry invalidates only the slots of the keys whose fields expired — never
     the whole shard.
   - Two new rows in the Redis-deviations table: slot-granular WATCH (aliasing,
     with the FM-TXN-033 cross-ref) and field-expiry slot invalidation.
2. Code — mirror the lazy path: `worker.rs:768-776` already enumerates `shrunk`
   keys and bumps their slots instead of the epoch. The active-expiry path does
   the same — expiry result carries the victim keys (or their slots), event loop
   bumps `slot_for_key(k)` for each; `bump_version_global()` call at
   `event_loop.rs:365` deleted (or reserved for paths that genuinely cannot
   enumerate victims — if any remain, each is documented at the call site).
3. Observability: `watch_aborted{reason=...}` counter at the abort site —
   `reason="watched-slot-write"` vs `reason="expiry"` (plus any remaining
   epoch reason). Starvation becomes diagnosable; misleading-silence dies.
4. Forcing tests:
   - Starvation regression: connection A holds WATCH on key `k`; hash `h` (different
     slot, same shard) continuously expires fields; A's EXEC succeeds (fails
     pre-fix: nil abort every cycle).
   - Same-slot field expiry still aborts the WATCH (safety preserved).
   - Metric emitted with correct reason on both abort classes.

## Cross-references

- [Issue 11](11-exec-fast-path-ignores-watch-set.md): same `check_watches`
  machinery — coordinate if in flight.
- [Issue 09](09-routing-epoch-carry-and-watch-recheck.md): WATCH re-check on
  routing-epoch change is a different epoch — unrelated mechanism, keep distinct.

## Acceptance criteria

- [ ] FM-TXN-033 amended + two deviation rows; `just lint-spec` green
- [ ] Active-expiry bumps per-slot; shard-wide `bump_version_global` gone from the
      expiry path (any survivor documented at the call site)
- [ ] Starvation forcing test fails pre-fix, passes post-fix; same-slot safety test
      passes
- [ ] `watch_aborted{reason}` counter live
- [ ] `just mutants-diff` on frogdb-core touched paths + txn gate (0.90) triaged

## Blocked by

None — coordinate with issue 11 if in flight.
