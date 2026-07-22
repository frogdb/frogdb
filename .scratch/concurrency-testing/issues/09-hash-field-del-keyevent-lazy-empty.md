# 09 — Lazy hash-field-death `"del"` keyspace event not emitted

Status: done
Type: AFK
Origin: effect-scope parity review 2026-07-22 (proposal lazy-expiry-effect-scope.md Resolution section)

## What to build

Active expiry emits a generic `"del"` (not `"expired"`) keyspace event for a key whose **last
hash field** expired and emptied it — the `emptied_keys` branch of `apply_expiry_effects`
(`frogdb-server/crates/core/src/shard/event_loop.rs`, `"del"` emission alongside
`KeyspaceEventFlags::GENERIC`). The lazy-read equivalent does not fire it: hash command handlers
call `Store::purge_expired_hash_fields` on read (`frogdb-server/crates/commands/src/hash.rs`, ~13
call sites), and when the last field expires it empties the key by calling `self.delete(key)`
directly (`frogdb-server/crates/core/src/store/hashmap.rs`, `purge_expired_hash_fields`) — a
distinct seam from the whole-key `check_and_delete_expired` → `uninstall` path that the sibling
effect-scope fix drains through. Because it never lands in `lazily_purged`, none of the lazy-purge
drain points (`ShardWorker::drain_lazy_purge_effects`, `frogdb-server/crates/core/src/shard/worker.rs`)
see it, and the `"del"` event is silently dropped whenever a hash key's last field dies via a lazy
read instead of the active sweep.

This is not a mechanical port. `purge_expired_hash_fields` is shared by the active sweep
(`frogdb-server/crates/core/src/active_expiry.rs`, ~lines 193 and 575), which already reports its
own emptied keys via `ExpiryResult::emptied_keys` and applies their effects through
`apply_expiry_effects`. Pushing the lazily-emptied key into `lazily_purged` (or a new
`lazily_emptied` buffer) directly inside `purge_expired_hash_fields` would also populate that
buffer when the *active* sweep calls the same function, and the next drain would re-fire `"del"`
for a key the sweep already reported — a double notification. A correct fix needs the active-sweep
call sites to own their `emptied_keys` reporting (drain-and-discard any such buffer on that path)
so the buffer only ever fires for genuinely lazy reads, plus its own D8 real-path repro.

## Acceptance criteria

- [x] Real-path repro of the missing lazy `"del"` keyspace event for last-hash-field-death (or a
      reasoned-unreachable note if investigation shows the path cannot actually be hit).
      Landed `regression_lazy_hash_field_death_emits_del_keyevent` in
      `frogdb-server/crates/server/tests/integration_pubsub.rs`: `DEBUG SET-ACTIVE-EXPIRE 0`,
      `HSET h f v`, `HPEXPIRE h 60 FIELDS 1 f`, brief sleep, then `HGET h f` reaps the last field
      and must emit `del`. Field TTL cannot be backdated (`DEBUG EXPIRE-BACKDATE` is whole-key
      only), so a short real TTL + sleep is used. Fail-before verified: the test failed on unfixed
      code (repro commit, `#[ignore]`d with the gap reason); the fix un-ignores it.
- [x] Fix routes the lazy-emptied case through a single drain point without double-firing when the
      same `purge_expired_hash_fields` call runs under the active sweep (i.e. the active-expiry
      call sites must not also report through the new lazy seam).
      Design: added a sibling store buffer `lazily_emptied` (`store/hashmap.rs`, trait default in
      `store/mod.rs`) that `purge_expired_hash_fields` pushes to when it empties a key.
      `ShardWorker::drain_lazy_purge_effects` (`shard/worker.rs`) now drains it and fires the same
      effect set as `apply_expiry_effects`' `emptied_keys` branch — `del` with `GENERIC` (never
      `expired`), tracking + search invalidation, key-expired probe, stream-waiter drain — under
      the one per-batch version bump. All five existing lazy drain seams inherit this for free.
      Double-fire prevention: the active sweep shares `purge_expired_hash_fields` and so also fills
      `lazily_emptied`, but it owns reporting via `ExpiryResult::emptied_keys`; `run_active_expiry`
      (`shard/event_loop.rs`) take-and-discards the buffer after `run_cycle`, so a later command's
      lazy drain never re-fires `del` for a swept key. Between event-loop iterations the buffer is
      empty (every command drains at its own seam), so the discard only drops the current cycle's
      own output. Pinned by `lazy_hash_field_death_del_event_fires_once_under_active_sweep`
      (integration) and `active_sweep_emptied_key_does_not_double_fire_del` (worker unit).
- [x] Existing hash-field-TTL and keyspace-notification tests stay green.
      Verified: `frogdb-core` store/shard/active_expiry (68 + effect_tests), `frogdb-commands`
      hash/hexpire (21), `frogdb-server` hash + keyspace/keyevent/notify (24) + `integration_hashes`
      (10) all pass. Also added worker-unit `lazy_emptied_hash_key_drains_del_event`.

## Blocked by

None — can start immediately.

## References

- Proposal: `.scratch/concurrency-testing/proposals/lazy-expiry-effect-scope.md` (see "Hash-field
  death (`"del"`) — reachable via a distinct seam, scoped as a follow-up" in the Resolution section)
- `frogdb-server/crates/core/src/store/hashmap.rs` (`purge_expired_hash_fields`)
- `frogdb-server/crates/core/src/shard/event_loop.rs` (`apply_expiry_effects`, `"del"` emission for
  `emptied_keys`)
