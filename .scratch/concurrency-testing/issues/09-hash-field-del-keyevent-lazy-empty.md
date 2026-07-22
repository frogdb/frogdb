# 09 — Lazy hash-field-death `"del"` keyspace event not emitted

Status: needs-triage
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

- [ ] Real-path repro of the missing lazy `"del"` keyspace event for last-hash-field-death (or a
      reasoned-unreachable note if investigation shows the path cannot actually be hit).
- [ ] Fix routes the lazy-emptied case through a single drain point without double-firing when the
      same `purge_expired_hash_fields` call runs under the active sweep (i.e. the active-expiry
      call sites must not also report through the new lazy seam).
- [ ] Existing hash-field-TTL and keyspace-notification tests stay green.

## Blocked by

None — can start immediately.

## References

- Proposal: `.scratch/concurrency-testing/proposals/lazy-expiry-effect-scope.md` (see "Hash-field
  death (`"del"`) — reachable via a distinct seam, scoped as a follow-up" in the Resolution section)
- `frogdb-server/crates/core/src/store/hashmap.rs` (`purge_expired_hash_fields`)
- `frogdb-server/crates/core/src/shard/event_loop.rs` (`apply_expiry_effects`, `"del"` emission for
  `emptied_keys`)
