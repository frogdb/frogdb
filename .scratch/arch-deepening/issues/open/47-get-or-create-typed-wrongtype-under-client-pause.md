# 47 — `get_or_create_typed` returns `WRONGTYPE` for an expired key while `CLIENT PAUSE` suppresses physical deletion

Status: needs-triage

## What to build

`StoreTypedExt::get_or_create_typed` (`frogdb-server/crates/core/src/store/typed.rs:198-213`) is the
"lazy-expire-then-create" seam — its own doc block at `:195-197` promises the key *"is purged up
front (`Store::purge_if_expired`) and then treated as absent — the fresh value is created without the
stale TTL, matching Redis lazy-expire-then-write semantics"*. The body does not keep that promise
under one state:

```rust
self.purge_if_expired(key);          // :202 — return value DISCARDED
match self.get(key) {                // :203 — raw, expiry-blind get
    Some(v) if T::from_value(&v).is_none() => return Err(WrongTypeError),
    Some(_) => {}
    None => { self.set(key.clone(), T::create_default()); }
}
```

`purge_if_expired` bottoms out in `HashMapStore::check_and_delete_expired`
(`store/hashmap.rs:480-498`), which has an early return the seam does not account for: when
`self.expiry_suppressed` is set — i.e. during `CLIENT PAUSE` — it *reports the key expired without
deleting it* (`hashmap.rs:485-487`, comment: *"During CLIENT PAUSE, suppress physical deletion but
treat as expired"*). So `purge_if_expired` returns `true` while the corpse is still in the map;
`self.get(key)` at `:203` is the raw expiry-blind door and hands back the **expired value**;
`T::from_value(&v)` inspects the *old* type; and a `get_or_create_list` against a paused,
past-deadline **string** key returns `WrongTypeError` instead of creating a fresh list. The client
sees `-WRONGTYPE` for a key that logically does not exist. Redis's `lookupKeyWrite` treats the key
as absent and creates.

The discarded `bool` at `:202` is the whole bug: `purge_if_expired` already returns exactly the fact
the seam needs. Branch on it — if it returned `true`, take the `None` arm unconditionally (create
fresh, no stale TTL) rather than re-probing through `self.get`. Roughly one line. The sibling
methods that call `purge_if_expired` up front — `get_typed_mut` (`typed.rs:135`, probe at `:138`),
`check_typed` (`:182`, probe at `:185`) — share the same discard-then-reprobe shape and should be
audited in the same pass; `get_typed` (`:169`) composes `get_with_expiry_check` and is not affected.

**Blast radius.** LIVE on main today, but narrow: it fires only inside the `CLIENT PAUSE` window,
which is also the one state where **both** expiry paths stall — `run_active_expiry`
(`shard/event_loop.rs:224`) returns early at `:232` *after* calling `set_expiry_suppressed(true)`,
so neither the 100 ms sweep nor lazy deletion can reap, and the window is bounded only by the
pause duration rather than by the ordinary ~100 ms. This defect is **pre-existing and independent
of proposal 97** — the review filed it here precisely because 97's Class-A/B migration routes ~21
additional call sites through `get_or_create_typed`, which would turn a narrow seam bug into a
broad one. Fixing it is a prerequisite for that migration being safe, not a consequence of it. It
belongs to whoever owns the pause/expiry interaction, and the fix should be pinned by a failure-mode
test rather than an incidental one, since the trigger state is otherwise never exercised.

## Acceptance criteria

- [ ] With `CLIENT PAUSE` active and a past-deadline string key `k`, a list-creating command
      (`RPUSH k v`, or any handler reaching `get_or_create_list`) creates a fresh list and succeeds
      instead of returning `-WRONGTYPE`
- [ ] The created value carries no stale TTL (the pre-pause deadline does not survive the create)
- [ ] `get_typed_mut` and `check_typed` are audited for the same discard-then-reprobe shape and
      either fixed or documented as unaffected at the code
- [ ] Regression unit test in `store/typed.rs` (alongside the five existing expiry tests at
      `:484-543`) named `get_or_create_typed_creates_when_expiry_is_suppressed`, which sets
      `expiry_suppressed`, plants an expired string, and asserts a list is created
- [ ] `just test frogdb-core typed` green

## Blocked by

None - can start immediately

## Source

Round 38-99 adversarial review of proposal 97 (`.scratch/arch-deepening/proposals/97-typed-store-access.md`),
defect F-7 — `get_or_create_typed` (`store/typed.rs:198-213`) × `check_and_delete_expired`
(`store/hashmap.rs:480-487`), named in the proposal's §Risks as a pre-existing defect 97 inherits
but does not cause, explicitly "issue to file, not implemented here".

## Comments
