# 52 — MIGRATE single grammar walker

**Status:** implemented (2026-07-16, `7b71e87c`, `ac233ce8`)
**Severity:** Moderate (duplicated grammar walk; divergence means wrong key routing/locking)
**Found:** round-6 deepening review fan-out over server command plumbing.

## Problem

The MIGRATE argument grammar
(`MIGRATE host port <key|""> destination-db timeout [COPY] [REPLACE] [AUTH password]
[AUTH2 username password] [KEYS key...]`) is walked by two independent hand-rolled loops that
must agree by convention:

- `MigrateArgs::parse` (`frogdb-server/crates/server/src/migrate.rs:87-177`) — the executor's
  parse; owns `Bytes`, validates, errors on unknown options.
- `MigrateCommand::dynamic_keys`
  (`frogdb-server/crates/server/src/commands/migrate_cmd.rs:54-87`) — the dispatcher's key
  extraction (feeds slot validation, ACL key checks, locking); returns borrowed
  `Vec<&'a [u8]>`, silently skips unknown args.

Both re-implement the AUTH (+2) / AUTH2 (+3) / KEYS (rest-are-keys) skipping. Any grammar change
applied to one but not the other makes the dispatcher extract different keys than the executor
migrates — cross-slot validation and ACL checks then guard the wrong keys. `dynamic_keys` has
**zero direct tests** today.

## Design

One walker, two consumers. An internal key-position walker, e.g.

```rust
fn key_positions(args: &[Bytes]) -> Vec<usize>
```

owning the positional-key rule (`args[2]` iff non-empty) and the single
COPY/REPLACE/AUTH/AUTH2/KEYS skip loop. Then:

- `dynamic_keys` = `key_positions(args).map(|i| args[i].as_ref())` — borrow-friendly.
- `MigrateArgs::parse` consumes the same positions for its owned `keys` (its
  validation/error-reporting walk remains, but the *key-selection* rule exists once).

Ownership difference (borrowed vs owned) is exactly why the shared core returns positions, not
values.

## Tests

- Direct `dynamic_keys` unit tests (none exist), incl. adversarial cases: a key literally named
  `"AUTH"` in the KEYS tail, empty single-key form (`""` + KEYS), AUTH2 mid-args before KEYS,
  COPY/REPLACE interleaving.
- Differential test: for arbitrary arg vectors (table-driven), keys extracted by
  `dynamic_keys(args)` == keys `MigrateArgs::parse(args)` would migrate.

## Verify

`just test frogdb-server migrate`.

## Implementation notes (2026-07-16)

- `7b71e87c` — `pub(crate) fn key_positions(args: &[Bytes]) -> Vec<usize>` lives in
  `server/src/migrate.rs` beside `MigrateArgs`; `dynamic_keys` maps positions to borrowed
  `args[i].as_ref()`, `parse` maps to owned clones **after** its strict validation walk succeeds
  (so the permissive walker never leaks lenient accept behavior into `parse` results). The
  permissive-vs-strict split is documented on the walker.
- `ac233ce8` — 10 direct `dynamic_keys` unit tests (previously zero), covering key-named-AUTH in
  the KEYS tail, empty single-key form, AUTH2 mid-args, AUTH consuming a "KEYS"-spelled token,
  COPY/REPLACE interleaving, bare 5-arg form, zero-key form, case-insensitivity, too-few args;
  plus a 13-case table-driven differential test (`dynamic_keys(args)` ==
  `MigrateArgs::parse(args).keys` on every success case).
- `just test frogdb-server migrate`: 20 passed, 0 failed.
