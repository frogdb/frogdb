# Proposal: Typed Store Access

Status: proposed
Date: 2026-06-12

## Problem

The seam between command implementations and the store is crossed with the raw `Value` enum. Every
command that touches a typed key must know and re-implement the same three-step protocol: read
the key, check its variant, emit `CommandError::WrongType` itself — and only then unwrap its way
to the inner value. The interface is nearly as complex as what it hides: a shallow seam where 297
command implementations each carry a copy of an invariant that belongs in one module.

Verified evidence (all paths relative to `frogdb-server/crates/commands/src/`):

| Symptom | Count | Where |
|---------|-------|-------|
| `.unwrap().as_*_mut().unwrap()` chains | 26 | `list.rs` ×11, `hash.rs` ×9, `set.rs` ×3, `stream/pending.rs` ×2, `json/mod.rs` ×1 (macro) |
| Hand-rolled `CommandError::WrongType` | 205 occurrences | 46 files |
| Commands hand-parsing args despite `ArgParser` | ~295 of 297 | `ArgParser` used only in `scan.rs`, `hash.rs` |

The unwrap chains are check-then-access: code checks `v.as_list().is_none()` at one line, then
trusts that check several lines later with `ctx.store.get_mut(key).unwrap().as_list_mut().unwrap()`
(`list.rs:160` → `list.rs:167`). The check and the access are separate store calls with arbitrary
code between them; if the invariant drifts during a refactor (early return removed, key deleted by
an extracted helper, expiry-on-read kicking in), the failure mode is a shard panic, not an error
reply. The 205 hand-rolled `WrongType` checks are the same invariant restated 205 times — zero
locality: a change to wrong-type semantics (e.g. how expired-but-present keys are treated) touches
46 files.

`json/mod.rs:88` shows the pattern has already metastasized into a macro:

```rust
$ctx.store.get_mut($key).unwrap().as_json_mut().unwrap()
```

## Current state

### The check-then-unwrap chain (LPUSHX, `list.rs:154-174`)

```rust
fn execute(&self, ctx: &mut CommandContext, args: &[Bytes]) -> Result<Response, CommandError> {
    let key = &args[0];

    // Check if key exists
    match ctx.store.get(key) {
        Some(v) => {
            if v.as_list().is_none() {
                return Err(CommandError::WrongType);
            }
        }
        None => return Ok(Response::Integer(0)),
    }

    let list = ctx.store.get_mut(key).unwrap().as_list_mut().unwrap();

    for elem in &args[1..] {
        list.push_front(elem.clone());
    }

    Ok(Response::Integer(list.len() as i64))
}
```

### Triple store lookup + unwrap chain (HDEL, `hash.rs:217-230`)

```rust
// Check if key exists
if ctx.store.get(key).is_none() {
    return Ok(Response::Integer(0));
}

// Verify type
if ctx.store.get(key).unwrap().as_hash().is_none() {
    return Err(CommandError::WrongType);
}

let (deleted, is_empty) = {
    let hash = ctx.store.get_mut(key).unwrap().as_hash_mut().unwrap();
    // ...
```

Three store lookups (two `get`, one `get_mut`) to perform one mutation, plus two unwraps standing
in for invariants enforced 10 lines earlier.

### Hand-rolled WrongType (APPEND, `string.rs:210-227`)

```rust
if let Some(existing) = ctx.store.get_mut(key) {
    if let Some(sv) = existing.as_string_mut() {
        let new_len = sv.append(value);
        Ok(Response::Integer(new_len as i64))
    } else {
        Err(CommandError::WrongType)
    }
} else {
    ctx.store.set(key.clone(), Value::string(value.clone()));
    Ok(Response::Integer(value.len() as i64))
}
```

This variant panics nowhere, but it calls `get_mut` *before* the type check. `Store::get_mut` is
copy-on-write (`frogdb-core/src/store/mod.rs:236-239`): a shared value is cloned before the
mutable reference is returned. APPEND against a wrong-typed multi-megabyte sorted set clones it
just to discover the type mismatch. The correct ordering (check on the cheap `Arc` from `get`,
then `get_mut`) is a store implementation detail that has leaked into 46 command files, and is
applied inconsistently.

### What already exists (and is itself duplicated)

- `Value` accessors `as_list`/`as_list_mut` etc., macro-generated at
  `frogdb-server/crates/types/src/types/mod.rs:73-116` for 14 type families (+ a hand-written
  `as_vectorset` at `:118`).
- `ValueType` trait (`frogdb-core/src/store/mod.rs:72`): `type_name`, `create_default`,
  `from_value`, `from_value_mut` — exactly the vocabulary a typed interface needs, already
  implemented per family.
- `get_or_create<T: ValueType>` — implemented **three times** with identical bodies:
  `CommandContextCore::get_or_create` (`frogdb-core/src/command.rs:592`),
  `CommandContext::get_or_create` (`frogdb-core/src/command.rs:940`), and a free function
  (`frogdb-core/src/command.rs:978`) — plus five thin per-family wrappers at
  `frogdb-server/crates/commands/src/utils.rs:247-296`.

So the deep half of the seam (`ValueType`, `get_or_create`) exists, but it only covers
create-if-missing. The far more common access-if-exists path has no typed interface at all, which
is why commands fall back to check-then-unwrap.

## Proposed design

Deepen the store's interface so the WrongType invariant is owned in one module. Commands stop
seeing `Value` for the common paths; they ask the store for the typed inner value and get a total
answer: present, absent, or wrong type.

### New module: `frogdb-core/src/store/typed.rs`

```rust
/// Key exists but holds a different type.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WrongTypeError;

// Local type in trait params satisfies the orphan rule; commands just use `?`.
impl From<WrongTypeError> for CommandError {
    fn from(_: WrongTypeError) -> Self {
        CommandError::WrongType
    }
}

/// Typed access to the store. Extension trait (not methods on `Store`) because the store is used
/// as `&mut dyn Store` (`CommandContextCore.store`) and generic methods would break object
/// safety. The blanket impl makes these callable on `dyn Store` directly.
pub trait StoreTypedExt: Store {
    /// Typed mutable access. `Ok(None)` = key absent, `Err` = key holds another type.
    ///
    /// Type-checks on the shared handle from `get()` *before* calling `get_mut()`, so a
    /// wrong-typed value is never copy-on-write cloned. This ordering lives here, once.
    fn get_typed_mut<T: ValueType>(&mut self, key: &[u8])
        -> Result<Option<&mut T>, WrongTypeError>
    {
        match self.get(key) {
            None => return Ok(None),
            Some(v) if T::from_value(&v).is_none() => return Err(WrongTypeError),
            Some(_) => {}
        }
        // Total even if the impossible happens between the calls — no panic path.
        match self.get_mut(key).map(T::from_value_mut) {
            Some(Some(t)) => Ok(Some(t)),
            _ => Err(WrongTypeError),
        }
    }

    /// Typed read access on the shared handle (no COW). `Deref<Target = T>`.
    fn get_typed<T: ValueType>(&mut self, key: &[u8])
        -> Result<Option<TypedArc<T>>, WrongTypeError>;

    /// Type check only — for up-front destination checks (RPOPLPUSH, SMOVE, COPY).
    fn check_typed<T: ValueType>(&mut self, key: &[u8]) -> Result<(), WrongTypeError>;

    /// Create-if-missing; absorbs the three existing `get_or_create` copies.
    /// Only calls `set()` when the key is absent, so an existing key's TTL is untouched.
    fn get_or_create_typed<T: ValueType>(&mut self, key: &Bytes)
        -> Result<&mut T, WrongTypeError>;
}

impl<S: Store + ?Sized> StoreTypedExt for S {}
```

`TypedArc<T>` wraps the `Arc<Value>` returned by `Store::get` and derefs to `&T`; it is only
constructed after a successful check, so the projection inside `Deref` is the single audited site
where the invariant is trusted.

### Per-family convenience methods

Macro-generated, mirroring `impl_value_accessors`, for the families commands actually mutate:

| Family | Mutable access | Read access | Check | Create |
|--------|----------------|-------------|-------|--------|
| list | `get_list_mut(key) -> Result<Option<&mut ListValue>, WrongTypeError>` | `get_list` | `check_list` | `get_or_create_list` |
| hash | `get_hash_mut(key) -> Result<Option<&mut HashValue>, WrongTypeError>` | `get_hash` | `check_hash` | `get_or_create_hash` |
| set | `get_set_mut(key) -> Result<Option<&mut SetValue>, WrongTypeError>` | `get_set` | `check_set` | `get_or_create_set` |
| zset | `get_zset_mut(key) -> Result<Option<&mut SortedSetValue>, WrongTypeError>` | `get_zset` | `check_zset` | `get_or_create_zset` |
| string | `get_string_mut(key) -> Result<Option<&mut StringValue>, WrongTypeError>` | `get_string` | `check_string` | `get_or_create_string` |
| stream | `get_stream_mut(key) -> Result<Option<&mut StreamValue>, WrongTypeError>` | `get_stream` | `check_stream` | `get_or_create_stream` |

The remaining families (json, bloom, hyperloglog, timeseries, cuckoo, topk, tdigest, cms,
vectorset) get the same treatment through the generic methods or the macro — `json/mod.rs:88`'s
unwrap macro is replaced outright.

### Before / after: LPUSHX

Before (`list.rs:154-174`, 20 lines, one latent panic, two store lookups for the check):

```rust
let key = &args[0];

match ctx.store.get(key) {
    Some(v) => {
        if v.as_list().is_none() {
            return Err(CommandError::WrongType);
        }
    }
    None => return Ok(Response::Integer(0)),
}

let list = ctx.store.get_mut(key).unwrap().as_list_mut().unwrap();

for elem in &args[1..] {
    list.push_front(elem.clone());
}

Ok(Response::Integer(list.len() as i64))
```

After (9 lines, no panic path, check-and-access is one call):

```rust
let key = &args[0];

let Some(list) = ctx.store.get_list_mut(key)? else {
    return Ok(Response::Integer(0));
};

for elem in &args[1..] {
    list.push_front(elem.clone());
}

Ok(Response::Integer(list.len() as i64))
```

The command now states only what is command-specific: LPUSHX returns 0 on a missing key. The
WrongType invariant, the check/COW ordering, and the panic-freedom are owned behind the seam.

### Why this is the right depth

- **Locality.** Wrong-type semantics live in `store/typed.rs` and nowhere else. The 205 scattered
  checks collapse into one implementation; future changes (e.g. type-aware expiry interactions)
  are a one-module edit instead of a 46-file sweep.
- **Leverage.** One ~150-line module (mostly macro) deletes ~400+ lines across commands and
  removes an entire panic class. Every future command gets the correct protocol for free —
  including the COW-avoidance ordering most existing commands get wrong.
- **Deletion test.** The change is a net code deletion at the call sites, and it deletes whole
  artifacts: the three `get_or_create` copies in `frogdb-core/src/command.rs` (592, 940, 978),
  the five wrappers in `commands/src/utils.rs:247-296`, and the `json/mod.rs:88` unwrap macro all
  become redundant and are removed. If the migration could not delete these, the new interface
  would be the wrong shape.
- **No new adapter layer.** This is not a wrapper around the store that commands may or may not
  use; it deepens the existing seam (`ValueType` + `Store`) that `get_or_create` already proved
  out. The raw `get`/`get_mut` remain for the genuinely polymorphic commands (TYPE, OBJECT,
  RENAME, DEBUG) — those are the only ones that should see `Value`.

## Migration plan

1. **Phase 0 — add the interface.** New `frogdb-core/src/store/typed.rs`: `WrongTypeError`,
   `StoreTypedExt`, `TypedArc`, the per-family macro, and the test matrix (below). No call sites
   change; `just check frogdb-core && just test frogdb-core`.
2. **Phase 1 — kill the unwrap chains (26 sites).** Migrate per file: `list.rs` (11), `hash.rs`
   (9), `set.rs` (3), `stream/pending.rs` (2), `json/mod.rs` macro (1). Mechanical; verify with
   `just test frogdb-commands` per file.
3. **Phase 2 — migrate hand-rolled WrongType checks per type family.** string → hash → set → list
   → `sorted_set/` → `stream/` → probabilistic types (`bloom.rs`, `hyperloglog.rs`, etc.). Each
   family is an independent PR-sized unit; the Redis-compat integration suite is the behavioral
   safety net.
4. **Phase 3 — deletion pass.** Remove `CommandContextCore::get_or_create`,
   `CommandContext::get_or_create`, the free `get_or_create` (`frogdb-core/src/command.rs:592,
   940, 978`), and `commands/src/utils.rs:247-296` wrappers, re-pointing the survivors at
   `StoreTypedExt`. FrogDB is pre-production — no deprecation shims.
5. **Gate — ban the pattern.** Add a grep gate to `just lint` and a lefthook job (alongside
   `sync-toolchain-check`):

   ```bash
   # Fails if check-then-unwrap reappears in command code.
   ! grep -rEn 'as_[a-z_]+_mut\(\)\s*\.unwrap\(\)|get_mut\([^)]*\)\s*\.unwrap\(\)' \
       frogdb-server/crates/commands/src/
   ```

   Clippy's `disallowed_methods` cannot express "this method followed by unwrap", so a grep gate
   is the honest tool. Scope it to `crates/commands` so store internals stay unconstrained.

ArgParser adoption (secondary): the same shallow-seam diagnosis applies to argument parsing —
`frogdb-server/crates/types/src/args.rs` has 20+ methods but 2 adopters out of 297 commands. That
migration is independent and larger; it should be a separate proposal rather than a rider here,
but Phase 2's per-family sweeps are a natural moment to adopt `ArgParser` opportunistically in
files already being touched.

## Testing impact

- **WrongType semantics tested once, at the interface.** A generic test matrix in
  `store/typed.rs`, instantiated per family by the macro: key absent → `Ok(None)`; right type →
  `Ok(Some)`; wrong type → `Err(WrongTypeError)`; `get_or_create_typed` on absent key creates the
  default; on wrong type errs without mutating. Today these cases can only be exercised through
  205 call sites via integration tests.
- **Panic class eliminated.** After Phase 1 there are zero `unwrap`s between a type check and the
  access in command code, and the gate keeps it that way. The interface implementation itself is
  written total (no `expect` on the second lookup), so the class is removed, not relocated.
- **COW regression test.** Wrong-typed access must not clone the value: assert via
  `Arc` sharing (`Store::get` twice, wrong-typed `get_typed_mut`, confirm the value was not
  copied). This property is untestable today because the ordering lives in 46 files.
- **TTL preservation pinned.** `get_or_create_typed` on an existing key must not touch expiry
  (it never calls `set` for present keys). One unit test pins this; today the property is
  implicit in three duplicated function bodies.
- **Existing suites unchanged.** The migration is behavior-preserving; the Redis-compat
  integration tests and Jepsen suites act as the end-to-end check that no reply changed.

## Risks / open questions

- **Borrow-checker shape.** `get_list_mut` borrows the whole store mutably, same as `get_mut`
  today — no regression, but also no help for two-key commands. RPOPLPUSH
  (`list.rs:967-1005`) and SMOVE (`set.rs:1021`) must stay two-phase: `check_list(dest)?` up
  front, `get_list_mut(source)?` to pop, drop the borrow, then `get_or_create_list(dest)?` to
  push. `check_typed` exists precisely so the up-front destination check doesn't regress into a
  raw `get` + `as_list()` reimplementation. A true two-key disjoint-borrow API is out of scope.
- **Entry-API semantics.** Some commands want richer compound operations: LPUSHX-style
  "mutate-only-if-exists" is covered, but "delete the key if the collection emptied" (e.g.
  `list.rs:998-1000` after `pop_back`) still lives in commands. A closure-based
  `with_list_mut(key, f)` that auto-deletes emptied collections would absorb that pattern too —
  deferred until the simple accessors land and we can see how many call sites want it.
- **Read path ergonomics.** `TypedArc<T>` does its projection inside `Deref` against an invariant
  established at construction. Alternative: a closure API (`read_typed(key, |v: &T| ...)`).
  Mutable accessors are Phase 1 regardless — that is where the panics are; the read-path shape
  can be settled during Phase 2.
- **`Store::get` takes `&mut self`** (lazy expiry on read), so even `check_typed` needs `&mut`.
  Fine for command execution (commands hold `&mut dyn Store`), but the typed interface cannot be
  used from shared-read contexts if those ever appear.
- **Family coverage.** `VectorSet` is boxed and its accessor is hand-written outside
  `impl_value_accessors` (`types/src/types/mod.rs:118`); confirm `ValueType` impls exist for
  every family commands mutate before Phase 2, or the macro gains escape hatches.
- **Error type home.** `WrongTypeError` in `frogdb-core::store` with `From<WrongTypeError> for
  CommandError` keeps the store seam free of command-error vocabulary. If other store-level
  typed errors appear later (e.g. encoding limits), revisit whether a small store-error enum is
  warranted.
