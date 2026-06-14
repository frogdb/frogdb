# Proposal: Probabilistic Typed Accessors

Status: proposed
Date: 2026-06-13

Direct continuation of [02-typed-store-access.md](02-typed-store-access.md) (**Implemented**:
`e792ed42`, `a1a702ca`, `760944d1`, `b8db0087`). Read that first — this proposal finishes the deep
module it introduced. Proposal 02 made `StoreTypedExt` the single owner of the `WrongType` invariant
for the **core** families (string/list/set/zset/hash/stream) by generating per-family accessors from
the `typed_family_accessors!` macro. The probabilistic/extension families were left out of the
macro and still hand-roll the invariant on every read path.

## Problem

Proposal 02 deepened the command/store seam so that one module owns the three-step protocol (read
the key, check its variant, emit `WrongType`, then project to the inner value) plus the
copy-on-write ordering it implies. But it only wired the **core** six families into the
`typed_family_accessors!` macro (`store/typed.rs:218-225`). The probabilistic/extension families —
bloom, cuckoo, topk, tdigest, cms, hyperloglog, timeseries, vectorset — never got accessors, so
each of their command read paths still restates the invariant by hand:

```rust
let tk = value.as_topk_mut().ok_or(CommandError::WrongType)?;
```

**The invariant now has two homes.** For core types it lives once, behind a deep accessor
(`ctx.store.get_list_mut(key)?`). For everything else it is copy-pasted at the call site. A change
to wrong-type semantics — how expired-but-present keys are treated, an error-message tweak, a future
type-aware-expiry interaction — is a one-module edit for half the families and a scattered sweep for
the other half. That is precisely the shallow seam proposal 02 set out to remove, left half-done.

Worse, every hand-rolled mutable site repeats the *bug* proposal 02 documented: it calls
`get_mut(key)` (copy-on-write) **before** the type check, so a wrong-typed value is cloned just to
discover the mismatch (see [Correctness flags](#correctness-flags)).

Verified evidence — hand-rolled `as_X*().ok_or(CommandError::WrongType)?` read-path chains (paths
relative to `frogdb-server/crates/commands/src/`; counts are multiline-aware, see note):

| Family | `as_X*().ok_or` sites | Files |
|--------|----------------------:|-------|
| tdigest | 13 | `tdigest.rs` |
| vectorset | 12 | 12 files: `vadd/vcard/vdim/vemb/vgetattr/vinfo/vlinks/vrandmember/vrange/vrem/vsetattr/vsim.rs` (1 each) |
| cuckoo | 9 | `cuckoo.rs` |
| timeseries | 9 | `timeseries.rs` |
| bloom | 8 | `bloom.rs` |
| topk | 6 | `topk.rs` |
| hyperloglog | 5 | `hyperloglog.rs` |
| cms | 4 | `cms.rs` |
| **Total** | **66** | **8 families** |

Verification commands:

```bash
# Multiline-aware (cuckoo and timeseries wrap `.ok_or` onto the next line):
rg -U -c "as_(bloom_filter|cuckoo_filter|topk|tdigest|cms|hyperloglog|timeseries|vectorset)(_mut)?\(\)\s*\n?\s*\.ok_or" \
   frogdb-server/crates/commands/src
```

Two measurement notes, both verified, both worth stating so the spec is honest:

- **A naive single-line grep undercounts to 57.** `cuckoo.rs` (4 of 9) and `timeseries.rs` (5 of 9)
  format the chain across two lines (`.as_cuckoo_filter_mut()` then `.ok_or(...)?`), so the
  line-based form of the grep in the [migration gate](#migration-plan) misses them. The real number
  is **66**.
- **The INDEX's "~128 remaining" is an over-estimate.** [INDEX.md](INDEX.md) (proposal 02 follow-up
  note) cites "~128 remaining `as_X().ok_or(WrongType)` read-path sites". The verified count of
  `.ok_or(WrongType)` chains across these families is **66**; counting *all* `as_X*()` projections in
  these files (including non-`ok_or` reads on already-fetched values) reaches 74. Either way, 66 is
  the number of invariant restatements this proposal deletes.

**JSON is already done — and proves the seam works for an extension family.** `json/mod.rs:58-91`
defines `get_json!` / `get_json_mut!` macros over the *generic* `StoreTypedExt::get_typed` /
`get_typed_mut` (`JsonValue` already implements `ValueType`, `core/src/store/mod.rs:203`). Proposal
02's deletion pass replaced the old `json/mod.rs:88` unwrap macro with these. JSON therefore has
**0** hand-rolled `ok_or` chains and is excluded from the table above — it is the existence proof
that a non-core family lives behind this seam cleanly. The eight families above are what remains.

## Current state

### Hand-rolled chain, read path (TOPK.QUERY, `topk.rs:248-262`)

```rust
match ctx.store.get(key) {
    Some(value) => {
        let tk = value.as_topk().ok_or(CommandError::WrongType)?;
        let results: Vec<Response> = items
            .iter()
            .map(|item| Response::Integer(if tk.query(item) { 1 } else { 0 }))
            .collect();
        Ok(Response::Array(results))
    }
    None => {
        let results: Vec<Response> = items.iter().map(|_| Response::Integer(0)).collect();
        Ok(Response::Array(results))
    }
}
```

### Hand-rolled chain, mutable path with COW-before-check (TOPK.ADD, `topk.rs:136-152`)

```rust
match ctx.store.get_mut(key) {
    Some(value) => {
        let tk = value.as_topk_mut().ok_or(CommandError::WrongType)?;
        let results: Vec<Response> = items
            .iter()
            .map(|item| match tk.add(item, 1) {
                Some(expelled) => Response::bulk(expelled),
                None => Response::Null,
            })
            .collect();
        Ok(Response::Array(results))
    }
    None => Err(CommandError::InvalidArgument {
        message: "Key does not exist".to_string(),
    }),
}
```

`get_mut` is called first; a wrong-typed value is copy-on-write cloned, then thrown away the instant
`as_topk_mut()` returns `None`. Same shape in BF.ADD (`bloom.rs:145-147`), CF.ADD
(`cuckoo.rs:158-162`), VADD (`vectorset/vadd.rs:176-177`), PFADD (`hyperloglog.rs:40-42`), and
twenty-odd others.

### The line-wrapped variant the line-based gate misses (CF.ADD, `cuckoo.rs:158-162`)

```rust
match ctx.store.get_mut(key) {
    Some(value) => {
        let cf = value
            .as_cuckoo_filter_mut()
            .ok_or(CommandError::WrongType)?;
        cf.add(item).map_err(|_| CommandError::InvalidArgument {
            message: "Filter is full".to_string(),
        })?;
    }
    None => {
        let mut cf = CuckooFilterValue::new(1024);
        // ... bespoke create-with-params ...
        ctx.store.set(key.clone(), Value::CuckooFilter(cf));
    }
}
```

### Boxed accessor (VADD, `vectorset/vadd.rs:176-177`)

```rust
if let Some(value) = ctx.store.get_mut(key) {
    let vs = value.as_vectorset_mut().ok_or(CommandError::WrongType)?;
    // ...
```

`VectorSetValue` is stored boxed — `Value::VectorSet(Box<VectorSetValue>)`
(`types/src/types/mod.rs:66`) — and its accessor is hand-written outside `impl_value_accessors!`
(`types/src/types/mod.rs:118-131`). `as_vectorset`/`as_vectorset_mut` already deref through the
`Box` and return `&VectorSetValue` / `&mut VectorSetValue`, so the boxing is invisible to a
`ValueType` impl.

### Contrast: the core-family accessor it should look like (from `store/typed.rs`)

A core command states only what is command-specific; the invariant, the COW ordering, and
panic-freedom are owned behind the seam (`store/typed.rs:114-130`, `190-216`):

```rust
// In command code, after proposal 02:
let Some(list) = ctx.store.get_list_mut(key)? else {
    return Ok(Response::Integer(0));
};
```

The eight probabilistic families have no such method to call. This proposal generates them.

## Proposed design

Extend the existing deep module to cover the remaining families. No new module, no new adapter
layer, no parallel mechanism — three concrete edits to code proposal 02 already shipped.

### 1. Split `ValueType` so create-if-missing is opt-in

`ValueType` (`core/src/store/mod.rs:77-89`) currently bundles four methods, one of which —
`create_default()` — assumes a *parameterless* default value. That holds for the core families
(`Value::list()`, `Value::hash()`, …) but **not** for the probabilistic families: every one is
created with command-supplied parameters and bespoke return semantics (verified: **zero** of these
commands call any `get_or_create` helper):

```rust
BloomFilterValue::with_options(capacity, error_rate, expansion, non_scaling) // BF.RESERVE
CuckooFilterValue::new(1024)                                                  // CF.ADD auto-create
TopKValue::new(k, width, depth, decay)                                        // TOPK.RESERVE
VectorSetValue::new(metric, quant, dim, m, ef)                               // VADD
```

`HyperLogLogValue::new()` and `TimeSeriesValue::new()` *are* parameterless, but even those commands
do not want a plain default — PFADD must return `1` on creation and seed the elements in the same
pass (`hyperloglog.rs:52-61`). So creation stays in the command; only the *access-if-exists* path
needs a seam.

Make `create_default` opt-in by lifting it into a sub-trait:

```rust
/// Type-safe projection of a `Value` to its inner type. Every family implements this.
pub trait ValueType: Sized {
    fn type_name() -> &'static str;
    fn from_value(value: &Value) -> Option<&Self>;
    fn from_value_mut(value: &mut Value) -> Option<&mut Self>;
}

/// A `ValueType` with a meaningful parameterless default, enabling create-if-missing.
pub trait DefaultValueType: ValueType {
    fn create_default() -> Value;
}
```

The seven families that have a default (the core six plus `JsonValue`) implement both; their
`create_default` bodies move verbatim from `ValueType` into `DefaultValueType`. The eight
probabilistic families implement `ValueType` only. `get_or_create_typed`'s bound changes from
`T: ValueType` to `T: DefaultValueType` (`store/typed.rs:159`) — no call-site change, because every
current caller already passes a defaulting type.

### 2. Add `ValueType` impls for the eight families

`type_name` / `from_value` / `from_value_mut`, delegating to the existing accessors — identical in
shape to the seven impls already in `core/src/store/mod.rs:95-219`. The boxed `VectorSetValue` needs
no special handling because `as_vectorset[_mut]` already deref the `Box`:

```rust
impl ValueType for TopKValue {
    fn type_name() -> &'static str { "TopK" }
    fn from_value(v: &Value) -> Option<&Self> { v.as_topk() }
    fn from_value_mut(v: &mut Value) -> Option<&mut Self> { v.as_topk_mut() }
}
// ...one each for BloomFilterValue, CuckooFilterValue, TDigestValue,
// CountMinSketchValue, HyperLogLogValue, TimeSeriesValue, VectorSetValue.
```

### 3. Teach `typed_family_accessors!` an optional create slot, then invoke it

The macro (`store/typed.rs:179-216`) currently hard-codes four accessors per family. Make the
`get_or_create` slot optional with a `$(...)?` group, so a family can request only
read/mut/check:

```rust
macro_rules! typed_family_accessors {
    ($(
        $ty:ty {
            $get:ident,
            $get_mut:ident,
            $check:ident
            $(, $get_or_create:ident )?   // omitted for no-default families
        }
    );* $(;)?) => {
        pub trait StoreTypedFamilyExt: StoreTypedExt {
            $(
                fn $get(&mut self, key: &[u8]) -> Result<Option<TypedArc<$ty>>, WrongTypeError> {
                    self.get_typed::<$ty>(key)
                }
                fn $get_mut(&mut self, key: &[u8]) -> Result<Option<&mut $ty>, WrongTypeError> {
                    self.get_typed_mut::<$ty>(key)
                }
                fn $check(&mut self, key: &[u8]) -> Result<(), WrongTypeError> {
                    self.check_typed::<$ty>(key)
                }
                $(
                    // Only emitted when a create ident is supplied; requires DefaultValueType.
                    fn $get_or_create(&mut self, key: &Bytes) -> Result<&mut $ty, WrongTypeError> {
                        self.get_or_create_typed::<$ty>(key)
                    }
                )?
            )*
        }
        impl<S: StoreTypedExt + ?Sized> StoreTypedFamilyExt for S {}
    };
}
```

The existing six-family invocation is unchanged (each keeps its fourth ident). Append the eight new
families with no create slot:

```rust
typed_family_accessors! {
    // --- core (unchanged, proposal 02) ---
    ListValue       { get_list,   get_list_mut,   check_list,   get_or_create_list };
    HashValue       { get_hash,   get_hash_mut,   check_hash,   get_or_create_hash };
    SetValue        { get_set,    get_set_mut,    check_set,    get_or_create_set };
    SortedSetValue  { get_zset,   get_zset_mut,   check_zset,   get_or_create_zset };
    StringValue     { get_string, get_string_mut, check_string, get_or_create_string };
    StreamValue     { get_stream, get_stream_mut, check_stream, get_or_create_stream };

    // --- probabilistic / extension (new; no parameterless default) ---
    BloomFilterValue   { get_bloom,      get_bloom_mut,      check_bloom };
    CuckooFilterValue  { get_cuckoo,     get_cuckoo_mut,     check_cuckoo };
    TopKValue          { get_topk,       get_topk_mut,       check_topk };
    TDigestValue       { get_tdigest,    get_tdigest_mut,    check_tdigest };
    CountMinSketchValue{ get_cms,        get_cms_mut,        check_cms };
    HyperLogLogValue   { get_hll,        get_hll_mut,        check_hll };
    TimeSeriesValue    { get_timeseries, get_timeseries_mut, check_timeseries };
    VectorSetValue     { get_vectorset,  get_vectorset_mut,  check_vectorset };
}
```

### Before / after: TOPK.ADD (`topk.rs:136-152`)

Before (17 lines, COW-before-check, two-level `match`):

```rust
match ctx.store.get_mut(key) {
    Some(value) => {
        let tk = value.as_topk_mut().ok_or(CommandError::WrongType)?;
        let results: Vec<Response> = items
            .iter()
            .map(|item| match tk.add(item, 1) {
                Some(expelled) => Response::bulk(expelled),
                None => Response::Null,
            })
            .collect();
        Ok(Response::Array(results))
    }
    None => Err(CommandError::InvalidArgument {
        message: "Key does not exist".to_string(),
    }),
}
```

After (no COW on wrong type, no hand-rolled invariant, command states only its own rule —
"TOPK.ADD on a missing key errors"):

```rust
let Some(tk) = ctx.store.get_topk_mut(key)? else {
    return Err(CommandError::InvalidArgument {
        message: "Key does not exist".to_string(),
    });
};
let results: Vec<Response> = items
    .iter()
    .map(|item| match tk.add(item, 1) {
        Some(expelled) => Response::bulk(expelled),
        None => Response::Null,
    })
    .collect();
Ok(Response::Array(results))
```

### Before / after: BF.ADD (`bloom.rs:144-157`) — the create branch stays in the command

```rust
// before
let added = match ctx.store.get_mut(key) {
    Some(value) => {
        let bf = value.as_bloom_filter_mut().ok_or(CommandError::WrongType)?;
        bf.add(item)
    }
    None => {
        let mut bf = BloomFilterValue::new(100, 0.01); // bespoke default
        let added = bf.add(item);
        ctx.store.set(key.clone(), Value::BloomFilter(bf));
        added
    }
};
```

```rust
// after — accessor owns the exists+check+COW path; bespoke create is untouched
let added = match ctx.store.get_bloom_mut(key)? {
    Some(bf) => bf.add(item),
    None => {
        let mut bf = BloomFilterValue::new(100, 0.01); // bespoke default, command's job
        let added = bf.add(item);
        ctx.store.set(key.clone(), Value::BloomFilter(bf));
        added
    }
};
```

The `WrongType` arm collapses into `?`; the COW clone on a wrong-typed key disappears; the
command-specific default (RedisBloom's capacity 100 / error 0.01) stays exactly where it belongs.

### Why this is the right depth

- **Completes the deep module — one home for the invariant.** After this change, `store/typed.rs`
  is the *only* place that knows the WrongType protocol and the COW ordering, for **all 14**
  families. Today it owns 6 and copy-paste owns 8. Closing that gap is the whole point of a deep
  module: behaviour migrates behind a fixed interface, callers shrink.
- **Locality.** A future change to wrong-type semantics (type-aware expiry, an error-detail tweak)
  becomes a one-module edit instead of touching 8 command files and 66 sites.
- **Leverage.** ~50 lines (8 trait impls + 8 macro lines + the optional-slot tweak) delete 66
  hand-rolled invariant restatements and remove ~24 needless COW clones by construction.
- **No new adapter.** This deepens the existing `ValueType` + `StoreTypedExt` seam that the core
  families and JSON already ride; the boxed `VectorSetValue` rides it without a special case.
  The genuinely polymorphic commands (TYPE, OBJECT, RENAME, DEBUG, the `*.SCANDUMP`/`*.LOADCHUNK`
  serialization paths) keep raw `get`/`get_mut` — they are the only ones that should see `Value`.
- **Deletion test.** The change is a net deletion at the call sites and it deletes a whole *concept*
  duplicate: the "second home" of the WrongType invariant. If the migration could not delete the 66
  chains, the interface would be the wrong shape.

## Migration plan

Each phase compiles and tests independently; FrogDB is pre-production, so no deprecation shims.

1. **Phase 0 — split the trait + add the interface.** Split `ValueType` /`DefaultValueType`
   (`core/src/store/mod.rs`), move the seven `create_default` bodies, retarget
   `get_or_create_typed`'s bound. Add the eight `ValueType` impls. Make the macro create-slot
   optional and append the eight families. No command changes.
   `just check frogdb-core && just test frogdb-core`. The generic WrongType matrix
   (`store/typed.rs:236-273`) gains six new instantiations (see [Testing impact](#testing-impact)).
2. **Phase 1 — migrate per family.** One family per PR-sized unit, in ascending blast radius:
   `cms` (4) → `hyperloglog` (5) → `topk` (6) → `bloom` (8) → `timeseries` (9) → `cuckoo` (9) →
   `vectorset` (12) → `tdigest` (13). Each site becomes `ctx.store.get_<fam>[_mut](key)?`; the
   bespoke create branches stay. Verify per family with the Redis-compat integration suite
   (`just test frogdb-commands <family>`), which is the behavioral safety net.
3. **Phase 2 — deletion confirmation.** After the last family, the eight files contain **zero**
   `as_<fam>*().ok_or(CommandError::WrongType)` chains. (JSON is already at zero.)
4. **Gate — extend `lint-no-typed-unwrap` to also ban the `ok_or` form.** The existing gate
   (`Justfile:617-630`, `lefthook.yml:43-46`) bans only `as_*_mut().unwrap()` /
   `get_mut(...).unwrap()`. It does **not** catch `as_X().ok_or(CommandError::WrongType)`, so it
   would not keep these migrations dead. Add a second pattern (multiline-aware, since cuckoo/
   timeseries wrap the call) to the same recipe:

   ```bash
   # Fails if a hand-rolled WrongType chain reappears in command code.
   # -U/-z so the `.ok_or` may sit on the line after the `.as_*()`.
   ! rg -Uzn 'as_[a-z_]+(_mut)?\(\)\s*\n?\s*\.ok_or\(CommandError::WrongType\)' \
       frogdb-server/crates/commands/src/
   ```

   Scope stays `crates/commands` so store internals are unconstrained; the polymorphic commands use
   raw `get`/`get_mut` and never match. Wire it into the existing `lint-no-typed-unwrap` job so
   `pre-commit` and `check-all` (`Justfile:637,640`) and the lefthook hook pick it up for free.

   This gate change should land with Phase 0 only if the core families are already `ok_or`-free; if
   not, land it in Phase 2 after both core remnants (19 `as_*_mut().ok_or` sites still exist in
   `stream/*`, see open questions) and the eight families are migrated, to avoid a red gate.

## Testing impact

- **The generic WrongType matrix extends to these families for free.** `typed_matrix::<T>`
  (`store/typed.rs:236-273`) already covers absent → `Ok(None)`, right type → `Ok(Some)`, wrong
  type → `Err(WrongTypeError)`. Adding `matrix_bloom`, `matrix_cuckoo`, `matrix_topk`,
  `matrix_tdigest`, `matrix_cms`, `matrix_hll`, `matrix_timeseries`, `matrix_vectorset` (seeded with
  a `Value::string("x")`) is one line each. Today these cases are exercised **only** through the
  integration suite per command — the unit-level invariant has no coverage for any probabilistic
  family.
- **COW-on-wrong-type regression, now generic.** `wrong_typed_access_does_not_cow`
  (`store/typed.rs:312-336`) asserts via `Arc::ptr_eq` that a wrong-typed `get_typed_mut` does not
  clone. It currently runs once (list vs hash). The same property is what the ~24 probabilistic
  COW-before-check sites violate today; after migration it holds for them by construction and the
  generic test pins it.
- **`get_or_create` matrix must skip no-default families.** The `get_or_create_typed` assertions in
  `typed_matrix` only apply to `T: DefaultValueType`; split the matrix into a base
  (`ValueType`) body and a `DefaultValueType` extension so the eight probabilistic instantiations
  compile.
- **Behaviour-preserving.** Replies do not change; the Redis-compat integration tests and Jepsen
  suites are the end-to-end check.

## Risks / open questions

- **Trait split blast radius.** Lifting `create_default` into `DefaultValueType` touches all seven
  existing `ValueType` impls and the `get_or_create_typed` bound. Mechanical, compiler-checked, and
  contained to `core/src/store/mod.rs` + `store/typed.rs:159`, but it is a real (small) refactor of
  shipped code, not purely additive. Alternative considered and rejected: keep one trait and have
  `create_default` return `Option<Value>` (`None` for no-default families) — that pushes a runtime
  `None` into a path that should be a compile-time absence, and `get_or_create` would gain a
  can't-happen error arm. The supertrait split keeps "has a default" in the type system.
- **Boxed / hand-written accessor (vectorset).** `Value::VectorSet(Box<VectorSetValue>)`
  (`types/src/types/mod.rs:66`) with hand-written `as_vectorset[_mut]` (`:118-131`). Because those
  accessors already deref the `Box`, the `ValueType` impl and the macro are uniform with the others
  — confirmed, no escape hatch needed. The only asymmetry is cosmetic (it lives outside
  `impl_value_accessors!`).
- **JSON is intentionally not added to the macro.** It already rides the generic
  `get_typed`/`get_typed_mut` via `get_json!`/`get_json_mut!` (`json/mod.rs:58-91`). Adding
  `get_json`/`get_json_mut` family wrappers and re-pointing those macros is optional sugar, not
  required for one-home-for-the-invariant; defer unless the wrappers read more cleanly at JSON call
  sites.
- **Core families still carry `ok_or` remnants.** 19 `as_(list|hash|set|sorted_set|stream|string)
  _mut().ok_or(WrongType)` sites remain (mostly `stream/basic.rs`, `stream/read.rs`,
  `stream/consumer_groups.rs`) — proposal 02's Phase 1 killed the `.unwrap()` chains but the gate
  never banned the `.ok_or` form, so these survived. They are out of scope here but mean the
  proposed gate extension (step 4) cannot be enabled until they are also migrated, or it lands red.
  Recommend folding them into Phase 1 (they already have core accessors to call).
- **`Store::get` takes `&mut self`** (lazy expiry on read), inherited from proposal 02: even
  `check_*` needs `&mut`. Fine for command execution; noted for completeness.
- **Naming.** `get_hll` vs `get_hyperloglog`, `get_cms` vs `get_count_min_sketch` — the table uses
  the short forms already used in command/accessor names (`as_hyperloglog`, `as_cms`). Bikeshed
  before Phase 0 so the gate and tests reference final names.

## Correctness flags

Two classes of `get_mut` misuse, both fixed for free by the migration. No *missing* WrongType checks
and no *wrong* defaults were found (bloom `new(100, 0.01)` and cuckoo `new(1024)` match RedisBloom
defaults).

### Class A — write-path COW-before-check (wrong type only)

The **COW-before-check** performance bug proposal 02 documents: the command calls
`ctx.store.get_mut(key)` (copy-on-write) and only *then* checks the type with
`as_X_mut().ok_or(CommandError::WrongType)?`, so a *wrong-typed* value is cloned (when the `Arc` is
shared — e.g. during an in-flight checkpoint/snapshot or replication fork) only to be discarded. The
result is still correct (WrongType is returned); the clone is wasted work. These commands genuinely
mutate, so they migrate to `get_<fam>_mut(key)?`, which checks on the shared `get()` handle first.

| Site | Command | Note |
|------|---------|------|
| `commands/src/bloom.rs:145` | BF.ADD | get_mut before `as_bloom_filter_mut().ok_or` |
| `commands/src/bloom.rs:190` | BF.MADD | get_mut before check |
| `commands/src/bloom.rs:410` | BF.INSERT | get_mut before check |
| `commands/src/cms.rs:171` | CMS.INCRBY | get_mut before `as_cms_mut().ok_or` |
| `commands/src/cuckoo.rs:158` | CF.ADD | get_mut before check (line-wrapped) |
| `commands/src/cuckoo.rs:205` | CF.ADDNX | get_mut before check |
| `commands/src/cuckoo.rs:343` | CF.INSERTNX | get_mut before check |
| `commands/src/cuckoo.rs:498` | CF.DEL | get_mut before check |
| `commands/src/hyperloglog.rs:40` | PFADD | get_mut before `as_hyperloglog_mut().ok_or` |
| `commands/src/tdigest.rs:139` | TDIGEST.ADD | get_mut before check |
| `commands/src/tdigest.rs:321` | TDIGEST.RESET | get_mut before check |
| `commands/src/timeseries.rs:409` | TS.ADD | get_mut before check (line-wrapped) |
| `commands/src/timeseries.rs:660` | TS.INCRBY / TS.DECRBY | get_mut before check (line-wrapped) |
| `commands/src/timeseries.rs:761` | TS.DEL | get_mut before check |
| `commands/src/topk.rs:136` | TOPK.ADD | get_mut before `as_topk_mut().ok_or` |
| `commands/src/topk.rs:186` | TOPK.INCRBY | get_mut before check |
| `commands/src/vectorset/vadd.rs:176` | VADD | get_mut before `as_vectorset_mut().ok_or` |
| `commands/src/vectorset/vrem.rs:32` | VREM | get_mut before check |
| `commands/src/vectorset/vsetattr.rs:41` | VSETATTR | get_mut before check |

### Class B — READONLY commands calling `get_mut` (clones on *every* call when shared) — sharper

Five t-digest **query** commands are declared `CommandFlags::READONLY` with `wal: NoOp`, take a
**mutable** handle via `get_mut`, but invoke only read methods on it (`td.quantile`, `td.cdf`,
`td.rank`, `td.revrank`, `td.trimmed_mean`). Because `get_mut` is copy-on-write, each query clones
the entire t-digest whenever the value is shared (during a checkpoint/snapshot/replication fork) —
**unconditionally, on the correct type, on the hot read path** — not just on a wrong-type mismatch.
This is strictly worse than Class A. Migrating them to the read accessor `ctx.store.get_tdigest(key)?`
(which projects the shared, non-COW `get()` handle through `TypedArc`) removes the clone entirely
*and* fixes the wrong-type case.

| Site | Command | Note |
|------|---------|------|
| `commands/src/tdigest.rs:362` | TDIGEST.QUANTILE | READONLY but `get_mut`; only calls `td.quantile()` |
| `commands/src/tdigest.rs:406` | TDIGEST.CDF | READONLY but `get_mut`; only calls `td.cdf()` |
| `commands/src/tdigest.rs:448` | TDIGEST.RANK | READONLY but `get_mut`; only calls `td.rank()` |
| `commands/src/tdigest.rs:492` | TDIGEST.REVRANK | READONLY but `get_mut`; only calls `td.revrank()` |
| `commands/src/tdigest.rs:660` | TDIGEST.TRIMMED_MEAN | READONLY but `get_mut`; only calls `td.trimmed_mean()` |

Note: the remaining read-only paths (TOPK.QUERY, VDIM/VINFO/VSIM/…) already use `get(key)` then
`as_X().ok_or` and are *not* COW bugs; they are migrated to `get_<fam>(key)?` only to retire the
duplicated invariant.
