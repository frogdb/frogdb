# 69 — `build_typed_param` → typed param combinators

**Candidate:** SV9 · **Effort:** M · **Crates:** `frogdb-server` (unlocked), `frogdb-config` (unlocked)

**Revision:** AMEND applied. All paths, line cites and censuses re-verified at HEAD
`43720822` (the pre-revision header's `08c143d6` was stale). The builder shape is now an
explicit, justified decision (§"Which builder shape" — **shape 2, single-type variant**) rather
than an interface sketch that does not compile; every count in the draft's bool arithmetic,
`parse`/`render` census, message census and sibling table has been re-derived from the file.

## Summary

`ConfigManager::build_typed_param` is a 76-arm, 1596-line exhaustive `match` that hand-writes the
whole `CONFIG SET`/`GET` lifecycle for every mutable parameter. The lane brief's premise — "each arm
hand-builds a typed param … near-identical structure" — is **half right, and the half that is wrong
is the important half**. Per-field census over the 62 typed arms, all counts re-derived at HEAD:

| Lifecycle field | Collapsible | Stays hand-written | Verdict |
|---|---|---|---|
| `name` | 62/62 (`id.name()`) | 0 | already single-sourced — but **80 hand-copied string literals** shadow it |
| `parse` | 45/62 (35 plain-int blocks + 10 bool) | 17 | collapsible with an escape hatch |
| `validate` | 56/62 (47 `no_validate` + 9 pure-bounds) | 6 contextual | collapsible; the 9 bounds become **data** |
| `render` | 43/62 (36 `to_string` + 7 `yes_no`) | 19 | collapsible with an escape hatch |
| `default` | 62/62 | 0 | **dead field** — 62+ lines, one reader, and that reader is its own unit test |
| `propagation` | 56/62 `None` | 6 | collapsible (default-`None` builder) |
| **`get`/`apply`** | **0/62** | **62/62** | **NOT collapsible — 62 distinct addresses** |

The `get`/`apply` row deserves a sharper statement than the draft's "82% bespoke", which was
backwards. Those closures are **structurally simple**: 52 of 62 `get`s are a single expression and
48 of those are a bare read of one named cell (16 `mgr.runtime.read().unwrap().<field>`, 17 atomic
`load`s, 15 zero-argument accessors); 45 of 62 `apply`s are ≤5 lines. What makes them
non-collapsible is not complexity, it is **identity**: each names a *different* cell, and no
combinator can supply an address. So the seam the draft drew is right, but the reason is different
— and the honest reason is stronger, because it does not depend on an arm's apply being long.

The thesis is therefore *not* "declare each param as data and delete the match". Proposal 13 already
delivered the data-shaped part (metadata registry, derive macro, identity enums) and `ConfigParam`
is already a struct literal, not imperative code. What remains is a **combinator layer over the
front half of the lifecycle** — name/parse/validate/render/default/propagation — while `get`/`apply`
stay closures. A proposal that promises to table-ify `get`/`apply` would be lying about this file.

Realistic outcome: match body **1596 → ~1125 lines (≈30%)**, against **~180 new lines** of shared
machinery in `frogdb-config` — so this is a *relocation* of ~500 lines of duplicated ceremony into
one place, not a pure deletion. Plus: the dead `default:` field deleted, 80 redundant name literals
made unrepresentable, 14 `NoopParam` arms compressed 4:1 **without** touching the exhaustive-match
invariant, 9 validation bounds turned into machine-readable data, and one LIVE wire-behavior defect
fixed.

**LIVE defect found:** two boolean parameters (`latency-tracking`, `key-memory-histograms`) reject
`on`/`off`, which the **other 8** boolean parameters accept — because their parse grammar is
hand-inlined instead of calling the shared `parse_yes_no` helper. There are **10** bool params in
total, not 20; the draft's "17 others / 20 booleans" double-counted parse sites as parameters.
Second, cosmetic-but-wire-visible: **≥19** distinct error message texts inside the match plus **3
more** in the shared helpers, for what is mostly one integer parse and one percentage bound.

## Files involved

All paths and line counts verified at HEAD (`43720822`); line counts are whole-file.

| File | Lines | Role in this proposal |
|---|---|---|
| `frogdb-server/crates/server/src/runtime_config.rs` | 5905 | **Primary.** `build_typed_param` :1717–3315 (match body :1719–3314). Also `Param = Box<dyn TomlRenderable>` :45, `TomlRenderable` :162–166 and its `ConfigParam` blanket impl :168–175, the shared helpers :585–666, `NoopParam` :697–733, `typed_param` :3318, `get` :3359, `set` :3388, `config_updates` :1278, and the in-file `#[cfg(test)] mod tests` from :3764. |
| `frogdb-server/crates/config/src/param.rs` | 282 | Owns `ConfigParam<T, C>` :85–115, `DynParam<C>` :122–140, the blanket impl :142–161, `no_validate` :165–167, `ConfigError` :28–41 (`Display` :43–57). **The combinator builder lands here.** Dead `default` field at :99; its only reader is `default_is_callable` :257–261. |
| `frogdb-server/crates/config/src/param_id.rs` | 368 | `param_id_enum!` :44–70; `MutableParamId` (76 variants) :81–196; `ImmutableParamId` (47) :205–276; count-stability tests :279–368 (:355 pins 76, :366 pins 47). Read-only, except one doc hotfix (H4). Same crate as `param.rs`, so the builder can take a `MutableParamId` directly. |
| `frogdb-server/crates/config/src/params.rs` | 1670 | `ConfigParamInfo` :31–46, `VIRTUAL_PARAMS` :56+, `GOLDEN_SNAPSHOT` :552+ (**123 rows**, pinned at :1467). **Untouched** — see "Golden interaction". |
| `frogdb-server/crates/config-derive/src/lib.rs` | 443 | `#[derive(ConfigParams)]`. Untouched. |
| `frogdb-server/ops/docs-gen/src/main.rs` | 771 | Only external consumer of the param table (`build_param_lookup` :378–388, `extract_fields` :644–694). Beneficiary, not editor — see "Leverage". |

## Problem

### P1 — The match is 1596 lines and its regularity is unevenly distributed

`build_typed_params` :1702–1707 maps `MutableParamId::ALL` through `build_typed_param`, whose match
body spans :1719–3314 — **1596 lines, 76 arms** (62 `ConfigParam`, 14 `NoopParam`). Arm length ranges
4 → 50 lines. Type histogram across the 62 typed arms (re-derived at HEAD):

```
u64 23 · bool 10 · String 7 · usize 5 · u8 3 · u32 3 · f64 3 · OptionalPathValue 3
EvictionPolicy 1 · MinReplicasMaxLagSecs 1 · i64 1 · Vec<f64> 1 · Vec<String> 1
```

Per-field regularity (mechanical census, not judgement):

```
parse    35 plain `s.parse::<T>()` blocks · 7 parse_yes_no · 3 inlined bool · 17 other
validate 47 no_validate · 9 pure-bounds (5 "> 0", 2 percent, 1 range 0.0–1.0, 1 range 1..=MAX) · 6 contextual
render   36 `v.to_string()` · 7 `yes_no(*v)` · 19 other
default  62 (dead)                     propagation  56 None · 6 non-None
get      16 lock-field reads · 17 atomic loads · 15 accessors · 4 one-expr transforms · 10 multi-line
apply    62 blocks, 358 lines total; 23 are the minimal 4-line form, 45 are ≤5 lines, 17 are ≥7
```

The dominant arm shape is therefore: **fully boilerplate front half, one-line `get`, two-line
`apply`.** That is the exact shape a front-half combinator collapses and a whole-lifecycle table does
not — because the front half is *the same everywhere* and the back half is *different everywhere*,
even when it is short.

A fully-regular arm is 18 lines of which ~13 are ceremony (`Maxmemory` :1721–1738):

```rust
Maxmemory => Box::new(ConfigParam::<u64, ConfigManager> {
    name: id.name(),
    parse: |s| {
        s.parse::<u64>().map_err(|_| ConfigError::InvalidValue {
            param: "maxmemory".to_string(),
            message: "must be a non-negative integer".to_string(),
        })
    },
    validate: ConfigParam::no_validate,
    default: || 0,
    get: |mgr| mgr.runtime.read().unwrap().maxmemory,
    apply: |mgr, v| {
        mgr.runtime.write().unwrap().maxmemory = v;
        Ok(())
    },
    render: |v| v.to_string(),
    propagation: Propagation::Eviction,
}),
```

The only two lines carrying param-specific knowledge that a combinator cannot supply are `get` and
`apply`. Everything else is derivable from `(id, u64, Propagation::Eviction)`.

### P2 — 80 hand-copied wire-name literals shadow `name: id.name()`

The doc comment at :1711–1716 says "The wire name comes from `id.name()`, pinning each literal to its
identity." It does — for the `name` field. But every error path inside the same arm re-types the
name as a raw string. A census matching every string literal in the match body against the set of
names `MutableParamId::name()` returns: **80 occurrences across 62 distinct names; every typed arm
has at least one.** `BatchSizeThresholdKb` :3034–3076 spells `"batch-size-threshold-kb"` three times
in the body of an arm whose first line is `name: id.name()`; `tls-handshake-timeout-ms` and
`tls-cluster-migration` do the same.

**Ruling: LATENT, zero drift today.** No arm carries a foreign param name. But nothing prevents it: a
rename touches `param_id.rs` and the compiler stays silent about the 80 copies, so `CONFIG SET
new-name bad` would report `ERR ... old-name ...`. `ConfigError`'s `Display` is documented as
wire-visible protocol surface (`param.rs:25–27`), so this is a protocol correctness hazard, not a
style nit.

Note the shape of the workaround already in the tree: `parse_yes_no(param, s)` (:590),
`validate_percent(param, v)` (:603) and `parse_percent_f64(param, s)` (:616) all take the wire name
as a **leading argument**, and their nine call sites (:2739, :2781, :2795, :2843, :2862, :2971,
:3020, :3139, :3302) each pass a hand-copied literal — nine of the 80. The file has already
discovered that the name has to be threaded; it just threads it by hand. See §"Which builder shape".

### P3 — `default: fn() -> T` is dead weight (62 arms)

`param.rs:99` declares it with the aspiration (`:97–98`) that it be "the same fn serde's
`#[serde(default)]` uses, so the file default and the CONFIG default cannot diverge." Nothing
enforces that and nothing reads it: `DynParam` (`:122–140`) exposes no `default` accessor, so the
blanket impl `:142–161` cannot forward it and no caller in `runtime_config.rs` reads `.default`. Its
only reader in the workspace is `param.rs:257–261`:

```rust
fn default_is_callable() {
    assert_eq!((p.default)(), 5);
}
```

An interface whose sole consumer is a test asserting the interface exists fails the deletion test.
All 62 arms pay for it (57 as a single `default: || …` line, 5 as a multi-line expression).
**Ruling: LATENT (dead code, no behavior).**

### P4 — LIVE: three bool arms hand-inline the boolean grammar, and two of them get it wrong

There are **10** `ConfigParam::<bool, …>` arms (:1897, :2337, :2447, :2737, :2779, :2793, :2969,
:3018, :3137, :3300). `parse_yes_no` (:590–599) is the shared grammar and accepts `yes|true|1|on` /
`no|false|0|off` with message `"must be yes/no"`. **Seven** arms call it; **three** inline it:

| Arm | Line | Accepts true | Accepts false | Message |
|---|---|---|---|---|
| `parse_yes_no` (helper) | 590 | `yes true 1 on` | `no false 0 off` | `must be yes/no` |
| `PerRequestSpans` | 1899 | `yes true 1 on` | `no false 0 off` | `must be yes/no` |
| `LatencyTracking` | 2339 | `yes 1 true` | `no 0 false` | **`must be yes or no`** |
| `KeyMemoryHistograms` | 2449 | `yes 1 true` | `no 0 false` | **`must be yes or no`** |

`PerRequestSpans` is a byte-exact clone — pure duplication. The other two are a **real divergence**:
`CONFIG SET latency-tracking on` and `CONFIG SET key-memory-histograms off` fail with
`ERR ... must be yes or no` while the same spelling succeeds on the other **8** booleans. The same
three arms also inline the render (`:1915`, `:2354`, `:2481`) as a 7-line `if *v { "yes" } else
{ "no" }` that is behaviorally identical to the one-line `yes_no` helper (:585–588), which the other
7 use as `render: |v| yes_no(*v)` — that part is latent duplication only.

**Ruling: LIVE, minor.** A client that speaks Redis's tolerant boolean spellings gets inconsistent
answers across parameters. **The fix direction is settled in this revision** (the draft left it
open): extend `on`/`off` to all 10 by pointing the three inlined arms at `parse_yes_no`. Real Redis
rejects `on`/`off` for `CONFIG SET` booleans, so Redis-strictness is defensible in the abstract —
but adopting it here means *removing* accepted spellings from 8 parameters, which can only turn
working `CONFIG SET`s into errors. Extending to 2 parameters can only turn errors into successes.
Under FrogDB's "deviations should be improvements" rule the asymmetric-risk direction wins.

### P5 — LIVE (cosmetic, wire-visible): ≥19 message dialects in the match, +3 in the helpers

Message-text census over the match body (same-line `message:` literals and `format!` templates):

```
26  must be a non-negative integer            2  must be an integer 1-100
 5  must be > 0                               2  must be yes or no
 3  must be a positive integer                1  must be yes/no
 1  must be an integer                        1  must be an integer 0-255
 1  must be a non-negative integer (seconds)  1  ... (milliseconds)
 1  must be between 0.0 and 1.0               1  must be a number between 0.0 and 1.0
 1  invalid flag characters                   1  requires tls-cluster to be enabled
 1  can't enable key-memory-histograms at runtime
 1  '{}' is not a valid percentile            1  '{}' is not between 0 and 100
 1  must be 0 (disabled), a byte value (e.g. 100mb), or a percentage (e.g. 5%)
```

That is 18 distinct same-line texts; the multi-line `format!("must be <= {} KiB", …)` at :3056 makes
**19**. A match-scoped census misses three more that are equally wire-visible because they live one
layer down in the shared helpers:

- `validate_percent` :607 — `"must be between 1 and 100"`
- `parse_percent_f64` :619 — `"must be a number between 0 and 100"`, :625 — `"must be between 0 and 100"`

The percent case is the sharpest instance in the file, because the *same parameter* answers with two
different texts depending on which stage rejects: `CONFIG SET status-memory-warning-percent abc` →
`must be an integer 1-100` (parse, :2838) but `CONFIG SET status-memory-warning-percent 0` →
`must be between 1 and 100` (validate, :607). One bound, two spellings, split across two files'
worth of indirection. `parse_percent_f64` does the same to itself within one helper.

`"must be a non-negative integer"` / `"must be a positive integer"` / `"must be an integer"` are
attached to identically-unsigned parses; the distinction is accidental, not semantic. **Ruling: LIVE,
cosmetic** — clients see arbitrary variation in a documented protocol surface. R1 settles what to do.

### P6 — 14 `NoopParam` arms are 56 lines of pure data

`:2493–2548` (the draft's `-2552` was wrong; the last arm, `ZsetMaxListpackValue`, closes at :2548),
four lines each, two fields, no logic:

```rust
Save => Box::new(NoopParam { name: id.name(), value: "" }),
SetMaxIntsetEntries => Box::new(NoopParam { name: id.name(), value: "512" }),
```

This is the one part of the file that genuinely *is* a table pretending to be code. **Ruling:
LATENT** (correct today). See H3 for why the obvious compression is the wrong one.

### P7 — `typed_param` is a linear scan (minor, latent)

`typed_param` :3318 linearly scans the 76 boxed params by name; `ConfigManager::get` :3359 walks the
123-row metadata registry calling it, so `CONFIG GET *` is O(76 × 123). Irrelevant at this size;
worth noting only because the combinator work naturally produces a name→index map. **Not a
justification for the proposal** — listed so a reviewer does not mistake it for one.

## Proposed change

### Which builder shape — the interface constraint that decides it

**The obvious builder does not compile, and the reason is documented in the tree.**
`ConfigParam::parse` is `fn(&str) -> Result<T, ConfigError>` (`param.rs:91`) and `validate` is
`fn(&T, &C) -> Result<(), ConfigError>` (`:95`) — **bare function pointers**, deliberately, so the
struct stays "cheap, `Send + Sync`, and free of captured state" (`:83–84`). Only a *non-capturing*
closure coerces to a `fn` pointer. So `int_param(id)` cannot synthesize a `parse` that knows `id`'s
wire name, and `.min(lo)` cannot synthesize a `validate` that knows `lo`: both are runtime arguments
to the builder, and a `fn` pointer has nowhere to put them.

The constraint is already written down at `runtime_config.rs:694–696`, inside the `NoopParam` doc:
"the reported value is per-instance data, so this is a small dedicated `DynParam` impl rather than a
literal with function pointers (which cannot capture the value)". H3 deletes those arms; it must not
delete that lesson. And the tree already contains the workaround at three call sites —
`parse_yes_no(param, s)`, `validate_percent(param, v)`, `parse_percent_f64(param, s)` all take the
wire name as a leading argument for exactly this reason (P2).

Three shapes resolve it. The draft's interface sketch was none of them, so it is replaced:

| Shape | How the per-param value crosses the `fn`-pointer barrier | Cost | Gives bounds-as-data? |
|---|---|---|---|
| **(1) declarative macro** — `param!(Maxmemory, u64, propagates = Eviction, get = …, apply = …)` | It doesn't; the macro pastes the literal at each expansion, so every captured value is a *token*, capture-free by construction | Cheapest. No interface change at all | **No.** A bound written in an expanded closure body is as unreadable as today |
| **(2) data-shaped param** — the builder produces a param carrying `name`/`bounds` as **fields**, and the `fn` pointers take them as *arguments* | The value rides in the struct; `DynParam::set` passes `self.name` / `&self.bounds` into the `fn` when it calls it | Deeper: `ConfigParam`'s field signatures change | **Yes** — this is the only shape that does |
| **(3) thread the name as an argument** — `parse: fn(&'static str, &str) -> …`, like `parse_yes_no` | Same trick, name only | Small interface change | **No.** Fixes P2, leaves the 9 bounds as closure bodies |

**Decision: shape (2)** — and shape (2) *subsumes* shape (3), because threading the name is exactly
how a data-shaped `name` field reaches the parse function. Shape (1) is rejected on the leverage
argument below: a macro delivers P2 (name single-sourcing) at the lowest cost and delivers nothing
else, and the docs-gen leverage — the one benefit of this proposal that outlives the line count — is
precisely bounds-as-data.

**Sub-decision: single-type, not sibling-type.** The review's shape (2) envisioned the builder
returning *its own* `DynParam` impl beside `ConfigParam`, on the `NoopParam` precedent. That is
rejected in favour of evolving `ConfigParam` itself into the data-shaped param, for three reasons
that are all about not creating the duplication this round exists to remove:

1. **`DynParam::set` is the ordered-lifecycle invariant** (`param.rs:151–156`: "The whole set
   lifecycle, in one ordered place: parse → validate → apply"). A sibling type means a second copy
   of that ordering, in a second impl, drifting independently. One ordering, two implementations, is
   the defect class — not the fix.
2. **`TomlRenderable` would need a third impl.** `Param = Box<dyn TomlRenderable>`
   (`runtime_config.rs:45`), `TomlRenderable: DynParam<ConfigManager>` (`:162`), blanket impl for
   `ConfigParam<T, ConfigManager>` (`:168–175`), hand-written impl for `NoopParam` (`:724–733`). A
   sibling adds a fourth trait surface for a trait whose whole body is one line.
3. **A sibling type cannot make P2 unrepresentable.** With both constructors available, 62 arms stay
   free to use the un-threaded literal and forget. Evolving the one type — and, at stage 2, sealing
   its fields so the builder is the *only* constructor — turns "no arm carries a foreign name" from a
   census result into a compile-time fact. That is the difference between fixing 80 copies and
   fixing the class.

The `NoopParam` precedent still applies, correctly read: it argues that per-instance data belongs in
**fields**, not that it belongs in a **second type**. `NoopParam` stays as-is — its lifecycle
genuinely differs (`set` ignores its input, `is_noop()` returns `true`).

### The module and its interface

The builder lands **inside `frogdb-config`'s existing `param` module** — `param.rs` already owns the
`ConfigParam` concept and `param_id.rs` is in the same crate, so `int_param(id: MutableParamId)`
needs no new dependency and this is **depth added to an existing module**, not a new one. ADR-0001
crate-lightness holds: the builder is generic code over `fn` pointers and adds no dependency.

```rust
// frogdb-config/src/param.rs

/// A declared bound, as *data*. The whole point of the exercise: a bound the
/// builder was told is a value anyone can read back, not a comparison buried in
/// a closure body only rustc can see.
pub struct Bounds<T> { pub min: Option<T>, pub max: Option<T> }

pub struct ConfigParam<T: 'static, C: ?Sized> {
    name: &'static str,
    /// Takes the wire name so the rejection can quote it — the generalisation of
    /// `parse_yes_no(param, s)`, which invented this signature locally.
    parse: fn(&'static str, &str) -> Result<T, ConfigError>,
    bounds: Bounds<T>,
    /// Enforces `bounds` against a parsed value. Set to the monomorphised
    /// `check_ord_bounds::<T>` by the numeric constructors (`T: PartialOrd +
    /// Display`) and to `no_bounds::<T>` by the rest. Both are generic *fn items*,
    /// non-capturing, so both coerce — the bound value rides in the struct.
    check_bounds: fn(&T, &Bounds<T>, &'static str) -> Result<(), ConfigError>,
    validate: fn(&T, &C) -> Result<(), ConfigError>,
    get: fn(&C) -> T,
    apply: fn(&C, T) -> Result<(), ConfigError>,
    render: fn(&T) -> String,
    propagation: Propagation,
}
```

`DynParam::set` becomes **parse → bounds → validate → apply**, still one ordered place, still one
impl. `DynParam` gains one defaulted method so the bounds cross the `dyn` boundary — the same move,
for the same reason, as `is_noop()` (`param.rs:131–139`: partition the registry "without downcasting
through the `dyn` boundary"):

```rust
/// Declared bounds, rendered through this parameter's own `render`. `None` when
/// the parameter declares none. Strings, not `T`, for the same reason `get`
/// returns a `String`: `T` is not object-safe.
fn bounds(&self) -> Option<(Option<String>, Option<String>)> { None }
```

Builder surface:

```rust
pub fn int_param<T, C>(id: MutableParamId) -> ParamBuilder<T, C>
    where T: FromStr + Display + PartialOrd + 'static;
pub fn float_param<C>(id: MutableParamId) -> ParamBuilder<f64, C>;
pub fn bool_param<C>(id: MutableParamId) -> ParamBuilder<bool, C>;
pub fn string_param<C>(id: MutableParamId) -> ParamBuilder<String, C>;
pub fn custom_param<T, C>(id: MutableParamId,
                          parse: fn(&'static str, &str) -> Result<T, ConfigError>)
    -> ParamBuilder<T, C>;

impl<T, C> ParamBuilder<T, C> {
    pub fn min(self, lo: T) -> Self;          // the 5 `if *v == 0` validates
    pub fn max(self, hi: T) -> Self;
    pub fn range(self, lo: T, hi: T) -> Self; // the percent + 0.0..=1.0 validates
    pub fn parse_with(self, f: fn(&'static str, &str) -> Result<T, ConfigError>) -> Self;
    pub fn validate_with(self, f: fn(&T, &C) -> Result<(), ConfigError>) -> Self;
    pub fn render_with(self, f: fn(&T) -> String) -> Self;
    pub fn propagates(self, p: Propagation) -> Self;   // default None covers 56/62
    pub fn cell(self, get: fn(&C) -> T,
                apply: fn(&C, T) -> Result<(), ConfigError>) -> ConfigParam<T, C>;
}
```

Two properties of this interface are deliberate and worth stating, because they are what the draft
got wrong:

- **The escape hatches inherit the constraint.** `.parse_with` takes the *name-threaded* signature,
  not `fn(&str) -> …`. The escape hatch can be any pure function of `(name, input)`; it still cannot
  close over per-param values. Anything that needs per-param data must become a builder *field* —
  which is the right pressure, and is how `bounds` came to exist.
- **`.cell(get, apply)` is the terminal combinator and always takes both explicitly.** There is no
  `.runtime_field(…)` shorthand even though it would fit the 16 lock-field reads, because the other
  46 would then read as exceptions to a rule holding for a quarter of the file. **The seam this
  proposal draws is: name/parse/bounds/validate/render/propagation are declarative; get/apply are
  code.**

`Maxmemory` becomes (shown post-63-hotfix, see R3):

```rust
Maxmemory => Box::new(
    int_param::<u64, _>(id)
        .propagates(Propagation::Eviction)
        .cell(
            |mgr| mgr.runtime.read().unwrap().maxmemory,
            |mgr, v| {
                mgr.runtime.write().unwrap().maxmemory = v;
                mgr.maxmemory_flag.store(v, Ordering::Relaxed);
                Ok(())
            },
        ),
),
```

and `MaxmemorySamples` (:1772–1781 today) turns a 10-line validate into `.min(1)` — one line, and a
bound a machine can read.

### Where the ~30% goes

| Deleted | count × lines | total |
|---|---|---|
| `name: id.name(),` (typed arms) | 62 × 1 | 62 |
| `default: …` | 62 × ≥1 | ≥62 |
| `validate: ConfigParam::no_validate,` | 47 × 1 | 47 |
| `propagation: Propagation::None,` | 56 × 1 | 56 |
| `render: v.to_string()` / `yes_no(*v)` | 43 × 1 | 43 |
| plain-int `parse` blocks | 35 × 6 | 210 |
| `parse_yes_no` one-liners | 7 × 1 | 7 |
| inlined bool parse + render (P4) | 3 × 15 | 45 |
| **added** `.cell( … )` wrapper | 62 × ~2 | **−124** |
| **subtotal, typed arms** | | **≈ −408** |
| `NoopParam` arms 4-line → 1-line (H3) | | −39 |
| **match body** | 1596 → **~1125** | **≈ −30%** |

Against that: **~180 new lines** in `param.rs` (builder, `Bounds`, `check_ord_bounds`, the typed
`parse` helpers, `DynParam::bounds`) plus their unit tests. The workspace nets roughly −290 lines.
The draft's "34%" counted the deletions and not the `.cell(` wrapper.

### Locality and leverage

**Locality.** Today, "what spellings does a boolean parameter accept?" is answered in 4 places
(`parse_yes_no` plus 3 inlined copies) with 2 message texts; after, one function, one text. "What
error text does an integer parse produce?" — 35 blocks and ≥5 texts today, one after. "What is this
parameter's legal range?" — 9 closure bodies and 5 texts today, one `Bounds<T>` each after. The
per-arm text that genuinely differs (`"must be an integer 0-255"`, the maxmemory-clients grammar)
survives via `.parse_with`, co-located with the thing that is actually special.

**Leverage — the docs-gen argument, and the reason shape (1) loses.** `docs-gen`
(`ops/docs-gen/src/main.rs`) is the only external consumer of the param table.
`build_param_lookup` :378–388 keys on `(section, field)`; `extract_fields` :644–694 emits
`config_param` and `mutable` flags; defaults come from `serde_json::to_value(Config::default())` and
enum value sets from `schema_for!(Config)`. **Bounds and legal-value sets living in
`validate`/`parse` are invisible to it** — the published config reference cannot say "1–100" for
`status.memory-warning-percent` or "≤ MAX_BATCH_SIZE_THRESHOLD_KB" for `batch-size-threshold-kb`,
because those facts exist only as closure bodies. Shape (2) makes them fields and `DynParam::bounds()`
makes them readable through the trait object, so a follow-up can surface them; today no follow-up is
possible, and under shape (1) it still would not be. That is the leverage: **9 closures become 9
machine-readable bounds.** **This proposal does not change docs-gen** — it makes the change possible,
and says so rather than claiming the benefit now.

`frogctl` and `frogdb-operator` are **not** consumers of the param table (the operator imports the
serde section structs from `frogdb-config`, which this proposal does not touch).

### Deletion test

| Thing | Verdict |
|---|---|
| `ConfigParam::default` field (`param.rs:99`) + 62 arm lines + `default_is_callable` (`:257–261`) | **Delete.** Zero production readers; the sole test asserts the field's own existence. If the file-default/CONFIG-default parity the doc aspires to is wanted, it must be a real cross-check against serde defaults — a separate proposal, not a dead field. |
| 3 inlined bool `parse` bodies (`:1899`, `:2339`, `:2449`) | **Delete**, replace with `bool_param`. Fixes P4. |
| 3 inlined bool `render` bodies (`:1915`, `:2354`, `:2481`) | **Delete**, replace with `yes_no`. |
| 14 `NoopParam` arms (`:2493–2548`) | **Compress 4:1** via a 3-line `noop(id, value)` helper — **not** a catch-all arm. See H3 and R6. |
| 80 in-arm wire-name literals | **Delete.** The builder threads `id.name()` into every error it constructs; after the stage-2 seal they are unrepresentable. |
| `ConfigParam` **public struct literal** | **Delete at stage 2** — this reverses the draft, which kept it as the escape hatch. With `.parse_with`/`.validate_with`/`.render_with`/`.cell` the builder is *total*: every field has an override. The literal therefore adds exactly one capability — the ability to forget name-threading — which is P2. Sealing the fields (drop `pub`, builder becomes sole constructor) is what makes the seam a compile-time fact instead of a convention. |
| `check_bounds` fn field | **Keep, and own it as machinery.** It exists because `Bounds<T>` needs a comparator that not every `T` has (`String`, `Vec<f64>`, `OptionalPathValue`). The alternative — splitting `ConfigParam` per type family — duplicates the whole lifecycle five ways to save one field. Declined. |
| `NoopParam` and its `TomlRenderable` impl (`:697–733`) | **Keep** — documented as never invoked because no-op rows are `section: None`, but it is a trait-completeness obligation, not a dead branch. |

### Golden interaction — no interaction

Duty 2, answered definitively: **the golden snapshot is metadata-only and this refactor cannot move
it.** `GOLDEN_SNAPSHOT` (`params.rs:552+`) pins rows of `ConfigParamInfo` (`:31–46`) — name, section,
field, mutability flags — produced by `#[derive(ConfigParams)]` plus `VIRTUAL_PARAMS`, all inside
`frogdb-config` and all sourced from the serde structs. It contains **123 rows**
(`params.rs:1467`; the "118" in the round ledger is stale — 118 was the mutability round's count).
`build_typed_param` consumes `MutableParamId`; it does not feed the registry. Since the combinators
change neither the identity enums nor the derive nor `VIRTUAL_PARAMS`, `GOLDEN_SNAPSHOT` is
byte-identical after this work, and so are `mutable_ids_match_registry_partition` and
`id_counts_are_stable` (76 at `param_id.rs:355`, 47 at `:366`).

What **is** client-visible and must stay byte-identical is different and narrower:

1. **`CONFIG GET` render output** for all 76 params (`render` → `DynParam::get`).
2. **`ConfigError` Display text** on every rejected `CONFIG SET` — explicitly documented as protocol
   surface at `param.rs:25–27`.
3. **`CONFIG REWRITE` TOML output** (`config_updates` :1278 via `TomlRenderable` :162–175).

(1) and (3) are safe by construction — the builder's default render is `|v| v.to_string()` and
`yes_no`, which is what those 43 arms already do, and `TomlRenderable`'s blanket impl goes through
the typed value, which is unchanged. (2) is the real constraint and is handled in Risks.

## Testability improvement

Today a bug in one arm's parse grammar is invisible unless someone wrote a test for *that parameter*
— which is exactly how P4 survived. The in-file tests (`:3764+`) cannot cover 62 arms × 6 fields.

1. **Grammar tests move from per-param to per-combinator.** One table test over `bool_param` covering
   `yes/true/1/on/no/false/0/off/garbage` proves the grammar for all **10** booleans at once, and an
   eleventh boolean added later inherits the proof. Same for `int_param` (the 35 plain-int arms) and
   `.min`/`.range` (the 9 bounded arms).
2. **A drift test becomes possible.** With the builder threading `id.name()` into every error, a loop
   over `MutableParamId::ALL` can assert that a deliberately-invalid `CONFIG SET <name>` reports
   `<name>` — P2's 80 literals become unrepresentable rather than merely undrifted. That test cannot
   be written today without 62 hand-written cases.
3. **A render round-trip becomes cheap.** `for id in ALL: set(get(id))` must be accepted and produce
   the same rendered string. The mutability round already built a REWRITE round-trip suite
   (SET → REWRITE → reparse → validate); this is its `CONFIG GET` counterpart, and the combinators make
   it a loop instead of a table of literals.
4. **Bound parity gets a hook — and it is now assertable, not just inspectable.** The 9 bounds become
   `Bounds<T>` fields readable through `DynParam::bounds()`, so a test can assert they match the
   boot-time `Section::validate()` bounds (`memory.rs`, `status.rs`, `persistence.rs`,
   `replication.rs`). Today that parity is maintained by hand-written comments — e.g.
   `StatusMemoryWarningPercent` :2841–2842 carries "Same bound as `StatusConfig::validate`" as
   *prose*. **Ruling on that parity: LATENT, correct today** (all 9 checked against their boot
   validators, no mismatch) — but it is comment-enforced, which is the condition under which it
   eventually stops being correct. This is the item shape (1) could not deliver.
5. **One bound, one message.** Folding `validate_percent` into `.range(1, 100)` removes the P5
   two-texts-for-one-bound split, and a single test can assert that a rejected value reports the same
   text whichever stage rejects it.

Mutation-testing note: `frogdb-server` and `frogdb-config` are **not locked crates** (locked set is
txn/persistence/replication/cluster per CLAUDE.md and `adr/0002`–`0004`), so no `just mutants-gate`
threshold applies. `just lint-failure-modes` is unaffected — see R5.

## Risks / scope boundaries

### R1 — Error-message byte-parity — SETTLED as normalize, on a clean grep

P5's dialects mean a naive `int_param` unifies 35 parse messages and a naive `.range` unifies the
percent split. Two resolutions were possible; **the grep the draft deferred has been run, and it is
clean, so option (b) is settled**:

- **(a) Preserve exactly.** Per-arm `.parse_with` / message override. Zero wire change; keeps ~5 arms
  verbose; leaves P5 unfixed.
- **(b) Normalize deliberately.** Collapse the 3 integer dialects to `"must be a non-negative
  integer"`, the 2 float-range dialects to one, and the 2 percent dialects to one, as an intentional,
  documented protocol change (FrogDB is pre-production; deviations that are improvements are
  allowed).

Evidence for (b): a workspace grep for each text finds **no test anywhere that asserts a CONFIG SET
rejection message**. `"Invalid value for"` appears in **zero** `.rs`/`.tcl`/`.py` files outside
`runtime_config.rs` and `param.rs`. The three integer texts appear only in unrelated contexts
(`debug_conn_command.rs:216`, `:429`, `:859`; `commands/replication.rs:517`; `acl/src/parser.rs:450`,
`:457` — all different commands with their own strings). The **sole** pin is
`param.rs:263–281 config_error_display_is_stable`, which asserts
`"ERR Invalid value for 'maxmemory': must be a non-negative integer"` — i.e. exactly the
**normalization target** — and it constructs `ConfigError` directly rather than going through the
match, so it neither blocks the collapse nor needs editing. **Ruling: option (b), no fallback needed.**

### R2 — `apply` side effects must not be disturbed

62 `apply` closures total 358 lines; 17 of them do substantially more than store a value:
late-published `OnceLock` handles with catch-up-on-publish (`set_snapshot_coordinator` :1064–1074,
`set_replication_lag_thresholds` :1099–1109, `set_backlog_ttl` :1116–1122,
`set_replication_self_fence` :1128–1138), TLS rebuild (`apply_tls` :1223–1257), cross-sibling
validation (`StatusDurabilityLagWarningMs` vs `Critical`), unit conversion (`BatchSizeThresholdKb`
:3066–3074 KiB↔bytes, `MinReplicasMaxLag` `div_ceil`). **The proposal moves none of them.**
`.cell()` takes them verbatim. Every arm's `get` and `apply` were audited for target agreement:
all 62 address the same cell, including the Redis alias pairs that intentionally share one
(`HashMaxZiplistEntries` / `HashMaxListpackEntries`, and the `*Value` pair). **Ruling: LATENT-clean,
no defect.** Also checked: `set_tls_runtime` :1181–1184 lacks the catch-up other late-publish setters
have, but `apply_tls` errors with `TLS_NOT_RUNNING` (:631–632) on a missing handle, so it is correct
by a different route. **Ruling: LATENT-clean.**

### R3 — Ordering against proposal 63's `shared_maxmemory` hotfix (mandatory)

**The ordering survives; the draft's justification for it did not, and is replaced.** The draft
called 63's hotfix "a 1–2 line behavioral fix to one closure body" that 69 "carries verbatim". Both
halves are wrong against 63 *as revised*: 63 §Hotfix explicitly withdraws the one-line version as
**not implementable** ("`ConfigManager` has no handle to `Server.shared_maxmemory` and no way to
acquire one") and replaces it with the five-edit `max_clients` pattern. Four of those five edits are
**outside** my match body:

| 63 hotfix step | model line (verified at HEAD) | inside :1719–3314? |
|---|---|---|
| 1. field `maxmemory_flag: Arc<AtomicU64>` | beside `max_clients` `:766` | no |
| 2. seed in `with_collaborators` | model `:993` | no |
| 3. `.store` into `Maxmemory`'s `apply` | model `:2331` (`Maxclients`' apply) → target `:1733` | **yes — first arm** |
| 4. vend `pub fn maxmemory_flag()` | model `max_clients_flag` `:3578` | no |
| 5. consume | `init.rs:397` (`let shared_maxmemory = Arc::new(…)`) | no (different file) |

So the true collision surface is **one arm**, and the hotfix is S-sized but not one-line.

**Refutation, with evidence:** the review's counter-claim that the hotfix "rewrites `get` **and**
`apply`" does not hold. 63 §Hotfix names it as a deliberate divergence: "`get` is not moved onto the
atomic — `Maxmemory`'s must keep reading `mgr.runtime.read().unwrap().maxmemory` (`:1731`)", because
`Propagation::Eviction` → `notify_eviction_change()` (`:3626–3628`) consumes that field. The
`max_clients` model *does* read its atomic in `get` (`:2329`, verified) — that is precisely the line
63 declines to copy.

**Ordering: 63's hotfix lands first; 69 rebases over it.** Restated justification:

1. 63 itself forbids folding the hotfix into a mechanical diff ("a behavior fix buried in a 200-line
   mechanical diff is unreviewable"). 69 *is* the 200-line mechanical diff.
2. The rebase is asymmetric. 69-over-63 means re-expressing a two-statement `apply` as the second
   argument to `.cell(…)` — line-shape only. 63-over-69 means re-deriving four out-of-range cites
   (`:766`, `:993`, `:3578`, `init.rs:397`) against a file whose middle 1600 lines just moved, for no
   benefit.
3. **69 must not convert the `Maxmemory` arm before 63's hotfix merges.** Leave it a struct literal
   in pass 1 and convert it in the sweep's final commit (which is also the commit that seals the
   fields — stage 2 cannot run while any literal remains).

### R4 — Sibling edges

| Sibling | Owns | Edge | Resolution |
|---|---|---|---|
| **63** server subsystem bundles | `server/mod.rs`, `server/init.rs`, `runtime_config.rs` (hotfix) | **Real, one arm inside my range.** Step 3 at `:1733`; the other four steps are outside. Also cites the `Propagation::Eviction` handler at `:3626–3628` (outside the match, untouched by 69) | R3 ordering |
| **41** persistence small-dedups | `wal/*` | **Read-only cite inside my range.** Cites `runtime_config.rs:3065–3070` as evidence that production drives the WAL threshold via `CONFIG SET`. Verified at HEAD: `:3065` is `BatchSizeThresholdKb`'s `get`, `:3066–3074` its `apply`. 41 edits `wal/`, not my file | Whoever lands second re-derives the number; the prose stays true |
| **66** shard worker builder | `core/src/shard/*`, `shards.rs` | **Read-only cite inside my range — recorded here, not "None" as the draft had it.** 66's coverage argument cites `runtime_config.rs:2422`, the `.store` inside `NotifyKeyspaceEvents`' `apply`, as the middle link of a four-hop chain (owner `:769`, store `:2422`, vend `:3588`, worker `worker.rs:185`). All four verified at HEAD; `:2422` is inside `:1719–3314`. 66 also cites `wal_failure_policy_flag` `:1167`, which is outside | Same treatment as 41: whoever lands second re-derives the number. 66's own boundary note ("no file overlap with any of the four") is true of its *edits* and silent about its *evidence*; this row closes that gap |
| **48** FCALL cross-shard | `subsystems.rs`, scripting | None. 63's table lists it as a `subsystems.rs:559` edge with *63*, not with 69 | None |
| **67** server small-dedups | `server/src/connection/*` | 67's own boundary table already records **"69 runtime_config.rs — None"** | None |
| **68** exec-framing | 11 files, primarily `connection/pubsub_conn_command.rs` (**primary deletion**: `exec_pubsub_in_transaction` 965–1006), `connection/transaction.rs` (primary edit), `connection/dispatch.rs`, `core/src/command_spec.rs`; read-only in `frogdb-txn` | None — no file in 68's set is `runtime_config.rs` or in `frogdb-config`. (The draft named 3 of the 11 and omitted the primary deletion) | None |
| **90** (future CT2) `CommandSpec::DEFAULT` sweep | **three crates**, not one: ~400 literals across `frogdb-commands` (296), `frogdb-server` (72), `frogdb-core` (32), per 68 §Risks. **Not on disk at this HEAD** | None, on stronger evidence than the draft's "different crate": `grep -c CommandSpec` is **0** in `runtime_config.rs` and **0** across every file in `frogdb-config/src` | Re-check once 90 is written |

### R5 — LOCKED-area contact: none, plus one pre-existing stale cite

`runtime_config.rs` is in `frogdb-server` (`crates/server/Cargo.toml:2`), which is **not** one of the
four locked areas. Grepping `.scratch/hardening/specs/*.md` and `adr/*.md` for `runtime_config.rs`
yields exactly two hits, both in `replication-failure-modes.md`, and neither is a live obligation:

- **`:1191`** cites `runtime_config.rs:2028-2046` for the GAP-4 min-replicas-max-lag rounding bug.
  **That cite is already stale at HEAD** — the min-replicas arms are elsewhere and the described
  integer arithmetic no longer exists (the `MinReplicasMaxLagSecs` newtype does `div_ceil`). It sits
  inside a closed `<details>` block preserving the original reasoning, so **it is archive text and
  should be left alone** — reported here so a reviewer does not read it as a live cite my refactor
  breaks. `just lint-failure-modes` cannot see it: the linter binds only backticked `Forced by` test
  names, never prose cites.
- **`:1201`** cites `runtime_config.rs` with no line number — unaffected by any reshaping.

The file carries 5 `// FM-*` tags (`:4474`, `:4530`, `:4548` FM-REPLICATION-046; `:4844`
FM-CLUSTER-059; `:5137` FM-PERSISTENCE-046) — **all in the test module, all far outside the match
body (:1719–3314). Untouched.** No seam lint or generator references this file by path.

### R6 — Scope boundaries the proposal will not cross

- **No `get`/`apply` table.** Stated up front so review does not push toward it.
- **No catch-all match arm, ever.** The exhaustiveness of `build_typed_param` is a documented
  compile-time partition check (`:1711–1716`: "a new mutable identity with no arm is a
  `non-exhaustive patterns` compile error … the compile-time replacement for the former runtime
  partition check"). Any `_ =>` arm converts "new `MutableParamId` fails the build" into "new
  `MutableParamId` silently serves a no-op". That trade is refused at any line count. See H3.
- **No metadata-registry work.** Proposal 13's residual gaps (per-struct coverage 13-02, the
  `#[param(skip)]` audit 13-01) stay filed; this is a different layer.
- **No immutable-param work.** `ImmutableParamId`'s 47 rows have no lifecycle closures.
- **No docs-gen change.** Enabled, not delivered.
- **No `frogctl` / operator change.** Neither consumes the param table.
- **No `VIRTUAL_PARAMS` change.** The hardcoded defaults in `ConfigManager::with_collaborators`
  (`lua_time_limit` 5000 at `:985`, listpack atomics `:986–991`, `notify_keyspace_events` `:994`,
  latency percentiles `:997`, `key_memory_histograms_state` `:998` — all verified) are virtual params
  with no serde backing **by design**, not drift. **Ruling: LATENT-clean.**

## Effort

**M**, matching the candidate estimate. Two stages, because the seal in stage 2 is only possible once
zero literals remain:

- **~0.5 day** — builder, `Bounds`, `check_ord_bounds`, typed parse helpers and `DynParam::bounds` in
  `param.rs` (~180 lines) + unit tests. Fields stay `pub`, so both construction paths compile and no
  arm is touched yet.
- **~1.5 days** — convert the 62 arms in family batches (memory → persistence → replication → TLS →
  status → cluster → hotshards), one commit per family so a regression bisects to a family.
  `Maxmemory` goes last (R3).
- **~0.5 day** — stage 2: seal `ConfigParam`'s fields; `NoopParam` compression (H3); `default` field
  deletion (H2); P4 bool fix + its table test; R1 normalization applied, including the helper
  dialects (H5).

The size risk the draft named — regression suite pinning message text — is **retired**: R1's grep is
clean, so no per-arm override budget is needed. The remaining size risk is the 17 bespoke `parse`
closures: if several need per-param data beyond the wire name, each becomes a builder field rather
than a `.parse_with`, and the builder grows. Audit those 17 first.

### Independently-landable hotfixes

Each stands alone, needs no builder, and can land before or without the refactor:

| # | Fix | Files | Ruling | Size |
|---|---|---|---|---|
| **H1** | **Boolean grammar unification.** Point `LatencyTracking` `:2339` and `KeyMemoryHistograms` `:2449` (and `PerRequestSpans` `:1899`, a byte-exact clone) at `parse_yes_no`, and their renders (`:1915`, `:2354`, `:2481`) at `yes_no`. **Policy, decided:** extend `on`/`off` to all **10** bool params — 7 accept it via the helper, `PerRequestSpans` accepts it inline, only 2 reject it; extending turns errors into successes, Redis-strictness would turn 8 parameters' working sets into errors. Add a table test asserting all 10 accept the same spellings | `runtime_config.rs` | **LIVE** | ~45 lines removed, 1 test |
| **H2** | **Delete the dead `default` field.** Remove `param.rs:99`, `default_is_callable` `:257–261`, and the 62 `default:` sites | `param.rs`, `runtime_config.rs` | LATENT (dead code) | ~−70 lines |
| **H3** | **`NoopParam` arms 4:1, exhaustiveness intact.** `:2493–2548` (the draft's `-2552` was wrong). **Not** the draft's catch-all arm and **not** a const lookup table: a catch-all destroys the `:1711–1716` compile-time partition check, and a `&[(MutableParamId, &str)]` scan needs a silent fallback for an absent id. Instead add `fn noop(id: MutableParamId, value: &'static str) -> Param { Box::new(NoopParam { name: id.name(), value }) }` and write each arm as `Save => noop(id, ""),`. 14 arms × 4 lines → 14 × 1 plus a 3-line helper; every id still named, so a new `MutableParamId` is still a compile error | `runtime_config.rs` | LATENT | −39 lines |
| **H4** | **Stale doc count.** `param_id.rs:10` says "the 45 restart-required parameters"; `id_counts_are_stable` `:366` asserts **47** | `param_id.rs` | LATENT (doc) | 1 line |
| **H5** | **Message normalization, including the helper dialects.** In the match: collapse `"must be a positive integer"` (3) and `"must be an integer"` (1) into `"must be a non-negative integer"` (26) on identically-unsigned parses. In the helpers (which a match-scoped census misses): collapse `validate_percent`'s `"must be between 1 and 100"` `:607` against the two arms' `"must be an integer 1-100"` `:2838`/`:2859`, and `parse_percent_f64`'s own two texts `:619`/`:625`. R1's grep is clean, so nothing pins any of them | `runtime_config.rs` | **LIVE** (cosmetic, wire-visible) | ~10 lines |

H1 is the one worth landing regardless of whether proposal 69 is accepted. H3 as revised is worth
landing too; H3 **as originally drafted** is not, and should be refused if it resurfaces.
