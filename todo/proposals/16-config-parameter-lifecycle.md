# Proposal: Config Parameter Lifecycle

Status: proposed
Date: 2026-06-15

## Problem

A runtime-mutable config parameter is one concept — "maxmemory-policy is an `EvictionPolicy`, it
defaults to `noeviction`, these are its legal values, CONFIG SET applies it like *this*, and a
change must be pushed to every shard." But that one concept is implemented as ~5 separate
fragments scattered across three crates. The interface a caller must satisfy to add a parameter
(its type, default, legal values / error modes, runtime-apply behaviour, and shard propagation) is
spread across five files that share no enforced link. This is a shallow seam in the worst way: there
is no single module you can read to know everything about a parameter, and no single module you can
edit to change it — the *interface is the union of five implementations*.

The cost shows up two ways. **Adding** a parameter means editing all five seams in lockstep, and the
compiler does not tell you when you miss one (a missing setter is a silent "immutable" at runtime, a
missing registry entry hides it from `docs-gen`). **Changing** the legal-value set of a parameter
means editing the same hardcoded list in three non-canonical places, none of which is the actual
source of truth (`EvictionPolicy`). The lists have already begun to drift in tone (different error
strings) and will eventually drift in content.

### Deletion test

The single param `maxmemory-policy` cannot be deleted, moved, or retyped by touching one place.
Retyping it from `String` to the existing `EvictionPolicy` enum — the obviously-correct change —
would require editing the default fn, the registry, two validators, the setter closure, the runtime
struct field, and two `EvictionConfig` builders. A module is deep when removing or changing a
concept is a local edit. Here it is a seven-site sweep, which is the definition of a shallow,
leaky seam.

### Evidence: the five seams of one parameter (`maxmemory-policy`)

All paths under `frogdb-server/crates/`.

| Lifecycle stage | What lives here | file:line |
|-----------------|-----------------|-----------|
| **default** | `default_maxmemory_policy()` returns `"noeviction"`; wired into `Default` | `config/src/memory.rs:59-61`, `:102` |
| **registry** | `ConfigParamInfo { name: "maxmemory-policy", section, field, mutable }` | `config/src/params.rs:41-47` |
| **validate** | `MemoryConfig::validate` hardcoded legal-value list; cross-field warn | `config/src/memory.rs:117-136`; `config/src/validators/memory.rs:14-26` |
| **set** (parse+validate+apply) | inline setter closure, *second* hardcoded legal-value list | `server/src/runtime_config.rs:507-535` (list `:513-524`) |
| **build / propagate** | string re-parsed into `EvictionPolicy` for the engine; shard-propagation name list | `server/src/server/util.rs:97-115`; `server/src/runtime_config.rs:1692-1706`, `:1629-1635` |

The runtime value is even stored twice in two shapes: as a `String` in `RuntimeConfig`
(`runtime_config.rs:88`) and re-parsed to `EvictionPolicy` every time the engine needs it
(`util.rs:98-106`, `runtime_config.rs:1698-1701`). The string→enum conversion that *should* happen
once at the seam happens three times, in three files, with two different failure modes (see
[Correctness flags](#correctness-flags)).

This is not unique to `maxmemory-policy`. There are **45 mutable parameters** (`config/src/params.rs`
registry, 45 `mutable: true`; `runtime_config.rs` registry, 45 `setter: Some`), of which **14 are
Redis-compat no-ops** and **~31 carry real parse/validate/apply logic** — each spread across the
same five seams. `durability-mode` and `wal-failure-policy` each carry the same hardcoded
legal-value list in *two* places (`runtime_config.rs:650`, `:671` and `config/src/persistence.rs:163`,
`:172`); `loglevel` duplicates its list at `runtime_config.rs:624`.

## Current state

### Seam 1 — default (`config/src/memory.rs:59-61`, `:98-112`)

```rust
fn default_maxmemory_policy() -> String {
    "noeviction".to_string()
}
// ...
impl Default for MemoryConfig {
    fn default() -> Self {
        Self {
            maxmemory: default_maxmemory(),
            maxmemory_policy: default_maxmemory_policy(),
            // ...
        }
    }
}
```

The type is `String`, even though `EvictionPolicy` (with `Default = NoEviction`) already exists in
`frogdb-core` (`core/src/eviction/policy.rs:9-41`).

### Seam 2 — registry (`config/src/params.rs:41-47`)

```rust
ConfigParamInfo {
    name: "maxmemory-policy",
    section: Some("memory"),
    field: Some("maxmemory-policy"),
    mutable: true,
    noop: false,
},
```

Knows the *name* and TOML location, but nothing about type, default, legal values, or apply. It is
a separate registry from the one in `runtime_config.rs` that owns the setters — two registries that
must agree by convention.

### Seam 3 — validate (`config/src/memory.rs:116-136`)

```rust
pub fn validate(&self) -> Result<()> {
    let valid_policies = [
        "noeviction", "volatile-lru", "allkeys-lru", "volatile-lfu",
        "allkeys-lfu", "volatile-random", "allkeys-random", "volatile-ttl",
        "tiered-lru", "tiered-lfu",
    ];
    if !valid_policies.contains(&self.maxmemory_policy.to_lowercase().as_str()) {
        anyhow::bail!(
            "invalid maxmemory_policy '{}', expected one of: {}",
            self.maxmemory_policy, valid_policies.join(", ")
        );
    }
    // ...
}
```

This list is a hand-copy of `EvictionPolicy::all_names()` (`core/src/eviction/policy.rs:106-119`).

### Seam 4 — the inline CONFIG SET setter (`server/src/runtime_config.rs:507-535`)

```rust
ParamMeta {
    name: "maxmemory-policy",
    mutable: true,
    noop: false,
    getter: |mgr| mgr.runtime.read().unwrap().maxmemory_policy.clone(),
    setter: Some(|mgr, val| {
        let valid_policies = [
            "noeviction", "volatile-lru", "allkeys-lru", "volatile-lfu",
            "allkeys-lfu", "volatile-random", "allkeys-random", "volatile-ttl",
            "tiered-lru", "tiered-lfu",
        ];
        let lower = val.to_lowercase();
        if !valid_policies.contains(&lower.as_str()) {
            return Err(ConfigError::InvalidValue {
                param: "maxmemory-policy".to_string(),
                message: format!("must be one of: {}", valid_policies.join(", ")),
            });
        }
        mgr.runtime.write().unwrap().maxmemory_policy = lower;
        Ok(())
    }),
},
```

A *third* hand-copy of the legal-value list. Parse (lowercase), validate (membership), and apply
(store the `String`) are inlined here, distinct from the `MemoryConfig::validate` copy at seam 3,
with a different error message. The `ParamMeta` shape (`runtime_config.rs:212-224`) only carries a
string `getter` and a string `setter`; it has no slot for type, default, or propagation, so all
per-param knowledge collapses into an opaque closure body.

### Seam 5 — build / propagate (`server/src/server/util.rs:97-115`)

```rust
pub fn build_eviction_config(config: &MemoryConfig) -> EvictionConfig {
    let policy = config
        .maxmemory_policy
        .parse::<EvictionPolicy>()
        .unwrap_or_else(|_| {
            unreachable!(
                "Invalid eviction policy '{}' should have been caught by validation",
                config.maxmemory_policy
            )
        });
    EvictionConfig { maxmemory: config.maxmemory, policy, /* ... */ }
}
```

And again, separately, when CONFIG SET propagates to shards (`runtime_config.rs:1692-1706`):

```rust
let eviction_config = {
    let config = self.runtime.read().unwrap();
    EvictionConfig {
        maxmemory: config.maxmemory,
        policy: config
            .maxmemory_policy
            .parse::<EvictionPolicy>()
            .unwrap_or(EvictionPolicy::NoEviction),   // <-- different failure mode than util.rs
        // ...
    }
};
```

Plus the shard-propagation seam decides *which* params trigger a shard notify with yet another
hardcoded name list (`runtime_config.rs:1629-1635`):

```rust
let eviction_params = [
    "maxmemory", "maxmemory-policy", "maxmemory-samples",
    "lfu-log-factor", "lfu-decay-time",
];
let normalized = name.to_lowercase().replace('_', "-");
if eviction_params.contains(&normalized.as_str()) {
    // notify shards ...
}
```

### Adding a parameter today

To add (say) `maxmemory-eviction-tenacity`, a contributor must: add a field + `default_*` fn to
`MemoryConfig` (seam 1); add a `ConfigParamInfo` to `params.rs` (seam 2); extend `MemoryConfig::validate`
(seam 3); add a `ParamMeta` with a hand-written parse/validate/apply closure to `runtime_config.rs`
(seam 4); thread it into `build_eviction_config` and, if it needs shards, into the `eviction_params`
list and `notify_eviction_change` (seam 5). Five files, three crates, zero compiler enforcement that
they stay consistent.

## Proposed design

Make a parameter a *value*, not a scattering of fragments. One `ConfigParam<T>` owns the entire
lifecycle of one parameter — parse, validate, default, current-value, runtime-apply, render, and
shard-propagation — so that the whole interface lives in one literal, in one place.

### New module: `config/src/param.rs` (the deep module)

```rust
/// Everything a caller must know about one runtime-mutable parameter, in one place:
/// its type, default, legal values (via `parse`), error modes, how CONFIG SET applies
/// it at runtime, how CONFIG GET renders it, and whether a change propagates to shards.
pub struct ConfigParam<T: 'static> {
    /// Redis-style name, e.g. "maxmemory-policy".
    pub name: &'static str,

    /// Parse + reject in one place. The set of legal values *is* this function's domain,
    /// so there is no separate "valid list" to keep in sync.
    pub parse: fn(&str) -> Result<T, ConfigError>,

    /// Cross-field / range validation against the prospective config. `Ok(())` for the
    /// common case where `parse` already fully constrains the value.
    pub validate: fn(&T, &ConfigCtx<'_>) -> Result<(), ConfigError>,

    /// The compile-time default — the *same* fn serde's `#[serde(default = ...)]` uses,
    /// so the file default and the CONFIG default cannot diverge.
    pub default: fn() -> T,

    /// Read the live typed value back (for CONFIG GET). Must round-trip with `render`.
    pub get: fn(&ConfigCtx<'_>) -> T,

    /// Apply a validated value: write runtime state and run side effects (log reload,
    /// client eviction, atomics). Runtime-apply is part of the interface, not a caller concern.
    pub apply: fn(&ConfigCtx<'_>, T) -> Result<(), ConfigError>,

    /// Render a typed value for the wire (CONFIG GET parity, e.g. ms→seconds).
    pub render: fn(&T) -> String,

    /// Whether a successful set must propagate to shards, and how.
    pub propagation: Propagation,
}

/// Which internal subsystem a change must be pushed to after a successful set.
#[derive(Clone, Copy, PartialEq, Eq)]
pub enum Propagation {
    None,
    Eviction,             // rebuild EvictionConfig, notify all shards
    KeyMemoryHistograms,  // SetKeyMemoryHistograms to all shards
}
```

`ConfigCtx<'_>` is a thin borrow of the handles an apply/get closure needs (the `RwLock<RuntimeConfig>`,
the listpack atomics, the optional log-reload handle, the client/ACL registries) — the same fields
`ConfigManager` exposes to its setter closures today (`runtime_config.rs:238-276`). It replaces the
ad-hoc `&ConfigManager` capture so `ConfigParam` can live in the `config` crate without depending on
the `server` crate.

### Type erasure for heterogeneous `T` (object safety)

Parameters have different `T` (`u64`, `usize`, `u8`, `f64`, `String`, `EvictionPolicy`, …), so they
cannot share a `Vec<ConfigParam<T>>`. The registry stores `Box<dyn DynParam>`. This mirrors how
proposal 02 (`02-typed-store-access.md`) keeps `StoreTypedExt`'s generic methods usable through
`&mut dyn Store`: the generic core stays generic, and an object-safe trait exposes only the
non-generic, string-typed surface across the `dyn` boundary.

```rust
/// Object-safe view used by the registry and by CONFIG GET/SET. All generic work
/// (parse/validate/apply over the concrete `T`) is monomorphized inside the impl;
/// only `&str`-in / `String`-out crosses the dyn boundary.
pub trait DynParam: Send + Sync {
    fn name(&self) -> &'static str;
    fn get(&self, ctx: &ConfigCtx<'_>) -> String;
    fn set(&self, ctx: &ConfigCtx<'_>, raw: &str) -> Result<(), ConfigError>;
    fn propagation(&self) -> Propagation;
}

impl<T: 'static> DynParam for ConfigParam<T> {
    fn name(&self) -> &'static str { self.name }

    fn get(&self, ctx: &ConfigCtx<'_>) -> String {
        (self.render)(&(self.get)(ctx))
    }

    /// The whole set lifecycle, in one ordered place: parse → validate → apply.
    fn set(&self, ctx: &ConfigCtx<'_>, raw: &str) -> Result<(), ConfigError> {
        let parsed = (self.parse)(raw)?;     // parse + legal-value check, once
        (self.validate)(&parsed, ctx)?;      // cross-field / range, once
        (self.apply)(ctx, parsed)            // runtime-apply + side effects, once
    }

    fn propagation(&self) -> Propagation { self.propagation }
}
```

### Before / after: `maxmemory-policy` defined once

After, the parameter is a single literal whose `parse` *is* `EvictionPolicy::from_str` — the source
of truth — so the three hardcoded legal-value lists (`memory.rs:117-128`, `runtime_config.rs:513-524`,
and the implicit one in seam 5) all delete:

```rust
ConfigParam::<EvictionPolicy> {
    name: "maxmemory-policy",
    // legal values = whatever EvictionPolicy::FromStr accepts. One source of truth.
    parse: |s| s.parse().map_err(|e: ParseEvictionPolicyError| ConfigError::InvalidValue {
        param: "maxmemory-policy".into(),
        message: e.to_string(),
    }),
    validate: |_p, _ctx| Ok(()),                 // membership already enforced by parse
    default: EvictionPolicy::default,            // == NoEviction; shared with serde default
    get: |ctx| ctx.runtime().maxmemory_policy,   // RuntimeConfig now stores EvictionPolicy
    apply: |ctx, p| { ctx.runtime_mut().maxmemory_policy = p; Ok(()) },
    render: |p| p.as_str().to_string(),          // == EvictionPolicy::as_str, the GET format
    propagation: Propagation::Eviction,
}
```

The deep version retypes `RuntimeConfig::maxmemory_policy` from `String` to `EvictionPolicy`
(`runtime_config.rs:88`). That single change collapses seam 5: `build_eviction_config`
(`util.rs:97-115`) and `notify_eviction_change` (`runtime_config.rs:1692-1706`) stop re-parsing a
string and stop needing their divergent `unreachable!()` / `unwrap_or(NoEviction)` fallbacks — the
value is already typed and already validated at the one seam that produced it.

The shard-propagation name list (`runtime_config.rs:1629-1635`) also deletes: `set_async` asks
`param.propagation()` instead of testing membership in a hardcoded `eviction_params` array.

### Why this is the right depth

- **Locality.** Everything about a parameter — type, default, legal values, error text,
  runtime-apply, GET formatting, shard propagation — is one literal you can read top to bottom.
  Changing the legal values of `maxmemory-policy` means changing `EvictionPolicy::FromStr` and
  nothing else; the three copies that exist today are gone.
- **Leverage.** ~31 real parameters each shed a 5-seam spread. The `DynParam::set` ordering
  (parse→validate→apply) is written once and every parameter inherits it, including the ones whose
  inline closures get the ordering subtly wrong today (e.g. validating after a partial write).
- **Deletion test.** The change is a net deletion: three hardcoded policy lists, the `eviction_params`
  propagation list, the `MemoryConfig::validate` / `PersistenceConfig::validate` per-section list
  walks, the two redundant `EvictionConfig` builders' fallbacks, and the parallel `params.rs`
  registry (folded into the typed registry, which still feeds `docs-gen`). If a parameter could not
  be deleted by removing one literal, the shape would be wrong.
- **No new adapter layer.** This is not a wrapper *over* the existing setters; it replaces them. The
  `ParamMeta` struct (`runtime_config.rs:212-224`) and its opaque string closures are deleted, not
  wrapped. `ConfigManager::set` (`runtime_config.rs:1422-1465`) keeps its outer responsibilities
  (name normalization, strict-config/immutable/no-op gating, old→new logging) and delegates the
  lifecycle to `DynParam::set`.

## Migration plan

Behavior-preserving throughout: CONFIG GET/SET wire outputs (names, values, error strings) stay
byte-identical. The Redis-compat integration suite is the safety net for each phase. FrogDB is
pre-production, so no deprecation shims — each phase deletes the seam it replaces.

1. **Phase 0 — add the interface.** New `config/src/param.rs`: `ConfigParam<T>`, `DynParam`,
   `ConfigCtx`, `Propagation`, and the registry-builder plumbing. `ConfigManager::set` learns to
   delegate to `DynParam::set` while the old `ParamMeta` registry still exists alongside. No
   parameter migrates yet. `just check frogdb-config && just check frogdb-server`.
2. **Phase 1 — memory family (the worst offender).** Migrate `maxmemory`, `maxmemory-policy`,
   `maxmemory-samples`, `lfu-log-factor`, `lfu-decay-time`, `maxmemory-clients`. Retype
   `RuntimeConfig::maxmemory_policy` to `EvictionPolicy`; delete the hardcoded lists at
   `memory.rs:117-128` and `runtime_config.rs:513-524`; route `set_async` propagation through
   `Propagation::Eviction`. Verify with `just test frogdb-server` (CONFIG SET/GET + eviction
   integration tests).
3. **Phase 2 — persistence + replication + slowlog + logging families.** Same pattern; deletes the
   duplicated `durability-mode` / `wal-failure-policy` lists (`persistence.rs:163`, `:172` vs
   `runtime_config.rs:650`, `:671`) and the `loglevel` list (`runtime_config.rs:624`). `loglevel`
   and `requirepass` exercise the side-effecting `apply` path (log-reload handle, ACL manager).
4. **Phase 3 — encoding thresholds + no-op compat params.** The 14 no-ops become trivial
   `ConfigParam<String>` (or a dedicated `noop()` constructor) whose `apply` is a no-op; this keeps
   the strict-config gating in `ConfigManager::set` unchanged.
5. **Phase 4 — deletion pass.** Remove `ParamMeta`, `ParamSetter`, `build_param_registry`, and fold
   the standalone `params.rs` `ConfigParamInfo` registry into the typed registry (preserving the
   `section`/`field`/`docs-gen` projection as a method on `DynParam`). Confirm `docs-gen` output is
   unchanged.

## Testing impact

- **Per-stage unit tests, in isolation.** Each `ConfigParam`'s `parse`, `validate`, and `default`
  become pure functions testable without a running server: `parse("allkeys-lru") == Ok(AllkeysLru)`,
  `parse("bogus").is_err()`, `default() == NoEviction`. Today these are only reachable through a full
  CONFIG SET integration round-trip.
- **Round-trip property.** A generic test over the whole registry: for every parameter,
  `render(parse(render(default()))?) == render(default())` — i.e. GET formatting and SET parsing are
  inverses. This pins the `min-replicas-max-lag` ms↔seconds asymmetry
  (`runtime_config.rs:762-778`) that is currently implicit in two hand-written closures.
- **Default-parity test.** For every parameter the serde file default and the `ConfigParam::default`
  agree (they are the same fn pointer), closing the seam-1/seam-4 drift class structurally.
- **Legal-value parity.** Because `parse` *is* `EvictionPolicy::from_str`, the
  "CONFIG SET accepts a value the engine can't build" class (today guarded by three copies) becomes
  unrepresentable; one test asserts every `EvictionPolicy` variant round-trips through CONFIG SET.
- **Existing suites unchanged.** Migration is behavior-preserving; the Redis-compat and Jepsen
  suites confirm no wire output changed.

## Risks / open questions

- **Type erasure ergonomics.** Heterogeneous `T` forces the `Box<dyn DynParam>` boundary; the
  generic `ConfigParam<T>` is only ever used through `DynParam`. Function-pointer fields (not
  closures) keep `ConfigParam` `Copy`-cheap and `Send + Sync` without capturing state — the cost is
  that apply/get must reach state through `ConfigCtx` rather than a captured handle. Confirm every
  current setter's captured handle (log-reload, ACL, client-eviction registry, listpack atomics) has
  a `ConfigCtx` accessor before Phase 2.
- **Runtime-apply needs server/shard handles.** `ConfigCtx` must expose enough of `ConfigManager`
  for side-effecting applies (e.g. `maxmemory-clients` triggers client eviction,
  `runtime_config.rs:561-588`) without pulling `ConfigManager`'s server-crate dependencies into the
  `config` crate. Open question: does `ConfigCtx` live in `config` (with trait-object handles) or
  does the registry itself live in `server`? Leaning toward the registry in `server` and only the
  `ConfigParam<T>`/`DynParam` *types* in `config`, so applies keep direct access.
- **Ordering of cross-field validation.** `validate` sees one prospective value, but rules like
  `EvictionPolicyWithoutLimitValidator` (`validators/memory.rs:14-26`) span `maxmemory` *and*
  `maxmemory-policy`. CONFIG SET mutates one parameter at a time, so the cross-field check must run
  against the *post-apply* snapshot (existing fields + the new one). Decide whether `validate`
  receives a "what config would become" view, or whether cross-field warnings stay a startup-only
  validator family (matching today's behaviour, where CONFIG SET does not run them).
- **CONFIG GET formatting parity.** `render` must reproduce every getter quirk exactly:
  `min-replicas-max-lag` divides ms by 1000 (`runtime_config.rs:762-766`); `tls-protocols` joins a
  list; `wal-failure-policy` maps an atomic `u8` back to a word (`runtime_config.rs:666-669`). The
  round-trip test above is the guard, but each must be ported by hand.
- **How many of the 45 migrate.** The ~31 real parameters are the target. The 14 no-ops can either
  migrate to a trivial `ConfigParam` or stay as a separate no-op table; migrating them keeps one
  registry but adds low-value literals. Recommend migrating them (Phase 3) so there is exactly one
  source of truth for "what names exist," which `docs-gen` and `all_param_names` both consume.

## Correctness flags

These are pre-existing bugs/robustness gaps surfaced while mapping the seams. They are independent of
this proposal but are exactly the class it eliminates.

- `server/src/runtime_config.rs:513-524` — the CONFIG SET `maxmemory-policy` setter hardcodes the
  valid-policy list, duplicating `EvictionPolicy::FromStr` / `all_names` (`core/src/eviction/policy.rs:147-167`,
  `:106-119`). If an `EvictionPolicy` variant is added/removed, this list drifts silently and CONFIG
  SET disagrees with the engine's own parser.
- `config/src/memory.rs:117-128` — `MemoryConfig::validate` hardcodes the *same* policy list a
  **third** time (config-file validation path), with yet another error-message wording. Triple
  duplication of one source of truth.
- `server/src/server/util.rs:101-106` vs `server/src/runtime_config.rs:1698-1701` — the same
  string→`EvictionPolicy` conversion has **two divergent failure modes**: `build_eviction_config`
  panics via `unreachable!()` on an unparseable policy, while `notify_eviction_change` silently falls
  back to `NoEviction`. A stored policy that ever fails to parse crashes the startup/build path but
  silently downgrades eviction on the runtime-propagation path. Storing `EvictionPolicy` in
  `RuntimeConfig` (this proposal) removes the re-parse and both fallbacks.
- `server/src/runtime_config.rs:650` & `:671` vs `config/src/persistence.rs:163` & `:172` — the
  `durability-mode` (`["async","periodic","sync"]`) and `wal-failure-policy`
  (`["continue","rollback"]`) legal-value lists are each duplicated between the CONFIG SET setter and
  `PersistenceConfig::validate`. Same drift class as the policy list.
- `server/src/runtime_config.rs:624` — the `loglevel` setter hardcodes
  `["trace","debug","info","warn","error"]` inline rather than deriving it from a log-level enum;
  another inline-validation copy with no single source of truth.
- `server/src/runtime_config.rs:1629-1635` — the set of params that trigger shard propagation is a
  hardcoded `eviction_params` name list, decoupled from the param definitions; adding an
  eviction-affecting param requires remembering to edit this list (silent no-propagation if missed).
