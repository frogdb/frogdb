# 69 — `build_typed_param` → typed param combinators

**Candidate:** SV9 · **Effort:** M · **Crates:** `frogdb-server` (unlocked), `frogdb-config` (unlocked)

## Summary

`ConfigManager::build_typed_param` is a 76-arm, 1596-line exhaustive `match` that hand-writes the
whole `CONFIG SET`/`GET` lifecycle for every mutable parameter. The lane brief's premise — "each arm
hand-builds a typed param … near-identical structure" — is **half right, and the half that is wrong
is the important half**:

| Lifecycle field | Mechanical | Bespoke | Verdict |
|---|---|---|---|
| `name` | 62/62 (`id.name()`) | 0 | already single-sourced — but **80 hand-copied string literals** shadow it |
| `parse` | 41/62 (66%) | 21/62 (34%) | collapsible with an escape hatch |
| `validate` | 56/62 (90%) | 6/62 (10%) | collapsible with an escape hatch |
| `render` | 50/62 (81%) | 12/62 (19%) | collapsible with an escape hatch |
| `default` | 62/62 | 0 | **dead field** — 62 lines, one reader, and that reader is its own unit test |
| `propagation` | 56/62 `None` | 6/62 | collapsible (default-`None` builder) |
| **`get`/`apply`** | **11/62 (18%)** | **51/62 (82%)** | **NOT collapsible. These are real per-param subsystem wiring.** |

So the honest thesis is *not* "declare each param as data and delete the match". Proposal 13 already
delivered the data-shaped part (metadata registry, derive macro, identity enums) and `ConfigParam`
is already a struct literal, not imperative code. What remains is a **combinator layer over the
front half of the lifecycle** — name/parse/validate/render/default/propagation — while `get`/`apply`
stay closures, because 82% of them touch a different subsystem handle in a different way. A proposal
that promises to table-ify `get`/`apply` would be lying about this file.

Realistic outcome: match body **1596 → ~1050 lines** (~34%), 62 dead `default:` lines deleted, 80
redundant name literals deleted, 14 `NoopParam` arms (56 lines) collapsed to a const slice, and one
LIVE wire-behavior defect fixed.

**LIVE defect found:** two boolean parameters (`latency-tracking`, `key-memory-histograms`) reject
`on`/`off`, which the other 17 boolean parameters accept — because their parse grammar is
hand-inlined instead of calling the shared `parse_yes_no` helper. Second, cosmetic-but-wire-visible:
16 distinct error message texts for what is mostly one integer parse (`"must be a non-negative
integer"` ×26 vs `"must be a positive integer"` ×3 vs `"must be an integer"` ×1 on identically
unsigned types).

## Files involved

All paths verified at HEAD (`08c143d6`); line counts are whole-file.

| File | Lines | Role in this proposal |
|---|---|---|
| `frogdb-server/crates/server/src/runtime_config.rs` | 5905 | **Primary.** `build_typed_param` :1717–3315 (match body :1719–3314). Also the shared helpers :583–666, `NoopParam` :697–722, `typed_param` :3318–3324, `get` :3359–3368, `set` :3388–3438, and 71 in-file `#[test]`s from :3765. |
| `frogdb-server/crates/config/src/param.rs` | 282 | Owns `ConfigParam<T, C>` :85–115, `DynParam<C>` :122–140, the blanket impl :142–161, `no_validate` :165–167, `ConfigError` :26+. **The combinator builder lands here.** Dead `default` field at :99; its only reader is `default_is_callable` :258–261. |
| `frogdb-server/crates/config/src/param_id.rs` | 368 | `param_id_enum!` :44–70; `MutableParamId` (76 variants) :81–196; `ImmutableParamId` (47) :205–276; count-stability tests :279–368. Read-only for this proposal, except one doc hotfix (below). |
| `frogdb-server/crates/config/src/params.rs` | 1670 | `ConfigParamInfo` :31–46, `VIRTUAL_PARAMS` :56+, `GOLDEN_SNAPSHOT` :552+ (**123 rows**, not 118). **Untouched** — see "Golden interaction". |
| `frogdb-server/crates/config-derive/src/lib.rs` | 443 | `#[derive(ConfigParams)]`. Untouched. |
| `frogdb-server/ops/docs-gen/src/main.rs` | 771 | Only external consumer of the param table (`build_param_lookup` :378–388, `extract_fields` :644–694). Beneficiary, not editor — see "Leverage". |

## Problem

### P1 — The match is 1596 lines and its regularity is unevenly distributed

`build_typed_params` :1702–1707 maps `MutableParamId::ALL` through `build_typed_param`, whose match
body spans :1719–3314 — **1596 lines, 76 arms** (62 `ConfigParam`, 14 `NoopParam`). Arm length ranges
4 → 50 lines. Type histogram across the 62 typed arms:

```
u64 23 · bool 10 · String 7 · usize 5 · u8 3 · u32 3 · f64 3 · OptionalPathValue 3
EvictionPolicy 1 · MinReplicasMaxLagSecs 1 · i64 1 · Vec<f64> 1 · Vec<String> 1
```

Scoring each arm 0–5 on how many of its five lifecycle fields are bespoke:

```
irr=0: 10 arms   irr=1: 2   irr=2: 29   irr=3: 10   irr=4: 9   irr=5: 2
```

**Honest irregularity rate: 34% of `parse`, 10% of `validate`, 19% of `render` are bespoke — but 82%
of `apply` and 74% of `get` are.** The bulk of the arms (`irr=2`, 29 of 62) are irregular *only* in
`get`/`apply`; their parse/validate/render are pure boilerplate. That is the exact shape a
front-half combinator collapses and a whole-lifecycle table does not.

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
    apply: |mgr, v| { mgr.runtime.write().unwrap().maxmemory = v; Ok(()) },
    render: |v| v.to_string(),
    propagation: Propagation::Eviction,
}),
```

The only two lines carrying param-specific knowledge that a combinator cannot supply are `get` and
`apply`. Everything else is derivable from `(id, u64, Propagation::Eviction)`.

### P2 — 80 hand-copied wire-name literals shadow `name: id.name()`

The doc comment at :1712–1716 says "The wire name comes from `id.name()`, pinning each literal to its
identity." It does — for the `name` field. But every error path inside the same arm re-types the
name as a raw string: **80 occurrences across the 62 typed arms; every typed arm has at least one.**
`BatchSizeThresholdKb` :3034–3076 spells `"batch-size-threshold-kb"` three times in the body of an
arm whose first line is `name: id.name()`.

**Ruling: LATENT, zero drift today.** A census of every hyphenated literal inside every arm against
`MutableParamId::name()` found no arm carrying a foreign param name. But nothing prevents it: a
rename touches `param_id.rs` and the compiler stays silent about the 80 copies, so `CONFIG SET
new-name bad` would report `ERR ... old-name ...`. `ConfigError`'s Display is documented as
wire-visible protocol surface (`param.rs:26–27`), so this is a protocol correctness hazard, not a
style nit.

### P3 — `default: fn() -> T` is dead weight (62 lines)

`param.rs:99` declares it with the aspiration (`:97–98`) that it be "the same fn serde's
`#[serde(default)]` uses, so the file default and the CONFIG default cannot diverge." Nothing
enforces that and nothing reads it: `DynParam` (`:122–140`) exposes no `default` accessor, so the
blanket impl `:142–161` cannot forward it and no caller in `runtime_config.rs` reads `.default`. Its
only reader in the workspace is `param.rs:258–261`:

```rust
fn default_is_callable() {
    assert_eq!((p.default)(), 5);
}
```

An interface whose sole consumer is a test asserting the interface exists fails the deletion test.
62 arms pay a line for it. **Ruling: LATENT (dead code, no behavior).**

### P4 — LIVE: three bool arms hand-inline the boolean grammar, and two of them get it wrong

`parse_yes_no` (:590–599) is the shared grammar and accepts `yes|true|1|on` / `no|false|0|off` with
message `"must be yes/no"`. Nine arms call it. Three arms inline it instead:

| Arm | Line | Accepts true | Accepts false | Message |
|---|---|---|---|---|
| `parse_yes_no` (helper) | 590 | `yes true 1 on` | `no false 0 off` | `must be yes/no` |
| `PerRequestSpans` | 1899 | `yes true 1 on` | `no false 0 off` | `must be yes/no` |
| `LatencyTracking` | 2339 | `yes 1 true` | `no 0 false` | **`must be yes or no`** |
| `KeyMemoryHistograms` | 2449 | `yes 1 true` | `no 0 false` | **`must be yes or no`** |

`PerRequestSpans` is a byte-exact clone — pure duplication. The other two are a **real divergence**:
`CONFIG SET latency-tracking on` and `CONFIG SET key-memory-histograms off` fail with
`ERR ... must be yes or no` while the same spelling succeeds on the other 17 booleans. All three also
inline the render (`:1915–1921`, `:2354–2360`, `:2481–2487`) as a 7-line `if *v { "yes" } else
{ "no" }` that is behaviorally identical to the one-line `yes_no` helper (:584–587) — that part is
latent duplication only.

**Ruling: LIVE, minor.** A client that speaks Redis's tolerant boolean spellings gets inconsistent
answers across parameters. Note the fix direction is a policy call: real Redis rejects `on`/`off`
for `CONFIG SET` booleans, so FrogDB is *more* permissive than Redis in 17 places and Redis-strict in
2. Either direction is defensible; **the inconsistency is the defect** and it must be resolved one
way for all 20.

### P5 — LIVE (cosmetic, wire-visible): 16 error-message dialects for ~6 concepts

Message-text census over the match body:

```
26  "must be a non-negative integer"          5  "must be > 0"
 3  "must be a positive integer"              2  "must be an integer 1-100"
 2  "must be yes or no"                       1  "must be yes/no"
 1  "must be between 0.0 and 1.0"             1  "must be a number between 0.0 and 1.0"
 1  "must be an integer"                      1  "must be an integer 0-255"
 1  "must be a non-negative integer (seconds)"     1  "... (milliseconds)"
 1  "requires tls-cluster to be enabled"      1  "invalid flag characters"
 1  "must be 0 (disabled), a byte value (e.g. 100mb), or a percentage (e.g. 5%)"
 1  "can't enable key-memory-histograms at runtime"
```

`"must be a non-negative integer"` / `"must be a positive integer"` / `"must be an integer"` are
attached to identically-unsigned parses; the distinction is accidental, not semantic. Same for the
two float-range spellings. **Ruling: LIVE, cosmetic** — clients see arbitrary variation in a
documented protocol surface. It is also the single biggest constraint on the refactor (see Risks).

### P6 — 14 `NoopParam` arms are 56 lines of pure data

`:2493–2552`, four lines each, two fields, no logic:

```rust
Save => Box::new(NoopParam { name: id.name(), value: "" }),
SetMaxIntsetEntries => Box::new(NoopParam { name: id.name(), value: "512" }),
```

This is the one part of the file that genuinely *is* a table pretending to be code. **Ruling:
LATENT** (correct today).

### P7 — `typed_param` is a linear scan (minor, latent)

`typed_param` :3318–3324 linearly scans the 76 boxed params by name; `ConfigManager::get` :3359–3368
walks the 123-row metadata registry calling it, so `CONFIG GET *` is O(76 × 123). Irrelevant at this
size; worth noting only because the combinator work naturally produces a name→index map. **Not a
justification for the proposal** — listed so a reviewer does not mistake it for one.

## Proposed change

### The module and its interface

Add a **builder module** inside `frogdb-config`'s existing `param` module — `param.rs` already owns
the `ConfigParam` concept, so this is **depth added to an existing module**, not a new one. The
builder is an **adapter**: it constructs the same `ConfigParam<T, C>` the arms construct today, so
`DynParam`, the blanket impl, `TomlRenderable`, and every call site are unchanged. `ConfigParam`
stays publicly constructible as a struct literal — an arm that outgrows the combinators drops back to
the literal with no ceremony, which is what keeps the escape hatch honest.

```rust
// frogdb-config: param builder — the mechanical front half only.
pub fn int_param<T: FromStr + Display, C>(id: MutableParamId) -> ParamBuilder<T, C>;
pub fn bool_param<C>(id: MutableParamId) -> ParamBuilder<bool, C>;
pub fn string_param<C>(id: MutableParamId) -> ParamBuilder<String, C>;

impl<T, C> ParamBuilder<T, C> {
    fn min(self, lo: T) -> Self;                    // the 9 `if *v == 0 / < / >` validates
    fn range(self, lo: T, hi: T) -> Self;
    fn parse_with(self, f: ParseFn<T>) -> Self;     // escape hatch: the 21 custom parses
    fn validate_with(self, f: ValidateFn<T, C>) -> Self;  // escape hatch: the 6 custom validates
    fn render_with(self, f: RenderFn<T>) -> Self;   // escape hatch: the 12 custom renders
    fn propagates(self, p: Propagation) -> Self;    // default None covers 56/62
    fn cell(self, get: GetFn<T, C>, apply: ApplyFn<T, C>) -> ConfigParam<T, C>;  // ALWAYS explicit
}
```

`.cell(get, apply)` is deliberately the **terminal** combinator and deliberately takes both closures
explicitly. There is no `.runtime_field(...)` shorthand even though it would fit 11 arms, because the
other 51 would then read as exceptions to a rule that only holds for 18% of the file. **The seam this
proposal draws is: parse/validate/render/name/default/propagation are declarative; get/apply are
code.** `Maxmemory` becomes:

```rust
Maxmemory => Box::new(
    int_param::<u64, _>(id)
        .propagates(Propagation::Eviction)
        .cell(
            |mgr| mgr.runtime.read().unwrap().maxmemory,
            |mgr, v| { mgr.runtime.write().unwrap().maxmemory = v; Ok(()) },
        ),
),
```

18 lines → 9, and the wire name, the parse, the error message and the render all now have exactly one
definition each in the workspace.

### Locality and leverage

**Locality.** Today, "what spellings does a boolean parameter accept?" is answered in 12 places
(`parse_yes_no` plus 3 inlined copies plus per-arm messages). After: one function. "What error text
does an integer parse produce?" — 30 places today, one after. The per-arm text that genuinely differs
(`"must be an integer 0-255"`, the maxmemory-clients grammar) survives via `.parse_with`, which is
where it belongs: co-located with the thing that is actually special.

**Leverage — the docs-gen argument.** `docs-gen` (`ops/docs-gen/src/main.rs`) is the only external
consumer of the param table. `build_param_lookup` :378–388 keys on `(section, field)`;
`extract_fields` :644–694 emits `config_param` and `mutable` flags; defaults come from
`serde_json::to_value(Config::default())` and enum value sets from `schema_for!(Config)`. **Bounds
and legal-value sets living in `validate`/`parse` are invisible to it** — the published config
reference cannot say "1–100" for `status.memory-warning-percent` or "≤ MAX_BATCH_SIZE_THRESHOLD_KB"
for `batch-size-threshold-kb`, because those facts exist only as closure bodies. Once `.min`/`.range`
capture them as data, a follow-up can surface them; today no follow-up is possible. That is the
leverage: the combinators turn 9 closures into 9 machine-readable bounds. **This proposal does not
change docs-gen** — it makes the change possible, and says so honestly rather than claiming the
benefit now.

`frogctl` and `frogdb-operator` are **not** consumers of the param table (the operator imports the
serde section structs from `frogdb-config`, which this proposal does not touch — ADR-0001
crate-lightness holds: the builder adds no dependency, it is generic code over `fn` pointers).

### Deletion test

| Thing | Verdict |
|---|---|
| `ConfigParam::default` field (`param.rs:99`) + 62 arm lines + `default_is_callable` (`:258–261`) | **Delete.** Zero production readers; the sole test asserts the field's own existence. If the file-default/CONFIG-default parity the doc aspires to is wanted, it must be a real cross-check against serde defaults — a separate proposal, not a dead field. |
| 3 inlined bool `parse` bodies (`:1899`, `:2339`, `:2449`) | **Delete**, replace with `bool_param`. Fixes P4. |
| 3 inlined bool `render` bodies (`:1915`, `:2354`, `:2481`) | **Delete**, replace with `yes_no`. |
| 14 `NoopParam` arms (`:2493–2552`) | **Delete**, replace with a `const NOOP_VALUES: &[(MutableParamId, &str)]` consulted before the match (or a single catch-all arm). 56 lines → ~18. |
| 80 in-arm wire-name literals | **Delete**, the builder threads `id.name()` into every error it constructs. |
| `ConfigParam` struct literal | **Keep public.** The escape hatch must stay first-class. |
| `NoopParam`'s `TomlRenderable` impl (`:724+`) | **Keep** — documented as never invoked because no-op rows are `section: None`, but it is a trait-completeness obligation, not a dead branch. |

### Golden interaction — no interaction

Duty 2, answered definitively: **the golden snapshot is metadata-only and this refactor cannot move
it.** `GOLDEN_SNAPSHOT` (`params.rs:552+`) pins rows of `ConfigParamInfo` (`:31–46`) — name, section,
field, mutability flags — produced by `#[derive(ConfigParams)]` plus `VIRTUAL_PARAMS`, all inside
`frogdb-config` and all sourced from the serde structs. It contains **123 rows** (`test_golden_snapshot_row_count`;
the "118" in the round ledger is stale — 118 was the mutability round's count). `build_typed_param`
consumes `MutableParamId`; it does not feed the registry. Since the combinators change neither the
identity enums nor the derive nor `VIRTUAL_PARAMS`, `GOLDEN_SNAPSHOT` is byte-identical after this
work, and so are `mutable_ids_match_registry_partition` / `id_counts_are_stable` (76 + 47).

What **is** client-visible and must stay byte-identical is different and narrower:

1. **`CONFIG GET` render output** for all 76 params (`render` → `DynParam::get`).
2. **`ConfigError` Display text** on every rejected `CONFIG SET` — explicitly documented as protocol
   surface at `param.rs:26–27`.
3. **`CONFIG REWRITE` TOML output** (`config_updates` :1278–1304 via `TomlRenderable` :162–175).

(1) and (3) are safe by construction — the builder's default render is `|v| v.to_string()` and
`yes_no`, which is what those arms already do, and `TomlRenderable`'s blanket impl goes through `get`
which is unchanged. (2) is the real constraint and is handled in Risks.

## Testability improvement

Today a bug in one arm's parse grammar is invisible unless someone wrote a test for *that parameter*
— which is exactly how P4 survived. The 71 in-file tests cannot cover 62 arms × 5 fields.

1. **Grammar tests move from per-param to per-combinator.** One table test over `bool_param` covering
   `yes/true/1/on/no/false/0/off/garbage` proves the grammar for all 20 booleans at once, and a
   twentieth boolean added later inherits the proof. Same for `int_param` (30 arms) and `.min`/`.range`
   (9 arms).
2. **A drift test becomes possible.** With the builder threading `id.name()` into every error, a loop
   over `MutableParamId::ALL` can assert that a deliberately-invalid `CONFIG SET <name>` reports
   `<name>` — P2's 80 literals become unrepresentable rather than merely undrifted. That test cannot
   be written today without 62 hand-written cases.
3. **A render round-trip becomes cheap.** `for id in ALL: set(get(id))` must be accepted and produce
   the same rendered string. The mutability round already built a REWRITE round-trip suite
   (SET → REWRITE → reparse → validate); this is its `CONFIG GET` counterpart, and the combinators make
   it a loop instead of a table of literals.
4. **Bound parity gets a hook.** The 9 `.min`/`.range` bounds become inspectable data, so a test can
   assert they match the boot-time `Section::validate()` bounds (`memory.rs:157–175`,
   `status.rs:71–87`, `persistence.rs:267+`, `replication.rs:359+`). Today that parity is maintained
   by hand-written comments — e.g. `StatusMemoryWarningPercent` :2833–2853 carries "Same bound as
   `StatusConfig::validate`" as *prose*. **Ruling on that parity: LATENT, correct today** (I checked
   all 9 against their boot validators and found no mismatch) — but it is comment-enforced, which is
   the condition under which it eventually stops being correct.

Mutation-testing note: `frogdb-server` and `frogdb-config` are **not locked crates** (locked set is
txn/persistence/replication/cluster per CLAUDE.md and `adr/0002`–`0004`), so no `just mutants-gate`
threshold applies. `just lint-failure-modes` is unaffected — see Risks.

## Risks / scope boundaries

### R1 — Error-message byte-parity is the hard constraint (highest risk)

P5's 16 dialects mean a naive `int_param` unifies 30 error strings and silently changes wire text for
~4 params. Two acceptable resolutions, and the proposal must pick one **before** coding:

- **(a) Preserve exactly.** `int_param` takes the message from a per-type default and every arm whose
  current text differs uses `.parse_with` or a `.message(...)` override. Zero wire change; keeps ~5
  arms verbose; leaves P5 unfixed.
- **(b) Normalize deliberately.** Collapse the 3 integer dialects to one and the 2 float dialects to
  one, as an intentional, documented protocol change (FrogDB is pre-production; deviations that are
  improvements are allowed). Fixes P5; requires updating any regression test asserting the old text.

**Recommendation: (b), gated on a grep of the regression suite for the exact strings.** If any
Redis-derived compatibility test asserts a message, fall back to (a) for that param only. This must
be settled in review, not discovered mid-implementation.

### R2 — `apply` side effects must not be disturbed

51 of 62 `apply` closures do more than store a value: late-published `OnceLock` handles with
catch-up-on-publish (`set_snapshot_coordinator` :1064–1074, `set_replication_lag_thresholds`
:1099–1109, `set_backlog_ttl` :1116–1122, `set_replication_self_fence` :1128–1138), TLS rebuild
(`apply_tls` :1223–1257), cross-sibling validation (`StatusDurabilityLagWarningMs` :2873–2906 vs
`Critical` :2907–2937), unit conversion (`BatchSizeThresholdKb` :3034–3076 KiB↔bytes,
`MinReplicasMaxLag` :2101–2135 `div_ceil`). **The proposal moves none of them.** `.cell()` takes them
verbatim. I audited get/apply targeting across all 62 arms: every arm's `get` and `apply` address the
same cell, including the two Redis alias pairs that intentionally share one
(`HashMaxZiplistEntries` :2228 / `HashMaxListpackEntries` :2264, and the `*Value` pair).
**Ruling: LATENT-clean, no defect.** Also checked: `set_tls_runtime` :1181–1184 lacks the
catch-up other late-publish setters have, but `apply_tls` errors with `TLS_NOT_RUNNING` (:631–632) on
a missing handle, so it is correct by a different route. **Ruling: LATENT-clean.**

### R3 — Conflict with proposal 63's `shared_maxmemory` hotfix (ordering, mandatory)

63 §Problem-5 and §Risks pin a hotfix at **`runtime_config.rs:1733`** — the `Maxmemory` arm's `apply`
(:1732–1735) must also store into the shared eviction atomic. That line is **inside my match's
range**, in the very first arm.

**Ordering: 63's hotfix lands first; 69 rebases over it.** The hotfix is a 1–2 line behavioral fix to
one closure body; 69 then carries that closure verbatim into `.cell(...)`. The reverse order forces
63 to re-derive its line cite against a reshaped file for no benefit. 69's rebase is mechanical
(the closure text is unchanged; only its surrounding syntax moves). **69 must not start the
`Maxmemory` arm until 63's hotfix is merged**, or must explicitly leave `Maxmemory` as a struct
literal in its first pass and convert it in a follow-up.

### R4 — Sibling edges

| Sibling | Owns | Edge | Resolution |
|---|---|---|---|
| **63** server subsystem bundles | `server/mod.rs`, `server/init.rs` | **Real, inside my range.** Hotfix at :1733; also cites the `Propagation::Eviction` handler at :3626–3628 (outside the match, untouched by 69) | R3 ordering |
| **41** persistence small-dedups | `wal/*` | **Read-only.** Cites `runtime_config.rs:3065–3070` as evidence that production drives the WAL threshold via `CONFIG SET`. Verified accurate at HEAD (`BatchSizeThresholdKb`'s `get`/`apply`). 41 edits `wal/`, not my file | None. If 69 lands first, 41's *cite* shifts by ~10 lines; the prose stays true. Whoever lands second re-derives the number |
| **66** shard worker builder | `core/src/shard/*`, `shards.rs` | **Read-only.** Cites `ConfigManager::wal_failure_policy_flag` :1167 — outside the match | None |
| **48** FCALL cross-shard | `subsystems.rs`, scripting | None. 63's table lists it as a `subsystems.rs:559` edge with *63*, not with 69 | None |
| **67** server small-dedups | `server/src/connection/*` | 67's own boundary table already records **"69 runtime_config.rs — None"** | None |
| **68** exec-framing | `connection/transaction.rs`, `connection/dispatch.rs`, `core/src/command_spec.rs` | None — `CommandSpec` lives in `frogdb-core`; no contact with the config crates | None |
| **90** (future CT2) commands sweep | `frogdb-commands` | None — different crate; `frogdb-commands` does not consume the param table | None |

### R5 — LOCKED-area contact: none, plus one pre-existing stale cite

`runtime_config.rs` is in `frogdb-server` (`crates/server/Cargo.toml:2`), which is **not** one of the
four locked areas. Grepping `.scratch/hardening/specs/*.md` and `adr/*.md` for `runtime_config.rs`
yields exactly two hits, both in `replication-failure-modes.md`, and neither is a live obligation:

- **`:1191`** cites `runtime_config.rs:2028-2046` for the GAP-4 min-replicas-max-lag rounding bug.
  **That cite is already stale at HEAD** — 2028–2046 is now the `ScatterGatherTimeoutMs` arm; the
  min-replicas arms are `MinReplicasMaxLagMs` :2073–2100 and `MinReplicasMaxLag` :2101–2135, and the
  described integer arithmetic no longer exists (the `MinReplicasMaxLagSecs` newtype does `div_ceil`).
  It sits inside a closed `<details>` block preserving the original reasoning, so **it is archive
  text and should be left alone** — reported here so a reviewer does not read it as a live cite my
  refactor breaks. `just lint-failure-modes` cannot see it: the linter binds only backticked
  `Forced by` test names, never prose cites.
- **`:1201`** cites `runtime_config.rs` with no line number — unaffected by any reshaping.

The file carries 5 `// FM-*` tags (`:4474`, `:4530`, `:4548` FM-REPLICATION-046; `:4844`
FM-CLUSTER-059; `:5137` FM-PERSISTENCE-046) — **all in the test module, all far outside the match
body (:1719–3314). Untouched.** No seam lint or generator references this file by path.

### R6 — Scope boundaries the proposal will not cross

- **No `get`/`apply` table.** Stated up front so review does not push toward it.
- **No metadata-registry work.** Proposal 13's residual gaps (per-struct coverage 13-02, the
  `#[param(skip)]` audit 13-01) stay filed; this is a different layer.
- **No immutable-param work.** `ImmutableParamId`'s 47 rows have no lifecycle closures.
- **No docs-gen change.** Enabled, not delivered.
- **No `VIRTUAL_PARAMS` change.** The hardcoded defaults in `ConfigManager::with_collaborators`
  (:961–1045 — `lua_time_limit` 5000 at :985, listpack atomics :986–991, `notify_keyspace_events`
  :994, latency percentiles :997, `key_memory_histograms_state` :998) are virtual params with no
  serde backing **by design**, not drift. **Ruling: LATENT-clean.**

## Effort

**M**, matching the candidate estimate. Roughly:

- **~0.5 day** — builder in `param.rs` (~150 lines) + its unit tests. Self-contained, no arms touched.
- **~1.5 days** — convert the 62 arms in family batches (memory → persistence → replication → TLS →
  status → cluster → hotshards), one commit per family so a regression bisects to a family.
- **~0.5 day** — `NoopParam` collapse, `default` field deletion, P4 bool fix + its regression test,
  P5 message decision applied.

The size risk is entirely R1 (message parity): if the regression suite pins message text widely,
per-arm overrides push this toward L. Grep first.

### Independently-landable hotfixes

Each stands alone, needs no builder, and can land before or without the refactor:

| # | Fix | Files | Ruling | Size |
|---|---|---|---|---|
| **H1** | **Boolean grammar unification.** Point `LatencyTracking` :2339 and `KeyMemoryHistograms` :2449 (and `PerRequestSpans` :1899, a byte-exact clone) at `parse_yes_no`, and their renders at `yes_no`. Add a table test asserting all 20 bool params accept the same spellings. Requires the R1-style policy call on `on`/`off` vs Redis strictness | `runtime_config.rs` | **LIVE** | ~30 lines removed, 1 test |
| **H2** | **Delete the dead `default` field.** Remove `param.rs:99`, `default_is_callable` :258–261, and the 62 `default:` lines | `param.rs`, `runtime_config.rs` | LATENT (dead code) | −64 lines |
| **H3** | **`NoopParam` arms → const slice.** `:2493–2552` → a `&[(MutableParamId, &str)]` table | `runtime_config.rs` | LATENT | −38 lines |
| **H4** | **Stale doc count.** `param_id.rs:10` says "the 45 restart-required parameters"; `id_counts_are_stable` asserts **47** | `param_id.rs` | LATENT (doc) | 1 line |
| **H5** | **Integer-message normalization.** Collapse `"must be a positive integer"` (3) and `"must be an integer"` (1) into `"must be a non-negative integer"` (26) on identically-unsigned parses, after grepping the regression suite | `runtime_config.rs` | **LIVE** (cosmetic, wire-visible) | ~4 lines |

H1 is the one worth landing regardless of whether proposal 69 is accepted.
