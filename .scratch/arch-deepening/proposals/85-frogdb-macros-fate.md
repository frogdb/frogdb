# Proposal 85 — The fate of `frogdb-macros`: delete the crate, and neither macro candidate needs it back

Round 38 · lane: core / macros · candidates **CT3 + PN7 + PN11**, ruled together · effort **S**
(CT3) + **S** (PN7) + **S** (PN11), independently landable · **no locked crate edited**
(`frogdb-macros`, `frogdb-core`, `frogdb-types`, `frogdb-metrics-derive`) · **zero `FM-` tags in
any edited region** · **one seam lint reads a file in scope (`lint-continuation-lock` parses
`message.rs`) — cleared by construction, and the clearance is what decides PN11's shape.**

**Verified at HEAD `61156affb15256f76c1cf83a4ad0d424ab23b0a1`** (worktree `arch-round-38-99`,
branch `main`). Every file:line below was re-derived at this SHA. Concurrent authors hold
`.scratch/arch-deepening/proposals/79-*.md` dirty in `git status`; no source file in this
proposal's set is touched by any uncommitted work.

## Corrections to the lane brief

Every brief claim was re-derived. Four are wrong, and **two of the four change the ruling.**

| Brief claim | Verified at HEAD |
|---|---|
| CT3: "dependency declared at `frogdb-server/crates/server/Cargo.toml:127`" | **Correct, exactly.** `frogdb-macros = { path = "../frogdb-macros" }` at `server/Cargo.toml:127`. Note it is a **bare path dep**, not `.workspace = true` like all 14 sibling `frogdb-*` deps around it (`:110-126`) — the crate was never given a `[workspace.dependencies]` entry, which is itself a marker that it was never adopted. Zero `frogdb_macros::` references anywhere in the workspace. |
| PN7: "~300 of 830 lines in core `store/mod.rs:98-360`" | **260 lines, `:99-358`** (31 % of the file, not 36 %). 22 impl blocks: **15** `ValueType` + **7** `DefaultValueType`. |
| PN11: "~85 lines of variant→string across 11 message enums" | **65 arms, 152 lines** (`message.rs:1028-1179`), plus a 27-line `ShardMessage` delegator (`:1000-1026`) that **cannot** be derived — 180 lines total in the region. |
| PN11: "silent mislabel risk on new variants" | **Overstated — a new variant cannot land silently, twice over.** (a) Every one of the 11 matches is exhaustive with no `_` arm, so a new variant is a **compile error**. (b) `scripts/continuation-lock-gate.py:80-92` pins the arm count of all 11 `*Msg` enums (4+11+3+8+2+18+5+3+6+3+2 = **65**, the exact probe-arm count), so a new variant also **fails `just lint-gates`**. The residual risk is not omission — it is a **wrong string** on an arm that does exist (copy-paste from the neighbour above). Real, but one order of magnitude smaller than the brief implies. |
| Brief's suggestion: "check whether `strum` is already a workspace dependency — if yes, `strum::IntoStaticStr` may kill PN11 outright" | **`strum` is not present anywhere.** Zero hits in every `Cargo.toml` in the tree and zero `name = "strum"` entries in `Cargo.lock`. Adopting it would be a **new third-party dependency** on `frogdb-core`. It is also the wrong tool: `IntoStaticStr` derives `From<&T> for &'static str`, not an inherent `fn probe_type_str(&self)`, so every one of the three call sites would change shape. **Rejected on both counts.** |

Three findings the brief did not name, all verified at HEAD, all load-bearing:

1. **`metrics-derive` already ships the derive PN11 wants** — `#[derive(MetricLabel)]`
   (`metrics-derive/src/lib.rs:28-115`) generates exactly `pub const fn as_str(&self) ->
   &'static str` from enum variants, with per-variant string overrides. It is live, consumed by
   16 label enums in `types/src/metrics/labels.rs`, and it is **the second of three proc-macro
   crates already in the workspace** (`config-derive`, `frogdb-macros`, `metrics-derive`). The
   precedent for PN11's home is therefore not "resurrect `frogdb-macros`" — it is "the derive
   crate that is already alive." (§Problem 4.)
2. **`ValueType::type_name()` has exactly one consumer in the entire workspace**, and it is a
   `debug_assert!` message (`store/typed.rs:80-85`). Its own doc says *"Type name for error
   messages"* (`store/mod.rs:73`) — there is no such error message: `WrongTypeError`
   (`typed.rs:47-53`) is a unit struct with a hard-coded string. Forty-five of PN7's 260 lines
   exist to feed one debug-only panic label, and they have **silently diverged** from
   `KeyType::as_str` on two families. (§Problem 2, hotfix **H2**.)
3. **Proposal 82's PN4 rewrites the same `PubSubMsg` probe arms PN11 rewrites**, and — a defect
   82 does not appear to name — collapsing six `PubSubMsg` registration variants into one
   **changes six USDT probe names**, on a surface `message.rs:1003-1005` explicitly documents as
   byte-stable for downstream consumers. Flagged to 82's author, not claimed here. (§Risks.)

## The tension, and the ruling

The brief is right that these three pull against each other, but the pull is weaker than it looks
once the third proc-macro crate is on the table:

- **CT3** wants to delete `frogdb-macros`.
- **PN11** wants a derive macro, which needs *a* proc-macro crate.
- The brief frames this as "CT3 deletes the crate PN11 wants to live in."

**It is not the crate PN11 wants to live in.** `frogdb-macros` is not a generic proc-macro home —
it is a single-purpose crate whose one export is a `#[derive(Command)]` that **cannot compile
against today's `Command` trait**, in a workspace that already has a *working* derive crate
(`metrics-derive`) exporting a *variant→string derive of exactly PN11's shape*. Keeping 447 dead
lines of `Command`-derive machinery alive as a landlord for a ~25-line unrelated derive is
negative leverage: it preserves the whole crate's carrying cost (workspace member, build-graph
node, 0 %-coverage entry in every audit report, five documentation entries claiming it is real)
to avoid a one-line dependency edge that a live crate already offers.

**RULING: Option A′ — delete the crate outright (CT3), and give PN11 a home in `metrics-derive`.**

| Option | Verdict |
|---|---|
| **A. Delete crate + `macro_rules!` for PN7 + drop-or-re-home PN11** | **Adopted, in the "re-home" variant.** PN7 needs no proc-macro crate at all (§Problem 3 — the identical pattern already exists twice in the same module tree). PN11 re-homes to `metrics-derive`. |
| **B. Keep the crate gutted, add the `probe_type_str` derive to it** | **Rejected.** Gutting leaves 15 lines of `Cargo.toml` + a `lib.rs` stub with one derive — a fourth proc-macro crate's worth of ceremony (member entry, lock entry, doc entries, dep edge) for something `metrics-derive` already does. The crate's *name* would then lie: nothing about `frogdb-macros` says "observability strings", and its whole documented identity (`lib.rs:1-59`, 59 lines of `#[derive(Command)]` prose) would have to be deleted anyway. B is A plus a leftover shell. |
| **C. Delete now, recreate if PN11 later** | **Rejected as unnecessary, not as wrong.** C is the right answer *if* `metrics-derive` did not exist. It does. Recreating a proc-macro crate when a live one is one line away is the cost C was trying to avoid, deferred rather than removed. |

The three land in any order. **CT3 does not block PN11 and PN11 does not resurrect CT3.** That is
the whole content of the ruling: the tension the brief identified dissolves the moment the second
derive crate is counted.

## Summary

Three unrelated-looking items, one shared shape: **`frogdb-core` and its neighbours hand-maintain
lists that a table or a derive should own, while the one crate built to own such tables is dead.**

- **CT3** — delete `frogdb-macros` (447 lines, 0 % covered, zero usages, structurally
  uncompilable against the trait it targets). Pure deletion; the only non-mechanical work is five
  documentation entries that assert the crate is live.
- **PN7** — replace 22 hand-written `ValueType`/`DefaultValueType` impls (`store/mod.rs:99-358`,
  260 lines) with a `macro_rules!` table in the defining module. **No proc-macro crate involved.**
  The precedent is not hypothetical: the same module tree already carries *two* macros of exactly
  this shape (`store/typed.rs:222-284`, `types/src/types/mod.rs:71-112`), and the family list is
  currently written out **four** times.
- **PN11** — replace 11 `probe_type_str` impls (152 lines, 65 arms, **all 65 verified to be
  identity maps** variant-name → string) with one derive. Home: `frogdb-metrics-derive`, not
  `frogdb-macros`.

The architecture argument is **locality of the declaration**. Today a family's identity is spread
across four files, and a message variant's probe name is declared 60 lines away from the variant
in the same file with nothing but a compiler exhaustiveness check tying them together. The macros
move each fact to the one place a reader already looks — the type list, and the variant itself.
Interface width is unchanged in every case; only the number of places the same fact is written
goes down.

## Files involved

Verified paths and line counts at `61156aff`.

| File | Lines | Role |
|---|---|---|
| `frogdb-server/crates/frogdb-macros/src/command.rs` | 370 | **CT3 — deleted.** `derive_command_impl` `:30-88`; the generated `impl Command` `:57-85`. **Zero `FM-` tags.** |
| `frogdb-server/crates/frogdb-macros/src/lib.rs` | 77 | **CT3 — deleted.** 59 lines of doc prose `:1-59` for an unused API; `#[proc_macro_derive(Command)]` `:74-77`. **Zero `FM-` tags.** |
| `frogdb-server/crates/frogdb-macros/Cargo.toml` | 15 | **CT3 — deleted.** `proc-macro = true` `:9`; deps `syn`/`quote`/`proc-macro2` `:12-14` (all three still used by `config-derive` + `metrics-derive`, so nothing is orphaned). |
| `Cargo.toml` | `:28` | **CT3 — one line removed** from `[workspace] members`. No `[workspace.dependencies]` entry exists to remove. **Zero `FM-` tags.** |
| `frogdb-server/crates/server/Cargo.toml` | `:127` | **CT3 — one line removed.** Carries two `FM-CLUSTER-…` comments, at `:40` and `:46`, both annotating `[[test]]` stanzas ~80 lines above the edit. **Zero `FM-` tags in the `[dependencies]` region.** |
| `Cargo.lock` | `:1782-1788`, `:1962` | **CT3 — regenerated**, not hand-edited. Package stanza + the `frogdb-server` dep entry. |
| `frogdb-server/crates/core/src/store/mod.rs` | 830 | **PN7 primary.** `ValueType` `:72-81`, `DefaultValueType` `:90-93`, the 22 impls `:99-358`, 15 `type_name` bodies. `Store` trait `:393-830` (57 methods) is **out of scope** — see §Follow-ups. **Zero `FM-` tags.** |
| `frogdb-server/crates/core/src/store/typed.rs` | — | **PN7 read-only precedent** + **H2 target.** `macro_rules! typed_family_accessors` `:222-264`, its 14-row table `:266-284`; `WrongTypeError` `:47-53`; the sole `type_name()` consumer `:80-85`. **Zero `FM-` tags.** |
| `frogdb-server/crates/types/src/types/mod.rs` | 1800 | **PN7 read-only precedent.** `macro_rules! impl_value_accessors` `:71-95`, 14-row table `:97-112`, hand-written `VectorSet` pair `:114-128`; `KeyType::as_str` `:298-320`. **Owned by proposal 84 at `:322-426`** — adjacent, non-overlapping. **Zero `FM-` tags.** |
| `frogdb-server/crates/core/src/shard/message.rs` | 1446 | **PN11 primary.** `ShardMessage::probe_type_str` `:1000-1026`; the 11 category impls `:1028-1179`. Enum declarations `:151`, `:210`, `:275`, `:376`, `:398`, `:499`, `:558`, `:719`, `:769`, `:813`, `:859`, `:899` gain **one `#[derive]` line each**. **Primary file of proposal 82** — see §Risks. **Zero `FM-` tags.** |
| `frogdb-server/crates/metrics-derive/src/lib.rs` | 485 | **PN11 primary (extended).** `MetricLabel` derive `:28-115`; arm generation `:74-76`; default-label rule `:72`. **Zero `FM-` tags.** |
| `frogdb-server/crates/core/Cargo.toml` | `:32-40` | **PN11 — one line added** (`frogdb-metrics-derive.workspace = true`). |
| `frogdb-server/crates/core/src/shard/event_loop.rs` | — | **Read-only.** Sole production consumer: `msg_kind` `:133`, USDT probe `:135-139`, panic attribution `:163`. **Adjacent to proposal 81's deletion at `:122-124`** — see §Risks. **Zero `FM-` tags.** |
| `frogdb-server/crates/core/src/probes.rs` | `:69-76` | **Read-only.** `fire_shard_message_received`, `usdt-probes`-gated. |
| `scripts/continuation-lock-gate.py` | `:71`, `:80-92`, `:160`, `:176-193` | **Read-only, must stay green.** The one seam lint that parses `message.rs`. §Seam-lint clearance. |
| `website/src/content/docs/architecture/architecture.md` | `:150`, `:182` | **CT3 doc edit.** Asserts `frogdb-macros/ # Proc macros` is a live crate. |
| `.claude/skills/db-architect/references/crate-map.md` | `:23`, `:44` | **CT3 doc edit.** Describes the derive as the crate's purpose. |
| `.claude/skills/doc-sync/references/checkable-assertions.md` | `:32` | **CT3 doc edit.** Row removed. |
| `.claude/skills/doc-sync/references/doc-map.md` | `:29` | **CT3 doc edit.** Row removed. |
| `CHANGELOG.md` | `:101` | **NOT edited.** Historical release note; the crate *was* added. Left alone deliberately. |

## Problem

### 1. `frogdb-macros` is not merely unused — it cannot be used (CT3)

Three independent proofs, in ascending order of finality.

**Zero call sites.** `grep -rn 'frogdb.macros' --include='*.rs' --include='*.toml' --include='*.md'`
over the tree returns nine hits: the crate's own `Cargo.toml` and `lib.rs` doc example, the
workspace member entry (`Cargo.toml:28`), the unused dep (`server/Cargo.toml:127`), two lock
entries, and five documentation rows. **No Rust file outside the crate names it.**

**Zero coverage.** `.scratch/testing-improvements/audit/coverage-depth-2026-08-07.md:31` records
`frogdb-macros | 0/217 | 0.0% | 0/404 | 0.0%` — the only crate in the workspace at a flat zero on
both region and function coverage. `coverage-summary.md:13` names it first among the lows. This is
not an under-tested crate; it is a crate that no test can reach.

**It could not compile if someone tried.** The derive generates (`command.rs:57-85`):

```rust
impl ::frogdb_core::Command for #struct_name {
    fn name(&self) -> &'static str { … }
    fn arity(&self) -> ::frogdb_core::Arity { … }
    fn flags(&self) -> ::frogdb_core::CommandFlags { … }
    #strategy_impl
    fn execute(…) { self.execute_impl(ctx, args) }
    fn keys<'a>(…) -> Vec<&'a [u8]> { … }
}
```

`Command` (`core/src/command.rs:714-717`) declares `fn spec(&self) -> &'static CommandSpec;` with
**no default body**. The generated impl does not provide it → `E0046`. Worse, the derive is not
merely incomplete but *inverted*: the trait's own doc (`command.rs:707-713`) says every mechanical
fact "is declared once as a `CommandSpec` … The remaining methods below are *derived* from the
spec and are **not overridable** per command." `name`/`arity`/`flags`/`execution_strategy` are
exactly the four the derive overrides. The macro implements the architecture the `CommandSpec`
seam was built to replace. It is not stale — it is **counter-current**.

**Deletion test, applied to the crate.** Remove it and observe: build graph loses one leaf node;
`Cargo.lock` loses one stanza and one dep string; five documentation rows stop lying; every future
coverage and mutation audit stops carrying a permanent 0 % row. **Nothing observable is lost**,
because nothing observable was ever produced. It passes.

### 2. `type_name()` is 45 lines feeding one `debug_assert!`, and it has already drifted (PN7)

`ValueType::type_name()` is declared at `store/mod.rs:73-74` as *"Type name for error messages"*
and implemented 15 times (`:100, 120, 140, 160, 180, 200, 220, 249, 263, 277, 291, 305, 319, 333,
347`). A workspace-wide grep for `type_name()` outside the definitions returns **exactly one
site**:

```rust
// store/typed.rs:80-85
debug_assert!(
    T::from_value(&arc).is_some(),
    "TypedArc::new called with a value that is not a {}",
    T::type_name()
);
```

There is no error message. `WrongTypeError` (`typed.rs:47-53`) is a unit struct whose `Display`
writes a fixed `"WRONGTYPE Operation against a key holding the wrong kind of value"` — no type
name enters it. So the doc is wrong, and the release-build cost of all 15 impls is zero because
`debug_assert!` compiles out.

Because nothing user-facing reads it, it drifted without anyone noticing. The user-visible type
names live in `KeyType::as_str` (`types/src/types/mod.rs:298-320`), and the two lists disagree on
two families:

| Family | `ValueType::type_name()` | `KeyType::as_str()` |
|---|---|---|
| sorted set | `"sorted set"` (`store/mod.rs:161`) | `"zset"` (`types/mod.rs:307`) |
| JSON | `"json"` (`store/mod.rs:221`) | `"ReJSON-RL"` (`types/mod.rs:312`) |

`"zset"` and `"ReJSON-RL"` are the Redis-compatible spellings; `"sorted set"` and `"json"` are
not. The divergence is invisible today only because the second column is the one users see. That
is the precise failure mode a macro table prevents: **one row, one family, every string beside
every other string.**

### 3. The value-family list is written out four times, in four idioms (PN7)

| # | Where | Shape | Rows |
|---|---|---|---|
| 1 | `types/src/types/mod.rs:97-112` | `macro_rules! impl_value_accessors` table | 14 + `VectorSet` hand-written at `:114-128` (it is `Box`ed, so it does not fit the macro) |
| 2 | `types/src/types/mod.rs:298-320` | `KeyType::as_str` match | 16 (incl. `None`) |
| 3 | `store/mod.rs:99-358` | **22 hand-written impl blocks** | 15 + 7 |
| 4 | `store/typed.rs:266-284` | `macro_rules! typed_family_accessors` table | 14 |

List 4's absence of JSON is **deliberate, not drift** — JSON commands route through their own
`get_json!`/`get_json_mut!` macros (`commands/src/json/mod.rs:83-115`), which wrap the *generic*
`StoreTypedExt::get_typed::<JsonValue>` with JSON-specific null-return semantics. Recorded here so
a future author does not "fix" it. But it is a fifth idiom for the same set.

The point stands regardless: **two of the four are already `macro_rules!` tables in the exact
shape PN7 proposes**, one of them (`typed_family_accessors`, `typed.rs:222-264`) sitting in the
sibling file of the 260 hand-written lines and covering the same 14 families. `impl_value_accessors`
even earns an explicit `$( … )?` optional-slot idiom for "families without a default" — the same
7-vs-15 split `DefaultValueType` encodes by hand. **The pattern is already established, in-tree,
in this module, by two authors, and `store/mod.rs` is the file that did not adopt it.**

That is the whole PN7 case. It needs no new mechanism and no new crate.

### 4. `probe_type_str`: 65 arms, all identity, one of which cannot be derived (PN11)

Verified programmatically over `message.rs:1028-1179`: **65 arms, 65 of which map a variant to a
string byte-identical to the variant's own name.** Zero exceptions. The 11 impls exist to write
each variant's name a second time.

The `ShardMessage` delegator (`:1000-1026`) is different and must stay hand-written: two of its
arms are **payload-dependent**, not variant-dependent —

```rust
ShardMessage::DriveTick(TickKind::Expiry)        => "DriveExpiryTick",
ShardMessage::DriveTick(TickKind::WaiterTimeout) => "DriveWaiterTimeoutTick",
```

— one variant, two strings, selected by an inner enum (`message.rs:1019-1022`, both
`cfg(any(test, feature = "shard-driver"))`). No variant-name derive can express that. PN11's honest
scope is therefore **the 11 category impls (152 lines), not the delegator (27 lines).**

The strings are load-bearing on two paths, not one: `event_loop.rs:133` binds `msg_kind` and feeds
it to the USDT probe (`probes.rs:69-76`) *and* to `recover_from_panic`'s panic-site attribution
(`event_loop.rs:163`). `message.rs:1003-1005` states the byte-stability contract explicitly:
*"byte-for-byte identical to the pre-split flat variant names (e.g. `"VllAbort"`), which downstream
USDT probe consumers depend on."* A derive that defaults to the exact variant name makes that
contract **structural** instead of documented.

### 5. `metrics-derive` already contains 90 % of PN11's derive — but not the last 10 %

`#[derive(MetricLabel)]` (`metrics-derive/src/lib.rs:28-115`) generates `pub const fn as_str(&self)
-> &'static str` plus `all_values()`, `all_values_str()`, and `Display`, from an enum's variants,
with `#[label = "…"]` overrides. Sixteen enums use it (`types/src/metrics/labels.rs`). It is
tested, live, and it is precisely the map PN11 needs.

Two gaps stop it being applied as-is, both small and both real:

**(a) It cannot match a struct variant.** The generated arm is a bare unit path
(`lib.rs:74-76`): `#name::#variant_name => #label_value`. Every one of the 65 `*Msg` variants
carries fields. Confirmed by compiling the shape:

```
$ echo 'enum M { A { x: u32 }, B } fn f(m:&M)->&str{ match m { M::A=>"A", M::B=>"B" } }' | rustc --edition 2021 --crate-type lib -
error[E0533]: expected unit struct, unit variant or constant, found struct variant `M::A`
help: the struct variant's field is being ignored: `M::A { x: _ }`
```

`all_values()`/`all_values_str()` (`lib.rs:93-104`) have the same problem — they *construct*
`#name::#variant_name` — and are meaningless for a payload-carrying enum anyway.

**(b) The default is wrong.** `MetricLabel` falls back to
`variant_name.to_string().to_lowercase()` (`lib.rs:72`), because Prometheus label values are
lower-snake. Probe names must be the **exact** variant name. A derive that silently lowercased
`VllAbort` to `vllabort` would break the documented byte-stability contract on every arm at once.

So PN11 is not "reuse `MetricLabel`" — it is "**add a sibling derive next to `MetricLabel`, in the
crate that already owns this genre**": `#[derive(VariantName)]`, emitting a shape-aware pattern
(`Variant { .. }` / `Variant(..)` / `Variant` per `syn::Fields`), defaulting to the exact variant
name, with an optional `#[variant_name = "…"]` override for future renames that must not move the
probe. Roughly 25 lines beside 115 that already do the hard part. Neither `frogdb-macros` nor
`strum` contributes anything to that.

## Proposed change

### CT3 — delete `frogdb-macros`

`git rm -r frogdb-server/crates/frogdb-macros`; drop `Cargo.toml:28` and
`server/Cargo.toml:127`; regenerate `Cargo.lock`; remove the four documentation rows
(`architecture.md:150`, `architecture.md:182`, `crate-map.md:23` + `:44`,
`checkable-assertions.md:32`, `doc-map.md:29`). Leave `CHANGELOG.md:101`.

**Module/interface:** removes a workspace node whose public interface is one derive with zero
implementors. **Depth:** the crate has negative depth — 447 lines of implementation behind an
interface nobody can call, which is the pathological case. **Leverage:** every future audit
(coverage, mutation, doc-sync, crate-map) stops paying attention to it.

While editing `architecture.md:182`, note that its **Macros** row lists `frogdb-macros` and
`frogdb-metrics-derive` and omits `frogdb-config-derive`, which is also a `proc-macro = true`
crate (`config-derive/Cargo.toml`). Pre-existing; correcting the row costs one word and is folded
into the same edit.

### PN7 — one `value_types!` table in `store/mod.rs`

A `macro_rules!` in the defining module — no crate, no dependency, no proc-macro:

```rust
value_types! {
    // ty                  type_name       accessor stem   default (omitted ⇒ no DefaultValueType)
    ListValue            { "list",         as_list,        Value::list() };
    SetValue             { "set",          as_set,         Value::set() };
    HashValue            { "hash",         as_hash,        Value::hash() };
    SortedSetValue       { "sorted set",   as_sorted_set,  Value::sorted_set() };
    StreamValue          { "stream",       as_stream,      Value::stream() };
    StringValue          { "string",       as_string,      Value::string(Bytes::new()) };
    JsonValue            { "json",         as_json,        Value::json(serde_json::Value::Null) };

    // no parameterless default — ValueType only
    BloomFilterValue     { "bloom",        as_bloom_filter };
    CuckooFilterValue    { "cuckoo",       as_cuckoo_filter };
    TopKValue            { "topk",         as_topk };
    TDigestValue         { "tdigest",      as_tdigest };
    CountMinSketchValue  { "cms",          as_cms };
    HyperLogLogValue     { "hyperloglog",  as_hyperloglog };
    TimeSeriesValue      { "TSDB-TYPE",    as_timeseries };
    VectorSetValue       { "vectorset",    as_vectorset };
}
```

The `_mut` accessor is derived by `paste`-free concatenation only if a `paste`-style ident is
already available; otherwise the table carries both idents, exactly as `impl_value_accessors!`
does (`types/mod.rs:97-112`, four columns). **Match the sibling's shape rather than inventing a
terser one** — two macros with the same job should read the same.

**260 lines → ~20 table rows + a ~40-line macro body.** The optional-default slot is the
`$( , $default:expr )?` idiom already proven at `typed.rs:228`, where the comment
*"omitted for no-default families"* states the same rule this table encodes.

**Deletion test, applied to the macro:** delete `value_types!` and the 260 lines come back
verbatim — so the macro is *not* free, it is a real abstraction with a real cost (one indirection
a reader must expand). It earns that cost by (a) putting the 15 families' four facts on one screen
where a divergence like `"sorted set"` vs `"zset"` is visible, and (b) making a new family a
one-line edit that cannot half-land. Two sibling macros in the same module tree already made this
trade. **Passes.**

**Locality:** a family's identity moves from four scattered blocks to one row. **Interface
width:** unchanged — `ValueType`/`DefaultValueType` keep their exact signatures and every caller
is untouched. **Depth:** the trait pair gets shallower in source and identical in behaviour, which
is the right direction for a pure-declaration seam.

### PN11 — `#[derive(VariantName)]` in `frogdb-metrics-derive`

Add beside `MetricLabel` (`metrics-derive/src/lib.rs:115`):

```rust
#[proc_macro_derive(VariantName, attributes(variant_name))]
pub fn derive_variant_name(input: TokenStream) -> TokenStream { … }
```

generating `pub const fn probe_type_str(&self) -> &'static str`, with the arm pattern chosen from
`syn::Fields` (`Named` → `V { .. }`, `Unnamed` → `V(..)`, `Unit` → `V`) and the string defaulting
to the **exact** variant ident. No `all_values()` — payload-carrying enums cannot supply one.

`frogdb-core/Cargo.toml` gains `frogdb-metrics-derive.workspace = true` (one line; the crate has
no internal deps per `architecture.md:182`, so this adds no cycle risk and no transitive weight).
`message.rs` gains 11 `#[derive(VariantName)]` lines and loses 152.

The delegator (`:1000-1026`) stays hand-written, and its two `DriveTick` arms keep their explicit
strings. That is correct: they are the one place where the probe name is genuinely a *decision*,
and the derive removing the 65 mechanical arms leaves exactly those two visible.

**Layering:** is a shard-probe derive at home in a crate named `metrics-derive`? Yes — `msg_kind`
is an observability label on two observability paths (USDT probe, panic-site attribution), which
is the same genre as `MetricLabel`'s Prometheus label values. If a rename is wanted, the honest
one is `frogdb-derive` for both, but that is a rename, not a new crate, and it is not claimed here.

**Deletion test, applied to the derive:** delete `VariantName` and the 152 lines come back, plus
the identity invariant reverts from *guaranteed* to *reviewed*. The 152 lines are pure restatement
— the compiler already knows every variant's name. **Passes, but by a narrower margin than PN7**,
because §Corrections established that omission is already double-guarded (exhaustive match +
`continuation-lock-gate.py` arm-count pins). PN11 buys the deletion of 152 restated lines and
closes the *wrong-string* hole; it does not close a hole that was open. Rated **S / LATENT**
accordingly, and it is the one of the three a reviewer could reasonably decline.

## Testability improvement

- **CT3:** removes the workspace's only 0 %-coverage crate. Every coverage and mutation report
  stops carrying a row that can never move — a permanent false signal in
  `coverage-summary.md:13`'s "lows" list, which currently leads with it.
- **PN7:** the 260 deleted lines are today untestable in any meaningful sense (each `type_name`
  body is a literal; each `from_value` body is a one-line delegation), yet they are 31 % of the
  file's surface and 15 of its 113 `fn` declarations — they inflate the denominator of every
  `frogdb-core` coverage figure without being testable. The macro deletes the denominator.
  `typed.rs:299-324`'s `typed_matrix_base::<T>` already provides the *real* per-family test, and it
  is generic over `ValueType`, so it covers whatever the table emits with no test changes.
- **PN7, new assertion made possible:** with all 15 `type_name` strings in one table beside the
  families, a table-driven test asserting `ValueType::type_name()` agrees with `KeyType::as_str()`
  (or documenting each deliberate divergence) becomes writable in ~10 lines. Today the two lists
  are 1300 lines apart in two crates and no such test exists.
- **PN11:** turns "every arm's string equals its variant's name" from an invariant a reviewer must
  check on 65 arms into one the compiler enforces on zero. The existing
  `keyspace_coordinator.rs:253` panic-message use keeps working unchanged.
- **PN11, mutation-testing effect:** each of the 65 arms is currently an independent mutable
  literal. `cargo mutants` on `frogdb-core` must generate and run a mutant per arm, none of which
  any test kills (nothing asserts probe strings). The derive removes 65 low-value mutants from the
  crate's surface, which raises the signal in every subsequent `frogdb-core` mutation run.

## Risks and scope boundaries vs sibling proposals

### Hard overlap — proposal 82 (PN4, `ChannelTable`)

82 names `message.rs` a **Primary** file and collapses six `PubSubMsg` registration variants
(`:275-372`) into one. `PubSubMsg::probe_type_str` (`:1040-1057`) is the 11-arm impl PN11 replaces.
**They edit the same impl block.**

Sequencing, in preference order:

1. **PN11 first.** After the derive, 82's variant collapse needs *no* probe edit at all — the arms
   are generated. PN11 makes 82 strictly smaller. This is the recommended order.
2. **82 first.** PN11 then applies to a 6-arm `PubSubMsg`. Mechanical rebase, no conflict in
   substance.

Either way the enum *declarations* (82's territory) and the *impl blocks* (PN11's) are 700 lines
apart; PN11's only touch inside 82's region is **one `#[derive(VariantName)]` attribute line per
enum**, which rebases trivially.

**Cross-proposal defect flagged to 82, not claimed here:** collapsing `Subscribe`/`PSubscribe`/
`ShardedSubscribe`/`Unsubscribe`/`PUnsubscribe`/`ShardedUnsubscribe` into one variant **changes six
USDT probe names**, on a surface `message.rs:1003-1005` documents as byte-stable for downstream
consumers. 82's `Risks` section should own that, or preserve the six names via a payload-dependent
arm in the collapsed variant's `probe_type_str` (the `DriveTick` idiom at `:1019-1022`). Raised
here because PN11's audit is what surfaced it.

### Adjacency, no conflict — proposal 81 (PN2, `NewConnection`)

81 deletes the `select!` arm at `event_loop.rs:122-124`. PN11's consumer sites are
`event_loop.rs:133` and `:163` — **same file, 9 lines below the deleted hunk.** PN11 does not edit
`event_loop.rs` at all (the derive changes only `message.rs` and `Cargo.toml`), so there is no
textual conflict. Noted so a merge-conflict in that file is recognised as belonging to 81.

### Adjacency, no conflict — proposal 84 (`BlockingOp`/`Direction` dedupe)

84 deletes `types/src/types/mod.rs:322-426`. PN7 reads that file at `:71-128` (the
`impl_value_accessors!` precedent) and `:298-320` (`KeyType::as_str`, evidence for §Problem 2).
`:320` and `:322` are adjacent lines; the ranges do not overlap, and **PN7 does not edit this file
at all** — it is read-only precedent and evidence. Clean.

### Disjoint — proposals 80 and 83

- **80** (`Response`/`WireResponse` fold) works in `protocol/src/response.rs` and states at
  `:584` that it does not touch `types/src/types/mod.rs`. No file in common with 85.
- **83** (lazy-expiry authority) touches `store/mod.rs`, but at `:522-573` — four defaulted `Store`
  trait drain methods. PN7's region is `:99-358` and the trait declarations at `:72-93`. **164
  lines apart, no overlap.** Both edit the same file, so land order matters for rebase hygiene
  only.

### Substantive risks

- **PN7 macro opacity.** `rustdoc` output for macro-generated impls is thinner than for
  hand-written ones. Mitigation: the same `#[doc = concat!(…)]` idiom both sibling macros already
  use (`typed.rs:235`, `types/mod.rs:77`) — copy it, do not invent.
- **PN7 `VectorSetValue` is `Box`ed** in the `Value` enum (`types/mod.rs:64`), which is exactly why
  `impl_value_accessors!` could not take it (`types/mod.rs:114-128` is its hand-written escape).
  PN7's table has no such problem — it names `as_vectorset`, an already-written accessor — but the
  author must not assume the two tables are row-for-row identical. They are not, and the reason is
  documented above.
- **PN11 default-string regression.** If `VariantName` is implemented by copying `MetricLabel` and
  the `.to_lowercase()` at `lib.rs:72` is copied with it, all 65 probe names break silently in the
  same commit. Mitigation: a unit test in `metrics-derive`'s own crate asserting
  `VariantName`'s default is the exact ident (`MetricLabel` has no such test today — its
  lowercasing rule is asserted only by the 16 consumer enums' explicit `#[label]` attributes).
- **PN11 dependency edge.** `frogdb-core` gains a proc-macro dependency. Proc-macro crates
  serialise the build graph at their consumers. `metrics-derive` is 485 lines with three
  dependencies already in the lock; the marginal build cost is a link edge, not a compile. Named
  because it is the one real cost of PN11 that CT3's deletion does not offset.
- **Not a risk: `syn`/`quote`/`proc-macro2` orphaning.** CT3 removes `frogdb-macros`'s three deps,
  but `config-derive` and `metrics-derive` both still declare them. `Cargo.lock` keeps every
  stanza.

## Effort

| Item | Effort | LIVE/LATENT | Blocked by |
|---|---|---|---|
| **CT3** — delete `frogdb-macros` | **S** | LATENT (dead code) | nothing |
| **PN7** — `value_types!` table | **S** | LATENT | nothing (83 shares the file; rebase only) |
| **PN11** — `VariantName` derive | **S** | LATENT | prefer *before* 82; either order works |

Combined **S**. No item requires a workspace rebuild to *design*; each requires
`just check frogdb-core` (PN7, PN11) or `just check` (CT3, because it changes the workspace member
list) to land.

## Hotfix candidates

Independently landable, smallest first. Standing policy observed: **no security findings appear
below** — none were encountered; had any been, they would be classification-only, filed and
parked, never proposed as fixes.

| # | Change | Files | LIVE/LATENT | Claimed? |
|---|---|---|---|---|
| **H1** | Fix the `ValueType::type_name` doc lie: *"Type name for error messages"* → it is a `debug_assert!` label; `WrongTypeError` carries no type name. One doc line. | `store/mod.rs:73` | LATENT | **CLAIMED** |
| **H2** | Record the `type_name` ↔ `KeyType::as_str` divergence (`"sorted set"`/`"zset"`, `"json"`/`"ReJSON-RL"`) at both declarations, so the next reader of either does not assume they agree. Two doc comments, no behaviour change. | `store/mod.rs:73-74`, `types/mod.rs:299` | LATENT | **CLAIMED** |
| **H3** | Note at `typed_family_accessors!` that JSON is deliberately absent because `commands/src/json/mod.rs:83-115` owns its own null-returning accessors. Prevents a future "fix" that adds four dead methods. | `store/typed.rs:265` | LATENT | **CLAIMED** |
| **H4** | Add `frogdb-config-derive` to the **Macros** row of the crate-layer table, which currently lists two of the three `proc-macro = true` crates. Folds into CT3's edit of the same line. | `website/.../architecture.md:182` | LATENT | **CLAIMED** (with CT3) |
| **H5** | Normalise `frogdb-metrics-derive`'s two consumers: `types/Cargo.toml:13` uses `.workspace = true`, `telemetry/Cargo.toml:18` uses a bare `{ path = "../metrics-derive" }`. Same inconsistency class as the dead `frogdb-macros` dep CT3 removes. One line. | `telemetry/Cargo.toml:18` | LATENT | **NOT CLAIMED** — cosmetic, and it touches a crate no other part of 85 opens. Offered to whoever is next in `frogdb-telemetry`. |
| **H6** | Add a `metrics-derive` unit test pinning `MetricLabel`'s default-label rule (lowercased ident). Today the rule is asserted only indirectly, by the 16 consumer enums that all override it. Would have caught the `"not_available"` → `"notavailable"` bug its own comment (`lib.rs:51-53`) records. | `metrics-derive/src/lib.rs` (tests) | LATENT | **NOT CLAIMED** — it is a prerequisite *for* PN11 (§Risks), so it lands with PN11 rather than as a standalone hotfix. |

## Seam-lint clearance

Fifteen chokepoint gates were checked against every file in scope
(`grep -rn 'message\.rs' Justfile scripts/*.py` and the same for `store/mod.rs`, `store/typed.rs`,
`types/src/types/mod.rs`, `frogdb-macros`).

- **`store/mod.rs`, `store/typed.rs`, `types/src/types/mod.rs`, `frogdb-macros/**`, `Cargo.toml`,
  `server/Cargo.toml`, `metrics-derive/src/lib.rs`: zero hits in `Justfile` and zero in
  `scripts/*.py`.** No gate reads any of them. PN7 and CT3 are unconstrained.
- **`message.rs`: one gate reads it — `lint-continuation-lock`**
  (`scripts/continuation-lock-gate.py:71`, run by `lint-gates` at `Justfile:329`, which lefthook
  runs unconditionally on every commit and CI runs as the `seam-gates` job).

  **This gate is what decides PN11's shape, and it is the strongest argument against the
  `macro_rules!` alternative the brief asked me to consider.** The gate parses `message.rs`
  textually:

  ```python
  # continuation-lock-gate.py:160
  VARIANT = re.compile(r"^ {4}([A-Z][A-Za-z0-9_]*)\s*[{(,]")
  # :178
  head = re.compile(rf"\benum\s+{re.escape(name)}\s*\{{")
  ```

  It finds `enum <Name> {`, then collects every variant at **exactly four columns of
  indentation**, and checks that count against the per-enum pins at `:80-92`.

  - **A `#[derive(VariantName)]` attribute leaves the enum declaration byte-identical.** The
    `enum CoreMsg {` head, the 4-column variants, and the arm counts are all untouched. The gate
    cannot tell PN11 happened. **Cleared by construction.**
  - **A `macro_rules!`-generated enum declaration would break the gate outright** — there would be
    no `enum CoreMsg {` line to match (`enum_variants()` returns `None`), and any variants inside a
    macro invocation would sit at different indentation. `lint-continuation-lock` would fail on all
    11 enums. This is the decisive reason PN11 must be a **derive** and not a declaration table:
    not aesthetics, an existing gate.
  - The gate reads `dispatch_*.rs` bodies for `can_execute_during_lock(` calls; PN11 does not touch
    those files. Its GATE/EXEMPT/GATE_GAP sets (`:96-150`) name arms, not probe strings. Untouched.

  Incidental confirmation: the gate's eleven pinned counts sum to **65**, exactly the probe-arm
  count PN11 replaces. The two surfaces are already in lockstep, which is how §Corrections
  established that a *missing* arm cannot land silently.

- **Locked areas:** none. `frogdb-core`, `frogdb-types`, `frogdb-metrics-derive`, `frogdb-macros`
  and `frogdb-server` are outside all four locked pairs (`frogdb-txn`+`frogdb-vll`,
  `frogdb-persistence`+`frogdb-recovery`, `frogdb-replication`+`frogdb-replication-runtime`,
  `frogdb-cluster`+`frogdb-cluster-runtime`; ADRs `0002`–`0004`). No `just mutants-diff` push gate
  applies.
- **`FM-` tags:** zero in every edited region. Counted per file: `store/mod.rs` 0, `message.rs` 0,
  `store/typed.rs` 0, `types/src/types/mod.rs` 0, `metrics-derive/src/lib.rs` 0,
  `event_loop.rs` 0, `frogdb-macros/src/lib.rs` 0, `frogdb-macros/src/command.rs` 0,
  root `Cargo.toml` 0. `server/Cargo.toml` carries two — `FM-CLUSTER-079..083` at `:40` and
  `FM-CLUSTER-084..092` at `:46` — both annotating `[[test]]` stanzas; CT3's edit is the single
  dependency line at `:127`, ~80 lines below, and removes no test. `just lint-failure-modes` is
  unaffected: no spec row names any file in this proposal.
- **Docs:** `doc-sync` is a skill, not a `just` recipe — no gate fails if the four documentation
  rows are missed, which is precisely why CT3 must edit them deliberately rather than relying on
  CI to notice.

## Follow-ups (out of scope, recorded)

- **The `Store` trait split (L).** `store/mod.rs:393-830` declares a **57-method** `Store` trait.
  The brief correctly rules this out of scope for PN7 and it stays out: PN7 touches only the
  `ValueType`/`DefaultValueType` declarations and impls (`:72-93`, `:99-358`), which are 164 lines
  clear of the trait. Recorded as a follow-up candidate for a later round. Note that proposal 83
  already works inside the trait at `:522-573`.
- **`frogdb-derive` consolidation (S).** If `VariantName` lands in `metrics-derive`, the crate
  holds two unrelated-genre derives and its name understates it. A rename to `frogdb-derive`
  covering both — and possibly absorbing `config-derive`, leaving the workspace with **one**
  proc-macro crate instead of three — is the natural end state of CT3's thesis. Not claimed; it is
  a rename with a wide `use` blast radius and deserves its own proposal.
- **Probe-name byte-stability test (S).** Nothing in the tree asserts the documented contract at
  `message.rs:1003-1005`. After PN11 the contract is structural for 65 of 68 names, but the three
  hand-written delegator strings (`DriveExpiryTick`, `DriveWaiterTimeoutTick`, `Shutdown`) remain
  unasserted. A three-line test would pin them. Deliberately not folded into PN11, which is meant
  to stay a pure refactor.
