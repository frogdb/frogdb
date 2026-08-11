# Proposal 85 — The fate of `frogdb-macros`: delete the crate, and neither macro candidate needs it back

**Revision 2** — amended after adversarial review. The core ruling (Option A′) is unchanged and was
confirmed on every leg; the amendments correct a false coverage claim, **reverse the recommended
land order against proposal 82**, exclude `PubSubMsg` from PN11's derive, repair the PN7 sketch,
and fix a set of line cites. See §Revision-2 changes for the full list.

Round 38 · lane: core / macros · candidates **CT3 + PN7 + PN11**, ruled together · effort **S**
(CT3) + **S** (PN7) + **S** (PN11), independently landable · **no locked crate edited**
(`frogdb-macros`, `frogdb-core`, `frogdb-types`, `frogdb-metrics-derive`) · **zero `FM-` tags in
any edited region** · **one seam lint reads a file in scope (`lint-continuation-lock` parses
`message.rs`) — cleared by construction, and the clearance is what decides PN11's shape.**

**Originally verified at `61156affb15256f76c1cf83a4ad0d424ab23b0a1`; every file:line below
re-verified for revision 2 at HEAD `e11bd514`** (worktree `arch-round-38-99`). The eleven commits
between the two SHAs touch **only** `.scratch/arch-deepening/proposals/*.md` — `git diff --stat
61156aff..e11bd514` lists no source file — so the code the original verification rested on is
byte-identical. Every cite below is stated at the *current* tree; where revision 1 was off by a
line or two, the corrected value is used and the drift is recorded in §Revision-2 changes.

## Corrections to the lane brief

Every brief claim was re-derived. Four are wrong, and **two of the four change the ruling.**

| Brief claim | Verified at HEAD |
|---|---|
| CT3: "dependency declared at `frogdb-server/crates/server/Cargo.toml:127`" | **Correct, exactly.** `frogdb-macros = { path = "../frogdb-macros" }` at `server/Cargo.toml:127`. Note it is a **bare path dep**, not `.workspace = true` like all **15** sibling `frogdb-*` deps above it (`:112-126`) — the crate was never given a `[workspace.dependencies]` entry, which is itself a marker that it was never adopted. Zero `frogdb_macros::` references anywhere in the workspace. |
| PN7: "~300 of 830 lines in core `store/mod.rs:98-360`" | **260 lines, `:99-358`** (31 % of the file, not 36 %). 22 impl blocks: **15** `ValueType` + **7** `DefaultValueType`. |
| PN11: "~85 lines of variant→string across 11 message enums" | **65 arms, 152 lines** (`message.rs:1028-1179`), plus a 27-line `ShardMessage` delegator (`:1000-1026`) that **cannot** be derived — 180 lines total in the region. |
| PN11: "silent mislabel risk on new variants" | **Overstated — a new variant cannot land silently, twice over.** (a) Every one of the 11 matches is exhaustive with no `_` arm, so a new variant is a **compile error**. (b) `scripts/continuation-lock-gate.py:80-92` pins the arm count of all 11 `*Msg` enums (4+11+3+8+2+18+5+3+6+3+2 = **65**, the exact probe-arm count), so a new variant also **fails `just lint-gates`**. The residual risk is not omission — it is a **wrong string** on an arm that does exist (copy-paste from the neighbour above). Real, but one order of magnitude smaller than the brief implies. |
| Brief's suggestion: "check whether `strum` is already a workspace dependency — if yes, `strum::IntoStaticStr` may kill PN11 outright" | **`strum` is not present anywhere.** Zero hits in every `Cargo.toml` in the tree and zero `name = "strum"` entries in `Cargo.lock`. Adopting it would be a **new third-party dependency** on `frogdb-core`. It is also the wrong tool: `IntoStaticStr` derives `From<&T> for &'static str`, not an inherent `fn probe_type_str(&self)`, so every one of the three call sites would change shape. **Rejected on both counts.** |

Three findings the brief did not name, all verified at HEAD, all load-bearing:

1. **`metrics-derive` already ships the derive PN11 wants** — `#[derive(MetricLabel)]`
   (`metrics-derive/src/lib.rs:28-115`) generates exactly `pub const fn as_str(&self) ->
   &'static str` from enum variants, with per-variant string overrides. It is live, consumed by
   **6** label enums in `types/src/metrics/labels.rs` (`:14`, `:28`, `:39`, `:56`, `:70`, `:81`;
   17 `#[label = "…"]` overrides between them), and it is **the second of three proc-macro
   crates already in the workspace** (`config-derive`, `frogdb-macros`, `metrics-derive`). The
   precedent for PN11's home is therefore not "resurrect `frogdb-macros`" — it is "the derive
   crate that is already alive." (§Problem 4.)
2. **`ValueType::type_name()` has exactly one consumer in the entire workspace**, and it is a
   `debug_assert!` message (`store/typed.rs:81-85`, inside `TypedArc::new` at `:80`). Its own doc
   says *"Type name for error messages"* (`store/mod.rs:73`) — there is no such error message:
   `WrongTypeError` (`typed.rs:47-53`) is a unit struct with a hard-coded string. Forty-five of
   PN7's 260 lines exist to feed one debug-only panic label, and they have **silently diverged**
   from `KeyType::as_str` on two families — **exactly two, the set is complete**. (§Problem 2,
   hotfix **H2**.)
3. **Proposal 82's step D rewrites the same `PubSubMsg` probe arms PN11 rewrites**, and
   collapsing the six `PubSubMsg` registration variants into **two** (`Register`/`Unregister`,
   each carrying a hoisted `SubKind`) **changes six USDT probe names** unless the collapsed
   variants match on their payload — on a surface `message.rs:1003-1005` explicitly documents as
   byte-stable for downstream consumers. 82's revision 2 now owns this and **chooses to preserve
   the six names** via the payload-dependent `DriveTick` idiom (82 §The message fold, consequence
   1, option (i)). That choice makes `PubSubMsg` **underivable**, which is why revision 2 of this
   proposal excludes it from PN11 and **reverses the recommended land order to 82-first**.
   (§Risks.)

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
node, 0 %-coverage row in every audit report, six documentation rows across four files claiming it
is real) to avoid a dependency edge that is not even new (§PN11: `metrics-derive` is already in
`frogdb-core`'s build graph transitively).

**RULING: Option A′ — delete the crate outright (CT3), and give PN11 a home in `metrics-derive`.**

| Option | Verdict |
|---|---|
| **A. Delete crate + `macro_rules!` for PN7 + drop-or-re-home PN11** | **Adopted, in the "re-home" variant.** PN7 needs no proc-macro crate at all (§Problem 3 — the identical pattern already exists twice in the same module tree). PN11 re-homes to `metrics-derive`. |
| **B. Keep the crate gutted, add the `probe_type_str` derive to it** | **Rejected.** Gutting leaves 15 lines of `Cargo.toml` + a `lib.rs` stub with one derive — a fourth proc-macro crate's worth of ceremony (member entry, lock entry, doc entries, dep edge) for something `metrics-derive` already does. The crate's *name* would then lie: nothing about `frogdb-macros` says "observability strings", and its whole documented identity (`lib.rs:1-59`, 59 lines of `#[derive(Command)]` prose) would have to be deleted anyway. B is A plus a leftover shell. |
| **C. Delete now, recreate if PN11 later** | **Rejected as unnecessary, not as wrong.** C is the right answer *if* `metrics-derive` did not exist. It does. Recreating a proc-macro crate when a live one is one line away is the cost C was trying to avoid, deferred rather than removed. |

The three land in any order **with respect to each other**. **CT3 does not block PN11 and PN11 does
not resurrect CT3.** That is the whole content of the ruling: the tension the brief identified
dissolves the moment the second derive crate is counted. (PN11 does have an ordering preference
against *other* proposals in the round — after 82 and 87 — for a reason unrelated to CT3; §Risks.)

## Summary

Three unrelated-looking items, one shared shape: **`frogdb-core` and its neighbours hand-maintain
lists that a table or a derive should own, while the one crate built to own such tables is dead.**

- **CT3** — delete `frogdb-macros` (447 lines, 0 % covered, zero usages, structurally
  uncompilable against the trait it targets). Pure deletion; the only non-mechanical work is six
  documentation rows, in four files, that assert the crate is live.
- **PN7** — replace 22 hand-written `ValueType`/`DefaultValueType` impls (`store/mod.rs:99-358`,
  260 lines) with a `macro_rules!` table in the defining module. **No proc-macro crate involved.**
  The precedent is not hypothetical: the same module tree already carries *two* macros of exactly
  this shape (`store/typed.rs:222-284`, `types/src/types/mod.rs:71-112`), and the family list is
  currently written out **four** times.
- **PN11** — replace the hand-written `probe_type_str` impls (152 lines, 65 arms, **all 65
  verified to be identity maps** variant-name → string) with one derive. Home:
  `frogdb-metrics-derive`, not `frogdb-macros`. **Scope: 10 of the 11 category enums** —
  `PubSubMsg` opts out, because proposal 82 keeps six payload-dependent probe names there
  (§Risks). That is 54 arms / 134 lines derived, 11 arms / 18 lines left hand-written.

The architecture argument is **locality of the declaration**. Today a family's identity is spread
across four files, and a message variant's probe name is declared 60 lines away from the variant
in the same file with nothing but a compiler exhaustiveness check tying them together. The macros
move each fact to the one place a reader already looks — the type list, and the variant itself.
Interface width is unchanged in every case; only the number of places the same fact is written
goes down.

## Files involved

Verified paths and line counts at `e11bd514`.

| File | Lines | Role |
|---|---|---|
| `frogdb-server/crates/frogdb-macros/src/command.rs` | 370 | **CT3 — deleted.** `derive_command_impl` `:35-88`; the generated `impl Command` `:57-85`. **Zero `FM-` tags.** |
| `frogdb-server/crates/frogdb-macros/src/lib.rs` | 77 | **CT3 — deleted.** 59 lines of doc prose `:1-59` for an unused API; `#[proc_macro_derive(Command)]` `:74-77`. **Zero `FM-` tags.** |
| `frogdb-server/crates/frogdb-macros/Cargo.toml` | 15 | **CT3 — deleted.** `proc-macro = true` `:10`; deps `syn`/`quote`/`proc-macro2` `:13-15` (all three still used by `config-derive` + `metrics-derive`, so nothing is orphaned). |
| `Cargo.toml` | `:28` | **CT3 — one line removed** from `[workspace] members`. No `[workspace.dependencies]` entry exists to remove. **Zero `FM-` tags.** |
| `frogdb-server/crates/server/Cargo.toml` | `:127` | **CT3 — one line removed.** Carries two `FM-CLUSTER-…` comments, at `:40` and `:46`, both annotating `[[test]]` stanzas ~80 lines above the edit. **Zero `FM-` tags in the `[dependencies]` region.** |
| `Cargo.lock` | `:1781-1788`, `:1962` | **CT3 — regenerated**, not hand-edited. Package stanza (`[[package]]` header at `:1781`) + the `frogdb-server` dep entry. |
| `frogdb-server/crates/core/src/store/mod.rs` | 830 | **PN7 primary.** `ValueType` `:72-81`, `DefaultValueType` `:90-93`, the 22 impls `:99-358`, 15 `type_name` bodies. `Store` trait `:393-830` (57 methods) is **out of scope** — see §Follow-ups. **Zero `FM-` tags.** |
| `frogdb-server/crates/core/src/store/typed.rs` | — | **PN7 read-only precedent** + **H2 target.** `macro_rules! typed_family_accessors` `:222-264`, its 14-row table `:266-284`, its optional-slot idiom `$(, $get_or_create:ident )?` `:228`; `WrongTypeError` `:47-53`; the sole `type_name()` consumer `:81-85`. **Zero `FM-` tags.** |
| `frogdb-server/crates/types/src/types/mod.rs` | 1800 | **PN7 read-only precedent.** `macro_rules! impl_value_accessors` `:71-95`, 14-row **four-column** table `:97-112`, hand-written `VectorSet` pair `:114-128`; `KeyType::as_str` `:298-320`. **Owned by proposal 84 at `:322-426`** — adjacent, non-overlapping. **Zero `FM-` tags.** |
| `frogdb-server/crates/core/src/shard/message.rs` | 1446 | **PN11 primary.** `ShardMessage::probe_type_str` `:1000-1026` (hand-written, out of scope); the 11 category impls `:1028-1179`, of which PN11 replaces **10** (`PubSubMsg` `:1040-1057` excluded — §Risks). The **11 `#[derive(Debug)]` attribute lines** at `:209`, `:274`, `:375`, `:397`, `:498`, `:557`, `:718`, `:768`, `:812`, `:858`, `:898` — one above each category enum — each gain `VariantName` (10 of them, if `PubSubMsg` `:274` opts out). `enum ShardMessage` `:151` is **not** in this set: it already has an inherent `probe_type_str`, so deriving there would emit a second one (`E0592`). **Primary file of proposal 82** — see §Risks. **Zero `FM-` tags.** |
| `frogdb-server/crates/metrics-derive/src/lib.rs` | 485 | **PN11 primary (extended).** `MetricLabel` derive `:28-115`; arm generation `:74-76`; default-label rule `:72`. **Zero `FM-` tags.** |
| `frogdb-server/crates/core/Cargo.toml` | `:32-40` | **PN11 — one line added** (`frogdb-metrics-derive.workspace = true`) beside `frogdb-types.workspace = true` `:32`, which **already pulls `metrics-derive` in transitively** (`types/Cargo.toml:13`). |
| `frogdb-server/crates/core/src/shard/event_loop.rs` | — | **Read-only.** Sole production consumer: `msg_kind` `:133`, USDT probe `:135-139`, panic attribution `:163`. **Adjacent to proposal 81's deletion at `:122-124`** — see §Risks. **Zero `FM-` tags.** |
| `frogdb-server/crates/core/src/probes.rs` | `:69-76` | **Read-only.** `fire_shard_message_received`, `usdt-probes`-gated. |
| `scripts/continuation-lock-gate.py` | `:71`, `:80-92`, `:160`, `:176-193` | **Read-only, must stay green.** The one seam lint that parses `message.rs`. §Seam-lint clearance. |
| `website/src/content/docs/architecture/architecture.md` | `:150`, `:182` | **CT3 doc edit.** Asserts `frogdb-macros/ # Proc macros` is a live crate — once in the source-tree listing (`:150`, beside `metrics-derive/` `:151`) and once in the crate-layer table (`:182`). Both also omit `config-derive` — hotfix **H4**. |
| `.claude/skills/db-architect/references/crate-map.md` | `:23`, `:44` | **CT3 doc edit.** Describes the derive as the crate's purpose. `:23` lists the same two-of-three macro crates as `architecture.md:182` — **H4** applies here too. |
| `.claude/skills/doc-sync/references/checkable-assertions.md` | `:32` | **CT3 doc edit.** Row removed. |
| `.claude/skills/doc-sync/references/doc-map.md` | `:29` | **CT3 doc edit.** Row removed. |
| `CHANGELOG.md` | `:101` | **NOT edited.** Historical release note; the crate *was* added. Left alone deliberately. |

## Problem

### 1. `frogdb-macros` is not merely unused — it cannot be used (CT3)

Three independent proofs, in ascending order of finality.

**Zero call sites.** `grep -rn 'frogdb.macros' --include='*.rs' --include='*.toml' --include='*.md'`
over the tree, excluding `.scratch/`, returns **eleven** hits in nine files: the crate's own
`Cargo.toml:2` and its `lib.rs:13` doc example, the workspace member entry (`Cargo.toml:28`), the
unused dep (`server/Cargo.toml:127`), `CHANGELOG.md:101`, and **six documentation rows in four
files** (`architecture.md:150` + `:182`, `crate-map.md:23` + `:44`, `checkable-assertions.md:32`,
`doc-map.md:29`). `Cargo.lock`'s two entries (`:1782`, `:1962`) are *not* among them — the lock is
matched by none of those globs; it is found separately and is regenerated, not edited. **No Rust
file outside the crate names it.**

**Zero coverage.** `.scratch/testing-improvements/audit/coverage-depth-2026-08-07.md:31` records
`frogdb-macros | 0/217 | 0.0% | 0/404 | 0.0%` (the two percentage columns are **lines** and
**regions**, per the header at `:25`). It is **one of eight crates at a flat 0.0 %** in that
table (`:27-34`: `config-derive`, `deb`, `docs-gen`, `frogctl`, `frogdb-macros`, `grafana`,
`helm`, `metrics-derive`) — the class is *proc-macro and build-tooling crates*, which
`llvm-cov` cannot instrument through a normal test run. So the coverage row is **not** evidence
that `frogdb-macros` is uniquely dead; the zero-call-site and cannot-compile proofs carry that
weight alone. What the row *is* good for is the audit-noise argument: `coverage-summary.md:13`
leads its "Lows" list with `frogdb-macros 0.0% (217L)`, and deleting the crate removes 217 lines
that can never move from the denominator of every future report. Note the same table shows
`metrics-derive` at `0/191 | 0.0%` — PN11's proposed home is itself a 0 % row, and PN11 adds
~25 lines to it (see hotfix **H6**).

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
`Cargo.lock` loses one stanza and one dep string; six documentation rows stop lying; every future
coverage and mutation audit stops carrying a permanent 0 % row. **Nothing observable is lost**,
because nothing observable was ever produced. It passes.

### 2. `type_name()` is 45 lines feeding one `debug_assert!`, and it has already drifted (PN7)

`ValueType::type_name()` is declared at `store/mod.rs:73-74` as *"Type name for error messages"*
and implemented 15 times (`:100, 120, 140, 160, 180, 200, 220, 249, 263, 277, 291, 305, 319, 333,
347`). A workspace-wide grep for `type_name()` outside the definitions returns **exactly one
site**:

```rust
// store/typed.rs:81-85, inside `TypedArc::new` (`:80`)
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
**exactly two families — the divergence set was enumerated string-by-string and is complete**:

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
| 1 | `types/src/types/mod.rs:97-112` | `macro_rules! impl_value_accessors` table, **four columns** (`Variant => Type, as_x, as_x_mut;`) | 14 + `VectorSet` hand-written at `:114-128` — **its absence from the table is unexplained, not forced** (see below) |
| 2 | `types/src/types/mod.rs:298-320` | `KeyType::as_str` match | 16 (incl. `None`) |
| 3 | `store/mod.rs:99-358` | **22 hand-written impl blocks** | 15 + 7 |
| 4 | `store/typed.rs:266-284` | `macro_rules! typed_family_accessors` table | 14 |

List 4's absence of JSON is **deliberate, not drift** — JSON commands route through their own
`get_json!`/`get_json_mut!` macros (`commands/src/json/mod.rs:83-115`), which wrap the *generic*
`StoreTypedExt::get_typed::<JsonValue>` with JSON-specific null-return semantics. Recorded here so
a future author does not "fix" it. But it is a fifth idiom for the same set.

**`VectorSet`'s hand-written escape is not caused by the `Box`.** Revision 1 asserted that
`Value::VectorSet(Box<VectorSetValue>)` (`types/mod.rs:64`) cannot fit `impl_value_accessors!`.
That is wrong, and the file disproves it on its own face: the macro body emits
`Value::$variant(v) => Some(v)` (`:78-81`), and the hand-written `as_vectorset`
(`:115-121`) is **`Value::VectorSet(v) => Some(v)` — byte-identical**. `Some(v)` is a coercion
site with a known target type, so `&Box<VectorSetValue>` deref-coerces to `&VectorSetValue`
exactly as it does in the hand-written body. The honest statement is: **`VectorSet`'s absence from
the table is unexplained** — most likely it was added after the macro and nobody folded it in.
Which makes it a *fifteenth* row the table should have, not an exception. (A one-line follow-up,
not claimed here: PN7 does not edit this file.)

The point stands regardless: **two of the four are already `macro_rules!` tables in the exact
shape PN7 proposes**, one of them (`typed_family_accessors`, `typed.rs:222-264`) sitting in the
sibling file of the 260 hand-written lines and covering the same 14 families. That macro carries an
explicit `$( … )?` optional-slot idiom for "families without a default" — `$(, $get_or_create:ident
)?` at `typed.rs:228`, commented *"omitted for no-default families"* — which is the same 7-vs-15
split `DefaultValueType` encodes by hand. (`impl_value_accessors!` has **no** such optional group;
its rows are flat four-token rows. Revision 1 attributed the idiom to the wrong macro, contradicting
its own §PN7 section, which cited `typed.rs:228` correctly.) **The pattern is already established,
in-tree, in this module, by two authors, and `store/mod.rs` is the file that did not adopt it.**

That is the whole PN7 case. It needs no new mechanism and no new crate.

### 4. `probe_type_str`: 65 arms, all identity, with a small non-derivable rim (PN11)

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
`cfg(any(test, feature = "shard-driver"))`). No variant-name derive can express that. The
delegator's full non-derivable inventory is exactly three strings: the two `DriveTick` arms and
`ShardMessage::Shutdown` (`:1023`); every other arm delegates.

`enum ShardMessage` (`:151`) is therefore doubly out of scope: not only is its impl
payload-dependent, it **already has an inherent `probe_type_str`**, so adding
`#[derive(VariantName)]` there would emit a second inherent method of the same name and fail with
`E0592`. The derive attaches to the 11 *category* enums only.

**And one of those 11 must also opt out.** Proposal 82's revision 2 chooses to keep `PubSubMsg`'s
six registration probe names byte-stable by matching on the collapsed variants' `SubKind` payload
— the `DriveTick` idiom again. A payload-dependent string is precisely what this section proves a
name-derive cannot express, so `PubSubMsg` keeps a hand-written impl. An override attribute cannot
rescue it either: the override would have to select a *different* string per `kind` value, which is
the same expressiveness the derive lacks.

PN11's honest scope is therefore **10 category impls — 54 arms, 134 lines** — leaving `PubSubMsg`
(`:1040-1057`, 11 arms) and the delegator (`:1000-1026`, 27 lines) hand-written. If 82 instead
takes its option (ii) and owns the probe rename, `PubSubMsg` becomes derivable and PN11 covers all
11. §Risks settles the order.

The strings are load-bearing on two paths, not one: `event_loop.rs:133` binds `msg_kind` and feeds
it to the USDT probe (`probes.rs:69-76`) *and* to `recover_from_panic`'s panic-site attribution
(`event_loop.rs:163`). `message.rs:1003-1005` states the byte-stability contract explicitly:
*"byte-for-byte identical to the pre-split flat variant names (e.g. `"VllAbort"`), which downstream
USDT probe consumers depend on."* A derive that defaults to the exact variant name makes that
contract **structural** instead of documented.

### 5. `metrics-derive` already contains 90 % of PN11's derive — but not the last 10 %

`#[derive(MetricLabel)]` (`metrics-derive/src/lib.rs:28-115`) generates `pub const fn as_str(&self)
-> &'static str` plus `all_values()`, `all_values_str()`, and `Display`, from an enum's variants,
with `#[label = "…"]` overrides. **Six** enums use it (`types/src/metrics/labels.rs:14`, `:28`,
`:39`, `:56`, `:70`, `:81`). It is live and it is precisely the map PN11 needs — though it is
**not tested**: `metrics-derive` contains no `#[test]`, no `mod tests` and no `tests/` directory
(hotfix **H6**).

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
`VllAbort` to `vllabort` would break the documented byte-stability contract on every arm at once —
and with `metrics-derive` carrying zero tests today, nothing in the crate would catch it.

So PN11 is not "reuse `MetricLabel`" — it is "**add a sibling derive next to `MetricLabel`, in the
crate that already owns this genre**": `#[derive(VariantName)]`, emitting a shape-aware pattern
(`Variant { .. }` / `Variant(..)` / `Variant` per `syn::Fields`), defaulting to the exact variant
name, with an optional `#[variant_name = "…"]` override for future renames that must not move the
probe. Roughly 25 lines beside 115 that already do the hard part. Neither `frogdb-macros` nor
`strum` contributes anything to that.

## Proposed change

### CT3 — delete `frogdb-macros`

`git rm -r frogdb-server/crates/frogdb-macros`; drop `Cargo.toml:28` and
`server/Cargo.toml:127`; regenerate `Cargo.lock`; remove the **six** documentation rows in four
files (`architecture.md:150` + `:182`, `crate-map.md:23` + `:44`, `checkable-assertions.md:32`,
`doc-map.md:29`). Leave `CHANGELOG.md:101`.

**Module/interface:** removes a workspace node whose public interface is one derive with zero
implementors. **Depth:** the crate has negative depth — 447 lines of implementation behind an
interface nobody can call, which is the pathological case. **Leverage:** every future audit
(coverage, mutation, doc-sync, crate-map) stops paying attention to it.

While editing `architecture.md`, note that the crate `config-derive` is missing from **both**
places CT3 already opens — the source-tree listing at `:150-151` (which lists `frogdb-macros/` and
`metrics-derive/` but no `config-derive/`) and the **Macros** row at `:182` (same two names) —
even though `config-derive/Cargo.toml:10` sets `proc-macro = true`. `crate-map.md:23` repeats the
omission. Pre-existing; correcting all three costs one word each and folds into the same edit
(hotfix **H4**).

### PN7 — one `value_types!` table in `store/mod.rs`

A `macro_rules!` in the defining module — no crate, no dependency, no proc-macro:

```rust
value_types! {
    // ty                type_name      as_x              as_x_mut              default
    //                                                                          (omitted ⇒ ValueType only)
    ListValue           { "list",        as_list,          as_list_mut,          Value::list() };
    SetValue            { "set",         as_set,           as_set_mut,           Value::set() };
    HashValue           { "hash",        as_hash,          as_hash_mut,          Value::hash() };
    SortedSetValue      { "sorted set",  as_sorted_set,    as_sorted_set_mut,    Value::sorted_set() };
    StreamValue         { "stream",      as_stream,        as_stream_mut,        Value::stream() };
    StringValue         { "string",      as_string,        as_string_mut,        Value::string(Bytes::new()) };
    JsonValue           { "json",        as_json,          as_json_mut,          Value::json(serde_json::Value::Null) };

    // no parameterless default — ValueType only
    BloomFilterValue    { "bloom",       as_bloom_filter,  as_bloom_filter_mut };
    CuckooFilterValue   { "cuckoo",      as_cuckoo_filter, as_cuckoo_filter_mut };
    TopKValue           { "topk",        as_topk,          as_topk_mut };
    TDigestValue        { "tdigest",     as_tdigest,       as_tdigest_mut };
    CountMinSketchValue { "cms",         as_cms,           as_cms_mut };
    HyperLogLogValue    { "hyperloglog", as_hyperloglog,   as_hyperloglog_mut };
    TimeSeriesValue     { "TSDB-TYPE",   as_timeseries,    as_timeseries_mut };
    VectorSetValue      { "vectorset",   as_vectorset,     as_vectorset_mut };
}
```

**The table carries both accessor idents explicitly.** It must: each impl body is
`value.as_list()` / `value.as_list_mut()` (`store/mod.rs:104-110`), and there is **no way to
build `as_list_mut` from an `as_list` stem in `macro_rules!` without `paste!`** — which is *not*
a workspace dependency (zero hits in any `Cargo.toml`; `paste` appears in `Cargo.lock:3512` as a
transitive of something else, which is not the same as being available to `frogdb-core`). Pulling
it in would be a new third-party dependency on a core crate — the exact objection this proposal
uses to reject `strum` in §Corrections, and it would be incoherent to accept it here. Two columns
cost nothing and match `impl_value_accessors!`'s own four-column rows (`types/mod.rs:97-112`)
exactly. **Match the sibling's shape rather than inventing a terser one** — two macros with the
same job should read the same.

**260 lines → 15 table rows + a ~40-line macro body.** The optional-default slot is the
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

`frogdb-core/Cargo.toml` gains `frogdb-metrics-derive.workspace = true` (one line). **This is not
a new edge in the build graph**: `frogdb-core` already depends on `frogdb-types`
(`core/Cargo.toml:32`), which depends on `frogdb-metrics-derive` (`types/Cargo.toml:13`), so the
proc-macro is already compiled before `frogdb-core` in every build. The declaration makes an
existing transitive available by name; the marginal build cost is ~zero, and there is no cycle
(`metrics-derive` has no internal deps).

`message.rs` gains **10** `#[derive(VariantName)]` attributes — appended to the existing
`#[derive(Debug)]` lines at `:209`, `:375`, `:397`, `:498`, `:557`, `:718`, `:768`, `:812`,
`:858`, `:898` — and loses 134 lines / 54 arms.

Two impls stay hand-written, for the same reason in both cases:

- **The delegator** (`:1000-1026`), whose two `DriveTick` arms and `Shutdown` arm are the three
  strings that are genuinely a *decision* rather than a restatement. The derive removing the
  mechanical arms leaves exactly those visible.
- **`PubSubMsg`** (`:1040-1057`, attribute line `:274`), because proposal 82 keeps six
  payload-dependent probe names there (§Problem 4, §Risks). If 82 takes its option (ii) instead,
  add the eleventh derive and delete 18 more lines; nothing else changes.

**Layering:** is a shard-probe derive at home in a crate named `metrics-derive`? Yes — `msg_kind`
is an observability label on two observability paths (USDT probe, panic-site attribution), which
is the same genre as `MetricLabel`'s Prometheus label values. If a rename is wanted, the honest
one is `frogdb-derive` for both, but that is a rename, not a new crate, and it is not claimed here.

**Deletion test, applied to the derive:** delete `VariantName` and the 134 lines come back, plus
the identity invariant reverts from *guaranteed* to *reviewed*. Those lines are pure restatement
— the compiler already knows every variant's name. **Passes, but by a narrower margin than PN7**,
because §Corrections established that omission is already double-guarded (exhaustive match +
`continuation-lock-gate.py` arm-count pins). PN11 buys the deletion of 134 restated lines and
closes the *wrong-string* hole; it does not close a hole that was open. Rated **S / LATENT**
accordingly, and it is the one of the three a reviewer could reasonably decline.

## Testability improvement

- **CT3:** removes 217 lines that can never be covered from every coverage and mutation report.
  It is honest to say this is **one of eight 0 %-coverage rows**, not the only one
  (`coverage-depth-2026-08-07.md:27-34` — the other seven are proc-macro and build-tooling
  crates that `llvm-cov` cannot instrument through a test run, and all of them are alive). What
  makes this row uniquely worth removing is not the zero, it is that the zero is *permanent by
  construction*: no test can reach a crate with no callers. `coverage-summary.md:13`'s "Lows"
  list currently leads with it.
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
  check on 54 arms into one the compiler enforces on zero; the 11 `PubSubMsg` arms stay a review
  obligation, which is the price of their byte-stable names. The existing
  `keyspace_coordinator.rs:253` panic-message use keeps working unchanged.
- **PN11, mutation-testing effect:** each of the 65 arms is currently an independent mutable
  literal. `cargo mutants` on `frogdb-core` must generate and run a mutant per arm, none of which
  any test kills (nothing asserts probe strings). The derive removes 54 low-value mutants from the
  crate's surface, which raises the signal in every subsequent `frogdb-core` mutation run.
- **PN11, in `metrics-derive`:** the crate has **no tests at all** — no `#[test]`, no `mod tests`,
  no `tests/` directory. The unit test PN11 must carry (§Risks) would be its first, and would
  incidentally pin `MetricLabel`'s own untested default rule. See **H6**.

## Risks and scope boundaries vs sibling proposals

### Hard overlap — proposal 82 (step D, the `PubSubMsg` fold) — **order reversed in revision 2**

82 names `message.rs` a **Primary** file and its step D collapses the six `PubSubMsg` registration
variants (`:275-372`) into **two** — `Register { kind: SubKind, … }` / `Unregister { kind: SubKind,
… }` (82 §The message fold). `PubSubMsg::probe_type_str` (`:1040-1057`) is the 11-arm impl PN11
would otherwise replace. **They edit the same impl block.**

Revision 1 recommended **PN11 first**. That was backwards, and 82's revision 2 (`8409c9c6`) makes
the reason explicit:

- Under **PN11-first**, `PubSubMsg`'s arms are generated from variant names. 82's fold then
  silently renames **six** documented-byte-stable probe names (`Subscribe`, `Unsubscribe`,
  `PSubscribe`, `PUnsubscribe`, `ShardedSubscribe`, `ShardedUnsubscribe` → `Register`,
  `Unregister`) with **zero diff in any `probe_type_str` body** — nothing for a reviewer to catch,
  because the arms no longer exist in the source. That is strictly worse than the status quo,
  where the rename would appear as six deleted lines.
- **An override attribute cannot rescue it.** `#[variant_name = "…"]` attaches one string to one
  variant; the correct name here depends on the `kind` *payload*, which §Problem 4 proves a
  name-derive cannot express (the `DriveTick` argument, `message.rs:1019-1022`).

So `PubSubMsg` must opt out of `VariantName` and keep a hand-written payload-dependent impl —
**under either order**. 82's revision 2 chooses exactly this (its consequence-1 option (i),
recommended there, with a `vs. proposal 85` section recording that the trade "costs PN11 one
enum"). This proposal **agrees and adopts it**: probe-name stability is an external contract,
derive coverage is internal tidiness.

Sequencing, in preference order:

1. **82 first.** PN11 then applies to a `PubSubMsg`-free set of 10 enums with no ambiguity about
   which arms are payload-dependent, and 82 has already updated the `continuation-lock-gate.py`
   pin. **Recommended.**
2. **PN11 first, with `PubSubMsg` excluded from the start.** Equivalent in outcome; the exclusion
   must be deliberate and commented at the enum, or the hazard above returns on rebase.
3. **PN11 first, deriving all 11.** **Rejected** — it converts 82's fold into a silent six-name
   probe rename.

Either way the enum *declarations* (82's territory) and the *impl blocks* (PN11's) are 700 lines
apart; PN11's only touch inside 82's region is **one attribute-line edit per enum**, which rebases
trivially.

### Same hazard, second instance — proposal 87 (`DebugIntrospectionMsg`)

87 (`.scratch/arch-deepening/proposals/87-debug-probe-table.md:469`) folds
`DebugIntrospectionMsg` from 6 variants to 2 and **states the same requirement from its own side**:
the five probe strings `GetLockTableInfo`, `GetWaitQueueInfo`, `GetWaitQueueLog`, `MemoryCheck`,
`ExpiryIndexCheck` are byte-stable, so after the fold `probe_type_str` "must match on the inner
`ShardProbe`". That is a payload-dependent string — so if 87 lands, **`DebugIntrospectionMsg` opts
out of `VariantName` too**, on identical reasoning to `PubSubMsg`, and PN11 covers 9 enums rather
than 10. Same preference order applies: 87 before PN11.

This is not a coincidence; it is the shape of the whole round. **Every proposal that folds a
message enum while preserving probe names converts one enum from derivable to hand-written.** PN11
should therefore be sequenced **last** among the `message.rs` proposals, and its value stated as
"whatever is still a pure identity map when it lands" — a floor of 9 enums, not a fixed 11.

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

- **80** (`Response`/`WireResponse` fold) works in `protocol/src/response.rs` and states — at
  `:666` after its own revision 2 — that it does not touch `frogdb-types/src/types/mod.rs`. No
  file in common with 85.
- **83** (lazy-expiry authority) touches `store/mod.rs`, but at `:522-573` — four defaulted `Store`
  trait drain methods. PN7's region is `:99-358` and the trait declarations at `:72-93`. **164
  lines apart, no overlap.** Both edit the same file, so land order matters for rebase hygiene
  only.

### Substantive risks

- **PN7 macro opacity.** `rustdoc` output for macro-generated impls is thinner than for
  hand-written ones. Mitigation: the same `#[doc = concat!(…)]` idiom both sibling macros already
  use (`typed.rs:235`, `types/mod.rs:77`) — copy it, do not invent.
- **PN7 table-shape mismatch with its sibling.** `impl_value_accessors!` covers 14 families and
  hand-writes `VectorSet` (`types/mod.rs:114-128`); PN7's table covers 15, `VectorSetValue`
  included. The author must not assume the two tables are row-for-row identical. **The reason is
  not the `Box`** — revision 1 claimed that and it is disproved by the file itself (§Problem 3):
  `Some(v)` deref-coerces `&Box<VectorSetValue>` to `&VectorSetValue`, and the hand-written
  `as_vectorset` body is byte-identical to what the macro would emit. The sibling's omission is
  simply unexplained, so PN7 must not copy it.
- **PN11 default-string regression — the sharpest risk in the proposal.** If `VariantName` is
  implemented by copying `MetricLabel` and the `.to_lowercase()` at `lib.rs:72` is copied with it,
  every derived probe name breaks silently in the same commit. There is nothing in the way:
  **`metrics-derive` has zero tests of any kind** — no `#[test]`, no `mod tests`, no `tests/`
  directory — so the copy-paste hazard is unguarded at the crate, and the consumers would not
  catch it either (`message.rs` has no probe-string assertion; §Follow-ups). Mitigation, and it is
  **mandatory, not optional**: a unit test in `metrics-derive` asserting `VariantName`'s default is
  the exact ident. It would be the crate's **first test**, and it should pin `MetricLabel`'s
  lowercasing rule in the same file — today that rule is asserted only indirectly, by the 6
  consumer enums' explicit `#[label]` attributes, which is how the `"not_available"` →
  `"notavailable"` bug its own comment records (`lib.rs:50-53`) got in. See **H6**.
- **PN11 dependency edge — smaller than revision 1 claimed.** `frogdb-core` gains a *declared*
  proc-macro dependency, but not a new *graph* edge: `frogdb-core` → `frogdb-types`
  (`core/Cargo.toml:32`) → `frogdb-metrics-derive` (`types/Cargo.toml:13`) already forces
  `metrics-derive` to compile before `frogdb-core` in every build today. The marginal cost is a
  name in a manifest, and there is no cycle. Revision 1 called this "the one real cost of PN11";
  that was overstated and is withdrawn — the real cost of PN11 is the untested-derive risk above.
- **Not a risk: `syn`/`quote`/`proc-macro2` orphaning.** CT3 removes `frogdb-macros`'s three deps,
  but `config-derive` and `metrics-derive` both still declare them. `Cargo.lock` keeps every
  stanza.

## Effort

| Item | Effort | LIVE/LATENT | Blocked by |
|---|---|---|---|
| **CT3** — delete `frogdb-macros` | **S** | LATENT (dead code) | nothing |
| **PN7** — `value_types!` table | **S** | LATENT | nothing (83 shares the file; rebase only) |
| **PN11** — `VariantName` derive | **S** | LATENT | not blocked, but **prefer *after* 82 and 87** — each fold they land converts one enum from derivable to hand-written (§Risks) |

Combined **S**. No item requires a workspace rebuild to *design*; each requires
`just check frogdb-core` (PN7, PN11) or `just check` (CT3, because it changes the workspace member
list) to land. PN11 additionally requires `just test frogdb-metrics-derive` to run its new — and
the crate's only — test.

## Hotfix candidates

Independently landable, smallest first. Standing policy observed: **no security findings appear
below** — none were encountered; had any been, they would be classification-only, filed and
parked, never proposed as fixes.

| # | Change | Files | LIVE/LATENT | Claimed? |
|---|---|---|---|---|
| **H1** | Fix the `ValueType::type_name` doc lie: *"Type name for error messages"* → it is a `debug_assert!` label; `WrongTypeError` carries no type name. One doc line. | `store/mod.rs:73` | LATENT | **CLAIMED** |
| **H2** | Record the `type_name` ↔ `KeyType::as_str` divergence at both declarations, so the next reader of either does not assume they agree. The divergence set is **complete at exactly two** — `"sorted set"` (`store/mod.rs:161`) vs `"zset"` (`types/mod.rs:307`), and `"json"` (`store/mod.rs:221`) vs `"ReJSON-RL"` (`types/mod.rs:312`); all other families agree string-for-string. Two doc comments, no behaviour change. | `store/mod.rs:73-74`, `types/mod.rs:299` | LATENT | **CLAIMED** |
| **H3** | Note at `typed_family_accessors!` that JSON is deliberately absent because `commands/src/json/mod.rs:83-115` owns its own null-returning accessors. Prevents a future "fix" that adds four dead methods. | `store/typed.rs:265` | LATENT | **CLAIMED** |
| **H4** | Add `frogdb-config-derive` everywhere the macro crates are enumerated: the source-tree listing `architecture.md:150-151` **and** the **Macros** table row `architecture.md:182` **and** `crate-map.md:23` — all three list two of the three `proc-macro = true` crates (`config-derive/Cargo.toml:10` sets it). All three lines are already opened by CT3, so this is three words in the same edit. | `website/.../architecture.md:150-151`, `:182`, `crate-map.md:23` | LATENT | **CLAIMED** (with CT3) |
| **H5** | Normalise `frogdb-metrics-derive`'s two consumers: `types/Cargo.toml:13` uses `.workspace = true`, `telemetry/Cargo.toml:18` uses a bare `{ path = "../metrics-derive" }`. Same inconsistency class as the dead `frogdb-macros` dep CT3 removes. One line. | `telemetry/Cargo.toml:18` | LATENT | **NOT CLAIMED** — cosmetic, and it touches a crate no other part of 85 opens. Offered to whoever is next in `frogdb-telemetry`. |
| **H6** | Give `metrics-derive` **its first test**: the crate contains **no `#[test]`, no `mod tests`, and no `tests/` directory** — zero tests of any kind — and is itself a 0 %-coverage row (`coverage-depth-2026-08-07.md:34`, `0/191`). Pin `MetricLabel`'s default-label rule (lowercased ident); today it is asserted only indirectly, by the 6 consumer enums that all override it, which is how the `"not_available"` → `"notavailable"` bug its own comment (`lib.rs:50-53`) got in. The stake for PN11 is concrete: the `.to_lowercase()` at `lib.rs:72` is one copy-paste away from breaking **all 65 probe names in a single commit** with nothing to catch it. | `metrics-derive/src/lib.rs` (tests) | LATENT | **NOT CLAIMED as standalone** — it is a **prerequisite** for PN11 (§Risks) and lands with it. Worth landing on its own if PN11 is declined. |

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

  Incidental confirmation: the gate's eleven pinned counts (`:80-92`) sum to **65**, exactly the
  probe-arm count in the eleven category impls. The two surfaces are already in lockstep, which is
  how §Corrections established that a *missing* arm cannot land silently.

  **One qualification, raised by 82 and accepted:** "PN11 leaves the gate untouched" is a claim
  about *PN11's own diff*, not about the file staying green through the round. 82's step D changes
  `PubSubMsg`'s variant count 11 → 7 and **must update the `dispatch_pubsub.rs` pin in the same
  commit** (`:81`); after that the sum is 61, not 65. PN11 still adds nothing to the gate's input
  either way — an attribute line is invisible to `VARIANT`/`enum_variants()` — but the "65" figure
  above is a pre-82 reading and should not be re-asserted after 82 lands.

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
- **Fold `VectorSetValue` into `impl_value_accessors!` (XS).** `types/mod.rs:114-128` hand-writes
  a pair the macro would emit byte-identically (§Problem 3). One row added, 15 lines deleted. Not
  claimed — PN7 does not edit that file, and proposal 84 is working 200 lines below.
- **`frogdb-derive` consolidation (S).** If `VariantName` lands in `metrics-derive`, the crate
  holds two unrelated-genre derives and its name understates it. A rename to `frogdb-derive`
  covering both — and possibly absorbing `config-derive`, leaving the workspace with **one**
  proc-macro crate instead of the two CT3 leaves behind — is the natural end state of CT3's thesis.
  Not claimed; it is a rename with a wide `use` blast radius and deserves its own proposal.
- **Probe-name byte-stability test (S).** Nothing in the tree asserts the documented contract at
  `message.rs:1003-1005`. After PN11 the contract is structural for the 54 derived names, but the
  hand-written remainder is unasserted: the three delegator strings (`DriveExpiryTick`,
  `DriveWaiterTimeoutTick`, `Shutdown`) plus `PubSubMsg`'s 11 — and those eleven are precisely the
  ones 82's fold puts at risk, so they are the *most* worth pinning, not the least. A short
  table-driven test would cover all fourteen. Deliberately not folded into PN11, which is meant to
  stay a pure refactor; **recommended as a companion to 82's step D instead.**

## Revision-2 changes

Adversarial review returned **AMEND**: the Option A′ ruling was confirmed on every leg (zero derive
usage; `spec()` has no default body → `E0046`; the `E0533` struct-variant failure reproduced; the
continuation-lock gate genuinely forces `#[derive]` over `macro_rules!`; all 65 identity arms
byte-verified; the non-derivable inventory complete at `DriveTick` ×2 + `Shutdown`). Every
amendment below was re-verified against the tree before being applied.

**Applied — changes the recommendation:**

| # | Was (rev 1) | Now (rev 2) |
|---|---|---|
| 1 | "the **only** crate at a flat zero on **region and function** coverage" | **False on both halves.** Eight crates sit at 0.0 % (`coverage-depth-2026-08-07.md:27-34`) and the columns are lines/regions, not region/function (header `:25`). Restated as one of eight, class named (proc-macro/tooling), argument re-grounded on *permanence* rather than uniqueness. `metrics-derive` — PN11's home — is itself a 0 % row (`:34`). §Problem 1, §Testability. |
| 2 | "**PN11 first** is the recommended order" against 82 | **Reversed to 82-first.** 82 folds six variants into **two** (not one), and under PN11-first that fold silently renames six byte-stable probe names with **zero diff in any probe impl**. An override attribute cannot express a payload-dependent name. `PubSubMsg` now **opts out of the derive** under either order, aligned with 82 rev 2's option (i) and its `vs. proposal 85` section. §Corrections finding 3, §Problem 4, §Risks. |
| 3 | Files table listed **12** enum declaration lines, including `:151` | **11 category enums**, and the edit targets are the **`#[derive(Debug)]` attribute lines** `:209 :274 :375 :397 :498 :557 :718 :768 :812 :858 :898`, not the `pub enum` lines. `enum ShardMessage` `:151` excluded — it already has an inherent `probe_type_str`, so a derive there is `E0592`. |
| 4 | PN11 scope "11 impls, 152 lines, 65 arms" | **10 impls, 134 lines, 54 arms** (floor of **9** if 87 lands too — same hazard, §Risks). Propagated through §Summary, §Proposed change, §Testability, §Follow-ups. |

**Applied — corrections that do not move the ruling:**

- "16 label enums" (stated 3×) → **6** (`labels.rs:14 :28 :39 :56 :70 :81`; 17 `#[label]` overrides).
- §Problem 3 attributed the `$( … )?` optional-slot idiom to `impl_value_accessors` — it belongs to
  `typed_family_accessors` (`typed.rs:228`), as §PN7 already said correctly. Self-contradiction
  removed.
- The `Box` explanation for `VectorSet`'s absence from `impl_value_accessors` is **disproved by the
  file**: the hand-written `as_vectorset` body (`types/mod.rs:115-121`) is byte-identical to what
  the macro emits, because `Some(v)` is a coercion site. Restated as *unexplained, not
  `Box`-caused*, and filed as an XS follow-up.
- Grep arithmetic: **11** hits in 9 files, not "nine hits ... two lock entries" (`Cargo.lock` is
  matched by none of the globs). Doc rows standardised at **six rows in four files** (rev 1 said
  "five" and "four" in different places).
- Line-cite drift repaired: `proc-macro = true` `:9`→`:10`; deps `:12-14`→`:13-15`;
  `derive_command_impl` `:30-88`→`:35-88`; `Cargo.lock` stanza `:1782-`→`:1781-1788`; the
  `debug_assert!` `:80-85`→`:81-85`; sibling workspace deps "14 at `:110-126`"→"15 at `:112-126`";
  proposal 80's disclaimer `:584`→`:666`.
- Header SHA: rev 1's `61156aff` is stale, but benignly — the diff to HEAD `e11bd514` is
  code-empty (proposals only). Restated with both SHAs and the evidence.
- PN11's "one real cost — a new link edge `core`→`metrics-derive`" **withdrawn**: the edge already
  exists transitively (`core/Cargo.toml:32` → `types/Cargo.toml:13`). The real cost is the
  untested-derive risk.
- PN7's sketch as printed was not implementable — a single accessor-stem column cannot yield
  `as_list_mut` without `paste!`, which is **not** a workspace dependency (lock-only transitive),
  i.e. the same third-party-dep objection used to reject `strum`. Replaced with the honest
  five-column table (`as_x` and `as_x_mut` both spelled out), matching `impl_value_accessors!`.
  "~20 table rows" → **15**.

**Refuted — review findings not applied, with evidence:**

- The review gave the `metrics-derive` label-enum count as *"6 enums, 20 `#[label]` attrs"*. The
  enum count is right; the attribute count is **17** (`grep -o '#\[label' labels.rs | wc -l`). 17
  is used.
- The review placed the sole `type_name()` consumer at `typed.rs:80-84`. The `debug_assert!` call
  spans **`:81-85`** (`:80` is `fn new`, `:85` is the closing `);`). `:81-85` is used.
- The review named 82's new payload type `SubscriptionKind`. 82's revision 2 explicitly **rejected**
  inventing that enum and instead **hoists the existing `SubKind`** from
  `server/src/connection/state.rs:291` (82 §Corrections B2). `SubKind` is used.

**Hotfix list after revision (unchanged in number, two widened):**

| # | Status |
|---|---|
| **H1** | CLAIMED — unchanged. |
| **H2** | CLAIMED — **strengthened**: divergence set now asserted **complete at exactly two**, with both pairs cited on both sides. |
| **H3** | CLAIMED — unchanged. |
| **H4** | CLAIMED with CT3 — **widened** from one line to three: `architecture.md:150-151` (tree listing) **and** `:182` (table row) **and** `crate-map.md:23` all omit `config-derive`. |
| **H5** | NOT CLAIMED — unchanged, offered to `frogdb-telemetry`. |
| **H6** | Prerequisite for PN11 — **strengthened**: `metrics-derive` has **zero tests of any kind**, is itself a 0 %-coverage row, and one copied `.to_lowercase()` would break all 65 probe names in a single commit. PN11's test would be the crate's first. |

**Security:** none. No security-classified finding arose in this revision; had one, it would be
filed and parked per standing policy, never proposed as a fix here.
