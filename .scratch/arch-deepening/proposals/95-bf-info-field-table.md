# Proposal 95 — `BF_INFO_FIELDS`: one table for the only `*.INFO` reply that answers "which fields exist" twice

Round 38 · lane: commands + types · effort **S** · **no locked crate edited** · **zero `FM-` tags
in any edited file** (verified: `grep -rn "FM-" frogdb-server/crates/commands/src` returns nothing)

**Verified at HEAD `8a17065247c8935e6476fbffc92a5e94a1b20854`; re-verified line-by-line at
`dd840ca3` (rev 2, after adversarial review)** — worktree `arch-round-38-99`, branch
`worktree-arch-round-38-99`. Every file:line, count, and type below was re-derived at these SHAs.
Nothing is inherited from the lane brief. Working tree carries only proposal-file churn
(`91`, `92`, `93`, `97`); **no code file in this set is dirty at either SHA**.

**Rev 2 changes** (all from review, each re-verified against the tree before applying): the
"no `FT.INFO` exists" claim is **withdrawn — `FT.INFO` does exist** and the census is corrected
(§Boundary); `commands/Cargo.toml` citations corrected (file is **60** lines, not 78); the
line-delta estimate corrected from "+2" to **≈ +21** (effort still **S**); **H1 now mandates a
pairwise-distinct fixture** because both existing fixtures have colliding field values that would
hide a swap; ~10 file:line citations retightened; the `TDIGEST.INFO` `Observations` gap promoted
out of the appendix into §Adjacent defects.

**The brief's headline does not survive contact with the tree, and this proposal says so before it
proposes anything.** The briefed refactor was "per-family field table for five `*.INFO`
commands, killing five drift-prone array/match pairs and ~207 static-name allocations." At this
SHA there is **one** such pair, not five, and there are **zero** allocations to kill — `bytes`
resolves `Bytes::from(<string literal>)` to `Bytes::from_static` by trait selection. Applying the
briefed change to all five families would add a fn-pointer indirection to four commands that have
nothing to unify, in exchange for no allocation saving. That version is **refuted and not
proposed.** What remains is real but small: `BF.INFO` alone, plus a test that is worth landing
whether or not the refactor does.

## Corrections to the lane brief

| Brief claim | Correction at `8a170652` |
|---|---|
| "each has a flat all-fields array **AND** a per-field match arm — drift-prone pairs" (×5) | **Refuted for 4 of 5.** Only `BF.INFO` accepts a field selector (`Arity::Range { min: 1, max: 2 }`, `bloom.rs:393`) and therefore only `BF.INFO` has a second site. `CF.INFO` (`cuckoo.rs:507`), `CMS.INFO` (`cms.rs:401`), `TOPK.INFO` (`topk.rs:390`), `TDIGEST.INFO` (`tdigest.rs:605`) are all **`Arity::Fixed(1)`** — key only, one flat `Response::Array(vec![…])`, no match, no second home. This matches upstream RedisBloom, where only `BF.INFO` takes an optional single-field argument. |
| "~207 `Bytes::from("literal")` allocations for static field names" | **Refuted twice over.** (a) The number is not INFO-scoped: `Bytes::from("` occurs **214 times on 207 lines** across `commands/src` (`grep -rno` = 214, `grep -rn` = 207 — the brief's 207 is a *line* count, not an occurrence count), of which **28** are `*.INFO` field labels (BF 5, CF 8, CMS 3, TOPK 4, TDIGEST 8) and 3 more in `tdigest.rs:29,32,34` are `nan`/`inf`/`-inf`. (b) **None of them allocate.** `bytes 1.11.1` (`Cargo.lock:615-616`) defines `impl From<&'static str> for Bytes { fn from(slice) -> Bytes { Bytes::from_static(slice.as_bytes()) } }` (`bytes-1.11.1/src/bytes.rs:954-958`), and `from_static` (`:167-174`) is a `const fn` that stores the pointer against `STATIC_VTABLE`. The **only** `&str`-shaped `From` impl for `Bytes` is the `'static` one (full list: `:948, :954, :960, :995, :1026, :1048`) — a non-`'static` `&str` does not compile, so every one of these 214 occurrences is already zero-allocation. Converting them to `Bytes::from_static` is a **legibility** change with **no runtime effect**. |
| "per-family `const FIELDS: &[(&str, fn(&T) -> Response)]`" | **Mechanism is sound** (compile-verified below), **applicability is one family.** The tuple shape is also insufficient for `BF.INFO`: the reply label (`"Number of items inserted"`) and the selector token (`"ITEMS"`) are *different strings for the same datum*, and unifying them is the entire point. A 3-field struct is required. |
| "S-M effort. Latent." | **S**, and *latent* is right but for the reachability reason, not the severity reason: `bloom`/`cuckoo`/`cms`/`topk`/`tdigest` are individually-gated cargo features (`commands/Cargo.toml:21`, `:22`, `:23`, `:24`, `:25` — the family block is `:19-30`), absent from `default` (`:15`), present only under the `full` aggregate (`:31-45`) — the same gating proposal 89 documents. |
| Implied: "the five families share a duplication problem" | **They share a *shape*, not a problem.** **Seven** `*.INFO` producers in `commands/src` build an alternating label/value `Response::Array` — the five probabilistic families plus `VINFO` and `TS.INFO` — and an **eighth** lives outside the crate (`FT.INFO`, `server/src/commands/search.rs:197`; §Boundary). Three of the seven in-crate producers have structure the briefed table cannot express (§4), and the eighth is nested label/value sub-arrays. Shape similarity is not leverage. |

## Summary

`BF.INFO` answers the question *"which fields does a bloom filter report, and how is each one
computed?"* in two places, eight lines apart, in the same function:

- the all-fields arm, `bloom.rs:415-427` — five `(label, accessor)` pairs;
- the selector arm, `bloom.rs:428-445` (the `match` itself `:435-444`) — the same five accessors
  again, behind five *different* string keys.

Nothing in the code ties `"Capacity"` to `"CAPACITY"`, or `"Number of items inserted"` to
`"ITEMS"`. A sixth field added to the array compiles fine with no matching arm; a changed accessor
in one arm compiles fine while the other keeps the old one. **There is no drift today** — I
checked all five, accessor-for-accessor, and they agree (§Problem 1) — but there is also no
mechanism preventing it, and the regression suite exercises only 3 of the 5 selectors and never
compares a selector's answer to the array's (§Problem 3).

The change is one `const BF_INFO_FIELDS: &[BfInfoField]` in `bloom.rs`, carrying
`(label, selector, fn(&BloomFilterValue) -> Response)`. Both arms walk it: the all-fields arm
`flat_map`s it, the selector arm `find`s in it. The table becomes the single authority; drift
becomes unrepresentable rather than merely absent.

**Scope is deliberately one command.** `CF.INFO`, `CMS.INFO`, `TOPK.INFO`, `TDIGEST.INFO`,
`VINFO`, and `TS.INFO` keep their `vec![]` literals (and `FT.INFO`, in another crate entirely,
keeps its nested builder), and §Risks states why converting them would be a regression in clarity.

## Files involved

Line counts at `8a170652`. All paths under `frogdb-server/crates/` unless noted.

| Path | Lines | Role in this change |
|---|---|---|
| `commands/src/bloom.rs` | 758 | **The only primary.** `BfInfo` `:387`; spec `:390-408` (`Arity::Range` `:393`, `KeySpec::First` `:395`); `execute` `:409-451`. Adds `struct BfInfoField` + `const BF_INFO_FIELDS` + its doc comment (~26 lines) near `:385`; the all-fields arm `:415-427` (13 lines) and the selector `match` `:435-444` (10 lines) collapse to a 10-line `flat_map` and a 7-line `find`. Net ≈ **+21 lines**, one authority instead of two. **Zero `FM-` tags.** |
| `redis-regression/tests/bloom_regression.rs` | 715 | **Primary (tests).** `bf_info_all_fields :165-190` pins labels **by index** — `items[0]="Capacity" … items[8]="Expansion rate"` (`:182-186`), `items.len()==10` `:179`. `bf_info_specific_field :193-211` exercises **only** `CAPACITY :202`, `ITEMS :206`, `FILTERS :209`, and **its fixture has colliding values** (`FILTERS == ITEMS == 1`) — see §H1. Gains the equivalence test (§Testability), which is the H1 hotfix and lands independently. |
| `commands/src/cuckoo.rs` | 839 | **Read-only evidence.** `CfInfo :501`, spec `Arity::Fixed(1)` `:507`, single flat array `:527-544` (8 labels at `:528,530,532,534,536,538,540,542`). **No second site — nothing to unify.** |
| `commands/src/cms.rs` | 434 | **Read-only evidence.** `CmsInfo :395`, `Arity::Fixed(1)` `:401`, array `:425-432` (3 labels `:426,428,430`). |
| `commands/src/topk.rs` | 425 | **Read-only evidence.** `TopkInfo :384`, `Arity::Fixed(1)` `:390`, array `:414-423` (4 labels `:415,417,419,421`); `decay` is a bulk string via `format!` `:422`. |
| `commands/src/tdigest.rs` | 689 | **Read-only evidence.** `TdInfo :599`, `Arity::Fixed(1)` `:605`, array `:626-643` (8 labels `:627…641`). `"Total compressions"` `:639-640` has **no accessor at all** — a hardcoded `Response::Integer(0)` with the comment "Not tracked, matches Redis placeholder". §4. |
| `commands/src/vectorset/vinfo.rs` | 77 | **Read-only — boundary case.** `VinfoCommand :11`, base array `:55-67` (6 labels `:56,58,60,62,64,66`), then a **conditional** field `:70-73`: `projection-input-dim` is pushed only when `info.original_dim > 0`. A flat `&[Field]` table cannot express "sometimes". §4, §Risks. |
| `commands/src/timeseries.rs` | 1368 | **Read-only — boundary case.** `TsInfoCommand :935`, `Arity::Range { min: 1, max: 2 }` `:941`. Base array `:962-979` (8 labels), then `labels` `:981-994` (a nested `Response::Array` built from the value's own map) and `rules` `:996-1010` (nested arrays of triples). Not tabular. **Adjacent defect:** `execute` `:957` reads `args[0]` only (`:958`) — `args[1]` is accepted by the arity and then **silently ignored**, so `TS.INFO key JUNK` returns full info where Redis errors, *and* `TS.INFO key DEBUG` returns the ordinary reply rather than upstream's extended one. §Adjacent defects. |
| `commands/src/lib.rs` | — | **Read-only.** Per-family feature gates `:27-60` (`bloom :27-28`, `cms :29-30`, `cuckoo :31-32`, `tdigest :52-53`, `timeseries :54-55`, `topk :56-57`, `vectorset :59-60`) — each family is separately compiled, so a *per-family* table needs no `cfg` care and a *shared cross-family* helper would. |
| `commands/Cargo.toml` | 60 | **Read-only.** Per-family features `:19-30` (`bloom :21`, `cuckoo :22`, `cms :23`, `topk :24`, `tdigest :25`); the `full` aggregate that turns them on `:31-45`. **Untouched by this change** (contrast proposal 89, which edits it). |
| `core/src/store/typed.rs` | 584 | **Read-only — the precedent and the type.** `typed_family_accessors!` (definition `:222-264`, invocation `:266-284`) is *already* the "one table, many families" pattern in this codebase, and it is a macro, not fn pointers, because it generates *signatures*. `BloomFilterValue { get_bloom, … }` `:276`. `get_*` returns `Result<Option<TypedArc<$ty>>, WrongTypeError>` `:236-238`; `TypedArc` derefs, so `fn(&BloomFilterValue) -> Response` is callable on the binding as written. |
| `types/src/bloom.rs` | 361 | **Read-only.** `pub struct BloomFilterValue` `:128` — the `T` in the table's fn pointer, re-exported at `types/src/lib.rs:35` and reachable from `commands` through `core/src/lib.rs:7`'s glob (same reach proposal 89 documents). |
| `server/src/info/mod.rs` | 1113 | **Read-only — the in-tree precedent.** `pub trait InfoSection` `:199-205` (`name() -> &'static str`, `render()`), `struct InfoBuilder` `:208-210`, its `impl` `:212`, `standard()` `:214-218` = "every section FrogDB serves, **in canonical order**". The server's own `INFO` had exactly this drift and it was closed with a registry. The datatype `*.INFO` commands never got the same treatment. |
| `Justfile` | — | **Read-only — gate check.** `lint-gates :329` lists 14 gates. `lint-info-seam :423-441` is scoped to three server files (`server/src/commands/info.rs`, `connection/scatter.rs`, `connection/info_handler.rs`) and forbids `.replace(…)`/`.replace_range(` — **it does not cover `commands/src`, and this change introduces neither construct.** `lint-format-float :1249-1269` pins one `fn format_float`, in `protocol/src/format.rs`; this change defines none. **No gate is affected.** |
| `website/src/data/commands.json` | — | **Read-only — generated, must not be hand-edited** (`_generated.warning`; source = `commands/src/lib.rs` + `server/src/server/register.rs`; regenerate via `just docs-gen`). The `BF.INFO` entry carries `name`/`arity`/`flags`/`family`/`execution_strategy`/`full`/`is_stub` and **no reply-field list**. **There is no third *home* for the field names** — confirmed by grepping `"Number of filters"`, `"Expansion rate"`, `"Merged nodes"`, `"Number of buckets"`, `"Number of items inserted"` across `*.rs`/`*.json`/`*.md`/`*.mdx`/`*.ts`/`*.astro`. Every hit is the single producer in `commands/src`, the regression-test oracle, or (for `"Number of items inserted"`) a **doc-comment occurrence at `types/src/cuckoo.rs:35`** — English prose describing a field, not a wire-string source. So: more than two *occurrences*, still exactly two *homes*. |
| `server/src/commands/search.rs` | 1335 | **Read-only — the seventh/eighth `*.INFO`, added in rev 2.** `FtInfoCommand :197`, `name: "FT.INFO"` `:202`, **`Arity::Fixed(1)` `:203`**, `ExecutionStrategy::ServerWide(ServerWideOp::FtInfo)` `:214`; registered `server/src/server/register.rs:206`; the reply is built in `core/src/shard/search/index_mgmt.rs:29` (`execute_ft_info`) as **nested label/value sub-arrays**, already `Bytes::from_static` throughout; oracle `redis-regression/tests/search_regression.rs:288`. **Declined for the same reason as the other four** — no selector, so no second home — and it *strengthens* the allocation refutation. |

## The compile check (make-or-break, done first)

The table's viability rests on one question the brief assumes: **can a non-capturing closure
coerce to a `fn` pointer inside a `const` item?** That was verified by compiling *and running* a
throwaway program on the workspace's pinned toolchain — **rustc 1.92.0 (ded5c06cf 2025-12-08)**
(`rust-toolchain.toml`), `--edition 2024` (root `Cargo.toml:44`), written outside the repo in the
job tmp dir:

```
[Bulk("Capacity"), Integer(100), Bulk("Number of items inserted"), Integer(7), Bulk("Decay"), Bulk("0.9")]
Some(Integer(7))
static label len 8
```

Confirmed by that run: the `const` initializer accepts closure→`fn` coercion; a table entry whose
accessor *allocates at call time* (the `format!` case, needed for a `decay`-shaped float field) is
fine because only the pointer is const; `iter().flat_map(…)` reproduces the alternating
label/value array; `iter().find(|f| f.selector == …)` serves the single-field path; and
`FIELDS[0].label` is a genuine `&'static str`, so `Bytes::from_static(label.as_bytes())` is
available without a lifetime dance. **No blocker.**

## Problem

### 1. Two homes for one fact, and no mechanism keeping them equal

`bloom.rs:415-427`, the all-fields arm:

```rust
Response::bulk(Bytes::from("Capacity")),                 // :417
Response::Integer(bf.capacity() as i64),
Response::bulk(Bytes::from("Size")),                     // :419
Response::Integer(bf.memory_size() as i64),
Response::bulk(Bytes::from("Number of filters")),        // :421
Response::Integer(bf.num_layers() as i64),
Response::bulk(Bytes::from("Number of items inserted")), // :423
Response::Integer(bf.count() as i64),
Response::bulk(Bytes::from("Expansion rate")),           // :425
Response::Integer(bf.expansion() as i64),
```

`bloom.rs:435-444`, the selector arm:

```rust
match field.as_str() {
    "CAPACITY"  => Ok(Response::Integer(bf.capacity()    as i64)),  // :436
    "SIZE"      => Ok(Response::Integer(bf.memory_size() as i64)),  // :437
    "FILTERS"   => Ok(Response::Integer(bf.num_layers()  as i64)),  // :438
    "ITEMS"     => Ok(Response::Integer(bf.count()       as i64)),  // :439
    "EXPANSION" => Ok(Response::Integer(bf.expansion()   as i64)),  // :440
    _ => Err(CommandError::InvalidArgument {                        // :441-443
        message: format!("Unknown field: {}", field),
    }),
}
```

**Drift audit, ruled honestly: none today.** All five accessors match one-for-one
(`Capacity`↔`CAPACITY`↔`capacity()`, `Size`↔`SIZE`↔`memory_size()`,
`Number of filters`↔`FILTERS`↔`num_layers()`, `Number of items inserted`↔`ITEMS`↔`count()`,
`Expansion rate`↔`EXPANSION`↔`expansion()`), the arities agree (5 and 5), and the selector tokens
match upstream RedisBloom's documented set. **There is no live compatibility bug here and this
proposal does not claim one.**

What there is, is an *unguarded* invariant. The two arms are related only by a human reading them
together. The label and the selector for the same datum are different string literals with no
syntactic connection, so no compiler check, no lint, and (§3) no test observes their agreement.
This is precisely the class of latent divergence the `redirect` seam (`lint-redirect-seam`,
`Justfile:449`) and the `format_float` seam (`lint-format-float`, `Justfile:1249-1269`) exist to close
elsewhere in this tree — and the class the server's own `INFO` closed with `InfoSection`
(`server/src/info/mod.rs:199-205`, registry `:208-218`).

### 2. Four of the five families have nothing to unify — the briefed change would be net-negative there

`CF.INFO`/`CMS.INFO`/`TOPK.INFO`/`TDIGEST.INFO` are `Arity::Fixed(1)` (`cuckoo.rs:507`,
`cms.rs:401`, `topk.rs:390`, `tdigest.rs:605`). Each has exactly one reply site. For those, a
`const FIELDS` table plus a `flat_map` is *strictly more indirection than a `vec![]` literal*: it
replaces a construction you can read top-to-bottom, in reply order, with a table plus a walk that
produces the same thing. It buys nothing (no second site to agree with, no allocation to save) and
costs the reader a hop. **Proposing it would be cargo-culting the shape.** This proposal
explicitly declines it; §Risks records the trigger that would change the answer.

**`FT.INFO` is a fifth decline, on the same test.** Rev 1 asserted no `FT.INFO` existed; that was
wrong (`server/src/commands/search.rs:197`). The ruling is unchanged and in fact strengthened:
`FT.INFO` is `Arity::Fixed(1)` (`:203`) — no selector, one reply site — it is built in a different
crate (`core/src/shard/search/index_mgmt.rs:29`, reached via `ServerWideOp::FtInfo` `:214`) out of
*nested* label/value sub-arrays that a flat table cannot carry, and every one of its labels is
**already** `Bytes::from_static`, which is one more independent demonstration that the brief's
allocation premise was never real.

### 3. The test oracle pins the array and under-covers the selector — the *one* path that could drift

`bloom_regression.rs:165-190` (`bf_info_all_fields`) is a good oracle for the array: it asserts
`items.len() == 10` (`:179`) and each label **by index** (`:182-186`), so reply **order** is
pinned, not just membership.

`bloom_regression.rs:193-211` (`bf_info_specific_field`) is not the matching oracle for the other
arm. Across the whole workspace there are exactly **six** `BF.INFO` invocations in tests
(`bloom_regression.rs:175, 202, 206, 209, 629, 665`) plus two label-search string comparisons
(`:633, :670`) — eight sites in all, not the nine claimed in rev 1 — and of the five selectors only
**`CAPACITY`**, **`ITEMS`**, and **`FILTERS`** are ever sent. Consequently:

- **`SIZE` and `EXPANSION` have zero coverage.** Deleting `bloom.rs:437` or `:440` turns a valid
  request into `Unknown field: SIZE` and the suite stays green.
- **No test compares the two arms.** The three covered selectors are asserted against
  hand-written constants — two loosely (`cap >= 100` `:204`, `unwrap_integer(&resp) >= 1` `:210`),
  one exactly (`assert_integer_eq(&resp, 1)` for `ITEMS` `:207`) — but *never* against the value
  the all-fields reply reports for the same key. A selector wired to the *wrong* accessor —
  `"SIZE" => bf.capacity()` — is invisible to the suite.
- **Worse, the fixture cannot see a swap even in principle.** `bf_info2` is
  `BF.RESERVE 0.01 100` + one `BF.ADD`, so `FILTERS == 1` **and** `ITEMS == 1`: exchanging those
  two arms passes every existing assertion, including the exact one. `bf_info`'s fixture has the
  same defect (`ITEMS == 2` and `EXPANSION == 2`). §H1 fixes the fixture, not just the coverage.
- **The error path is untested.** `bloom.rs:441-443` has no forcing test.

So the arm with the drift risk is the arm the oracle does not constrain. That asymmetry is the
strongest argument in this proposal, and it is fixable **today, without the refactor** (§H1).

### 4. Constraints the table must satisfy — checked, not assumed

The brief asks whether a `fn(&T) -> Response` accessor is sufficient. Answering per family:

- **Extra context (`ctx`, `args`)?** No. Every `BF.INFO` value is a method on the bound value
  (`bf.capacity()`, `bf.memory_size()`, `bf.num_layers()`, `bf.count()`, `bf.expansion()`); `ctx`
  is used only to *fetch* the value (`bloom.rs:412`) and `args[1]` only to *select* (`:430`).
  Neither belongs inside an accessor. **`fn(&BloomFilterValue) -> Response` is sufficient for
  `BF.INFO`.**
- **Non-integer values?** Not in `BF.INFO` (all five are `Response::Integer`). Elsewhere yes —
  `topk.rs:421-422` renders `decay` as a bulk string via `format!`, `tdigest.rs:635-638` does the
  same for the two weights. The compile check above confirms an allocating accessor is a legal
  table entry, so this is not a structural obstacle; it is simply not exercised at the one site
  being changed.
- **Fields with no accessor?** Yes, one, and **outside** the scope: `tdigest.rs:639-640`,
  `"Total compressions"` → `Response::Integer(0)`, hardcoded. A table would have to carry
  `|_| Response::Integer(0)`, which is honest but no clearer than the literal it replaces.
- **Conditional fields?** Yes, one, and **outside** the scope: `vinfo.rs:70-73` emits
  `projection-input-dim` only when `info.original_dim > 0`. A flat `&[Field]` has no way to say
  "sometimes"; expressing it needs a predicate column or an `Option`-returning accessor, i.e. a
  second concept in the table for a single site. **Scoped out.**
- **Non-scalar fields?** Yes, two, and **outside** the scope: `timeseries.rs:981-1010` builds
  `labels` (`:981-994`) and `rules` (`:996-1010`) as nested `Response::Array`s derived from
  collections on the value, not from a scalar accessor. `FT.INFO`
  (`core/src/shard/search/index_mgmt.rs:29`) is nested throughout — every value is itself a
  sub-array — so the flat table cannot express it either.

These are the reasons §2's ruling is structural, not aesthetic: three of the seven in-crate
`*.INFO` producers contain a field the briefed table shape cannot carry, and the eighth
(`FT.INFO`) is not flat at all.

## Proposed change

One primary edit, in `commands/src/bloom.rs`, above `BfInfo` (`:387`):

```rust
/// One `BF.INFO` field: its reply label, the selector token that asks for it
/// alone, and the single accessor both reply paths call.
///
/// `BF.INFO` is the only probabilistic `*.INFO` command that takes a field
/// argument (`CF`/`CMS`/`TOPK`/`TDIGEST` are `Arity::Fixed(1)`), so it is the
/// only one where "which fields exist" could be answered twice — and this
/// table is the answer. Slice order **is** reply order; Redis clients index
/// `BF.INFO` positionally.
struct BfInfoField {
    label: &'static str,
    selector: &'static str,
    value: fn(&BloomFilterValue) -> Response,
}

const BF_INFO_FIELDS: &[BfInfoField] = &[
    BfInfoField { label: "Capacity", selector: "CAPACITY",
                  value: |bf| Response::Integer(bf.capacity() as i64) },
    BfInfoField { label: "Size", selector: "SIZE",
                  value: |bf| Response::Integer(bf.memory_size() as i64) },
    BfInfoField { label: "Number of filters", selector: "FILTERS",
                  value: |bf| Response::Integer(bf.num_layers() as i64) },
    BfInfoField { label: "Number of items inserted", selector: "ITEMS",
                  value: |bf| Response::Integer(bf.count() as i64) },
    BfInfoField { label: "Expansion rate", selector: "EXPANSION",
                  value: |bf| Response::Integer(bf.expansion() as i64) },
];
```

Both arms of `execute` then walk it — `:415-427` becomes

```rust
Ok(Response::Array(
    BF_INFO_FIELDS
        .iter()
        .flat_map(|f| [
            Response::bulk(Bytes::from_static(f.label.as_bytes())),
            (f.value)(&bf),
        ])
        .collect(),
))
```

and `:435-444` becomes

```rust
BF_INFO_FIELDS
    .iter()
    .find(|f| f.selector == field)
    .map(|f| (f.value)(&bf))
    .ok_or_else(|| CommandError::InvalidArgument {
        message: format!("Unknown field: {field}"),
    })
```

Everything else in `execute` — the UTF-8 decode of `args[1]` `:430-434` (`.to_uppercase()` on
`:434`), the missing-key error `:447-449` — is untouched.

**What this is and is not.** It is not a new abstraction: `BfInfoField` is private to `bloom.rs`,
has no trait, no registry, and no cross-crate surface. It is the smallest structure that makes the
label↔selector↔accessor correspondence a *single expression* instead of a convention. The
`Bytes::from_static` in the reply path is a **legibility** change only — §Corrections proves the
current code already resolves to it — kept because a table entry typed `&'static str` should say
so at the use site.

**Ordering guarantee.** `BF.INFO`'s reply order is observable and pinned by index at
`bloom_regression.rs:182-186`. `[T]::iter()` is defined to yield in slice order and the table
literal preserves the current sequence, so the reply is byte-identical. The doc comment states
this so a future editor knows reordering the table is a wire-visible change — a guarantee the
`vec![]` form conveys only by accident. This mirrors `InfoBuilder::standard()`
(`server/src/info/mod.rs:214-218`), whose doc (`:213`) says "in canonical order" for the same reason.

## Testability improvement

The refactor makes one drift *unrepresentable*; the tests make three more *observable*. Both
belong in `bloom_regression.rs`, and **the tests do not depend on the refactor** (§H1).

0. **A fixture whose five values are pairwise distinct — a precondition, not a nicety.** Neither
   existing fixture can observe a swapped accessor: `bf_info2` (`:198-200`) yields
   `FILTERS == ITEMS == 1`, and `bf_info` (`:170-173`) yields `ITEMS == EXPANSION == 2`. An
   equivalence test built on either would pass with the `FILTERS`/`ITEMS` arms exchanged — the
   exact mutation it exists to catch. The new test therefore builds its own key, e.g.
   `BF.RESERVE bf_info_eq 0.01 100 EXPANSION 3` (`EXPANSION` ≠ any plausible count) followed by
   enough `BF.ADD`s to force **≥ 2 layers**, so `CAPACITY`, `SIZE`, `FILTERS`, `ITEMS`,
   `EXPANSION` are five different integers. **And the test asserts that distinctness itself**
   (collect the five values, assert all pairs differ, with a message naming the colliding pair) so
   a later fixture edit that re-introduces a collision fails loudly instead of silently degrading
   the pin to a tautology.
1. **Arm-equivalence (the load-bearing one).** On that fixture, for each of the five selectors,
   assert `BF.INFO key <SELECTOR>` equals the value the all-fields reply carries at that label's
   index — comparing the *two reply paths against each other* rather than each against a
   hand-written constant. This is the assertion that has never existed, and it is exactly the
   invariant the table enforces structurally. Against the refactored code it is a tautology by
   construction; against today's code it is a genuine check that can fail. With item 0 in place,
   any pairwise exchange of the five accessors is caught.
2. **Selector completeness.** Extend `bf_info_specific_field` (`:193-211`) to all five selectors,
   closing the `SIZE`/`EXPANSION` gap. Post-refactor this also becomes an existence proof that the
   table's `selector` column is reachable — deleting a row fails it.
3. **Unknown-field error.** `BF.INFO key NOSUCHFIELD` → error, forcing `bloom.rs:441-443`.

**Mutation posture.** `frogdb-commands` is not one of the four locked crates
(`txn`/`persistence`/`replication`/`cluster`, ADRs `0002`–`0004`), so no gate applies and
`just mutants-gate` is not in play. Worth stating anyway, because it is the honest measure of the
gap: today, mutating `bloom.rs:437` (`memory_size` → `capacity`) survives the whole suite. With
test 1 it dies. With the refactor the mutation has only one site to attack instead of two, and
killing it there kills it for both reply paths at once — that is the leverage, small as it is.

**Test placement.** These are end-to-end reply-shape assertions, so `redis-regression` is right
(and `BF.*` needs `full`/`cmd-full`, which that crate already requests — `commands/Cargo.toml:31-45`).
A unit test in `commands` could assert the table's internal consistency (labels unique, selectors
unique and uppercase, arity 5) more cheaply and without the feature dance; adding one is optional
and noted, not required. It would *not* substitute for item 0 — internal uniqueness of the
*strings* says nothing about distinctness of the *values* a given fixture produces.

## Deletion test

*If `BF_INFO_FIELDS` were deleted, what fact would have no home?* — "The set of fields `BF.INFO`
reports, their wire order, the token that requests each one alone, and the accessor that computes
it." Today that fact has **two partial homes** and neither is authoritative: the array knows
labels+order+accessors but not selectors; the match knows selectors+accessors but not labels or
order. Delete either and the command silently loses a capability. The table is the first place the
whole fact is stated once. **The table passes.**

*Same question for a hypothetical `CF_INFO_FIELDS`.* — Nothing. The `vec![]` at `cuckoo.rs:527-544`
is already the single home; a table would re-state it in a second syntax. **It fails, and is not
proposed** — which is §2.

**Vocabulary.** `bloom.rs` is the *module*; the `Command` impl is the *interface*; the table is a
*seam* only in the narrow sense that two call sites inside one function pass through it — it is
deliberately **not** a cross-module *adapter*, and claiming otherwise would oversell an S-sized
change. The *leverage* is low (one command, five fields) and the *locality* is total: the table,
both consumers, and the type it reads all sit within 60 lines of one file, and no other crate can
observe the change.

## Risks / scope boundaries vs siblings

**Behavioral risk: none identified.** The reply bytes are unchanged on every path (all-fields
order-preserved; each selector maps to the same accessor; unknown-field message string
`"Unknown field: {field}"` preserved verbatim). `bf_info_all_fields` (`:165-190`) is the
byte-shape oracle and must pass unmodified — that is the acceptance condition.

**Gate risk: none.** `lint-info-seam` (`Justfile:423-441`) is file-scoped to three `server/src`
paths and forbids `.replace(…)`/`.replace_range(`; neither the scope nor the constructs apply.
`lint-format-float` (`Justfile:1249-1269`) pins `fn format_float` definitions; this change defines none.
No other member of `lint-gates` (`:329`) touches `commands/src/bloom.rs`.

**Scope risk — the one to watch.** The obvious "while we're here" is converting the other four
families. §2 rules that out *now*, and the trigger to revisit is explicit and narrow: **if a second
`*.INFO` command gains a field selector** (upstream RedisBloom adding `CF.INFO key <field>`, say),
that family acquires the two-homes problem and should get the same table — at which point sharing
the `Field` struct across families becomes worth discussing, and would need a home unconditional
w.r.t. the per-family feature gates (`commands/src/lib.rs:27-60`), i.e. `commands/src/utils.rs`
(1241 lines, already the crate's unconditional shared-helper module). **Until that trigger fires,
one table in one file.**

**Sibling 89 (`probabilistic-chunk-codec`) — same two files, disjoint regions, order preference
stated.** 89's primaries in these files are `BfScandump::execute :517-564` / `BfLoadchunk::execute
:592-667` in `bloom.rs`, and `CfScandump::execute :577-625` / `CfLoadchunk::execute :653-747` in
`cuckoo.rs`. This proposal touches `bloom.rs:387-452` **only** — strictly above every 89 region —
and touches `cuckoo.rs` **not at all** (it is read-only evidence here). The regions do not
overlap, so there is no textual conflict. **Land 89 first.** Rationale: 89 carries a durability
defect fix and deletes ~130 lines from `bloom.rs`, all *below* this proposal's region, so its
line numbers survive this change landing either way — but this change landing first shifts nothing
of 89's and 89 landing first shifts nothing of this. The tiebreak is priority, not mechanics: 89 is
a correctness fix, this is an S-sized clarity fix. If this lands first, 89's `bloom.rs` citations
below `:452` shift by **≈ +21** (rev 1 said `+~2`; see §Effort). Note also that 89 edits
`commands/Cargo.toml:54` (dropping `bitvec`; `[dependencies]` starts at `:47`)
while this proposal does not touch that file — and 89 cites the file's length as 60, which is
correct; rev 1 of this proposal said 78, which was not.

**Sibling 90 (`CommandSpec::DEFAULT`) — same files, same `impl` blocks, different functions.**
90 rewrites the `static SPEC` initializers across 296 sites, including `BfInfo`'s at
`bloom.rs:391-407` — the *sibling function* of the one edited here, inside the same
`impl Command for BfInfo`. Different hunks; git merges them. The interaction to be aware of is a
semantic one, not a textual one: 90 makes `Arity` the kind of field that is *spelled only when
exceptional*, and `BF.INFO`'s `Arity::Range { min: 1, max: 2 }` `:393` — the very fact that makes
it the only family with a selector — is exceptional and must survive 90's fold explicitly.

**How dangerous, precisely.** Spec validation
(`core/src/command_spec.rs:823-828`) rejects an arity too small for the declared keys:
`BF.INFO` is `KeySpec::First` (`bloom.rs:395`), so `min_required_args() == 1` and a fold to
`Arity::Fixed(0)` would be **caught** at once by `SpecError::ArityTooSmallForKeys` (`:668`,
`:826`). A fold to `Arity::Fixed(1)` would **not** be caught — `min == needs`, validation passes —
and it silently deletes the selector arm's reachability: every `BF.INFO key <FIELD>` becomes a
wrong-arity error, while the whole spec suite stays green. So the hazard is narrower than "90
might change the arity" and sharper: exactly one wrong default, `Fixed(1)`, is both plausible and
invisible to the existing guard. **Order indifferent; if 90 lands first, re-confirm `:393` still
reads `Range { min: 1, max: 2 }` before applying this**, and note that the arm-equivalence test
from §H1 would also catch that fold — one more reason to land H1 first.

**Siblings 92/93 (expiry).** Both are on disk at `dd840ca3`. 92's subject is
`commands/src/expiry.rs` and the lazy-expiry authority; 93's primary is `commands/src/hash.rs`
(field-expiry writes and purge calls). **No file overlap with this proposal** — neither touches
`bloom.rs`, and the only file all three could share is `commands/src/lib.rs`, which this proposal
does not edit.

**Boundary, stated once.** `VINFO` (`vectorset/vinfo.rs:11`, conditional field `:70-73`) and
`TS.INFO` (`timeseries.rs:935`) are **out of scope** on the structural grounds in §4 (a
conditional field; two nested-collection fields), not on grounds of family membership. `FT.INFO`
(`server/src/commands/search.rs:197`) is likewise out of scope, and on the *same* test as the four
probabilistic declines — `Arity::Fixed(1)` (`:203`), no selector, one reply site, built in another
crate (`core/src/shard/search/index_mgmt.rs:29`) out of nested sub-arrays, already
`Bytes::from_static`. **Rev 1's claim that no `FT.INFO` exists in this tree is withdrawn**; the
ruling it was offered in support of is unaffected and its `from_static` reply strengthens the
allocation refutation. `XINFO` (`commands/src/stream/info.rs`, `name: "XINFO"` `:20`) is a
subcommand router with three distinct reply shapes — a different problem entirely, and already
using `Bytes::from_static` at `:49-51`, further evidence that the allocation premise was never
real.

## Effort

**S.** One file. Counted off the sketch above rather than estimated: **added ≈ 43** — the table
block is 26 lines (8-line doc comment, 5-line `struct`, blank, 12-line `const`), the `flat_map`
replacement 10, the `find` replacement 7 — against **removed ≈ 22** (all-fields `vec!` `:416-427`
= 12, selector `match` `:435-444` = 10), so **net ≈ +21**. Rev 1's "net ≈ +2" was wrong by an
order of magnitude; the sizing is unchanged because effort here is one file and one reviewer's
read, not line count, and +21 lines that delete a duplicated invariant is still **S**. No new module, no new crate edge, no
`Cargo.toml` change, no generated artifact to regenerate (`commands.json` carries no reply
fields), no doc update (the doc comment at `bloom.rs:386` already lists all five selectors and
stays correct). The mechanism is compile-verified on the pinned toolchain. Iteration is
`just check frogdb-commands` plus `just test frogdb-redis-regression bf_info`.

**Independently-landable hotfix.**

**H1 — the distinct-value fixture + arm-equivalence + coverage tests, alone, with no refactor.**
This is the item worth landing first and separately. It is test-only (`bloom_regression.rs`),
touches no production code, conflicts with no sibling, and closes §Problem 3's real gap: it makes
the two arms' agreement *observed* rather than merely *true*, covers the two selectors nobody
tests, and forces the unknown-field error. Landing it before the refactor also gives the refactor
its acceptance oracle — the tests must pass identically before and after, which is the only proof
the reply bytes did not move. **If only one thing from this proposal ships, ship H1.**

**H1 is not optional in its fixture.** Reusing `bf_info` or `bf_info2` would produce a test that
*looks* like an equivalence pin and is not one: with `FILTERS == ITEMS` (or
`ITEMS == EXPANSION`), the swap it exists to catch passes. H1 must therefore (a) reserve its own
key with an `EXPANSION` distinct from every count — `BF.RESERVE bf_info_eq 0.01 100 EXPANSION 3`
— and insert enough elements to force `FILTERS ≥ 2`, and (b) **assert pairwise-distinctness of the
five values inside the test**, so the pin cannot decay silently when someone later retunes the
fixture. `bf_expansion_grows_capacity` (`:646-681`) is the in-file precedent for driving a filter
past one layer.

No other hotfix is claimed. There is no live compatibility bug in `BF.INFO` (§Problem 1), so there
is nothing else to fix ahead of the refactor.

## Adjacent defects — surfaced here, to be FILED by the orchestrator

Two compatibility gaps found while verifying. Neither is in this proposal's scope and neither is
fixed here, but both are **candidate issues, not footnotes** — they outlive whatever happens to
the refactor.

**D1 — `TDIGEST.INFO` is likely missing the `Observations` field (probable real compat gap).**
FrogDB's reply carries 8 fields (`tdigest.rs:626-643`: `Compression`, `Capacity`, `Merged nodes`,
`Unmerged nodes`, `Merged weight`, `Unmerged weight`, `Total compressions`, `Memory usage`) and
the oracle pins **exactly** those 8 by name and index
(`redis-regression/tests/tdigest_regression.rs:410-437`, `assert_array_len(&info, 16)` `:418`).
Upstream RedisBloom documents an `Observations` field between `Unmerged weight` and
`Total compressions`; the string `"Observations"` appears **nowhere in this repo** (grep across
`*.rs`/`*.md`/`*.mdx`). If upstream does emit it, every client that indexes `TDIGEST.INFO`
positionally past `Unmerged weight` reads the wrong value against FrogDB, and our own oracle
currently *pins the divergence in place*. **Upstream not verifiable from this environment** — the
issue should begin by confirming the field against RedisBloom source, then move the wire change
and the oracle in lockstep. **File it.**

**D2 — `TS.INFO` accepts and silently discards its second argument (two-part).**
`timeseries.rs:941` declares `Arity::Range { min: 1, max: 2 }`, but `execute` (`:957`) reads
`args[0]` only (`:958`) and never inspects `args[1]`. Two consequences: (a) `TS.INFO key JUNK`
returns the ordinary reply where upstream errors; (b) the one token upstream *does* accept there,
`DEBUG`, is equally ignored — and FrogDB has none of the extra chunk-level fields the `DEBUG`
variant is supposed to add, so even a correctly-spelled `TS.INFO key DEBUG` is non-conformant in
reply *content*, not just in validation. (a) is a one-line fix; (b) is a feature gap. **File it**,
ideally as one issue with the two parts distinguished.

## Adjacent findings (evidence, not proposals)

Surfaced while verifying; each is out of scope here and none is asserted as a defect without the
caveat attached.

1. **`"Total compressions"` is a hardcoded `0`** (`tdigest.rs:639-640`), self-documented as a
   Redis-matching placeholder. Correct today per the comment; noted because it is the one
   `*.INFO` field in the family with no backing state, and it would be the awkward entry if
   §Risks' trigger ever brings `TDIGEST.INFO` into a table.
2. **`TOPK.INFO` `decay` and `TDIGEST.INFO`'s two weights render floats with `format!("{}", …)`**
   (`topk.rs:422`, `tdigest.rs:636,638`) rather than the canonical `format_float`
   (`protocol/src/format.rs`, re-exported at `commands/src/utils.rs:27` precisely so reply and
   store paths agree). `lint-format-float` (`Justfile:1249-1269`) pins the *definition* count, not
   the *call* sites, so this is outside the gate. Whether upstream's rendering matches `{}` for
   these fields is unverified; flagged for a float-rendering audit, not fixed here.
3. **Every one of the seven datatype `*.INFO` commands returns `CommandError::InvalidArgument`
   ("Key does not exist" / "TSDB: the key does not exist") for a missing key** (`bloom.rs:447`,
   `cuckoo.rs:545`, `cms.rs:421`, `topk.rs:410`, `tdigest.rs:645`, `vinfo.rs:37`,
   `timeseries.rs:1014`). Consistent within the tree; whether the rendered prefix matches
   upstream's is a compat-audit question spanning all seven and is not this proposal's business.
   (`FT.INFO` is the eighth `*.INFO` but errors differently — `Response::error("Unknown index
   name")`, `core/src/shard/search/index_mgmt.rs:36` — because its lookup is index-name, not key.)
