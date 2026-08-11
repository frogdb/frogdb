# Proposal 95 — `BF_INFO_FIELDS`: one table for the only `*.INFO` reply that answers "which fields exist" twice

Round 38 · lane: commands + types · effort **S** · **no locked crate edited** · **zero `FM-` tags
in any edited file** (verified: `grep -rn "FM-" frogdb-server/crates/commands/src` returns nothing)

**Verified at HEAD `8a17065247c8935e6476fbffc92a5e94a1b20854`** (worktree `arch-round-38-99`,
branch `worktree-arch-round-38-99`). Every file:line, count, and type below was re-derived at this
SHA. Nothing is inherited from the lane brief. Working tree carries one untracked proposal file
(`92-expiry-decision-table.md`); **no code file in this set is dirty**.

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
| "~207 `Bytes::from("literal")` allocations for static field names" | **Refuted twice over.** (a) The number is not INFO-scoped: my count of `Bytes::from("` across all of `commands/src` is **214**, of which **28** are `*.INFO` field labels (BF 5, CF 8, CMS 3, TOPK 4, TDIGEST 8) and 3 more in `tdigest.rs:29,32,34` are `nan`/`inf`/`-inf`. (b) **None of them allocate.** `bytes 1.11.1` (`Cargo.lock:615-616`) defines `impl From<&'static str> for Bytes { fn from(slice) -> Bytes { Bytes::from_static(slice.as_bytes()) } }` (`bytes-1.11.1/src/bytes.rs:954-958`), and `from_static` (`:167-174`) is a `const fn` that stores the pointer against `STATIC_VTABLE`. The **only** `&str`-shaped `From` impl for `Bytes` is the `'static` one (full list: `:948, :954, :960, :995, :1026, :1048`) — a non-`'static` `&str` does not compile, so every one of these 214 sites is already zero-allocation. Converting them to `Bytes::from_static` is a **legibility** change with **no runtime effect**. |
| "per-family `const FIELDS: &[(&str, fn(&T) -> Response)]`" | **Mechanism is sound** (compile-verified below), **applicability is one family.** The tuple shape is also insufficient for `BF.INFO`: the reply label (`"Number of items inserted"`) and the selector token (`"ITEMS"`) are *different strings for the same datum*, and unifying them is the entire point. A 3-field struct is required. |
| "S-M effort. Latent." | **S**, and *latent* is right but for the reachability reason, not the severity reason: `bloom`/`cuckoo`/`cms`/`topk`/`tdigest` are individually-gated cargo features (`commands/Cargo.toml:20-27`), absent from `default`, present only under `full` (`:34-41`) — the same gating proposal 89 documents. |
| Implied: "the five families share a duplication problem" | **They share a *shape*, not a problem.** Six families (adding `VINFO`, `TS.INFO`) build an alternating label/value `Response::Array`. Two of the six have structure the briefed table cannot express (§4). Shape similarity is not leverage. |

## Summary

`BF.INFO` answers the question *"which fields does a bloom filter report, and how is each one
computed?"* in two places, thirteen lines apart, in the same function:

- the all-fields arm, `bloom.rs:415-427` — five `(label, accessor)` pairs;
- the selector arm, `bloom.rs:428-446` — the same five accessors again, behind five *different*
  string keys.

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
`VINFO`, and `TS.INFO` keep their `vec![]` literals, and §Risks states why converting them would
be a regression in clarity.

## Files involved

Line counts at `8a170652`. All paths under `frogdb-server/crates/` unless noted.

| Path | Lines | Role in this change |
|---|---|---|
| `commands/src/bloom.rs` | 758 | **The only primary.** `BfInfo` `:387`; spec `:390-408`; `execute` `:410-452`. Adds `struct BfInfoField` + `const BF_INFO_FIELDS` (~24 lines) near `:385`; the all-fields arm `:415-427` (13 lines) and the selector `match` `:435-444` (10 lines) both collapse to ~4 lines each. Net ≈ **+2 lines**, one authority instead of two. **Zero `FM-` tags.** |
| `redis-regression/tests/bloom_regression.rs` | 715 | **Primary (tests).** `bf_info_all_fields :165-190` pins labels **by index** — `items[0]="Capacity" … items[8]="Expansion rate"`, `items.len()==10` `:179`. `bf_info_specific_field :193-211` exercises **only** `CAPACITY :202`, `ITEMS :206`, `FILTERS :209`. Gains the equivalence test (§Testability), which is the H1 hotfix and lands independently. |
| `commands/src/cuckoo.rs` | 839 | **Read-only evidence.** `CfInfo :501`, spec `Arity::Fixed(1)` `:507`, single flat array `:527-544` (8 labels at `:528,530,532,534,536,538,540,542`). **No second site — nothing to unify.** |
| `commands/src/cms.rs` | 434 | **Read-only evidence.** `CmsInfo :395`, `Arity::Fixed(1)` `:401`, array `:425-431` (3 labels `:426,428,430`). |
| `commands/src/topk.rs` | 425 | **Read-only evidence.** `TopkInfo :384`, `Arity::Fixed(1)` `:390`, array `:414-422` (4 labels `:415,417,419,421`); `decay` is a bulk string via `format!` `:422`. |
| `commands/src/tdigest.rs` | 689 | **Read-only evidence.** `TdInfo :599`, `Arity::Fixed(1)` `:605`, array `:626-642` (8 labels `:627…641`). `"Total compressions"` `:639-640` has **no accessor at all** — a hardcoded `Response::Integer(0)` with the comment "Not tracked, matches Redis placeholder". §4. |
| `commands/src/vectorset/vinfo.rs` | 77 | **Read-only — boundary case.** `VinfoCommand :11`, base array `:55-67` (6 labels `:56,58,60,62,64,66`), then a **conditional** field `:70-73`: `projection-input-dim` is pushed only when `info.original_dim > 0`. A flat `&[Field]` table cannot express "sometimes". §4, §Risks. |
| `commands/src/timeseries.rs` | 1368 | **Read-only — boundary case.** `TsInfoCommand :935`, `Arity::Range { min: 1, max: 2 }` `:941`. Base array `:960-976` (8 labels), then `labels` `:978-987` (a nested `Response::Array` built from the value's own map) and `rules` `:989-1010` (nested arrays of triples). Not tabular. **Adjacent finding:** `execute` reads `args[0]` only — `args[1]` is accepted by the arity and then **silently ignored**, so `TS.INFO key JUNK` returns full info where Redis errors. §Adjacent findings. |
| `commands/src/lib.rs` | — | **Read-only.** Per-family feature gates `:27-60` (`bloom :27-28`, `cms :29-30`, `cuckoo :31-32`, `tdigest :52-53`, `timeseries :54-55`, `topk :56-57`, `vectorset :59-60`) — each family is separately compiled, so a *per-family* table needs no `cfg` care and a *shared cross-family* helper would. |
| `commands/Cargo.toml` | 78 | **Read-only.** Features `:20-27`; the aggregate that turns them on `:34-41`. **Untouched by this change** (contrast proposal 89, which edits it). |
| `core/src/store/typed.rs` | 584 | **Read-only — the precedent and the type.** `typed_family_accessors!` `:222-286` is *already* the "one table, many families" pattern in this codebase, and it is a macro, not fn pointers, because it generates *signatures*. `BloomFilterValue { get_bloom, … }` `:277`. `get_*` returns `Result<Option<TypedArc<$ty>>, WrongTypeError>` `:238-240`; `TypedArc` derefs, so `fn(&BloomFilterValue) -> Response` is callable on the binding as written. |
| `types/src/bloom.rs` | 361 | **Read-only.** `pub struct BloomFilterValue` `:128` — the `T` in the table's fn pointer, re-exported at `types/src/lib.rs:35` and reachable from `commands` through `core/src/lib.rs:7`'s glob (same reach proposal 89 documents). |
| `server/src/info/mod.rs` | 1113 | **Read-only — the in-tree precedent.** `pub trait InfoSection` `:199-205` (`name() -> &'static str`, `render()`), `InfoBuilder` `:207-218`, `standard()` `:213-217` = "every section FrogDB serves, **in canonical order**". The server's own `INFO` had exactly this drift and it was closed with a registry. The datatype `*.INFO` commands never got the same treatment. |
| `Justfile` | — | **Read-only — gate check.** `lint-gates :329` lists 14 gates. `lint-info-seam :423-441` is scoped to three server files (`server/src/commands/info.rs`, `connection/scatter.rs`, `connection/info_handler.rs`) and forbids `.replace(…)`/`.replace_range(` — **it does not cover `commands/src`, and this change introduces neither construct.** `lint-format-float :1249-1271` pins one `fn format_float`, in `protocol/src/format.rs`; this change defines none. **No gate is affected.** |
| `website/src/data/commands.json` | — | **Read-only — generated, must not be hand-edited** (`_generated.warning`; source = `commands/src/lib.rs` + `server/src/server/register.rs`; regenerate via `just docs-gen`). The `BF.INFO` entry carries `name`/`arity`/`flags`/`family`/`execution_strategy`/`full`/`is_stub` and **no reply-field list**. **There is no third home for the field names** — confirmed by grepping `"Number of filters"`, `"Expansion rate"`, `"Merged nodes"`, `"Number of buckets"` across `*.rs`/`*.json`/`*.md`/`*.mdx`/`*.ts`/`*.astro`: every hit is either the single producer in `commands/src` or the regression-test oracle. |

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
`Justfile:449-475`) and the `format_float` seam (`lint-format-float:1249-1271`) exist to close
elsewhere in this tree — and the class the server's own `INFO` closed with `InfoSection`
(`server/src/info/mod.rs:199-217`).

### 2. Four of the five families have nothing to unify — the briefed change would be net-negative there

`CF.INFO`/`CMS.INFO`/`TOPK.INFO`/`TDIGEST.INFO` are `Arity::Fixed(1)` (`cuckoo.rs:507`,
`cms.rs:401`, `topk.rs:390`, `tdigest.rs:605`). Each has exactly one reply site. For those, a
`const FIELDS` table plus a `flat_map` is *strictly more indirection than a `vec![]` literal*: it
replaces a construction you can read top-to-bottom, in reply order, with a table plus a walk that
produces the same thing. It buys nothing (no second site to agree with, no allocation to save) and
costs the reader a hop. **Proposing it would be cargo-culting the shape.** This proposal
explicitly declines it; §Risks records the trigger that would change the answer.

### 3. The test oracle pins the array and under-covers the selector — the *one* path that could drift

`bloom_regression.rs:165-190` (`bf_info_all_fields`) is a good oracle for the array: it asserts
`items.len() == 10` (`:179`) and each label **by index** (`:183-187`), so reply **order** is
pinned, not just membership.

`bloom_regression.rs:193-211` (`bf_info_specific_field`) is not the matching oracle for the other
arm. Across the whole workspace there are exactly nine `BF.INFO` call sites in tests
(`bloom_regression.rs:175, 202, 206, 209, 629, 665`, plus label-search helpers at `:633, 670`),
and of the five selectors only **`CAPACITY`**, **`ITEMS`**, and **`FILTERS`** are ever sent.
Consequently:

- **`SIZE` and `EXPANSION` have zero coverage.** Deleting `bloom.rs:437` or `:440` turns a valid
  request into `Unknown field: SIZE` and the suite stays green.
- **No test compares the two arms.** Even the three covered selectors are asserted loosely
  (`cap >= 100` `:203`, `unwrap_integer(&resp) >= 1` `:210`) against constants, never against the
  value the all-fields reply reports for the same key. A selector wired to the *wrong* accessor —
  `"SIZE" => bf.capacity()` — is invisible to the suite.
- **The error path is untested.** `bloom.rs:441-443` has no forcing test.

So the arm with the drift risk is the arm the oracle does not constrain. That asymmetry is the
strongest argument in this proposal, and it is fixable **today, without the refactor** (§H1).

### 4. Constraints the table must satisfy — checked, not assumed

The brief asks whether a `fn(&T) -> Response` accessor is sufficient. Answering per family:

- **Extra context (`ctx`, `args`)?** No. Every `BF.INFO` value is a method on the bound value
  (`bf.capacity()`, `bf.memory_size()`, `bf.num_layers()`, `bf.count()`, `bf.expansion()`); `ctx`
  is used only to *fetch* the value (`bloom.rs:413`) and `args[1]` only to *select* (`:432`).
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
- **Non-scalar fields?** Yes, two, and **outside** the scope: `timeseries.rs:978-1010` builds
  `labels` and `rules` as nested `Response::Array`s derived from collections on the value, not
  from a scalar accessor.

These are the reasons §2's ruling is structural, not aesthetic: three of the six `*.INFO`
producers contain a field the briefed table shape cannot carry.

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

Everything else in `execute` — the UTF-8 check `:432-435`, `.to_uppercase()` `:435`, the
missing-key error `:447-449` — is untouched.

**What this is and is not.** It is not a new abstraction: `BfInfoField` is private to `bloom.rs`,
has no trait, no registry, and no cross-crate surface. It is the smallest structure that makes the
label↔selector↔accessor correspondence a *single expression* instead of a convention. The
`Bytes::from_static` in the reply path is a **legibility** change only — §Corrections proves the
current code already resolves to it — kept because a table entry typed `&'static str` should say
so at the use site.

**Ordering guarantee.** `BF.INFO`'s reply order is observable and pinned by index at
`bloom_regression.rs:183-187`. `[T]::iter()` is defined to yield in slice order and the table
literal preserves the current sequence, so the reply is byte-identical. The doc comment states
this so a future editor knows reordering the table is a wire-visible change — a guarantee the
`vec![]` form conveys only by accident. This mirrors `InfoBuilder::standard()`
(`server/src/info/mod.rs:213-217`), whose doc says "in canonical order" for the same reason.

## Testability improvement

The refactor makes one drift *unrepresentable*; the tests make three more *observable*. Both
belong in `bloom_regression.rs`, and **the tests do not depend on the refactor** (§H1).

1. **Arm-equivalence (the load-bearing one).** For each of the five selectors, assert
   `BF.INFO key <SELECTOR>` equals the value the all-fields reply carries at that label's index —
   comparing the *two reply paths against each other* rather than each against a hand-written
   constant. This is the assertion that has never existed, and it is exactly the invariant the
   table enforces structurally. Against the refactored code it is a tautology by construction;
   against today's code it is a genuine check that can fail.
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
(and `BF.*` needs `full`/`cmd-full`, which that crate already requests — `commands/Cargo.toml:34-41`).
A unit test in `commands` could assert the table's internal consistency (labels unique, selectors
unique and uppercase, arity 5) more cheaply and without the feature dance; adding one is optional
and noted, not required.

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
`lint-format-float` (`:1249-1271`) pins `fn format_float` definitions; this change defines none.
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
a correctness fix, this is an S-sized clarity fix. If this lands first, the only stale reference is
`+~2` on 89's `bloom.rs` line citations. Note also that 89 edits `commands/Cargo.toml:54` (dropping
`bitvec`) while this proposal does not touch that file.

**Sibling 90 (`CommandSpec::DEFAULT`) — same files, same `impl` blocks, different functions.**
90 rewrites the `static SPEC` initializers across 296 sites, including `BfInfo`'s at
`bloom.rs:391-407` — the *sibling function* of the one edited here, inside the same
`impl Command for BfInfo`. Different hunks; git merges them. The interaction to be aware of is a
semantic one, not a textual one: 90 makes `Arity` the kind of field that is *spelled only when
exceptional*, and `BF.INFO`'s `Arity::Range { min: 1, max: 2 }` `:393` — the very fact that makes
it the only family with a selector — is exceptional and must survive 90's fold explicitly. **Order
indifferent; if 90 lands first, re-confirm `:393` still reads `Range { min: 1, max: 2 }` before
applying this.**

**Siblings 92/93 (expiry).** `92-expiry-decision-table.md` is present in the working tree
(untracked). Its subject is `commands/src/expiry.rs` and the lazy-expiry authority; 93 is not on
disk at this SHA. **No file overlap with this proposal** — confirmed against 92's stated scope; if
93 lands in `commands/src`, the only shared file would be `lib.rs`, which this proposal does not
edit.

**Boundary, stated once.** `VINFO` (`vectorset/vinfo.rs:11`) and `TS.INFO`
(`timeseries.rs:935`) are **out of scope** on the structural grounds in §4 (a conditional field;
two nested-collection fields), not on grounds of family membership. `XINFO`
(`commands/src/stream/info.rs:20`) is a subcommand router with three distinct reply shapes — a
different problem entirely, and already using `Bytes::from_static` at `:49-51`, further evidence
that the allocation premise was never real. No `FT.INFO` exists in this tree.

## Effort

**S.** One file, ~24 lines added and ~23 replaced, net ≈ +2. No new module, no new crate edge, no
`Cargo.toml` change, no generated artifact to regenerate (`commands.json` carries no reply
fields), no doc update (the doc comment at `bloom.rs:386` already lists all five selectors and
stays correct). The mechanism is compile-verified on the pinned toolchain. Iteration is
`just check frogdb-commands` plus `just test frogdb-redis-regression bf_info`.

**Independently-landable hotfix.**

**H1 — the arm-equivalence + coverage tests, alone, with no refactor.** This is the item worth
landing first and separately. It is test-only (`bloom_regression.rs`), touches no production code,
conflicts with no sibling, and closes §Problem 3's real gap: it makes the two arms' agreement
*observed* rather than merely *true*, covers the two selectors nobody tests, and forces the
unknown-field error. Landing it before the refactor also gives the refactor its acceptance oracle
— the tests must pass identically before and after, which is the only proof the reply bytes did
not move. **If only one thing from this proposal ships, ship H1.**

No other hotfix is claimed. There is no live compatibility bug in `BF.INFO` (§Problem 1), so there
is nothing else to fix ahead of the refactor.

## Adjacent findings (evidence, not proposals)

Surfaced while verifying; each is out of scope here and none is asserted as a defect without the
caveat attached.

1. **`TS.INFO` silently ignores its second argument.** `timeseries.rs:941` declares
   `Arity::Range { min: 1, max: 2 }` but `execute` (`:957`) reads `args[0]` only — `args[1]` is
   never inspected. Upstream accepts `TS.INFO key DEBUG` and rejects anything else; here
   `TS.INFO key JUNK` returns the ordinary reply. A one-line validation, but it is a different
   command family and a different file from this proposal's scope.
2. **`TDIGEST.INFO` may be missing an `Observations` field.** The reply carries 8 fields
   (`tdigest.rs:626-642`) and the oracle pins exactly those 8 by name
   (`tdigest_regression.rs:410-437`). Upstream RedisBloom's `TDIGEST.INFO` is documented with an
   `Observations` field between `Unmerged weight` and `Total compressions`; the string
   `"Observations"` appears **nowhere** in this repo (grep across `*.rs`/`*.md`/`*.mdx`).
   **Stated as unverified** — I could not check upstream source from this environment, and adding
   a field is a wire change that would need the oracle updated in lockstep. Worth a
   compat-audit item, not a change here.
3. **`"Total compressions"` is a hardcoded `0`** (`tdigest.rs:639-640`), self-documented as a
   Redis-matching placeholder. Correct today per the comment; noted because it is the one
   `*.INFO` field in the family with no backing state, and it would be the awkward entry if
   §Risks' trigger ever brings `TDIGEST.INFO` into a table.
4. **`TOPK.INFO` `decay` and `TDIGEST.INFO`'s two weights render floats with `format!("{}", …)`**
   (`topk.rs:422`, `tdigest.rs:636,638`) rather than the canonical `format_float`
   (`protocol/src/format.rs`, re-exported at `commands/src/utils.rs:27` precisely so reply and
   store paths agree). `lint-format-float` pins the *definition* count, not the *call* sites, so
   this is outside the gate. Whether upstream's rendering matches `{}` for these fields is
   unverified; flagged for a float-rendering audit, not fixed here.
5. **Every one of the six `*.INFO` commands returns `CommandError::InvalidArgument`
   ("Key does not exist" / "TSDB: the key does not exist") for a missing key** (`bloom.rs:447`,
   `cuckoo.rs:546`, `cms.rs:420`, `topk.rs:410`, `tdigest.rs:645`, `vinfo.rs:37`,
   `timeseries.rs:1014`). Consistent within the tree; whether the rendered prefix matches
   upstream's is a compat-audit question spanning all six and is not this proposal's business.
