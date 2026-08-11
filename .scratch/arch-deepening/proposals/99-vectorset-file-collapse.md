# Proposal 99 — `vectorset/`: twelve files, twelve commands, and three facts with no home

Round 38 · lane: commands · effort **S** · **no locked crate edited** · **zero `FM-` tags in any
edited file** (verified: `grep -rn "FM-" frogdb-server/crates/commands/src` returns nothing)

**Verified at HEAD `dd840ca3bb3a70319c424d62885e753e51abfdf5`** (worktree `arch-round-38-99`,
branch `worktree-arch-round-38-99`). Every path, line count, line number and hash below was
re-derived at this SHA. Nothing is inherited from the lane brief. Working tree carries three dirty
proposal files (`81`, `82`, `84`); **no code file in this set is dirty**.

## Summary

`frogdb-server/crates/commands/src/vectorset/` is **13 files / 1,180 lines** holding **12
commands** — one file per command, a ratio no other submodule in the crate approaches for a family
this size (`sorted_set` 4.0 commands/file, `json` 3.5, `stream` 3.4). `vdim.rs` is the exemplar:
48 lines, of which **18 are the `spec()` fn**, **6 are an import preamble that is byte-identical
across 9 of the 12 files** (md5 `1f545a7c…`, verified), and **16 are the `execute` fn**. The file
exists to hold a 14-line function body.

That alone would be a taste argument. The reason to propose it is what the twelve-file split
*hides*: three facts about vector sets are stated repeatedly, in files that never see each other.

1. **The FP32 wire encoding** — "a vector on the wire is little-endian 4-byte `f32`, length must
   be a multiple of 4" — is spelled **three times**: decoded in `vadd.rs:81-89`, decoded again in
   `vsim.rs:81-89` (same error string, same `chunks_exact(4)` fold), encoded in `vemb.rs:47-51`.
   No file owns it.
2. **The numeric-argument error vocabulary** — `parse_usize`/`parse_f32` are **byte-identical
   duplicates** (`vadd.rs:265-285` vs `vsim.rs:255-275`, `diff` clean, verified), and four *more*
   hand-rolled `from_utf8 → map_err → parse → map_err` ladders sit inline in `vlinks.rs:39-46`,
   `vrange.rs:42-49`, `vrange.rs:51-58`, `vrandmember.rs:50-57` — six spellings of one idea, four
   different error messages, in a crate that already exports a canonical `utils::parse_usize`
   which `list.rs`, `sort.rs` and `expiry.rs` all use.
3. **The missing-key reply policy** — eleven `else` branches across eleven files, in **four
   different shapes** (`Integer(0)` ×3, `null` ×5, `Array(vec![])` ×2, and one *error*), with
   nothing that lets a reader see them together.

The change: collapse the twelve command files to **three, grouped by operation class** —
`write.rs` (VADD, VREM, VSETATTR — exactly the three specs carrying `flags: CommandFlags::WRITE`),
`search.rs` (VSIM, VEMB, VLINKS, VRANGE, VRANDMEMBER — the reads that walk the HNSW graph or
iterate elements), `meta.rs` (VCARD, VDIM, VINFO, VGETATTR — scalar projections of set or element
state) — and give facts 1 and 2 a single home in `mod.rs`, where `stream/mod.rs` and `json/mod.rs`
already keep exactly this kind of shared helper.

**Honest deletion-test verdict, stated up front:** the *file move* deletes nothing. What deletes
something is the merge's by-product — one FP32 codec pair instead of three sites, one numeric-parse
helper pair instead of six, ~33 lines of duplicated import preamble, and 9 of 12 `mod`/`pub use`
lines. Fact 3 is **not** deleted, only made visible on one screen; §Deletion test says so plainly
rather than dressing navigability as leverage.

## Corrections to the lane brief

| Brief claim | Ruling at `dd840ca3` |
|---|---|
| "13 files + mod.rs" | **Off by one.** 13 files **total** = 12 command files + `mod.rs`. |
| "`vdim.rs` is 48 lines = 18 lines static SPEC + 8 imports + 12 logic" | **48 lines confirmed; the breakdown is 18 / 6 / 16.** The `spec()` fn is `:13-30` = 18 lines (the `CommandSpec` literal itself is `:14-28` = 15); imports are `:3-8` = **6** lines; `execute` is `:32-47` = **16** lines (body `:33-46` = 14). Direction of the brief's point is right, the arithmetic is not. |
| "vectorset is the outlier" | **Half true, and the softer half matters.** `event_sourcing/` is *also* 1 command per file (6 files, `all.rs`→`ES.ALL`, `append.rs`→`ES.APPEND`, …, verified). vectorset is the **larger** instance (12 files / 1,180 lines vs 6 / 553) and the only one that names files after the *command* rather than the *operation*. `event_sourcing` is explicitly **out of scope** (§Risks) — it would need its own duplication audit, which I did not do. |
| "the vectorset feature gate interacts with file layout" | **Refuted.** The gate is a single `#[cfg(feature = "vectorset")] pub mod vectorset;` at `commands/src/lib.rs:59-60`. There are **zero `#[cfg]` attributes anywhere inside the directory** (verified by grep). File layout and feature gating are orthogonal; the collapse needs no `cfg` care at all. |
| "any test files mirror the layout" | **Refuted.** Two flat regression files (`vectorset_regression.rs` 959, `vectorset_filter_regression.rs` 474) plus one fuzz target (115), all driving the wire protocol through `frogdb_test_harness` with RESP string literals (`vectorset_regression.rs:4-5, 12-21`). **Nothing imports a `vectorset::*` module path.** Zero unit tests live inside the module (no `#[cfg(test)]`, verified). |
| "`git log --follow` still works; say so" | **Refuted as stated — see §Risks.** `--follow` tracks one path through *one* rename at git's default 50% similarity. Only `write.rs` clears that bar. `meta.rs` does not (largest contributor `vinfo.rs` ≈ 38%), and `search.rs` is borderline (`vsim.rs` ≈ 52%). The accurate statement is: `git log -- <old/path>` always works and is unaffected; `--follow` survives for at most one source file per merged file. |
| "small (S), latent" | **Confirmed, both.** No behavior changes on any wire path (§Risks). |

## Files involved

Line counts at `dd840ca3`. All paths under `frogdb-server/crates/` unless noted.

| Path | Lines | Role in this change |
|---|---|---|
| `commands/src/vectorset/mod.rs` | 29 | **Primary.** Module doc `:1-3`; 12 `mod` decls `:5-16`; 12 named `pub use` `:18-29`. Becomes 3 `mod` + 12 `pub use` + the shared FP32 codec and numeric parsers (~75 lines). |
| `commands/src/vectorset/vadd.rs` | 285 | **Primary → `write.rs`.** Real weight: `execute :36-262` is a 227-line hand-rolled option parser (REDUCE/FP32/VALUES/NOQUANT/Q8/BIN/EF/SETATTR/M/CAS/NOTHREAD). **Keeps its own file's worth of body** — the merge does not touch it. Loses its duplicate `parse_usize :265-274` / `parse_f32 :276-285`. |
| `commands/src/vectorset/vsim.rs` | 275 | **Primary → `search.rs`.** Real weight: `execute :37-252`, query-vector source parse + 9 options + filter + brute-force path. **Keeps its body.** Loses `parse_usize :255-264` / `parse_f32 :266-275` — **byte-identical** to vadd's (`diff` clean). |
| `commands/src/vectorset/vrange.rs` | 78 | **Primary → `search.rs`.** Two copies of the same count-parse ladder in one `if/else` (`:42-49`, `:51-58`). |
| `commands/src/vectorset/vinfo.rs` | 77 | **Primary → `meta.rs`.** 12-element base reply `:55-68` + conditional `projection-input-dim` `:70-73`. The **only** vectorset command that *errors* on a missing key (`:37-39`). |
| `commands/src/vectorset/vemb.rs` | 64 | **Primary → `search.rs`.** Holds the FP32 **encode** direction `:47-51` — the inverse of vadd/vsim's decode. |
| `commands/src/vectorset/vlinks.rs` | 64 | **Primary → `search.rs`.** Inline layer-parse ladder `:37-50`, `"Invalid layer number"` written twice in one expression (`:41`, `:45`). |
| `commands/src/vectorset/vrandmember.rs` | 64 | **Primary → `search.rs`.** Inline count-parse ladder `:50-57`, `"Invalid count"` twice (`:52`, `:56`). |
| `commands/src/vectorset/vsetattr.rs` | 57 | **Primary → `write.rs`.** JSON attr parse `:35-43`; `execute` body is 24 lines. |
| `commands/src/vectorset/vrem.rs` | 50 | **Primary → `write.rs`.** `execute` body 17 lines incl. the empty-set key delete `:41-43`. |
| `commands/src/vectorset/vgetattr.rs` | 49 | **Primary → `meta.rs`.** `execute` body 16 lines. |
| `commands/src/vectorset/vdim.rs` | 48 | **Primary → `meta.rs`.** The exemplar. `execute` body 14 lines. |
| `commands/src/vectorset/vcard.rs` | 40 | **Primary → `meta.rs`.** `execute` body **6 lines** — the smallest in the crate's gated families. |
| `commands/src/lib.rs` | 500 | **Read-only.** `#[cfg(feature = "vectorset")] pub mod vectorset;` `:59-60`; registration `:446-460` names the 12 command **types**, not their modules — `registry.register(vectorset::VaddCommand)` `:449` etc. **Unchanged by this proposal**, provided `mod.rs` keeps re-exporting all 12 names. |
| `commands/Cargo.toml` | 78 | **Read-only.** `vectorset = []` `:27`, absent from `default = ["core-profile"]` `:15`, present in `full` `:41`. **Untouched.** |
| `commands/src/utils.rs` | 1,241 | **Read-only — the helper that should have been used.** `:19` re-exports `parse_f64, parse_i64, parse_u64, parse_usize` from `frogdb_core`. |
| `types/src/args.rs` | 400+ | **Read-only — the canonical parsers.** `parse_i64 :308-311`, `parse_usize :316-319`; both return `CommandError::NotInteger`. Re-exported at `types/src/lib.rs:30`. **No `f32` parser exists** — only `parse_f64 :328`. |
| `types/src/error.rs` | 300+ | **Read-only — why H2 is a wire change.** `InvalidArgument { message } => "ERR {message}"` `:119`; `NotInteger => "ERR value is not an integer or out of range"` `:132`. |
| `commands/src/stream/mod.rs` | 199 | **Read-only — the precedent.** `mod` decls `:20-24`, glob re-exports `:26-30`, then **shared `pub(crate)` helpers in the module root**: `entry_to_response :41`, `parse_trim_options :53`, `parse_delete_ref_strategy :130`, `parse_ids_block :156`. Exactly the shape `vectorset/mod.rs` needs. |
| `commands/src/json/mod.rs` | 131 | **Read-only — the second precedent.** Same shape: `parse_path :32`, `parse_json_value_limited :46`, `enforce_growth_limits :62`, `json_error_to_command_error :75`, `single_or_multi :122`. |
| `commands/src/sorted_set/mod.rs` | 27 | **Read-only — the counter-example.** Pure re-export module, no helpers; 8 files hold 32 commands. |
| `redis-regression/tests/vectorset_regression.rs` | 959 | **Read-only — the acceptance oracle.** Wire-level, harness-driven. Must pass **byte-identically, unmodified**. |
| `redis-regression/tests/vectorset_filter_regression.rs` | 474 | **Read-only — same.** |
| `testing/fuzz/fuzz_targets/vectorset_ops.rs` | 115 | **Read-only.** Drives commands by name; layout-blind. |
| `Justfile` | — | **Read-only — gate check.** `lint-no-typed-unwrap :1012-1035` is the only gate touching this crate and it is a **recursive grep over `crates/commands/src/`** — directory-scoped, so file moves are invisible to it. This change introduces neither `as_*_mut().unwrap()` nor `.ok_or(…WrongType…)`. `lint-info-seam :423-441` is scoped to three `server/src` files. **No gate is affected.** |
| `frogdb-server/ops/docs-gen/src/main.rs` | — | **Read-only — proof the generated docs are layout-blind.** `derive_family(name)` `:497, :628` derives a command's family from its **dot-namespaced name prefix**, not its source path. `commands.json` cannot observe this change. |

## Problem

### 1. Twelve files, twelve commands — against the crate's own convention

Commands per file, counted by `grep -rc ': CommandSpec = CommandSpec {'` across all five
submodules of `commands/src`:

| Submodule | Files | Commands | Cmds/file | Total lines | Mean file |
|---|---|---|---|---|---|
| `sorted_set` | 8 | 32 | **4.0** | 2,549 | 319 |
| `json` | 6 | 21 | **3.5** | 1,270 | 212 |
| `stream` | 5 | 17 | **3.4** | 2,316 | 463 |
| `event_sourcing` | 6 | 6 | 1.0 | 553 | 92 |
| **`vectorset`** | **12** | **12** | **1.0** | **1,180** | **96** |

The three large families all split by *operation class* — `sorted_set/{basic,range,rank,pop,count,scan,set_ops,store_remove}`, `stream/{basic,read,pending,consumer_groups,info}`,
`json/{basic,array,object,numeric,string,mutation}`. `vectorset` splits by *command identity*, and
does so at twice `event_sourcing`'s scale. The median vectorset file is 57 lines; the median
`sorted_set` file is 279.

### 2. The identical preamble, nine times over

`vcard.rs`, `vdim.rs`, `vgetattr.rs`, `vrem.rs`, `vsetattr.rs` (`:3-8`) and `vemb.rs`,
`vlinks.rs`, `vrandmember.rs`, `vrange.rs` (`:5-10`) carry a **byte-identical** six-line block —
verified, all nine hash to md5 `1f545a7cb9b49b8476647af3a9242a7b`:

```rust
use bytes::Bytes;
use frogdb_core::{
    AccessSpec, Arity, Command, CommandContext, CommandError, CommandFlags, CommandSpec, EventSpec,
    ExecutionStrategy, KeySpec, LookupSpec, StoreTypedFamilyExt, WaiterWake, WalStrategy,
};
use frogdb_protocol::Response;
```

Fourteen names from `frogdb_core`, of which **four exist only to spell defaults in the `spec()`
literal** (`AccessSpec`, `ExecutionStrategy`, `LookupSpec`, `WaiterWake` — this is precisely the
population proposal 90's import prune targets; §Siblings). `vinfo.rs` adds two names, `vadd.rs`
four, `vsim.rs` one. **54 lines of preamble for 9 files**; three merged files need ~21.

### 3. Two byte-identical helper pairs, and four more hand-rolled copies of the same ladder

`vadd.rs:265-285` and `vsim.rs:255-275` are the same 21 lines — `diff` returns clean:

```rust
fn parse_usize(b: &[u8]) -> Result<usize, CommandError> {
    std::str::from_utf8(b)
        .map_err(|_| CommandError::InvalidArgument { message: "Invalid integer".to_string() })?
        .parse()
        .map_err(|_| CommandError::InvalidArgument { message: "Invalid integer".to_string() })
}
fn parse_f32(b: &[u8]) -> Result<f32, CommandError> { /* … "Invalid float" … */ }
```

They are not the only copies. The same `from_utf8 → map_err → parse → map_err` ladder, with the
error literal written **twice inside a single expression**, appears inline at:

| Site | Parses | Error literal (written twice) |
|---|---|---|
| `vlinks.rs:39-46` | `usize` layer | `"Invalid layer number"` `:41`, `:45` |
| `vrandmember.rs:50-57` | `i64` count | `"Invalid count"` `:52`, `:56` |
| `vrange.rs:42-49` | `usize` count (`COUNT` form) | `"Invalid count"` `:44`, `:48` |
| `vrange.rs:51-58` | `usize` count (bare form) | `"Invalid count"` `:53`, `:57` |

So **six** spellings of "parse a number out of a vectorset argument", across **five** files,
producing **four** different client-visible errors — while `commands/src/utils.rs:19` re-exports a
canonical `parse_usize`/`parse_i64` that `list.rs:23`, `sort.rs:14` and `expiry.rs:122` already
import. `vectorset/` is the family that opted out, and the twelve-file split is what let it opt
out unnoticed: no two of those six sites are ever open in the same editor tab.

### 4. The FP32 wire format has three homes and no owner

Decode, `vadd.rs:81-89`:

```rust
if !blob.len().is_multiple_of(4) {
    return Err(CommandError::InvalidArgument {
        message: "FP32 blob length must be a multiple of 4".to_string(),
    });
}
vector = blob.chunks_exact(4)
    .map(|c| f32::from_le_bytes([c[0], c[1], c[2], c[3]]))
    .collect::<Vec<f32>>();
```

Decode again, `vsim.rs:81-89` — same check, same error string, same fold, different binding.
Encode, `vemb.rs:47-51`:

```rust
let mut blob = Vec::with_capacity(vec.len() * 4);
for &v in vec { blob.extend_from_slice(&v.to_le_bytes()); }
```

"A FrogDB vector on the wire is little-endian `f32`" is an interoperability contract with clients.
It is currently an emergent property of three unconnected code blocks. Change the endianness in
two of the three and the round-trip test still passes.

**Adjacent, verified, and deliberately not "fixed" here:** the two `VALUES` bounds checks differ —
`vadd.rs:100` uses `i + count >= rest.len()`, `vsim.rs:99` uses `i + count > rest.len()`. **Both
are correct**: VADD must reserve one trailing slot for the element name, VSIM must not. That
asymmetry is exactly why a shared `VALUES` parser needs an explicit `reserve_trailing` parameter
rather than a blind merge — and exactly the class of off-by-one that a shared helper with a named
parameter makes reviewable. §Proposed change treats the FP32 codec as in-scope and the `VALUES`
loop as **out** of scope for that reason.

### 5. The missing-key policy is stated eleven times in four shapes

| Command | Missing-key reply | Site |
|---|---|---|
| VCARD | `Integer(0)` | `vcard.rs:37` |
| VREM | `Integer(0)` | `vrem.rs:47` |
| VSETATTR | `Integer(0)` | `vsetattr.rs:54` |
| VDIM | `null` | `vdim.rs:45` |
| VEMB | `null` | `vemb.rs:40` |
| VGETATTR | `null` | `vgetattr.rs:37` |
| VLINKS | `null` | `vlinks.rs:53` |
| VRANDMEMBER | `null` | `vrandmember.rs:38` |
| VRANGE | `Array(vec![])` | `vrange.rs:65` |
| VSIM | `Array(vec![])` | `vsim.rs:42` |
| **VINFO** | **`Err("Key does not exist")`** | `vinfo.rs:37-39` |
| VADD | *(creates the key)* | — |

Eleven branches, four shapes, one outlier. **This proposal does not change any of them** — the
per-command shapes may well all be correct against upstream, and I did not verify them against
Redis 8 (§Adjacent findings). The point is narrower and provable: today no reader can see the
column above without opening eleven files. After the collapse it is three files, and the VINFO
outlier sits four lines from VDIM's `null`.

## Proposed change

### The grouping

Three command files plus `mod.rs`. The cut is **spec-derived where it can be** and domain-derived
where it cannot:

| New file | Commands | Membership predicate | ≈ lines |
|---|---|---|---|
| `vectorset/write.rs` | VADD, VREM, VSETATTR | **Exactly the specs with `flags: CommandFlags::WRITE`** and a non-`NoOp` `wal:` — verified: `vadd.rs:21/24` (`PersistFirstKey`), `vrem.rs:17/20` (`PersistOrDeleteFirstKey`), `vsetattr.rs:17/20` (`PersistFirstKey`). All other nine are `READONLY` + `WalStrategy::NoOp`. | ~355 |
| `vectorset/search.rs` | VSIM, VEMB, VLINKS, VRANGE, VRANDMEMBER | Reads that walk the HNSW graph or iterate elements — they return element *collections* and take element-selection options. | ~475 |
| `vectorset/meta.rs` | VCARD, VDIM, VINFO, VGETATTR | Reads that project a single scalar (or one flat metadata array) out of set or element state; bodies are 6–41 lines. | ~180 |
| `vectorset/mod.rs` | — | Doc, 3 `mod`, 12 named `pub use`, plus the shared helpers below. | ~75 |

`write.rs` is the one boundary a future editor cannot get wrong: it is *checkable from the spec
literal*, and it is also the boundary that matters most operationally — those three commands are
the ones that hit the WAL, the replication feed, and the keyspace-event suppression path. That is
the first question a reader opening a vectorset file needs answered, and today the answer is
scattered across twelve `flags:` lines.

**The cut's one cost, stated:** VGETATTR (read) and VSETATTR (write) land in different files. That
is the price of making `write.rs` spec-checkable, and it is small — both bodies are under 25 lines
and each `impl` gets a one-line doc cross-reference. The alternative that keeps them together —
splitting by *subject* (set-level vs element-level) — would put VADD next to VGETATTR and give up
the WRITE predicate entirely. **A two-file variant** (`write.rs` + `read.rs`, the latter ~725
lines) is a legitimate simplification of this proposal and within crate norms
(`sorted_set/set_ops.rs` is 802); it is not preferred, because `meta.rs`'s four commands are
exactly the ones whose missing-key inconsistency (§Problem 5) is worth reading as a block.

### The shared surface in `mod.rs`

Following `stream/mod.rs:41-199` and `json/mod.rs:32-131` verbatim in shape — private/`pub(super)`
helpers in the module root, below the re-exports:

```rust
/// FrogDB's FP32 vector wire encoding: little-endian 4-byte `f32`, one per
/// dimension. This pair is the single authority for that format; VADD and VSIM
/// decode with it, VEMB encodes with it.
pub(super) fn decode_fp32(blob: &[u8]) -> Result<Vec<f32>, CommandError> { … }
pub(super) fn encode_fp32(v: &[f32]) -> Bytes { … }

/// The vectorset family's numeric argument parsers.
pub(super) fn parse_usize(b: &[u8]) -> Result<usize, CommandError> { … }  // moved verbatim
pub(super) fn parse_f32(b: &[u8]) -> Result<f32, CommandError> { … }      // moved verbatim
```

`parse_usize`/`parse_f32` **move verbatim**, error strings unchanged — see H2 for the separate,
deliberate question of whether they should instead defer to `utils::parse_usize`. The four inline
ladders in `vlinks`/`vrange`/`vrandmember` are rewritten to call `parse_usize` (or `parse_i64` for
VRANDMEMBER's `i64`) — **this is a wire change** and is therefore split out as H2, *not* folded
into the move. The mechanical collapse itself must be byte-identical on every path.

### Vocabulary, and what this change actually is

`vectorset` is the *module*; the twelve `impl Command for V*Command` blocks are the *interface* and
are **untouched** — same types, same names, same `mod.rs` re-exports, so `lib.rs:446-460`'s twelve
`registry.register(…)` lines compile unchanged and the crate's public surface is bit-for-bit
identical. `mod.rs`'s helper section becomes a genuine *seam*: the one place the FP32 format and
the family's numeric-argument vocabulary are decided. It is deliberately **not** an *adapter* —
nothing crosses a crate boundary — and the *leverage* is honestly low: three call sites unified,
not thirty. The *locality* gain is the real product: 1,180 lines that today require twelve file
opens to survey become three files, and the two duplications that the split concealed become
impossible to reintroduce without noticing.

## Deletion test

Applied honestly, one question at a time.

**Q: If `mod.rs`'s `decode_fp32`/`encode_fp32` pair were deleted, what fact would have no home?**
"A FrogDB vector on the wire is little-endian 4-byte `f32`, and a blob whose length is not a
multiple of 4 is a client error." Today that fact has **three partial homes**
(`vadd.rs:81-89`, `vsim.rs:81-89`, `vemb.rs:47-51`), none authoritative, and the encode side shares
no code with either decode side. **Passes.**

**Q: If `mod.rs`'s `parse_usize`/`parse_f32` were deleted?** "How this family reports a
malformed numeric argument." Today: six sites, four messages, two byte-identical function copies.
**Passes** — though the *fully* correct answer is `utils::parse_usize`, which is H2's question and
a wire change.

**Q: If `write.rs`/`search.rs`/`meta.rs` were three files instead of twelve — what fact gains a
home?** **Nothing. This part fails the deletion test, and the proposal says so.** Grouping twelve
`impl` blocks into three files moves complexity; it does not concentrate it. The payoff there is
navigability (one survey instead of twelve opens; §Problem 5's table becomes readable), import
hygiene (−33 lines of identical preamble), and convention conformance (§Problem 1). Those are real
and they are worth an S; they are not leverage, and pricing them as leverage would be dishonest.

**Q: What about the missing-key policy table (§Problem 5)?** It is **not** unified by this change
and gets no new home. It becomes *visible*. That is a precondition for someone later ruling on it,
not a fix.

## Testability improvement

The module has **zero unit tests** (no `#[cfg(test)]` anywhere in the directory, verified) and its
entire coverage is two wire-level regression files that never import a module path. Three concrete
gains, none of which requires the collapse but two of which the collapse makes practical:

1. **A place to put unit tests at all.** Today a `#[cfg(test)] mod tests` for the FP32 codec would
   have to be written twice (vadd's copy and vsim's copy) or arbitrarily in one of them. After the
   collapse, `mod.rs` gets one test module covering `decode_fp32`/`encode_fp32` round-trip,
   odd-length rejection, and endianness — the first in-crate test the family has ever had, and the
   thing that makes the §Problem 4 contract *checked* rather than *coincidental*. This is the
   single highest-value test in the proposal and it is unavailable without a shared home.
2. **A parse-helper test module.** `parse_usize`/`parse_f32` currently have zero direct coverage:
   grepping `"Invalid integer"`, `"Invalid float"`, `"Invalid count"`, `"Invalid layer number"`
   across both regression files returns **nothing** (verified). Every malformed-argument path in
   the family is untested. Post-collapse these are four unit tests in one file.
3. **The mechanical acceptance oracle already exists — twice.** `vectorset_regression.rs` (959
   lines) and `vectorset_filter_regression.rs` (474) must pass **unmodified and byte-identically**;
   they are layout-blind, so any diff in their output is a real regression from the move. And
   proposal 90's **commit 1** — the registry-driven `spec_golden` test comparing
   `format!("{:?}", cmd.spec())` per command name — is *exactly* the net for a file move: it is
   indifferent to which file a spec lives in and would catch a spec literal mangled during the
   cut. §Siblings makes landing 90-commit-1 first the recommended sequence for this reason.

**Mutation posture.** `frogdb-commands` is not one of the four locked crates
(`txn`/`persistence`/`replication`/`cluster`, ADRs `0002`–`0004`); no `just mutants-gate` applies
and `just lint-failure-modes` is vacuously clean (**zero `FM-` tags in `commands/src`**, verified).
Worth noting for scale: today, mutating `vsim.rs:81` (`is_multiple_of(4)` → `true`) survives
whatever kills the identical mutation in `vadd.rs:81`, because they are separate code. After the
collapse there is one site, and one killing test kills it for all three FP32 users.

## Risks / scope boundaries vs siblings

### Behavioral risk: none, if the collapse stays mechanical

Every `impl Command` block, every `static SPEC` literal, every `execute` body, and every error
string moves **verbatim**. The only edits are: (a) delete one of the two identical
`parse_usize`/`parse_f32` pairs and re-path the calls; (b) replace the three FP32 blocks with calls
to the shared pair, preserving the error string `"FP32 blob length must be a multiple of 4"`
exactly; (c) merge import blocks; (d) rewrite `mod.rs`'s `mod`/`pub use` lines. Acceptance is both
regression files passing unmodified plus `just check frogdb-commands --features vectorset`.
**The `VALUES` bounds asymmetry (§Problem 4) is explicitly out of scope** — merging that loop is
where a real bug would be introduced, and it buys ~10 lines.

### Git history: the brief's claim needs qualifying

Git detects renames by content similarity at a **50% default threshold**, and `--follow` tracks one
path. Estimating each merged file's dominant contributor after dedup and import merge:

| New file | Dominant source | Share | `--follow` verdict |
|---|---|---|---|
| `write.rs` (~355) | `vadd.rs` (~265 after dedup) | ~75% | **Detected** — history follows to `vadd.rs` |
| `search.rs` (~475) | `vsim.rs` (~255 after dedup) | ~54% | **Borderline** — may or may not be detected |
| `meta.rs` (~180) | `vinfo.rs` (~69) | ~38% | **Not detected** |

So: **`git log -- frogdb-server/crates/commands/src/vectorset/vdim.rs` keeps working forever**
(path-based log is unaffected by rename detection), and `git log -S<symbol>` / `git log -G` find
any moved body. **`--follow` is what degrades**, for 9 of the 12 files. The substantive loss is
near zero: the directory has **9 commits in its entire life** (`git log --oneline -- vectorset/`),
of which 8 are directory-wide sweeps (`refactor(core): require CommandSpec…`, `refactor(commands):
migrate vector set to typed store accessors`, …) and one is the original authoring commit
`cd047359` — which created **all twelve files at once**. There is no per-command narrative to
lose; there never was one.

### Sibling 90 (`CommandSpec::DEFAULT`) — the ordering constraint, stated precisely

This is the only sibling with a real interaction, and the brief's framing needs one correction.

**What is *not* true:** that 99 "invalidates 90's derived positions." 90's sweep (`90:274-291`) is
an `awk` driven by `grep -rl ': CommandSpec = CommandSpec {' .` and anchored on *exact strings at
fixed indentation* — never on line numbers. It re-derives its own file list at run time and works
on any file containing the pattern. **The sweep script survives a file collapse untouched.**

**What *is* true:** 90's *verification arithmetic* is stated in file counts, and 99 changes them.
Concretely, at HEAD I verified 90's headline numbers hold — `grep -rl` returns **56** files, total
sites **296**, of which **vectorset contributes 12** (matching 90's family table at `90:431`).
After 99, the same greps return **47 files / 296 sites**. And 90's import-prune table (`90:318-326`
— `LookupSpec` 48 files, `ExecutionStrategy` 48, `WaiterWake` 48, `AccessSpec` 38, "**182 dead
import names across 54 of the 56 files**") is derived per-file: all 12 vectorset files are in each
of those four buckets today (verified — each name appears in the file *only* on its spec line), and
after 99 they contribute **3** instead of 12. The counts become **39 / 39 / 39 / 29 = 146 names
across 45 of 47 files**. 90's commit-3 acceptance step "review the diff against the 182/54 table"
must be re-derived, which is one grep.

**Ordering, therefore:**

- **90 commit 1 (the `spec_golden` test) SHOULD land first**, before 99. It is registry-driven and
  file-layout-indifferent, and it is the strongest possible net for a file move — it pins all 13
  fields of all 12 vectorset specs by command name, so a spec literal damaged during the cut fails
  loudly. 90 itself already argues commit 1 is independently landable "any time — early is better,
  since the golden protects the siblings too" (`90:526-528`). This proposal is the concrete case
  for that.
- **99 lands before 90 commit 3.** This is not a preference; it is 90's own ruling
  (`90:507`, "**land SOLO, and land LAST** … commit 3 (the sweep) lands after 67/70/80/89 are
  merged, re-derived from the merged tree"). 99 is another commands-crate writer and joins that
  list. The asymmetry 90 states holds here exactly: 90 is *regenerable* (re-run the awk, re-run the
  golden), 99 is not cheaply rebasable across a 1,900-line deletion inside every block it moves.
- **If the order is inverted anyway**, 99 is still mechanically fine — it would be moving already
  swept 9-line spec blocks instead of 15-line ones — but it forces 99 to rebase 12 spec literals
  across 90's sweep, and re-invalidates 90's counts in the other direction. No reason to.

**The compounding is real and worth stating.** After 90, each vectorset spec literal drops from 15
lines to **9** (13 fields → 6 explicit + `..CommandSpec::DEFAULT`), and the shared import block
loses its four default-only names. `vdim.rs` post-90 is ≈ **40 lines for a 14-line function** — the
disproportion the brief describes gets *worse*, not better, which is why 99 is the natural follow-on
to 90 rather than a competitor to it.

### Siblings with no overlap

- **89 (`probabilistic-chunk-codec`)** — `bloom.rs` + `cuckoo.rs` + `commands/Cargo.toml:54`.
  **No file overlap.** 99 touches no `Cargo.toml`.
- **95 (`BF_INFO_FIELDS`)** — `bloom.rs` + `bloom_regression.rs`. It cites `vectorset/vinfo.rs:11`
  and `:70-73` as **read-only evidence** and explicitly scopes VINFO **out** (`95:384-386`: the
  conditional `projection-input-dim` field cannot be expressed in a flat table). 99 moves
  `vinfo.rs`'s body into `meta.rs` unchanged. **The only interaction is stale path citations in
  95's prose** — if 99 lands first, 95's evidence rows should read `vectorset/meta.rs`. No code
  conflict either way; both can land in any order.
- **85 (`frogdb-macros-fate`)**, **71 (`search-query-plan`)** — both name vectorset in passing
  (grep hit) but neither edits `commands/src/vectorset/`. 71's subject is the query planner; 85's
  is the macros crate. **No conflict.**
- **91, 92, 93, 94, 96** — commands-crate lane siblings on `expiry.rs`, `hash.rs`, `geo.rs`,
  RESP3 shaping. **No file overlap with `vectorset/`.** They join 90's "land before commit 3" queue
  alongside 99.

### Scope boundaries

- **`event_sourcing/` is out of scope.** It has the identical 1-command-per-file shape (6 files,
  553 lines) and would be the obvious "while we're here". I did not audit it for the duplications
  that justify 99 (§Problem 3/4), so proposing it would be extrapolating. It deserves its own
  verification pass; if the same shared-fact duplication is present, the same argument applies.
- **No behavior change ships with the collapse.** H2 is where behavior changes live, deliberately
  separated.
- **The 12 exported type names do not change.** `VaddCommand` … `VsimCommand` stay exactly as
  `lib.rs:446-460` and any external consumer see them. `mod.rs` keeps **named** re-exports rather
  than adopting the sibling `pub use module::*` glob (`stream/mod.rs:26-30`,
  `sorted_set/mod.rs:20-27`, `json/mod.rs:19-24`) — the explicit list is better hygiene and its 12
  lines are not the problem; the 12 `mod` declarations above them are.

## Effort

**S.** Four files where there were thirteen; net ≈ **−50 lines** (−33 duplicated preamble,
−21 duplicate helper pair, −~9 FP32 dedup, −9 `mod` lines, +~45 for `mod.rs`'s helper section and
its doc comments, plus ~+20 of section banners in the merged files). Zero new crate edges, zero
`Cargo.toml` change, zero `cfg` work (the feature gate is a single line at `lib.rs:59-60` and is
untouched), zero generated-artifact regeneration (`docs-gen` derives family from the command
**name**, `main.rs:497,628` — layout is invisible to it), zero gate impact
(`lint-no-typed-unwrap` is directory-scoped, `Justfile:1012-1035`).

The work is mostly `git mv`-shaped and reviewable by diff shape: every hunk should be either a pure
move, an import merge, or one of the two dedups. Iteration loop is
`cargo check -p frogdb-commands --features vectorset --all-targets` — note that plain
`just check frogdb-commands` runs at `default = core-profile` and **will not compile this module at
all** (`Cargo.toml:15,27,41`), which is the single most likely way to waste an hour on this change.
Acceptance is `cargo nextest run -p frogdb-redis-regression vectorset` with both regression files
unmodified.

**Independently-landable hotfixes.**

**H1 — hoist the duplicate `parse_usize`/`parse_f32` pair into `mod.rs`, no file collapse.**
Delete `vsim.rs:255-275`, move `vadd.rs:265-285` to `vectorset/mod.rs` as `pub(super)`, point both
call sites at it. **−21 lines, byte-identical behavior, touches 3 files, conflicts with no
sibling**, and it is the one piece of §Problem 3 that is pure loss with no design question
attached. Ships today, ships alone, and makes the full proposal smaller if it ever lands.

**H2 — route the family's six numeric parses through `utils::parse_usize`/`parse_i64`. Ruling
required, not a free fix.** This is a **wire change**: the local helpers emit
`ERR Invalid integer` / `ERR Invalid count` / `ERR Invalid layer number`
(`InvalidArgument { message } => "ERR {message}"`, `types/src/error.rs:119`) whereas the canonical
helper emits `ERR value is not an integer or out of range` (`error.rs:132`) — which is what real
Redis returns for a malformed numeric argument, so this is very likely a **compatibility
improvement**, not merely a normalization. **Zero tests observe the current strings** (verified by
grep across both regression files), so it would land silently either way — which is the argument
for adding the four assertions *first*. Flagged, sized S, and left to a compat ruling rather than
smuggled into a file move.

No other hotfix is claimed. There is no known live defect in the vectorset command family, and this
proposal does not assert one.

## Adjacent findings (evidence, not proposals)

Surfaced while verifying. Each is out of scope; none is asserted as a defect without its caveat.

1. **VINFO is the only vectorset command that errors on a missing key** (`vinfo.rs:37-39`,
   `"Key does not exist"`), against `null` from VDIM/VEMB/VGETATTR/VLINKS/VRANDMEMBER and `0`/`[]`
   from the rest (§Problem 5). **Unverified against upstream** — I could not check Redis 8's VINFO
   from this environment. Noted because proposal 95 records the identical shape for `BF.INFO`
   (`bloom.rs:447`) and five other `*.INFO` commands, so it may be a deliberate house pattern
   rather than a divergence. A compat-audit item spanning all seven, not a change here.
2. **VSIM accepts and silently discards `EF`, `FILTER-EF`, and `EPSILON` values**
   (`vsim.rs:160-168`: "Accept but skip value argument"). Likewise VADD's `CAS`/`NOTHREAD`
   (`vadd.rs:167-169`) and VSIM's `NOTHREAD` (`:169-171`), both documented as no-ops in FrogDB's
   single-threaded shard model. The `CAS`/`NOTHREAD` no-ops are defensible; silently ignoring a
   *tuning* parameter that changes recall (`EF`, `EPSILON`) is a different category. Whether this
   is documented as a deviation is unchecked.
3. **`VADD … EF` and `VADD … M` are only honored on key creation** (`vadd.rs:228-229` reads them
   solely in the `else` branch); on an existing key both are parsed, validated, and dropped
   (`:194-225` never reads `ef` or `m_param`). Matches upstream's semantics as far as I know, but
   the code gives no signal that the drop is intentional.
4. **VSIM's `TRUTH` + `FILTER` path scans the whole set** (`vsim.rs:205-213`: `vs.card()` as the
   brute-force fetch count) with no cap. Correct for exhaustive search by definition; noted as an
   unbounded-work path with no configured ceiling, unlike the `count * 4 + 10` over-fetch on the
   indexed path (`:199-203`).
5. **`vemb.rs:56` renders floats with `format!("{v}")` and `vsim.rs:237` with
   `format!("{}", hit.score)`**, rather than the canonical `format_float`
   (`protocol/src/format.rs`, re-exported at `commands/src/utils.rs:27`). Same class as proposal
   95's adjacent finding 4 for `topk`/`tdigest`; `lint-format-float` pins definitions, not call
   sites, so both are outside the gate. A float-rendering audit item spanning several families.
