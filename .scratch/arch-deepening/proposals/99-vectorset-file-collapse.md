# Proposal 99 — `vectorset/`: twelve files, twelve commands, and three facts with no home

Round 38 · lane: commands · effort **S** · **no locked crate edited** · **zero `FM-` tags in any
edited file** (verified: `grep -rn "FM-" frogdb-server/crates/commands/src` returns nothing)

**Verified at HEAD `dd840ca3bb3a70319c424d62885e753e51abfdf5`** (worktree `arch-round-38-99`,
branch `worktree-arch-round-38-99`). Every path, line count, line number and hash below was
re-derived at this SHA. Nothing is inherited from the lane brief. **No code file in this set is
dirty**; the only dirty paths in the tree are `.scratch/arch-deepening/proposals/*.md` held by
concurrent authors.

**Revision 2 (adversarial review applied).** Re-verified at `3663b197`;
`git diff dd840ca3..HEAD -- frogdb-server testing Justfile` is **empty**, so every line number
below still holds. This pass: adds **§Problem 6** (two live DoS-class defects the authored draft
missed and its closing sentence explicitly denied), **reverses** §Problem 4's ruling that the
`VALUES` loop is out of scope, **withdraws** §Problem 5 as a reason to prefer three files over two,
corrects the helper visibility from `pub(super)` to private, narrows H2 to integer parses, and
fixes eleven counting/citation errors (`lib.rs` 473 not 500, `Cargo.toml` 60 not 78, 11 commits not
9, median 64 not 57, net ≈ −7 not −50, `--follow` degrades 10-11 of 12 not 9, the fuzz target's
actual entry point, and the 90/95 line citations). **Nothing in §Problem 6 is fixed by this
proposal** — those are filed issues, parked.

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

And what the split *concealed*: **two live DoS-class defects** (§Problem 6) that are visible the
moment the sites sit on one screen — an unchecked `i + count` on an attacker-supplied `usize` in
both `VALUES` bounds checks (plus a third overflow at `vsim.rs:200`), and a missing `i64::MIN`
guard in VRANDMEMBER that three sibling families carry with an explaining comment. **Both are
documented here and fixed nowhere**: they are filed as issues, and this proposal writes no fix for
either (security work is parked per standing user direction).

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
rather than dressing navigability as leverage. The one thing the collapse buys beyond navigability
and dedup is the **seam the two defects need**: a single `VALUES` parser and a single numeric
vocabulary are the places a bounds check can be written once and reviewed once (§Problem 6).

## Corrections to the lane brief

| Brief claim | Ruling at `dd840ca3` |
|---|---|
| "13 files + mod.rs" | **Off by one.** 13 files **total** = 12 command files + `mod.rs`. |
| "`vdim.rs` is 48 lines = 18 lines static SPEC + 8 imports + 12 logic" | **48 lines confirmed; the breakdown is 18 / 6 / 16.** The `spec()` fn is `:13-30` = 18 lines (the `CommandSpec` literal itself is `:14-28` = 15); imports are `:3-8` = **6** lines; `execute` is `:32-47` = **16** lines (body `:33-46` = 14). Direction of the brief's point is right, the arithmetic is not. |
| "vectorset is the outlier" | **Half true, and the softer half matters.** `event_sourcing/` is *also* 1 command per file (6 files, `all.rs`→`ES.ALL`, `append.rs`→`ES.APPEND`, …, verified). vectorset is the **larger** instance (12 files / 1,180 lines vs 6 / 553) and the only one that names files after the *command* rather than the *operation*. `event_sourcing` is explicitly **out of scope** (§Risks) — it would need its own duplication audit, which I did not do. |
| "the vectorset feature gate interacts with file layout" | **Refuted.** The gate is a single `#[cfg(feature = "vectorset")] pub mod vectorset;` at `commands/src/lib.rs:59-60`. There are **zero `#[cfg]` attributes anywhere inside the directory** (verified by grep). File layout and feature gating are orthogonal; the collapse needs no `cfg` care at all. |
| "any test files mirror the layout" | **Refuted.** Two flat regression files (`vectorset_regression.rs` 959, `vectorset_filter_regression.rs` 474) plus one fuzz target (115), all driving the wire protocol through `frogdb_test_harness` with RESP string literals (`vectorset_regression.rs:4-5, 12-21`). **Nothing imports a `vectorset::*` module path.** Zero unit tests live inside the module (no `#[cfg(test)]`, verified). |
| "`git log --follow` still works; say so" | **Refuted as stated — see §Risks.** `--follow` tracks one path through *one* rename at git's default 50% similarity. Only `write.rs` clears that bar. `meta.rs` does not (largest contributor `vinfo.rs` ≈ 38%), and `search.rs` is borderline (`vsim.rs` ≈ 54%). The accurate statement is: `git log -- <old/path>` always works and is unaffected; `--follow` survives for **at most one** source file per merged file — i.e. it degrades for **11 of the 12** files, or 10 if `search.rs` is detected. |
| "small (S), latent" | **Half refuted.** S is confirmed and the *collapse* is behavior-preserving (§Risks). "Latent" is not: the family carries **two live DoS-class defects**, reachable by any client permitted to issue VADD/VSIM/VRANDMEMBER (§Problem 6). They are **not fixed here** — documented, filed, parked. |

## Files involved

Line counts at `dd840ca3`. All paths under `frogdb-server/crates/` unless noted.

| Path | Lines | Role in this change |
|---|---|---|
| `commands/src/vectorset/mod.rs` | 29 | **Primary.** Module doc `:1-3`; 12 `mod` decls `:5-16`; 12 named `pub use` `:18-29`. Becomes 3 `mod` + 12 `pub use` + the shared FP32 codec and numeric parsers (~75 lines). |
| `commands/src/vectorset/vadd.rs` | 285 | **Primary → `write.rs`.** Real weight: `execute :36-262` is a 227-line hand-rolled option parser (REDUCE/FP32/VALUES/NOQUANT/Q8/BIN/EF/SETATTR/M/CAS/NOTHREAD). **Keeps its own file's worth of body** — the merge does not touch it. Loses its duplicate `parse_usize :265-274` / `parse_f32 :276-285`. Also holds **defect F1 site 1**: `i + count >= rest.len()` `:100` on an attacker `usize` from `:97` (§Problem 6). |
| `commands/src/vectorset/vsim.rs` | 275 | **Primary → `search.rs`.** Real weight: `execute :37-252`, query-vector source parse + 9 options + filter + brute-force path. **Keeps its body.** Loses `parse_usize :255-264` / `parse_f32 :266-275` — **byte-identical** to vadd's (`diff` clean). Holds **F1 sites 2 and 3**: `i + count > rest.len()` `:99`, and `count * 4 + 10` `:200` on the `COUNT` value parsed at `:132` (§Problem 6). |
| `commands/src/vectorset/vrange.rs` | 78 | **Primary → `search.rs`.** Two copies of the same count-parse ladder in one `if/else` (`:42-49`, `:51-58`). |
| `commands/src/vectorset/vinfo.rs` | 77 | **Primary → `meta.rs`.** 12-element base reply `:55-68` + conditional `projection-input-dim` `:70-73`. The **only** vectorset command that *errors* on a missing key (`:37-39`). |
| `commands/src/vectorset/vemb.rs` | 64 | **Primary → `search.rs`.** Holds the FP32 **encode** direction `:47-51` — the inverse of vadd/vsim's decode. |
| `commands/src/vectorset/vlinks.rs` | 64 | **Primary → `search.rs`.** Inline layer-parse ladder `:37-50`, `"Invalid layer number"` written twice in one expression (`:41`, `:45`). |
| `commands/src/vectorset/vrandmember.rs` | 64 | **Primary → `search.rs`.** Inline count-parse ladder `:50-57`, `"Invalid count"` twice (`:52`, `:56`). Holds **defect F2**: the parsed `i64` goes to `vs.rand_member(count)` `:59` with **no `i64::MIN` guard** and no magnitude cap (§Problem 6). |
| `commands/src/vectorset/vsetattr.rs` | 57 | **Primary → `write.rs`.** JSON attr parse `:35-43`; `execute` body is 24 lines. |
| `commands/src/vectorset/vrem.rs` | 50 | **Primary → `write.rs`.** `execute` body 17 lines incl. the empty-set key delete `:41-43`. |
| `commands/src/vectorset/vgetattr.rs` | 49 | **Primary → `meta.rs`.** `execute` body 16 lines. |
| `commands/src/vectorset/vdim.rs` | 48 | **Primary → `meta.rs`.** The exemplar. `execute` body 14 lines. |
| `commands/src/vectorset/vcard.rs` | 40 | **Primary → `meta.rs`.** `execute` body **6 lines** — the smallest in the crate's gated families. |
| `commands/src/lib.rs` | 473 | **Read-only.** `#[cfg(feature = "vectorset")] pub mod vectorset;` `:59-60`; registration `:446-460` names the 12 command **types**, not their modules — `registry.register(vectorset::VaddCommand)` `:449` etc. **Unchanged by this proposal**, provided `mod.rs` keeps re-exporting all 12 names. |
| `commands/Cargo.toml` | 60 | **Read-only.** `vectorset = []` `:27`, absent from `default = ["core-profile"]` `:15`, present in `full` `:31-41`. **Untouched.** |
| `commands/src/utils.rs` | 1,241 | **Read-only — the helper that should have been used.** `:19` re-exports `parse_f64, parse_i64, parse_u64, parse_usize` from `frogdb_core`. |
| `types/src/args.rs` | 499 | **Read-only — the canonical parsers.** `parse_u64 :300-303`, `parse_i64 :308-311`, `parse_usize :316-319`; all three return `CommandError::NotInteger`. Re-exported at `types/src/lib.rs:30`. **No `f32` parser exists** — only `parse_f64 :328-337`, which additionally maps `inf`/`+inf`/`-inf` by hand and yields `NotFloat`. This is why H2 covers the family's **integer** parses only. |
| `types/src/error.rs` | 336 | **Read-only — why H2 is a wire change.** `InvalidArgument { message } => "ERR {message}"` `:119`; `NotInteger => "ERR value is not an integer or out of range"` `:132`; `NotFloat => "ERR value is not a valid float"` `:135`. |
| `commands/src/set.rs` | — | **Read-only — the guard vectorset is missing.** SRANDMEMBER rejects `i64::MIN` at `:819-825`, with the reason written in the comment (`:819-820`). §Problem 6 / F2. |
| `commands/src/hash.rs` | — | **Read-only — same.** HRANDFIELD guards `i64::MIN` **and** the `WITHVALUES` magnitude overflow at `:813-824`. |
| `commands/src/sorted_set/pop.rs` | — | **Read-only — same.** ZRANDMEMBER guards `i64::MIN` and `count.abs() > i64::MAX / 2` at `:336-350`. Three families carry this guard; vectorset is the fourth and has none. |
| `core/src/shard/panic_guard.rs` | — | **Read-only — the doctrine F1/F2 are measured against.** `:13-17`: the shard-boundary catch is "the *structural backstop*, not a substitute for fixing the arithmetic that panics. **A caught panic is always a bug**". An integer-overflow panic in a command parser is therefore a defect even though the node survives. |
| `commands/src/stream/mod.rs` | 199 | **Read-only — the precedent.** `mod` decls `:20-24`, glob re-exports `:26-30`, then **shared `pub(crate)` helpers in the module root**: `entry_to_response :41`, `parse_trim_options :53`, `parse_delete_ref_strategy :130`, `parse_ids_block :156`. Exactly the shape `vectorset/mod.rs` needs. |
| `commands/src/json/mod.rs` | 131 | **Read-only — the second precedent.** Same shape: `parse_path :32`, `parse_json_value_limited :46`, `enforce_growth_limits :62`, `json_error_to_command_error :75`, `single_or_multi :122`. |
| `commands/src/sorted_set/mod.rs` | 27 | **Read-only — the counter-example.** Pure re-export module, no helpers; 8 files hold 32 commands. |
| `redis-regression/tests/vectorset_regression.rs` | 959 | **Read-only — the acceptance oracle.** Wire-level, harness-driven. Must pass **byte-identically, unmodified**. |
| `redis-regression/tests/vectorset_filter_regression.rs` | 474 | **Read-only — same.** |
| `testing/fuzz/fuzz_targets/vectorset_ops.rs` (repo root, **not** under `frogdb-server/crates/`) | 115 | **Read-only — and it does not cover the parse path.** The target constructs `frogdb_types::vectorset::VectorSetValue` **directly** (`:3`, `:45-115`) and drives seven `Op` variants whose fields are already-typed (`vector: [f32; 4]`, `count: u8`). **No RESP bytes, no command dispatch, no argument parsing.** It is layout-blind — and it is also the reason the defects in §Problem 6 were never fuzzed out: every one of them lives in the argument parser the fuzzer never reaches. |
| `Justfile` | — | **Read-only — gate check.** `lint-no-typed-unwrap :1012-1039` is the only gate touching this crate and it is a **recursive grep over `crates/commands/src/`** (`:1017`, `:1027`) — directory-scoped, so file moves are invisible to it. This change introduces neither `as_*_mut().unwrap()` nor `.ok_or(…WrongType…)`. `lint-info-seam :423-440` is scoped to three `server/src` files. **No gate is affected.** |
| `frogdb-server/ops/docs-gen/src/main.rs` | — | **Read-only — proof the generated docs are layout-blind.** `derive_family(name)` `:497, :628` derives a command's family from its **dot-namespaced name prefix**, not its source path. `commands.json` cannot observe this change. |

## Problem

### 1. Twelve files, twelve commands — against the crate's own convention

Commands per file, counted by `grep -rh ': CommandSpec = CommandSpec {' <dir> | wc -l` across all
five submodules of `commands/src`. **Convention, applied uniformly: command files only — each
submodule's `mod.rs` is excluded from both the file count and the line total** (the authored draft
mixed the two conventions row to row; these are the re-derived numbers):

| Submodule | Command files | Commands | Cmds/file | Lines (excl. `mod.rs`) | Mean file |
|---|---|---|---|---|---|
| `sorted_set` | 8 | 32 | **4.0** | 2,522 | 315 |
| `json` | 6 | 21 | **3.5** | 1,270 | 212 |
| `stream` | 5 | 17 | **3.4** | 2,117 | 423 |
| `event_sourcing` | 6 | 6 | 1.0 | 505 | 84 |
| **`vectorset`** | **12** | **12** | **1.0** | **1,151** | **96** |

(With `mod.rs` included the totals are 2,549 / 1,401 / 2,316 / 553 / **1,180** — the last is the
figure quoted in §Summary, which counts all 13 files.)

The three large families all split by *operation class* — `sorted_set/{basic,range,rank,pop,count,scan,set_ops,store_remove}`, `stream/{basic,read,pending,consumer_groups,info}`,
`json/{basic,array,object,numeric,string,mutation}`. `vectorset` splits by *command identity*, and
does so at twice `event_sourcing`'s scale. The median vectorset file is **64** lines (identical
whether or not `mod.rs` is counted); the median `sorted_set` command file is **≈318** (8 files:
55/90/122/279/358/400/416/802).

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

**The `VALUES` bounds checks — asymmetric on purpose, and both wrong for a second reason.** The
two checks differ: `vadd.rs:100` uses `i + count >= rest.len()`, `vsim.rs:99` uses
`i + count > rest.len()`. **The asymmetry itself is correct** and I traced it: VADD must reserve
one trailing slot for the element name (`vadd.rs:112` reads `rest[i]` after the value loop), VSIM
must not (`vsim.rs` ends at the last value). A blind merge of the two loops silently breaks one of
them, which is why a shared `VALUES` parser needs an explicit `reserve_trailing` parameter.

What the split hid is that **both checks overflow on attacker input** — the same `i + count`, the
same unchecked `usize` — which is defect **F1** (§Problem 6). That reverses the authored draft's
ruling: a shared
`parse_values(rest: &[Bytes], i: &mut usize, reserve_trailing: bool) -> Result<Vec<f32>, _>` is not
a hazard to be avoided, it is **the strongest leverage in this proposal** — one bounds expression,
one place to put a `checked_add`, one place a reviewer looks. §Proposed change therefore treats the
`VALUES` loop as **in scope for the seam** and **out of scope for the fix**: the parser lands
carrying today's arithmetic verbatim (mechanical, behavior-identical), and the `checked_add` lands
under the filed security issue, not here.

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
column above without opening eleven files. After the collapse it is at most three.

**Where the branches land, stated honestly — because it argues against the three-file cut, not
for it:**

| Proposed file | Missing-key branches | Distinct shapes present |
|---|---|---|
| `write.rs` (VREM, VSETATTR; VADD creates) | 2 of 11 | 1 of 4 (`Integer(0)`) |
| `search.rs` (VEMB, VLINKS, VRANDMEMBER, VRANGE, VSIM) | 5 of 11 | 2 of 4 (`null`, `Array(vec![])`) |
| `meta.rs` (VCARD, VDIM, VINFO, VGETATTR) | 4 of 11 | 3 of 4 (`Integer(0)`, `null`, **error**) |
| *2-file variant:* `read.rs` (= `search.rs` + `meta.rs`) | **9 of 11** | **4 of 4** |

So the two-file variant strictly dominates on this axis: it is the only cut that puts all four
shapes — including the VINFO outlier *and* the `Array(vec![])` pair it most needs to be compared
against — in one buffer. **The authored draft used §Problem 5 as the reason to prefer three files
over two. That reason is withdrawn**; on §Problem 5's own metric the ranking is the reverse.

The three-file cut is still the recommendation, on the two grounds that survive: (a) `write.rs` is
the only boundary derivable from the spec literal (§Proposed change), and (b) `read.rs` would be
~725 lines, which is inside crate norms but is also the second-largest file in the crate. The
choice between them is a genuine judgement call, priced in §Proposed change, and the orchestrator
may take either — the collapse's other claims are unchanged by which is picked.

### 6. Two live DoS-class defects the split kept apart — documented here, fixed nowhere

**Handling rule for this whole section, stated first: these are FILED AS ISSUES, and this proposal
implements no fix for either. Security work is parked per standing user direction.** No code in
§Proposed change touches the arithmetic below; the collapse is what makes the fix a one-line
review when the park is lifted. Both were found by reading the six numeric-parse sites of
§Problem 3 next to each other — which is the whole argument of this proposal, arriving as
evidence rather than as prediction.

The doctrine they are measured against is `core/src/shard/panic_guard.rs:13-17`: the shard-boundary
catch is "the *structural backstop*, not a substitute for fixing the arithmetic that panics. **A
caught panic is always a bug**." A caught panic still costs the client its answer and, in release
builds where the arithmetic wraps instead of panicking, the outcome is worse than a panic.

**F1 — unchecked `i + count` on an attacker-supplied `usize`, three sites.**

| Site | Expression | Source of `count` |
|---|---|---|
| `vadd.rs:100` | `i + count >= rest.len()` | `parse_usize(&rest[i])` `:97` — any decimal string up to `usize::MAX` |
| `vsim.rs:99` | `i + count > rest.len()` | `parse_usize(&rest[i])` `:97` |
| `vsim.rs:200` | `count * 4 + 10` | `COUNT` value, `parse_usize` `:132` (default `10usize`, `:117`) |

`parse_usize` (`vadd.rs:265-274`) is `str::parse::<usize>()` with no ceiling, so `count` is fully
attacker-controlled up to `18446744073709551615`. Trace for
`VADD k VALUES 18446744073709551614 a b`:

- **release** (`overflow-checks = false`): `i + count` wraps mod 2^64 to a small value, the guard
  at `:100` *passes*, and control reaches `Vec::with_capacity(count)` `:106`, which panics with
  `capacity overflow` (the requested `count * 4` bytes exceeds `isize::MAX`). The shard's panic
  guard converts that into `-ERR internal error` plus a `ShardPanicsIsolated` increment — survivable,
  and by `panic_guard.rs:13-17`'s own doctrine still a bug.
- **debug/test** (`overflow-checks = true`): the panic happens one line earlier, at the `+` itself.

`vsim.rs:200` is the same shape reached from an otherwise *valid* command: `VSIM k VALUES 1 0.5
COUNT 4611686018427387904 FILTER '.x == 1'` multiplies into overflow **before** any search runs. It
is only evaluated when a `FILTER` is present (`:199`), which is why the ordinary `COUNT` path looks
safe — and the clamp that would have made the value harmless
(`count.min(self.name_to_id.len())`, `types/src/vectorset.rs:342`) sits *downstream* of the
multiply, so it never gets the chance.

Neither the regression suite nor the fuzz target can see this: grep for the family's four error
literals across both regression files returns **nothing**, and the fuzz target never builds a
command (§Files involved). **Action: file a security issue covering all three sites; do not
implement (parked).** The fix, when unparked, is one `checked_add`/`checked_mul` inside the shared
`parse_values` helper plus a ceiling on `COUNT` — i.e. exactly the seam §Proposed change creates.

**F2 — VRANDMEMBER accepts `i64::MIN` and unbounded negative counts; three sibling families do
not.**

`vrandmember.rs:50-57` parses an `i64` and hands it straight to `vs.rand_member(count)` `:59`.
`types/src/vectorset.rs:500-508` implements the negative branch as `let n = (-count) as usize;`
then `(0..n).map(…).collect()`:

- `VRANDMEMBER k -9223372036854775808` → negating `i64::MIN` overflows: debug panics; **release
  wraps back to `i64::MIN`, giving `n = 2^63`** — an unbounded loop cloning `Bytes` into a `Vec`
  on the shard thread. The shard is single-threaded per ADR boundary, so this is a whole-shard
  hang, not one slow client.
- `VRANDMEMBER k -1000000000` needs no overflow at all: it materialises a **10^9-element `Vec`**
  before a single byte is written to the client.

The guard exists three times elsewhere in this crate, each with a comment explaining why:
`set.rs:819-825` (SRANDMEMBER), `hash.rs:813-824` (HRANDFIELD — which also caps the `WITHVALUES`
magnitude), `sorted_set/pop.rs:336-350` (ZRANDMEMBER — which also rejects
`count.abs() > i64::MAX / 2`). Vectorset is the fourth `*RANDMEMBER`-shaped command and the only
one without it. It is also the family's **only unbounded store-materialising path**, which I
checked rather than assumed: `vs.range` (`vrange.rs:74`) is a `BTreeMap` iterator with
`.take(count)` and no pre-allocation (`types/src/vectorset.rs:514-526`), and `vs.search`
(`vsim.rs:215`) clamps to `count.min(self.name_to_id.len())` (`:342`). Only `rand_member`'s
negative branch turns a client integer directly into allocation count. **Action: file a security
issue; do not implement (parked).**

**Why this belongs in an architecture proposal at all.** F1 and F2 are not the deliverable — the
collapse is. They are the evidence for the collapse's premise: a family whose numeric-argument
vocabulary has six spellings in five files will not have its bounds checks compared, and did not.
After the collapse there is one `parse_values` and one numeric-parse pair to audit, and the
sibling guard that `set.rs`/`hash.rs`/`pop.rs` carry has one obvious place to live.

## Proposed change

### The grouping

Three command files plus `mod.rs`. The cut is **spec-derived where it can be** and domain-derived
where it cannot:

| New file | Commands | Membership predicate | ≈ lines |
|---|---|---|---|
| `vectorset/write.rs` | VADD, VREM, VSETATTR | **Exactly the specs with `flags: CommandFlags::WRITE`** and a non-`NoOp` `wal:` — verified: `vadd.rs:21/24` (`PersistFirstKey`), `vrem.rs:17/20` (`PersistOrDeleteFirstKey`), `vsetattr.rs:17/20` (`PersistFirstKey`). All other nine are `READONLY` + `WalStrategy::NoOp`. | ~355 |
| `vectorset/search.rs` | VSIM, VEMB, VLINKS, VRANGE, VRANDMEMBER | **Domain-derived, and it is a disjunction — stated plainly rather than dressed as a predicate.** Two subjects share this file: VSIM/VEMB/VLINKS reach the vector/graph structures (`vs.search` `vsim.rs:215`, `vs.get_vector` `vemb.rs:43`, `vs.links` `vlinks.rs:56`), while VRANGE/VRANDMEMBER touch **only** the lexicographic `BTreeMap` index (`vs.range` `vrange.rs:74`, `vs.rand_member` `vrandmember.rs:43/59`). What actually unites all five is weaker: they return element *collections* and take element-selection options. A reader cannot check this membership from the spec literal the way `write.rs` allows. | ~475 |
| `vectorset/meta.rs` | VCARD, VDIM, VINFO, VGETATTR | Reads that project a single scalar (or one flat metadata array) out of set or element state; bodies are 6–41 lines. | ~180 |
| `vectorset/mod.rs` | — | Doc, 3 `mod`, 12 named `pub use`, plus the shared helpers below (FP32 codec pair, numeric parsers, `parse_values`). | ~90 |

`write.rs` is the one boundary a future editor cannot get wrong: it is *checkable from the spec
literal*, and it is also the boundary that matters most operationally — those three commands are
the ones that hit the WAL, the replication feed, and the keyspace-event suppression path. That is
the first question a reader opening a vectorset file needs answered, and today the answer is
scattered across twelve `flags:` lines.

**The cut's one cost, stated:** VGETATTR (read) and VSETATTR (write) land in different files. That
is the price of making `write.rs` spec-checkable, and it is small — both bodies are under 25 lines
and each `impl` gets a one-line doc cross-reference. The alternative that keeps them together —
splitting by *subject* (set-level vs element-level) — would put VADD next to VGETATTR and give up
the WRITE predicate entirely.

**The two-file variant, re-priced.** `write.rs` + `read.rs` (the latter ~725 lines) is a legitimate
simplification, within crate norms (`sorted_set/set_ops.rs` is 802) and with a real advantage the
authored draft got backwards: it is the *only* cut that puts all four missing-key shapes on one
screen (§Problem 5's table — `read.rs` holds 9 of 11 branches and 4 of 4 shapes, versus `meta.rs`'s
4 and 3). Three files is still the recommendation, but now for exactly two reasons — the
spec-checkable `write.rs` boundary, and keeping the largest merged file at ~475 lines rather than
~725, i.e. mid-pack for the crate instead of just under its 802-line ceiling. **§Problem 5 is no
longer offered as a reason to prefer three.**
Either cut is acceptable to this proposal; the orchestrator picks.

### The shared surface in `mod.rs`

Following `json/mod.rs:32-121` in shape — helpers in the module root, below the re-exports.

**Visibility: plain private `fn`, not `pub(super)`.** The authored draft said `pub(super)`; that is
wrong here. `vectorset` is declared at `lib.rs:60`, so inside `vectorset/mod.rs` `super` *is* the
crate root and `pub(super)` resolves to the same reach as `pub(crate)` — it would export these
helpers to every other command family. A **private** item in `vectorset/mod.rs` is already visible
to `vectorset::write`, `vectorset::search` and `vectorset::meta`, because a child module can see
its ancestors' private items. `json/mod.rs` is the precedent to copy: four of its five helpers are
bare `fn` (`parse_path :32`, `parse_json_value_limited :46`, `enforce_growth_limits :62`,
`json_error_to_command_error :75`) and only `single_or_multi :122` is `pub(crate)`, because that one
really is used from outside. `stream/mod.rs:41-156` marks all four `pub(crate)` — the looser of the
two precedents, and not the one to follow.

```rust
/// FrogDB's FP32 vector wire encoding: little-endian 4-byte `f32`, one per
/// dimension. This pair is the single authority for that format; VADD and VSIM
/// decode with it, VEMB encodes with it.
fn decode_fp32(blob: &[u8]) -> Result<Vec<f32>, CommandError> { … }
fn encode_fp32(v: &[f32]) -> Bytes { … }

/// The vectorset family's numeric argument parsers.
fn parse_usize(b: &[u8]) -> Result<usize, CommandError> { … }  // moved verbatim
fn parse_f32(b: &[u8]) -> Result<f32, CommandError> { … }      // moved verbatim

/// The `VALUES <count> v1..vN` argument form, shared by VADD and VSIM.
/// `reserve_trailing` is VADD's element-name slot — see §Problem 4 for why the
/// two bounds checks legitimately differ, and §Problem 6 (F1) for the overflow
/// this expression carries today and continues to carry after the move.
fn parse_values(
    rest: &[Bytes],
    i: &mut usize,
    reserve_trailing: bool,
) -> Result<Vec<f32>, CommandError> { … }  // arithmetic moved VERBATIM, including the overflow
```

`parse_usize`/`parse_f32` **move verbatim**, error strings unchanged — see H2 for the separate,
deliberate question of whether they should instead defer to `utils::parse_usize`. The four inline
ladders in `vlinks`/`vrange`/`vrandmember` are rewritten to call `parse_usize` (or `parse_i64` for
VRANDMEMBER's `i64`) — **this is a wire change** and is therefore split out as H2, *not* folded
into the move. The mechanical collapse itself must be byte-identical on every path.

**`parse_values` carries the F1 arithmetic unchanged.** The `>=`/`>` distinction becomes the
`reserve_trailing` flag and nothing else moves: `i + count` stays `i + count`. Hardening it is the
filed security issue's job, not this proposal's (§Problem 6). Anyone tempted to "just add the
`checked_add` while we're in there" is changing wire behavior — a previously-panicking input starts
returning an error reply — and that belongs in the issue with its own regression test.

### Vocabulary, and what this change actually is

`vectorset` is the *module*; the twelve `impl Command for V*Command` blocks are the *interface* and
are **untouched** — same types, same names, same `mod.rs` re-exports, so `lib.rs:446-460`'s twelve
`registry.register(…)` lines compile unchanged and the crate's public surface is bit-for-bit
identical. `mod.rs`'s helper section becomes a genuine *seam*: the one place the FP32 format and
the family's numeric-argument vocabulary are decided. It is deliberately **not** an *adapter* —
nothing crosses a crate boundary — and the *leverage* is honestly low: a handful of call sites
unified, not thirty. The *locality* gain is the real product: 1,180 lines that today require twelve
file opens to survey become three files, and the **three** duplications the split concealed — FP32
codec, numeric-parse vocabulary, `VALUES` bounds arithmetic — become impossible to reintroduce
without noticing. The third of those is the one that already cost something (§Problem 6).

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

**Q: If `mod.rs`'s `parse_values` were deleted?** "How many `VALUES` arguments a vectorset command
may consume, and whether one trailing slot is reserved." Today that fact is split across two
expressions in two files (`vadd.rs:100`, `vsim.rs:99`) that differ in one character and are never
read together — which is how the shared overflow in both (§Problem 6, F1) survived. **Passes, and
this is the strongest pass in the section**: it is the only helper whose absence produced an actual
defect rather than a maintenance hazard.

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
entire coverage is two wire-level regression files that never import a module path, plus a fuzz
target that never builds a command (`testing/fuzz/fuzz_targets/vectorset_ops.rs` drives
`VectorSetValue` directly — §Files involved). **The argument parser, where both §Problem 6 defects
live, has no coverage of any kind.** Four concrete gains, none of which requires the collapse but
three of which the collapse makes practical:

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
4. **A home for the §Problem 6 regression tests when the security park lifts.** The pins those
   issues need — `VADD k VALUES <2^64-2> a b`, `VSIM … COUNT <2^62> FILTER …`, `VRANDMEMBER k
   -9223372036854775808` — are wire-level and belong in `vectorset_regression.rs` either way. But
   the *unit* tests that make the bounds arithmetic cheap to re-check (`parse_values` with
   `count = usize::MAX`, with and without `reserve_trailing`) require the shared helper to exist.
   **This proposal writes none of them**; it makes them writable. The fuzz target could also be
   extended to drive the parser instead of `VectorSetValue`, which would have found F1 — a separate
   filing, not part of this change.

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
exactly; (c) hoist the two `VALUES` loops into `parse_values` with the `reserve_trailing` flag,
**arithmetic byte-for-byte unchanged**; (d) merge import blocks; (e) rewrite `mod.rs`'s
`mod`/`pub use` lines. Acceptance is both regression files passing unmodified plus
`just check frogdb-commands --features vectorset`.

**The security boundary, stated as a rule for the implementer.** Edit (c) is the highest-risk hunk
in the change *and* it sits on top of live defect F1. The rule is: `parse_values` reproduces
`i + count >= rest.len()` / `i + count > rest.len()` exactly, selected by the flag. **No
`checked_add`, no ceiling, no clamp ships with this proposal** — those are the filed security
issues' content and are parked per standing user direction. A reviewer who sees hardening in this
diff should reject it as scope creep, not because the hardening is wrong but because it changes
wire behavior with no test and no ruling.

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
any moved body. **`--follow` is what degrades**, and for **11 of the 12** files — only `vadd.rs`
clears the threshold outright; `vsim.rs` may or may not, so the honest range is 10-11. (The
authored draft said 9; that number came from counting the files that "lose" as 12 minus the three
merged files, which is not how `--follow` works — each merged file can inherit at most **one**
predecessor.)

The substantive loss is near zero: the directory has **11 commits in its entire life**
(`git log --oneline -- frogdb-server/crates/commands/src/vectorset/`), of which **nine** are
directory-wide sweeps (`refactor(core): require CommandSpec…` `693808df`, `refactor(commands):
migrate vector set to typed store accessors` `fc491f32`, `refactor(search): make reindexing a
CommandSpec fact` `38ce99d1`, …), **one** is the original authoring commit `cd047359` — which
created **all twelve files at once** — and **one** is a single-file fix, `048dc792`
(*"fix(persistence,types,commands): enforce VectorSet bounds at creation"*, `+14` lines in
`vadd.rs` and nothing else in the directory). That single-file commit is the *only* per-command
history in the family, and it belongs to `vadd.rs`, which is precisely the one file whose
`--follow` survives. There is no per-command narrative to lose; there never was one.

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
  since the golden protects the siblings too" (`90:526-527`). This proposal is the concrete case
  for that.
- **99 lands before 90 commit 3.** This is not a preference; it is 90's own ruling, stated in two
  places: the section heading `90:507` ("**land SOLO, and land LAST**") and the concrete rule at
  `90:526-528` ("commit 3 (the sweep) lands after 67/70/80/89 are merged, re-derived from the
  merged tree, never merged across them"). 99 is another commands-crate writer and joins that
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

### Siblings needing an orchestrator ruling

Two siblings are adjacent enough to name explicitly. **Neither is resolved here** — both are put to
the orchestrator, because in each case the call is about the *round's* sequencing, not about 99's
content.

- **97 (`typed-store-access`)** — **ruling requested: does 97's accessor sweep reach
  `commands/src/vectorset/`?** Evidence at HEAD, both directions: (a) 97 names vectorset exactly
  once, in its corrected accessor count (`97:32`, "the 30 `Value` family accessors … plus
  `as_vectorset`"), and has **no `commands/src/vectorset/` row in its file table**; (b) `grep -rn
  'as_vectorset' frogdb-server/crates/commands/src/` returns **zero hits** — every vectorset
  command already goes through the typed accessor 97 advocates (`ctx.store.get_vectorset(key)?`,
  eleven sites, e.g. `vcard.rs:35`, `vinfo.rs:36`, `vrange.rs:64`). So the overlap looks nil, and
  99 is *downstream* of 97's argument rather than in tension with it. **If** 97's optional rename
  wave later touches `StoreTypedFamilyExt::get_vectorset` or its name, it would touch all twelve
  vectorset files today and three after 99 — which is an argument for 99 landing first, not a
  conflict. Orchestrator decides; 99 needs no change either way.
- **98 (`scan-grammar-unify`)** — **ruling requested: is VRANGE's cursor grammar in 98's scope?**
  98 enumerates three SCAN-grammar sites and six glob implementations and names **neither VRANGE
  nor vectorset anywhere** (verified: `grep -in 'vrange\|vectorset' 98-scan-grammar-unify.md`
  returns nothing). But VRANGE *is* a cursor-paged read with its own hand-rolled
  `key cursor [COUNT n]` parse (`vrange.rs:36-62`, including a positional-or-keyword count form and
  two copies of the same ladder), and the sentinel handling at `:68-72` treats `"0"` and empty
  identically — a fourth grammar 98 does not count. **99 does not touch that grammar** (the parse
  moves verbatim into `search.rs`). The ruling needed is only whether 98 should widen to include
  it; if it does, 98 should land **after** 99 so it edits one file instead of one-of-twelve.

### Siblings with no overlap

- **89 (`probabilistic-chunk-codec`)** — `bloom.rs` + `cuckoo.rs` + `commands/Cargo.toml:54`.
  **No file overlap.** 99 touches no `Cargo.toml`.
- **95 (`BF_INFO_FIELDS`)** — `bloom.rs` + `bloom_regression.rs`. It cites `vectorset/vinfo.rs` as
  **read-only evidence** (its file-table row `95:77`, `VinfoCommand :11` and the conditional field
  `:70-73`) and explicitly scopes VINFO **out** in two places — `95:221` ("Conditional fields? Yes,
  one, and **outside** the scope: `vinfo.rs:70-73`") and `95:435` (the boundary restatement). 99
  moves `vinfo.rs`'s body into `meta.rs` unchanged. **The only interaction is stale path citations
  in 95's prose** — if 99 lands first, 95's evidence rows should read `vectorset/meta.rs`. No code
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
- **The §Problem 6 defects are out of scope as *fixes* and in scope as *justification*.** F1 (three
  overflow sites) and F2 (VRANDMEMBER's missing `i64::MIN` guard) are filed as security issues and
  **not implemented by this proposal or any of its hotfixes** — security work is parked per
  standing user direction. Same disposition for the VSIM arity gap in §Adjacent findings 6. What
  99 contributes is the seam that makes each fix a single reviewable line; the ruling on when to
  unpark is not 99's to make.
- **`event_sourcing`'s and the wider crate's numeric-parse hygiene is out of scope.** H2 covers
  `vectorset/` only.
- **The 12 exported type names do not change.** `VaddCommand` … `VsimCommand` stay exactly as
  `lib.rs:446-460` and any external consumer see them. `mod.rs` keeps **named** re-exports rather
  than adopting the sibling `pub use module::*` glob (`stream/mod.rs:26-30`,
  `sorted_set/mod.rs:20-27`, `json/mod.rs:19-24`) — the explicit list is better hygiene and its 12
  lines are not the problem; the 12 `mod` declarations above them are.

## Effort

**S.** Four files where there were thirteen. **Net ≈ −7 lines — roughly line-neutral, not −50.**
The authored draft's headline "−50" contradicted its own itemization; here is the itemization,
re-added:

| Item | Δ lines |
|---|---|
| Duplicated 6-line import preamble, 9 files → 3 merged blocks | −33 |
| Second `parse_usize`/`parse_f32` copy deleted (`vsim.rs:255-275`) | −21 |
| FP32 codec: three inline blocks → two shared fns | −~9 |
| `VALUES` loop: two copies → one `parse_values` | −~10 |
| 12 `mod` declarations → 3 | −9 |
| `mod.rs` helper section (four fns + `parse_values`) with doc comments | +~55 |
| Section banners / `impl` doc cross-references in the three merged files | +~20 |
| **Net** | **≈ −7** |

The number was never the point — "13 files → 4" is — but a proposal that prices a `git mv` at −50
lines is claiming leverage it does not have, and §Deletion test already refuses to do that.

Zero new crate edges, zero `Cargo.toml` change, zero `cfg` work (the feature gate is a single line
at `lib.rs:59-60` and is untouched), zero generated-artifact regeneration (`docs-gen` derives family
from the command **name**, `main.rs:497,628` — layout is invisible to it), zero gate impact
(`lint-no-typed-unwrap` is directory-scoped, `Justfile:1012-1039`).

The work is mostly `git mv`-shaped and reviewable by diff shape: every hunk should be either a pure
move, an import merge, or one of the two dedups. Iteration loop is
`cargo check -p frogdb-commands --features vectorset --all-targets` — note that plain
`just check frogdb-commands` runs at `default = core-profile` and **will not compile this module at
all** (`Cargo.toml:15,27,41`), which is the single most likely way to waste an hour on this change.
Acceptance is `cargo nextest run -p frogdb-redis-regression vectorset` with both regression files
unmodified.

**Independently-landable hotfixes.**

**H1 — hoist the duplicate `parse_usize`/`parse_f32` pair into `mod.rs`, no file collapse.**
Delete `vsim.rs:255-275`, move `vadd.rs:265-285` to `vectorset/mod.rs` as a **plain private `fn`**
(not `pub(super)` — see §The shared surface for why that spelling would over-export), point both
call sites at it. **−21 lines, byte-identical behavior, touches 3 files, conflicts with no
sibling**, and it is the one piece of §Problem 3 that is pure loss with no design question
attached. Ships today, ships alone, and makes the full proposal smaller if it ever lands.

**H2 — route the family's *integer* parses through `utils::parse_usize`/`parse_i64`. Ruling
required, not a free fix — and narrower than the authored draft claimed.**

*Scope, corrected.* §Problem 3 counts six spellings; H2 can only absorb the integer ones. Those are
the `parse_usize` copy (`vadd.rs:265-274` ≡ `vsim.rs:255-264`, six call sites between them:
`vadd.rs:62/103/145/171`, `vsim.rs:97/132`) and the four inline ladders (`vlinks.rs:39-46`,
`vrandmember.rs:50-57` — an `i64`, so `parse_i64` — `vrange.rs:42-49`, `vrange.rs:51-58`). **The
`parse_f32` copy is out of H2's reach entirely**: `types/src/args.rs` exposes
`parse_u64`/`parse_i64`/`parse_usize`/`parse_f64` and **no `f32` parser** (`:300-337`), and
`parse_f64` is not a drop-in — it hand-maps `inf`/`+inf`/`-inf` before delegating and yields
`NotFloat`, whereas the local helper is a bare `str::parse::<f32>()`. Routing floats through it
would mean an `f64 → f32` narrowing plus a different accepted-input set. So H2 = **integer sites
only**; `parse_f32` stays a family-local helper regardless of the ruling.

*Why it is a wire change.* The local helpers emit `ERR Invalid integer` / `ERR Invalid count` /
`ERR Invalid layer number` (`InvalidArgument { message } => "ERR {message}"`,
`types/src/error.rs:119`) whereas the canonical helper emits
`ERR value is not an integer or out of range` (`error.rs:132`). That string is **canonical within
FrogDB** — it is what every non-exotic family already returns for a malformed integer — and it is
plausibly closer to upstream Redis too, but I did not verify vector-set error text against Redis 8
from this environment, so the compat claim is *unproven* and should not be the argument. The
argument is internal consistency. **Zero tests observe the current strings** (verified by grep for
all four literals across both regression files), so it would land silently either way — which is the
argument for adding the assertions *first*. Flagged, sized S, and left to a compat ruling rather
than smuggled into a file move.

**No hotfix touches §Problem 6.** F1, F2 and the §Adjacent-findings-6 arity gap are **filed as
issues; no fix is implemented here** (security parked per standing user direction). The authored
draft's closing sentence — "there is no known live defect in the vectorset command family" — was
false and has been removed: there are two, they are described at file:line in §Problem 6, and this
proposal's contribution to them is the seam, not the patch.

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
   brute-force fetch count) with no cap. **Not promotable to a defect**: the work is bounded by the
   set's own cardinality, which is what exhaustive search means, so it is a capacity question and
   not an amplification. That is exactly the line the `count * 4 + 10` over-fetch on the indexed
   path (`:199-203`) crosses — there the bound comes from a client integer, which is why it is F1's
   third site (§Problem 6) and this one is not.
5. **`vemb.rs:56` renders floats with `format!("{v}")` and `vsim.rs:237` with
   `format!("{}", hit.score)`**, rather than the canonical `format_float`
   (`protocol/src/format.rs`, re-exported at `commands/src/utils.rs:27`). Same class as proposal
   95's adjacent finding 4 for `topk`/`tdigest`; `lint-format-float` pins definitions, not call
   sites, so both are outside the gate. A float-rendering audit item spanning several families.
6. **NEW — VSIM accepts a trailing `EF` / `FILTER-EF` / `EPSILON` with no value at all, where
   every other valued option errors.** `vsim.rs:160-168`:

   ```rust
   } else if opt.eq_ignore_ascii_case(b"EF")
       || opt.eq_ignore_ascii_case(b"FILTER-EF")
       || opt.eq_ignore_ascii_case(b"EPSILON")
   {
       // Accept but skip value argument (these tune search behavior).
       i += 1;
       if i < rest.len() {
           i += 1; // skip the value
       }
   }
   ```

   The `if i < rest.len()` makes the value **optional**, so `VSIM k ELE a EF` succeeds and returns
   results. `COUNT` (`:127-131`, `"COUNT requires a value"`) and `FILTER` (`:145-149`, `"FILTER
   requires an expression"`) both reject the same shape, and `VSIM`'s arity is `AtLeast(3)`
   (`vsim.rs:21`), which cannot express it either. This is an **arity/grammar gap**, distinct from
   finding 2 (which is about discarding a value that *was* supplied): an argument list Redis
   rejects is silently accepted. **Action: file an issue; do not implement a fix here** — it is a
   wire-behavior change (a currently-succeeding command starts erroring) and it falls under the
   same parked-security disposition as §Problem 6, since the correct fix is written in the same
   hunk. Post-collapse both the strict branches and the lax one sit in `search.rs` within ~40
   lines of each other; today the comparison requires believing the option loop is uniform, which
   it is not.
