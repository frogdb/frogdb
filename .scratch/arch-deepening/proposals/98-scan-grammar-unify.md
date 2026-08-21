# Proposal 98 — One SCAN grammar, one glob: fold the third (dead) scan parser and rule the remaining glob implementations

Round 38. Candidate labels: PN13 (glob duplication), CT10-adjacent (scan grammar).

Verified at HEAD `dd840ca3bb3a70319c424d62885e753e51abfdf5`. All line numbers below are from that
commit unless stated otherwise. The differential numbers in §Problem 2 come from compiling both
matchers verbatim into a standalone binary (`rustc --edition 2021 -O`) and running them — they are
measurements, not readings.

## Re-anchoring note (rev 2) — HF-B has landed

**This proposal was authored against `dd840ca3`, before HF-B landed.** HF-B (upstream `0b034a4f`,
*"use canonical Redis glob in H/S/ZSCAN MATCH and FUNCTION LIST"*) is now **on `origin/main` as part
of the `d48e1b44` batch**. What that changes, stated once here rather than hedged throughout:

- **H2 is done.** It was *"merge `0b034a4f`"*; it is now history. Every H2 obligation below is
  written in the past tense and the hotfix list marks it **LANDED**.
- **`commands/src/utils.rs` line numbers in this document are pre-HF-B.** HF-B deletes
  `simple_glob_match` (`:57-88`, doc comment included) for a net **−44** in that file. Post-merge,
  every `utils.rs` reference at or below `:88` shifts by roughly **−28** (the fn plus its banner;
  the balance of the −44 is import/call-site churn). This document deliberately keeps the **pre-HF-B**
  numbers with that annotation rather than printing post-HF-B numbers it cannot verify from this
  worktree, which predates `d48e1b44`. **The implementer re-derives them from `main`, not from here.**
  Affected: `:114`, `:141-158`, `:148`, `:172-173`, `:175-179`, `:194-225`, `:230-235`, `:244-267`,
  the inline tests `:973`/`:982`/`:1104`, and proposal 94's `:791-920` boundary.
- **Glob count: four → three, not six → three.** Six existed at authoring; HF-B folded two
  (`utils.rs`'s `simple_glob_match`, `scripting/registry.rs`'s `matches_pattern`). Four survive on
  `main` and 98 folds one more (H3).
- **Deletion total: ≈ −98 production lines, not −130.** The −32 that HF-B contributed is no longer
  98's to claim.
- **§Problem 2's divergence table is now historical rationale** — the evidence that justified the
  landed fix, retained because it is the only written record of *what* HF-B changed on the wire, and
  because the compat caveat in §Risks now describes shipped behavior an operator may hit.

Everything else — the unreachable-`execute` argument, the promote-and-widen move, H1/H3/H4 — is
unaffected by HF-B and stands as written.

## Corrections to the lane brief

The brief's framing is directionally right and factually wrong in five places. Each correction
changes what this proposal should contain, so they lead.

| Brief claim | Ruling |
|---|---|
| *"FIVE glob implementations"* | **Six at authoring; four on `main`.** The brief's five are `types/src/glob.rs:23`, `commands/src/utils.rs:60`, `core/src/shard/search/mod.rs:23`, `frogctl/src/commands/watch.rs:64`, `testing/src/pubsub_oracle.rs:242`. It missed `scripting/src/registry.rs`'s `matches_pattern` (`:199`)/`match_pattern_recursive` (`:205`) — the only **recursive** one, and the only one with genuinely exponential worst-case shape. HF-B has since deleted #2 and #6. |
| *"the compat-divergence fix is a normal hotfix"* | **It was already written, and it has now landed.** Commit `0b034a4f` (*"fix(commands,scripting): use canonical Redis glob in H/S/ZSCAN MATCH and FUNCTION LIST"*, 2026-08-10) folded impls #2 and #6 into the canonical one and added 181 lines of new `scan_regression.rs` pins. At authoring (`dd840ca3`) it was an ancestor of neither `HEAD` nor `origin/main` — `git branch --contains` returned only `worktree-agent-a84216c599d8af135`. It is now **on `origin/main` as part of the `d48e1b44` batch** (= HF-B). **98 never re-proposes it**; 98 covers the residue it does not touch, and §Problem 2 is kept as the record of what it changed. |
| *"`ScanRequest::parse` handles only SCAN's grammar"* | **Backwards.** `ScanRequest::parse` (`commands/src/utils.rs:194`) has exactly three callers — `hash.rs:739` (HSCAN), `set.rs:1113` (SSCAN), `sorted_set/scan.rs:40` (ZSCAN). **SCAN never calls it.** Its own doc comment (`utils.rs:172-173`) claims it is *"shared by the whole SCAN family (SCAN, HSCAN, SSCAN, ZSCAN)"* — that sentence is false at HEAD. |
| *"`parse_key_type` duplicated `scan.rs:150-162` vs `scatter.rs:98-110`"* | **Half right, and the interesting half is different.** `scatter.rs` has no such function — it has an **inline** six-arm `match` at `:100-113`. And `scan.rs`'s `parse_key_type` (`:151-163`) is **dead**, because its only caller (`ScanCommand::execute` `:67-105`) is unreachable. §Problem 1. |
| *"the real glob has `MAX_STAR_COUNT` (verify)"* | **The cap exists (`types/src/glob.rs:18`) and does not close the cost.** A two-star pattern never trips it: `*` + 10,000 `a` + `b` + `*` against 100,000 `a` measures **752 ms** for a single match call. §Flagged for sign-off. |

## Summary

SCAN's wire grammar — `cursor [MATCH p] [COUNT n] [TYPE t]` — is written **three** times in this
repo, and the copy that the SCAN *command* owns is **unreachable**. `ScanCommand` and `KeysCommand`
declare `ExecutionStrategy::ServerWide`, which the connection intercepts at `DispatchStage::ServerWide`
strictly before `DispatchStage::Execute`; their `execute()` bodies are never entered on **any** of the
four routes into a registry handler (§Problem 1). They are not dormant — they are decoys that would
fabricate a plausible single-shard answer if something ever reached them.

The proposal has one architectural move and one set of rulings:

**Move.** Promote `ScanRequest` from `commands::utils` into `commands::scan` — the module that
already exports `cursor` to the server across the crate boundary — widen it with the two
command-specific fields (`key_type`, `novalues`), and replace the `extra: impl FnMut(&mut ArgParser)`
closure hook with a data capability record. `connection/scatter.rs::handle_scan` then calls the
shared parser instead of hand-rolling the grammar for the third time, and the two dead `execute`
bodies plus `parse_key_type` are deleted. One wire grammar, one owner, one place a new **SCAN-family**
option lands.

Scope note on that last clause: the SCAN family is `SCAN`/`KEYS`/`HSCAN`/`SSCAN`/`ZSCAN`. It is
**not** every cursor+`COUNT` grammar in the repo — `VRANGE` (`commands/src/vectorset/vrange.rs:36-62`)
is a fourth, feature-gated one that 98 does not touch and proposal 99 owns (§Sibling boundaries).

**Rulings.** Six glob implementations at authoring, one canonical (`frogdb_types::glob_match`). Two
were folded by HF-B (`0b034a4f`), now on `main`. Of the four that survive, one (`glob_match_simple`,
FT.CONFIG) should fold — H3. Two (`frogctl`, the pub/sub oracle) must **stay**, for reasons that are
dependency-structural and worth writing down so the next round does not re-litigate them. **Four → three.**

The deletion test is the argument's spine: after the move, `commands/src/scan.rs` loses ~67 lines of
unreachable code, `connection/scatter.rs` loses ~51 lines of grammar, and nothing that any client
can observe changes. Deleting a decoy is strictly better than maintaining it, and today the decoy is
*the* thing that makes `scan.rs` look like it owns SCAN.

## Files involved

Verified paths and line counts at `dd840ca3` — i.e. **pre-HF-B**. `utils.rs`, `scripting/registry.rs`
and `scan_regression.rs` all moved on `d48e1b44`; their rows say so, and the `utils.rs` shift rule is
in the re-anchoring note above.

`commands/src/scan.rs` and `commands/src/utils.rs` are both in the **`core-profile`** feature set
(`commands/Cargo.toml:14-18`: `default = ["core-profile"]`, and the comment at `:16-17` names `scan`
and `utils` in the always-compiled surface). So every file 98 edits compiles under a bare
`just check frogdb-commands` — **no feature-flag trap**, unlike the vectorset files in §Sibling
boundaries.

| File | Lines | Role in this proposal |
|---|---|---|
| `frogdb-server/crates/commands/src/scan.rs` | 197 | **Primary.** `cursor` module `:24-39` (live, the server's sole import). `ScanCommand::execute` `:67-105` and `KeysCommand::execute` `:134-147` — **unreachable, deleted**. `parse_key_type` `:151-163` — dead with them. The two `CommandSpec` statics `:49-63` / `:116-130` **stay** (they are proposal 90's targets — §Risks). New home for `ScanRequest`. Zero `FM-` tags. |
| `frogdb-server/crates/commands/src/utils.rs` | 1241 **pre-HF-B** (−44 on `main`) | **Primary.** `simple_glob_match` `:57-88` — **already deleted by HF-B**; `hash_cursor_scan` `:114-158`; `ScanRequest` `:175-179` + `::parse` `:194-225` — **moved out**; `empty_scan_reply` `:230-235`, `scan_reply` `:244-267` — move with it. Region touched: **`:53-267`**. **All of these are pre-HF-B numbers; subtract ~28 after merge.** Proposal 94 owns `:791-920` (also pre-HF-B) — disjoint, §Risks. Zero `FM-` tags. |
| `frogdb-server/crates/server/src/connection/scatter.rs` | 434 | **Primary.** `handle_scan` `:43-194`; the third grammar copy is `:46-118` (empty-args guard `:46-48`, cursor parse `:51-58`, option loop `:68-118`, inline key-type match `:99-113`) — **~51 lines replaced by one call**. Cursor walk `:120-193`, `encode_final_cursor` `:364-370`, and `absorb_scan_reply` `:352` untouched. Covered by `lint-info-seam` — §Seam-lint clearance. Zero `FM-` tags. |
| `frogdb-server/crates/types/src/glob.rs` | 384 | **Read-only. The canonical implementation.** `glob_match` `:23`, `MAX_STAR_COUNT = 100` `:18`, four fast paths `:24-52` (match-all `:24-27`, wildcard-free exact `:28-34`, `prefix*` `:35-43`, `*suffix` `:44-52`), iterative core from `:53`, `*` arm `:63-77`, `match_char_class` `:131-198`. Prod `:1-198`, tests `:199-384`. |
| `frogdb-server/crates/core/src/shard/search/mod.rs` | 100 | **H3 only.** `glob_match_simple` `:23-39` — **deleted** (17 lines). |
| `frogdb-server/crates/core/src/shard/search/config.rs` | 99 | **H3 only, one line.** The call at `:42` becomes a canonical-glob call. Proposal 70 lists this file **read-only** — §Risks. |
| `frogdb-server/crates/commands/src/hash.rs` | 2327 | **Caller.** HSCAN `:734-763`; the `NOVALUES` closure `:739-746` becomes a capability field. |
| `frogdb-server/crates/commands/src/set.rs` | 1127 | **Caller.** SSCAN `:1113` — `\|_\| Ok(false)` becomes `ScanCaps::NONE`. |
| `frogdb-server/crates/commands/src/sorted_set/scan.rs` | 55 | **Caller.** ZSCAN `:40` — same. |
| `frogdb-server/crates/scripting/src/registry.rs` | 413 **pre-HF-B** | **Read-only — and `matches_pattern` (`:199`, with `match_pattern_recursive` `:205`) is deleted by HF-B.** The sixth glob. Evidence for §Problem 2 only; on `main` the row is history, not a target. |
| `frogdb-server/crates/server/src/connection/dispatch.rs` | — | **Read-only. The unreachability proof, routes 1 and 2.** `PRE_DISPATCH_ORDER` `:124-141` (16 stages; `CommandLookup` idx 6, `ServerWide` idx 13, `Execute` idx 15); `CommandLookup`'s arity short-circuit `:543-559`; `dispatch_server_wide` `:232-…`, `ServerWideOp::Scan => self.handle_scan(args).await` at `:238`. |
| `frogdb-server/crates/server/src/connection/transaction.rs` | — | **Read-only.** EXEC's deferral into the same `dispatch_server_wide` at `:141`, `:240` — closes route 2. |
| `frogdb-server/crates/core/src/scripting/gate.rs` | — | **Read-only. Route 3.** `reject_server_wide` — trait method `:164`, called at `:278` ahead of dispatch, impl `:441-451`: any handler whose `execution_strategy()` is `ServerWide(_)` is refused inside a script before `handler.execute` (`:506`) is reached. Also holds the `ServerWide` **test probe** spec at `:980`. |
| `frogdb-server/crates/server/src/connection/routing.rs` | — | **Read-only. Route 4.** `:137-140`: the keyed cross-shard path derives its op from the declared strategy and returns `redirect::crossslot()` for anything that is not `ScatterGather` — a `ServerWide` command never reaches an executor here either. |
| `frogdb-server/crates/core/src/shard/execution.rs` | — | **Read-only.** The **single live** `handler.execute(&mut ctx, &command.args)` call site at `:241` — the door all four routes are proven not to open for SCAN/KEYS. Also KEYS + `ScatterOp::Scan` shard side `:780-812`; canonical glob at `:785`. |
| `frogdb-server/crates/core/src/store/hashmap.rs` | — | **Read-only.** `scan_filtered` `:1037-1097`, canonical glob at `:1085` — the code that actually serves SCAN MATCH. |
| `frogctl/src/commands/watch.rs` | 111 | **Read-only, KEEP.** Self-contained `glob_match` `:64-94`. Ruling in §Problem 3. |
| `frogdb-server/crates/testing/src/pubsub_oracle.rs` | 1138 | **Read-only, KEEP.** Self-contained `glob_match` `:242-…`; it is the file's **only** matcher — `SubKind::covers` `:225` resolves to it, not to the canonical one. Ruling in §Problem 3. |
| `frogdb-server/crates/redis-regression/tests/scan_regression.rs` | 110 **pre-HF-B**; **291 on `main`** | **Read-only.** Three iteration tests and no MATCH pins at `dd840ca3`; HF-B added its 181 lines of `[class]`/`\escape` pins. |
| `frogdb-server/crates/redis-regression/tests/scan_tcl.rs` | — | **Read-only, must stay green.** Every MATCH pin: `:159`, `:190`, `:572`, `:582`, `:608`, `:635`, `:689`, `:820`. |
| `testing/fuzz/fuzz_targets/glob_match.rs` | 13 | **Read-only.** Evidence for §Flagged: panic-only, and structurally unable to reach the costly inputs. |
| `Justfile` | `:423-441` | **Read-only.** `lint-info-seam`, the one gate scoped to an edited file. |

Test surface that must stay green: `scan_tcl.rs` (8 MATCH pins), `scan_regression.rs` (291 lines on
`main`), `types/src/glob.rs` inline tests (`:199-384`), `commands/src/scan.rs` inline cursor tests
(`:169`, `:189` — these test the `cursor` module, which survives), and **eleven** `ScanRequest` /
`scan_reply` inline tests in `commands/src/utils.rs` that move with their subject:

| Test | Line (pre-HF-B) | What the move costs it |
|---|---|---|
| `parses_cursor_only` | `:973` | `\|_\| Ok(false)` → `ScanCaps::NONE` |
| `parses_match_and_count` | `:982` | same |
| `options_are_case_insensitive` | `:991` | same |
| `cursor_round_trip_through_reply` | `:999` | **struct literals `:1003`, `:1032`** — two new fields, mechanical |
| `unknown_option_is_syntax_error` | `:1063` | `ScanCaps::NONE` |
| `bad_cursor_is_invalid_argument` | `:1070` | same |
| `missing_flag_value_is_syntax_error` | `:1080` | same |
| `empty_key_yields_empty_envelope` | `:1088` | **struct literal `:1089`** — mechanical |
| `empty_scan_reply_shape` | `:1104` | none |
| `hscan_extra_flag_novalues_toggles` | `:1109` | **rewrite** — pins the `extra` closure Step B deletes; becomes a `ScanCaps::HASH` test |
| `extra_hook_takes_precedence_over_syntax_error` | `:1149` | **rewrite** — same; becomes "capability wins over unknown-option" under `ScanCaps` |

Two rewrites and three struct-literal updates, not "a couple of tests move". The two rewrites are the
real content: they are the tests that encode the closure protocol Step B replaces, so they are the
tests that must be re-expressed as capability assertions rather than mechanically ported.

## Problem

### 1. The SCAN grammar is written three times, and the copy SCAN owns is unreachable

`ScanCommand`'s spec declares `strategy: ExecutionStrategy::ServerWide(ServerWideOp::Scan)`
(`scan.rs:62`); `KeysCommand`'s declares `ServerWideOp::Keys` (`scan.rs:129`).

The proof is a closed enumeration, and it is worth stating that way because "unreachable" is exactly
the kind of claim that deserves an exhaustive rather than an illustrative argument. There is **one
live call site** for a registry handler's `execute` on the command path —
`core/src/shard/execution.rs:241`, `handler.execute(&mut ctx, &command.args)` — and **four routes**
that could plausibly arrive at it. All four are closed for a `ServerWide` command:

1. **Ordinary dispatch.** `PRE_DISPATCH_ORDER` (`dispatch.rs:124-141`) is a 16-element `const` array
   with `DispatchStage::ServerWide` at index **13** and `DispatchStage::Execute` at index **15**.
   `dispatch_server_wide` routes `ServerWideOp::Scan` to `self.handle_scan(args)`
   (`dispatch.rs:238`) and returns; `Execute` is never reached.
2. **EXEC.** `transaction.rs:141`/`:240` defer queued server-wide commands back into
   `dispatch_server_wide`, not into the executor — the same door.
3. **Scripting.** `core/src/scripting/gate.rs` refuses them before they get near a handler:
   `reject_server_wide` (trait `:164`, call `:278`, impl `:441-451`) matches on
   `handler.execution_strategy()` and returns *"ERR {} is not allowed from scripts: server-wide
   commands cannot run inside a shard-local script"* for any `ServerWide(_)`. The script path's own
   `handler.execute` (`gate.rs:506`, and `shard/scripting.rs:223`) sits **behind** that gate.
4. **Keyed cross-shard routing.** `server/src/connection/routing.rs:137-140` derives the scatter op
   from the declared strategy and returns `redirect::crossslot()` for anything that is not
   `ScatterGather(_)` — a `ServerWide` command gets `-CROSSSLOT`, not an executor.

So `ScanCommand::execute` (`scan.rs:67-105`) and `KeysCommand::execute` (`:134-147`) are dead on
every route. The proof is **stronger** than the two-route version this proposal originally stated:
routes 3 and 4 close by *strategy*, which means no future handler-registration change can reopen
them without also changing the declared strategy. Three consequences, in ascending order of how much
they should bother us:

1. **A third grammar is maintained for nothing.** `scan.rs:81-92` parses `MATCH`/`COUNT`/`TYPE` via
   `ArgParser`; `scatter.rs:68-118` parses the same three options via a hand-rolled index loop with
   its own error strings; `utils.rs:194-225` parses two of the three for the *other* three commands.
   Adding a SCAN option today means finding all three.

2. **The dead copy is a decoy, not a stub.** Proposal 67's SV6 catalogued the server-wide commands
   whose `execute()` bodies are a one-line "reached shard executor" refusal, and **explicitly
   excluded eight** on the grounds that they *"carry real `execute()` bodies"* (67:672-676: DBSIZE,
   FLUSHDB, FLUSHALL, SCAN, KEYS, RANDOMKEY, ES.ALL, and the gate's test probe). That is the
   trap: the bodies are real, so they read as live code, so nobody deletes them — but if a future
   dispatch refactor ever let one through, `ScanCommand::execute` would call
   `ctx.store.scan_filtered(...)` on **one** shard and return a well-formed `[cursor, [keys]]` that
   silently omits every other shard. A refusal fails loudly; a plausible wrong answer does not. The
   comment at `scan.rs:95` (*"In connection.rs, this is routed based on cursor to the correct
   shard"*) documents a routing scheme that does not exist — `handle_scan` walks shards itself
   (`scatter.rs:133-182`).

3. **The false doc.** `utils.rs:172-173` advertises `ScanRequest` as *"shared by the whole SCAN
   family (SCAN, HSCAN, SSCAN, ZSCAN)"*. `grep -rn ScanRequest` returns exactly three call sites,
   none of them SCAN. The abstraction that was built to be the single owner of this grammar never
   acquired its most important client.

#### The server-wide census, so the "SV6 covers everything afterwards" claim is not overstated

`grep -rn 'ExecutionStrategy::ServerWide' frogdb-server/crates` returns 42 hits; six are not spec
declarations (`gate.rs:443` and `guards.rs:670` pattern-match on the variant, `dispatch.rs:224` is a
doc comment, `dispatch.rs:713`/`:1059` and `transaction.rs:141` destructure it). **36 spec
declarations** remain — 35 commands plus the gate's test probe (`gate.rs:980`). Eight of the 36 carry
real bodies and are the ones 67's SV6 excluded:

| Excluded by 67 SV6 | Site | Body |
|---|---|---|
| DBSIZE | `server/src/commands/server.rs:40` | reads `ctx.store.len()` on **one** shard |
| FLUSHDB | `server/src/commands/server.rs:72` | `ctx.store.clear()` on **one** shard |
| FLUSHALL | `server/src/commands/server.rs:120` | `ctx.store.clear()` on **one** shard |
| **SCAN** | `commands/src/scan.rs:62` | **98 deletes it** |
| **KEYS** | `commands/src/scan.rs:129` | **98 deletes it** |
| RANDOMKEY | `commands/src/generic.rs:648` | one-shard pick |
| ES.ALL | `commands/src/event_sourcing/all.rs:29` | one-shard read |
| gate test probe | `core/src/scripting/gate.rs:980` | not a command |

**98 clears two of the eight.** After 98, 67's SV6 covers **30 of 36**, not 30 of 30 — the remaining
six residue owners belong to nobody, and two of them (FLUSHDB, FLUSHALL) are *write* decoys, which is
strictly worse than SCAN's read decoy: a one-shard `clear()` reached by a future refactor destroys
data and returns `+OK`. **Recorded as issue I3 (server-wide decoy residue, six commands).** 98 scopes
itself to SCAN/KEYS deliberately — the other six need the same reachability proof done per command,
and that is not this proposal's argument.

### 2. Six glob implementations, measured against each other (state at `dd840ca3`; HF-B has since folded two)

Ranked by what they cost:

| # | Location | Supports | Ruling | On `main` today |
|---|---|---|---|---|
| 1 | `types/src/glob.rs:23` `glob_match` | `*` `?` `[abc]` `[a-z]` `[^a]` `[!a]` `\` + 4 fast paths + star cap | **Canonical.** | present |
| 2 | `commands/src/utils.rs:60` `simple_glob_match` (doc from `:57`) | `*` `?` only | **Folded by HF-B.** Had exactly one call site (`utils.rs:148`). | **deleted** |
| 3 | `core/src/shard/search/mod.rs:23` `glob_match_simple` | exact `*`, `*x*`, `*x`, `x*`, else literal `==`; **case-insensitive** | **Fold — H3.** | present |
| 4 | `frogctl/src/commands/watch.rs:64` `glob_match` | `*` `?`, case-insensitive | **Keep.** §Problem 3. | present |
| 5 | `testing/src/pubsub_oracle.rs:242` `glob_match` | `*` `?` | **Keep.** §Problem 3. | present |
| 6 | `scripting/src/registry.rs:199` `matches_pattern` (+ `match_pattern_recursive` `:205`) | `*` `?`, **recursive** | **Folded by HF-B.** | **deleted** |

**Six → four (HF-B) → three (H3).** Rows 2 and 6 are retained because they are the record of what HF-B
changed; they are not work items.

**The table below is now historical rationale, not a proposal.** It is the evidence that justified
HF-B, and it is the only written statement of what HF-B changed on the wire — which is why it stays,
and why §Risks' compat caveat is written about *shipped* behavior rather than *pending* behavior. It
is machine-produced by running both functions, copied verbatim from `dd840ca3`, over the same inputs.
Column 3 is what HSCAN/SSCAN/ZSCAN answered **before** HF-B; column 4 is what SCAN and KEYS answered
then and what all five answer now.

| pattern | text | H/S/ZSCAN pre-HF-B (`simple`) | canonical — SCAN/KEYS then, **all five now** | |
|---|---|---|---|---|
| `[ab]*` | `apple` | false | true | **DIVERGE** |
| `[ab]*` | `banana` | false | true | **DIVERGE** |
| `[ab]*` | `cherry` | false | false | agree |
| `[ab]*` | `[ab]xyz` | true | false | **DIVERGE** |
| `[a-c]` | `b` | false | true | **DIVERGE** |
| `[^a]` | `b` | false | true | **DIVERGE** |
| `[!a]` | `b` | false | true | **DIVERGE** |
| `a\*` | `a*` | false | true | **DIVERGE** |
| `a\*` | `abc` | false | false | agree |
| `a\*` | `a\*` | true | false | **DIVERGE** |
| `\?` | `?` | false | true | **DIVERGE** |
| `\?` | `x` | false | false | agree |
| `*` | `anything` | true | true | agree |
| `foo*` | `foobar` | true | true | agree |
| `*a*` | `bab` | true | true | agree |
| `key:1??` | `key:123` | true | true | agree |
| `a?c` | `abc` | true | true | agree |

Note the divergence runs **both ways**: `simple` treated `[ab]` and `\*` as literals, so it won the
`[ab]xyz` and `a\*` rows that the canonical matcher (correctly, per Redis) loses. That is why HF-B was
a behavior *change* toward Redis and not a pure bug fix — and why §Risks keeps the caveat even though
the change has shipped.

Plus the star-cap row:

```
`*?` x50000 vs 'a' x50000: simple=true (66.5µs)   canonical=false (417ns)
```

`MAX_STAR_COUNT = 100` makes the canonical matcher return `false` for a pattern that genuinely
matches. That is a deliberate, Redis-mirroring guard (Redis caps at
`GLOB_MATCH_MAX_RECURSION = 100`), but the mirroring is **structural, not behavioral** — Redis's cap
bounds recursion depth in a recursive matcher, FrogDB's bounds star count in an iterative one, and
whether the two agree on a given adversarial pattern is **not verified here**. Flagged as an
open question, not claimed as a divergence.

### 3. Two implementations that must NOT be folded, and why (so the next round stops asking)

Both were checked at the `Cargo.toml` level, because that is where the answer lives:

**`frogctl/src/commands/watch.rs:64`.** Stated precisely, because the loose version of this claim is
false: `frogctl/Cargo.toml`'s **`[dependencies]` block (`:27-44`) contains no `frogdb-*` crate**. There
*is* one `frogdb-*` entry in the file — `frogdb-test-harness` at `:47` — but it is a
**`[dev-dependencies]`** entry, so it does not ship in the binary and does not constrain the runtime
graph. The shipped CLI shares no code with the server; it speaks RESP over a socket.

Folding this would mean adding `frogdb-types` to the CLI's **production** graph, which drags in
`usearch` (`types/Cargo.toml:26`) and `murmur3` (`:20`) — to save 30 lines in a `--match` filter that
runs client-side over already-fetched output. *Not* tokio: `frogctl` already declares
`tokio.workspace = true` at `:32`, so tokio is not a cost of the fold and claiming it would be
padding the argument. Two real transitive additions is still the answer, and the two that are real
are enough.

The correct fix is the **documentation** one: this matcher is **case-insensitive** and the canonical
one is not, so `frogctl watch --match` silently behaves unlike server-side `MATCH`. That is a
one-line help-text change — **H4**.

**`testing/src/pubsub_oracle.rs:242`.** Its own doc comment (`:239-241`) states the reason:
*"Self-contained so the oracle has no dependency on the server crate; the workload only ever uses
trailing-`*` patterns, but the full matcher keeps the checker honest."* A differential oracle that
imports the implementation under test proves nothing — the divergence it exists to detect becomes
definitionally unreachable. `testing/Cargo.toml`'s `[dependencies]` (`:10-15`) is six small crates —
`base64`, `bytes`, `rand`, `serde`, `serde_json`, `thiserror` — and **no `frogdb-*` at all**.

**Correction to an earlier draft of this section:** it claimed the file "already calls the canonical
glob at `:225`". It does not. `SubKind::covers` at `:225` reads
`SubKind::Pattern(p) => glob_match(p, channel)`, and that `glob_match` resolves to the **local** `fn`
at `:242` — the file's *only* matcher. Its imports (`:53-56`: `bytes`, `rand`, `std::collections`)
contain no `frogdb_types`. The KEEP ruling does not depend on the retracted sentence and survives
intact on the two facts that are real: the `Cargo.toml` has no server dependency, and the doc comment
says independence is the point.

Proposal 82 lists this file **read-only except a doc-comment hotfix** — no conflict.

### 4. Secondary, unpinned: `COUNT 0` misbehaves on both paths — but **differently**, and only one is a loop

Not the centerpiece; recorded because the unification is where both get fixed for free. An earlier
draft asserted "infinite loop on both paths". That is right for H/S/ZSCAN and **wrong as stated** for
SCAN. The corrected statement, with the cursor arithmetic done:

**HSCAN/SSCAN/ZSCAN — genuine non-termination, reachable from cursor 0.** `hash_cursor_scan`
(`utils.rs:140-144`, pre-HF-B) enters `for (hash, item) in hashed.into_iter().skip(start)` and hits
`if emitted >= count { new_cursor = hash; break; }` on the **first** item with `0 >= 0`. So the call
returns `(hash_of_first_item, [])`. The client's next call passes that hash as the cursor,
`partition_point` (`:130-134`) lands on the same item, and the same hash comes back. **From
`HSCAN k 0 COUNT 0` onward the client never terminates** for any non-empty collection. (An empty
collection returns `new_cursor = 0` and terminates — the initialiser at `:138`.)

**SCAN/KEYS — no loop from cursor 0; an empty reply and immediate termination.** `scatter.rs:133`'s
`while all_keys.len() < count && next_shard < self.num_shards` is false immediately, so
`next_shard`/`next_position` keep their decoded values and `encode_final_cursor`
(`scatter.rs:364-370`) runs `cursor::encode(next_shard as u16, next_position)`
(`scan.rs:29-31`: `((shard_id as u64) << POSITION_BITS) | (position & POSITION_MASK)`). For a client
that started at cursor `0` that is `encode(0, 0)` = **`0`** — so `SCAN 0 COUNT 0` replies
`["0", []]` and the iteration **ends**, silently returning nothing rather than spinning.
Non-termination on this path requires a **non-zero** cursor: `SCAN <c> COUNT 0` with `c != 0` echoes
`c` back forever, because encode∘decode round-trips. That is still a bug — a client that resumes a
paged scan with `COUNT 0` hangs — but it is a *narrower* bug than the H/S/ZSCAN one and the two need
separate pins.

Neither grammar rejects `COUNT 0`. `grep` for `COUNT", "0"` across `frogdb-server/crates` finds pins
for HOTKEYS, ZMPOP, BZMPOP, LMPOP, LPOS — **none for the SCAN family**. Redis's `scanGenericCommand`
rejects `count < 1` with a syntax error; that claim is from knowledge of the Redis source, **not
verified against a running server here**.

**Ruling needed (R2), and it has two axes, not one.** A bare "reject `count < 1`" changes *both*
behaviors: it fixes the H/S/ZSCAN hang **and** turns today's `SCAN 0 COUNT 0` empty-but-terminating
reply into an error. Whether the second is desirable is a separate compat question from the first,
and the ruling must answer both. One shared parser means one place to implement whichever answer
comes back.

### 5. Adjacent finding, flagged not proposed: `hash_cursor_scan` ignores COUNT for non-matching items

`utils.rs:140-151` (pre-HF-B) applies the MATCH filter **after** the `emitted >= count` break. An item
that fails the pattern `continue`s without incrementing `emitted`, so a pattern matching nothing walks
the **entire** collection in a single call — COUNT provides no bound on work, only on output. Redis
bounds work, not output, for exactly this reason. Separately, `utils.rs:123-128` collects and sorts
the whole collection on **every** call, so a full HSCAN iteration of an *n*-element hash is
O(n² log n) in aggregate regardless of COUNT.

This is a real cost bug and it is **out of scope for 98** — fixing it changes cursor semantics and
belongs in its own proposal with its own compat pins. **Filed as issue I2 (`hash_cursor_scan` cost).**
Recorded here because 98 reads the function and a reader will otherwise ask.

## Proposed change

### The move: one grammar module, capability-shaped

**Step A — promote and widen `ScanRequest`.** Move `ScanRequest`, `ScanRequest::parse`,
`empty_scan_reply`, and `scan_reply` out of `commands::utils` (a grab-bag) into `commands::scan` (the
module that already owns the wire concept, and already exports `cursor` across the crate boundary to
`scatter.rs:44`). This is a **locality** move: the SCAN wire format's cursor codec and its argument
grammar stop living in two modules.

Widen the struct to carry the full grammar rather than two-thirds of it:

```rust
pub struct ScanRequest<'a> {
    pub cursor: u64,
    pub pattern: Option<&'a [u8]>,
    pub count: usize,
    pub key_type: Option<KeyType>,  // SCAN only
    pub novalues: bool,             // HSCAN only
}
```

**Step B — replace the closure hook with a capability record.** Today the command-specific options
enter through `extra: impl FnMut(&mut ArgParser<'a>) -> Result<bool, CommandError>`
(`utils.rs:196`) — a closure that mutates a captured local (`hash.rs:739-746` sets `novalues` by
side effect) and returns a bool meaning "I consumed a token". Two of the three callers pass
`|_| Ok(false)` (`set.rs:1113`, `sorted_set/scan.rs:40`), which is a closure whose whole job is to
say "not applicable".

Replace it with data:

```rust
pub struct ScanCaps { pub key_type: bool, pub novalues: bool }
impl ScanCaps {
    pub const NONE: Self       = Self { key_type: false, novalues: false };
    pub const KEYSPACE: Self   = Self { key_type: true,  novalues: false };
    pub const HASH: Self       = Self { key_type: false, novalues: true  };
}

pub fn parse(args: &'a [Bytes], caps: ScanCaps) -> Result<Self, CommandError>
```

Why this is the better **interface**, not merely a different one: a closure is an opaque escape hatch
— the parser cannot see what its caller will accept, so it cannot reason about the grammar it is
implementing. A record is inspectable, which is what buys the testability in §Testability. It also
puts `TYPE`'s six-arm key-type mapping in **one** place (the parser) instead of two
(`scan.rs:151-163` dead, `scatter.rs:100-113` live) — and that mapping is precisely the thing the
brief noticed was duplicated.

The **adapter** framing is the honest one for `scatter.rs`: `handle_scan` is the connection-side
adapter between the wire and the shard fan-out. Its job is the cursor walk (`:120-193`), which is
genuinely connection-side and stays. Parsing is not its job; it does it only because nothing
exported a parser it could call.

**Step C — call it from `scatter.rs`.** `handle_scan:46-118` collapses to roughly:

```rust
let request = match ScanRequest::parse(args, ScanCaps::KEYSPACE) {
    Ok(r) => r,
    Err(e) => return e.to_response(),
};
let (shard_id, position) = cursor::decode(request.cursor);
```

Note `to_response()`, not `e.into()`: `handle_scan` returns `Response`, and the **only**
`impl From<CommandError>` in the workspace is `core/src/error.rs:119`, `From<CommandError> for
FrogDbError`. There is no `From<CommandError> for Response`, so the `.into()` an earlier draft wrote
does not compile. `to_response()` is the conversion the executor itself uses
(`shard/execution.rs:242-243`, `Err(err) => err.to_response()`).

Two live behaviors in `:46-118` are **not** grammar and must be accounted for, not swept up:

**(a) The empty-args guard `:46-48`** returns `"ERR wrong number of arguments for 'scan' command"`.
This is **provably dead**, and the proof is the same shape as §Problem 1's: `ScanCommand`'s spec
declares `arity: Arity::AtLeast(1)` (`scan.rs:51`); `Arity::check` (`core/src/command.rs:891-897`)
counts arguments **excluding** the command name (its own doc: *"GET = Fixed(1)"*); and
`DispatchStage::CommandLookup` — index **6**, ten stages ahead of `ServerWide` at 13 — calls
`command_lookup_check(cmd_name, &cmd.args)` and short-circuits on failure
(`dispatch.rs:549-558`, whose comment says it *"validate[s] its arity BEFORE the pause check"*).
`SCAN` with zero arguments is therefore rejected at stage 6, with the string `WrongArity` renders
(`types/src/error.rs:116`: `"ERR wrong number of arguments for '{command}' command"`) — **byte-identical
to the guard's literal.** The guard is a fifth decoy and Step C may delete it rather than port it.
Two caveats the implementer owns: the MULTI route rejects at queue time via `queue_command` rather
than at `CommandLookup` (`dispatch.rs:543-550`), and this is a *reading* proof, not an executed one —
so pin `SCAN` with no arguments in Step C's acceptance regardless. It costs one line.

**(b) The `TYPE` token is lower-cased and the lower-cased form is echoed.** `scatter.rs:99` does
`let type_str = args[i].to_ascii_lowercase();` and `:108-111` formats
`"ERR unknown type: {}"` over **`type_str`**, not over the original bytes — so `SCAN 0 TYPE FOO`
answers `ERR unknown type: foo` today. The promoted parser must reproduce **both** halves: accept the
key type case-insensitively *and* down-case it in the error. This is exactly the kind of detail a
"move the parser" change loses silently, and it is client-visible.

The error-mapping subtlety is the step's main risk. `scatter.rs` emits raw strings while
`ScanRequest::parse` yields `CommandError` variants. There are **six** distinct strings, not five —
`"ERR syntax error"` has **four** sites, not three:

| `scatter.rs` site | String | `CommandError` variant |
|---|---|---|
| `:53`, `:57` | `ERR invalid cursor` | `InvalidArgument { message: "invalid cursor" }` |
| `:75` (MATCH, no value) | `ERR syntax error` | `SyntaxError` |
| **`:82` (COUNT, no value)** | `ERR syntax error` | `SyntaxError` |
| `:97` (TYPE, no value) | `ERR syntax error` | `SyntaxError` |
| `:115` (unknown option) | `ERR syntax error` | `SyntaxError` |
| `:90` | `ERR value is not an integer or out of range` | `NotInteger` |
| `:108-111` | `ERR unknown type: {lowercased}` | `InvalidArgument { message: "unknown type: …" }` |
| `:46-48` (dead, see (a)) | `ERR wrong number of arguments for 'scan' command` | `WrongArity { command: "scan" }` |

The mapping is now **traced**, not merely asserted: the rendering lives in the
`define_command_errors!` block at `types/src/error.rs` — `InvalidArgument { message } => "ERR
{message}"` (`:119`), `SyntaxError => "ERR syntax error"` (`:122`), `NotInteger => "ERR value is not
an integer or out of range"` (`:132`), `WrongArity { command } => "ERR wrong number of arguments for
'{command}' command"` (`:116`) — and the variants are produced by the `ArgParser` primitives in
`types/src/args.rs` (`parse_usize :316-319` → `NotInteger`; the flag/value helpers `:90-92`,
`:119-122`, `:206-212` → `SyntaxError`). Every string above is accounted for by a rendering rule that
was read. What remains unverified is the **wire**: nothing was byte-compared against a running
server. Step C's acceptance is a test that drives all **six** error paths through a real connection
and byte-compares.

Option-token case-insensitivity already agrees: `scatter.rs:70` uppercases the token, and
`ArgParser::try_flag` (`types/src/args.rs:173-181`) uses `eq_ignore_ascii_case`.

**Step D — delete.** `ScanCommand::execute`, `KeysCommand::execute`, and `parse_key_type` go. The two
`CommandSpec` statics stay. `Command` requires an `execute`, so the two commands take the same
one-line refusal the other 28 server-wide commands already carry (proposal 67's SV6 catalogue) —
which is what makes them *stop* being decoys, and what moves 67's SV6 from **28 of 36 to 30 of 36**.
The remaining six (DBSIZE, FLUSHDB, FLUSHALL, RANDOMKEY, ES.ALL, gate probe) stay excluded; see the
census in §Problem 1 and issue I3.

### Deletion test, per module

Line numbers below are pre-HF-B; the `utils.rs` row is the one HF-B moved, and its `simple_glob_match`
contribution is now **HF-B's, not 98's**.

| Module | Lines out | Lines in | Net |
|---|---|---|---|
| `commands/src/scan.rs` | 39 (`execute` `:67-105`) + 14 (`execute` `:134-147`) + 14 (`parse_key_type` `:150-163`) = **67** | ~4 (two refusal bodies) + ~35 (`ScanRequest` + `ScanCaps`, relocated in) | dead code gone; relocation neutral |
| `commands/src/utils.rs` | ~95 (`ScanRequest` `:172-226`, `empty_scan_reply` `:228-235`, `scan_reply` `:237-267`) | 0 | **relocated out**, not deleted |
| `server/src/connection/scatter.rs` | 51 (`:46-118` grammar + the dead arity guard) | ~6 | **−45** |
| `core/src/shard/search/mod.rs` (H3) | 18 (`:22-39`) | 0 | **−18** |
| ~~`commands/src/utils.rs` `simple_glob_match` `:57-88`~~ | ~~32~~ | — | **HF-B's, already on `main` — struck from 98's ledger** |

**Bottom line: ≈ −98 production lines.** The earlier headline of −130 counted HF-B's 32-line
`simple_glob_match` deletion; that has landed and is no longer 98's to claim, so the figure comes down
by exactly that much. Three wire grammars become one, and the glob count goes **four → three**
(canonical + two justified independents).

Precision caveat, because a deletion test that overstates itself is self-defeating: the per-module
numbers are pre-HF-B `wc` readings, and how much of the `utils.rs` → `scan.rs` relocation nets out
depends on how the moved parser's doc comments land in its new home. The **shape** of the claim —
one grammar replaces three, and the largest single block deleted is unreachable code — does not
depend on the last dozen lines either way.

The sharper test is the one that does not count lines. Ask of each shallow module: *if I delete it,
what stops working?* — `parse_key_type`: nothing, it is dead. `ScanCommand::execute`: nothing, it is
unreachable. `scatter.rs:68-118`: SCAN option parsing, which is the shared parser's job.
`glob_match_simple`: FT.CONFIG GET's pattern filter, which the canonical glob does strictly better.
Four modules, four answers, and only one of them ("FT.CONFIG needs *a* matcher") is a reason to keep
*code* rather than a reason to keep *this* code.

### H3 in detail: FT.CONFIG's matcher

`glob_match_simple` (`search/mod.rs:23-39`) is called from exactly one place —
`search/config.rs:42`, imported at `config.rs:5` — against a fixed four-entry table of uppercase keys
(`MINPREFIX`, `MAXEXPANSIONS`, `TIMEOUT`, `DEFAULT_DIALECT`, `config.rs:33-38`).

**What it actually supports, read off the body (`mod.rs:23-39`), because the earlier "leading and/or
trailing `*`" summary was loose enough to make the identity claim below wrong:**

| Pattern shape | Handled as |
|---|---|
| exactly `*` | `return true` (`:24-26`, before the case fold) |
| `*x*` | `t.contains(x)` |
| `*x` | `t.ends_with(x)` |
| `x*` | `t.starts_with(x)` |
| anything else | **literal `p == t`** |

So `?` is a **literal character**, and so is an **interior** `*`: `MIN*FIX` falls through every
`strip_prefix`/`strip_suffix` arm to `p == t` and matches nothing. Both sides are
`to_ascii_uppercase`d at `:28-29`, which is why case-handling is a non-issue here — the four keys are
already uppercase ASCII literals (`config.rs:33-38`).

**The corrected identity claim.** The earlier draft said the fold is "behavior-identical for every
pattern containing no `[`, `]`, or `\`". That is **false and self-contradicting** — it is the same
sentence that then advertises `TIMEOU?` as a fixed divergence, and `?` is neither `[`, `]`, nor `\`.
Stated correctly, the two matchers agree on exactly three shapes:

1. the pattern `*`;
2. a wildcard-free literal (no `*`, `?`, `[`, `\`) — both reduce to equality, modulo the case fold;
3. a **single edge** `*` (`x*`, `*x`, or `*x*`) whose remainder is wildcard-free.

**For every other pattern H3 is a behavior change, not a refactor** — and the direction is toward
Redis, which is the reason to do it. Two live divergences it fixes, both worth pinning:

- `FT.CONFIG GET TIMEOU?` returns **empty** today (`?` is a literal) where Redis returns `TIMEOUT`.
- `FT.CONFIG GET MIN*FIX` returns **empty** today (interior `*` is a literal) where the canonical
  matcher returns `MINPREFIX`.

Both go in as regression pins with H3; the second is the one that proves the fold changed more than
`?`-handling, which is precisely what the retracted sentence obscured.

`frogdb-core` already depends on `frogdb-types` and already calls `glob_match` in four places
(`pubsub.rs:487`, `shard/execution.rs:785`, `store/hashmap.rs:1085`, and via
`commands/src/scan.rs:141`'s `frogdb_core::glob_match` re-export), so the fold costs no dependency.

## Testability improvement

**Today the SCAN grammar cannot be unit-tested at all.** The only reachable copy lives inside
`handle_scan`, an `async fn` on the connection type that needs `self.core.shard_senders`,
`self.state.id`, and `self.num_shards` — i.e. a running server. So every assertion about SCAN's
argument handling is an integration test over a socket (`scan_tcl.rs`), and the parser's error paths
are correspondingly under-covered. All **eleven** `utils.rs` inline tests (§Files involved) exercise
the copy that SCAN does not use — including the two that pin the closure protocol Step B removes.

After the move, `ScanRequest::parse(args, ScanCaps::KEYSPACE)` is a pure function over `&[Bytes]`
returning `Result<ScanRequest, CommandError>`. Three things become cheap that are expensive today:

1. **Table-driven grammar tests in `frogdb-commands`.** Every option order, every duplicate, every
   truncated flag, every bad key type — as `#[test]`s in the crate that owns the grammar. This is the
   mutation-score point too: `cargo mutants -p <crate>` runs only that package's own tests, so
   grammar tests written as `frogdb-server` integration tests contribute nothing to
   `frogdb-commands`' score.

2. **Capability-matrix tests.** With `ScanCaps` as data, one test asserts the property the closure
   design cannot express: *for every `(caps, option)` pair, the option is accepted iff the capability
   is set.* The claim is narrowed to the **negative** direction, which is where the genuine gap is:
   `HSCAN k 0 TYPE string` must be a syntax error, and `SCAN 0 NOVALUES` must be one too. Neither is
   pinned anywhere today. The *positive* direction is already covered —
   `hscan_extra_flag_novalues_toggles` (`utils.rs:1109`) and
   `extra_hook_takes_precedence_over_syntax_error` (`:1149`) pin that `NOVALUES` is accepted where it
   applies, which is exactly why those two tests are rewrites rather than deletions. What `ScanCaps`
   buys is that the negative half becomes expressible as one table rather than a hand-written case
   per pair.

3. **A differential test against the dead copy — before it is deleted.** Step D is the delete;
   step C can first assert that `ScanRequest::parse` and `scan.rs:81-92`'s loop agree on a corpus of
   argument vectors. That converts "I read both and they look the same" into a green test, and then
   the test is deleted with its subject. This is the only chance to get that assurance, and it exists
   only because the dead copy is still there.

Regression additions this proposal owns: SCAN/HSCAN `COUNT 0` on **both** axes (§Problem 4, after the
R2 ruling), `FT.CONFIG GET TIMEOU?` **and `MIN*FIX`** (H3), `SCAN` with no arguments (step C(a)), and
the **six** `scatter.rs` error strings including the lower-cased `unknown type` echo (step C).

## Risks / scope boundaries

### The compat risk, stated plainly — **shipped, not pending**

HF-B **changed** what HSCAN/SSCAN/ZSCAN `MATCH` means for patterns containing `[`, `]`, or `\`, and
that change is live on `origin/main` (`d48e1b44`). This subsection is retained as the operator-facing
record of a behavior change that already happened, not as a gate on future work.

Per §Problem 2's table the change is bidirectional: `HSCAN h 0 MATCH "[ab]*"` now matches fields
beginning with `a` or `b` (Redis-correct) and no longer matches the literal field `[ab]xyz`
(Redis-correct, and a behavior loss for anyone who relied on it). Two facts bounded the risk when it
landed and still bound the blast radius:

- **Nothing in the repo pinned the old behavior.** `grep -rn 'MATCH", "\['` and
  `grep -rn 'MATCH", ".*\\\\'` across `frogdb-server/crates` returned **zero** hits. Every existing
  MATCH pin (`scan_tcl.rs:159`, `:190`, `:572`, `:582`, `:608`, `:635`, `:689`, `:820`) uses only
  `*` and `?`, on which the two matchers agree.
- **`simple_glob_match` had exactly one call site** (`utils.rs:148`, inside `hash_cursor_scan`), so
  the blast radius is precisely the three per-key SCAN commands.

The acceptance gate — a green `just test frogdb-redis-regression scan` over the 181 new
`scan_regression.rs` lines — belonged to the lander and is discharged on `main`. **It was never run in
this session** (no build; §Honesty), so this document does not assert the outcome, only that the
obligation transferred with the commit.

Nothing in 98's remaining scope depends on it.

### Sibling boundaries

| Sibling | Contact | Ruling |
|---|---|---|
| **90** (commandspec-default) | Same file `commands/src/scan.rs`; 90 rewrites the `CommandSpec` statics `:49-63` and `:116-130`, 98 deletes the `execute` bodies below them (`:67-105`, `:134-147`). Also same file `utils.rs`, no static there. | **Disjoint regions, but 98 must land first.** 90's own §"land SOLO, and land LAST" (90:507) and its commit-3 sweep are a scripted `awk` pass over 56 files (90:578); running it against a `scan.rs` that still contains the dead bodies is harmless, but 98 rebasing onto a swept `scan.rs` means re-deriving line numbers. **Ordering CONFIRMED: 98 before 90's commit 3**, on 90's own text at 90:507 and 90:578. |
| **94** (resp3-shape-once) | Same file `commands/src/utils.rs`. | **Disjoint.** 94 owns `:791-920` (`score_response` … `members_array`, its H1 is `:895-912`); 98 owns `:53-267`. Verified against 94's files table (94:73). Both are **pre-HF-B** numbers. HF-B's deletion (`:57-88`) is inside 98's region and above 94's, so 98's region shrinks at its head while 94's slides down ~28 lines with its content untouched. **Disjointness is preserved either way** — the deletion cannot move 94's block into 98's. No overlap, either order. |
| **70** (acl-registry-consult) | Same file `core/src/shard/search/config.rs` — H3 edits the call at `:42`, inside the `GET` arm. | **No conflict at HEAD.** 70 lists this file **read-only** (70:116) and reads the dispatch shape `:20-78` to derive an ACL declaration; its edits land in the acl crate and `command_spec.rs`. H3's one-line change inside `GET`'s body does not alter the `match` structure 70 reads. If 70 ever converts to an edit here, H3 rebases trivially. |
| **71** (search-query-plan) | Same directory `core/src/shard/search/`. | **Different file.** 71 edits `query.rs`; H3 touches `mod.rs` + `config.rs`. 71's own boundary table (71:737) already draws this line. |
| **82** (pubsub-channel-table) | Same file `testing/src/pubsub_oracle.rs`. | **No conflict.** 82 is read-only there except a doc-comment hotfix (82:84, its H2). 98 is read-only there, full stop — its ruling is *keep*. |
| **67** (server-small-dedups, SV6) | Directly enabling. | 67's SV6 (67:672-676) excluded eight commands because they *"carry real `execute()` bodies"*. 98 step D removes the exclusion for two of them. **Ordering CONFIRMED: 98 lands before 67's SV6**; 67 SV6 then covers **30 of 36**. Not a conflict — 67 has not landed (all 28 copies still present at HEAD). The six-command residue is issue I3. |
| **49** (function-registry-surface) | 49:578 names `0b034a4f`. | **98 is the second citation, not a rediscovery.** 49 named the commit for the `scripting/registry.rs` half; 98 names it for the `commands/utils.rs` half. Neither re-authors it, and as of `d48e1b44` neither can — it has landed. |
| **97** (typed-store-access) | **None.** | **Zero contact, verified.** `grep -n 'scan\.rs\|utils\.rs\|scatter\.rs\|glob'` over `97-typed-store-access.md` returns **no hits**. No file, no region, no ordering constraint in either direction. Listed so the absence is on the record rather than inferred. |
| **99** (vectorset-file-collapse) | **No file conflict — but a grammar-ownership question.** | 99 owns `commands/src/vectorset/*`; 98 owns none of it. **The coordination item is that `VRANGE` is a fourth cursor+`COUNT` grammar**: `vrange.rs:36-62`, with the *same* count ladder written twice inside one `if/else` (`:42-49` and `:51-58`, both yielding `"Invalid count"` — 99:80, 99:175-176). 99 unifies it onto `utils::parse_usize` / `types/src/args.rs:316-319`, **not** onto `ScanRequest`, which is the right call — VRANGE's cursor is a vector-index cursor, not a keyspace cursor, and `ScanCaps` has nothing to say about it. But it does mean the repo ends the round with **two** cursor-grammar owners. That is a defensible outcome and it should be a *decided* one. **Escalated as issue I1 for orchestrator ruling; 98 does not resolve it and does not block on it.** Note also that `vrange.rs` is behind `#[cfg(feature = "vectorset")]` (`commands/src/lib.rs:59`), so it is **invisible to a default `just check`** — unlike 98's files, which are all `core-profile`. |

### Locked-crate ruling

**No locked crate is touched.** Files edited live in `frogdb-commands`
(`crates/commands/{scan,utils,hash,set,sorted_set/scan}.rs`), `frogdb-server`
(`crates/server/src/connection/scatter.rs`), and — for H3 only — `frogdb-core`
(`crates/core/src/shard/search/{mod,config}.rs`). None of the eight locked crates (`frogdb-txn`,
`frogdb-vll`, `frogdb-persistence`, `frogdb-recovery`, `frogdb-replication`,
`frogdb-replication-runtime`, `frogdb-cluster`, `frogdb-cluster-runtime`) appears.

`scatter.rs` is in **`frogdb-server`**, not `frogdb-cluster-runtime` — the brief's suspicion was
reasonable and wrong; the path is `frogdb-server/crates/server/src/connection/scatter.rs`, verified
against `crates/server/Cargo.toml` (`name = "frogdb-server"`).

`grep -c "FM-"` over all five edited files returns **0** for each. **No mutation-gate obligation;
`just mutants-diff` is not required by policy for this change.** (Running it on `frogdb-commands`
anyway is cheap insurance for the new parser, but that is a choice, not the gate.)

### Seam-lint clearance

Fourteen gates run under `just lint-gates` (`Justfile:329`). One is scoped to a file 98 edits:
**`lint-info-seam`** (`Justfile:423-441`) lists `connection/scatter.rs` among its three files and
fails on `grep -nE '\.replace\("[a-z_]+:0\\r\\n"|\.replace_range\('`. Step C introduces neither — it
*removes* code and adds one parser call. **Clear.**

No other gate names any edited file. `lint-keyspace-notify-routing`, `lint-redirect-seam`,
`lint-clock-seam`, `lint-durable-ack`, `lint-continuation-lock`, `lint-script-gate` were checked
against the file list; no intersection.

### What this proposal deliberately does not do

- Does not touch `hash_cursor_scan`'s cost behavior (§Problem 5) — separate proposal, separate pins
  (issue I2).
- Does not touch the cursor walk in `scatter.rs:120-193`, `encode_final_cursor` `:364-370`, or
  `absorb_scan_reply` `:352`.
- Does not fold impls #4 and #5 — §Problem 3 argues they should never be folded.
- Does not re-author or re-propose `0b034a4f` / HF-B, which has landed.
- Does not touch `VRANGE`'s cursor grammar (`vectorset/vrange.rs:36-62`) — proposal 99's file, and
  the ownership question is issue I1.
- Does not claim the six remaining server-wide decoys (issue I3).
- Does not add a work budget to `glob_match` — see below.

## Flagged for orchestrator/user sign-off (security-adjacent, NOT proposed) — **R1, ESCALATED**

**Handling rule, stated first and binding: this section is DOCUMENT-ONLY.** Security work is parked
per standing user direction. No code in §Proposed change, no hotfix, and no acceptance test below
implements any fix. **Escalated: the H/S/ZSCAN amplifier is live on `main` as of HF-B
(`d48e1b44`) — it is present-tense today, not conditional on a future merge. Awaiting user ruling
between (a) a work budget, (b) a pattern-length cap, or (c) accept + document. Do not implement.**

Nothing in §Proposed change or the hotfix list depends on the ruling; 98 can land in full while R1 is
still open.

**The finding.** `MAX_STAR_COUNT = 100` (`types/src/glob.rs:18`) is presented as the guard, and it
does not bound the cost of a two-star pattern. Measured, both matchers compiled verbatim from HEAD:

```
n=10000  k=1000 : canonical(`*a^k b`)=false (1.1µs, fast path) | canonical(`*a^k b*`)=false (7.6 ms)
n=100000 k=10000: canonical(`*a^k b`)=false (3.2µs, fast path) | canonical(`*a^k b*`)=false (752 ms)
```

A single `glob_match` call at **752 ms**, star count 2, well under the cap of 100. The trailing `*`
matters only because it defeats the `*suffix` fast path (`glob.rs:44-52`); the shape is otherwise
ordinary. Scaling is quadratic in (text × pattern), as the algorithm implies.

**Why the cap cannot bound this, structurally.** In the iterative core, `star_count` is incremented
only in the `b'*'` arm (`glob.rs:63-67`), which then advances `pi` past the star run and records
`star_pi = pi` (`:73`). Every backtrack restores `pi = star_pi` — i.e. to the position **after** the
star — so a star is counted **once, on first encounter, no matter how many times it is backtracked
through**. `MAX_STAR_COUNT` therefore bounds the number of `*` **characters** in the pattern, not the
number of backtracking steps. A two-star pattern can backtrack `O(n·m)` times and never approach the
cap. This is a reading of the control flow, not a measurement, and it explains the measurement above.

**Why the amplification is what makes it worth a decision, not the single call:**

- **PSUBSCRIBE** — the hot loop is `core/src/pubsub.rs:741-742`: for **every** PUBLISH,
  `for (pattern, compiled, _, sender) in &self.pattern_subs { if compiled.matches(channel) … }`
  walks the whole `pattern_subs: Vec<(Bytes, GlobPattern, ConnId, PubSubSender)>` (declared `:523`)
  and matches each entry. `pubsub.rs:487` is *not* the loop — it is the one-line body of
  `GlobPattern::matches`, the callee. Cite the loop, because the cost is `patterns × publishes`, and
  the delegator alone makes it look like one call. Two further call sites exist on the introspection
  path — `channels()` `:810` and `shard_channels()` `:834` — matching an operator-supplied pattern
  against every registered channel. A client registers a pattern once; every subsequent PUBLISH pays.
  The doc at `pubsub.rs:466-467` reads *"an iterative O(nm) algorithm with no catastrophic
  backtracking"* — **true and misleading**: O(n·m) with n and m both attacker-chosen and no cap on
  either is the whole finding, and "no catastrophic backtracking" invites the reader to stop there.
- **HSCAN/SSCAN/ZSCAN — live on `main` today.** This was written as conditional on `0b034a4f`
  landing. It has landed: `hash_cursor_scan` (`utils.rs:147`, pre-HF-B numbering) **now runs** the
  canonical matcher **per item**, and per §Problem 5 the item loop is not bounded by COUNT for
  non-matching items. Per-call cost multiplies by collection size, on `origin/main`, reachable by any
  authenticated client. **This is what moves R1 from "flagged" to "escalated".**
- **ACL** — `acl/src/permissions.rs:73`/`:101` match key and channel patterns on the command path.
  Operator-supplied, so lower severity, but it is a third amplifier.

**What is not covered.** `testing/fuzz/fuzz_targets/glob_match.rs` (13 lines) only checks for panics,
and structurally **cannot** reach these inputs: `let split = data[0] as usize % data.len()` bounds
the pattern by `data[0]`, i.e. **≤ 255 bytes**. No amount of fuzzing time finds a 20 KB pattern.

**The options, for the record — none of them proposed here:** (a) a work budget in `glob_match`
(step counter, refuse past a limit) — changes semantics for legitimate large patterns and needs a
compat decision; (b) a pattern-length cap at the command boundary; (c) accept and document. Option
(a) additionally interacts with the star cap's already-unverified relationship to Redis's behavior
(§Problem 2). **This needs a user ruling before any code moves. Until that ruling lands, the correct
action here is none.**

## Effort

| Step | Effort | Notes |
|---|---|---|
| ~~Merge `0b034a4f` (glob fold, impls #2 + #6)~~ | **DONE** | **LANDED** as HF-B in the `d48e1b44` batch: `utils.rs −44`, `registry.rs ±123`, `scan_regression.rs +181`. No longer 98's work. |
| H1 — delete unreachable `execute` bodies + `parse_key_type` | **S** | ~67 lines out, two one-line refusals in. Zero behavior change (unreachable code, four routes proven closed). |
| H3 — FT.CONFIG fold | **S** | One call swapped, 18 lines deleted, **two** regression pins (`TIMEOU?`, `MIN*FIX`) — and it is a behavior change, not a pure refactor. |
| H4 — document `frogctl watch --match` case-insensitivity | **S** | Help-text string. |
| Steps A+B — promote and widen `ScanRequest`, `ScanCaps` | **M** | Mechanical move plus three call-site updates, **plus eleven inline tests** — nine mechanical, two rewritten (`:1109`, `:1149`, §Files involved). The design content is the closure→record swap. |
| Step C — `scatter.rs` calls the shared parser | **M** | The only step with real risk: **six** error strings must byte-match, including the lower-cased `unknown type` echo. Needs the differential test *before* H1 deletes its subject. |

**Total: M**, and slightly *smaller* than at authoring, because the glob half is now history rather
than a review obligation. One PR of three commits (H1 → A+B → C), with H3 and H4 landable any time in
any order. The brief's **S for glob / M for scan grammar** split was correct; the S has since gone to
zero.

**Rebase obligation, first task for the implementer:** re-derive every `commands/src/utils.rs` line
number in this document against `origin/main`. They are all pre-HF-B and shift by roughly −28
(re-anchoring note). Do not trust a `utils.rs` line number printed here.

## Independently-landable hotfixes

**H1 — delete the unreachable SCAN/KEYS `execute` bodies and `parse_key_type`.**
`commands/src/scan.rs`: remove `:67-105`, `:134-147`, `:151-163`; replace the two bodies with the
one-line server-wide refusal used by the other 28 such commands. Zero behavior change — the code is
unreachable on **all four** routes (§Problem 1). Removes the decoy that would fabricate a single-shard
SCAN answer if a dispatch refactor ever let one through. Lands alone; moves proposal 67's SV6 from
**28 of 36 to 30 of 36** (the other six excluded commands are issue I3, not H1's business).

**H2 — ~~merge commit `0b034a4f`~~ — LANDED, no action.** It is on `origin/main` as part of the
`d48e1b44` batch (= HF-B). It folded `commands/src/utils.rs`'s `simple_glob_match` and
`scripting/src/registry.rs`'s recursive `matches_pattern` into `frogdb_types::glob_match` and added
181 lines of `scan_regression.rs` pins, fixing the H/S/ZSCAN `[class]`/`\escape` divergence recorded
in §Problem 2. Retained in this list only so a reader who arrives via the round plan does not go
looking for unlanded work. **Its one consequence for 98 is the `utils.rs` line shift** — see the
re-anchoring note.

**H3 — route FT.CONFIG GET through the canonical glob.** `core/src/shard/search/config.rs:42` calls
`frogdb_types::glob_match` over the ASCII-uppercased pattern; delete `glob_match_simple`
(`search/mod.rs:23-39`, 17 lines) and its import (`config.rs:5`). **Not** a pure refactor: identical
only for the pattern `*`, wildcard-free literals, and a single edge `*` over a wildcard-free
remainder (§H3 in detail). Everything else changes toward Redis — including two live divergences that
are the pins: `FT.CONFIG GET TIMEOU?` and `FT.CONFIG GET MIN*FIX`, both empty today, `TIMEOUT` and
`MINPREFIX` under the canonical matcher. No new dependency (`frogdb-core` already uses
`frogdb_types::glob_match` in four places).

**H4 — document `frogctl watch --match` case-insensitivity.** `frogctl/src/commands/watch.rs`'s
matcher (`:64-94`) is case-insensitive; server-side `MATCH` is not. One line of help text. Do **not**
fold the implementation — `frogctl`'s `[dependencies]` block has no `frogdb-*` crate (the
`frogdb-test-harness` entry at `Cargo.toml:47` is a dev-dependency) and folding would add
`frogdb-types` and transitively `usearch` and `murmur3` to the shipped CLI (§Problem 3).

**Not a hotfix, needs a ruling (R2):** SCAN/HSCAN `COUNT 0` (§Problem 4) — the fix is one line in the
shared parser, but the ruling has **two axes**: rejecting `count < 1` fixes the H/S/ZSCAN hang *and*
converts today's terminating-but-empty `SCAN 0 COUNT 0` reply into an error. It also depends on
confirming Redis's answer against a live server, which this session did not do.

## Honesty about verification

What was executed: `git` (HEAD, ancestry, `branch --contains`, `show --stat` for `0b034a4f`),
`grep`/`wc` over the working tree, and a standalone `rustc -O` binary containing verbatim copies of
`types/src/glob.rs:23-198` and `commands/src/utils.rs:60-88`, which produced every number in
§Problem 2 and §Flagged.

What was **not** executed: **no build, no test run, no server** (per the round's local-mode, no-server
constraint). Specifically unverified —

- The 181 new `scan_regression.rs` lines in `0b034a4f`/HF-B have not been run **here**. That
  obligation transferred to the lander with the commit; this document does not assert its outcome.
- **This worktree predates `d48e1b44`.** Every post-HF-B fact in this document — the shifted
  `utils.rs` line numbers, `registry.rs`'s and `simple_glob_match`'s deletion, `scan_regression.rs` at
  291 lines — is taken from the landing record, **not read off the tree**. Where a post-HF-B number
  could not be verified locally, the pre-HF-B number is printed with the shift annotated rather than
  a guessed post-HF-B one. The implementer re-derives from `main`.
- The **six** `scatter.rs` error strings were traced to `CommandError` variants and to their
  rendering rules **by reading** `types/src/error.rs:113-132` and `types/src/args.rs:90-319`, not by
  byte-comparing wire output. This is stronger than the earlier "checked term by term" — the
  rendering is now cited — but it is still reading. Step C must test it.
- **Step C(a)'s dead-guard proof is a reading proof.** `Arity::AtLeast(1)` (`scan.rs:51`) +
  `Arity::check` (`core/src/command.rs:891-897`) + `CommandLookup` at stage 6 (`dispatch.rs:549-558`)
  imply `SCAN` with no arguments never reaches `scatter.rs:46`. No server confirmed it, and the MULTI
  path rejects elsewhere (`queue_command`). Pin it in Step C regardless.
- Redis's actual answers for `COUNT 0` (§Problem 4) and for the star-cap patterns (§Problem 2) are
  stated from knowledge of the Redis source, not from a live comparison. Both are marked as such at
  the point of claim. FrogDB's own `COUNT 0` behavior, by contrast, **was** derived here from the
  cursor arithmetic (`scatter.rs:133`, `:364-370`, `scan.rs:29-31`, `utils.rs:138-144`) and the two
  paths differ — see §Problem 4.
- The timings in §Flagged are single-run, on this laptop, in a `-O` binary with no server around
  them. The **shape** (quadratic, cap-evading) is the claim; the absolute milliseconds are
  illustrative. The *structural* reason the cap cannot bind (`glob.rs:63-77`, star counted once per
  star character) is a control-flow reading, and it is what the timings corroborate.
