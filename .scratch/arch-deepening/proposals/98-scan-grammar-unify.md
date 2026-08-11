# Proposal 98 — One SCAN grammar, one glob: fold the third (dead) scan parser and rule the six glob implementations

Round 38. Candidate labels: PN13 (glob duplication), CT10-adjacent (scan grammar).

Verified at HEAD `dd840ca3bb3a70319c424d62885e753e51abfdf5`. All line numbers below are from that
commit unless stated otherwise. The differential numbers in §Problem 2 come from compiling both
matchers verbatim into a standalone binary (`rustc --edition 2021 -O`) and running them — they are
measurements, not readings.

## Corrections to the lane brief

The brief's framing is directionally right and factually wrong in five places. Each correction
changes what this proposal should contain, so they lead.

| Brief claim | Ruling |
|---|---|
| *"FIVE glob implementations"* | **Six.** The brief's five are `types/src/glob.rs:23`, `commands/src/utils.rs:60`, `core/src/shard/search/mod.rs:23`, `frogctl/src/commands/watch.rs:64`, `testing/src/pubsub_oracle.rs:242`. It missed `scripting/src/registry.rs`'s `matches_pattern`/`match_pattern_recursive` — the only **recursive** one, and the only one with genuinely exponential worst-case shape. |
| *"the compat-divergence fix is a normal hotfix"* | **The fix is already written and unmerged.** Commit `0b034a4f` (*"fix(commands,scripting): use canonical Redis glob in H/S/ZSCAN MATCH and FUNCTION LIST"*, 2026-08-10) folds impls #2 and #6 into the canonical one and adds 181 lines of new `scan_regression.rs` pins. `git merge-base --is-ancestor 0b034a4f HEAD` → **not an ancestor**; same for `origin/main`; `git branch --contains` → only `worktree-agent-a84216c599d8af135`. **98 must not re-propose it.** The action is *merge the existing commit*, and 98 covers the residue it does not touch. |
| *"`ScanRequest::parse` handles only SCAN's grammar"* | **Backwards.** `ScanRequest::parse` (`commands/src/utils.rs:194`) has exactly three callers — `hash.rs:739` (HSCAN), `set.rs:1113` (SSCAN), `sorted_set/scan.rs:40` (ZSCAN). **SCAN never calls it.** Its own doc comment (`utils.rs:172-173`) claims it is *"shared by the whole SCAN family (SCAN, HSCAN, SSCAN, ZSCAN)"* — that sentence is false at HEAD. |
| *"`parse_key_type` duplicated `scan.rs:150-162` vs `scatter.rs:98-110`"* | **Half right, and the interesting half is different.** `scatter.rs` has no such function — it has an **inline** six-arm `match` at `:100-113`. And `scan.rs`'s `parse_key_type` (`:151-163`) is **dead**, because its only caller (`ScanCommand::execute` `:67-105`) is unreachable. §Problem 1. |
| *"the real glob has `MAX_STAR_COUNT` (verify)"* | **The cap exists (`types/src/glob.rs:18`) and does not close the cost.** A two-star pattern never trips it: `*` + 10,000 `a` + `b` + `*` against 100,000 `a` measures **752 ms** for a single match call. §Flagged for sign-off. |

## Summary

SCAN's wire grammar — `cursor [MATCH p] [COUNT n] [TYPE t]` — is written **three** times in this
repo, and the copy that the SCAN *command* owns is **unreachable**. `ScanCommand` and `KeysCommand`
declare `ExecutionStrategy::ServerWide`, which the connection intercepts at `DispatchStage::ServerWide`
strictly before `DispatchStage::Execute`; their `execute()` bodies are never entered on any route.
They are not dormant — they are decoys that would fabricate a plausible single-shard answer if
something ever reached them.

The proposal has one architectural move and one set of rulings:

**Move.** Promote `ScanRequest` from `commands::utils` into `commands::scan` — the module that
already exports `cursor` to the server across the crate boundary — widen it with the two
command-specific fields (`key_type`, `novalues`), and replace the `extra: impl FnMut(&mut ArgParser)`
closure hook with a data capability record. `connection/scatter.rs::handle_scan` then calls the
shared parser instead of hand-rolling the grammar for the third time, and the two dead `execute`
bodies plus `parse_key_type` are deleted. One wire grammar, one owner, one place a new SCAN option
lands.

**Rulings.** Six glob implementations, one canonical (`frogdb_types::glob_match`). Two are folded by
the already-written `0b034a4f`. One (`glob_match_simple`, FT.CONFIG) should fold — H3. Two
(`frogctl`, the pub/sub oracle) must **stay**, for reasons that are dependency-structural and worth
writing down so the next round does not re-litigate them.

The deletion test is the argument's spine: after the move, `commands/src/scan.rs` loses ~67 lines of
unreachable code, `connection/scatter.rs` loses ~51 lines of grammar, and nothing that any client
can observe changes. Deleting a decoy is strictly better than maintaining it, and today the decoy is
*the* thing that makes `scan.rs` look like it owns SCAN.

## Files involved

Verified paths and line counts at `dd840ca3`.

| File | Lines | Role in this proposal |
|---|---|---|
| `frogdb-server/crates/commands/src/scan.rs` | 197 | **Primary.** `cursor` module `:24-39` (live, the server's sole import). `ScanCommand::execute` `:67-105` and `KeysCommand::execute` `:134-147` — **unreachable, deleted**. `parse_key_type` `:151-163` — dead with them. The two `CommandSpec` statics `:49-63` / `:116-130` **stay** (they are proposal 90's targets — §Risks). New home for `ScanRequest`. Zero `FM-` tags. |
| `frogdb-server/crates/commands/src/utils.rs` | 1241 | **Primary.** `simple_glob_match` `:60-88` (deleted by `0b034a4f`); `hash_cursor_scan` `:114-158`; `ScanRequest` `:175-179` + `::parse` `:194-225` — **moved out**; `empty_scan_reply` `:230-235`, `scan_reply` `:244-267` — move with it. Region touched: **`:53-267`**. Proposal 94 owns `:791-920` — disjoint, §Risks. Zero `FM-` tags. |
| `frogdb-server/crates/server/src/connection/scatter.rs` | 434 | **Primary.** `handle_scan` `:43-194`; the third grammar copy is `:50-118` (cursor parse `:50-58`, option loop `:68-118`, inline key-type match `:100-113`) — **~51 lines replaced by one call**. Cursor walk `:120-193` and `absorb_scan_reply` `:352` untouched. Covered by `lint-info-seam` — §Seam-lint clearance. Zero `FM-` tags. |
| `frogdb-server/crates/types/src/glob.rs` | 384 | **Read-only. The canonical implementation.** `glob_match` `:23`, `MAX_STAR_COUNT = 100` `:18`, four fast paths `:24-52`, `match_char_class` `:131-198`. Prod `:1-198`, tests `:199-384`. |
| `frogdb-server/crates/core/src/shard/search/mod.rs` | 100 | **H3 only.** `glob_match_simple` `:23-39` — **deleted** (17 lines). |
| `frogdb-server/crates/core/src/shard/search/config.rs` | 99 | **H3 only, one line.** The call at `:42` becomes a canonical-glob call. Proposal 70 lists this file **read-only** — §Risks. |
| `frogdb-server/crates/commands/src/hash.rs` | 2327 | **Caller.** HSCAN `:734-763`; the `NOVALUES` closure `:739-746` becomes a capability field. |
| `frogdb-server/crates/commands/src/set.rs` | 1127 | **Caller.** SSCAN `:1113` — `\|_\| Ok(false)` becomes `ScanCaps::NONE`. |
| `frogdb-server/crates/commands/src/sorted_set/scan.rs` | 55 | **Caller.** ZSCAN `:40` — same. |
| `frogdb-server/crates/scripting/src/registry.rs` | 413 | **Read-only.** `matches_pattern` `:198-236` — the sixth glob, folded by `0b034a4f`. Evidence for §Problem 2 only. |
| `frogdb-server/crates/server/src/connection/dispatch.rs` | — | **Read-only. The unreachability proof.** `PRE_DISPATCH_ORDER` `:124-141`; `dispatch_server_wide` `:232-…`, `ServerWideOp::Scan => self.handle_scan(args).await` at `:238`. |
| `frogdb-server/crates/server/src/connection/transaction.rs` | — | **Read-only.** EXEC's deferral into the same `dispatch_server_wide` at `:141`, `:240` — closes the second route. |
| `frogdb-server/crates/core/src/store/hashmap.rs` | — | **Read-only.** `scan_filtered` `:1037-1097`, canonical glob at `:1085` — the code that actually serves SCAN MATCH. |
| `frogdb-server/crates/core/src/shard/execution.rs` | — | **Read-only.** KEYS + `ScatterOp::Scan` shard side `:780-812`; canonical glob at `:785`. |
| `frogctl/src/commands/watch.rs` | 111 | **Read-only, KEEP.** Self-contained `glob_match` `:64-94`. Ruling in §Problem 3. |
| `frogdb-server/crates/testing/src/pubsub_oracle.rs` | 1138 | **Read-only, KEEP.** Self-contained `glob_match` `:239-266`; canonical-glob call at `:225`. Ruling in §Problem 3. |
| `frogdb-server/crates/redis-regression/tests/scan_regression.rs` | 110 | **Read-only at HEAD; `0b034a4f` grows it to 291.** Three iteration tests, no MATCH pins. |
| `frogdb-server/crates/redis-regression/tests/scan_tcl.rs` | — | **Read-only, must stay green.** Every MATCH pin: `:159`, `:190`, `:572`, `:582`, `:608`, `:635`, `:689`, `:820`. |
| `testing/fuzz/fuzz_targets/glob_match.rs` | 13 | **Read-only.** Evidence for §Flagged: panic-only, and structurally unable to reach the costly inputs. |
| `Justfile` | `:423-441` | **Read-only.** `lint-info-seam`, the one gate scoped to an edited file. |

Test surface that must stay green: `scan_tcl.rs` (8 MATCH pins), `scan_regression.rs`,
`types/src/glob.rs` inline tests (`:199-384`), `commands/src/utils.rs` inline tests (`:973`, `:982`,
`:1104`), `commands/src/scan.rs` inline cursor tests (`:169`, `:189` — these test the `cursor`
module, which survives).

## Problem

### 1. The SCAN grammar is written three times, and the copy SCAN owns is unreachable

`ScanCommand`'s spec declares `strategy: ExecutionStrategy::ServerWide(ServerWideOp::Scan)`
(`scan.rs:62`); `KeysCommand`'s declares `ServerWideOp::Keys` (`scan.rs:129`). The connection's
`PRE_DISPATCH_ORDER` (`dispatch.rs:124-141`) puts `DispatchStage::ServerWide` at index 13 and
`DispatchStage::Execute` at index 15. `dispatch_server_wide` routes `ServerWideOp::Scan` to
`self.handle_scan(args)` (`dispatch.rs:238`) and returns; `Execute` is never reached. EXEC takes the
same door — `transaction.rs:141`/`:240` defer queued server-wide commands back into
`dispatch_server_wide`, not into the executor.

So `ScanCommand::execute` (`scan.rs:67-105`) and `KeysCommand::execute` (`:134-147`) are dead on
every route. Three consequences, in ascending order of how much they should bother us:

1. **A third grammar is maintained for nothing.** `scan.rs:81-92` parses `MATCH`/`COUNT`/`TYPE` via
   `ArgParser`; `scatter.rs:68-118` parses the same three options via a hand-rolled index loop with
   its own error strings; `utils.rs:194-225` parses two of the three for the *other* three commands.
   Adding a SCAN option today means finding all three.

2. **The dead copy is a decoy, not a stub.** Proposal 67's SV6 catalogued 28 server-wide commands
   whose `execute()` bodies are a one-line "reached shard executor" refusal, and **explicitly
   excluded SCAN and KEYS** on the grounds that they *"carry real `execute()` bodies"*. That is the
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

### 2. Six glob implementations, measured against each other

Ranked by what they cost:

| # | Location | Supports | Ruling |
|---|---|---|---|
| 1 | `types/src/glob.rs:23` `glob_match` | `*` `?` `[abc]` `[a-z]` `[^a]` `[!a]` `\` + 4 fast paths + star cap | **Canonical.** |
| 2 | `commands/src/utils.rs:60` `simple_glob_match` | `*` `?` only | **Already folded by `0b034a4f`.** One call site (`utils.rs:148`). |
| 3 | `core/src/shard/search/mod.rs:23` `glob_match_simple` | leading/trailing `*` only, **case-insensitive** | **Fold — H3.** |
| 4 | `frogctl/src/commands/watch.rs:64` `glob_match` | `*` `?`, case-insensitive | **Keep.** §Problem 3. |
| 5 | `testing/src/pubsub_oracle.rs:242` `glob_match` | `*` `?` | **Keep.** §Problem 3. |
| 6 | `scripting/src/registry.rs:199` `matches_pattern` | `*` `?`, **recursive** | **Already folded by `0b034a4f`.** |

The #1-vs-#2 divergence table below is machine-produced by running both functions, copied verbatim
from HEAD, over the same inputs. Column 3 is what HSCAN/SSCAN/ZSCAN answer at HEAD; column 4 is what
SCAN and KEYS answer.

| pattern | text | H/S/ZSCAN (`simple`) | SCAN/KEYS (canonical) | |
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

Note the divergence runs **both ways**: `simple` treats `[ab]` and `\*` as literals, so it wins the
`[ab]xyz` and `a\*` rows that the canonical matcher (correctly, per Redis) loses. That is why this is
a behavior *change* toward Redis and not a pure bug fix — §Risks.

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

**`frogctl/src/commands/watch.rs:64`.** `frogctl/Cargo.toml` declares **no dependency on any
`frogdb-*` crate**. `frogctl` speaks RESP to a server over a socket; it shares no code with it.
Folding this would mean adding `frogdb-types` to the CLI's dependency graph — which transitively
drags in `usearch`, tokio, and murmur3 — to save 30 lines in a `--match` filter that runs
client-side over already-fetched output. The correct fix is the **documentation** one: this matcher
is **case-insensitive** and the canonical one is not, so `frogctl watch --match` silently behaves
unlike server-side `MATCH`. That is a one-line help-text change — **H4**.

**`testing/src/pubsub_oracle.rs:242`.** Its own doc comment states the reason: *"Self-contained so
the oracle has no dependency on the server crate."* A differential oracle that imports the
implementation under test proves nothing — the divergence it exists to detect becomes definitionally
unreachable. `testing/Cargo.toml` has six small dependencies and no `frogdb-core`. Note the file
already calls the **canonical** glob at `:225` for `SubKind::Pattern`; `:242` is the independent
second opinion, and the two coexisting is the design, not an oversight. Proposal 82 lists this file
**read-only except a doc-comment hotfix** — no conflict.

### 4. Secondary, unpinned: `COUNT 0` is a client-visible infinite loop on both paths

Not the centerpiece; recorded because the unification is where it gets fixed for free.

Neither grammar rejects `COUNT 0`. On the SCAN path, `scatter.rs:133`'s loop condition
`while all_keys.len() < count` is false immediately, `next_shard`/`next_position` are unchanged, and
`encode_final_cursor` returns **the cursor the client sent**, with an empty key array — a client
looping until cursor 0 never terminates. On the HSCAN/SSCAN/ZSCAN path, `hash_cursor_scan`
(`utils.rs:141-145`) checks `emitted >= count` before emitting, sets `new_cursor = hash` of the first
item at or after the cursor, and breaks; the next call's `partition_point` lands on that same item
and returns the same cursor — same non-termination.

`grep` for `COUNT", "0"` across `frogdb-server/crates` finds pins for HOTKEYS, ZMPOP, BZMPOP, LMPOP,
LPOS — **none for the SCAN family**. Redis's `scanGenericCommand` rejects `count < 1` with a syntax
error; that claim is from knowledge of the Redis source, **not verified against a running server
here**, so H1 should confirm it before pinning. One shared parser means one place to add the check.

### 5. Adjacent finding, flagged not proposed: `hash_cursor_scan` ignores COUNT for non-matching items

`utils.rs:141-155` applies the MATCH filter **after** the `emitted >= count` break. An item that
fails the pattern `continue`s without incrementing `emitted`, so a pattern matching nothing walks the
**entire** collection in a single call — COUNT provides no bound on work, only on output. Redis
bounds work, not output, for exactly this reason. Separately, `utils.rs:123-128` collects and sorts
the whole collection on **every** call, so a full HSCAN iteration of an *n*-element hash is
O(n² log n) in aggregate regardless of COUNT.

This is a real cost bug and it is **out of scope for 98** — fixing it changes cursor semantics and
belongs in its own proposal with its own compat pins. Recorded here because 98 reads the function
and a reader will otherwise ask.

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

**Step C — call it from `scatter.rs`.** `handle_scan:50-118` collapses to roughly:

```rust
let request = match ScanRequest::parse(args, ScanCaps::KEYSPACE) {
    Ok(r) => r,
    Err(e) => return e.into(),
};
let (shard_id, position) = cursor::decode(request.cursor);
```

One error-mapping subtlety, and it is the step's only real risk: `scatter.rs` today emits raw
strings (`"ERR invalid cursor"` `:53`/`:57`, `"ERR syntax error"` `:75`/`:97`/`:115`,
`"ERR value is not an integer or out of range"` `:90`, `"ERR unknown type: {}"` `:108-111`) while
`ScanRequest::parse` yields `CommandError` variants. The mapping was checked term by term and the
strings line up (`InvalidArgument{"invalid cursor"}`, `SyntaxError`, `NotInteger`,
`InvalidArgument{"unknown type: …"}`) — but "line up by inspection" is not "line up on the wire", so
step C's acceptance is a test that drives all five error paths through a real connection and
byte-compares. Option-token case-insensitivity already agrees: `scatter.rs:70` uppercases the token,
and `ArgParser::try_flag` (`types/src/args.rs:173-181`) uses `eq_ignore_ascii_case`.

**Step D — delete.** `ScanCommand::execute`, `KeysCommand::execute`, and `parse_key_type` go. The two
`CommandSpec` statics stay. `Command` requires an `execute`, so the two commands take the same
one-line refusal the other 28 server-wide commands already carry (proposal 67's SV6 catalogue) —
which is what makes them *stop* being decoys, and what lets 67's SV6 finally cover the whole set
instead of 28 of 30.

### Deletion test, per module

| Module | Lines out | Lines in | Net |
|---|---|---|---|
| `commands/src/scan.rs` | 39 (`execute` `:67-105`) + 14 (`execute` `:134-147`) + 14 (`parse_key_type` `:150-163`) = **67** | ~4 (two refusal bodies) + ~35 (`ScanRequest` + `ScanCaps`, relocated) | +... (relocation, not growth) |
| `commands/src/utils.rs` | ~95 (`ScanRequest` `:172-226`, `empty_scan_reply` `:228-235`, `scan_reply` `:237-267`) + 32 (`simple_glob_match` `:57-88`, via `0b034a4f`) | 0 | **−127** |
| `server/src/connection/scatter.rs` | 51 (`:50-118` grammar) | ~6 | **−45** |
| `core/src/shard/search/mod.rs` (H3) | 18 (`:22-39`) | 0 | **−18** |

Counting the relocation as neutral: roughly **−130 production lines**, three wire grammars become
one, and six glob implementations become three (canonical + two justified independents).

The sharper test is the one that does not count lines. Ask of each shallow module: *if I delete it,
what stops working?* — `parse_key_type`: nothing, it is dead. `ScanCommand::execute`: nothing, it is
unreachable. `scatter.rs:68-118`: SCAN option parsing, which is the shared parser's job.
`glob_match_simple`: FT.CONFIG GET's pattern filter, which the canonical glob does strictly better.
Four modules, four answers, and only one of them ("FT.CONFIG needs *a* matcher") is a reason to keep
*code* rather than a reason to keep *this* code.

### H3 in detail: FT.CONFIG's matcher

`glob_match_simple` (`search/mod.rs:23-39`) is called from exactly one place —
`search/config.rs:42` — against a fixed four-entry table of uppercase keys (`MINPREFIX`,
`MAXEXPANSIONS`, `TIMEOUT`, `DEFAULT_DIALECT`, `config.rs:33-38`). It handles only leading and/or
trailing `*`, and it uppercases both sides.

Because the operand set is four known-uppercase ASCII strings, routing through the canonical glob
over an ASCII-uppercased pattern is **behavior-identical for every pattern containing no `[`, `]`,
or `\`**, and strictly more Redis-correct otherwise. It also fixes a small live divergence:
`FT.CONFIG GET TIMEOU?` returns empty today (`?` is a literal to `glob_match_simple`) where Redis
returns `TIMEOUT`. `frogdb-core` already depends on `frogdb-types` and already calls `glob_match` in
four places (`pubsub.rs:487`, `shard/execution.rs:785`, `store/hashmap.rs:1085`, and via
`commands/src/scan.rs:141`'s `frogdb_core::glob_match` re-export), so the fold costs no dependency.

## Testability improvement

**Today the SCAN grammar cannot be unit-tested at all.** The only reachable copy lives inside
`handle_scan`, an `async fn` on the connection type that needs `self.core.shard_senders`,
`self.state.id`, and `self.num_shards` — i.e. a running server. So every assertion about SCAN's
argument handling is an integration test over a socket (`scan_tcl.rs`), and the parser's error paths
are correspondingly under-covered. The `utils.rs` inline tests (`:973` `parses_cursor_only`, `:982`
`parses_match_and_count`) test the copy that SCAN does not use.

After the move, `ScanRequest::parse(args, ScanCaps::KEYSPACE)` is a pure function over `&[Bytes]`
returning `Result<ScanRequest, CommandError>`. Three things become cheap that are expensive today:

1. **Table-driven grammar tests in `frogdb-commands`.** Every option order, every duplicate, every
   truncated flag, every bad key type — as `#[test]`s in the crate that owns the grammar. This is the
   mutation-score point too: `cargo mutants -p <crate>` runs only that package's own tests, so
   grammar tests written as `frogdb-server` integration tests contribute nothing to
   `frogdb-commands`' score.

2. **Capability-matrix tests.** With `ScanCaps` as data, one test asserts the property the closure
   design cannot express: *for every `(caps, option)` pair, the option is accepted iff the capability
   is set.* Concretely — that `HSCAN k 0 TYPE string` is a syntax error and `SCAN 0 NOVALUES` is too.
   Neither is pinned anywhere today.

3. **A differential test against the dead copy — before it is deleted.** Step D is the delete;
   step C can first assert that `ScanRequest::parse` and `scan.rs:81-92`'s loop agree on a corpus of
   argument vectors. That converts "I read both and they look the same" into a green test, and then
   the test is deleted with its subject. This is the only chance to get that assurance, and it exists
   only because the dead copy is still there.

Regression additions this proposal owns: SCAN/HSCAN `COUNT 0` (§Problem 4, after confirming Redis's
answer), `FT.CONFIG GET TIMEOU?` (H3), and the five `scatter.rs` error strings (step C).

## Risks / scope boundaries

### The compat risk, stated plainly

`0b034a4f` changes what HSCAN/SSCAN/ZSCAN `MATCH` means for patterns containing `[`, `]`, or `\`.
Per §Problem 2's table this is bidirectional: `HSCAN h 0 MATCH "[ab]*"` starts matching fields
beginning with `a` or `b` (Redis-correct) and stops matching the literal field `[ab]xyz`
(Redis-correct, and a behavior loss for anyone relying on it). Two facts bound the risk:

- **Nothing in the repo pins the old behavior.** `grep -rn 'MATCH", "\['` and
  `grep -rn 'MATCH", ".*\\\\'` across `frogdb-server/crates` return **zero** hits. Every existing
  MATCH pin (`scan_tcl.rs:159`, `:190`, `:572`, `:582`, `:608`, `:635`, `:689`, `:820`) uses only
  `*` and `?`, on which the two matchers agree.
- **`simple_glob_match` has exactly one call site** (`utils.rs:148`, inside `hash_cursor_scan`), so
  the blast radius is precisely the three per-key SCAN commands.

Required before merging `0b034a4f`: a green `just test frogdb-redis-regression scan`. The 181 new
lines it adds to `scan_regression.rs` are its own pins; they have not been executed in this session
(no build was run — §Honesty).

### Sibling boundaries

| Sibling | Contact | Ruling |
|---|---|---|
| **90** (commandspec-default) | Same file `commands/src/scan.rs`; 90 rewrites the `CommandSpec` statics `:49-63` and `:116-130`, 98 deletes the `execute` bodies below them (`:67-105`, `:134-147`). Also same file `utils.rs`, no static there. | **Disjoint regions, but 98 must land first.** 90's own §"land SOLO, and land LAST" (90:507) and its commit-3 sweep are a scripted `awk` pass over 56 files (90:578); running it against a `scan.rs` that still contains the dead bodies is harmless, but 98 rebasing onto a swept `scan.rs` means re-deriving line numbers. Brief's ordering (98 before 90 commit 3) is correct and should be held. |
| **94** (resp3-shape-once) | Same file `commands/src/utils.rs`. | **Disjoint.** 94 owns `:791-920` (`score_response` … `members_array`, its H1 is `:895-912`); 98 owns `:53-267`. Verified against 94's files table (94:73). No overlap, either order. |
| **70** (acl-registry-consult) | Same file `core/src/shard/search/config.rs` — H3 edits the call at `:42`, inside the `GET` arm. | **No conflict at HEAD.** 70 lists this file **read-only** (70:116) and reads the dispatch shape `:20-78` to derive an ACL declaration; its edits land in the acl crate and `command_spec.rs`. H3's one-line change inside `GET`'s body does not alter the `match` structure 70 reads. If 70 ever converts to an edit here, H3 rebases trivially. |
| **71** (search-query-plan) | Same directory `core/src/shard/search/`. | **Different file.** 71 edits `query.rs`; H3 touches `mod.rs` + `config.rs`. 71's own boundary table (71:737) already draws this line. |
| **82** (pubsub-channel-table) | Same file `testing/src/pubsub_oracle.rs`. | **No conflict.** 82 is read-only there except a doc-comment hotfix (82:84, its H2). 98 is read-only there, full stop — its ruling is *keep*. |
| **67** (server-small-dedups, SV6) | Directly enabling. | 67's SV6 excluded SCAN/KEYS because they *"carry real `execute()` bodies"*. 98 step D removes that exclusion. **98 should land first**; 67 SV6 then covers 30 of 30. Not a conflict — 67 has not landed (all 28 copies still present at HEAD). |
| **49** (function-registry-surface) | 49:578 names `0b034a4f`. | **98 is the second citation, not a rediscovery.** 49 named the commit for the `scripting/registry.rs` half; 98 names it for the `commands/utils.rs` half. Neither should re-author it. |

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

- Does not touch `hash_cursor_scan`'s cost behavior (§Problem 5) — separate proposal, separate pins.
- Does not touch the cursor walk in `scatter.rs:120-193` or `absorb_scan_reply` `:352`.
- Does not fold impls #4 and #5 — §Problem 3 argues they should never be folded.
- Does not re-author `0b034a4f`.
- Does not add a work budget to `glob_match` — see below.

## Flagged for orchestrator/user sign-off (security-adjacent, NOT proposed)

Per standing round policy, the denial-of-service dimension is **flagged, not proposed**. Nothing in
§Proposed change or the hotfix list depends on it.

**The finding.** `MAX_STAR_COUNT = 100` (`types/src/glob.rs:18`) is presented as the guard, and it
does not bound the cost of a two-star pattern. Measured, both matchers compiled verbatim from HEAD:

```
n=10000  k=1000 : canonical(`*a^k b`)=false (1.1µs, fast path) | canonical(`*a^k b*`)=false (7.6 ms)
n=100000 k=10000: canonical(`*a^k b`)=false (3.2µs, fast path) | canonical(`*a^k b*`)=false (752 ms)
```

A single `glob_match` call at **752 ms**, star count 2, well under the cap of 100. The trailing `*`
matters only because it defeats the `*suffix` fast path (`glob.rs:55-62`); the shape is otherwise
ordinary. Scaling is quadratic in (text × pattern), as the algorithm implies.

**Why the amplification is what makes it worth a decision, not the single call:**

- **PSUBSCRIBE** — `core/src/pubsub.rs:487` runs the canonical matcher against **every** published
  channel for **every** registered pattern. A client registers the pattern once; every subsequent
  PUBLISH pays. The doc at `pubsub.rs:466-467` reads *"an iterative O(nm) algorithm with no
  catastrophic backtracking"* — **true and misleading**: O(n·m) with n and m both attacker-chosen and
  no cap on either is the whole finding, and "no catastrophic backtracking" invites the reader to
  stop there.
- **HSCAN/SSCAN/ZSCAN** — once `0b034a4f` lands, `hash_cursor_scan` (`utils.rs:147`) runs the
  canonical matcher **per item**, and per §Problem 5 the item loop is not bounded by COUNT for
  non-matching items. Per-call cost multiplies by collection size.
- **ACL** — `acl/src/permissions.rs:73`/`:101` match key and channel patterns on the command path.
  Operator-supplied, so lower severity, but it is a third amplifier.

**What is not covered.** `testing/fuzz/fuzz_targets/glob_match.rs` (13 lines) only checks for panics,
and structurally **cannot** reach these inputs: `let split = data[0] as usize % data.len()` bounds
the pattern by `data[0]`, i.e. **≤ 255 bytes**. No amount of fuzzing time finds a 20 KB pattern.

**The options, for the record — none of them proposed here:** (a) a work budget in `glob_match`
(step counter, refuse past a limit) — changes semantics for legitimate large patterns and needs a
compat decision; (b) a pattern-length cap at the command boundary; (c) accept and document. Option
(a) additionally interacts with the star cap's already-unverified relationship to Redis's behavior
(§Problem 2). **This needs a ruling before any code moves.**

## Effort

| Step | Effort | Notes |
|---|---|---|
| Merge `0b034a4f` (glob fold, impls #2 + #6) | **S** | Already written: `utils.rs −44`, `registry.rs ±123`, `scan_regression.rs +181`. Work is review + a green regression run, not authoring. |
| H1 — delete unreachable `execute` bodies + `parse_key_type` | **S** | ~67 lines out, two one-line refusals in. Zero behavior change (unreachable code). |
| H3 — FT.CONFIG fold | **S** | One call swapped, 18 lines deleted, one regression pin. |
| H4 — document `frogctl watch --match` case-insensitivity | **S** | Help-text string. |
| Steps A+B — promote and widen `ScanRequest`, `ScanCaps` | **M** | Mechanical move plus three call-site updates; the design content is the closure→record swap. |
| Step C — `scatter.rs` calls the shared parser | **M** | The only step with real risk: five error strings must byte-match. Needs the differential test *before* H1 deletes its subject. |

**Total: M.** One PR of four commits (merge `0b034a4f` → H1 → A+B → C), with H3 and H4 landable any
time in any order. The **S for glob / M for scan grammar** split in the brief is correct; the S is
smaller than the brief assumed, because the glob work is already written.

## Independently-landable hotfixes

**H1 — delete the unreachable SCAN/KEYS `execute` bodies and `parse_key_type`.**
`commands/src/scan.rs`: remove `:67-105`, `:134-147`, `:151-163`; replace the two bodies with the
one-line server-wide refusal used by the other 28 such commands. Zero behavior change — the code is
unreachable on every route (§Problem 1). Removes the decoy that would fabricate a single-shard SCAN
answer if a dispatch refactor ever let one through. Lands alone; unblocks proposal 67's SV6 covering
30 of 30 instead of 28 of 30.

**H2 — merge commit `0b034a4f` (the glob compat fix). Already written; needs review, not authoring.**
On branch `worktree-agent-a84216c599d8af135`, not in `HEAD` or `origin/main`. Folds
`commands/src/utils.rs`'s `simple_glob_match` and `scripting/src/registry.rs`'s recursive
`matches_pattern` into `frogdb_types::glob_match`, and adds 181 lines of `scan_regression.rs` pins.
Fixes the H/S/ZSCAN `[class]`/`\escape` divergence in §Problem 2. **Behavior change toward Redis —
requires a green `just test frogdb-redis-regression scan`.** No existing test pins the old behavior
(verified by grep, §Risks).

**H3 — route FT.CONFIG GET through the canonical glob.** `core/src/shard/search/config.rs:42` calls
`frogdb_types::glob_match` over the ASCII-uppercased pattern; delete `glob_match_simple`
(`search/mod.rs:23-39`, 17 lines). Identical for every `[`/`\`-free pattern against the four-entry
uppercase key table; fixes `FT.CONFIG GET TIMEOU?` returning empty where Redis returns `TIMEOUT`.
Add that as the pin. No new dependency (`frogdb-core` already uses `frogdb_types::glob_match` in four
places).

**H4 — document `frogctl watch --match` case-insensitivity.** `frogctl/src/commands/watch.rs`'s
matcher (`:64-94`) is case-insensitive; server-side `MATCH` is not. One line of help text. Do **not**
fold the implementation — `frogctl` has no `frogdb-*` dependency and folding would add `frogdb-types`
(and transitively `usearch`, tokio, murmur3) to the CLI (§Problem 3).

**Not a hotfix, needs a ruling:** SCAN/HSCAN `COUNT 0` non-termination (§Problem 4) — the fix is one
line in the shared parser, but it depends on confirming Redis's answer against a live server, which
this session did not do.

## Honesty about verification

What was executed: `git` (HEAD, ancestry, `branch --contains`, `show --stat` for `0b034a4f`),
`grep`/`wc` over the working tree, and a standalone `rustc -O` binary containing verbatim copies of
`types/src/glob.rs:23-198` and `commands/src/utils.rs:60-88`, which produced every number in
§Problem 2 and §Flagged.

What was **not** executed: **no build, no test run, no server** (per the round's local-mode, no-server
constraint). Specifically unverified —

- The 181 new `scan_regression.rs` lines in `0b034a4f` have not been run. H2's acceptance is that run.
- The five `scatter.rs` error strings were matched to `CommandError` variants **by reading**
  `types/src/args.rs:173-254` and the error-rendering path, not by byte-comparing wire output. Step C
  must test this.
- Redis's actual answers for `COUNT 0` (§Problem 4) and for the star-cap patterns (§Problem 2) are
  stated from knowledge of the Redis source, not from a live comparison. Both are marked as such at
  the point of claim.
- The timings in §Flagged are single-run, on this laptop, in a `-O` binary with no server around
  them. The **shape** (quadratic, cap-evading) is the claim; the absolute milliseconds are
  illustrative.
