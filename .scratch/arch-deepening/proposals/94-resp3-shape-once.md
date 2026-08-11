# Proposal 94 — RESP2/RESP3 reply shape is decided twice: once in 27 handler branches, once in a downgrader that already knows the answer

**Revision 2** (adversarial review applied). The design changed in one place: there is a **third
renderer** — `wire_response_to_lua` (`core/src/scripting/bindings.rs:93-176`) — that takes no
`ProtocolVersion` and renders a `WireResponse` into a Lua value for `redis.call`. Under rev 1's fold
it silently changes what every RESP2 client's *scripts* see. It is now a co-primary design surface
(§Problem 6, §Proposed change F), the acceptance criterion is **two** goldens, and nothing pins it
today. Six smaller amendments are folded in; the diagnosis, the 27 shape decisions, the 12
byte-identical folds and the H1/H2/H3 hotfixes are unchanged.

Round 38 · lane: commands + types · effort **M (top of band)** · **no locked crate edited**, **zero `FM-` tags in
any primary edited file** (verified: `grep -rn "FM-" frogdb-server/crates/commands/src` → nothing;
`grep -c "FM-" frogdb-server/crates/protocol/src/response.rs` → `0`) · **one seam gate is the
precedent for this change and one gate has a hole this change closes** (§Spec / gates)

**Verified at HEAD `8a17065247c8935e6476fbffc92a5e94a1b20854`** (worktree `arch-round-38-99`).
`git diff --name-only 54baa2bb..HEAD | grep -v '^\.scratch/'` is **empty**, so every source line
number in proposals 80, 84, 86 and this document refers to the same unchanged tree. Every count and
every file:line below was re-derived at this SHA; nothing is inherited from the lane brief.

## Corrections to the lane brief

| Brief claim | Verified at HEAD |
|---|---|
| "~43 `is_resp3()` branches inside `frogdb-commands` handlers" | **Adjusted — two different numbers were conflated.** `grep -rn "is_resp3()" frogdb-server/crates/commands/src \| wc -l` → **24** (actual protocol-version *queries*). `grep -rn "is_resp3"` (the bare identifier, which also matches every *use* of the `let is_resp3 = …` locals) → **43**. The brief counted identifier mentions. The number that matters for the fold is neither: it is **27 shape-decision sites** (some queries feed two decisions, e.g. `pop.rs:45` → `:73` and `:76`). Full enumeration in §Problem 1. |
| "`to_resp2_frame` … ALREADY downgrades Map/Set/Double etc., [so the branches are redundant]" | **Half right, and the half that is wrong is the whole design problem.** Of the 27 decisions, **12 are byte-identical redundancy** the downgrader already performs; **8 are a nested-vs-flat choice the downgrader provably cannot infer** (both shapes are legal `Array`s); **7 carry a score-finiteness *policy* that no downgrader sees** because it is applied before the `Response` is even built. §Problem 2 / §Problem 3. |
| "Add `Response::ScoredPairs` (or similar) for the ZRANGE nested-vs-flat case" | **Right instinct, undercounted by two.** One carrier closes the ZRANGE case; the tree needs **three** (`Score`, `ScoredMembers`, `Pairs`) because `HRANDFIELD … WITHVALUES` (`hash.rs:853`) is the same nested-vs-flat shape over `(Bytes, Bytes)` rather than `(Bytes, f64)`, and the score-finiteness policy is orthogonal to pairing. §Proposed change. |
| "`MapReply` has only 2 callers" | **Confirmed** — `hotkeys.rs:445`, `auth_conn_command.rs:453`. Verdict: **fold, do not keep**. Its rule (`reply.rs:19` — "flatten/map lives in `MapReply::finish` and nowhere else") is *already* `to_resp2_frame`'s `Map` arm (`response.rs:297-306`), stated a second time in a second module. §Problem 4. |
| "Dead code: `format_scored_response`/`pop_response` (`utils.rs:799-928`)" | **Confirmed for the two named functions, wrong for the range.** `grep -rnw` across `frogdb-server`, `frogctl`, `testing`: `format_scored_response` and `pop_response` have **zero call sites outside their own definitions** (`utils.rs:902`, `:910`). The cited range `:799-928` is mostly *live* — it also contains `score_response` (7 callers), `scored_array` (9 live: 8 handler sites + `members_array`), `scored_array_resp3` (**8**), `scored_array_with_scores_resp3` (2), `members_array` (2). The dead region is exactly **`:898-912`** (15 lines, two fns with their doc comments) — `:895-896` are `scored_array_resp3`'s own closing braces and deleting them does not compile. **Hotfix H1.** |
| "XINFO? CLIENT INFO?" as downgrader gaps | **XINFO: a different defect, not a gap.** `stream/info.rs` has **zero** `is_resp3` and returns flat `Response::Array` in *both* protocols (`:207`, `:238`, `:281`, `:320`) — it never grew the RESP3 Map shape at all. Not a branch to fold; a one-line beneficiary of the fold. **CLIENT INFO: refuted.** `client_info` (`client_conn_command.rs:341-349`) returns a single `Response::bulk` in both protocols, correctly — no shape branch exists. |
| Rated "latent" | **One live defect found, in the sibling copy the brief did not look at.** `core/src/shard/blocking.rs:1198-1204`'s `zset_score_reply` is a second, *divergent* implementation of `commands/src/utils.rs:799`'s `score_response`. A `BZPOPMIN` served **immediately** and the same `BZPOPMIN` served **by the waiter** put different bytes — and for `+inf`, a different RESP3 *type* — on the wire for the same score. §Problem 5. **Hotfix H2.** |

Four findings the brief did not name, all verified at HEAD: the **live immediate-vs-blocked score
divergence** (§Problem 5), the **third renderer** — `wire_response_to_lua` — which consumes the same
`WireResponse` with no protocol version and would silently change `redis.call`'s Lua shape for every
RESP2 client under the naive fold (§Problem 6), **three in-tree shape asymmetries between sibling
commands** that exist only because each handler decides independently (§Problem 3), and the fact that
the tree **already has this seam, for one command family, with a gate enforcing it** —
`lint-pubsub-confirmation-seam` (`Justfile:1128-1156`) — which makes this proposal a generalization
of an accepted local ruling rather than a new idea (§Proposed change, precedent).

## Summary

A RESP2 reply and a RESP3 reply are two renderings of one value. FrogDB owns a **downgrader** that
says so: `WireResponse::to_resp2_frame` (`protocol/src/response.rs:274-334`) maps `Map` → flat
array, `Set` → array, `Push` → array, `Double` → `format_float` bulk, `Attribute` → inner, `BigNumber`
→ bulk. **Eighteen** reply sites in `frogdb-server` already trust it and emit the RESP3 shape
unconditionally (`debug_conn_command.rs` ×17, `client_conn_command.rs:808`), and a **nineteenth**
precedent is stronger than all of them: `invalidation_to_response` (`frame_io.rs:186-197`) builds a
`WireResponse::Push` for client-side-caching invalidations **unconditionally, with no
`ProtocolVersion` in scope at all** — RESP2 trackers get the downgraded `Array` and nobody writes a
branch for it.

The `frogdb-commands` handlers do not. Twenty-seven sites across nine files ask
`ctx.protocol_version.is_resp3()` and hand-build both shapes. Twelve of those produce, in the RESP2
arm, **byte-for-byte what the downgrader would have produced from the RESP3 arm** — pure
duplication. The other fifteen exist because two facts about a reply are **not representable in
`Response` at all**, so the handler is the only place they can be spent:

- **Pairing** — `[m, s, m, s]` and `[[m, s], [m, s]]` are both `Response::Array`. Once the handler
  has built one, the shape choice is *gone*; the downgrader sees an array of arrays and has no
  basis to flatten it (`ZMPOP` is nested in **both** protocols — `pop.rs:273`, comment `:263-264`).
- **Score finiteness** — `score_response` (`utils.rs:799-805`) emits `Double` only for finite
  scores and a bulk string otherwise, a deliberate divergence from `Double`'s normal RESP3
  rendering, pinned at `resp3.rs:858` / `:875`. By the time a `Response::Bulk("inf")` exists, the
  policy that produced it is unrecoverable.

Because those two facts have no home in the type, **every handler re-decides them**, and they have
already drifted — three verified asymmetries between commands that should agree (§Problem 3) and one
live cross-path divergence (§Problem 5).

The change: give `Response` the two missing facts as data (`ScoredMembers { pairing }`,
`Score(f64)`, plus `Pairs` for the `(Bytes, Bytes)` case), render both protocols from them **in the
two encoders that already sit side by side in one file**, and delete all 27 branches, four of the
five `scored_array*` helpers, `score_response`, `zset_score_reply`, and `MapReply`. One module owns
reply shape; the handlers state *what*, never *how it looks on which wire*.

**The catch, and why this is rev 2.** There is a **third renderer**, and it is not a wire encoder:
`wire_response_to_lua` (`bindings.rs:93-176`) turns a `WireResponse` into the Lua value a
`redis.call` returns. It takes **no `ProtocolVersion`**, and it renders `Map` → a Lua *hash* table
(`:139-148`) where `Array` → a Lua *sequence* (`:110-118`). Today a RESP2 connection running
`redis.call('HGETALL', k)` gets a flat sequence because the *handler* built an `Array`. After the
naive fold the handler always builds a `Map`, and the same script on the same RESP2 connection gets
`t[1] == nil`. That is a **breaking change for every RESP2 client that runs a script**, and
**nothing in the tree pins it**. The fold is therefore two seams, not one (§Problem 6, §F).

## Files involved

Line counts at `8a170652`.

| Path | Lines | Role in this change |
|---|---:|---|
| `frogdb-server/crates/protocol/src/response.rs` | 1770 (941 code + 829 tests, 45 `#[test]`) | **Primary (the interface).** `Response` `:647-743` and `WireResponse` `:116-175` each gain 3 carrier variants; `into_wire` `:770-837`, `from_wire` `:843-881`, `to_resp2_frame` `:274-334` and `to_resp3_frame` `:341-432` each gain 3 arms (~60 lines added). **Concurrently owned by 80 (primary), 84 (deletes `:473-550`), 86 (doc-only `:195-199`, `:267-273`, `:323-332`)** — §Risks. |
| `frogdb-server/crates/protocol/src/reply.rs` | 168 | **Deleted.** `MapReply` `:23-92` + its 4 tests `:94-166`. Its rule is `to_resp2_frame`'s `Map` arm (`response.rs:297-306`), already. |
| `frogdb-server/crates/protocol/src/lib.rs` | 27 | `mod reply;` + `pub use reply::MapReply;` `:19` removed. |
| `frogdb-server/crates/commands/src/utils.rs` | 1241 | **Primary.** `score_response` `:790-805`, `scored_array` `:807-841`, `scored_array_with_scores_resp3` `:843-871`, `scored_array_resp3` `:873-896`, `format_scored_response` `:898-904`, `pop_response` `:906-912`, `members_array` `:914-920` — **~130 lines deleted**, replaced by nothing (call sites construct the carrier directly). `:898-912` is **H1**, independently landable. Zero `FM-` tags. |
| `frogdb-server/crates/commands/src/set.rs` | 1127 | **7 decisions deleted** (`:164`, `:171`, `:355`, `:390`, `:418`, `:453`, `:480`) — all `Set`-vs-`Array`, all byte-identical folds. Largest single-file win. |
| `frogdb-server/crates/commands/src/hash.rs` | 2327 | **3 decisions**: `:385`, `:403` (`Map`-vs-flat, byte-identical fold); `:853` (`HRANDFIELD … WITHVALUES`, the `Pairs` carrier). `:853-872` also carries a **latent panic** the fold deletes: the RESP3 arm does `v.unwrap()` on an `Option<Bytes>` (`:858`) while the RESP2 arm at `:867-869` treats the same value as genuinely optional (`if let Some(v)`). One `Pairs` construction has one behavior. |
| `frogdb-server/crates/commands/src/string.rs` | 1805 | **2 decisions** (`:769`, `:781`, `INCRBYFLOAT`) + the now-dead `let is_resp3` `:764`. Byte-identical fold. |
| `frogdb-server/crates/commands/src/sorted_set/basic.rs` | 416 | **3 decisions**: `:272` `ZSCORE`, `:315` `ZMSCORE` (both → `Score`), `:410` `ZINCRBY` (byte-identical `Double` fold). Also the **read-only** evidence for **A3, a live compat defect**: `ZADD … INCR` `:131` returns `Response::bulk(format_float(new_score))` and never asks the version at all, where upstream `zaddGenericCommand` uses `addReplyDouble`. Filed separately; not fixed here. |
| `frogdb-server/crates/commands/src/sorted_set/pop.rs` | 358 | **5 decisions** (`:73`, `:76`, `:140`, `:143`, `:351`) — the densest pairing cluster: `ZPOPMIN`/`ZPOPMAX` are nested **iff RESP3 and an explicit count**, `ZMPOP` `:273` is nested in **both**. Three `Pairing` values are live in one 358-line file. |
| `frogdb-server/crates/commands/src/sorted_set/range.rs` | 400 | **1 decision** (`:125`, `ZRANGE`). Also read-only evidence for **A1, a live compat defect**: `:185`, `:234`, `:291` (`ZRANGEBYSCORE`, `ZREVRANGE`, `ZREVRANGEBYSCORE`) call `scored_array` unconditionally — flat in RESP3 where `ZRANGE` `:126` is nested, and where upstream nests all four off `resp > 2 && withscores`. **No test pins the FrogDB behavior.** Filed separately; not fixed here. |
| `frogdb-server/crates/commands/src/sorted_set/set_ops.rs` | 802 | **3 decisions** (`:197`, `:389`, `:696` — `ZUNION`/`ZINTER`/`ZDIFF`). |
| `frogdb-server/crates/commands/src/blocking.rs` | 905 | **3 decisions** (`:512`, `:597`, `:725` → `Score`). **Read-only evidence for 84** (`:12`, `:230`, `:231`, `:365`) — 84 does not edit this file, §Risks. |
| `frogdb-server/crates/core/src/shard/blocking.rs` | 2090 | **Primary, 4 lines + a deletion.** `zset_score_reply` `:1198-1204` **deleted**; its 3 call sites `:952`, `:984`, `:1021` become `Response::Score(score)`; `let is_resp3 = …` `:934` dies with them. This is **H2** and is independently landable. Its one `FM-CLUSTER-038` tag `:2065` is in the test module, 1000+ lines below the lowest edit. **Concurrently owned by 84 (`:13`, `:1098`, `:1579`) and 88 (`:234`, `:271`, `:327-388`, `:426`, `:754-840`, `:1089-1185`)** — §Risks. |
| `frogdb-server/crates/core/src/pubsub.rs` | 1523 | **2 decisions** (`:363`, `:456` — `Push`-vs-`Array`). Byte-identical fold (`response.rs:313-316`). Note this is the *inside* of the seam that `lint-pubsub-confirmation-seam` already enforces: the gate is satisfied either way, and the fold removes the last branch from the seam's own body. |
| `frogdb-server/crates/core/src/scripting/bindings.rs` | 420 | **CO-PRIMARY (rev 2) — the third renderer.** `wire_response_to_lua` `:93-176` takes **no `ProtocolVersion`** and is where `redis.call`'s Lua shape is decided: `Map` → Lua hash table `:139-148`, `Array` → sequence `:110-118`, `Set` `:149-157` / `Push` `:158-166` → sequence, `Double` → `Value::Number` `:118-121`, `Bulk` → `Value::String` `:106`. Also gains 3 arms for the new carriers. Whatever §F decides lands here. **Concurrently owned by 80** (`response_to_lua` `:82-87`, `wire_response_to_lua` `:93-176`) — §Risks. |
| `frogdb-server/crates/core/src/scripting/gate.rs` | 1215+ | **Read-only evidence for §Problem 6.** `ScriptInvoker` stores the *connection's* protocol version (`:302` field, `:364` captured from `ctx.protocol_version`) and spends it building the sub-command `CommandContext` at `:478`. So a `redis.call` inside `EVAL` executes handlers under the caller's `ProtocolVersion` — which is exactly why the handler branches currently produce RESP2-shaped Lua on a RESP2 connection, and why deleting them changes Lua. |
| `frogdb-server/crates/redis-regression/tests/scripting_tcl.rs` | — | **The hole, proven.** No `redis.call` anywhere in the tree names HGETALL / SMEMBERS / ZRANGE / SUNION / ZPOPMIN / HRANDFIELD / ZSCORE / INCRBYFLOAT (`grep -rniE "redis\.(call\|pcall)\('(hgetall\|smembers\|zrange\|sunion\|...)"` → **zero hits**; the whole inventory is SET/GET/INFO/INCR/EXISTS/RPUSH/LPUSH/PUBLISH/PING/ROLE/MGET/LLEN/DEL/TOUCH/JSON + blocking `pcall`s). `tcl_script_with_resp3_map` `:2400` tests the **opposite** direction (a Lua table with string keys → a RESP3 Map on the wire) and constrains nothing here. Gains the Lua-shape golden. |
| `frogdb-server/benchmarks/benches/protocol.rs` | — | **Read-only dependency of `into_wire`'s signature.** 13 `into_wire()` occurrences (`:23`, `:42`, `:63`, `:86`, `:109`, `:127`, `:158`, `:181`, `:201`, `:225`, `:264`, `:285`, `:329`). Named because §A's "do not change `into_wire`'s signature" argument is only as strong as its dependent list. |
| `frogdb-server/crates/core/src/scripting/executor.rs` | 1215 | **Read-only, explicitly out of scope.** 4 `is_resp3` sites `:371`, `:378`, `:386`, `:402` — these convert *Lua tables* to replies, where the RESP2/RESP3 difference is Lua-surface semantics, not one value in two renderings. §Proposed change (scope). |
| `frogdb-server/crates/server/src/connection/guards.rs` | — | **Read-only, explicitly out of scope.** `:182` (command *gating* in subscribe mode) and `:459` (`PING` in subscribe mode returns `+PONG` in RESP3 vs a 2-element array in RESP2) are **semantic**, not shape: different data, not one value in two renderings. Named so the classification is complete. |
| `frogdb-server/crates/server/src/connection/hotkeys.rs` | 557 | `MapReply::with_capacity(ctx.protocol_version, 6)` `:445` → `Response::Map(…)`; the comment at `:436` updates. |
| `frogdb-server/crates/server/src/connection/auth_conn_command.rs` | 672 | `MapReply::with_capacity(version, 7)` `:453` → `Response::Map(…)`; doc `:448`. The `let proto = if version.is_resp3() { 3 } else { 2 }` at `:451` **stays** — it is HELLO's *payload*, a reported number, not a shape. |
| `frogdb-server/crates/commands/src/stream/info.rs` | 326 | **Beneficiary, optional.** `:207`, `:238`, `:281`, `:320` — flat `Array` in both protocols today. §Proposed change (item D). |
| `frogdb-server/crates/server/tests/resp3.rs` | 926 (whole file) | **The net.** Existing pins that constrain the fold: `:253` / `:293` (HGETALL both protocols), `:331` (SMEMBERS RESP3), `:370` (ZSCORE Double), `:401` (INCRBYFLOAT Double), `:772-926` (the wire-byte block, i.e. from the first wire-byte test to EOF: ZINCRBY `+inf` → `,inf\r\n` `:775`; ZSCORE `+inf` → `$3\r\ninf\r\n` `:858`; NaN rejected before the wire `:808`; ZADD INCR always bulk `:897`, `:914`). Gains the **wire** shape-golden harness. |
| `frogdb-server/crates/redis-regression/tests/zset_regression.rs` | 286 | **The net.** `:20`, `:51`, `:82`, `:111` pin `ZPOPMIN`/`ZPOPMAX` flat-vs-nested by count in RESP3 — the four tests that make `Pairing::NestedInResp3` a *pinned* requirement rather than a guess. `:241` pins `e+308` score rendering. |
| `frogdb-server/crates/redis-regression/tests/zset_tcl.rs` | — | **The net + the hole.** `tcl_bzpopmin_bzpopmax_readraw_resp3` `:2995` (immediate path) and `tcl_bzpopmin_bzpopmax_blocking_resp3` `:3024` (waiter path) both assert `resp3_double(&arr[2])` — but only for **finite** scores (`1.0`, `2.0`, `3.25`), which is exactly why H2's divergence is invisible today. Eight further RESP3 pins the fold must not contradict: `tcl_zinter_resp3` `:2880` (nested pairs + Double, explicitly), `tcl_basic_zpopmin_zpopmax_resp3` `:2909`, `tcl_zpopmin_zpopmax_with_count_resp3` `:2936`, `tcl_zmpop_resp3` `:2971`, `tcl_zmpop_readraw_resp3` `:3053`, `tcl_bzmpop_readraw_resp3` `:3072`, `tcl_zrangestore_resp3` `:3109`, `tcl_zrandmember_resp3` `:3146` (nested pairs). |
| `frogdb-server/crates/redis-regression/tests/hash_tcl.rs` | — | **Read-only evidence for a hole.** `:36` — `HRANDFIELD with RESP3` is listed as `intentional-incompatibility:protocol — RESP3-only`, i.e. the upstream case was **skipped**, not ported. `hash.rs:853`'s `Pairs` shape is therefore entirely unpinned. |
| `frogdb-server/crates/protocol/src/format.rs` | 152 | **Read-only evidence.** `format_float` `:39-74`; the non-finite arms `:40-48` (`"inf"` / `"-inf"` / `"nan"`) and the `-0.0` normalization `:49-52`; the pins that make H2's divergence provable: `:94` `format_float(-0.0) == "0"`, `:117` `format_float(1e17) == "1e+17"`. |
| `Justfile` | — | **Primary, ~25 lines.** `lint-pubsub-confirmation-seam` `:1128-1156` is the **precedent** (read-only). `lint-format-float` `:1249-1269` gains the call-site half (**H3**). `lint-gates` `:329` gains one name. |

Read-only, not edited: `frogdb-server/crates/server/src/connection/debug_conn_command.rs`
(17 unconditional `Response::Map(` sites — the in-tree proof the downgrader is trusted),
`frogdb-server/crates/server/src/connection/client_conn_command.rs:341-349` (CLIENT INFO, refuting a
brief claim) and `:808` (CLIENT TRACKINGINFO, an 18th unconditional `Map`),
`frogdb-server/crates/server/src/connection/frame_io.rs:186-197`
(`invalidation_to_response` — a **19th and stronger** precedent: an unconditional
`WireResponse::Push` on a live RESP2-reachable path, in a function that has no `ProtocolVersion`
parameter to branch on even if someone wanted to),
`frogdb-server/crates/commands/src/sorted_set/store_remove.rs` (279 — `ZRANGESTORE` writes a key and
has no reply shape at all).

---

## Problem

### 1. The count, and what it is a count *of*

`grep -rn "is_resp3()" frogdb-server/crates/commands/src | wc -l` → **24**. Distribution:

| File | `.is_resp3()` queries | shape decisions they drive |
|---|---:|---:|
| `set.rs` | 7 | 7 |
| `sorted_set/pop.rs` | 3 | 5 |
| `sorted_set/set_ops.rs` | 3 | 3 |
| `sorted_set/basic.rs` | 3 | 3 |
| `hash.rs` | 3 | 3 |
| `blocking.rs` | 3 | 3 |
| `string.rs` | 1 | 2 |
| `sorted_set/range.rs` | 1 | 1 |
| **total** | **24** | **27** |

Queries ≠ decisions because six files bind `let is_resp3 = …` once and spend it more than once
(`pop.rs:45` → `:73` and `:76`; `string.rs:764` → `:769` and `:781`).

### 2. Classification — 12 redundant, 8 unrepresentable-pairing, 7 unrepresentable-policy

**(a) Pure duplication — 12 decisions.** The RESP2 arm is byte-for-byte what `to_resp2_frame`
produces from the RESP3 arm. Deleting the branch is provably wire-invisible:

| Site(s) | RESP3 arm | RESP2 arm | Downgrader arm that already does it |
|---|---|---|---|
| `set.rs:164`, `:171`, `:355`, `:390`, `:418`, `:453`, `:480` (7) | `Response::Set(v)` | `Response::Array(v)` | `response.rs:307-310` |
| `hash.rs:385`, `:403` (2) | `Response::Map(pairs)` | flat `Array` built by hand `:392-400` | `response.rs:297-306` |
| `string.rs:769`, `:781` (2) | `Response::Double(x)` | `Response::bulk(format_float(x))` | `response.rs:286` — *the identical expression* |
| `sorted_set/basic.rs:410` (1) | `Response::Double(s)` | `Response::bulk(format_float(s))` | `response.rs:286` |

`hash.rs:392-400` is worth reading as the shape of the waste: nine lines that build
`[k1,v1,k2,v2,…]` from a hash, sitting under an arm whose sibling built the same pairs as a `Map`
that `response.rs:298-305` flattens identically.

Two more of the same class live outside `frogdb-commands`: `core/src/pubsub.rs:363` and `:456`,
`Push` vs `Array`, covered by `response.rs:313-316`.

**(b) Pairing — 8 decisions the downgrader cannot infer.** Both shapes are `Response::Array`:

| Site | Command | RESP3 | RESP2 |
|---|---|---|---|
| `range.rs:125` | `ZRANGE … WITHSCORES` | `[[m,s],…]` (`scored_array_resp3`) | `[m,s,…]` (`scored_array`) |
| `pop.rs:73` | `ZPOPMIN key count` | `[[m,s],…]` | `[m,s,…]` |
| `pop.rs:140` | `ZPOPMAX key count` | `[[m,s],…]` | `[m,s,…]` |
| `pop.rs:351` | `ZRANDMEMBER … WITHSCORES` | `[[m,s],…]` | `[m,s,…]` |
| `set_ops.rs:197` | `ZUNION … WITHSCORES` | `[[m,s],…]` | `[m,s,…]` |
| `set_ops.rs:389` | `ZINTER … WITHSCORES` | `[[m,s],…]` | `[m,s,…]` |
| `set_ops.rs:696` | `ZDIFF … WITHSCORES` | `[[m,s],…]` | `[m,s,…]` |
| `hash.rs:853` | `HRANDFIELD … WITHVALUES` | `[[f,v],…]` | `[f,v,…]` |

A `WireResponse::Array(vec![Array([m,s]), …])` reaching `to_resp2_frame` is indistinguishable from
any other array-of-2-arrays, and flattening it unconditionally would be **wrong**: `ZMPOP`
(`pop.rs:273` — `scored_array_resp3(results, true)` called **unconditionally**, with the reason
stated in the comment at `:263-264`: *"ZMPOP always uses nested format `[[member, score], …]` in both
RESP2 and RESP3"*) and `BZMPOP` (`blocking.rs:722-726`) are nested in *both* protocols. This is the real gap, and it is a **missing fact in the type**, not a missing rule in the
downgrader.

(`scored_array_resp3` has **8** call sites in all — `pop.rs:74`, `:141`, `:273`, `:352`,
`range.rs:126`, `set_ops.rs:198`, `:390`, `:697` — and `:273` is the one that is not a branch arm.)

**(c) Score policy — 7 decisions carrying a rule that is spent before the type exists.**
`score_response` (`utils.rs:799-805`):

```rust
if is_resp3 && score.is_finite() { Response::Double(score) } else { Response::bulk(Bytes::from(format_float(score))) }
```

The `is_finite()` gate is a deliberate divergence — `resp3.rs:855-857` documents it and `:858`,
`:875` pin it (`ZSCORE` of `+inf` in RESP3 is `$3\r\ninf\r\n`, **not** `,inf\r\n`). Call sites:
`basic.rs:272` (ZSCORE), `:315` (ZMSCORE), `blocking.rs:512` (BZPOPMIN), `:597` (BZPOPMAX), `:725`
(BZMPOP), and via `scored_array_with_scores_resp3` `utils.rs:859` from `pop.rs:76`, `:143`.

Once the function returns, `Response::Bulk("inf")` is a bulk string like any other. The
*policy* — "this is a **score**, and scores render non-finite values as text in both protocols" —
is not in the value. Seven call sites therefore have to know it.

### 3. Three asymmetries that exist only because 27 sites decide independently — **two of them are live Redis-compat defects**

All three are in-tree facts, verifiable without reference to Redis. Rev 1 rated all three as
"pinned asymmetries, preserved not fixed." That rating was **wrong for A1 and A3**: neither is
pinned in the direction that matters, and both diverge from Redis. They are **defects to be filed**,
not follow-ups to be considered.

- **A1 — `ZRANGE` vs its three legacy siblings. LIVE COMPAT DEFECT, unpinned.**
  `ZRANGE … WITHSCORES` is nested in RESP3 (`range.rs:121-129`). `ZRANGEBYSCORE` `:185`,
  `ZREVRANGE` `:234`, `ZREVRANGEBYSCORE` `:291` call `scored_array` **unconditionally** — flat in
  RESP3. Upstream Redis nests **all four**: `genericZrangebyscoreCommand` and friends compute
  `should_emit_array_length = (resp > 2 && withscores)` from the *connection*, not the command, so
  `ZREVRANGEBYSCORE … WITHSCORES` on a RESP3 connection is an array of pairs there and a flat array
  here. **No test in the tree pins the FrogDB behavior** (the ZRANGE-family RESP3 pins in
  `zset_tcl.rs` cover ZINTER/ZRANDMEMBER/ZPOP*/ZMPOP, not the legacy range trio). So this is not
  "preserved current behavior" — it is an unnoticed incompatibility. **File as a compat defect.**
- **A2 — `ZSCORE` vs `ZINCRBY` on non-finite. Pinned both ways; the rationale is thin.** `ZSCORE`
  routes `+inf` to a bulk string in RESP3 (`basic.rs:272` → the `is_finite` gate; pinned
  `resp3.rs:858`). `ZINCRBY` `:410` has **no** gate and emits `Double(inf)` → `,inf\r\n` (pinned
  `resp3.rs:775`). Two commands, one score type, two RESP3 wire types. The gate's stated reason
  (`utils.rs:792-795`) is *"to avoid client-side formatting differences (e.g., Tcl displays float
  infinity as `Inf`)"* — a **test-harness display artifact**, not a protocol requirement, and
  probably itself a compat bug. Rev 2 therefore **does not** promote this rationale into
  `Response::Score`'s doc comment. `Score` reproduces the gate because two tests pin it, and the
  doc says only *"pinned by `resp3.rs:858`/`:875`; see A2 — the justification is a Tcl display
  artifact and is under review."* Do not harden a workaround by writing it into a type.
- **A3 — `ZADD … INCR` vs `ZINCRBY`. LIVE COMPAT DEFECT, pinned in the wrong direction.**
  `basic.rs:131` returns `Response::bulk(format_float(...))` unconditionally and never reads
  `ctx.protocol_version` — so `ZADD k INCR 3 m` is `$1\r\n3\r\n` in RESP3 while `ZINCRBY k 3 m` is
  `,3\r\n`. Upstream `zaddGenericCommand` calls `addReplyDouble`, which emits `,3\r\n` under RESP3.
  The FrogDB behavior is pinned at `resp3.rs:890-895` / `:897` / `:914`, and the pin's own doc
  comment concedes the point: *"flagged … as a RESP3-consistency gap relative to ZINCRBY, out of
  scope for this encoder/dispatch-change-free pin task."* This proposal **is** that dispatch change.
  **File as a compat defect**; the fix flips those two golden rows.

The structural point stands and is now sharper: A1 and A3 are not stylistic drift, they are wire
incompatibilities that a 27-authority design **cannot notice**, because no one site is wrong on its
own terms. Under one authority they are not expressible — A2 and A3 collapse to one rule the moment
`Response::Score` exists; A1 collapses the moment `Pairing` is a field rather than a helper choice.

**Filing note for the orchestrator:** A1 and A3 are to be filed as Redis-compat defects
(`.scratch/` issue tracker), each citing the file:line above and the golden row it flips. This
proposal does not fix them (§Proposed change D) — it makes each a one-line fix.

### 4. `MapReply` — the same rule, stated a second time, one crate away

`protocol/src/reply.rs:19` claims single ownership: *"The flatten/map rule lives in
`MapReply::finish` and nowhere else."* It does not. `to_resp2_frame`'s `Map` arm
(`response.rs:297-306`) is the same rule, in the same crate, 130 lines away, and is what the 18
unconditional `Response::Map(` producers in `frogdb-server` rely on.

`MapReply`'s real contribution is `field_if` (`reply.rs:56-64`) — "the predicate lives ONCE here,
not once per protocol arm." That value is **entirely a consequence of the RESP2 arm existing**. With
no arm, `field_if` is `if cond { pairs.push(...) }`. Two callers (`hotkeys.rs:445`,
`auth_conn_command.rs:453`); the `ProtocolVersion` each threads in becomes unused.

### 5. The live one — `zset_score_reply`, a second `score_response` that disagrees

`core/src/shard/blocking.rs:1198-1204`:

```rust
fn zset_score_reply(score: f64, is_resp3: bool) -> Response {
    if is_resp3 { Response::Double(score) } else { Response::bulk(Bytes::from(score.to_string())) }
}
```

versus `commands/src/utils.rs:799-805`'s `score_response`. Same concept, three differences:

| Score | Immediate path (`commands/src/blocking.rs:512` → `score_response`) | Waiter path (`core/src/shard/blocking.rs:952` → `zset_score_reply`) |
|---|---|---|
| `+inf`, **RESP3** | `$3\r\ninf\r\n` (bulk — the `is_finite` gate) | `,inf\r\n` (**Double** — no gate) |
| `-0.0`, RESP2 | `format_float(-0.0)` = `"0"` (pinned `format.rs:94`) | `(-0.0f64).to_string()` = `"-0"` |
| `1e17`, RESP2 | `format_float(1e17)` = `"1e+17"` (pinned `format.rs:117`) | `to_string()` = `"100000000000000000"` |
| `NaN`, RESP2 | `format_float(NaN)` = `"nan"` (`format.rs:46-48`) | `f64::NAN.to_string()` = `"NaN"` |

(Rows 2 and 3 verified by compiling and running a two-branch comparison on the pinned toolchain;
`format.rs:94` and `:117` pin the left column independently. Row 4 is **not reachable today** —
`ZADD`/`ZINCRBY` reject a NaN result before it can be stored (`resp3.rs:808`
`test_zincrby_resp3_nan_result_is_rejected_not_wired`) — and is listed because it is the fourth way
the two renderers disagree, and because the fold makes it *structurally* impossible rather than
*incidentally* unreachable. A future score source that admits NaN would resurrect it under the
current design and cannot under the proposed one.)

So `BZPOPMIN k 0` on a key that **already has** a `+inf` member returns a bulk string in RESP3, and
the *same command* on an empty key that receives a `+inf` member a millisecond later returns a
`Double`. The client sees the wire type of a score depend on whether it had to block.

Why the test suite misses it: `tcl_bzpopmin_bzpopmax_readraw_resp3` (`zset_tcl.rs:2995`) and
`tcl_bzpopmin_bzpopmax_blocking_resp3` (`:3024`) cover **both** paths — with scores `1.0`, `2.0`,
`3.25`. Every value in the disagreement set is non-finite, negative zero, or ≥ 1e17.

Why the gate misses it: `lint-format-float` (`Justfile:1249-1269`) greps for `fn format_float`
**definitions** outside `protocol/src/format.rs`. It enforces *one definition*; it does not enforce
*one call path*. `score.to_string()` is not a definition, so the gate passes while a second float
renderer runs on a live reply path. The gate's own comment (`Justfile:1245-1248`) names exactly this
hazard — "Five copies had drifted" — and the sixth is sitting inside its blind spot.

### 6. The third renderer — `redis.call` shape is decided by the *handler*, and nothing says so

`Response` has two wire encoders. It also has a **non-wire** one:

```rust
// core/src/scripting/bindings.rs:93
fn wire_response_to_lua(lua: &mlua::Lua, response: WireResponse) -> LuaResult<Value>
```

**No `ProtocolVersion` parameter.** Its shape decisions, all verified at HEAD:

| `WireResponse` | Lua value | bindings.rs |
|---|---|---|
| `Array(v)` | sequence — `t[1]`, `t[2]`, … | `:110-118` |
| `Map(pairs)` | **hash table** — `t[k] = v` | `:139-148` |
| `Set(v)` / `Push(v)` | sequence | `:149-157`, `:158-166` |
| `Double(n)` | `Value::Number` | `:118-121` |
| `Bulk(Some(b))` | `Value::String` | `:106` |

And the protocol version *does* reach the handlers underneath a script. `ScriptInvoker` captures the
**connection's** version (`gate.rs:302` field, `:364` `protocol_version: ctx.protocol_version`) and
spends it constructing the sub-command `CommandContext` (`:478`). So today:

| On a **RESP2** connection | handler builds | Lua sees |
|---|---|---|
| `redis.call('HGETALL', k)` | `Array` (`hash.rs:403` RESP2 arm) | flat sequence — `t[1]=='f1'` |
| `redis.call('SMEMBERS', k)` | `Array` (`set.rs:355` RESP2 arm) | sequence |
| `redis.call('INCRBYFLOAT', k, 1)` | `Bulk` (`string.rs:781` RESP2 arm) | **string** |
| `redis.call('ZSCORE', k, m)` | `Bulk` (`basic.rs:272`, RESP2) | **string** |
| `redis.call('ZRANGE', k, 0, -1, 'WITHSCORES')` | flat `Array` (`range.rs:128`) | flat sequence |

**This is the Redis contract.** In upstream Redis, `redis.call` returns RESP2-shaped Lua *regardless
of the client's protocol*, unless the script opts in with `redis.setresp(3)`. A RESP2 client's script
indexing `result[1]` is correct, portable, documented behavior.

**Under rev 1's fold it breaks.** Delete the handler branches and every handler emits the RESP3
shape unconditionally; `wire_response_to_lua` has no version to consult, so on a **RESP2**
connection `redis.call('HGETALL')` returns a Lua **hash table** (`t[1] == nil`),
`redis.call('INCRBYFLOAT')` returns a Lua **number** rather than a string, `ZSCORE` likewise, and
`ZRANGE … WITHSCORES` returns a sequence of 2-element tables. Every one of those is a silent,
run-time breakage in user scripts.

**Nothing in the tree catches it.** No `redis.call`/`redis.pcall` anywhere names a shape-sensitive
command: the complete inventory across all `.rs`/`.lua` sources is SET/GET/INFO/INCR/EXISTS/RPUSH/
LPUSH/PUBLISH/PING/ROLE/MGET/LLEN/DEL/TOUCH/RANDOMKEY/SELECT/WAIT/LRANGE/HINCRBY/XREAD/JSON.\*/
PROBEREADKEY plus the blocking `pcall`s — **zero** HGETALL, SMEMBERS, ZRANGE, SUNION, HRANDFIELD,
ZSCORE, ZPOPMIN, INCRBYFLOAT. `scripting_tcl.rs:2400` `tcl_script_with_resp3_map` tests the
**opposite** direction (Lua table with string keys → RESP3 `Map` on the wire) and is silent on this.
`redis.setresp` exists only as a load-time smoke test (`functions_tcl.rs:588`).

**Consequence for the acceptance criterion.** Rev 1 said "the wire golden proves the fold is
byte-identical." The wire golden proves **nothing** about `bindings.rs` — a script's reply is
rendered by a different function, into a different value space, and the wire golden never calls it.
Rev 1's acceptance criterion was therefore not just incomplete but *misleading about its own
coverage*. §F fixes the design; §Testability step 2 fixes the criterion.

---

## Proposed change

**One module decides reply shape: `frogdb-protocol`'s two encoders** — and `redis.call`'s Lua shape
gets an explicit, stated owner rather than an accidental one (§F). Handlers state the value.

### The precedent this generalizes

`lint-pubsub-confirmation-seam` (`Justfile:1128-1156`) already rules, for one command family, that
*"`PubSubConfirmation::to_response(protocol)` … is the single owner of the RESP3 Push vs RESP2 Array
confirmation shape"*, and fails the build on a hand-built confirmation label in the pub/sub handlers.
The ruling is accepted; only its **scope** is one family. This proposal applies the same ruling to
the rest of the reply surface and — because the downgrader can then do the work — removes the branch
from `PubSubConfirmation::to_response`'s own body too (`pubsub.rs:363`).

### (A) Three carrier variants, on both `Response` and `WireResponse`

```rust
/// A sorted-set score. RESP3: Double when finite, bulk `format_float` otherwise;
/// RESP2: always bulk `format_float`.
///
/// The finiteness gate is reproduced because `resp3.rs:858` and `:875` pin it,
/// NOT because it is right: its stated justification (`utils.rs:792-795`) is a
/// Tcl display artifact, and it disagrees with ZINCRBY (§Problem 3 A2). Under
/// review as a compat defect — do not build on it.
Score(f64),

/// Members with optional scores. RESP2 is always flat with bulk scores.
ScoredMembers { members: Vec<(Bytes, f64)>, with_scores: bool, pairing: Pairing },

/// Field/value pairs. RESP2 flat; RESP3 per `pairing`.
Pairs { pairs: Vec<(Bytes, Bytes)>, pairing: Pairing },

pub enum Pairing { Flat, NestedInResp3, NestedAlways }
```

`Pairing` has exactly three inhabitants because the tree exhibits exactly three behaviors, all in
`pop.rs`: `ZPOPMIN` without count is `Flat` (`:76`), with count is `NestedInResp3` (`:73`/`:74`),
`ZMPOP` is `NestedAlways` (`:273`, unconditional; comment `:263-264`).

**They live on `WireResponse` too, and resolve in the encoders — not in `into_wire`.** Reason:
`into_wire()` takes no `ProtocolVersion`, and its signature has **five production dependents plus a
benchmark suite**:

| Dependent | Where |
|---|---|
| `impl From<Response> for BytesFrame` | `response.rs:933-940` |
| `Response::try_to_resp2_frame` | `response.rs:920` |
| `Response::try_to_resp3_frame` | `response.rs:928` |
| `response_to_lua` (scripting) | `bindings.rs:83` |
| `ConnectionHandler::narrow_to_wire` | `frame_io.rs:42` (fn `:31-53`, owned by proposal 80) |
| 13 call sites | `frogdb-server/benchmarks/benches/protocol.rs:23`, `:42`, `:63`, `:86`, `:109`, `:127`, `:158`, `:181`, `:201`, `:225`, `:264`, `:285`, `:329` |

(Rev 1 cited `response.rs:463-467` here. That is `impl From<WireResponse> for BytesFrame` — it
consumes a `WireResponse` and never calls `into_wire`, so it is not a dependent at all. The real one
is `:933-940`.)

Resolving in `to_resp2_frame` / `to_resp3_frame` keeps every signature, and puts the two renderings
of each carrier **on the same screen** — the property the current design cannot have, because the
two renderings live in a handler branch and a downgrader arm in different crates.

Cost, stated plainly: three arms in `Response::into_wire`, three in `from_wire`, three in
`to_resp2_frame`, three in `to_resp3_frame`, three in `bindings.rs`'s `wire_response_to_lua`
`:93-176`. ~60 lines added to `response.rs` against ~130 deleted from `utils.rs` and 27 branches
deleted from nine handler files.

**Rejected alternative — a `Response::ScoredPairs` on `Response` only, resolved by a
`into_wire(version)`:** fewer variants, but it changes a signature three call sites and one
concurrent proposal depend on, and it splits the two renderings across a crate boundary again.

### (B) Delete the 12 redundant branches

Emit the RESP3 shape unconditionally. `Set`, `Map`, `Push`, `Double` are all already downgraded
correctly, and each fold is provably byte-identical (§Problem 2a). This is the part with **zero**
wire-visible effect, and it is the part the shape golden proves rather than argues.

### (C) `MapReply` folds into `Response::Map`

Delete `reply.rs` (168 lines). `hotkeys.rs:445` and `auth_conn_command.rs:453` build
`Response::Map(vec![…])`; `field_if` becomes `if cond { … }` at the two sites that use it. The
`ProtocolVersion` argument disappears from both.

### (D) Scope — what is explicitly *not* folded

- **`scripting/executor.rs:371-402`** (4 sites). Lua-table → reply conversion — the *return* half of
  the script boundary, where RESP2/RESP3 differ in Lua surface semantics (which table key the script
  must use), not in the rendering of one value. Different problem, different owner. Note this is
  **not** `bindings.rs`'s `wire_response_to_lua`, which is the `redis.call` half and **is** in
  scope (§F).
- **`guards.rs:182`** — command *gating* in subscribe mode. Not a reply at all.
- **`guards.rs:459`** — `PING` in subscribe mode: `+PONG` in RESP3 vs `["pong", ""]` in RESP2. This
  is **different data**, not one value in two shapes. Genuinely unfoldable, and named here so the
  27-vs-31 arithmetic is closed.
- **`auth_conn_command.rs:451`** — `let proto = if version.is_resp3() { 3 } else { 2 }` is HELLO's
  *payload*. Reporting the protocol number is not choosing a shape.
- **`stream/info.rs`** — `XINFO`'s missing RESP3 Map is a *beneficiary*, listed as optional item D
  and deliberately not bundled: it is a wire-visible **addition**, and this proposal's value
  proposition is that everything else is wire-invisible. It should be its own change, on top.
- **`ZADD … INCR` (A3) and `ZRANGEBYSCORE`/`ZREVRANGE`/`ZREVRANGEBYSCORE` (A1)** — the fold makes
  them one-line fixes but does **not** apply them. Same reason: they are the only wire-visible rows,
  and mixing them into a 27-site refactor destroys the "golden is byte-identical" acceptance
  criterion. **They are filed as Redis-compat defects, not deferred as cleanup** (§Problem 3): A1 is
  an unpinned divergence from upstream's `resp > 2 && withscores` nesting rule across all four range
  commands; A3 is `addReplyDouble` upstream (`,3\r\n`) vs an unconditional bulk here (`$1\r\n3\r\n`),
  pinned at `resp3.rs:890-895`. Each issue names the golden row it flips.

### (E) The gate

`lint-resp3-shape-once`, added to `lint-gates` (`Justfile:329`) — the compile-free family that runs
on every commit:

1. `grep -rn 'is_resp3' frogdb-server/crates/commands/src/` must be **empty**. After the fold there
   is no legitimate reason for a command handler to know the *shape*; the seam is in
   `frogdb-protocol`.

   **Escape hatch, stated up front.** "Shape" is not "data." A future command whose *payload*
   genuinely differs by protocol — the class `guards.rs:459` (`PING` in subscribe mode) and
   `auth_conn_command.rs:451` (HELLO reporting `proto: 3`) already belong to — has a legitimate
   reason to read the version, and an empty-grep gate would force it to lie or to be waived
   wholesale. The gate therefore accepts a single-line
   `// resp-shape-exempt: <reason>` marker on the preceding line, mirroring how the other
   chokepoint gates in the family handle their genuine exceptions. Reviewing the exemption list is
   the point; a gate with no escape hatch gets disabled the first time it is inconvenient.
2. `lint-format-float` (`Justfile:1249-1269`) gains its missing half: no `to_string()` /
   `format!("{}", …)` applied to an `f64` on a reply path outside `format.rs`. This is **H3** and is
   what would have caught §Problem 5 at the commit that introduced it.

### (F) The third renderer — `redis.call`'s Lua shape (rev 2, required)

The fold **must** decide who owns `redis.call`'s Lua shape, because it takes that decision away from
the handlers (§Problem 6). Two viable designs; both preserve today's behavior, and the choice is a
design decision this proposal puts to the reviewer rather than assumes:

**F-1 — thread `ProtocolVersion` into the Lua renderer** (preferred).
`wire_response_to_lua(lua, response, version)`; `Map` under `Resp2` renders as the flattened
sequence (`k1, v1, k2, v2, …`), `Set`/`Push` as sequences (unchanged), `Score`/`Double` under
`Resp2` as a Lua **string** via `format_float`, `Pairs`/`ScoredMembers` per `pairing` exactly as the
wire encoders do. The version is already in hand — `ScriptInvoker` holds it (`gate.rs:302`) and
`response_to_lua` (`bindings.rs:82-87`) is called from inside the invoker, so this is a
parameter-threading change of two functions, not a plumbing project.
*Cost:* touches every arm of a 84-line match. *Benefit:* `redis.setresp(3)` becomes implementable
later by passing a different version — which is the actual Redis contract, and is unimplementable
under the current design at any price.

**F-2 — normalize through RESP2 before Lua.** `response_to_lua` calls `to_resp2_frame()` (or a
`WireResponse → WireResponse` RESP2-normalizer) and renders the result. Fewer touched arms, and it
makes "RESP2-shaped unless `setresp(3)`" literally true by construction.
*Cost:* a frame round-trip on every `redis.call`, and `setresp(3)` support later needs the version
back anyway. *Also:* `to_resp2_frame` returns `Resp2BytesFrame`, so this needs the normalizer
variant, not the encoder.

**Recommendation: F-1.** It is the honest shape of the fact — the Lua renderer *is* protocol-
dependent — and it leaves `setresp` reachable. F-2 is acceptable as a smaller first step.

Either way, the seam statement becomes: *shape is decided in `frogdb-protocol`'s two wire encoders
and in `bindings.rs`'s one Lua renderer, from the same carriers, and nowhere else* — three
renderings of one value, in two files, instead of two renderings in twenty-seven.

---

## Testability improvement

**The deletion test.** Delete `frogdb-protocol`'s shape authority today and nothing fails to
compile — 27 handlers still know both shapes, so the downgrader's `Map`/`Set`/`Push`/`Double` arms
are dead weight for those commands and load-bearing for 18 others. That is the diagnosis: the
authority is *partial*, and partial authority is worse than none, because the handler cannot tell
which regime it is in. After the change, deleting the arms breaks every RESP2 client immediately and
loudly, and no handler can compensate — the seam is real when its removal is unsurvivable.

**What is testable after that is not testable now:**

1. **Shape is a unit-testable pure function.** `Response::ScoredMembers { pairing: NestedInResp3, … }`
   → both frames, in `protocol/src/response.rs`'s own `#[cfg(test)]` module, with no server, no
   socket, no `HELLO`. Today, verifying `ZRANGE`'s RESP3 pairing requires `zset_regression.rs`'s
   full `TestServer` + `connect_resp3` + `HELLO 3` round trip (`:51-81`, 30 lines for one shape
   assertion). Three `Pairing` values × two protocols = **6 assertions in ~15 lines**, and they
   cover *every* command that uses that pairing rather than the four that currently have tests.
2. **The asymmetries become compile-time impossibilities.** A1 (ZRANGE vs ZRANGEBYSCORE) is
   expressible today because `scored_array` and `scored_array_resp3` are two functions a handler
   picks between. With `pairing` a field on one variant, "these four commands disagree" requires
   four different literal values — visible in one grep, and pinnable by a single test that asserts
   the four call sites pass the same `Pairing`.
3. **Mutation-testable shape.** `cargo mutants -p frogdb-protocol` on a `to_resp2_frame` arm that
   flattens `ScoredMembers` produces a surviving-or-killed verdict on *reply shape itself*.
   Currently the mutant would have to land in one of 27 handler `if`s, in a crate
   (`frogdb-commands`) with **no `tests/` directory at all** (verified — proposal 90 §Files makes
   the same observation), so each mutant is killable only from `frogdb-server` integration tests,
   which by the campaign's own rule (`CLAUDE.md`: "put the forcing test in the mutated crate")
   contributes nothing to the owning crate's score.
4. **One latent panic stops being expressible.** `hash.rs:858` — inside the RESP3 arm of
   `HRANDFIELD … WITHVALUES` — does `Response::bulk(v.unwrap())` on an `Option<Bytes>` that the
   RESP2 arm 9 lines below (`:867-869`) handles as genuinely optional. Whether that `unwrap` can fire
   depends on `hash.random_fields(count, with_values)`'s postcondition, which nothing states; the
   two arms disagree about what it is. A single `Pairs { pairs: Vec<(Bytes, Bytes)>, … }`
   construction has one answer, and the type carries it.

**Golden-first plan (required — the change is wire-visible by construction, even where it is
wire-*invariant* in fact).**

**Pinned today** (the fold must not contradict any of these):

| Surface | Pins |
|---|---|
| HGETALL, both protocols | `resp3.rs:253`, `:293` |
| SMEMBERS RESP3 `Set` | `resp3.rs:331` |
| ZSCORE / INCRBYFLOAT Double | `resp3.rs:370`, `:401` |
| ZSCORE / ZINCRBY / ZADD-INCR wire bytes, incl. non-finite | `resp3.rs:772-926` |
| ZPOPMIN/ZPOPMAX flat-vs-nested by count | `zset_regression.rs:20`, `:51`, `:82`, `:111`; `zset_tcl.rs:2909`, `:2936` |
| **ZINTER WITHSCORES nested pairs + Double, RESP3** | `zset_tcl.rs:2880` |
| **ZRANDMEMBER WITHSCORES nested pairs, RESP3** | `zset_tcl.rs:3146` |
| ZMPOP / BZMPOP nested-always | `zset_tcl.rs:2971`, `:3053`, `:3072` |
| BZPOPMIN/BZPOPMAX RESP3 Double, both paths (finite only) | `zset_tcl.rs:2995`, `:3024` |
| ZRANGESTORE RESP3 | `zset_tcl.rs:3109` |

(Rev 1 listed ZINTER and ZRANDMEMBER as *unpinned*. Both are pinned, and `tcl_zinter_resp3:2891`
even states the nested-pair rule in a comment. Corrected.)

**Genuinely unpinned — the residual-risk surface:**

- SUNION / SINTER / SDIFF RESP3 `Set` shape (**zero** RESP3 tests for any of them)
- ZUNION and ZDIFF pairing (ZINTER's sibling pins do not cover them)
- ZMSCORE score type in RESP3
- ZRANGE … WITHSCORES RESP3 pairing (every in-tree `ZRANGE` assertion is on a RESP2 connection)
- HRANDFIELD … WITHVALUES pairing — and this one is *deliberately* absent: `hash_tcl.rs:36` records
  `HRANDFIELD with RESP3` as `intentional-incompatibility:protocol`, i.e. the upstream case was
  skipped rather than ported
- every non-finite score on the waiter path (the hole that let §Problem 5 live)
- **every `redis.call` reply shape, on both protocols** (§Problem 6 — the largest hole, and the one
  rev 1 did not know about)

So, in order:

1. **Land the wire golden first, against unmodified code.** A table-driven harness in
   `server/tests/resp3.rs`: `(setup commands, command, protocol)` → raw wire bytes, one file
   `resp3_shape_golden.txt`. Rows: every command named in §Problem 2 (a) and (b), each in RESP2 and
   RESP3, each with a finite score, `+inf`, `-inf`, `-0.0`, `1e17`, and empty/missing-key. ~35
   commands × 2 protocols. Same pattern proposal 90 uses for `CommandSpec` — golden lands *before*
   the sweep, on the pre-change tree, so it records the truth rather than the intent.
2. **Land the Lua golden first too** — new, and non-negotiable, because step 1 proves nothing about
   `bindings.rs` (§Problem 6). A second table in `scripting_tcl.rs`:
   `EVAL "local r = redis.call(...) return <shape probe>" 1 k`, run on **both** protocols, where the
   shape probe reports the structure rather than the contents — e.g.
   `return {type(r), #r, tostring(r[1]), tostring(r.f1)}` — so a sequence→hash flip is a diff and
   not a silent pass. Minimum rows, each × RESP2 and RESP3:
   `HGETALL` (sequence vs hash), `SMEMBERS` (sequence), `ZRANGE k 0 -1 WITHSCORES` (flat vs nested),
   `ZPOPMIN k 2` (flat vs nested), `INCRBYFLOAT` (string vs number), `ZSCORE` (string vs number),
   `HRANDFIELD k 2 WITHVALUES` (flat vs nested). These land red-free on the unmodified tree and
   become the only thing standing between the fold and a silent breakage of every RESP2 client's
   scripts.
3. **Add the two missing waiter-path rows** (BZPOPMIN blocked, `+inf` and `-0.0`, both protocols).
   They **fail immediately** — this is the H2 regression test, and it is the reason H2 is filed as a
   defect fix rather than as cleanup.
4. **H2, then H1, then the fold** (§F decided before the fold starts, since it changes
   `bindings.rs`).
5. **The gate** (§E) lands last, once the grep is empty.

**Acceptance criterion, restated (rev 2).** After the fold, **the wire golden AND the Lua golden are
both byte-identical to their pre-change recordings, except exactly two rows** — the two waiter-path
BZPOPMIN rows from step 3, which flip to agree with the immediate path. One golden is not the
criterion; two are. A green wire golden with an unrecorded Lua golden is precisely the state in
which rev 1's fold would have shipped a breaking change.

---

## Risks / scope boundaries vs siblings

### vs proposal 86 (`resp3-egress-codec`, rev 2) — **zero design overlap, one shared file, disjoint regions**

86's subject is **server-side egress**: how an already-chosen `WireResponse` reaches the socket —
`frame_io.rs` (the sink-vs-`write_all` split), `codec.rs` (the `Outbound` encoder), `util.rs`
(`estimate_resp2_frame_size`), `connection.rs` (`resp3_buf`), `lifecycle.rs`. 94's subject is
**commands-side construction**: which `Response` a handler builds. The two meet at exactly one type
— `WireResponse` — and on opposite sides of it: 86 consumes it, 94 produces it.

**86 does not edit either encoder body.** Its own Files table says so verbatim for
`protocol/src/response.rs`: *"**Doc-only edits**: `sanitize_error_message`'s cross-crate claim
`:195-199` (H4) and the two `NullArray` notes `:267-273`, `:323-332` … **Neither encoder body is
touched.**"* 94 edits the encoder bodies and neither doc block.

The one hazard is **textual, not semantic**: 86's `NullArray` doc edits at `:267-273` and `:323-332`
**bracket** `to_resp2_frame` (`:274-334`), whose match block 94 extends by three arms. A three-arm
insertion before `:333` shifts 86's `:323-332` block. Mitigation: land 86's four-line doc hotfix
first (it is H4 there, independently landable and already scoped as a hotfix), or rebase 94's
insertion point. Either order works; the two edits cannot produce a semantic conflict because
neither changes what the other's lines mean.

One genuine **sequencing** note, not a conflict: 86 adds a `HELLO 3`-mid-pipeline test to
`server/tests/resp3.rs`, and 94 adds the shape-golden harness to the same file. Different regions,
same file, ordinary merge.

### vs proposal 84 (`blocking-op-dedupe`, rev 2) — **two shared files, disjoint regions, no ordering constraint**

- `protocol/src/response.rs`: 84 **deletes** `Direction` `:473-493` and `BlockingOp` `:495-550` to a
  new `protocol/src/blocking.rs`. 94 touches `:116-175`, `:274-334`, `:341-432`, `:647-743`,
  `:770-881`. **No line in common**, and 84 explicitly keeps `Response::BlockingNeeded` `:709-716`
  verbatim, which is inside 94's enum region but not on a line 94 edits (94 appends variants).
  84-first shifts 94's `:647-743` up by 78; 94-first shifts nothing 84 deletes. Either order.
- `core/src/shard/blocking.rs`: 84 edits `:13`, `:1098`, `:1579`. 94 edits `:934`, `:952`, `:984`,
  `:1021`, and deletes `:1198-1204`. **Disjoint.** 84 lists `commands/src/blocking.rs` as
  *read-only evidence* (`:12`, `:230`, `:231`, `:365`); 94 edits `:512`, `:597`, `:725` in that
  file — also disjoint, and 94's edits do not disturb the `Direction::parse` call sites 84 cites.
- **H2 is the interaction worth naming.** 84 moves `BlockingOp` between crates; H2 changes what the
  waiter path *replies*. Both touch `core/src/shard/blocking.rs` for unrelated reasons. H2 is 8
  lines and should land first precisely because it is a defect fix with a failing test behind it.

### vs proposal 90 (`CommandSpec::DEFAULT`) — **maximum file overlap, zero region overlap, but a real sweep collision**

90 rewrites the `static SPEC: CommandSpec = CommandSpec { … }` block inside `fn spec()` at 296 sites
across 56 files in `crates/commands/src`. 94 rewrites `fn execute()` bodies in nine of those files.
Within any one command struct these are **adjacent but disjoint** — `spec()` then `execute()`, e.g.
`set.rs:145-156` (90) vs `set.rs:158-178` (94).

The collision is mechanical, not semantic: both are large mechanical sweeps over the same files, and
whichever lands second rebases across the other. 90 is characterized as "S per file / M total" and
is a pure `..DEFAULT` substitution with a `Debug`-golden proving bit-identity; 94's edits are
localized to 27 known sites. **Recommended order: 90 first.** Its golden makes its own sweep
verifiable independently, its diff is uniform enough to re-apply mechanically, and 94's 27 sites are
small enough to rebase by hand. This is a scheduling preference, not a dependency — there is no
shared symbol and no shared line.

### vs proposal 80 (`response-wire-fold`) — **the closest sibling; must be sequenced**

80 owns `protocol/src/response.rs` as **primary**, folding `Response`/`WireResponse`/`InternalAction`
and killing `WireResult`. 94 **adds variants to both enums** and arms to both encoders. This is a
genuine design interaction, not just a textual one: if 80 collapses `Response` and `WireResponse`
into one type, 94's carriers land on one enum instead of two and the "resolve in the encoders, not
in `into_wire`" argument (§A) needs re-derivation against the folded shape.

**94 should land after 80, or 80's author should be told the three carriers are coming.** 94 does
not block on it — the carriers are additive under either shape — but the arm count and the
`bindings.rs:93-176` churn differ. 80 also owns `bindings.rs` (`response_to_lua` `:82-87`,
`wire_response_to_lua` `:93-176`).

**Rev 2 upgrades this from a merge note to a design conversation.** `bindings.rs` is no longer
"three forced arms" for 94 — under §F it is a **co-primary** edit: either its signature changes
(F-1) or its input is pre-normalized (F-2), and either way `response_to_lua` `:82-87` — a function
80 is folding — is on the path. 80 and 94 must agree on `wire_response_to_lua`'s signature before
either sweeps. If 80 lands first, 94's §F applies to the folded type; if 94 lands first, 80 inherits
a renderer that takes a `ProtocolVersion`. **Neither can be authored in ignorance of the other**,
which was not true in rev 1.

### vs proposal 88 (`served-wake-effects`) — **shared file, regions verified disjoint**

88 owns `core/src/shard/blocking.rs` at `:234`, `:271`, `:327-388`, `:426`, `:754-840`,
`:1089-1185`. 94/H2 touches `:934`, `:952`, `:984`, `:1021`, `:1198-1204` — between 88's `BLMove`
arm (ends `:840`) and its `StreamSatisfaction` arm (starts `:1089`) for three of them, and below all
of them for the deletion. **No overlap.** 88 is about *effects* of a satisfaction; 94/H2 is about
the *reply bytes* of one. Both can land in either order.

### Compat risk — the honest statement

Reply shape is wire-visible, and 27 branches are being deleted. The claim is **not** "this is
safe"; it is "this is *provably* wire-invariant except for a listed, tested set of rows, and the
proof is a golden that lands first."

- The 12 pure folds are byte-identical by construction — each RESP2 arm is the downgrader arm's
  literal expression (§Problem 2a).
- The 15 pairing/policy sites move a decision without changing its outcome: `Pairing` and
  `Score`'s finiteness gate reproduce today's behavior exactly, including the three asymmetries,
  which are **preserved here and fixed elsewhere** — A1 and A3 ship as separately filed compat
  defects (§Problem 3, §Proposed change D).
- The only intended behavior change is **H2** — two golden rows on the waiter path, both currently
  wrong relative to the immediate path, both with a failing test written before the fix.
- **The largest residual risk is not on the wire at all.** It is `redis.call`'s Lua shape
  (§Problem 6): unpinned, unnoticed by rev 1, and breaking for every RESP2 client running a script
  if the fold ships without §F. It is retired by the Lua golden in §Testability step 2, which must
  land before the fold — not by any amount of wire-golden green.
- Wire-side residual risk concentrates on the commands with **no shape pin today**:
  SUNION/SINTER/SDIFF, ZUNION/ZDIFF, ZMSCORE, ZRANGE, HRANDFIELD (ZINTER and ZRANDMEMBER *are*
  pinned — `zset_tcl.rs:2880`, `:3146`). This is exactly why the golden covers all of them before a
  line changes: the untested surface is enumerated in §Testability and is pinned in step 1, not
  discovered in step 3.

### Spec / gates clearance

- **No locked crate edited.** `frogdb-protocol`, `frogdb-commands`, `frogdb-core` are none of txn /
  persistence / replication / cluster.
- **`FM-` tags:** zero in `crates/commands/src` (whole tree), zero in `protocol/src/response.rs`,
  zero in `core/src/pubsub.rs`. `core/src/shard/blocking.rs` has one — `FM-CLUSTER-038` at `:2065`,
  in the test module, 844 lines below 94's lowest edit and 860 below H2's.
  `just lint-failure-modes` is unaffected.
- **`lint-no-typed-unwrap`** (`Justfile:1012-1040`) — the only gate that greps
  `crates/commands/src/`. It looks for `as_*_mut().unwrap()` and hand-rolled `WrongType` chains; 94
  removes reply-construction branches and introduces neither pattern.
- **`lint-pubsub-confirmation-seam`** (`:1128-1156`) — 94 does not touch
  `pubsub_conn_command.rs`'s labels and adds no `*-1` literal. It edits the *inside* of
  `PubSubConfirmation::to_response` (`pubsub.rs:363`), which the gate does not inspect and whose
  single-ownership property 94 strengthens.
- **`lint-format-float`** (`:1249-1269`) — passes today **and misses §Problem 5**. H3 closes it.
- **`lint-gates`** (`:329`) gains one name.

---

## Effort — **M, top of band**, with three independently landable hotfixes

**M (top of band)**, decomposed: ~60 lines added to `protocol/src/response.rs` (3 variants × 2
enums, arms in 4 methods), ~130 deleted from `commands/src/utils.rs`, 168 deleted with `reply.rs`,
27 branch sites edited across 9 handler files (each 3-12 lines), 2 `MapReply` call sites, 2 in
`core/src/pubsub.rs`, **plus §F's rework of `wire_response_to_lua` (an 84-line match, every arm
protocol-aware under F-1) and a second golden harness**.

Rev 1 rated this mid-**M** on the assumption that `bindings.rs` was three mechanical arms. It is a
design surface with its own contract, its own test suite (which must be written from nothing), and
its own concurrent owner (proposal 80). That is what moves it to the top of the band.

Still not **L**: no crate-graph change, no signature change to `into_wire` / `to_resp2_frame` /
`to_resp3_frame` / `narrow_to_wire`, no trait added, no locked crate — and §F's signature change is
confined to two private functions in one module. Not **S**: two golden harnesses (~35 commands × 2
protocols on the wire, ~7 shape probes × 2 protocols in Lua) must land first, and three concurrent
proposals hold the primary file.

### H1 — delete two dead functions (**S**, standalone, zero risk)

`commands/src/utils.rs:898-912` — `format_scored_response` `:898-904` (whose own doc says
*"Deprecated: Use `scored_array` instead"*) and `pop_response` `:906-912`. **Zero call sites** in
`frogdb-server`, `frogctl`, `testing`, verified with `grep -rnw`. They survive only because `utils`
is `pub mod` (`commands/src/lib.rs:58`), which suppresses `dead_code`. **15 lines** — the range
starts at `:898`, not `:895`: `:895-896` are `scored_array_resp3`'s `}` `}` and deleting them does
not compile. No test change, no wire effect. Lands today, independent of everything above and of
proposals 80/84/86/88/90.

### H2 — fold `zset_score_reply` into the one score renderer (**S**, standalone, **fixes a live defect**)

`core/src/shard/blocking.rs:1198-1204` deleted; `:952`, `:984`, `:1021` call the same renderer as
`commands/src/blocking.rs` (`:512`, `:597`, `:725`), and `let is_resp3 = …` `:934` dies with them.
Requires two new regression rows in `zset_tcl.rs` next to `tcl_bzpopmin_bzpopmax_blocking_resp3`
`:3024` — `+inf` (RESP3 wire type) and `-0.0` (RESP2 text) — written **before** the fix, failing.
(A `1e17` row is worth a third; the `NaN` row of §Problem 5 is unreachable today — `ZADD`/`ZINCRBY`
reject it, `resp3.rs:808` — and is left to the fold, which makes it impossible rather than merely
unreached.) ~8 production lines, ~40 test lines. Independent of the fold;
the fold subsequently deletes the call sites again, which is why H2 should land first rather than be
absorbed.

Pre-fold, the shared renderer is `commands/src/utils::score_response`, which lives in a crate
`frogdb-core` does not depend on. Two options, both S: move `score_response` into
`frogdb-protocol` next to `format_float` (`format.rs`) — the natural home, and where `Response::Score`
puts it permanently anyway — or have `core` call `format_float` directly with the `is_finite` gate
inline. **Prefer the move**: it is the first step of the fold, done as a defect fix.

### H3 — give `lint-format-float` its missing half (**S**, standalone)

`Justfile:1249-1269` currently gates *definitions* of `format_float`. Add the call-site half: no
`f64` rendered via `to_string()` / `format!("{}", …)` on a reply path outside `format.rs`. This is
the gate that would have caught H2's divergence at its introducing commit, and the gate's own
comment (`:1245-1248` — "Five copies had drifted") already argues for it. Lands independently; H2's
diff is what makes it pass.
