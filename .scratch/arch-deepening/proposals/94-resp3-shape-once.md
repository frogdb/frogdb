# Proposal 94 — RESP2/RESP3 reply shape is decided twice: once in 27 handler branches, once in a downgrader that already knows the answer

Round 38 · lane: commands + types · effort **M** · **no locked crate edited**, **zero `FM-` tags in
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
| "Dead code: `format_scored_response`/`pop_response` (`utils.rs:799-928`)" | **Confirmed for the two named functions, wrong for the range.** `grep -rnw` across `frogdb-server`, `frogctl`, `testing`: `format_scored_response` and `pop_response` have **zero call sites outside their own definitions** (`utils.rs:902`, `:910`). The cited range `:799-928` is mostly *live* — it also contains `score_response` (7 callers), `scored_array` (9), `scored_array_resp3` (7), `scored_array_with_scores_resp3` (2), `members_array` (2). The dead region is exactly **`:895-912`** (18 lines, two fns with their doc comments). **Hotfix H1.** |
| "XINFO? CLIENT INFO?" as downgrader gaps | **XINFO: a different defect, not a gap.** `stream/info.rs` has **zero** `is_resp3` and returns flat `Response::Array` in *both* protocols (`:207`, `:238`, `:281`, `:320`) — it never grew the RESP3 Map shape at all. Not a branch to fold; a one-line beneficiary of the fold. **CLIENT INFO: refuted.** `client_info` (`client_conn_command.rs:341-349`) returns a single `Response::bulk` in both protocols, correctly — no shape branch exists. |
| Rated "latent" | **One live defect found, in the sibling copy the brief did not look at.** `core/src/shard/blocking.rs:1198-1205`'s `zset_score_reply` is a second, *divergent* implementation of `commands/src/utils.rs:799`'s `score_response`. A `BZPOPMIN` served **immediately** and the same `BZPOPMIN` served **by the waiter** put different bytes — and for `+inf`, a different RESP3 *type* — on the wire for the same score. §Problem 5. **Hotfix H2.** |

Three findings the brief did not name, all verified at HEAD: the **live immediate-vs-blocked score
divergence** (§Problem 5), **three in-tree shape asymmetries between sibling commands** that exist
only because each handler decides independently (§Problem 3), and the fact that the tree **already
has this seam, for one command family, with a gate enforcing it** — `lint-pubsub-confirmation-seam`
(`Justfile:1128-1156`) — which makes this proposal a generalization of an accepted local ruling
rather than a new idea (§Proposed change, precedent).

## Summary

A RESP2 reply and a RESP3 reply are two renderings of one value. FrogDB owns a **downgrader** that
says so: `WireResponse::to_resp2_frame` (`protocol/src/response.rs:274-333`) maps `Map` → flat
array, `Set` → array, `Push` → array, `Double` → `format_float` bulk, `Attribute` → inner, `BigNumber`
→ bulk. Seventeen command sites in `frogdb-server` already trust it and emit the RESP3 shape
unconditionally (`debug_conn_command.rs` ×17, `client_conn_command.rs:808`).

The `frogdb-commands` handlers do not. Twenty-seven sites across nine files ask
`ctx.protocol_version.is_resp3()` and hand-build both shapes. Twelve of those produce, in the RESP2
arm, **byte-for-byte what the downgrader would have produced from the RESP3 arm** — pure
duplication. The other fifteen exist because two facts about a reply are **not representable in
`Response` at all**, so the handler is the only place they can be spent:

- **Pairing** — `[m, s, m, s]` and `[[m, s], [m, s]]` are both `Response::Array`. Once the handler
  has built one, the shape choice is *gone*; the downgrader sees an array of arrays and has no
  basis to flatten it (`ZMPOP` is nested in **both** protocols — `pop.rs:301-302`).
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

## Files involved

Line counts at `8a170652`.

| Path | Lines | Role in this change |
|---|---:|---|
| `frogdb-server/crates/protocol/src/response.rs` | 1770 (941 code + 829 tests, 45 `#[test]`) | **Primary (the interface).** `Response` `:647-743` and `WireResponse` `:116-175` each gain 3 carrier variants; `into_wire` `:770-836`, `from_wire` `:838-880`, `to_resp2_frame` `:274-333` and `to_resp3_frame` `:339-433` each gain 3 arms (~60 lines added). **Concurrently owned by 80 (primary), 84 (deletes `:473-550`), 86 (doc-only `:195-199`, `:267-273`, `:323-332`)** — §Risks. |
| `frogdb-server/crates/protocol/src/reply.rs` | 168 | **Deleted.** `MapReply` `:23-92` + its 4 tests `:94-166`. Its rule is `to_resp2_frame`'s `Map` arm (`response.rs:297-306`), already. |
| `frogdb-server/crates/protocol/src/lib.rs` | 27 | `mod reply;` + `pub use reply::MapReply;` `:19` removed. |
| `frogdb-server/crates/commands/src/utils.rs` | 1241 | **Primary.** `score_response` `:791-805`, `scored_array` `:807-841`, `scored_array_with_scores_resp3` `:843-869`, `scored_array_resp3` `:871-893`, `format_scored_response` `:895-904`, `pop_response` `:906-912`, `members_array` `:914-920` — **~130 lines deleted**, replaced by nothing (call sites construct the carrier directly). `:895-912` is **H1**, independently landable. Zero `FM-` tags. |
| `frogdb-server/crates/commands/src/set.rs` | 1127 | **7 decisions deleted** (`:164`, `:171`, `:355`, `:390`, `:418`, `:453`, `:480`) — all `Set`-vs-`Array`, all byte-identical folds. Largest single-file win. |
| `frogdb-server/crates/commands/src/hash.rs` | 2327 | **3 decisions**: `:385`, `:403` (`Map`-vs-flat, byte-identical fold); `:853` (`HRANDFIELD … WITHVALUES`, the `Pairs` carrier). |
| `frogdb-server/crates/commands/src/string.rs` | 1805 | **2 decisions** (`:769`, `:781`, `INCRBYFLOAT`) + the now-dead `let is_resp3` `:764`. Byte-identical fold. |
| `frogdb-server/crates/commands/src/sorted_set/basic.rs` | 416 | **3 decisions**: `:272` `ZSCORE`, `:315` `ZMSCORE` (both → `Score`), `:410` `ZINCRBY` (byte-identical `Double` fold). Also the **read-only** evidence for asymmetry A3: `ZADD … INCR` `:131` never asks the version at all. |
| `frogdb-server/crates/commands/src/sorted_set/pop.rs` | 358 | **5 decisions** (`:73`, `:76`, `:140`, `:143`, `:351`) — the densest pairing cluster: `ZPOPMIN`/`ZPOPMAX` are nested **iff RESP3 and an explicit count**, `ZMPOP` `:273` is nested in **both**. Three `Pairing` values are live in one 358-line file. |
| `frogdb-server/crates/commands/src/sorted_set/range.rs` | 400 | **1 decision** (`:125`, `ZRANGE`). Also read-only evidence for asymmetry **A1**: `:185`, `:234`, `:291` (`ZRANGEBYSCORE`, `ZREVRANGE`, `ZREVRANGEBYSCORE`) call `scored_array` unconditionally — flat in RESP3 where `ZRANGE` is nested. |
| `frogdb-server/crates/commands/src/sorted_set/set_ops.rs` | 802 | **3 decisions** (`:197`, `:389`, `:696` — `ZUNION`/`ZINTER`/`ZDIFF`). |
| `frogdb-server/crates/commands/src/blocking.rs` | 905 | **3 decisions** (`:512`, `:597`, `:725` → `Score`). **Read-only evidence for 84** (`:12`, `:230`, `:231`, `:365`) — 84 does not edit this file, §Risks. |
| `frogdb-server/crates/core/src/shard/blocking.rs` | 2090 | **Primary, 4 lines + a deletion.** `zset_score_reply` `:1198-1205` **deleted**; its 3 call sites `:952`, `:984`, `:1021` become `Response::Score(score)`; `let is_resp3 = …` `:934` dies with them. This is **H2** and is independently landable. Its one `FM-CLUSTER-038` tag `:2065` is in the test module, 1000+ lines below the lowest edit. **Concurrently owned by 84 (`:13`, `:1098`, `:1579`) and 88 (`:234`, `:271`, `:327-388`, `:426`, `:754-840`, `:1089-1185`)** — §Risks. |
| `frogdb-server/crates/core/src/pubsub.rs` | 1523 | **2 decisions** (`:363`, `:456` — `Push`-vs-`Array`). Byte-identical fold (`response.rs:313-316`). Note this is the *inside* of the seam that `lint-pubsub-confirmation-seam` already enforces: the gate is satisfied either way, and the fold removes the last branch from the seam's own body. |
| `frogdb-server/crates/core/src/scripting/bindings.rs` | 420 | **Forced churn, 3 arms.** `wire_response_to_lua` `:93-176` matches `WireResponse` exhaustively; 3 new variants = 3 new arms. **Concurrently owned by 80** — §Risks. |
| `frogdb-server/crates/core/src/scripting/executor.rs` | 1215 | **Read-only, explicitly out of scope.** 4 `is_resp3` sites `:371`, `:378`, `:386`, `:402` — these convert *Lua tables* to replies, where the RESP2/RESP3 difference is Lua-surface semantics, not one value in two renderings. §Proposed change (scope). |
| `frogdb-server/crates/server/src/connection/guards.rs` | — | **Read-only, explicitly out of scope.** `:182` (command *gating* in subscribe mode) and `:459` (`PING` in subscribe mode returns `+PONG` in RESP3 vs a 2-element array in RESP2) are **semantic**, not shape: different data, not one value in two renderings. Named so the classification is complete. |
| `frogdb-server/crates/server/src/connection/hotkeys.rs` | 557 | `MapReply::with_capacity(ctx.protocol_version, 6)` `:445` → `Response::Map(…)`; the comment at `:436` updates. |
| `frogdb-server/crates/server/src/connection/auth_conn_command.rs` | 672 | `MapReply::with_capacity(version, 7)` `:453` → `Response::Map(…)`; doc `:448`. The `let proto = if version.is_resp3() { 3 } else { 2 }` at `:451` **stays** — it is HELLO's *payload*, a reported number, not a shape. |
| `frogdb-server/crates/commands/src/stream/info.rs` | 326 | **Beneficiary, optional.** `:207`, `:238`, `:281`, `:320` — flat `Array` in both protocols today. §Proposed change (item D). |
| `frogdb-server/crates/server/tests/resp3.rs` | 926 | **The net.** Existing pins that constrain the fold: `:253` / `:293` (HGETALL both protocols), `:331` (SMEMBERS RESP3), `:370` (ZSCORE Double), `:401` (INCRBYFLOAT Double), `:775`-`:930` (the wire-byte block: ZINCRBY `+inf` → `,inf\r\n`; ZSCORE `+inf` → `$3\r\ninf\r\n`; ZADD INCR always bulk). Gains the shape-golden harness. |
| `frogdb-server/crates/redis-regression/tests/zset_regression.rs` | 286 | **The net.** `:20`, `:51`, `:82`, `:111` pin `ZPOPMIN`/`ZPOPMAX` flat-vs-nested by count in RESP3 — the four tests that make `Pairing::NestedInResp3WithCount` a *pinned* requirement rather than a guess. `:241` pins `e+308` score rendering. |
| `frogdb-server/crates/redis-regression/tests/zset_tcl.rs` | — | **The net + the hole.** `tcl_bzpopmin_bzpopmax_readraw_resp3` `:2994` (immediate path) and `tcl_bzpopmin_bzpopmax_blocking_resp3` `:3021` (waiter path) both assert `resp3_double(&arr[2])` — but only for **finite** scores (`1.0`, `2.0`, `3.25`), which is exactly why H2's divergence is invisible today. |
| `frogdb-server/crates/protocol/src/format.rs` | 152 | **Read-only evidence.** `format_float` `:39-77`; the pins that make H2's divergence provable: `:94` `format_float(-0.0) == "0"`, `:117` `format_float(1e17) == "1e+17"`. |
| `Justfile` | — | **Primary, ~25 lines.** `lint-pubsub-confirmation-seam` `:1128-1156` is the **precedent** (read-only). `lint-format-float` `:1249-1269` gains the call-site half (**H3**). `lint-gates` `:329` gains one name. |

Read-only, not edited: `frogdb-server/crates/server/src/connection/debug_conn_command.rs`
(17 unconditional `Response::Map(` sites — the in-tree proof the downgrader is trusted),
`frogdb-server/crates/server/src/connection/client_conn_command.rs:341-349` (CLIENT INFO, refuting a
brief claim) and `:808` (CLIENT TRACKINGINFO, an 18th unconditional `Map`),
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
(`pop.rs:273`, comment `:301-302`) and `BZMPOP` (`blocking.rs:722-726`) are nested in *both*
protocols. This is the real gap, and it is a **missing fact in the type**, not a missing rule in the
downgrader.

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

### 3. Three asymmetries that exist only because 27 sites decide independently

All three are in-tree facts, verifiable without reference to Redis:

- **A1 — `ZRANGE` vs its four legacy siblings.** `ZRANGE … WITHSCORES` is nested in RESP3
  (`range.rs:121-129`). `ZRANGEBYSCORE` `:185`, `ZREVRANGE` `:234`, `ZREVRANGEBYSCORE` `:291` call
  `scored_array` unconditionally — **flat in RESP3**. Same file, same helper module, four commands
  that return the same value, two answers.
- **A2 — `ZSCORE` vs `ZINCRBY` on non-finite.** `ZSCORE` routes `+inf` to a bulk string in RESP3
  (`basic.rs:272` → the `is_finite` gate; pinned `resp3.rs:858`). `ZINCRBY` `:410` has **no** gate
  and emits `Double(inf)` → `,inf\r\n` (pinned `resp3.rs:775`). Two commands, one score type, two
  RESP3 wire types.
- **A3 — `ZADD … INCR` vs `ZINCRBY`.** `basic.rs:131` returns `Response::bulk(format_float(...))`
  unconditionally and never reads `ctx.protocol_version` — so `ZADD k INCR 3 m` is `$1\r\n3\r\n` in
  RESP3 while `ZINCRBY k 3 m` is `,3\r\n`. The test that pins it says so in its own doc comment
  (`resp3.rs:889-894`: "flagged … as a RESP3-consistency gap relative to ZINCRBY, out of scope for
  this encoder/dispatch-change-free pin task"). This proposal **is** that dispatch change.

None of these is a bug this proposal claims to have found — all three are pinned as current
behavior. The point is structural: they are the failure mode of a decision with 27 authorities, and
under one authority **they are not expressible**. A2 and A3 collapse to one rule the moment
`Response::Score` exists; A1 collapses the moment `Pairing` is a field rather than a helper choice.

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

`core/src/shard/blocking.rs:1198-1205`:

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

(The last two verified by compiling and running a two-branch comparison on the pinned toolchain;
`format.rs:94` and `:117` pin the left column independently.)

So `BZPOPMIN k 0` on a key that **already has** a `+inf` member returns a bulk string in RESP3, and
the *same command* on an empty key that receives a `+inf` member a millisecond later returns a
`Double`. The client sees the wire type of a score depend on whether it had to block.

Why the test suite misses it: `tcl_bzpopmin_bzpopmax_readraw_resp3` (`zset_tcl.rs:2994`) and
`tcl_bzpopmin_bzpopmax_blocking_resp3` (`:3021`) cover **both** paths — with scores `1.0`, `2.0`,
`3.25`. Every value in the disagreement set is non-finite, negative zero, or ≥ 1e17.

Why the gate misses it: `lint-format-float` (`Justfile:1249-1269`) greps for `fn format_float`
**definitions** outside `protocol/src/format.rs`. It enforces *one definition*; it does not enforce
*one call path*. `score.to_string()` is not a definition, so the gate passes while a second float
renderer runs on a live reply path. The gate's own comment (`Justfile:1245-1248`) names exactly this
hazard — "Five copies had drifted" — and the sixth is sitting inside its blind spot.

---

## Proposed change

**One module decides reply shape: `frogdb-protocol`'s two encoders.** Handlers state the value.

### The precedent this generalizes

`lint-pubsub-confirmation-seam` (`Justfile:1128-1156`) already rules, for one command family, that
*"`PubSubConfirmation::to_response(protocol)` … is the single owner of the RESP3 Push vs RESP2 Array
confirmation shape"*, and fails the build on a hand-built confirmation label in the pub/sub handlers.
The ruling is accepted; only its **scope** is one family. This proposal applies the same ruling to
the rest of the reply surface and — because the downgrader can then do the work — removes the branch
from `PubSubConfirmation::to_response`'s own body too (`pubsub.rs:363`).

### (A) Three carrier variants, on both `Response` and `WireResponse`

```rust
/// A sorted-set score. RESP3: Double when finite, bulk `format_float` otherwise
/// (deliberate divergence — see resp3.rs:855). RESP2: always bulk `format_float`.
Score(f64),

/// Members with optional scores. RESP2 is always flat with bulk scores.
ScoredMembers { members: Vec<(Bytes, f64)>, with_scores: bool, pairing: Pairing },

/// Field/value pairs. RESP2 flat; RESP3 per `pairing`.
Pairs { pairs: Vec<(Bytes, Bytes)>, pairing: Pairing },

pub enum Pairing { Flat, NestedInResp3, NestedAlways }
```

`Pairing` has exactly three inhabitants because the tree exhibits exactly three behaviors, all in
`pop.rs`: `ZPOPMIN` without count is `Flat` (`:76`), with count is `NestedInResp3` (`:73`), `ZMPOP`
is `NestedAlways` (`:273`, comment `:301-302`).

**They live on `WireResponse` too, and resolve in the encoders — not in `into_wire`.** Reason:
`into_wire()` takes no `ProtocolVersion` and three things depend on that signature —
`From<Response> for BytesFrame` (`response.rs:463-467`), `try_to_resp2_frame`/`try_to_resp3_frame`
(`:920`, `:928`), and proposal 80's fold, which owns `narrow_to_wire` (`frame_io.rs:31-53`).
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

- **`scripting/executor.rs:371-402`** (4 sites). Lua-table → reply conversion, where RESP2/RESP3
  differ in *Lua surface semantics* (which table key the script must use), not in the rendering of
  one value. Different problem, different owner.
- **`guards.rs:182`** — command *gating* in subscribe mode. Not a reply at all.
- **`guards.rs:459`** — `PING` in subscribe mode: `+PONG` in RESP3 vs `["pong", ""]` in RESP2. This
  is **different data**, not one value in two shapes. Genuinely unfoldable, and named here so the
  27-vs-31 arithmetic is closed.
- **`auth_conn_command.rs:451`** — `let proto = if version.is_resp3() { 3 } else { 2 }` is HELLO's
  *payload*. Reporting the protocol number is not choosing a shape.
- **`stream/info.rs`** — `XINFO`'s missing RESP3 Map is a *beneficiary*, listed as optional item D
  and deliberately not bundled: it is a wire-visible **addition**, and this proposal's value
  proposition is that everything else is wire-invisible. It should be its own change, on top.
- **`ZADD … INCR` (A3) and `ZRANGEBYSCORE` et al. (A1)** — the fold makes them one-line fixes but
  does **not** apply them. Same reason: they are the only wire-visible rows, and mixing them into a
  27-site refactor destroys the "golden is byte-identical" acceptance criterion. File as follow-ups
  with the golden rows they would flip already identified.

### (E) The gate

`lint-resp3-shape-once`, added to `lint-gates` (`Justfile:329`) — the compile-free family that runs
on every commit:

1. `grep -rn 'is_resp3' frogdb-server/crates/commands/src/` must be **empty**. After the fold there
   is no legitimate reason for a command handler to know the protocol version; the seam is in
   `frogdb-protocol`.
2. `lint-format-float` (`Justfile:1249-1269`) gains its missing half: no `to_string()` /
   `format!("{}", …)` applied to an `f64` on a reply path outside `format.rs`. This is **H3** and is
   what would have caught §Problem 5 at the commit that introduced it.

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

**Golden-first plan (required — the change is wire-visible by construction, even where it is
wire-*invariant* in fact).** The regression suite pins RESP2/RESP3 shape for a *subset*: HGETALL
both protocols (`resp3.rs:253`, `:293`), SMEMBERS RESP3 (`:331`), ZSCORE/INCRBYFLOAT Double (`:370`,
`:401`), the ZSCORE/ZINCRBY/ZADD-INCR wire-byte block (`:775-930`), ZPOPMIN/ZPOPMAX flat-vs-nested
(`zset_regression.rs:20`, `:51`, `:82`, `:111`), BZPOP RESP3 doubles (`zset_tcl.rs:2994`, `:3021`).
**Not pinned anywhere:** SUNION/SINTER/SDIFF RESP3 `Set` shape, ZUNION/ZINTER/ZDIFF pairing,
ZRANDMEMBER pairing, HRANDFIELD WITHVALUES pairing, ZMSCORE score type, ZRANGE RESP3 pairing, and
every non-finite score on the waiter path. That last hole is what let §Problem 5 live.

So, in order:

1. **Land the golden first, against unmodified code.** A table-driven harness in
   `server/tests/resp3.rs`: `(setup commands, command, protocol)` → raw wire bytes, one file
   `resp3_shape_golden.txt`. Rows: every command named in §Problem 2 (a) and (b), each in RESP2 and
   RESP3, each with a finite score, `+inf`, `-inf`, `-0.0`, `1e17`, and empty/missing-key. ~35
   commands × 2 protocols. Same pattern proposal 90 uses for `CommandSpec` — golden lands *before*
   the sweep, on the pre-change tree, so it records the truth rather than the intent.
2. **Add the two missing waiter-path rows** (BZPOPMIN blocked, `+inf` and `-0.0`, both protocols).
   They **fail immediately** — this is the H2 regression test, and it is the reason H2 is filed as a
   defect fix rather than as cleanup.
3. **H2, then H1, then the fold.** After the fold the golden must be byte-identical **except** the
   two rows from step 2, which flip to agree with the immediate path.
4. **The gate** (§E) lands last, once the grep is empty.

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
**bracket** `to_resp2_frame` (`:274-333`), whose match block 94 extends by three arms. A three-arm
insertion before `:333` shifts 86's `:323-332` block. Mitigation: land 86's four-line doc hotfix
first (it is H4 there, independently landable and already scoped as a hotfix), or rebase 94's
insertion point. Either order works; the two edits cannot produce a semantic conflict because
neither changes what the other's lines mean.

One genuine **sequencing** note, not a conflict: 86 adds a `HELLO 3`-mid-pipeline test to
`server/tests/resp3.rs`, and 94 adds the shape-golden harness to the same file. Different regions,
same file, ordinary merge.

### vs proposal 84 (`blocking-op-dedupe`, rev 2) — **two shared files, disjoint regions, no ordering constraint**

- `protocol/src/response.rs`: 84 **deletes** `Direction` `:473-493` and `BlockingOp` `:495-550` to a
  new `protocol/src/blocking.rs`. 94 touches `:116-175`, `:274-333`, `:339-433`, `:647-743`,
  `:770-880`. **No line in common**, and 84 explicitly keeps `Response::BlockingNeeded` `:709-716`
  verbatim, which is inside 94's enum region but not on a line 94 edits (94 appends variants).
  84-first shifts 94's `:647-743` up by 78; 94-first shifts nothing 84 deletes. Either order.
- `core/src/shard/blocking.rs`: 84 edits `:13`, `:1098`, `:1579`. 94 edits `:934`, `:952`, `:984`,
  `:1021`, and deletes `:1198-1205`. **Disjoint.** 84 lists `commands/src/blocking.rs` as
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
`wire_response_to_lua` `:93-176`), which is exactly where 94's three forced arms go.

### vs proposal 88 (`served-wake-effects`) — **shared file, regions verified disjoint**

88 owns `core/src/shard/blocking.rs` at `:234`, `:271`, `:327-388`, `:426`, `:754-840`,
`:1089-1185`. 94/H2 touches `:934`, `:952`, `:984`, `:1021`, `:1198-1205` — between 88's `BLMove`
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
  which are **preserved, not fixed** (§Proposed change D).
- The only intended behavior change is **H2** — two golden rows on the waiter path, both currently
  wrong relative to the immediate path, both with a failing test written before the fix.
- Residual risk concentrates on the commands with **no shape pin today** (SUNION/SINTER/SDIFF,
  ZUNION/ZINTER/ZDIFF, ZRANDMEMBER, HRANDFIELD, ZMSCORE, ZRANGE). This is exactly why the golden
  covers all of them before a line changes: the untested surface is enumerated in §Testability and
  is pinned in step 1, not discovered in step 3.

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

## Effort — **M**, with three independently landable hotfixes

**M**, decomposed: ~60 lines added to `protocol/src/response.rs` (3 variants × 2 enums, arms in 4
methods), ~130 deleted from `commands/src/utils.rs`, 168 deleted with `reply.rs`, 27 branch sites
edited across 9 handler files (each 3-12 lines), 3 arms in `bindings.rs`, 2 `MapReply` call sites,
2 in `core/src/pubsub.rs`. Not L: no crate-graph change, no signature change to `into_wire` /
`to_resp2_frame` / `to_resp3_frame` / `narrow_to_wire`, no trait added, no locked crate. Not S: the
golden harness (~35 commands × 2 protocols) is real work and must land first, and three concurrent
proposals hold the primary file.

### H1 — delete two dead functions (**S**, standalone, zero risk)

`commands/src/utils.rs:895-912` — `format_scored_response` (whose own doc says *"Deprecated: Use
`scored_array` instead"*) and `pop_response`. **Zero call sites** in `frogdb-server`, `frogctl`,
`testing`, verified with `grep -rnw`. They survive only because `utils` is `pub mod`
(`commands/src/lib.rs:58`), which suppresses `dead_code`. 18 lines, no test change, no wire effect.
Lands today, independent of everything above and of proposals 80/84/86/88/90.

### H2 — fold `zset_score_reply` into the one score renderer (**S**, standalone, **fixes a live defect**)

`core/src/shard/blocking.rs:1198-1205` deleted; `:952`, `:984`, `:1021` call the same renderer as
`commands/src/blocking.rs`. Requires two new regression rows in `zset_tcl.rs` next to
`tcl_bzpopmin_bzpopmax_blocking_resp3` `:3021` — `+inf` (RESP3 wire type) and `-0.0` (RESP2 text) —
written **before** the fix, failing. ~8 production lines, ~40 test lines. Independent of the fold;
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
