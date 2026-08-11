# Proposal 93 — Hash field TTL lives in two books: 17 hand-paired writes, 13 hand-placed purges, three disagreeing readers — and three commands that write only one book

Round 38 · lane: commands + types · candidate **CT4** · effort **M** · **no locked crate
edited**, **no seam lint constrains the change** (§Spec / gates).

**`FM-` tags in edited files — corrected (rev 2).** `commands/src/hash.rs` and
`types/src/types/hash.rs` are clean (`grep -n 'FM-'` → nothing). **`core/src/store/hashmap.rs`
— this proposal's Primary file — is not**: it carries three `FM-PERSISTENCE-044` tags at
`:2608`, `:2638`, `:2666`, on the tests
`persist_on_expired_key_deletes_instead_of_immortalizing`,
`persist_on_expired_key_leaves_no_expiry_index_orphan` and
`nondestructive_probes_do_not_see_a_past_deadline_key`, named as the forcing tests of the spec
row at `.scratch/hardening/specs/persistence-failure-modes.md:626`. That row is **key-level**
expiry (`persist`, `check_and_delete_expired`, `ExpiryIndex`); this proposal edits none of the
three tests, renames none of them, and touches none of the code they force. **`just
lint-failure-modes` stays green** — but the earlier blanket "zero `FM-` tags in any edited
file" claim was wrong and is withdrawn, and any implementer must keep those three tests
byte-identical or update the spec row first.

**Verified at HEAD `175a997d5f1c7f4ca4ecfa6decb2d3ad361e9fd1`** (worktree `arch-round-38-99`,
branch `worktree-arch-round-38-99`). Every code cite was derived at `25d38736`; the two commits
since (`d2155ce6`, `175a997d`) touch only `.scratch/arch-deepening/proposals/*.md`, so no code
line number moved. Nothing below is inherited from the lane brief — the brief's headline counts
are corrected in the next section.

> **Rev 2 (post-adversarial-review).** Seven amendments, each re-verified against the tree
> before it was applied:
> **(1)** HINCRBY/HINCRBYFLOAT re-ruled — they are the *opposite* defect (Redis keeps the field
> TTL across an increment; FrogDB destroys it), so their hotfix inverts and two proposed
> regression tests are withdrawn as would-be-golden-wrong (§2a, §Effort H1b).
> **(2)** Design (c) had a **routing hole** — the ghost-producing commands reach
> `get_or_create_typed`, not the family accessors the draft edited, so the advertised behaviour
> fixes would not have landed; the purge moves to the generic layer behind a `ValueType`
> const (§(c)).
> **(3)** Warm-tier carve-out — I1 is scoped to hot entries and the reconcile early-returns on
> warm ones, or it deletes a spilled hash's real deadlines; plus §5a, a pre-existing
> ghost-free variant of the sweep starvation.
> **(4)** The "zero `FM-` tags in any edited file" claim was false and is withdrawn.
> **(5)** §5's starvation is quantified against `DEFAULT_BATCH_SIZE = 1024`.
> **(6)** "Immortal" softened to "unreachable by any background mechanism"; several counts and
> two gate claims corrected (§Spec / gates); the 83 conflict upgraded textual → **semantic**.
> **(7)** Prior art (issue 82 §F3) cited and superseded; the I1 audit moved into the
> established `assert_consistent` home; the reconcile's staleness window given an explicit
> safety argument.
> The core thesis is unchanged: the index is a derived structure, install/uninstall already
> model the seam, and the fold direction is forced by durability.

**Ruling on the brief's "latent (no verified live bug)" framing: refuted. This is LIVE.**
Three shipped commands — **HMSET**, **HINCRBY**, **HINCRBYFLOAT** — clear the value-side field
TTL and never touch the index, leaving behind an index entry the value book does not back: a
*ghost*. The ghost is (a) returned to clients by `HTTL`, (b) used as `current_expiry` by
`HEXPIRE`'s NX/XX/GT/LT decision, (c) promoted back into the value book by `HSETEX … KEEPTTL`
so the field really does die, and (d) **unreachable by any background reaper**, where it can
starve the whole shard's field sweep.

**Rev 2 — the three commands do not share one defect; they share one mechanism and split into
two defects with opposite fixes.** For **HMSET** the value book is right and the index is a
stale leftover: Redis's HSET-family clears a field's TTL on overwrite, so the fix is to drop
the index entry. For **HINCRBY**/**HINCRBYFLOAT** it is the other way round — Redis
*preserves* the field TTL across an increment, so the index entry is **correct** and the value
book's clear is a **destroyed durable TTL**. §Problem 2 states both, and the hotfix in §Effort
is split accordingly (**H1a** for HMSET, **H1b** for the two increments). The
independently-landable HMSET hotfix and its regression test (four lines away from an existing
passing test) remain 9 lines.

## Corrections to the lane brief

| Brief claim | Verified at HEAD |
|---|---|
| "every mutation site writes TWO statements … at **~35 sites**" | **Adjusted down and made precise.** There are **17** explicit field-expiry write statements in production code — **6** value-side (`commands/src/hash.rs:1139`, `:1606`, `:1890`, `:1899`, `:2061`, `:2073`) and **11** index-side (`:87`, `:236`, `:1151`, `:1154`, `:1612`, `:1810`, `:1912`, `:1919`, `:2053`, `:2066`, `:2080`) — spread over **7** handler families. Add the **13** purge calls and the total field-expiry statement count in `commands/src/hash.rs` is **30**. Two further value-side writes are *implicit*, inside `frogdb-types` (`types/src/types/hash.rs:262` in `HashValue::set`, `:321` in `HashValue::remove`), and those two are where the live bug lives. |
| "13 hand-placed purge calls" | **Verified exactly.** `commands/src/hash.rs:173, 330, 381, 442, 483, 526, 570, 696, 748, 828, 1178, 1773, 1862` — 13, covering 16 commands (`:1178` is inside the shared `execute_httl_common`). A 14th call at `core/src/shard/active_expiry.rs:224` is the active sweep and is not a handler. |
| "reads paper over the split with `or_else` **fallbacks**" (plural) | **Adjusted — and the truth is worse.** There is exactly **one** `or_else`, at `:2030-2031`. The other three index reads (`:1033`, `:1195`, `:1576`) have **no fallback at all**: they read the index book *only* and never consult the value they are holding. So the readers do not merely paper over the split — they disagree about which book is authoritative. §Problem 3. |
| "hash.rs:2030-2031 is a KNOWN disagreement site" | **Confirmed as a site, re-ruled as an effect.** The `or_else` arm is **unreachable in a consistent system** (it fires only when the index holds a TTL the value does not) and, in the diverged system H1a fixes, it is the **data-loss amplifier**: it copies the ghost deadline back into the value book at `:2073`, after which the field genuinely expires. It is not itself the bug. §Problem 4. |
| "M-L effort" | **M.** The change is mechanical once the direction is fixed: 11 index writes and 4 index reads deleted, 13 purge calls deleted, 5 trait methods deleted, ~38 lines added — spread over five files after rev 2 (`typed.rs`, `mod.rs`, `hashmap.rs`, `noop.rs`, `types/hash.rs`). The size comes from the number of handler bodies touched (12), not from difficulty. §Effort. |

Two findings the brief did not name: **`Store::remove_all_field_expiries` has zero production
callers** (`grep -rn remove_all_field_expiries` returns only the trait default
`store/mod.rs:659` and the impl `store/hashmap.rs:1376`) — the `publish = false` dead-`pub`
pattern proposal 84 §Problem 3 documented, recurring; and **`purge_expired_hash_fields`
unconditionally calls `Arc::make_mut`** (`hashmap.rs:1415`) *before* discovering there is
nothing to purge, so all 13 current call sites pay a potential copy-on-write on every hash read.

## Summary

Hash field TTL is stored twice, in two structures with no relationship the compiler or the
store can see:

- **The value book** — `HashValue.field_expiries: Option<HashMap<Bytes, Instant>>`, with
  `set_field_expiry` / `remove_field_expiry` / `get_field_expiry` /
  `remove_expired_fields` at `types/src/types/hash.rs:518-576`. This is the **durable** one:
  it is what `frogdb-persistence` serializes (`serialization/registry.rs:85`, `:90-93`, keyed
  on `hash.has_field_expiries()`) and what `HashMapStore::install` re-derives the index from
  (`store/hashmap.rs:388-396`).
- **The index book** — `HashMapStore.field_expiry_index: FieldExpiryIndex`
  (`store/hashmap.rs:133`), a `BTreeMap<(Instant, key, field)>` + `HashMap<key, {field →
  Instant}>` pair (`core/src/noop.rs:169-174`) that exists so active expiry can scan due
  fields in deadline order. It is **derived**, not durable — nothing serializes it.

The store already knows this. `install`/`uninstall`/`resize` carry an eight-line banner
(`hashmap.rs:337-346`) declaring themselves *"the ONLY code permitted to reconcile the derived
side-structures … so the 'derived structures move atomically with the entry' invariant lives in
one place instead of being re-stated at nine call sites"*, and `uninstall:469-471` duly settles
`expiry_index`, `ts_labels` and `field_expiry_index` together.

**The `Store` trait then punches a hole straight through that banner.** `set_field_expiry`,
`remove_field_expiry`, `remove_all_field_expiries` (`store/mod.rs:646-661`) are *public write
access to a derived structure*, and `frogdb-commands` uses them 11 times. Meanwhile the fourth
mutation path — in-place edits through `get_mut` — has a deferred-reconcile queue
(`pending_keysizes_refreshes`, `hashmap.rs:199`, drained at `execution.rs:289` after every
command) that settles **sizes and histograms but not field expiries**. So every handler that
mutates a hash in place must hand-write the index update the store declines to derive, and
three handlers forget.

**The proposal, in one sentence: make the index book derived-by-construction — delete its
public write API, reconcile it from the value in the existing post-command flush, and move the
purge into the generic typed-access layer every hash caller already routes through (§(c)) — so
that "index entry exists iff the value-side TTL is
set" stops being a convention repeated at 17 sites and becomes a property of the one seam that
can violate it.**

Net effect: `commands/src/hash.rs` loses ~88 lines of "Phase 3: sync store index" scaffolding
and 13 purge calls; the `Store` **interface** loses five methods; `HashMapStore` gains ~24
lines of reconciliation; the three-phase immutable/mutable/index dance that HEXPIRE, HGETEX,
HSETEX, HPERSIST and HGETDEL are all written in collapses to one pass, because the borrow
gymnastics existed only to touch both books.

## Files involved

| Path | Lines | Role in this change |
|---|---|---|
| `frogdb-server/crates/commands/src/hash.rs` | 2327 | **Primary.** 11 index writes deleted (`:87`, `:236`, `:1151`, `:1154`, `:1612`, `:1810`, `:1912`, `:1919`, `:2053`, `:2066`, `:2080`), 13 purge calls deleted (`:173`, `:330`, `:381`, `:442`, `:483`, `:526`, `:570`, `:696`, `:748`, `:828`, `:1178`, `:1773`, `:1862`), 4 index reads retargeted at the value book (`:1033`, `:1195`, `:1576`, `:2030-2031`). Zero `FM-` tags. **Owned concurrently by proposal 90**, which rewrites the **28** `static SPEC: CommandSpec` literals in this file (`grep -c` = 28) — disjoint regions (specs vs `execute` bodies); see §Risks. 6 commits of recent churn, most recently `00dfb0ab fix(expiry): read the expiry domain's clock through one seam (R4)`. |
| `frogdb-server/crates/core/src/store/mod.rs` | 830 | **Primary.** The `Store` trait. `set_field_expiry` `:648-651`, `remove_field_expiry` `:653-656`, `remove_all_field_expiries` `:658-661`, `get_field_expiry` `:663-667` — **four methods deleted** (25 lines with docs). `get_expired_fields` `:669-673`, `get_expired_fields_limited` `:675-684` and `purge_expired_hash_fields` `:686-691` **stay** (the sweep's interface). One method added: `has_field_expiries(&self, key) -> bool`. **Also (rev 2): `pub trait ValueType` `:72-82` gains `const IS_HASH: bool = false`, overridden to `true` in the `HashValue` impl — the family probe §(c) routes the purge on.** The four `take_lazily_*` docs at `:511-575` describe the purge reporting contract and are **unchanged** — this proposal moves *where* purge is called from, not what it reports. |
| `frogdb-server/crates/core/src/store/hashmap.rs` | 2977 | **Primary.** Impls of the four deleted methods removed (`:1364-1382`). `install`'s field-expiry re-derivation `:388-396` and `uninstall`'s `:471` are the **model** the change generalizes and are untouched. `flush_keysizes_refreshes` `:610-630` gains the field-expiry reconcile. `purge_expired_hash_fields` `:1392-1460` gains an O(1) short-circuit before the `Arc::make_mut` at `:1415`; its `ValueLocation::Warm => return 0` arm `:1416` is §5a's mechanism and is **not** changed here. `assert_consistent` `:906-930` gains the field-expiry audit (§Testability 5). **Carries three `FM-PERSISTENCE-044` tags (`:2608`, `:2638`, `:2666`) — key-level expiry tests, untouched by this change.** |
| `frogdb-server/crates/core/src/store/typed.rs` | 584 | **Primary.** `get_typed` `:165`, `get_typed_mut` `:131` and `get_or_create_typed` `:198` gain the `T::IS_HASH`-gated purge prelude (§(c)); `HashValue` **stays** in the `typed_family_accessors!` list (`:269`) and no wrapper is hand-written. The doc at `:162-164` — *"Hash field-level TTL is a separate concern: commands that need it still call `Store::purge_expired_hash_fields` before reading"* — is the **exact sentence this proposal deletes**. |
| `frogdb-server/crates/core/src/command.rs` | — | **Read-only evidence (rev 2) — not edited under the preferred design.** `CommandContextCore::get_or_create` `:1082-1087` and the `CommandContext` twin `:1644-1649` both forward to `get_or_create_typed`; they are the routing every ghost-producing handler actually uses, and the reason the purge must live in the generic layer. Edited only under §(c)'s recorded alternative. |
| `frogdb-server/crates/core/src/noop.rs` | 392 | **Primary, ~15 lines.** `FieldExpiryIndex` `:169-277` gains `replace_key(key, fields)` (remove-then-set, 10 lines) and `contains_key(key)` (3 lines). Its three unit tests `:341`, `:363`, `:379` are unedited. |
| `frogdb-server/crates/types/src/types/hash.rs` | 587 | **Primary (rev 2 — was "read-only evidence"). Zero `FM-` tags.** The value book: `field_expiries` field, `set_field_expiry` `:518-521`, `remove_field_expiry` `:524-534`, `get_field_expiry` `:537-539`, `has_field_expiries` `:542-544`, `field_expiries()` `:546-549`, `remove_expired_fields` `:553-576`, `to_vec_with_expiries` `:579-586`. **The two implicit clears — `:262` inside `set`, `:321` inside `remove` — are the live bug's mechanism** (§Problem 2). **Edited by H1b:** `incr_by` `:397` and `incr_by_float` `:434` must stop routing through the TTL-clearing `set`; they get a keep-TTL variant (§2a). Every other caller of `set` keeps today's clear-on-write, which is Redis-correct. |
| `frogdb-server/crates/core/src/shard/execution.rs` | 2132 | **Read-only evidence — not edited.** `self.store.flush_keysizes_refreshes();` `:289`, after every command on both the single-command and MULTI/EXEC paths. This is the drain point the reconcile rides on, and the reason no new seam is needed. |
| `frogdb-server/crates/core/src/shard/post_execution.rs` | — | **Read-only evidence.** The second flush call site, `:383`. |
| `frogdb-server/crates/core/src/shard/active_expiry.rs` | 704 | **Read-only, must NOT be edited.** The field sweep `:194-237`: `get_expired_fields_limited` `:203`, the per-key `purge_expired_hash_fields` `:224`, the `!purged_any` break `:234`, `DEFAULT_BATCH_SIZE = 1024` `:34` (applied `:120`) — the threshold §5 is quantified against. This is where a ghost index entry starves the sweep (§Problem 5) and where the *warm-tier* variant starves it with no ghost at all (§5a). Its `#[cfg(test)]` module (`:243`+) contains the 6 test-only dual-writes at `:268/272` and `:345-349` that become single writes. |
| `frogdb-server/crates/core/src/shard/event_loop.rs` | 1229 | **Primary, test-only, 4 lines.** `:878/880` and `:904/908` are dual-write test fixtures (`hash.set_field_expiry` + `store.set_field_expiry`) that become single value-side writes. The tests themselves (`:797`, `:933`, `:990`) keep their names and assertions. |
| `frogdb-server/crates/persistence/src/serialization/registry.rs` | 711 | **Read-only evidence — not edited.** `encode_hash` `:85` and `encode_hash_with_field_expiry` `:90-93` both dispatch on `hash.has_field_expiries()` — **the value book, never the index**. This is the proof that the value book is the durable one and the index is derived (§Problem 6). Its round-trip test `hash_with_field_expiry_preserves_ttl` `:692-707` is the net. |
| `frogdb-server/crates/persistence/src/serialization/collections.rs` | 257 | **Read-only evidence.** `serialize_hash_with_field_expiry` `:73`, `deserialize_hash_with_field_expiry` `:203` — the wire format carries per-field deadlines from the value book only. |
| `frogdb-server/crates/server/src/commands/search.rs` | 1335 | **Primary, small.** `FT.SUGADD` `:906-923` (`ctx.get_or_create::<HashValue>` `:906`, two `hash.set` calls) and `FT.SUGDEL` `:1136-1150` (**raw `ctx.store.get_mut` `:1136` + `as_hash_mut` `:1137`**, two `hash.remove` calls) mutate hashes value-side with no index maintenance and no purge — the same omission class as HMSET, in a different crate. Index maintenance becomes automatic for both (the reconcile rides on `get_mut`); **purge-on-read reaches SUGADD via `get_or_create_typed` but *not* SUGDEL**, which needs an explicit reroute to `get_hash_mut` — §(c). |
| `frogdb-server/crates/redis-regression/tests/hash_field_expire_tcl.rs` | 1718 | **Primary, +2 tests (rev 2, was +3).** `tcl_field_ttl_overridden_by_hset` `:615-657` is the existing passing witness for HSET; the identical test with `HMSET` at `:634` **fails at `:647` today** and is hotfix H1a's regression test. Its mirror image — `HINCRBY` at the same position asserting `HTTL … → 100`, **not** `-1` — is H1b's. §Testability. |
| `frogdb-server/crates/redis-regression/tests/hash_regression.rs` | 501 | **Read-only evidence.** The 5 field-expiry tests (`:256`, `:280`, `:302`, `:373`, `:441`) — all HGETEX/HSETEX/HEXPIRE/HTTL. `hsetex_keepttl_preserves_existing_field_ttl` `:441-467` is the only test that exercises `:2030-2031`, and it passes because the books agree in its scenario. |
| `.scratch/hardening/specs/persistence-failure-modes.md` | — | **Read-only.** `FM-PERSISTENCE-044` `:626` — *"a key past its deadline is never resurrected by a command that reads through the expiry window"* — owns the three tagged tests in `hashmap.rs`. **Key-level**, not field-level: its Forced-by list is `persist_on_expired_key_deletes_instead_of_immortalizing`, `persist_on_expired_key_leaves_no_expiry_index_orphan`, `nondestructive_probes_do_not_see_a_past_deadline_key`, none of which this change edits or renames. `just lint-failure-modes` stays green. |
| `.scratch/hardening/specs/*.md` | — | **Read-only.** `grep -rn 'hash_field\|HFE\|hexpire\|field.*expir'` over every failure-mode spec returns **nothing**. No `FM-` row governs hash **field** expiry, so no spec-first obligation attaches to the behaviour this proposal changes. |
| `.scratch/testing-improvements-round2/issues/open/82-…-residual-test-gaps.md` | — | **Read-only prior art.** §F3 (`:422` acceptance, `:449` re-triage) already inventories the seven non-purging hash paths. 93 supersedes it — see §(c). |

## Problem

### 1. Two books, seventeen hand-paired writes, and a store that already knows better

`HashMapStore`'s lifecycle banner (`hashmap.rs:337-346`) states the design rule and names the
cost of breaking it — *"Before this seam existed, `set` skipped the expiry indexes — an
overwritten key kept its stale index entry and active expiry would later delete the (now
persistent) key: silent data loss"* (`:352-355`). That failure was fixed for the **key-level**
expiry index by routing every whole-value write through `install`.

The **field-level** index got the same treatment for whole-value writes (`install:388-396`
re-derives it from `hash.field_expiries()`; `uninstall:471` drops it) — and nothing at all for
in-place writes. `get_mut` (`hashmap.rs:1298-1345`) snapshots the entry for the deferred
size/histogram refresh at `:1320-1329` and stops there; `flush_keysizes_refreshes`
(`:610-630`) reconciles `keysizes`, `memory_used` and `metadata.memory_size` and stops there.
There is no third structure in that flush, so the field-expiry index is the one derived
structure whose reconciliation is delegated to callers in another crate.

That delegation is the `Store` trait's field-expiry mutator family (`store/mod.rs:646-661`),
and `frogdb-commands` discharges it by hand at 11 sites paired with 6 value-side writes. A
representative pair, HGETEX: a 21-line `match` on `expiry_action` that mutates the value
(**`commands/src/hash.rs:1885-1905`**), then a 17-line `match` on the **same** `expiry_action`
that mutates the index (**`:1908-1924`**), separated by the comment `// Phase 3: sync store
index` (`:1907`). HSETEX does the same thing three times over (`:2039-2085`), HEXPIRE twice
(`:1133-1158`), HPERSIST twice (`:1601-1613`).

The three-phase shape is not incidental. Every one of these handlers is written as *immutable
gather → mutable apply → index sync* **because the two books need incompatible borrows**:
`ctx.store.get_field_expiry` takes `&self` while `hash.set_field_expiry` needs `&mut` through
`ctx.store.get_hash_mut`. HEXPIRE's `drop(hash);` at `:1042` carries the comment *"Drop the
shared read handle before the mutable pass below, so `get_hash_mut` does not copy-on-write a
still-shared value"* — a real cost paid to keep two books in step. Collapse the books and the
phases collapse with them.

### 2. Three shipped commands write only the value book — LIVE

`HashValue::set` clears the field's TTL as its first statement (`types/src/types/hash.rs:262`,
`self.remove_field_expiry(&field);`); `HashValue::remove` does the same at `:321`. Every
handler that reaches `set`/`remove` must therefore also clear the index. **HSET does**
(`commands/src/hash.rs:85-88`, comment: *"HSET clears field expiry on overwritten fields -
update store index"*). **HDEL does** (`:234-237`). **HGETDEL does** (`:1807-1812`). **HSETEX
does** (`:2051-2054`).

These do not:

| Command | Value book cleared at | Index cleared? | Verdict |
|---|---|---|---|
| **HMSET** | `hash.rs:294` → `types/hash.rs:262` | **no** — the handler body is `:279-298`, complete, with no `remove_field_expiry` and no purge | **LIVE ghost producer; the index is the wrong book** |
| **HINCRBY** | `hash.rs:616` → `incr_by` `types/hash.rs:397` → `:262` | **no** — handler body `:610-618` | **LIVE divergence producer; the *value* is the wrong book** (§2a) |
| **HINCRBYFLOAT** | `hash.rs:659` → `incr_by_float` `types/hash.rs:434` → `:262` | **no** — handler body `:652-662` | same as HINCRBY (§2a) |
| **HSETNX** | `hash.rs:132` → `set_nx` `types/hash.rs:306` → `:262` | **no** — but `set_nx` returns early when the field exists (`:303-305`), so it only reaches `set` for absent fields and creates no ghost of its own | **LATENT** (it does not clean up a pre-existing ghost either) |
| **FT.SUGADD** | `server/src/commands/search.rs:919`, `:929` → `:262` | **no** | **LIVE**, scoped to FT suggestion dictionaries |
| **FT.SUGDEL** | `server/src/commands/search.rs:1144`, `:1150` → `types/hash.rs:321` | **no** | **LIVE**, same scope |

Reproduction, HMSET (the pure ghost case):

```
HSET   h f v1
HEXPIRE h 100 FIELDS 1 f      → 1     (both books now hold now+100s)
HMSET  h f v2                 → OK    (value book cleared; index still holds now+100s)
HTTL   h FIELDS 1 f           → 100   ← Redis answers -1
```

The divergence is **one-directional**: the index holds entries the value does not. The reverse
cannot happen — every value-side write in a handler is paired with an index write, and
`install` re-derives the index from the value on every whole-value write. So the whole failure
surface is "an index entry the value does not back", which makes the fix tractable. What the
*direction* does **not** settle is which side is wrong — §2a is the case where it is the value.

### 2a. HINCRBY / HINCRBYFLOAT are the opposite defect: the index is right and the durable book is wrong

**Redis preserves a hash field's TTL across an increment.** `hincrbyCommand` and
`hincrbyfloatCommand` write the new value with `HASH_SET_TAKE_VALUE | HASH_SET_KEEP_TTL`
(`t_hash.c:2560`, `:2613`) — the explicit keep-TTL flag, distinct from the plain
`hashTypeSet` the HSET family uses, which clears it. So `HSET h f 1; HEXPIRE h 100 FIELDS 1 f;
HINCRBY h f 1; HTTL h FIELDS 1 f` answers `100` in Redis, not `-1`.

> **Upstream-confidence note.** There is no vendored Redis C source in this repository
> (`find . -iname 't_hash.c*'` → nothing), so those two line numbers come from knowledge of
> Redis 8.0, not from a file in the tree. The *behaviour* — increment keeps the field TTL — is
> the load-bearing claim and is what an implementer must re-confirm against a real
> `redis-server` (one `HEXPIRE` + `HINCRBY` + `HTTL` round trip) before writing the fix.

FrogDB does the opposite, and does it below the `Store` layer:
`HincrbyCommand::execute` (`commands/src/hash.rs:615-616`) and
`HincrbyfloatCommand::execute` (`:658-659`) call `HashValue::incr_by`
(`types/src/types/hash.rs:397`) / `incr_by_float` (`:434`), each of whose last act is
`self.set(field, …)` (`:397`, `:434`) — and `set`'s **first statement** is
`self.remove_field_expiry(&field)` (`:262`).

**So for these two commands the index book holds the truth and the durable book has lost it.**
The field TTL that reached the WAL/RDB is gone from the only structure that is serialized
(`registry.rs:85`, `:90-93`, keyed on `hash.has_field_expiries()`), which makes this a
**durable-state destruction**, not merely a wrong reply.

**The third wire-visible defect this exposes, live today.** Because the value book no longer
holds a deadline for that field, `purge_expired_hash_fields` can never reap it —
`hash.remove_expired_fields(now)` (`hashmap.rs:1424` → `types/hash.rs:553-576`) enumerates the
*value's* expiries and returns nothing, so neither the lazy purge nor the active sweep will
ever remove the field. Meanwhile `execute_httl_common` reads the surviving ghost
(`hash.rs:1195`) and hands it to the converters:

- `HTTL` (`:1419-1428`) and `HPTTL` (`:1459-1465`) both open with
  `if expires_at <= now { return -2; }`. Once the ghost deadline passes, **`HTTL` reports `-2`
  ("no such field") for a field `HGET` still returns** — a reply Redis cannot produce, because
  in Redis a field whose TTL elapsed is *gone*, and one whose TTL was kept is *live with a
  positive TTL*.
- `HEXPIRETIME` (`:1496`) and `HPEXPIRETIME` (`:1527`) use `instant_to_unix_secs`/`_ms` with
  **no past-deadline guard at all**, so they report an absolute expiry timestamp in the past
  for a field that will never expire.

Between the increment and the ghost's deadline, `HTTL` counts down normally — which is
accidentally the *Redis-correct* answer, sourced from the wrong book. After the deadline it
flips to `-2`. Nothing in between is observable as a transition, which is why this survived.

**Correct fix (H1b, §Effort): a keep-TTL variant of `HashValue::set`.** `incr_by` and
`incr_by_float` must write the value without clearing `field_expiries` — e.g. a private
`set_keeping_field_ttl(field, value, thresholds)` (or a `KeepFieldTtl` flag threaded through
`set`) used by exactly those two call sites, leaving every HSET-family caller on the
TTL-clearing `set`. **Do not** add `ctx.store.remove_field_expiry` after `hash.rs:616`/`:659`:
that would delete the *surviving* copy of a TTL Redis keeps, converting a recoverable
divergence into permanent loss, and would pin non-Redis behaviour as golden.

**Fold acceptance criterion (post-fold).** After this proposal lands there is one book, so the
increment path's behaviour is whatever `HashValue::set` does — and the acceptance test is that
**`HSET h f 1; HEXPIRE h 100 FIELDS 1 f; HINCRBY h f 1; HTTL h FIELDS 1 f` still reports the
field's TTL (~100), not `-1`.** The fold does not deliver that on its own: without H1b the fold
makes the value book authoritative and therefore makes `HTTL` answer `-1` — self-consistent,
still wrong. **H1b is a prerequisite of the fold's correctness, not an optional companion.**

### 3. Three readers, three different authorities

| Reader | Site | Book consulted |
|---|---|---|
| `HEXPIRE`/`HPEXPIRE`/`HEXPIREAT`/`HPEXPIREAT` — `current_expiry` for NX/XX/GT/LT | `commands/src/hash.rs:1033` | **index only** |
| `HTTL`/`HPTTL`/`HEXPIRETIME`/`HPEXPIRETIME` — the reply | `:1195` | **index only** |
| `HPERSIST` — "did this field have a TTL" | `:1576` | **index only** |
| `HSETEX … KEEPTTL` — the TTL to carry over | `:2030-2031` | **value, then index** |
| WAL / RDB / `DUMP` encode | `persistence/serialization/registry.rs:85`, `:90-93` | **value only** |
| `HashMapStore::install` index re-derivation | `store/hashmap.rs:388-396` | **value only** |
| active expiry sweep | `shard/active_expiry.rs:203` | **index only** |

Seven readers. Four say the index is authoritative, three say the value is. Each of the first
three commands is holding a live `&HashValue` at the moment it reaches past it into the index —
`:1031` calls `hash.contains(f)` on the line above `:1033`; `:1190` calls `hash.contains` four
lines above `:1195`; `:1574` calls `hash.contains(f)` two lines above `:1576`. The value book
is in hand and is ignored.

Consequences of a ghost, all wire-visible:

- **HTTL** returns a TTL for a field that has none (`:1195`).
- **HPERSIST** returns `1` ("removed a TTL") instead of `-1` and then removes the ghost —
  accidentally self-healing, which is exactly the kind of behaviour that makes the underlying
  bug hard to reproduce twice.
- **HEXPIRE NX** sees `current_expiry.is_some()` and skips, returning `0` where Redis returns
  `1`; **XX** does the mirror; **GT**/**LT** compare against a deadline that no longer exists.

### 4. `hash.rs:2030-2031` — the ruling

```rust
2029:                    .map(|(field, _)| {
2030:                        hash.get_field_expiry(field)
2031:                            .or_else(|| ctx.store.get_field_expiry(key, field))
2032:                    })
```

**In a consistent system the `or_else` arm is unreachable.** It fires only when the index holds
a deadline the value does not — the ghost condition — because the reverse direction cannot
occur (§Problem 2). `hsetex_keepttl_preserves_existing_field_ttl`
(`hash_regression.rs:441-467`), the only test that exercises this code, passes through the
first arm.

**In the diverged system it is the amplifier.** `saved_expiries` is read at `:2025-2036`,
*before* `hash.set(...)` at `:2043` clears the value book; the KEEPTTL branch then writes the
saved deadline back into the value book at `:2073` and the index at `:2080`. So after

```
HSET h f v1 ; HEXPIRE h 100 FIELDS 1 f ; HMSET h f v2      (ghost created)
HSETEX h KEEPTTL FIELDS 1 f v3                              (ghost promoted)
```

the field `f` — which HMSET made persistent, and which HTTL should have reported as `-1` — now
carries a real, durable, replicated deadline in the value book and dies 100 seconds after the
original HEXPIRE. **Silent data loss, from a fallback whose stated job was resilience.**

**Ruling: the `or_else` is LATENT in isolation and LIVE in composition.** It is not the defect;
it converts §Problem 2's wrong-answer defect into a data-loss defect. It should be deleted
rather than fixed — after the fold there is one book and `hash.get_field_expiry(field)` is the
whole expression. This is the general shape of the finding: **an `or_else` between two copies
of one fact is not a fallback, it is a silent commitment to whichever copy is wrong.**

### 5. No background mechanism can reap a ghost, and enough of them starve the active sweep

`purge_expired_hash_fields` removes index entries **only for fields the value book reported as
expired** (`hashmap.rs:1424` `hash.remove_expired_fields(now)` → `:1437-1440` `for field in
&removed_fields { self.field_expiry_index.remove(key, field); }`). A ghost is unknown to the
value book, so `remove_expired_fields` never returns it and the index entry is never removed.

**Scoped claim (rev 2): no *background* mechanism can delete a ghost.** Neither the lazy purge
nor the active sweep will ever reap it; only a restart, a `DEL`/whole-key overwrite (which
routes through `uninstall:471`), or a subsequent client command that happens to clear that
exact field's index entry by hand will. Six commands do the latter and are therefore
accidental ghost-cleaners for the fields they name: **HSET** (`hash.rs:85-88`), **HDEL**
(`:234-237`), **HPERSIST** (`:1610-1613`), **HGETDEL** (`:1807-1812`), **HGETEX … PERSIST**
(`:1916-1921`) and **HSETEX** (`:2051-2054`). "Immortal" is therefore the wrong word — the
right one is *unreachable by anything the server does on its own*, which is what makes the
starvation below possible and what makes reproduction non-deterministic in a live workload.

`FieldExpiryIndex::get_expired_limited` (`core/src/noop.rs:250-266`) iterates `by_time` — keyed
`(Instant, key, field)` — from the front and `break`s at the first non-due entry. Ghosts with
past deadlines therefore sit **permanently at the head of the scan**. In `run_active_expiry`
(`shard/active_expiry.rs:194-237`):

- `:203` fetches a batch of at most `batch_size` due `(key, field)` pairs — the oldest first,
  i.e. the ghosts;
- `:208-211` filters out keys already purged this cycle via `seen`;
- `:212-216`: if every key in the batch was already seen, `break` — *"Whole batch was keys
  already purged this cycle; their fields are gone, so there is nothing left to make progress
  on"*, a comment that is true only under the invariant this proposal is establishing;
- `:234`: `if batch_len < self.batch_size || !purged_any { break; }`.

**Quantified.** The shard-wide break needs `batch_len == batch_size` to be *false-avoiding* —
i.e. the ghosts must fill an entire batch, because `batch_len < self.batch_size` breaks the
loop anyway (that is the normal "index drained" exit). `batch_size` is
`DEFAULT_BATCH_SIZE = 1024` (`active_expiry.rs:34`, applied at `:120`). So:

- **Fewer than 1024 head-of-index ghosts:** no starvation. The ghosts merely consume batch
  slots — each cycle wastes up to *(ghost count)* of its 1024 entries and its budget on
  no-op purges, and genuine expirees behind them are still reached in the same batch.
- **1024 or more ghosts with past deadlines at the head of `by_time`:** every batch is all
  ghosts, `purged_any` stays `false`, `:234` breaks, and **no field TTL anywhere in the shard
  is actively reaped again** until a restart or a client command clears the ghosts by hand
  (§5, six commands). They are only cleaned up if a client happens to run one of the 16
  commands that carry a purge call against that key; memory for untouched hashes is never
  reclaimed.

**1024 is one command away.** A single `HMSET` over a hash whose 1024 fields all carry TTLs
crosses the threshold in one round trip — the handler (`hash.rs:279-298`) clears the value
book for every field it writes and leaves all 1024 index entries behind. No accumulation over
time is required.

### 5a. The same starvation exists with zero ghosts, via the warm tier — a separate, pre-existing bug

**Its own finding; cited here, not fixed by this proposal — for the orchestrator to file.**

`spill_key` (`hashmap.rs:747-786`) flips a hot entry to `ValueLocation::Warm` and reconciles
through `resize` (`:781`); it **never touches `field_expiry_index`**. `unspill_key`
(`:791-851`) restores the value and again reconciles through `resize` (`:844`), not `install`.
So a hash spilled while holding field TTLs keeps every one of its index entries — legitimately,
since the value still exists and the TTLs are still real — but `purge_expired_hash_fields`
**cannot act on them**: its `ValueLocation::Warm => return 0` arm (`hashmap.rs:1416`) bails out
before any purge.

The consequence at `active_expiry.rs:194-237` is byte-for-byte the §5 failure with **no ghost
anywhere in the system**: due `(key, field)` pairs for spilled hashes sit permanently at the
head of `by_time`, `purge_expired_hash_fields` returns 0 for each, `purged_any` stays `false`,
and `:234` breaks the shard-wide field sweep. Unlike ghosts, these entries are not stale — they
are correct index entries for a value the purge path structurally refuses to touch — so
**neither this proposal's fold nor either hotfix fixes them**. The plausible fixes (unspill
before purging, or skip-and-continue instead of breaking on a non-purgeable key) are a separate
design question in the tiered-storage area.

Secondary: `FieldExpiryIndex`'s own memory is not counted anywhere — `hot_entry_memory_size`
(`hashmap.rs:300-305`) is `key.len() + value.memory_size() + size_of::<KeyMetadata>() +
size_of::<Entry>()`, and `HashValue::memory_size` (`types/hash.rs:481-486`) counts the **value
book's** expiries. An unbounded ghost accumulation is therefore invisible to `INFO memory`,
`maxmemory` and eviction.

### 6. Crash and replica behaviour — the divergence is not durable, which is its own problem

**Flagged for the orchestrator, not fixed here** (durability/consistency, not security):

- **Persistence is value-only.** `encode_hash` / `encode_hash_with_field_expiry`
  (`registry.rs:85`, `:90-93`) dispatch on `hash.has_field_expiries()` and serialize
  `hash.to_vec_with_expiries()` (`types/hash.rs:579-586`). The index is never written to the
  WAL, an RDB, or a `DUMP` payload. `WalStrategy::PersistFirstKey` (`core/src/command.rs:658`)
  stages `WalAction::Persist(key)` — the whole value — so a field TTL reaches disk only via the
  value book.
- **Recovery re-derives.** Restore routes through `replace_entry` → `install`
  (`hashmap.rs:328-334`, `:367`), whose `:388-396` rebuilds the index from
  `hash.field_expiries()` after `uninstall` (`:377`) has dropped the previous key's entries
  wholesale.
- **Therefore ghosts do not survive a restart** — and that is the awkward part. `HTTL h FIELDS
  1 f` answers `100` before a restart and `-1` after one, with no command in between. The same
  applies across a replica fullsync: the replica installs the value and derives a clean index,
  so **master and replica answer `HTTL` differently for the same field** until the master is
  restarted. Command-stream replication reproduces the ghost deterministically (replicas replay
  the same handlers), so a replica built by streaming agrees with the master while a replica
  built by fullsync does not.
- **No WAL/replication path is changed by this proposal.** After the fold the value book is
  both the only book and the only serialized one, so the pre/post-restart and master/replica
  answers converge by construction.

## Proposed change

### The direction is forced by the durability finding

The index is not persisted, is rebuilt from the value on every whole-value write, and is
rebuilt from the value on recovery. **It is already a cache of the value book everywhere except
in `frogdb-commands`.** So the fold direction is not a judgement call: the value book is the
model, the index book is the derived structure, and the change is to stop letting anything
outside `HashMapStore` write a derived structure.

### (a) Delete the index's public write interface

`store/mod.rs`: remove `set_field_expiry` `:648-651`, `remove_field_expiry` `:653-656`,
`remove_all_field_expiries` `:658-661` (zero production callers today — pure deletion) and
`get_field_expiry` `:663-667`. `store/hashmap.rs:1364-1382`: remove the four impls. Add one
read-only probe used by (c):

```rust
/// Whether `key` is a hash with at least one field TTL. O(1); the guard that
/// makes purge-on-read affordable at every hash accessor.
fn has_field_expiries(&self, key: &[u8]) -> bool { let _ = key; false }
```

`get_expired_fields`, `get_expired_fields_limited` and `purge_expired_hash_fields` **stay**:
they are the active sweep's interface (`active_expiry.rs:203`, `:224`), and the sweep is a
legitimate consumer of the derived structure.

The 11 index-write statements in `commands/src/hash.rs` are deleted with their scaffolding
(`:85-88`, `:234-237`, `:1147-1158`, `:1610-1613`, `:1807-1812`, `:1907-1924`, `:2051-2054`,
`:2064-2067`, `:2077-2082` — ≈62 lines). The four index reads are retargeted at the
`&HashValue` each site is already holding: `:1033` → `hash.get_field_expiry(f)`, `:1195` →
`hash.get_field_expiry(field_arg)`, `:1576` → `hash.get_field_expiry(f).is_some()`,
`:2030-2031` → `hash.get_field_expiry(field)` with the `or_else` **deleted, not preserved**.

### (b) Reconcile the index in the flush that already exists

`get_mut` already queues every in-place-mutated key (`hashmap.rs:1320-1329`) and
`flush_keysizes_refreshes` (`:620-630`) already drains that queue after every command
(`execution.rs:289`, `post_execution.rs:383`). Rename it `flush_pending_refreshes` and add one
call per key:

```rust
/// Re-derive `key`'s field-expiry index entries from the value — the in-place
/// arm of the derived-structure seam, matching what `install` does for whole-value
/// writes (`:388-396`) and `uninstall` for removals (`:471`).
///
/// Invariant (I1): after this returns, if `key` is a **hot** entry then for every
/// field `f`,
///   field_expiry_index.get(key, f) == data[key].as_hash().get_field_expiry(f)
fn reconcile_field_expiries(&mut self, key: &[u8]) {
    // Warm entry: the value lives in RocksDB and the index entries this key
    // owns are legitimate (`spill_key` `:747-786` deliberately leaves them in
    // place — it reconciles through `resize`, not `install`). Re-deriving from
    // an absent hot value would read "no expiries" and delete real entries. The
    // warm arm of I1 is owned by spill/unspill, not by this seam.
    //
    // An *absent* key falls through on purpose: `uninstall` already dropped its
    // index entries, so the `remove_key` below is an idempotent repeat.
    if let Some(entry) = self.data.get(key)
        && entry.hot_value().is_none()
    {
        return;
    }
    let live = self
        .data
        .get(key)
        .and_then(Entry::hot_value)
        .and_then(|v| v.as_hash())
        .and_then(|h| h.field_expiries().cloned());
    match live {
        Some(map) if !map.is_empty() => self.field_expiry_index.replace_key(key, map),
        _ => self.field_expiry_index.remove_key(key),
    }
}
```

plus `FieldExpiryIndex::replace_key(&mut self, key: &[u8], fields: HashMap<Bytes, Instant>)` in
`core/src/noop.rs` (`remove_key` `:226` then `set` `:192` each — 10 lines).

**The warm early-return is load-bearing, not defensive boilerplate.** Without it, one
`reconcile_field_expiries` on a spilled hash calls `remove_key` and silently deletes that
hash's real, still-due field deadlines — the exact "silent data loss" the `install`/`uninstall`
banner (`hashmap.rs:352-355`) was written to prevent, reintroduced in field form by the fix for
it. The path is narrow today (`get_mut` unspills warm keys up front at `hashmap.rs:1304-1310`,
and `spill_key` drains the pending queue before flipping the location, `:751`), which is
precisely why it must be an explicit guarded arm with a comment rather than an accident of
call ordering.

**Staleness window, and why it is safe.** The invariant is *not* maintained continuously: from
the moment a handler mutates a hash through `get_hash_mut` until the end-of-command flush
(`execution.rs:289`, `post_execution.rs:383`), the index may disagree with the value. That is
sound **only because, post-fold, the index has exactly one reader — the active sweep
(`active_expiry.rs:203`) — and the shard is single-threaded**: the sweep runs from the same
event loop as command execution, so it cannot observe the store mid-command. This is why (a)
retargeting the four command-side index *reads* at the value book is not an optimisation but a
precondition of (b): any surviving index reader inside a command would read across the window.
**MULTI/EXEC does not widen it** — `flush_keysizes_refreshes` is called per queued command on
the EXEC path too (`execution.rs:289` covers both), so the window is one command, never one
transaction.

**Cost.** One `HashMap` clone of size *(number of TTL'd fields on that key)* per in-place-mutated
key per command; zero allocation and one `HashMap::get` for the overwhelmingly common case of a
hash with no field TTLs (`field_expiries()` is `None` → `remove_key` on an absent key). The
queue already exists, is already bounded by the keys one command touched, and is already
drained at the right moment. **A warm hash with 10 000 TTL'd fields mutated by HSET would clone
a 10 000-entry map per command** — the one real cost, and the reason (b) is stated with a
measured alternative:

> **Alternative (b′), if the clone is measured to matter:** have `get_mut` snapshot
> `has_field_expiries(key)` alongside the size snapshot and skip the reconcile entirely when
> the key had no field TTLs before *and* has none after. This removes the clone from every hash
> that never used HEXPIRE, at the cost of one bool in the queue tuple. Deferred to
> implementation because it is a pure optimisation of a correct seam; the correctness argument
> does not depend on it.

**Alternative (b″) rejected:** reconcile eagerly by returning a `Drop`-guard wrapper from
`get_hash_mut`. It re-establishes the invariant sooner, but it needs a new wrapper type on the
hot path, it fires per-accessor-call rather than per-command, and it duplicates a drain
mechanism the store already owns.

### (c) Purge-on-read moves under the typed accessors — at `get_typed*`, not at the family wrappers

**Rev 2: this section had a routing hole and is rewritten.** The earlier version put the purge
in the hand-written `get_hash` / `get_hash_mut` / `get_or_create_hash` family wrappers
(`store/typed.rs:269`) and then claimed the headline behaviour fixes. **The commands that need
those fixes do not call the family wrappers.** Verified at HEAD:

| Command | Entry point | Reaches |
|---|---|---|
| HSET `hash.rs:72`, HSETNX `:130`, HMSET `:289`, HINCRBY `:615`, HINCRBYFLOAT `:658`, HSETEX `:2002` | `ctx.get_or_create::<HashValue>(key)` | `CommandContextCore::get_or_create` (`core/src/command.rs:1082-1087`, and its `CommandContext` twin `:1644-1649`) → `get_or_create_typed` (`store/typed.rs:198`) |
| FT.SUGADD `server/src/commands/search.rs:906` | same generic `ctx.get_or_create::<HashValue>` | same |
| FT.SUGDEL `server/src/commands/search.rs:1136` | **raw `ctx.store.get_mut(key)`** + `as_hash_mut()` (`:1137`) | neither typed layer |

Both `get_or_create` shims call `self.store.get_or_create_typed(key)` — the **generic**
method, which the earlier draft left untouched. So "HINCRBY on an elapsed field starts from 0"
and "HSETNX on an elapsed field returns 1" would **not** have landed: those handlers would have
kept reaching the value with the expired field still present. The read-side commands (HGET,
HRANDFIELD, HSCAN, …) do go through `get_hash`/`get_hash_mut` and would have been fixed; the
write-side ones this proposal advertises would not.

**Corrected placement: the purge goes in the generic layer, gated by a compile-time family
probe.** `get_typed`, `get_typed_mut` and `get_or_create_typed` (`store/typed.rs:165`, its
`_mut` sibling, and `:198`) each gain the same three-line prelude, conditioned on the value
type being a hash:

```rust
/// Set by `HashValue` alone. The one family whose *fields* carry TTLs, and
/// therefore the one whose typed read must reap before it can answer.
trait ValueType { const IS_HASH: bool = false; /* … */ }
impl ValueType for HashValue { const IS_HASH: bool = true; /* … */ }

fn get_typed<T: ValueType>(&mut self, key: &[u8]) -> Result<Option<TypedArc<T>>, WrongTypeError> {
    // Monomorphised away for the other thirteen families: `IS_HASH` is a const,
    // so this compiles to nothing outside the hash instantiation.
    if T::IS_HASH && self.has_field_expiries(key) {
        self.purge_expired_hash_fields(key);
    }
    // … unchanged body …
}
```

The `has_field_expiries` probe (§(a)) is the O(1) guard; `IS_HASH` keeps the other thirteen
families paying literally nothing. In `get_or_create_typed` the prelude must sit **before** the
create-if-missing check, beside the existing `purge_if_expired(key)` at `:199`, so a hash
emptied by the field purge is recreated fresh — the same ordering that method already
documents at `:192-197`.

> **Alternative, if a `ValueType` const is judged too clever:** reroute
> `CommandContextCore::get_or_create` / `CommandContext::get_or_create`
> (`command.rs:1082-1087`, `:1644-1649`) to dispatch hashes to a purging
> `get_or_create_hash`. It is a smaller type-system change but a *larger* hole: it fixes only
> the create path, leaves `get_typed`/`get_typed_mut` unpurged for any future direct caller,
> and re-states the rule in a second crate. Recorded and not preferred.

`check_typed`/`check_hash` do **not** purge: it is a type probe (`typed.rs:176-190`) and a
purge would make a destination check mutate the destination.

**FT.SUGDEL needs an explicit edit** — it is the one hash mutator that bypasses both typed
layers (`search.rs:1136` `ctx.store.get_mut(key)` → `:1137` `value.as_hash_mut()`). Post-fold
it still gets correct *index* maintenance for free (the reconcile rides on `get_mut`, §(b)),
but it gets **no purge-on-read**, so a suggestion dictionary field past its TTL stays visible
to it. Two options, decided at implementation: switch it to `get_hash_mut` (preferred — it is
the same access, typed), or accept the gap and say so at the call site. It is called out here
because "purge-on-read is now a property of the accessor" is false for any caller that does not
use an accessor.

Hand-written wrappers are no longer needed at all: `HashValue` **stays** in the
`typed_family_accessors!` list (`store/typed.rs:269`) and the family methods keep delegating to
the generic ones. That is a net simplification against the earlier draft — one prelude in three
generic methods instead of four hand-copied wrappers.

The **13 purge calls in `commands/src/hash.rs` are deleted**, and the 13 commands that never
had one — **HSET, HSETNX, HDEL, HMSET, HINCRBY, HINCRBYFLOAT, HEXPIRE, HPEXPIRE, HEXPIREAT,
HPEXPIREAT, HPERSIST, HSETEX**, plus **FT.SUGADD** (and **FT.SUGDEL** if it is rerouted) in
`frogdb-server` — get correct lazy field expiry. That closes a second family of wire-visible
defects that this audit found but does not otherwise treat: today `HINCRBY h f 1` on a field
whose TTL elapsed increments the **stale** value instead of starting from 0, and `HSETNX h f v`
on such a field returns 0 without writing.

**Prior art: this inventory is already tracked.**
`.scratch/testing-improvements-round2/issues/open/82-commands-core-types-residual-test-gaps.md`
§F3 ("Hash-field expiry: seven commands skip the field purge", acceptance criterion at `:422`,
re-triage row still-valid at `:449`) names HSETNX, HDEL, HINCRBY, HINCRBYFLOAT,
`execute_hexpire_common`, HPERSIST and HSETEX — the same seven non-purging paths, re-confirmed
against this tree on 2026-08-06. **93 does not re-derive that list; it supersedes it.** F3 asks
for seven tests pinning seven hand-fixed sites; (c) removes the possibility of the omission and
turns F3 into one seam test plus the acceptance tests below. **If 93 lands, issue 82 §F3 should
be closed against it rather than implemented separately** — flag for the orchestrator.

### (d) Make the purge cheap enough to sit on every read

`purge_expired_hash_fields` (`hashmap.rs:1392-1460`) calls `Arc::make_mut` at `:1415` — a
potential deep clone of the whole hash — *before* `remove_expired_fields` discovers at
`types/hash.rs:554-557` that `field_expiries` is `None` and there is nothing to do. Add the
O(1) short-circuit at the top:

```rust
fn purge_expired_hash_fields(&mut self, key: &[u8]) -> usize {
    let now = crate::clock::now();
    // Nothing due: no snapshot, no `Arc::make_mut`, no copy-on-write. The index
    // is the cheap oracle for "does this key have any field TTL at all", which
    // is what makes purge-on-read affordable at the accessor seam.
    if !self.field_expiry_index.contains_key(key) {
        return 0;
    }
    …
}
```

This also removes an existing per-read COW risk from the 13 current call sites. The clock read
stays `crate::clock::now()` — the same seam `:1394` uses today, so `lint-clock-seam` is
unaffected. (**Rev 2:** the earlier claim that this *removes* a clock read from
`frogdb-commands` was wrong and is withdrawn — the purge's clock read is `hashmap.rs:1394`, in
`frogdb-core`, and `commands/src/hash.rs` keeps all seven of its own reads at `:1079`, `:1247`,
`:1293`, `:1420`, `:1460`, `:1657`, `:1664`, none of which is the purge's. Net clock-read delta
in `frogdb-commands`: **zero**.)

### (e) The invariant, stated once, in the place that owns it

> **I1 — the field-expiry index is derived from every HOT hash.** For every key *k* whose entry
> is `ValueLocation::Hot` and every field *f*:
> `field_expiry_index.get(k, f) == data[k].as_hash().get_field_expiry(f)`.
> The index has no public writer; it is established by `install` (whole-value write), dropped
> by `uninstall` (removal), narrowed by `purge_expired_hash_fields` (reap), and re-derived by
> `reconcile_field_expiries` (in-place mutation). No code outside `HashMapStore` may write it.
>
> **Warm carve-out (rev 2).** A `ValueLocation::Warm` entry's index entries are *not* derivable
> in RAM — the value is in RocksDB and `spill_key` (`:747-786`) deliberately leaves the index
> untouched while reconciling everything else through `resize`. For warm keys the index is the
> **only** in-memory record of the field deadlines, so I1 is scoped to hot entries and
> `reconcile_field_expiries` early-returns on warm ones (§(b)). The warm arm has its own
> pre-existing defect, §5a, which this proposal does not fix.

Enforced structurally (no public mutator survives), and audited dynamically — but **not** by a
`debug_assert` buried in `reconcile_field_expiries`. `HashMapStore` already has the established
home for exactly this: `assert_consistent` (`hashmap.rs:906-930`, `#[cfg(test)]`), which
compares `memory_used`, `audit_expiry_index()` and every keysize/key-memory histogram against a
from-scratch recompute, with the doc *"the belt-and-suspenders check the lifecycle seam is
meant to make trivially true"*. **Add a sibling `audit_field_expiry_index()` — returning the
anomaly list, hot entries only — and one more assertion inside `assert_consistent`.** That
inherits its **22 existing call sites** for free (including the `FM-PERSISTENCE-044` test at
`:2663`, which already calls it)
and follows the shape `audit_expiry_index` already set for the key-level index, instead of
inventing a second, weaker convention in a hot-path method.

### Alternatives considered and rejected

**(R1) The brief's atomic `Store::set_field_expiry(key, field, deadline)` that writes value +
index + shard bookkeeping in one call.** Better than today, rejected as the primary. It keeps
the index a *co-equal* book with a public writer, so I1 remains a convention rather than a
structural property: any future handler that reaches `get_hash_mut` and calls
`hash.set_field_expiry` directly — which is exactly what HMSET, HINCRBY and HINCRBYFLOAT do
today, transitively through `HashValue::set` — re-creates the ghost. **It cannot fix the live
bug**, because the live bug's write is *inside `frogdb-types`* (`hash.rs:262`), below the
`Store` layer where R1's atomic method lives. R1 is the right shape for the *value-first* write
(`hash.set_field_expiry` alone) which is what this proposal ends up with.

**(R2) Move the index into `HashValue`** (a per-hash `BTreeMap` by deadline). Removes the
second book entirely, but the active sweep needs a *global* deadline order across all keys
(`active_expiry.rs:203`), which a per-value structure cannot provide without scanning every
hash in the shard. Rejected.

**(R3) Keep both books and add a seam lint** that every `Store::set_field_expiry` is adjacent
to a `HashValue::set_field_expiry`. Machinery to preserve the problem — and unenforceable
against the real defect, which is an *absent* pair at a site that names neither method
(`hash.rs:294` is `hash.set(...)`).

**(R4) Fix only the three handlers (H1a + H1b) and stop.** This is the hotfix, and it should
land first (§Effort). It is not the proposal because it leaves 17 hand-paired writes, 13
hand-placed purges, three disagreeing readers, and 13 commands with no lazy field expiry — i.e.
it fixes the three instances and none of the mechanism. Note that **H1b is not optional under
the full proposal either**: the fold makes the value book authoritative, and without the
keep-TTL variant that means HINCRBY silently discards the field TTL by design (§2a).

### Deletion test

- **The value book (`HashValue.field_expiries` + its six methods, `types/hash.rs:518-576`)** —
  delete it and hash field TTL has no durable representation: `serialize_hash_with_field_expiry`
  (`collections.rs:73`) has nothing to serialize and RDB/WAL round-trips silently drop every
  HEXPIRE. **Earns its keep — it is the model.**
- **`FieldExpiryIndex` (`noop.rs:169-277`) and `purge_expired_hash_fields`** — delete them and
  active expiry must scan every hash in the shard to find one due field
  (`active_expiry.rs:203` has no other source of deadline order). **Earns its keep — it is a
  real index.**
- **`Store::set_field_expiry` / `remove_field_expiry` / `get_field_expiry`** — delete them and
  nothing reappears: the 11 writes are subsumed by `reconcile_field_expiries`, the 4 reads by
  the `&HashValue` already in hand. **Do not earn their keep — this is the change.**
- **`Store::remove_all_field_expiries`** — delete it and nothing reappears; **it has zero
  production callers today**. Pure deletion, valid independent of the rest.
- **The 13 `purge_expired_hash_fields` calls in `commands/src/hash.rs`** — delete them and one
  call reappears, inside `get_typed`/`get_typed_mut`/`get_or_create_typed` (§(c) rev 2).
  **Thirteen become three, and thirteen commands that never had one are fixed by the same
  move** — plus FT.SUGDEL, which needs a one-line reroute because it uses no accessor at all.
- **The `or_else` at `hash.rs:2031`** — delete it and nothing reappears; under I1 it is
  unreachable, and under the current code it is a data-loss path (§Problem 4). **Pure
  deletion.**
- **`install`'s re-derivation (`hashmap.rs:388-396`) and `uninstall`'s `:471`** — delete either
  and the key-level failure the banner at `:352-355` describes returns, in field form.
  **Earn their keep; untouched — they are the model `reconcile_field_expiries` copies.**

## Testability improvement

**1. I1 becomes one audit function instead of a 17-site convention.** Today "the two books
agree" cannot be tested as a property, because there is no seam it could be checked at — you
would have to assert after each of the 30 field-expiry statements in `commands/src/hash.rs`.
After the change it is `audit_field_expiry_index()` inside `assert_consistent`
(`hashmap.rs:906-930`, §(e)), so **every store test that already calls `assert_consistent`
becomes a books-agree test for free** (22 call sites today), and the property is stated where
`audit_expiry_index` already states the key-level one. It must be checked **only when settled**
— `assert_consistent`'s own doc says so (*"Call only when settled (flush any pending `get_mut`
refreshes first)"*), which is exactly right for a per-command reconcile (§(b) staleness
window). The property test that would find today's bug is writable today — and this proposal's
claim is precisely that **it would fail today**, which is why H1a/H1b exist.

**2. The live bug's regression tests already exist in template form, four lines away.**
`tcl_field_ttl_overridden_by_hset` (`hash_field_expire_tcl.rs:615-657`) sets two field TTLs,
overwrites one with `HSET` at `:634`, and asserts `HTTL` returns `-1` at `:647`.

- **H1a:** replace `HSET` with `HMSET` → `tcl_field_ttl_overridden_by_hmset`, same `-1`
  assertion at `:647`, **fails today**.
- **H1b (rev 2 — replaces the two tests the earlier draft proposed):** replace `HSET` with
  `HINCRBY myhash field2 1` and assert `HTTL … field2` returns **~100, not `-1`**, i.e.
  `tcl_field_ttl_preserved_by_hincrby` / `…_by_hincrbyfloat`. The earlier draft's
  `…_by_hincrby` / `…_by_hincrbyfloat` asserted `-1` and would have **pinned non-Redis
  behaviour as golden** — they are withdrawn. There is no existing test in either regression
  file that constrains this (`grep -i hincrby hash_field_expire_tcl.rs` → nothing;
  `hash_regression.rs:150-168` exercises HINCRBY/HINCRBYFLOAT with no TTL involved), so
  nothing has to be un-pinned first.

**3. Purge-on-read becomes testable once instead of per-command.** Today "a hash read never
observes an expired field" is discharged at 13 sites and violated at 13 more, and the suite has
**no** test for the violated ones: `grep` over `hash_field_expire_tcl.rs` and
`hash_regression.rs` finds field-expiry coverage only for HGETEX/HSETEX/HEXPIRE-family/HTTL/
HPERSIST. After the change it is one seam, and four new tests pin the most damaging gaps:
`HINCRBY` on an elapsed field must start from 0 (today it increments the stale value),
`HSETNX` on an elapsed field must return 1 and write, `HDEL` on an elapsed field must return 0,
and — the §2a acceptance criterion — `HINCRBY` on a **live** TTL'd field must leave the TTL
reported by `HTTL`. These are **behaviour fixes**, so they are stated here as part of the
change's acceptance criteria, not as free wins. **Each must be written against the real
routing** (§(c)): a test that only exercises `get_hash` would pass against a fix that never
reaches HINCRBY.

**4. The starvation failure becomes reachable in a unit test.** `active_expiry.rs`'s test
module already builds dual-book fixtures by hand (`:257-273`, `:340-349`, comment: *"both on
the value and in the store's field-expiry index, matching HEXPIRE"*). After the change those
fixtures become single value-side writes, and the interesting scenario — an index entry the
value does not back — becomes constructible **only** by reaching inside `HashMapStore`, which
is the point: the state is no longer reachable from any command. A `#[test]` that constructs it
directly and asserts the sweep still drains behind it is the standing guard. **It does not
cover §5a**: the warm-tier starvation needs a spilled entry, not a ghost, and stays reachable
after this proposal — its test belongs with its own fix.

**5. A store-level golden.** `store/hashmap.rs`'s existing trio
(`set_overwrite_clears_stale_field_expiry_index` `:1818`,
`set_indexes_field_expiries_carried_by_the_value` `:1843`,
`set_with_options_overwrite_clears_stale_field_expiry_index` `:1865`) already pins I1 for the
`install` path. The change adds the in-place sibling —
`in_place_mutation_reconciles_field_expiry_index` — which is the test that does not exist today
because the behaviour does not exist today; plus a warm-carve-out test
(`reconcile_does_not_drop_a_spilled_hashs_field_deadlines`) that spills a hash with field TTLs,
runs a reconcile, and asserts the index still holds them — the guard on the one way §(b) could
delete durable state.

## Risks / scope boundaries vs siblings

**vs proposal 92 (hash field-expiry DECISION logic — the NX/XX/GT/LT table).** The boundary is
the value of `current_expiry`:

- **92 owns the decision.** Given `(field_exists, current_expiry, requested_deadline,
  conditions)`, produce `(action, reply_code)` — the ladder at `commands/src/hash.rs:1055-1124`
  and its `-2`/`0`/`1`/`2` replies. 92 may relocate that ladder into a pure, table-driven
  function.
- **93 owns the storage.** Where `current_expiry` is read from (`:1033` — the index today, the
  value after this change), how the resulting action is written (`:1133-1158` — two books
  today, one after), and when an elapsed field stops being visible at all (the missing purge).
- **They compose at the call site.** After both land, `execute_hexpire_common` reads
  `hash.get_field_expiry(f)` (93), passes it to 92's decision function, and applies the result
  through `hash.set_field_expiry` / `hash.remove` (93). Neither reaches into the other.
- **Ordering: 93 first is cheaper, but neither blocks the other.** 93 deletes `:1147-1158` (the
  index-sync loop) and retargets `:1033`; 92 rewrites `:1055-1124`. Disjoint line ranges in one
  function. If 92 lands first, 93's edit to `:1033` becomes an edit to whatever 92 named the
  input. **One shared correction either way: 92's decision table is fed a value that is wrong
  today** (§Problem 3) — 92 should not pin the current NX/XX behaviour against ghost inputs as
  golden.
- **Conflict class: textual-plus-type, still mechanical (rev 2).** 92 explicitly replaces the
  local `enum FieldAction` (`hash.rs:1045-1050`) with its shared verdict type (92 §"Files
  involved", its `hash.rs` row). 93's mutable-apply loop `match`es on that enum's variants. So
  whichever lands second rewrites the *other's* match arms, not just neighbouring lines — a
  type-level dependency, not a pure text conflict. It stays mechanical because the arm set
  (`NotFound`/`Delete`/`Skip`/`SetExpiry`) is preserved by 92's own design; it is called out so
  the second lander expects a compile error rather than a clean rebase.

**vs proposal 90 (`CommandSpec::DEFAULT`, solo-last sweep over `frogdb-commands`).** **Real
file conflict, disjoint regions.** 90 rewrites all **28** `static SPEC: CommandSpec` literals in
`commands/src/hash.rs` (`grep -c 'static SPEC: CommandSpec' hash.rs` → 28; the earlier "26" was
wrong; 90 cites `hash.rs:37-38` as its shape example); 93 edits only `execute`
bodies. **93 changes no spec literal** — no `wal:`, `reindex:`, `lookup:`, `access:` or
`event:` value moves, because purge-on-read is a store concern and the commands' declared
effects are unchanged. Verified by inspection of all 12 handler bodies 93 touches: every edit
is inside `fn execute`. 90 is **solo-last** by its own scoping, so 93 lands first and 90
rebases over it; the rebase is mechanical (90's edits are between `static SPEC` and `&SPEC` on
lines 93 never touches).

**vs proposal 84 (`BlockingOp`/`Direction` dedupe).** **No overlap.** 84's file set is
`protocol/{response,blocking,lib}.rs`, `types/src/types/mod.rs`, `types/src/lib.rs`,
`core/src/lib.rs`, `core/src/shard/{blocking,wait_queue}.rs`,
`server/src/connection/{util,blocking}.rs`, `commands/src/stream/read.rs`,
`commands/src/blocking.rs` and one website page. 93 touches `commands/src/hash.rs`,
`types/src/types/hash.rs`, `core/src/store/{mod,hashmap,typed}.rs`, `core/src/noop.rs`,
`core/src/shard/{event_loop}.rs`, `server/src/commands/search.rs` and two regression files.
**Zero shared files** — the nearest miss is `types/src/types/mod.rs` (84) vs
`types/src/types/hash.rs` (93), which are siblings, not the same file.

**vs proposal 83 (lazy-expiry authority).** Adjacent by name, disjoint by *topic* (83 =
**key-level** TTL authority: `check_and_delete_expired`, `purge_if_expired`,
`take_lazily_purged`; 93 = **field-level**) — but **not disjoint by mechanism.** The two share
`store/hashmap.rs` and `store/mod.rs`. 93's edits there are confined to `:388-396` (untouched),
`:610-630` (the flush), `:1364-1382` (deleted), `:1392-1415` (the purge guard) and the trait
block at `store/mod.rs:646-691`.

**Upgraded to a SEMANTIC conflict (rev 2 — was "textual").** 83 replaces the four
`take_lazily_*` methods (`store/mod.rs:522`, `:540`, `:555`, `:573`) with a single
`take_expiry_report` returning an `ExpiryReport`, and states that "`purge_if_expired` and
`purge_expired_hash_fields` keep pushing into the report exactly as" they do today (83
§"Proposed change"). **93 changes *which call sites* invoke `purge_expired_hash_fields`** —
from 13 hand-placed calls in `frogdb-commands` to the typed accessors in `frogdb-core`, which
means a different, larger set of commands now populates the `lazily_expired_fields` /
`lazily_shrunk` / `lazily_emptied` buffers (every hash accessor path, including READONLY
commands and `frogdb-server` callers). 83's restructured pipeline inherits that changed
population set. The conflict is therefore in 83's *contract*, not only in the text of the file:
whichever lands second must re-derive which commands can emit a lazy field-expiry report.
**Flag for the orchestrator; 93-before-83 is the cheaper order** (83 then restructures a
pipeline whose producers are already final).

### Other risks

- **Behaviour changes, deliberately.** Purge-on-read at the accessor seam changes 13 commands'
  observable behaviour on elapsed fields (HINCRBY, HSETNX, HDEL, HMSET, HEXPIRE-family,
  HPERSIST, HSETEX, HSET). Every change moves *toward* Redis, but they are changes, and each
  needs its own regression test before merge. This is the largest risk in the proposal and the
  reason it is **M** rather than **S**.
- **`FT.SUG*` share the hash type.** `search.rs:906` reaches `get_or_create_typed` and inherits
  purge-on-read; **`search.rs:1136` reaches nothing typed and does not** (§(c)). Suggestion
  dictionaries have no field TTLs unless a client sets one on the underlying key, so the
  practical effect is the `has_field_expiries` probe returning false. Named because it is a
  cross-crate behaviour change *and* because SUGDEL is the proposal's one known
  purge-on-read hole.
- **A typed read becomes a mutation.** It already is, at 13 call sites, and the reporting
  contract (`take_lazily_expired_fields` / `take_lazily_shrunk` / `take_lazily_emptied`,
  `store/mod.rs:526-575`) is unchanged — the worker drains those after every command
  regardless. What changes is that a READONLY command can now reap on *any* hash accessor path,
  including ones in `frogdb-server`. The buffers are drained at the same seam either way. See
  the 83 conflict above: this widening of the *producer set* is the semantic overlap.
- **Audit cost.** `audit_field_expiry_index` iterates each hot hash's expiries; it is
  `#[cfg(test)]`, inside `assert_consistent`, and runs only where that is already called — no
  release or debug-build cost at all, unlike the `debug_assert`-in-`reconcile` the earlier
  draft proposed.
- **Mutation-score exposure: none.** No locked crate is edited (`frogdb-core`,
  `frogdb-commands`, `frogdb-types`, `frogdb-server` — the four locked pairs are txn/vll,
  persistence/recovery, replication/replication-runtime, cluster/cluster-runtime, ADRs
  `0002`–`0004`). `frogdb-persistence` is *read as evidence only*; **no file in it is edited**,
  so no `just mutants-diff` obligation attaches.
- **Two findings this proposal deliberately does not fix**, both cited for the orchestrator to
  file separately: **§5a** (warm-tier field sweep starvation, zero ghosts, pre-existing) and
  the `FieldExpiryIndex` memory being invisible to `INFO memory`/`maxmemory` (§Problem 5,
  secondary).

## Spec / gates

- **Failure-mode specs: no *field*-expiry row, but one edited file is tagged.** `grep -rn
  'hash_field\|HFE\|hexpire\|HEXPIRE\|field.*expir'` over `.scratch/hardening/specs/*.md`
  returns nothing — no `FM-` row covers hash **field** expiry, so no spec-first obligation
  attaches to the behaviour changes here. **But `store/hashmap.rs` carries three
  `FM-PERSISTENCE-044` tags (`:2608`, `:2638`, `:2666`)** for the key-level row at
  `persistence-failure-modes.md:626`. This change does not rename, move, retag or delete any of
  those three tests, so `just lint-failure-modes` (`scripts/failure-modes.py:243`) sees no
  change. **Unaffected — but not untagged.**
- **Seam lints (`Justfile:329`, 14 gates).** `lint-clock-seam`: the purge's clock read stays
  `crate::clock::now()` (`hashmap.rs:1394`) and the change adds none — net −0.
  `lint-metrics-chokepoint`: `lazily_expired_fields` (`hashmap.rs:1435`) and its worker drain
  (`shard/worker.rs:742-743`) are untouched; the counter is bumped from the same statement.
  `lint-no-typed-unwrap` (commands-scoped): **neutral.** (Rev 2: the earlier "removes pressure"
  claim is withdrawn — the gate's regexes are
  `as_[a-z_]+_mut\(\)\s*\.unwrap\(\)` and `get_mut\([^)]*\)\s*\.unwrap\(\)` (`Justfile:1016`),
  and `hash.rs:2006-2007` is `store.get(key).unwrap()` / `as_hash().unwrap()` — neither
  matches. Deleting those lines changes nothing the gate can see.) The remaining **eleven**
  gates cover redirects, pubsub confirmation, failover atomicity, INFO, float format,
  durable-ack, nested config, error sanitisation, keyspace-notify routing, script gating and
  continuation locks — none reachable from this diff.
- **Feature profile.** `pub mod hash;` (`commands/src/lib.rs:39`) is ungated — hash is in
  `core-profile`, so `just test frogdb-commands` and `just test frogdb-core` cover the change
  without `cmd-full`.
- **Docs.** `grep` the website for hash-field-TTL architecture prose before merge; the
  behaviour changes in §Risks are user-visible and belong in the Redis-compatibility deltas
  page if one documents HFE.

## Effort

**M.** Twelve handler bodies edited in `commands/src/hash.rs` (all deletions), one purge prelude
added to three generic methods in `store/typed.rs`, one assoc const on `ValueType`
(`store/mod.rs:72-82`), ~24 lines added in `store/hashmap.rs` (reconcile + audit), ~15 in
`core/src/noop.rs`, five methods deleted from the `Store` trait, a keep-TTL variant in
`types/src/types/hash.rs` (H1b), one accessor reroute in `server/src/commands/search.rs`
(FT.SUGDEL), ~10 test-fixture lines simplified in
`core/src/shard/{event_loop,active_expiry}.rs`. Net ≈ **−115 production lines**, plus ~7 new
tests. The size is breadth, not depth: once I1 is stated, every edit is forced. (Rev 2 is
*smaller* than rev 1 in `typed.rs` — three preludes instead of four hand-written wrappers —
and slightly larger in `types` and `search.rs`.)

### Independently-landable hotfix H1a (HMSET) — LIVE, 9 lines, ships today

Mirror HSET's index clear (`commands/src/hash.rs:85-88`) into HMSET, the one ghost producer
whose value book is right. No design change, no interface change, no dependency on the rest of
this proposal:

- **HMSET** — after the `get_or_create` block closes (`hash.rs:295`), add the loop
  `for chunk in args[1..].chunks(2) { ctx.store.remove_field_expiry(key, &chunk[0]); }`.

HMSET's `hash` borrow must be released first: it is already scoped by the `let` at `:289` and
needs a block, exactly as HSET's `:71-83` is scoped today.

**Regression test:** one copy of `tcl_field_ttl_overridden_by_hset`
(`hash_field_expire_tcl.rs:615-657`) with `HSET` at `:634` replaced by `HMSET myhash field2
value4`, asserting `HTTL … field2 → -1` at the `:647` position. **Fails before the fix.**

**H1a closes**, for HMSET only: the wrong `HTTL`/`HPERSIST`/`HEXPIRE`-condition answers
(§Problem 3), the `HSETEX KEEPTTL` data-loss path (§Problem 4), and the ghost contribution to
the active-sweep starvation (§Problem 5). It does **not** close: the FT.SUG* pair, the 13
commands with no lazy field expiry, §5a, or the ability for the fourteenth handler to make the
same omission tomorrow. That is what the proposal is for.

### Independently-landable hotfix H1b (HINCRBY / HINCRBYFLOAT) — LIVE, opposite direction

**Do not** mirror H1a here. These two commands must *keep* the field TTL (§2a), so the fix is
in `frogdb-types`, not in the handler:

- Add a keep-TTL write path to `HashValue` — e.g. `set_keeping_field_ttl(field, value,
  thresholds)`, the body of `set` (`types/src/types/hash.rs:261-297`) minus its opening
  `self.remove_field_expiry(&field)` (`:262`).
- Point `incr_by` (`:397`) and `incr_by_float` (`:434`) at it. **No other caller changes** —
  the HSET family's clear-on-write is Redis-correct.
- Nothing in `frogdb-commands` is touched: `hash.rs:615-616` and `:658-659` keep their current
  two lines.

**Regression tests:** `tcl_field_ttl_preserved_by_hincrby` / `…_by_hincrbyfloat`, same template
as above but asserting `HTTL … field2` is **positive (~100)**, not `-1`. Both **fail before the
fix** — today the value book has been cleared and the index still counts down, so the assertion
sees whatever phase of §2a's two-stage wrongness the test's timing lands in.

**Before writing H1b, re-confirm the upstream behaviour against a real `redis-server`** (one
`HSET` + `HEXPIRE` + `HINCRBY` + `HTTL` round trip). The proposal's Redis citation is from
knowledge, not from vendored source (§2a).

**H1b closes:** the destroyed durable field TTL, the un-reapable field it leaves behind, and
`HTTL`'s post-deadline `-2` for a live field (§2a). It is also a **prerequisite for the fold**,
not an optional companion.

### Independently-landable hotfix H2 — LATENT, 4 lines, defensive

Make `purge_expired_hash_fields` evict index entries the value book does not back, so any ghost
that already exists in a running process is reaped rather than left for a restart: after the
`remove_expired_fields` call (`hashmap.rs:1424`), drop every indexed field for `key` whose
deadline is past and which `hash.get_field_expiry` reports as absent. Strictly a belt-and-braces
companion to H1a — with H1a in place no new HMSET ghosts are created, and with the full
proposal in place the state is unreachable — but it converts a background-unreachable ghost
into a self-healing one for processes that have been running since before H1a.

**Ordering caveat with H1b:** H2 must land **with or after** H1b, never before. Today the
HINCRBY divergence looks exactly like an HMSET ghost to H2 (index entry, no value-side
deadline), so H2 alone would delete the *only surviving copy* of a TTL Redis preserves — a
defensive fix that destroys durable state. Once H1b is in, HINCRBY produces no such entry and
H2's rule is safe. Ship only if operators cannot restart.
