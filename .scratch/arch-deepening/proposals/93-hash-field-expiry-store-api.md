# Proposal 93 — Hash field TTL lives in two books: 17 hand-paired writes, 13 hand-placed purges, three disagreeing readers — and three commands that write only one book

Round 38 · lane: commands + types · candidate **CT4** · effort **M** · **no locked crate
edited**, **zero `FM-` tags in any edited file** (verified: `grep -rn 'FM-'` over
`crates/commands/src`, `crates/types/src/types/hash.rs`, `crates/core/src/store/` returns
nothing), **no seam lint constrains the change** (§Spec / gates).

**Verified at HEAD `175a997d5f1c7f4ca4ecfa6decb2d3ad361e9fd1`** (worktree `arch-round-38-99`,
branch `worktree-arch-round-38-99`). Every code cite was derived at `25d38736`; the two commits
since (`d2155ce6`, `175a997d`) touch only `.scratch/arch-deepening/proposals/*.md`, so no code
line number moved. Nothing below is inherited from the lane brief — the brief's headline counts
are corrected in the next section.

**Ruling on the brief's "latent (no verified live bug)" framing: refuted. This is LIVE.**
Three shipped commands — **HMSET**, **HINCRBY**, **HINCRBYFLOAT** — clear the value-side field
TTL and never touch the index, leaving a permanent ghost entry that is (a) returned to clients
by `HTTL`, (b) used as `current_expiry` by `HEXPIRE`'s NX/XX/GT/LT decision, (c) promoted back
into the value book by `HSETEX … KEEPTTL` so the field really does die, and (d) **immortal in
the time-ordered active-expiry scan**, where it can starve the whole shard's field sweep. An
independently-landable 9-line hotfix (**H1**) and its regression test (four lines away from an
existing passing test) are specified in §Effort.

## Corrections to the lane brief

| Brief claim | Verified at HEAD |
|---|---|
| "every mutation site writes TWO statements … at **~35 sites**" | **Adjusted down and made precise.** There are **17** explicit field-expiry write statements in production code — **6** value-side (`commands/src/hash.rs:1139`, `:1606`, `:1890`, `:1899`, `:2061`, `:2073`) and **11** index-side (`:87`, `:236`, `:1151`, `:1154`, `:1612`, `:1810`, `:1912`, `:1919`, `:2053`, `:2066`, `:2080`) — spread over **7** handler families. Add the **13** purge calls and the total field-expiry statement count in `commands/src/hash.rs` is **30**. Two further value-side writes are *implicit*, inside `frogdb-types` (`types/src/types/hash.rs:262` in `HashValue::set`, `:321` in `HashValue::remove`), and those two are where the live bug lives. |
| "13 hand-placed purge calls" | **Verified exactly.** `commands/src/hash.rs:173, 330, 381, 442, 483, 526, 570, 696, 748, 828, 1178, 1773, 1862` — 13, covering 16 commands (`:1178` is inside the shared `execute_httl_common`). A 14th call at `core/src/shard/active_expiry.rs:224` is the active sweep and is not a handler. |
| "reads paper over the split with `or_else` **fallbacks**" (plural) | **Adjusted — and the truth is worse.** There is exactly **one** `or_else`, at `:2030-2031`. The other three index reads (`:1033`, `:1195`, `:1576`) have **no fallback at all**: they read the index book *only* and never consult the value they are holding. So the readers do not merely paper over the split — they disagree about which book is authoritative. §Problem 3. |
| "hash.rs:2030-2031 is a KNOWN disagreement site" | **Confirmed as a site, re-ruled as an effect.** The `or_else` arm is **unreachable in a consistent system** (it fires only when the index holds a TTL the value does not) and, in the diverged system H1 fixes, it is the **data-loss amplifier**: it copies the ghost deadline back into the value book at `:2073`, after which the field genuinely expires. It is not itself the bug. §Problem 4. |
| "M-L effort" | **M.** The change is mechanical once the direction is fixed: 11 index writes and 4 index reads deleted, 13 purge calls deleted, 5 trait methods deleted, ~38 lines added in two files. The size comes from the number of handler bodies touched (12), not from difficulty. §Effort. |

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
purge into the typed hash accessors — so that "index entry exists iff the value-side TTL is
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
| `frogdb-server/crates/commands/src/hash.rs` | 2327 | **Primary.** 11 index writes deleted (`:87`, `:236`, `:1151`, `:1154`, `:1612`, `:1810`, `:1912`, `:1919`, `:2053`, `:2066`, `:2080`), 13 purge calls deleted (`:173`, `:330`, `:381`, `:442`, `:483`, `:526`, `:570`, `:696`, `:748`, `:828`, `:1178`, `:1773`, `:1862`), 4 index reads retargeted at the value book (`:1033`, `:1195`, `:1576`, `:2030-2031`). Zero `FM-` tags. **Owned concurrently by proposal 90**, which rewrites the 26 `static SPEC: CommandSpec` literals in this file — disjoint regions (specs vs `execute` bodies); see §Risks. 6 commits of recent churn, most recently `00dfb0ab fix(expiry): read the expiry domain's clock through one seam (R4)`. |
| `frogdb-server/crates/core/src/store/mod.rs` | 830 | **Primary.** The `Store` trait. `set_field_expiry` `:648-651`, `remove_field_expiry` `:653-656`, `remove_all_field_expiries` `:658-661`, `get_field_expiry` `:663-667` — **four methods deleted** (25 lines with docs). `get_expired_fields` `:669-673`, `get_expired_fields_limited` `:675-684` and `purge_expired_hash_fields` `:686-691` **stay** (the sweep's interface). One method added: `has_field_expiries(&self, key) -> bool`. The four `take_lazily_*` docs at `:511-575` describe the purge reporting contract and are **unchanged** — this proposal moves *where* purge is called from, not what it reports. |
| `frogdb-server/crates/core/src/store/hashmap.rs` | 2977 | **Primary.** Impls of the four deleted methods removed (`:1364-1382`). `install`'s field-expiry re-derivation `:388-396` and `uninstall`'s `:471` are the **model** the change generalizes and are untouched. `flush_keysizes_refreshes` `:610-630` gains the field-expiry reconcile. `purge_expired_hash_fields` `:1392-1460` gains an O(1) short-circuit before the `Arc::make_mut` at `:1415`. Zero `FM-` tags. |
| `frogdb-server/crates/core/src/store/typed.rs` | 584 | **Primary.** `HashValue` leaves the `typed_family_accessors!` list (`:269`); `get_hash` / `get_hash_mut` / `check_hash` / `get_or_create_hash` become four hand-written methods that purge first. The doc at `:162-164` — *"Hash field-level TTL is a separate concern: commands that need it still call `Store::purge_expired_hash_fields` before reading"* — is the **exact sentence this proposal deletes**. |
| `frogdb-server/crates/core/src/noop.rs` | 392 | **Primary, ~15 lines.** `FieldExpiryIndex` `:169-277` gains `replace_key(key, fields)` (remove-then-set, 10 lines) and `contains_key(key)` (3 lines). Its three unit tests `:341`, `:363`, `:379` are unedited. |
| `frogdb-server/crates/types/src/types/hash.rs` | 587 | **Read-only evidence, likely unedited.** The value book: `field_expiries` field, `set_field_expiry` `:518-521`, `remove_field_expiry` `:524-534`, `get_field_expiry` `:537-539`, `has_field_expiries` `:542-544`, `field_expiries()` `:546-549`, `remove_expired_fields` `:553-576`, `to_vec_with_expiries` `:579-586`. **The two implicit clears — `:262` inside `set`, `:321` inside `remove` — are the live bug's mechanism** (§Problem 2). No signature changes needed; this crate is already correct on its own terms. |
| `frogdb-server/crates/core/src/shard/execution.rs` | 2132 | **Read-only evidence — not edited.** `self.store.flush_keysizes_refreshes();` `:289`, after every command on both the single-command and MULTI/EXEC paths. This is the drain point the reconcile rides on, and the reason no new seam is needed. |
| `frogdb-server/crates/core/src/shard/post_execution.rs` | — | **Read-only evidence.** The second flush call site, `:383`. |
| `frogdb-server/crates/core/src/shard/active_expiry.rs` | 704 | **Read-only, must NOT be edited.** The field sweep `:194-237`: `get_expired_fields_limited` `:203`, the per-key `purge_expired_hash_fields` `:224`, the `!purged_any` break `:234`. This is where a ghost index entry starves the sweep (§Problem 5). Its `#[cfg(test)]` module (`:243`+) contains the 6 test-only dual-writes at `:268/272` and `:345-349` that become single writes. |
| `frogdb-server/crates/core/src/shard/event_loop.rs` | 1229 | **Primary, test-only, 4 lines.** `:878/880` and `:904/908` are dual-write test fixtures (`hash.set_field_expiry` + `store.set_field_expiry`) that become single value-side writes. The tests themselves (`:797`, `:933`, `:990`) keep their names and assertions. |
| `frogdb-server/crates/persistence/src/serialization/registry.rs` | 711 | **Read-only evidence — not edited.** `encode_hash` `:85` and `encode_hash_with_field_expiry` `:90-93` both dispatch on `hash.has_field_expiries()` — **the value book, never the index**. This is the proof that the value book is the durable one and the index is derived (§Problem 6). Its round-trip test `hash_with_field_expiry_preserves_ttl` `:692-707` is the net. |
| `frogdb-server/crates/persistence/src/serialization/collections.rs` | 257 | **Read-only evidence.** `serialize_hash_with_field_expiry` `:73`, `deserialize_hash_with_field_expiry` `:203` — the wire format carries per-field deadlines from the value book only. |
| `frogdb-server/crates/server/src/commands/search.rs` | 1335 | **Primary, small.** `FT.SUGADD` `:906-923` (two `hash.set` calls) and `FT.SUGDEL` `:1136-1150` (two `hash.remove` calls) mutate hashes value-side with no index maintenance and no purge — the same omission class as HMSET, in a different crate. After the change they need **no** code: the reconcile is automatic. |
| `frogdb-server/crates/redis-regression/tests/hash_field_expire_tcl.rs` | 1718 | **Primary, +3 tests.** `tcl_field_ttl_overridden_by_hset` `:615-657` is the existing passing witness for HSET; the identical test with `HMSET` at `:634` **fails at `:647` today** and is hotfix H1's regression test. §Testability. |
| `frogdb-server/crates/redis-regression/tests/hash_regression.rs` | 501 | **Read-only evidence.** The 5 field-expiry tests (`:256`, `:280`, `:302`, `:373`, `:441`) — all HGETEX/HSETEX/HEXPIRE/HTTL. `hsetex_keepttl_preserves_existing_field_ttl` `:441-467` is the only test that exercises `:2030-2031`, and it passes because the books agree in its scenario. |
| `.scratch/hardening/specs/*.md` | — | **Read-only.** `grep -rn 'hash_field\|HFE\|hexpire\|field.*expir'` over every failure-mode spec returns **nothing**. No `FM-` row governs hash field expiry, so no spec-first obligation attaches and `just lint-failure-modes` is untouched. |

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
representative pair, HGETEX (`commands/src/hash.rs:1883-1924`): a 21-line `match` on
`expiry_action` that mutates the value, then an 18-line `match` on the **same**
`expiry_action` that mutates the index, separated by the comment `// Phase 3: sync store
index`. HSETEX does the same thing three times over (`:2039-2085`), HEXPIRE twice
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
| **HMSET** | `hash.rs:294` → `types/hash.rs:262` | **no** — the handler body is `:279-298`, complete, with no `remove_field_expiry` and no purge | **LIVE ghost producer** |
| **HINCRBY** | `hash.rs:616` → `incr_by` `types/hash.rs:397` → `:262` | **no** — handler body `:610-618` | **LIVE ghost producer** |
| **HINCRBYFLOAT** | `hash.rs:659` → `incr_by_float` `types/hash.rs:434` → `:262` | **no** — handler body `:652-662` | **LIVE ghost producer** |
| **HSETNX** | `hash.rs:132` → `set_nx` `types/hash.rs:306` → `:262` | **no** — but `set_nx` returns early when the field exists (`:303-305`), so it only reaches `set` for absent fields and creates no ghost of its own | **LATENT** (it does not clean up a pre-existing ghost either) |
| **FT.SUGADD** | `server/src/commands/search.rs:919`, `:929` → `:262` | **no** | **LIVE**, scoped to FT suggestion dictionaries |
| **FT.SUGDEL** | `server/src/commands/search.rs:1144`, `:1150` → `types/hash.rs:321` | **no** | **LIVE**, same scope |

Reproduction, three commands:

```
HSET   h f v1
HEXPIRE h 100 FIELDS 1 f      → 1     (both books now hold now+100s)
HMSET  h f v2                 → OK    (value book cleared; index still holds now+100s)
HTTL   h FIELDS 1 f           → 100   ← Redis answers -1
```

The divergence is **one-directional**: the index holds entries the value does not. The reverse
cannot happen — every value-side write in a handler is paired with an index write, and
`install` re-derives the index from the value on every whole-value write. So the whole failure
surface is "ghost index entries", which makes both the ruling and the fix tractable.

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

### 5. A ghost with a past deadline is immortal, and can starve the active sweep

`purge_expired_hash_fields` removes index entries **only for fields the value book reported as
expired** (`hashmap.rs:1424` `hash.remove_expired_fields(now)` → `:1437-1440` `for field in
&removed_fields { self.field_expiry_index.remove(key, field); }`). A ghost is unknown to the
value book, so `remove_expired_fields` never returns it and the index entry is never removed.
Once its deadline passes, **nothing in the system can delete it** short of `DEL`/overwrite of
the whole key (which routes through `uninstall:471`) or a restart.

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

With `batch_size` or more ghosts at the head of `by_time`, every batch in every cycle consists
entirely of ghosts, `purge_expired_hash_fields` returns 0 for each, and the sweep breaks
without ever reaching a genuinely expired field behind them. **Field TTLs on every other key in
the shard stop being actively reaped**; they are only cleaned up if a client happens to run one
of the 16 commands that carry a purge call against that key. Memory for untouched hashes is
never reclaimed.

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
/// Invariant (I1): after this returns, for every field `f`,
///   field_expiry_index.get(key, f) == data[key].as_hash().get_field_expiry(f)
fn reconcile_field_expiries(&mut self, key: &[u8]) {
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
`core/src/noop.rs` (`remove_key` then `set` each — 10 lines, reusing `:192-207` and `:226-233`).

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

### (c) Purge-on-read moves into the typed hash accessors

`HashValue` leaves the `typed_family_accessors!` list (`store/typed.rs:269`); `get_hash`,
`get_hash_mut`, `check_hash` and `get_or_create_hash` become four hand-written methods on
`StoreTypedFamilyExt`:

```rust
/// Typed read access to the hash at `key`, with lazy field expiry applied.
///
/// The single purge-on-read seam: a field past its TTL is physically removed
/// before any caller can observe it, so "a hash read never sees an expired
/// field" is a property of this function rather than a checklist discharged at
/// 13 call sites (and forgotten at 13 more). The reap's observable effects are
/// reported through the existing `take_lazily_{expired_fields,shrunk,emptied}`
/// buffers, which the worker already drains after every command.
fn get_hash(&mut self, key: &[u8]) -> Result<Option<TypedArc<HashValue>>, WrongTypeError> {
    if self.has_field_expiries(key) {
        self.purge_expired_hash_fields(key);
    }
    self.get_typed::<HashValue>(key)
}
```

`get_hash_mut` and `get_or_create_hash` take the same prelude — in `get_or_create_hash` it must
run **before** the create-if-missing check so a hash emptied by the purge is recreated fresh,
matching the existing `purge_if_expired` ordering documented at `typed.rs:192-197`.
`check_hash` does not purge: it is a type probe (`typed.rs:176-190`) and a purge would make a
destination check mutate the destination.

Hand-writing four methods rather than adding a `purge_fields` flag to the macro is deliberate:
one family behaving differently from thirteen is clearer as four visible methods than as a
conditional in a macro that generates fifty-six.

The **13 purge calls in `commands/src/hash.rs` are deleted**, and the 13 commands that never
had one — **HSET, HSETNX, HDEL, HMSET, HINCRBY, HINCRBYFLOAT, HEXPIRE, HPEXPIRE, HEXPIREAT,
HPEXPIREAT, HPERSIST, HSETEX**, plus **FT.SUGADD/FT.SUGDEL** in `frogdb-server` — get correct
lazy field expiry for free. That closes a second family of wire-visible defects that this audit
found but does not otherwise treat: today `HINCRBY h f 1` on a field whose TTL elapsed
increments the **stale** value instead of starting from 0, and `HSETNX h f v` on such a field
returns 0 without writing.

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
unaffected; in fact the change **removes** a clock read from `frogdb-commands` (`hash.rs:1079`
keeps its own, unrelated to the purge).

### (e) The invariant, stated once, in the place that owns it

> **I1 — the field-expiry index is derived.** For every key *k* and field *f*:
> `field_expiry_index.get(k, f) == data[k].as_hash().get_field_expiry(f)`.
> The index has no public writer; it is established by `install` (whole-value write), dropped
> by `uninstall` (removal), narrowed by `purge_expired_hash_fields` (reap), and re-derived by
> `reconcile_field_expiries` (in-place mutation). No code outside `HashMapStore` may write it.

Enforced structurally (no public mutator survives), and checked dynamically by a
`debug_assert` at the end of `reconcile_field_expiries` that the two agree — which turns every
existing hash test in the workspace into a books-agree test under `cfg(debug_assertions)`.

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

**(R4) Fix only the three handlers (H1) and stop.** This is the hotfix, and it should land
first (§Effort). It is not the proposal because it leaves 17 hand-paired writes, 13 hand-placed
purges, three disagreeing readers, and 13 commands with no lazy field expiry — i.e. it fixes
the three instances and none of the mechanism.

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
  call reappears, inside `get_hash`/`get_hash_mut`/`get_or_create_hash`. **Thirteen become
  three, and thirteen commands that never had one are fixed by the same move.**
- **The `or_else` at `hash.rs:2031`** — delete it and nothing reappears; under I1 it is
  unreachable, and under the current code it is a data-loss path (§Problem 4). **Pure
  deletion.**
- **`install`'s re-derivation (`hashmap.rs:388-396`) and `uninstall`'s `:471`** — delete either
  and the key-level failure the banner at `:352-355` describes returns, in field form.
  **Earn their keep; untouched — they are the model `reconcile_field_expiries` copies.**

## Testability improvement

**1. I1 becomes a one-line assertion instead of a 17-site audit.** Today "the two books agree"
cannot be tested as a property, because there is no seam it could be checked at — you would
have to assert after each of the 30 field-expiry statements in `commands/src/hash.rs`. After
the change it is checked once, in `reconcile_field_expiries`, under `debug_assert`, and **every
existing hash test in the workspace becomes a books-agree test for free** (`just test
frogdb-core`, `just test frogdb-commands`, the 39 tests in `hash_field_expire_tcl.rs`, the 25
in `hash_regression.rs`). The property test that would find today's bug is writable today —
and this proposal's claim is precisely that **it would fail today**, which is why H1 exists.

**2. The live bug's regression test already exists in template form, four lines away.**
`tcl_field_ttl_overridden_by_hset` (`hash_field_expire_tcl.rs:615-657`) sets two field TTLs,
overwrites one with `HSET` at `:634`, and asserts `HTTL` returns `-1` at `:647`. Replace `HSET`
with `HMSET` and it fails at `:647` today. Three tests —
`tcl_field_ttl_overridden_by_hmset`, `…_by_hincrby`, `…_by_hincrbyfloat` — are ~40 lines total
and land with H1, independent of the rest of the proposal.

**3. Purge-on-read becomes testable once instead of per-command.** Today "a hash read never
observes an expired field" is discharged at 13 sites and violated at 13 more, and the suite has
**no** test for the violated ones: `grep` over `hash_field_expire_tcl.rs` and
`hash_regression.rs` finds field-expiry coverage only for HGETEX/HSETEX/HEXPIRE-family/HTTL/
HPERSIST. After the change it is one seam, and three new tests pin the three most damaging
gaps: `HINCRBY` on an elapsed field must start from 0 (today it increments the stale value),
`HSETNX` on an elapsed field must return 1 and write, `HDEL` on an elapsed field must return 0.
These are **behaviour fixes**, so they are stated here as part of the change's acceptance
criteria, not as free wins.

**4. The starvation failure becomes reachable in a unit test.** `active_expiry.rs`'s test
module already builds dual-book fixtures by hand (`:257-273`, `:340-349`, comment: *"both on
the value and in the store's field-expiry index, matching HEXPIRE"*). After the change those
fixtures become single value-side writes, and the interesting scenario — an index entry the
value does not back — becomes constructible **only** by reaching inside `HashMapStore`, which
is the point: the state is no longer reachable from any command. A `#[test]` that constructs it
directly and asserts the sweep still drains behind it is the standing guard.

**5. A store-level golden.** `store/hashmap.rs`'s existing trio
(`set_overwrite_clears_stale_field_expiry_index` `:1818`,
`set_indexes_field_expiries_carried_by_the_value` `:1843`,
`set_with_options_overwrite_clears_stale_field_expiry_index` `:1865`) already pins I1 for the
`install` path. The change adds the in-place sibling —
`in_place_mutation_reconciles_field_expiry_index` — which is the test that does not exist today
because the behaviour does not exist today.

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

**vs proposal 90 (`CommandSpec::DEFAULT`, solo-last sweep over `frogdb-commands`).** **Real
file conflict, disjoint regions.** 90 rewrites all 26 `static SPEC: CommandSpec` literals in
`commands/src/hash.rs` (it cites `hash.rs:37-38` as its shape example); 93 edits only `execute`
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

**vs proposal 83 (lazy-expiry authority).** Adjacent by name, disjoint by scope: 83 concerns
**key-level** TTL authority (`check_and_delete_expired`, `purge_if_expired`,
`take_lazily_purged`), 93 concerns **field-level**. The two share `store/hashmap.rs` and
`store/mod.rs`. 93's edits there are confined to `:388-396` (untouched), `:610-630` (the
flush), `:1364-1382` (deleted), `:1392-1415` (the purge guard) and the trait block at
`store/mod.rs:646-691`. **If 83 restructures the `take_lazily_*` family (`store/mod.rs:511-575`)
this is a live conflict** — those four docs sit 70 lines above 93's trait deletions in the same
file. Flag for the orchestrator; the resolution is textual, not semantic.

### Other risks

- **Behaviour changes, deliberately.** Purge-on-read at the accessor seam changes 13 commands'
  observable behaviour on elapsed fields (HINCRBY, HSETNX, HDEL, HMSET, HEXPIRE-family,
  HPERSIST, HSETEX, HSET). Every change moves *toward* Redis, but they are changes, and each
  needs its own regression test before merge. This is the largest risk in the proposal and the
  reason it is **M** rather than **S**.
- **`FT.SUG*` share the hash type.** `search.rs:906`/`:1136` mutate hashes through the same
  accessors, so they inherit purge-on-read. Suggestion dictionaries have no field TTLs unless a
  client sets one on the underlying key, so the practical effect is the `has_field_expiries`
  probe returning false. Named because it is a cross-crate behaviour change, not because it is
  expected to bite.
- **`get_hash` becomes a mutation.** It already is, at 13 call sites, and the reporting
  contract (`take_lazily_expired_fields` / `take_lazily_shrunk` / `take_lazily_emptied`,
  `store/mod.rs:526-575`) is unchanged — the worker drains those after every command
  regardless. What changes is that a READONLY command can now reap on *any* hash accessor path,
  including ones in `frogdb-server`. The buffers are drained at the same seam either way.
- **`debug_assert` cost.** I1's check clones nothing but iterates the key's expiries. Debug
  builds only; if the concurrency suites slow measurably, gate it behind
  `cfg(feature = "expensive-assertions")`.
- **Mutation-score exposure: none.** No locked crate is edited (`frogdb-core`,
  `frogdb-commands`, `frogdb-types`, `frogdb-server` — the four locked pairs are txn/vll,
  persistence/recovery, replication/replication-runtime, cluster/cluster-runtime, ADRs
  `0002`–`0004`). `frogdb-persistence` is *read as evidence only*; **no file in it is edited**,
  so no `just mutants-diff` obligation attaches.

## Spec / gates

- **Failure-mode specs: silent.** `grep -rn 'hash_field\|HFE\|hexpire\|HEXPIRE\|field.*expir'`
  over `.scratch/hardening/specs/*.md` returns nothing. No `FM-` row covers hash field expiry,
  no edited file contains an `FM-` tag, `just lint-failure-modes`
  (`scripts/failure-modes.py:243`) sees no renamed/moved/retagged test. **Unaffected.**
- **Seam lints (`Justfile:329`).** `lint-clock-seam`: the purge's clock read stays
  `crate::clock::now()` (`hashmap.rs:1394`) and the change adds none — net −0.
  `lint-metrics-chokepoint`: `lazily_expired_fields` (`hashmap.rs:1435`) and its worker drain
  (`shard/worker.rs:742-743`) are untouched; the counter is bumped from the same statement.
  `lint-no-typed-unwrap` (commands-scoped): the change **removes** `ctx.store.get(key).unwrap()`
  /`as_hash().unwrap()` pressure at `hash.rs:2006-2007` rather than adding any. The remaining
  twelve gates cover redirects, pubsub confirmation, failover atomicity, INFO, float format,
  durable-ack, nested config, error sanitisation, keyspace-notify routing, script gating and
  continuation locks — none reachable from this diff.
- **Feature profile.** `pub mod hash;` (`commands/src/lib.rs:39`) is ungated — hash is in
  `core-profile`, so `just test frogdb-commands` and `just test frogdb-core` cover the change
  without `cmd-full`.
- **Docs.** `grep` the website for hash-field-TTL architecture prose before merge; the
  behaviour changes in §Risks are user-visible and belong in the Redis-compatibility deltas
  page if one documents HFE.

## Effort

**M.** Twelve handler bodies edited in `commands/src/hash.rs` (all deletions), four methods
hand-written in `store/typed.rs`, ~24 lines added in `store/hashmap.rs`, ~15 in `core/src/noop.rs`,
five methods deleted from the `Store` trait, ~10 test-fixture lines simplified in
`core/src/shard/{event_loop,active_expiry}.rs`. Net ≈ **−115 production lines**, plus ~6 new
tests. The size is breadth, not depth: once I1 is stated, every edit is forced.

### Independently-landable hotfix H1 — LIVE, 9 lines, ships today

Mirror HSET's index clear (`commands/src/hash.rs:85-88`) into the three handlers that omit it.
No design change, no interface change, no dependency on the rest of this proposal:

- **HMSET** — after the `get_or_create` block closes (`hash.rs:295`), add the loop
  `for chunk in args[1..].chunks(2) { ctx.store.remove_field_expiry(key, &chunk[0]); }`.
- **HINCRBY** — after `:616`, add `ctx.store.remove_field_expiry(key, &args[1]);`.
- **HINCRBYFLOAT** — after `:659`, the same.

Each needs the `hash` borrow released first — `incr_by` already returns an owned `i64`/`f64`,
so the borrow ends at the semicolon in both increment cases; HMSET's `hash` is already scoped
by the `let` at `:289` and needs a block, exactly as HSET's `:71-83` is scoped today.

**Regression tests:** three copies of `tcl_field_ttl_overridden_by_hset`
(`hash_field_expire_tcl.rs:615-657`) with `HSET` at `:634` replaced by `HMSET myhash field2
value4`, `HINCRBY myhash field2 1` (seeding an integer value) and `HINCRBYFLOAT myhash field2
1.5`. Each asserts `HTTL … field2 → -1` at the `:647` position and **fails before the fix**.

**H1 closes:** the wrong `HTTL`/`HPERSIST`/`HEXPIRE`-condition answers (§Problem 3), the
`HSETEX KEEPTTL` data-loss path (§Problem 4), and the active-sweep starvation (§Problem 5) —
for the three commands that produce ghosts. It does **not** close: the FT.SUG* pair, the 13
commands with no lazy field expiry, or the ability for the fourteenth handler to make the same
omission tomorrow. That is what the proposal is for.

### Independently-landable hotfix H2 — LATENT, 4 lines, defensive

Make `purge_expired_hash_fields` evict index entries the value book does not back, so any ghost
that already exists in a running process is reaped rather than immortal: after the
`remove_expired_fields` call (`hashmap.rs:1424`), drop every indexed field for `key` whose
deadline is past and which `hash.get_field_expiry` reports as absent. Strictly a belt-and-braces
companion to H1 — with H1 in place no new ghosts are created, and with the full proposal in
place the state is unreachable — but it converts an immortal ghost into a self-healing one for
processes that have been running since before H1. Ship only if operators cannot restart.
