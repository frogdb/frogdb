# Proposal 97 — 69 command reads reach the store through a door the store documents as expiry-blind; the server counts the key as a miss, fires `keymiss`, and then answers from the corpse

Round 38 · lane: commands + core store · candidate **CT4** · effort **L** (**M** if the rename
and the extension families are deferred — §Effort) · **no locked crate edited by the core
change**; the optional rename touches **3 test-only lines in `frogdb-recovery`** (locked) and
that cost is priced in §Risks · **zero `FM-` tags in `crates/commands/src`** (verified: `grep -rn
'FM-' frogdb-server/crates/commands/src` returns nothing) · **one commands-scoped seam gate
(`lint-no-typed-unwrap`) is in scope — and it overclaims** (§Spec / gates)

**Verified at HEAD `04b27dc81679f4e25fa49bf253e832453a46fed0`** (worktree `arch-round-38-99`,
branch `worktree-arch-round-38-99`). Every count and every `file:line` below was re-derived at
this SHA by exact-string grep or by reading the cited region. Nothing is inherited from the lane
brief; three of its numbers are corrected and one framing is refuted. No code file in this
proposal's set is dirty — the only modified paths in the tree are three
`.scratch/arch-deepening/proposals/*.md` held by concurrent authors.

**Ruling on the brief's "latent-to-LIVE, verify honestly" framing: LIVE, and wider than the
brief suspected.** The brief guessed LLEN/LRANGE/BLPOP. All three are confirmed, and the same
static trace produces four more defect families, one of which **resurrects expired data
permanently** and one of which **silently destroys a client's element**. Deterministic
reproductions using only `DEBUG SET-ACTIVE-EXPIRE 0` — a command the regression suite already
drives at 30+ sites — are given for each in §Problem. **Four** independently-landable hotfixes
are specified in §Effort (rev 2 adds **H1b**: the data-loss shape has an unfixed twin in
`frogdb-core`'s own blocked-client path).

## Corrections to the lane brief

| Brief claim | Verified at HEAD |
|---|---|
| "`Store::get` does no expiry check (`typed.rs:118-130` doc, `hashmap.rs:934-943` impl)" | **Confirmed exactly.** `HashMapStore::get` is `hashmap.rs:934-943`: hot fast-path, then `unspill_key`; no `check_and_delete_expired`. The doc is at `typed.rs:120-130` (the `# Expiry` block of `get_typed_mut`; the brief's `:118` is the two lines above it). Better evidence the brief missed: the store has a **standing unit test that pins the bypass as intended behaviour** — `hot_expired_key_get_vs_get_with_expiry_check_contract` (`hashmap.rs:2104-2140`), whose comment reads *"Callers that need Redis's 'expired reads as absent' semantics (scatter MGET, COPY, DUMP) must use `get_with_expiry_check`, **never raw `get`**"*. The contract is written down, tested, and violated 69 times one crate away. |
| "There is NO blanket pre-dispatch purge — `execution.rs:191` is an `exists_unexpired` probe only" | **Confirmed, and it is worse than "only a probe".** `execution.rs:191` is inside the keyspace hit/miss seam (`:171-214`): it calls the **non-mutating** `exists_unexpired` (`store/mod.rs:421-423`, `&self`), increments `misses`, and **emits the `keymiss` keyspace notification** (`:194-205`). Nothing between that probe and `handler.execute` at `:241` purges anything (read `:215-241`). So for `LLEN` on an expired key the server increments `keyspace_misses`, tells every `keymiss` subscriber the key was not found, and then hands the handler a store that still contains it. §Problem 2. The only two `purge_if_expired` sweeps over a command's keys in the whole runtime are `dispatch_core.rs:121` (WATCH/`GetVersion`) and `worker.rs:681` (`purge_expired_watches` at EXEC) — both watch-set-scoped, neither on the dispatch path. |
| "69 raw `ctx.store.get()` call sites" | **Confirmed exactly, 69.** Per-file tally in §Problem 1. |
| "58 raw `as_*()` downcasts" | **Adjusted up to 60**, and the derivation restated exactly. `impl_value_accessors!` (macro `types/src/types/mod.rs:71-95`, invocation `:97-112`) lists **14 families** and generates **28 methods** (14 `as_X` + 14 `as_X_mut`); `as_vectorset`/`as_vectorset_mut` are hand-written alongside (`:116`, `:124`) and have **zero** callers in `frogdb-commands`. The 60 uses in `commands/src` are **42 shared + 18 mut**: `as_string` 13, `as_list` 10, `as_sorted_set` 9, `as_set` 4, `as_stream` 3, `as_json`/`as_hash`/`as_cms` 1 each (= 42); `as_string_mut` 8, `as_list_mut` 7, `as_sorted_set_mut` 3 (= 18). Eleven distinct methods of the 28. |
| "46 hand-rolled WrongType constructions" | **Adjusted: 46 is the grep count of the *string*; 39 are constructions.** `grep -rn 'WrongType' commands/src` → 46 lines; `grep -rn 'CommandError::WrongType'` → **39**, the other 7 being prose. All 39 are the `} else { Err(CommandError::WrongType) }` shape; **zero** are `.ok_or(…WrongType)`. That distinction is the whole finding in §Problem 6 — it is exactly why `lint-no-typed-unwrap` passes today. |
| "Worst file: `blocking.rs` (905 lines, 0 tests, 14 raw gets)" | **Confirmed exactly on all three numbers.** `wc -l` = 905; `grep -c '#\[test\]\|#\[cfg(test)\]'` = 0; 14 raw gets at `:67`, `:150`, `:235`, `:259`, `:261`, `:391`, `:491`, `:576`, `:704`, `:806`, `:827`, `:829`, `:889`, `:899`. It is also the file with the **most dangerous** shape, not merely the most instances (§Problem 3). |
| "The correct deep seam already exists: `StoreTypedExt` (`get_typed`/`check_typed`)" | **Confirmed, `core/src/store/typed.rs`.** `get_typed` composes `get_with_expiry_check` (`:169`); `get_typed_mut` (`:135`), `check_typed` (`:182`) and `get_or_create_typed` (`:202`) each call `purge_if_expired` up front. Five existing unit tests pin the expiry behaviour (`typed.rs:484-543`). The seam is correct, complete, tested, and **the 14 family wrappers it generates are already used by most of the crate** — this proposal is a migration, not a design. |
| "Rename `Store::get` → `get_unchecked` so a raw get is greppable/lintable" | **Endorsed with a cost the brief did not price — and the first draft of this proposal understated it ~5×.** Re-derived at HEAD: `store.get(` has **153** textual matches workspace-wide (69 `commands/src`, **49 `core/src`**, 18 `testing/src`, 9 `core/tests`, **3 `recovery/src`**, 1 `server/tests`, 1 `testing/fuzz`, 3 `benchmarks`). Of the 49 in `core/src`, **26 are real production `Store::get` calls**, not 3 — the full inventory and a LIVE/LATENT ruling for each is in §Problem 7. `recovery/src/tests.rs:204`, `:624`, `:1454` are real `Store::get` calls in a **locked crate**. §Risks prices this and offers a rename-free variant. |
| "Effort M-L" | **L**, delivered as five commits, the first three of which are the hotfixes. **M** if the rename (commit 5) and the eight extension families are deferred. §Effort. |
| — *(not in the brief)* | **A third expiry-blind primitive: `Store::contains`.** 14 command sites call `ctx.store.contains(key)` (`store/mod.rs:411`, `&self`, a bare map probe with *no* expiry logic at all — strictly blinder than `get`). `MSETNX` (`string.rs:918`) and `COPY` without `REPLACE` (`generic.rs:592`) both answer `0` ("a key was in the way") against a key that is past its deadline. §Problem 5. |
| — *(not in the brief; **added in rev 2**)* | **The `blocking.rs` data-loss shape has an unfixed twin in `frogdb-core` itself.** `core/src/shard/blocking.rs:790-793` is the blocked-then-woken BLMOVE deposit and repeats §Problem 3's B2 exactly (`store.get(dest).is_none()` → skip create; `store.get_mut(dest)` → purge → `None` → push never runs), and `:763` repeats B1 (raw `get(dest)` type-probe → `WRONGTYPE` on wake against a corpse). `ListSatisfaction::check_key` (`:671`) purges the **source** key only. §Problem 3c, hotfix **H1b**. |
| — *(not in the brief; **added in rev 2**)* | **~20 raw-`get` sites sit in `READONLY`-flagged handlers, so the migration *does* make read commands mutating.** Rev 1's §Risks claimed "no `READONLY` command gains a mutation"; that is true only of `exists_for_write`'s Class C sites. At whole-change level it is **false** — §Risks now states it correctly and records the resulting **HARD** ordering **83 → 97**. |
| — *(not in the brief; **added in rev 2**)* | **A fourth blind primitive: `Store::get_metadata`.** `hashmap.rs:1542-1544` is `self.data.get(key).map(…)` — no expiry, `&self`. `OBJECT FREQ` (`generic.rs:441`) and `OBJECT IDLETIME` (`generic.rs:452`) both read through it, so both answer for a corpse where Redis answers "no such key". Two sites, outside the 69 + 14. §Problem 5. |
| — *(not in the brief)* | **`lint-no-typed-unwrap` prints a claim it does not check.** Its success line is `OK: no check-then-unwrap or hand-rolled WrongType in crates/commands` (`Justfile:1039`), but its two patterns only match the **`_mut`** forms. Eight immutable check-then-unwraps survive it (`hash.rs:2006-2007`, `stream/pending.rs:262-263`, `:362-363`, `:420-421`) and all 39 hand-rolled WrongType sites survive it. §Problem 6, hotfix **H3**. |

## Summary

`frogdb-core` exposes two ways to read a value out of the store and states, in a doc comment and
in a unit test, that only one of them is safe:

- **`Store::get`** (`store/mod.rs:398-402`, impl `hashmap.rs:934-943`) — the raw hot fast-path.
  No expiry check. A key ten minutes past its deadline that the sampled sweeper has not reached
  comes back with its value intact.
- **`Store::get_with_expiry_check`** (`store/mod.rs:495`, impl `hashmap.rs:1132-1163`) — purges
  the key if it is due, reports absent, touches LRU/LFU.

The type-safe layer above them, `StoreTypedExt` (`store/typed.rs:114-214`), routes **every** one
of its four generic methods through the second door, and generates 56 family wrappers
(`get_list`, `check_zset`, `get_or_create_hash`, …) from them. It is the seam. It works. It is
tested five ways (`typed.rs:484-543`).

**69 command handlers walk past it into the first door**, and 14 more reach past both into
`Store::contains`, which is blinder still. There is no pre-dispatch purge to cover them: the
execution seam's only per-key expiry work is a **non-mutating** `exists_unexpired` probe
(`execution.rs:191`) whose entire output is two counters and a `keymiss` notification. So a
single `LLEN` against an expired list produces, in one command:

```
DEBUG SET-ACTIVE-EXPIRE 0
RPUSH mylist a b c        → 3
PEXPIRE mylist 50         → 1
… 100 ms …
EXISTS mylist             → 0       ← exists_unexpired  (basic.rs:885)
TYPE   mylist             → none    ← exists_unexpired  (generic.rs:52)
LLEN   mylist             → 3       ← raw get           (list.rs:383)      Redis: 0
LRANGE mylist 0 -1        → a b c   ← raw get           (list.rs:427)      Redis: (empty)
INFO stats                → keyspace_misses incremented by every one of the four
```

The proposal is not to add a check. **It is to delete the second door from the crate that must
not have it**: migrate the 69 raw reads and the 14 `contains` probes onto the typed seam that
already exists, rename `Store::get` to `Store::get_unchecked` so the remaining legitimate users
(warm unspill in rollback, the seam's own internal type-probe) are named as such, and add the
one grep gate that keeps the door shut. The 60 `as_*()` downcasts and the 39 hand-rolled
`WrongType` constructions come out in the same motion, because a raw read is the only reason
to have either.

Net effect: `frogdb-commands` stops seeing the `Value` enum outside the handful of genuinely
polymorphic commands the seam's own doc already carves out (`typed.rs:111-113`: *"TYPE, OBJECT,
RENAME, DEBUG, … are the only ones that should still see `Value`"*), the check-then-project
shape disappears from ~40 handlers, and "a command never observes an expired key" becomes a
property of one function instead of a convention discharged correctly 200-odd times and
forgotten 83.

## Files involved

| Path | Lines | Role in this change |
|---|---|---|
| `frogdb-server/crates/core/src/store/typed.rs` | 584 | **Primary, small (~30 lines added).** The seam. `StoreTypedExt` `:114-214`, `get_typed_mut` `:131-150`, `get_typed` `:165-174`, `check_typed` `:181-190`, `get_or_create_typed` `:198-213`; `typed_family_accessors!` `:222-264` and its 14-family list `:266-284`. This proposal **adds one method** (`exists_for_write`, §(b)) and **changes no existing behaviour here**. The module banner `:1-23` gains the expiry paragraph. Line `:107` is a doc reference **proposal 91 also edits** — §Risks. The "only TYPE/OBJECT/RENAME/DEBUG should still see `Value`" carve-out is `:110-112`. Its 5 expiry tests `:484-543` are the model for the new ones. **`get_or_create_typed` `:198-213` carries a pre-existing CLIENT-PAUSE hole this proposal routes ~21 new call sites through — §Risks, "Pre-existing defects 97 inherits".** |
| `frogdb-server/crates/core/src/store/mod.rs` | 830 | **Primary, ~20 lines.** `Store::get` `:398-402` → `get_unchecked` (rename + a doc that names the two legitimate callers). `contains` `:411`, `exists_unexpired` `:413-423`, `get_with_expiry_check` `:494-497`, `purge_if_expired` `:499-509` are **read-only evidence and unchanged**. **Proposal 93 deletes four methods at `:646-667` in this file** — 200+ lines away; §Risks. |
| `frogdb-server/crates/core/src/store/hashmap.rs` | 2977 | **Primary, rename only.** `impl Store` `:933`, `get` `:934-943`, `set` `:945-951` (doc `:946-948`), `contains` `:957-959`, `exists_unexpired` `:961`, `get_with_expiry_check` `:1132-1163`, `get_mut` `:1298-1345` (**note `:1299-1301`: `get_mut` *does* purge — the asymmetry §Problem 3 turns on**), `purge_if_expired` `:1166-1168`, `check_and_delete_expired` `:480-498` (**note `:485-487`: under CLIENT PAUSE it returns `true` without deleting** — §Risks), `get_metadata` `:1542-1544` (the fourth blind primitive), `get_expired_keys_limited` `:1356-1358` (**index-driven**, not sampled — §Problem 2). The contract test `hot_expired_key_get_vs_get_with_expiry_check_contract` `:2104-2140` is **kept verbatim**, renamed to match the new method name; it is the anchor of the whole argument. Three `FM-PERSISTENCE-044` tags at `:2608`, `:2638`, `:2666` — untouched regions. |
| `frogdb-server/crates/commands/src/blocking.rs` | 905 | **Primary, worst file.** 14 raw gets (`:67`, `:150`, `:235`, `:259`, `:261`, `:391`, `:491`, `:576`, `:704`, `:806`, `:827`, `:829`, `:889`, `:899`), 10 hand `WrongType`, **0 tests**. Eight handlers share one `get`→`key_type()`→`as_X()`→`get_mut()` shape; §Problem 3 shows it splits its brain across the expiry boundary. Hotfix **H1** lives at `:259-263` and `:827-831`. **Overlaps 94** (`:512`, `:597`, `:725`) and **80** (`:95`, `:177`, `:294`, `:435`, `:520`, `:605`, `:749`, `:852`) — §Risks. |
| `frogdb-server/crates/commands/src/sorted_set/set_ops.rs` | 802 | **Primary, 12 raw gets** (`:178`, `:268`, `:345`, `:358`, `:457`, `:473`, `:584`, `:597`, `:669`, `:679`, `:764`, `:777`) — every ZUNION/ZINTER/ZDIFF source read, `-STORE` variants included. Class A (§Problem 2). |
| `frogdb-server/crates/commands/src/bitmap.rs` | 668 | **Primary, 6 raw gets.** `:123` GETBIT, `:194` BITCOUNT, `:267` BITOP source, `:365` BITPOS — Class A. `:503`/`:506` **BITFIELD — the resurrection site** (§Problem 4), hotfix **H2**. **`execute_bitfield` (`:492-495`) is shared: `BitfieldCommand::execute` calls it with `readonly=false` (`:418`), `BitfieldRoCommand::execute` with `readonly=true` (`:487`), and BITFIELD_RO's spec is `flags: READONLY`, `wal: NoOp`, `lookup: LookupSpec::None` (`:466-483`). H2's purge must therefore be gated `if !readonly` — §Effort.** |
| `frogdb-server/crates/commands/src/string.rs` | 1805 | **Primary, 3 raw gets + 3 `contains`.** `:246` STRLEN, `:290` GETRANGE, `:1217` GETSET (Class A); `:918` MSETNX, `:1509`, `:1518` (Class D, §Problem 5). 13 hand `WrongType` — the densest in the crate. |
| `frogdb-server/crates/commands/src/list.rs` | 1144 | **Primary, 4 raw gets:** `:383` LLEN, `:427` LRANGE, `:476` LINDEX, `:745` LPOS. The brief's headline, confirmed. |
| `frogdb-server/crates/commands/src/sort.rs` | 1124 | **Primary, 4 production raw gets** (`:126` the SORT source via `extract_elements`, `:169`/`:184`/`:198` the `BY`/`GET` pattern lookups) + 1 test-only (`:919`, inside `#[cfg(test)]` at `:553`). The pattern lookups are Redis `lookupKeyRead` semantically, so an expired weight key must sort as absent, not as its stale value. |
| `frogdb-server/crates/commands/src/generic.rs` | 736 | **Primary, 2 raw gets + 3 `contains` + 2 `get_metadata`.** `:362` OBJECT ENCODING, `:598` COPY source (Class A); `:592` COPY dest-without-REPLACE and `:466` OBJECT REFCOUNT (Class C/D). **`:197` RENAME dest is NOT a defect and rev 1 was wrong to list it** — `:196` is `let _ = ctx.store.get_with_expiry_check(new_key);`, which purges immediately before the `contains` at `:197`. It is the crate's own template for the fix. `:441` OBJECT FREQ and `:452` OBJECT IDLETIME read `get_metadata` (no expiry at all) — two further blind probes. Contains the *correct* pattern for contrast twice more: TYPE's comment `:48-51` and its `exists_unexpired` call at **`:52`**. |
| `frogdb-server/crates/commands/src/{bloom,cuckoo,topk,tdigest,cms,timeseries}.rs` | 758 / 839 / 425 / 689 / 434 / 1368 | **Primary, the "Key already exists" family — 7 sites, one shape.** `bloom.rs:95`, `cuckoo.rs:84`, `topk.rs:103`, `tdigest.rs:96`, `cms.rs:41`, `cms.rs:106` are byte-identical (`"Key already exists"`); **`timeseries.rs:134` is the same *shape* but a different message (`"TSDB: key already exists"`)** — same defect, so a text-matching migration script must not assume seven identical strings. §Problem 4b. Plus `cms.rs:376`, `tdigest.rs:272`, `timeseries.rs:1254` (Class A/D). **Feature-gated**: these families need `full`/`cmd-full` — §Spec / gates. |
| `frogdb-server/crates/commands/src/{set,geo,json/basic,stream/basic,stream/pending,stream/consumer_groups,sorted_set/basic,hash,basic}.rs` | 1127 / 1372 / 467 / 519 / 458 / 406 / 416 / 2327 / 1054 | **Primary, the tail.** `set.rs:24` (`get_set_inline`, the shared set-read helper — one edit fixes every SET command that uses it); `json/basic.rs:79` (JSON.SET NX, spec `:23`), `:323` (**JSON.MGET, spec `:298` — rev 1 mis-attributed this to JSON.GET, whose handler at `:132-252` has no raw get**); `stream/pending.rs:262` (**XCLAIM**, spec `:131`), `:362`, `:420` (**XAUTOCLAIM**, spec `:300`) — three immutable check-then-unwraps; **rev 1 mis-attributed all three to XPENDING, whose handler (spec `:21`) has no raw get at all**; `stream/basic.rs:92` (XADD NOMKSTREAM), `stream/consumer_groups.rs:110`; `geo.rs:150`, `:417`; `sorted_set/basic.rs:172`; `hash.rs:2006-2007` (**the fourth immutable check-then-unwrap; proposal 93 also names it**); `basic.rs:613`. |
| `frogdb-server/crates/core/src/shard/execution.rs` | 2132 | **Read-only evidence — not edited.** The dispatch seam: `is_write` `:154`, the lookup probe `:171-214` (`exists_unexpired` `:191`, `keymiss` emit `:194-205`), the `new`-event snapshot `:223-232`, `handler.execute` `:241`. This is the proof that no blanket purge exists. |
| `frogdb-server/crates/core/src/shard/dispatch_core.rs` | 535 | **Read-only evidence.** `CoreMsg::GetVersion` `:101-137`; the only pre-handler `purge_if_expired` loop in the runtime (`:120-122`), scoped to a WATCH argument list. |
| `frogdb-server/crates/core/src/shard/worker.rs` | 1034 | **Read-only evidence.** `purge_expired_watches` `:679-686`, the second and last such loop, scoped to the EXEC watch set. **Proposal 83 rewrites `:738-851` in this file** — no overlap, but a real coupling; §Risks. |
| `frogdb-server/crates/core/src/shard/active_expiry.rs` | 704 | **Read-only evidence.** `DEFAULT_BUDGET` 25 ms `:24`, `DEFAULT_BATCH_SIZE` 1024 `:34`, `run_cycle` `:152`, the key-level batch loop `:156-192`. **Correction to rev 1: the sweep is *index-driven*, not sampled** — it pulls due keys from an ordered expiry index (`store.get_expired_keys_limited` → `hashmap.rs:1356-1358` → `expiry_index.get_expired_limited`), unlike Redis's random 20-key sample. It is time-budgeted, so the window is ordinarily ≈100 ms (§Problem 2) and only unbounded in the three named cases. (`basic.rs:882` and `generic.rs:48` both *say* "sampled"; those comments are themselves inaccurate — a doc nit worth fixing in commit 3.) |
| `frogdb-server/crates/core/src/shard/event_loop.rs` | 1229 | **Read-only evidence.** `expiry_interval` = 100 ms `:24`; `run_active_expiry` `:224` returns early under CLIENT PAUSE `:232` **after** `set_expiry_suppressed(true)`, which also makes `check_and_delete_expired` report expired-but-not-delete (`hashmap.rs:485-487`). This is the one state in which *both* expiry paths stall. **Proposal 83 rewrites this file** — §Risks. |
| `frogdb-server/crates/core/src/shard/blocking.rs` | 2090 | **Primary in rev 2 — the file rev 1 consciously excluded.** 9 raw `Store::get` sites; three are the same defect shapes as `commands/src/blocking.rs`: **`:763`** (BLMOVE dest type-probe on wake = B1 twin), **`:790`/`:793`** (BLMOVE dest deposit = B2 twin, **data loss**) — hotfix **H1b**. `ListSatisfaction::check_key` `:671`, `ZsetSatisfaction` `:914`, stream `:1079` purge the **source** key only. `apply_restore` `:430`/`:445`/`:474` repeats the shape but is LATENT (§Problem 3c). `:1082`/`:1100`/`:1134`/`:1208`/`:1218` are post-`check_key` reads on the just-purged key — LATENT. **83 also edits this file** — §Risks. |
| `frogdb-server/crates/core/src/command.rs` | 2014 | **Read-only evidence.** `pub store: &'a mut dyn Store` at `:1059` (`CommandContextCore`) and `:1262` (`CommandContext`). Combined with `impl Store for HashMapStore` (`hashmap.rs:933`) being the **only** production impl, this proves no purging wrapper sits between a handler and the raw store. **Proposal 91 rewrites this struct** — §Risks. |
| `frogdb-server/crates/core/src/shard/rollback.rs` | — | **Read-only evidence — the reason `get` must survive the rename rather than be deleted.** `:36` and `:59-62`: *"Use `store.get()` which unspills warm keys to hot tier"* — a deliberate expiry-blind read, correct for a rollback snapshot. |
| `frogdb-server/crates/core/src/shard/search_hook.rs` | 193 | **Read-only, flagged not fixed.** `refresh_key` `:75`, `reindex_hash_key` `:128`, `reindex_json_key` `:159` all read through raw `get`. LATENT (they run post-write on a key just written), named because a `lint-store-expiry-seam` scoped only to `crates/commands` deliberately does not cover them. §Problem 7. |
| `frogdb-server/crates/core/src/shard/search/query.rs` | 858 | **Read-only evidence — a plausible LIVE defect outside this proposal's scope.** `:115`, `:208`, `:756` hydrate `FT.SEARCH`/`FT.AGGREGATE` hits by reading each matching doc key through raw `get`. A doc past its deadline that neither the 100 ms sweep nor a lazy read has reaped is **returned as a search result**. Not fixed here (out of the gate's scope, and `frogdb-core`'s search hook owns index lifetime); **filed as a follow-up issue** — §Problem 7. |
| `frogdb-server/crates/core/src/shard/timeseries_execution.rs` | 350 | **Read-only evidence.** `:32`, `:63` resolve `TS.MRANGE`/compaction-rule source keys through raw `get`. Same class as `query.rs`; **LATENT-suspect, needs a ruling** — §Problem 7. |
| `frogdb-server/crates/recovery/src/tests.rs` | — | **LOCKED crate, 3 test-only lines** (`:204`, `:624`, `:1454`), touched **only** by the optional rename in commit 5. Pure identifier substitution. §Risks prices the `mutants-diff` obligation. |
| `Justfile` | 1387 | **Primary, ~35 lines.** `lint-gates` `:329` currently lists **exactly 14** members and gains `lint-store-expiry-seam` as the **15th**; `lint-no-typed-unwrap` (recipe `:1012`, doc from `:998`, body to `:1039`) has its two patterns — at **`:1016`** and **`:1026`**, not `:1017`/`:1027` — widened and its success message (`:1039`) corrected (**hotfix H3**). Scoping rationale quoted from `:1010`. |
| `frogdb-server/crates/redis-regression/tests/expire_tcl.rs` | 908 | **Primary, +12 tests.** Today the file's only structural read against a TTL'd key is `tcl_expire_write_on_expire_should_work` `:74-87`, which uses LRANGE on a **live** key. The expired-key assertions (`:71-72`) use `GET` and `EXISTS` — the two commands that already do it right. §Testability. |
| `.scratch/hardening/specs/*.md` | — | **Read-only.** No `FM-` row governs command-level lazy key expiry: `grep -rn 'lookupKeyRead\|lazy expiry\|expired-but'` returns only replication/persistence rows about *snapshot* and *replica* expiry (`replication-failure-modes.md:128`, `:659`, `:1135`; `persistence-failure-modes.md:494`). No spec-first obligation attaches. |

## Problem

### 1. One seam, two doors, and 83 handlers that chose the unmarked one

`StoreTypedExt` was built to own exactly this. Its module banner (`typed.rs:1-23`) states the
case:

> Commands that touch a typed key … must enforce the same three-step protocol: read the key,
> check its variant, emit `WrongType` if it mismatches, and only then project to the inner
> value. Re-implementing that protocol at every call site leaks the `Value` enum across the
> command/store seam and scatters one invariant across dozens of files; the check-then-`unwrap`
> shape it produces is also a latent panic class.

Every one of its four generic methods honours expiry: `get_typed` composes
`get_with_expiry_check` (`:169`); `get_typed_mut` (`:135`), `check_typed` (`:182`) and
`get_or_create_typed` (`:202`) each call `purge_if_expired` first. `typed_family_accessors!`
(`:222-284`) generates 56 wrappers over them for 14 families. It is used widely and correctly.

Alongside it, 69 handlers call `ctx.store.get(key)` and 14 call `ctx.store.contains(key)`:

| File | raw `get` | `contains` | File | raw `get` | `contains` |
|---|---|---|---|---|---|
| `blocking.rs` | **14** | — | `timeseries.rs` | 2 | — |
| `sorted_set/set_ops.rs` | 12 | — | `tdigest.rs` | 2 | — |
| `bitmap.rs` | 6 | — | `json/basic.rs` | 2 | — |
| `sort.rs` | 5 (1 test) | — | `geo.rs` | 2 | — |
| `list.rs` | 4 | — | `topk.rs` | 1 | — |
| `string.rs` | 3 | 3 | `stream/consumer_groups.rs` | 1 | — |
| `stream/pending.rs` | 3 | — | `stream/basic.rs` | 1 | — |
| `cms.rs` | 3 | — | `sorted_set/basic.rs` | 1 | — |
| `generic.rs` | 2 | 3 | `set.rs` | 1 | — |
| `expiry.rs` | — | 8 | `hash.rs` / `cuckoo.rs` / `bloom.rs` / `basic.rs` | 1 each | — |
| | | | **Total** | **69** | **14** |

The three doors differ in exactly the way that matters:

| Primitive | Site | Expiry | LRU/LFU touch | Mutating |
|---|---|---|---|---|
| `Store::contains` | `store/mod.rs:411`, `hashmap.rs:958-960` | **none at all** | no | no (`&self`) |
| `Store::get` | `store/mod.rs:402`, `hashmap.rs:934-943` | **none** | no | `&mut` (unspill only) |
| `Store::exists_unexpired` | `store/mod.rs:421`, `hashmap.rs:961` | honoured | no | no (`&self`) |
| `Store::get_with_expiry_check` | `store/mod.rs:495`, `hashmap.rs:1132-1163` | honoured (purges) | **yes** | `&mut` |
| `Store::get_mut` | `store/mod.rs:468`, `hashmap.rs:1298` | **honoured** (`:1299-1301`) | yes | `&mut` |

The store is not confused about this. `hashmap.rs:2104-2140` is a `#[test]` that asserts the
difference and explains it: *"`Store::get` is the raw hot fast-path with NO expiry check, so a
hot key past its TTL but not yet lazily purged still comes back from `get` … Callers that need
Redis's 'expired reads as absent' semantics (scatter MGET, COPY, DUMP) must use
`get_with_expiry_check`, never raw `get`."* The rule is written, tested, and named at a class
of caller ("scatter MGET, COPY, DUMP") that turns out to be three of eighty-three.

### 2. Nothing purges before a handler runs — and the seam that looks like it might, contradicts itself

The only per-key expiry work on the dispatch path is the keyspace hit/miss probe
(`execution.rs:171-214`). Read it end to end:

```rust
191:   if self.store.exists_unexpired(key) {
192:       hits += 1;
193:   } else {
194:       misses += 1;
…      // `keymiss` (the `m` class) fires on a read lookup that misses, before the
…      // command runs — matching Redis, which emits it only from `lookupKeyRead`
206:       if !is_write {
207:           self.emit_keyspace_notification(key, "keymiss", …MISS);
```

`exists_unexpired` is `&self` (`store/mod.rs:421`). It cannot purge and does not. Between
`:214` and `handler.execute(&mut ctx, …)` at `:241` there is the `new`-event snapshot
(`:223-232`, another `exists_unexpired`) and the context construction — nothing else. The only
two places in the whole runtime that sweep `purge_if_expired` over a list of keys are
`dispatch_core.rs:120-122` (`CoreMsg::GetVersion`, the WATCH argument list) and `worker.rs:681`
(`purge_expired_watches`, the EXEC watch set). Neither is on the dispatch path.

**So the server decides the key is absent, tells subscribers so, counts the miss, and then lets
the handler read it.** For `LLEN` — `lookup: LookupSpec::FirstKey`, `flags: READONLY`
(`list.rs:363-375`) — every branch of that sentence fires:

```
DEBUG SET-ACTIVE-EXPIRE 0            +OK          (conn_command.rs:579, used at 30+ regression sites)
RPUSH mylist a b c                   :3
PEXPIRE mylist 50                    :1
… 100 ms …
EXISTS mylist                        :0           basic.rs:885   exists_unexpired
TYPE   mylist                        +none        generic.rs:52  exists_unexpired
LLEN   mylist                        :3           list.rs:383    raw get      ← Redis: :0
LRANGE mylist 0 -1                   a b c        list.rs:427    raw get      ← Redis: (empty)
LINDEX mylist 0                      $1 a         list.rs:476    raw get      ← Redis: (nil)
LPOS   mylist a                      :0           list.rs:745    raw get      ← Redis: (nil)
```

`DEBUG SET-ACTIVE-EXPIRE 0` makes it deterministic; it is not what makes it real — but rev 1
overstated *how* real, and the correction matters for how the reviewer should weigh the class.

**Correction (rev 2): active expiry here is index-driven and ticks every 100 ms, not sampled.**
`run_cycle` (`active_expiry.rs:152-192`) pulls due keys from an ordered expiry index —
`store.get_expired_keys_limited(now, 1024)` → `hashmap.rs:1356-1358` →
`expiry_index.get_expired_limited` — rather than Redis's random 20-key sample, and the shard event
loop drives it on a `tokio::time::interval(100 ms)` (`event_loop.rs:24`). **So the ordinary
stale window is ≈100 ms, not "unbounded".** (`basic.rs:882` and `generic.rs:48` both call it "the
sampled sweeper"; those comments are stale — a doc nit to fix alongside the migration.)

That is still a live window, and there are exactly three states in which it is genuinely
unbounded — each of which the tests can force and an operator can hit:

1. **TTL avalanche.** `run_cycle` returns with `budget_exhausted = true` when the 25 ms budget
   (`:24`) runs out mid-batch (`:158-161`, `:169-172`); the backlog carries to the next tick and
   grows if arrivals outpace 25 ms/100 ms of deletion.
2. **`CLIENT PAUSE`.** `run_active_expiry` (`event_loop.rs:224`) calls
   `store.set_expiry_suppressed(paused)` and then returns early at `:232`. The suppression flag
   *also* short-circuits lazy purge (`hashmap.rs:485-487` returns `true` **without deleting**), so
   in this one state **both** expiry paths stall and every corpse is fully immortal until the
   pause lifts. This is the state §Risks' `get_or_create_typed` note turns on.
3. **`DEBUG SET-ACTIVE-EXPIRE 0`** — the regression-suite lever (`event_loop.rs:237`).

The defect table below is unchanged by this correction: a ≈100 ms window in which `EXISTS` says
`0` and `LLEN` says `3` **on the same connection, one command apart** is a wire-visible
contradiction regardless of how short it is, and the three states above make it arbitrarily long.

**Class A — commands whose entire read is a raw `get`, and which therefore serve the corpse.**
Not exhaustive; these are the sites read and confirmed:

| Command(s) | Site | Answer on an expired key | Redis |
|---|---|---|---|
| `LLEN` / `LRANGE` / `LINDEX` / `LPOS` | `list.rs:383`, `:427`, `:476`, `:745` | stale length / elements / element / position | `0` / empty / nil / nil |
| `STRLEN` / `GETRANGE` | `string.rs:246`, `:290` | stale length / substring | `0` / empty |
| `GETSET` | `string.rs:1217` | **returns the expired value to the client**, then overwrites | nil |
| `GETBIT` / `BITCOUNT` / `BITPOS` | `bitmap.rs:123`, `:194`, `:365` | stale bit / count / position | `0` / `0` / `-1` |
| `BITOP` (each source) | `bitmap.rs:267` | expired source contributes its bytes | contributes empty |
| `ZUNION` / `ZINTER` / `ZDIFF` (+`STORE`) | `set_ops.rs` ×12 | expired source contributes its members; `ZINTER` **fails to short-circuit** on an expired operand | empty operand |
| `SORT` and its `BY`/`GET` patterns | `sort.rs:126`, `:169`, `:184`, `:198` | sorts the stale collection; stale weights | empty / nil weights |
| every SET-family read through `get_set_inline` | `set.rs:24` | stale membership | empty |
| `JSON.MGET` | `json/basic.rs:323` | stale document | nil |
| `XCLAIM` / `XAUTOCLAIM` ×3 | `stream/pending.rs:262`, `:362`, `:420` | stale PEL; also the 3 immutable check-then-unwraps of §Problem 6 | NOGROUP / empty |
| `OBJECT ENCODING` | `generic.rs:362` | reports the expired key's encoding | error/nil |
| `OBJECT FREQ` / `OBJECT IDLETIME` | `generic.rs:441`, `:452` (`get_metadata`) | reports the corpse's LFU counter / idle time | nil |
| `COPY` (source) | `generic.rs:598` | **copies an expired value into a fresh, TTL-free key** | `0`, no copy |
| `SET … GET` (old value) | `basic.rs:613` | returns the expired old value | nil |
| `CMS.MERGE` / `TDIGEST.MERGE` (sources) | `cms.rs:376`, `tdigest.rs:272` | merges stale sketch data | empty |

`COPY` deserves its own line: it reads an expired source through `generic.rs:598` and writes it
to a destination whose metadata is fresh. **The expired data outlives its deadline
permanently**, in a new key, and reaches disk and replicas as a legitimate write.

**Flag split of the 69 sites (derived at HEAD by attributing each raw `get` to the nearest
preceding `CommandSpec`, then hand-checking the shared helpers).** **20 sit in `READONLY`-flagged
handlers** — `bitmap.rs:123`/`:194`/`:365` (GETBIT/BITCOUNT/BITPOS), `bitmap.rs:503`/`:506`
(reachable as BITFIELD_RO), `generic.rs:362` (OBJECT), `json/basic.rs:323` (JSON.MGET),
`list.rs:383`/`:427`/`:476`/`:745`, `set_ops.rs:178`/`:345`/`:358`/`:584`/`:597`/`:669`/`:679`
(ZUNION/ZINTER/ZINTERCARD/ZDIFF), `string.rs:246`/`:290`. **49 sit in `WRITE`-flagged handlers**
(all 14 of `blocking.rs`, the `-STORE` set-op variants, the sketch creates/merges, XCLAIM /
XAUTOCLAIM, COPY, GETSET, SET…GET, XADD, XGROUP, JSON.SET, GEOSEARCHSTORE, …). Two shared helpers
straddle the line and pull far more `READONLY` **commands** in than that count suggests:
`set.rs:24` (`get_set_inline`, **24 call sites** in `set.rs`, most of them SMEMBERS / SISMEMBER /
SMISMEMBER / SCARD / SRANDMEMBER / SINTER / SUNION / SDIFF / SINTERCARD) and `sort.rs:126`/`:169`/
`:184`/`:198` (reached from both `SORT` — `WRITE`, `sort.rs:459` — and `SORT_RO` — `READONLY`,
`:532`). **This is the fact §Risks turns into a hard ordering constraint against proposal 83.**

### 3. `blocking.rs`: raw `get` for the type check, `get_mut` for the mutation — the two disagree about whether the key exists

The eight blocking handlers share one shape, written out eight times (BLPOP `:67-90`, BRPOP
`:150-173`, BLMOVE `:235-280`, BLMPOP `:391-420`, BZPOPMIN `:491-514`, BZPOPMAX `:576-599`,
BZMPOP `:704-740`, BRPOPLPUSH `:806-850`):

```rust
if let Some(value) = ctx.store.get(key) {                    // ← NO expiry check
    if value.key_type() != frogdb_core::KeyType::List {      // ← type-checked against a corpse
        return Err(CommandError::WrongType);
    }
    if let Some(list) = value.as_list() && !list.is_empty() {
        if let Some(list_mut) = ctx.store.get_mut(key)…      // ← get_mut DOES purge (hashmap.rs:1299)
```

`get_mut` checks expiry (`hashmap.rs:1298-1301`); `get` does not (`:934-943`). The handler
therefore holds two mutually inconsistent views of the same key inside one `if` block. Three
distinct wire-visible outcomes follow, all confirmed by reading the code:

**B1 — `BLPOP` on an expired *string* key answers `WRONGTYPE` instead of blocking. LIVE.**

```
DEBUG SET-ACTIVE-EXPIRE 0
SET k somestring ; PEXPIRE k 50 ; … 100 ms …
BLPOP k 0        → -WRONGTYPE Operation against a key holding the wrong kind of value
```

Redis: the key is gone, so `BLPOP` blocks. FrogDB returns an error immediately. **The key is
immortal *with respect to this command*** — `blocking.rs:68-70` returns before anything touches
`get_mut`, so no number of `BLPOP`s will ever reap it; only the 100 ms index-driven sweep will,
and under any of §Problem 2's three stall states it will not. (Rev 1 said "forever"; that is only
true in those three states. The wire-visible defect — an error where Redis blocks — needs no
stall to fire.) Same at `:151-153`, `:236-238`, `:392-394`, `:492-494`, `:577-579`, `:705-707`,
`:807-809` — eight commands.

**B2 — `BLMOVE`/`BRPOPLPUSH` silently destroy the moved element when the destination is an
expired list. LIVE. This is data loss, not a wrong answer.**

`blocking.rs:255-272` (and the identical `:823-839`):

```rust
if let Some(elem) = elem {                       // already popped from a LIVE source
    delete_if_empty_list(ctx, source);
    if ctx.store.get(dest).is_none() {           // ← raw get: the expired dest reads as PRESENT
        ctx.store.set(dest.clone(), Value::list());   //   so the create is SKIPPED
    } else if let Some(v) = ctx.store.get(dest) …     //   type check passes (it is a list)
    if let Some(dest_list) = ctx.store.get_mut(dest)  // ← get_mut PURGES it → None
        .and_then(|v| v.as_list_mut())
    {
        …push…                                   // ← never runs
    }
}                                                // ← element dropped on the floor; no error
```

```
DEBUG SET-ACTIVE-EXPIRE 0
RPUSH src a          ; RPUSH dst old ; PEXPIRE dst 50 ; … 100 ms …
BLMOVE src dst LEFT RIGHT 0     → $1 a        ← the client is told the move succeeded
LLEN src                        → :0          ← removed from source
EXISTS dst                      → :0          ← never arrived anywhere
```

The element is acknowledged to the client, removed from the source, and does not exist. The
write is replicated and persisted in exactly that shape. This is hotfix **H1**.

**B3 — `delete_if_empty_list` / `delete_if_empty_zset` (`:887-905`) are LATENT**, and worth
naming as the counter-example: they use raw `get`, but they are only reached immediately after
a `get_mut` on the same key, which has already purged it. They are correct *by call-site
context* — which is the entire failure mode this proposal is about, since nothing states or
checks that context.

`blocking.rs` has **905 lines and zero tests** (`grep -c '#\[test\]\|#\[cfg(test)\]'` → 0).
Every one of B1 and B2 is reachable only through a live server, and the crate has no `TestServer`
harness of its own.

### 3c. The same two defects exist a second time, in `frogdb-core` — and rev 1 excluded that file on purpose

**This is the correction that most changes the shape of the work.** Rev 1's §Risks scoped
`lint-store-expiry-seam` to `crates/commands/src` and named `core/src/shard/blocking.rs` only as
"a different file from `commands/src/blocking.rs`". It is a different file, and it contains the
**same two defects**, in the *blocked-then-woken* path that `commands/src/blocking.rs` hands off
to. The immediate path (`commands`) and the wake path (`core`) are two independent
implementations of BLMOVE, and both are wrong the same way.

The waiter-satisfaction seam is `trait WaiterSatisfaction` (`core/src/shard/blocking.rs:639-654`)
with two hooks: `check_key` (`:649`), which validates a key before a waiter is popped, and
`satisfy` (`:652-653`), which executes the op. **`ListSatisfaction::check_key` (`:668`) purges —
but only the key it is called with, which is the BLPOP/BLMOVE *source*:**

```rust
668:    fn check_key(&mut self, store: &mut HashMapStore, key: &Bytes) -> KeyReady {
669:        // Lazily purge an expired key so a blocker woken by a write doesn't
670:        // observe a stale value. Load-bearing for reblock-after-expire.
671:        if store.purge_if_expired(key) {          // ← `key` is the SOURCE
672:            return KeyReady::No;
```

The identical purge appears at `:914` (`ZsetSatisfaction::check_key`, `:913`) and `:1079`
(stream, `:1076`). **Nothing purges the BLMOVE *destination*.** So inside `satisfy`:

**B1-core — `WRONGTYPE` on wake against an expired destination. LIVE.**

```rust
762:                let dest_is_wrong_type = store
763:                    .get(dest)                        // ← raw get: corpse reads as PRESENT
764:                    .map(|v| v.as_list().is_none())   // ← a stale string ⇒ true
765:                    .unwrap_or(false);
766:                if dest_is_wrong_type {
767:                    return Satisfaction::Reject(Response::error(
768:                        "WRONGTYPE Operation against a key holding the wrong kind of value",
```

A blocked `BLMOVE src dst` whose `dst` holds an expired **string** is rejected with `WRONGTYPE`
when `src` is finally pushed to. Redis: `dst` is gone, so the move creates a fresh list. The
comment above it ("without consuming the source element so the next waiter can attempt it") shows
the ordering was thought about carefully — against the wrong view of the key.

**B2-core — the woken BLMOVE drops the element. LIVE. Data loss, identical to B2.**

```rust
787:                cleanup_empty_list(store, key);              // source element already popped
…
790:                if store.get(dest).is_none() {               // ← raw get: corpse ⇒ Some
791:                    store.set(dest.clone(), Value::list());  //   create SKIPPED
792:                }
793:                if let Some(dest_list) = store.get_mut(dest).and_then(|v| v.as_list_mut()) {
794:                    match dest_dir { … }                     // ← get_mut PURGES ⇒ None ⇒ never runs
795:                }
```

Reproduction is B2's with a block in the middle: `DEBUG SET-ACTIVE-EXPIRE 0`; `RPUSH dst old`;
`PEXPIRE dst 50`; sleep 100 ms; **`BLMOVE src dst LEFT RIGHT 0` on connection A (blocks — `src`
is empty)**; `RPUSH src a` on connection B. A is woken, is told `a`, `src` is empty, and
`EXISTS dst` is `0`. The `Satisfaction::Done` that follows emits the `lpush`/`rpush` keyspace
event and propagates the write — so the *replica* is told about a push that never happened here
either.

**`apply_restore` (`:430`, `:445`, `:474`) repeats the shape and is LATENT**: it re-creates a key
whose elements a failed wake consumed microseconds earlier, so a TTL can in principle elapse in
between, but the window is a few instructions wide and the outcome is a lost restore rather than
a wrong answer. Named, not fixed.

**Consequences the orchestrator must rule on** (§Risks):

1. **H1 as written does not fix this** — it edits `commands/src/blocking.rs` only. Rev 2 adds
   **H1b** for the core twin (§Effort).
2. **`lint-store-expiry-seam` scoped to `crates/commands/src` cannot catch it**, and would leave
   the crate that *owns* the expiry contract exempt from it. Three options, ruling requested:
   **(i)** keep the `commands`-only scope and accept that `core`'s 26 sites are governed by review
   alone; **(ii)** scope the gate to `crates/commands/src` **plus** `crates/core/src/shard`, with
   `rollback.rs` and `active_expiry.rs` the only allowlisted files; **(iii)** land (i) now and
   file (ii) as a follow-up. This proposal recommends **(ii)** — the gate's whole argument is
   that "safe-by-context" must stop being a category, and `core/src/shard/blocking.rs` is the
   proof that it does not hold even inside `frogdb-core`.
3. **83 also edits `core/src/shard/blocking.rs`.** H1b is 2 lines in fn bodies 83 does not
   restructure, but the two must not land concurrently unrebased — §Risks.

### 4. Two shapes that make the corpse permanent

**4a — `BITFIELD` resurrects an expired string with its TTL erased. LIVE.**

```rust
503:  let key_is_new = ctx.store.get(key).is_none();       // expired key → false
506:  let mut data = if let Some(value) = ctx.store.get(key) {
507:      if let Some(sv) = value.as_string() { sv.as_bytes_vec() }   // ← the corpse's bytes
…
583:  ctx.store.set(key.clone(), Value::String(new_sv));   // ← Store::set clears TTL (hashmap.rs:946-951)
```

```
DEBUG SET-ACTIVE-EXPIRE 0
SET counter "" ; BITFIELD counter SET u8 0 200 ; PEXPIRE counter 50 ; … 100 ms …
BITFIELD counter INCRBY u8 0 1  → :201       ← Redis: :1 (fresh key, starts at 0)
TTL counter                     → :-1        ← the deadline is gone
```

The expired value is read, incremented, written back, and its deadline destroyed by
`Store::set`, whose doc says exactly why (`hashmap.rs:945-948`: *"a plain SET/MSET overwrite
clears any TTL (Redis semantics)"*). The key is now permanent, holding data the client asked to
have deleted. Hotfix **H2**.

**4b — the "Key already exists" family: seven sites, one shape, all wrong on an expired key.**

`bloom.rs:95`, `cuckoo.rs:84`, `topk.rs:103`, `tdigest.rs:96`, `cms.rs:41`, `cms.rs:106` are
byte-for-byte the same; `timeseries.rs:134` is the same shape with a different message string
(`"TSDB: key already exists"` — rev 1 wrongly called all seven byte-identical, which matters only
in that a `sed`-driven migration must key off the `get(key).is_some()` shape, not the text):

```rust
if ctx.store.get(key).is_some() {
    return Err(CommandError::InvalidArgument { message: "Key already exists".into() });
}
```

An expired filter therefore blocks its own re-creation:

```
DEBUG SET-ACTIVE-EXPIRE 0
BF.RESERVE bf 0.01 1000 → +OK ; PEXPIRE bf 50 ; … 100 ms …
EXISTS bf               → :0                  ← the key does not exist
BF.RESERVE bf 0.01 1000 → -ERR Key already exists   ← RedisBloom: +OK
```

`EXISTS` and `BF.RESERVE` disagree about whether the same key exists, in the same connection,
one command apart. The same shape sits at `json/basic.rs:79` (`JSON.SET … NX` refuses against an
expired document), `stream/basic.rs:92` (`XADD NOMKSTREAM`), `stream/consumer_groups.rs:110`
(`XGROUP CREATE MKSTREAM`), `timeseries.rs:1254` and `tdigest.rs:272` (dest-must-exist checks),
and `geo.rs:417` (`GEOSEARCHSTORE` source-missing check).

### 5. `Store::contains` is a third door, and it is blinder than the second

`contains` (`store/mod.rs:411`, `hashmap.rs:958-960`) is `self.data.contains_key(key)` — a bare
map probe, `&self`, no expiry logic whatsoever. Fourteen command sites use it.

- **`MSETNX` (`string.rs:918`)** — `if ctx.store.contains(&pair[0]) { return Ok(Integer(0)) }`.
  An expired key blocks the whole multi-set. Redis: sets all, returns `1`.
- **`COPY` without `REPLACE` (`generic.rs:592`)** — `if !replace && ctx.store.contains(dest)`.
  An expired destination makes `COPY` a no-op returning `0`. Redis: `1`.
- **`RENAME` destination (`generic.rs:197`) — REFUTED in rev 2; rev 1 was wrong to list it.**
  Line `:196` is `let _ = ctx.store.get_with_expiry_check(new_key);`, which purges an expired
  destination immediately before the `contains` at `:197`. The discarded return value is the
  give-away that the call exists *only* for its purge side effect. **RENAME is already correct**,
  and this is the crate's own worked example of the `exists_for_write` shape §(b) proposes to name.
  The migration should replace `:196-197` with the named method rather than "fix" a defect that
  is not there.
- **`OBJECT REFCOUNT` (`generic.rs:466`)** — reports `1` for a corpse where Redis reports nil.
  **It needs `exists_unexpired`, not `exists_for_write`:** `OBJECT` is `READONLY`, and the sibling
  subcommands `TYPE` (`generic.rs:52`) and `EXISTS` (`basic.rs:885`) already use the non-mutating
  probe for exactly that reason. Migrating it to the mutating one would be the mistake §(R1)
  rejects, one call site at a time.
- **`MSET`-family `NX`/`XX` (`string.rs:1509`, `:1518`)** — same class as MSETNX, both directions.
- **The eight `expiry.rs` sites (`:283`, `:372`, `:449`, `:533`, `:611`, `:664`, `:749`,
  `:799`)** are the `if !ctx.store.contains(key) { return … }` guard immediately above each of
  the EXPIRE-family decision ladders. They are the *input* to proposal 92's decision table; the
  boundary is stated in §Risks. **Ruling (rev 2): these eight are LIVE, and they are the worst
  `contains` sites, not the most boring.** Verified by reading `:276-292`, `:365-378`,
  `:443-455`: nothing purges before the guard (unlike `generic.rs:196`), so `contains` returns
  `true` for a corpse, the ladder falls through, and `EXPIRE k 100` **installs a fresh deadline on
  an already-expired key and returns `1`**. Redis returns `0` and the key stays gone. The key is
  resurrected with a new TTL — a strictly worse version of §Problem 4a, reachable with two
  commands and no sub-command syntax:

  ```
  DEBUG SET-ACTIVE-EXPIRE 0
  SET k v ; PEXPIRE k 50 ; … 100 ms …
  EXISTS k    → :0
  EXPIRE k 100 → :1        ← Redis: :0        the corpse is now live for 100 s
  GET k       → "v"        ← Redis: (nil)
  ```

  Rev 1 filed these as ordinary Class C. They are a fifth LIVE family. They are **not** promoted
  to a fourth hotfix, because the guard and 92's ladder are four lines apart and a purge inserted
  above the guard changes what 92 is pinning — the boundary warning in §Risks is now
  load-bearing in both directions.

A fourth blind primitive completes the set: **`Store::get_metadata`** (`hashmap.rs:1542-1544`,
`self.data.get(key).map(|e| e.metadata.clone())` — `&self`, no expiry). `OBJECT FREQ`
(`generic.rs:441`) and `OBJECT IDLETIME` (`generic.rs:452`) both read it, so both answer for a
corpse. Two more sites, outside the 69 + 14, and both `READONLY` — so like REFCOUNT they need a
non-mutating expiry-aware guard, i.e. `exists_unexpired` gating an unchanged `get_metadata` call.

The three sites `basic.rs:885`, `generic.rs:52` and the execution seam's `:191` show the crate
already knows the right primitive for a pure existence question — `exists_unexpired`, added with
a doc that names Redis `lookupKeyRead` (`store/mod.rs:413-423`). Three call sites use it. Fourteen
use the one below it, and two more use the one below *that*.

### 6. `lint-no-typed-unwrap` prints a claim about the code that the code does not satisfy

The gate (`Justfile:998-1039`) is the enforcement half of the typed seam. It runs in
`lint-gates` (`:329`), on every commit via lefthook, and in CI. It passes today:

```
$ just lint-no-typed-unwrap
OK: no check-then-unwrap or hand-rolled WrongType in crates/commands
```

Both halves of that sentence are false.

**Half one — "no check-then-unwrap".** The pattern is
`as_[a-z_]+_mut\(\)[[:space:]]*\.unwrap\(\)|get_mut\([^)]*\)[[:space:]]*\.unwrap\(\)` — the
**`_mut` forms only**. Eight immutable check-then-unwraps sail through:

```
hash.rs:2006-2007            let value = ctx.store.get(key).unwrap();
                             let hash  = value.as_hash().unwrap();
stream/pending.rs:262-263    let value = ctx.store.get(key).unwrap();
       :362-363, :420-421    let stream = value.as_stream().unwrap();
```

`hash.rs:2006` is guarded by a `get_or_create` four lines above (`:2002`), so it does not panic
today; the three `stream/pending.rs` pairs are guarded by earlier passes in the same handler.
They are the exact panic class the gate exists to remove, in the exact shape it does not look
for. (Proposal 93 also names `hash.rs:2006-2007`, as pressure its change relieves.)

**Half two — "no hand-rolled WrongType".** The pattern is `\.ok_or(_else)?\([^)]*WrongType`.
There are **zero** `.ok_or` forms in the crate and **39** hand-rolled `CommandError::WrongType`
constructions, every one of them the shape the gate cannot see:

```rust
match ctx.store.get(key) {
    Some(value) => {
        if let Some(list) = value.as_list() { … }
        else { Err(CommandError::WrongType) }        // ← 39 of these
    }
    None => …
}
```

Distribution: `string.rs` 12, `blocking.rs` 10, `bitmap.rs` 6, `list.rs` 4, `set_ops.rs` 3,
`sort.rs` 2, `set.rs` 1, `basic.rs` 1. **Every one of the 39 sits directly under one of the 69
raw `get` calls** — they are the same defect seen from the type side, which is why one migration
removes both.

This is the general lesson worth carrying out of the proposal: **a gate whose success message is
broader than its patterns is worse than no gate, because it converts an unchecked invariant into
a checked-looking one.** Hotfix **H3** fixes the gate independently of everything else.

### 7. The `frogdb-core` inventory rev 1 understated ~5× — and a ruling for each site

Rev 1 priced the rename against "3 in `core/src/shard/{rollback,search_hook}.rs`, 4 in the seam
and its impl". Re-derived at HEAD: **`store.get(` has 153 textual matches workspace-wide** (rev 1
said 149) and **49 of them are in `core/src`**, of which **26 are real production `Store::get`
calls**. The full list, each with a ruling:

| Site(s) | What reads there | Ruling |
|---|---|---|
| `shard/blocking.rs:763`, `:790`, `:793` | woken BLMOVE dest type-probe + deposit | **LIVE** — §Problem 3c, hotfix **H1b** |
| `shard/blocking.rs:430`, `:445`, `:474` | `apply_restore` re-create-then-fill | **LATENT** — microsecond window, lost restore not wrong answer |
| `shard/blocking.rs:1082`, `:1100`, `:1134` | stream `satisfy` reads after `check_key` `:1076` purged the same key | **LATENT by call-site context** — the exact category §(c) Class D says must stop existing |
| `shard/blocking.rs:1208`, `:1218` | stream helpers, same post-purge context | **LATENT** |
| `shard/search/query.rs:115`, `:208`, `:756` | `FT.SEARCH`/`FT.AGGREGATE` hydrate each hit's doc from the store | **LIVE-suspect, out of scope** — an expired-but-unreaped doc is returned as a search result. Not reproduced here (needs a live `FT.CREATE` fixture); **file as a follow-up issue**, do not fold into this proposal |
| `shard/timeseries_execution.rs:32`, `:63` | `TS.MRANGE`/compaction source-key resolution | **LATENT-suspect** — reached from handlers that have already probed the key; needs a read before commit 5 renames it |
| `shard/search_hook.rs:75`, `:128`, `:159` | post-write re-index | **LATENT** — key is live by construction (written by the command that triggered the hook) |
| `shard/search/create.rs:43`, `index_mgmt.rs:135` | index backfill sweeps the keyspace | **LATENT** — indexes an expired doc that the sweep will reap and the hook will then delete; converges |
| `shard/active_expiry.rs:223`, `:229` | `existed_before` / `is_none()` around a deletion **inside the sweep** | **Correct by construction** — this *is* the expiry path |
| `shard/execution.rs:1161` | XREAD `$` last-id snapshot | **LATENT** — the key was just probed by the same seam |
| `shard/rollback.rs:62` | warm-unspill snapshot | **Legitimate by design** — the reason `get_unchecked` survives the rename (`:36`, `:59-62`) |
| `store/typed.rs:138`, `:185`, `:203` (`self.get`) | post-purge type probe inside the seam | **Legitimate by design** — the seam's own second consumer |

The remaining 23 of the 49 are test bodies (`rollback.rs` ×9, `persistence/store_recovery.rs`
×5, `store/hashmap.rs` ×4, `store/typed.rs` ×2, `scripting/executor.rs` ×2) and two doc-comment
examples (`store/mod.rs:17`, `command.rs:1053`). **Commit 5's rename is therefore ~45 lines, not
~25** — §Effort is corrected.

## Proposed change

### The direction is already decided by the tree

`StoreTypedExt` exists, honours expiry in all four methods, owns `WrongTypeError`, avoids the
copy-on-write-before-type-check mistake (`typed.rs:16-23`), and generates 56 family wrappers.
`hashmap.rs:2104-2140` states the rule that the raw door is for the store's own use. Nothing
needs designing. What is missing is (i) the migration, (ii) one method for the write-path
existence question, and (iii) a gate so the count does not climb back.

### (a) Rename the unsafe door so a bypass is visible at the call site

`Store::get` → `Store::get_unchecked` (`store/mod.rs:398-402`, impl `hashmap.rs:934`), with a
doc that names its two legitimate consumers:

```rust
/// Raw hot-tier read. **Does not check expiry** — a key past its deadline that
/// lazy or active expiry has not yet reaped comes back with its value intact.
/// May unspill a warm value to hot (hence `&mut self`).
///
/// Two callers are legitimate: [`crate::shard::rollback`], which snapshots the
/// physical entry and must not let a read mutate the keyspace it is about to
/// restore; and this crate's own [`typed`] layer, which calls it only *after*
/// [`Store::purge_if_expired`] as a non-COW type probe. Command code must use
/// [`StoreTypedExt`] (which composes [`Store::get_with_expiry_check`]) —
/// enforced by `just lint-store-expiry-seam`.
fn get_unchecked(&mut self, key: &[u8]) -> Option<Arc<Value>>;
```

The rename is the leverage: it makes an expiry-blind read *say so where it is written*, and it
makes the gate in (d) a one-token grep instead of a semantic analysis. `get` is **not** deleted —
`rollback.rs:59-62` needs it by name and by intent, and the seam's own `check_typed`/
`get_typed_mut` use it as a post-purge type probe (`typed.rs:138`, `:185`, `:203`).

Blast radius, exact (re-derived in rev 2; rev 1 understated it ~5×): **153** `store.get(` textual
matches workspace-wide, of which the true `Store::get` calls are the **69 in `commands`** (all
migrated away by (c), so they vanish rather than get renamed), **26 production sites in
`core/src`** (inventory + per-site ruling in §Problem 7), **23 test/doc lines in `core`**, and
**3 test-only lines in `frogdb-recovery`** (`tests.rs:204`, `:624`, `:1454`). The rest are
`HashMap`-model reads in `frogdb-testing`/`frogdb-server` tests, the fuzz target and the
benchmarks, which the rename does not touch. **Commit 5 is therefore ≈45 lines, not ≈25.**
§Risks prices the locked-crate line.

### (b) One new method, for the question the typed seam does not currently answer

The seam covers "read it", "read it mutably", "type-check it", "create it if absent". It has no
answer for *"does this key exist, from a writer's point of view"* — the question §Problem 4b's
seven sites and §Problem 5's fourteen are asking. Redis answers it with `lookupKeyWrite`, which
expires the key first. Add the equivalent to `StoreTypedExt`:

```rust
/// Write-path existence probe: `true` iff the key exists and is not past its
/// deadline, **reaping it if it is**.
///
/// The mutating counterpart of [`Store::exists_unexpired`] (`&self`, used by the
/// execution seam's hit/miss accounting, which must not perturb the keyspace).
/// Mirrors Redis `lookupKeyWrite`: a create-if-absent handler must neither fail
/// against a corpse nor inherit its TTL, and the reap's effects are reported
/// through the existing lazy-purge buffers the worker drains after every command.
fn exists_for_write(&mut self, key: &[u8]) -> bool {
    !self.purge_if_expired(key) && self.contains(key)
}
```

Four lines, no new state, no new drain. It consumes the report machinery `purge_if_expired`
already feeds (`store/mod.rs:511-519`, drained at `worker.rs:679-686` / `execution.rs`), so a
key reaped this way fires the same `expired` notification, tracking invalidation and version
bump as any other lazy purge.

### (c) Migrate the 69 + 14, in four mechanical classes

**Class A — a raw read that is the whole read (≈45 sites).** Collapses `get` + `as_X()` + the
hand `WrongType` into one call:

```rust
-        match ctx.store.get(key) {
-            Some(value) => {
-                if let Some(list) = value.as_list() {
-                    Ok(Response::Integer(list.len() as i64))
-                } else {
-                    Err(CommandError::WrongType)
-                }
-            }
-            None => Ok(Response::Integer(0)),
-        }
+        match ctx.store.get_list(key)? {
+            Some(list) => Ok(Response::Integer(list.len() as i64)),
+            None => Ok(Response::Integer(0)),
+        }
```

Ten lines to four, at `list.rs:383`; the same edit, verbatim in shape, at 44 more sites. The
polymorphic readers (`sort.rs:126` reads List **or** Set **or** SortedSet; `set_ops.rs`'s
`iter_zset_or_set`; `generic.rs:362` OBJECT ENCODING) cannot use a family wrapper — they take
`ctx.store.get_with_expiry_check(key)` directly, which is what `generic.rs:108`, `:206`, `:309`
and `basic.rs:819` already do. **That is not a new pattern; it is the crate's own.**

**Class B — the `get`→`get_mut` split (14 sites, all `blocking.rs`).** The immutable pass exists
only to type-check before paying copy-on-write — which is precisely what `get_typed_mut` does,
in the right order, with a doc explaining why (`typed.rs:16-23`, `:131-150`). The whole shape
collapses:

```rust
-            if let Some(value) = ctx.store.get(key) {
-                if value.key_type() != frogdb_core::KeyType::List {
-                    return Err(CommandError::WrongType);
-                }
-                if let Some(list) = value.as_list() && !list.is_empty() {
-                    if let Some(list_mut) = ctx.store.get_mut(key).and_then(|v| v.as_list_mut())
-                        && let Some(elem) = list_mut.pop_front() {
+            if let Some(list) = ctx.store.get_list_mut(key)?
+                && let Some(elem) = list.pop_front()
+            {
```

Eight handlers, ~7 lines each, and B1/B2/B3 all become unreachable: there is one view of the
key, it is expiry-honouring, and it is `&mut`.

**Class C — existence probes (≈21 sites: the 7 "Key already exists", the 6 create/dest-exists,
the 14 `contains` minus the ones that are genuinely read-only).** `ctx.store.get(key).is_some()`
and `ctx.store.contains(key)` → `ctx.store.exists_for_write(key)`.

**Class D — safe-by-context (≈4 sites: `blocking.rs:889`, `:899`, `geo.rs:150`,
`sorted_set/basic.rs:172`).** These are delete-if-empty helpers reached only after a `get_mut`.
They migrate anyway — `get_list(key)?` costs nothing on an already-purged key and removes the
reader's obligation to reconstruct the call graph to know the code is correct. **The point of a
seam is that safe-by-context stops being a category.**

### (d) The gate: `lint-store-expiry-seam`

A fifteenth compile-free grep gate in `lint-gates` (`Justfile:329` — that recipe lists exactly
**fourteen** members today: `lint-info-seam` … `lint-continuation-lock`), in the same shape as
the fourteen that exist:

```just
# Gate: command code reads the store through the expiry-honouring typed seam.
# `Store::get_unchecked` and `Store::contains` skip lazy key expiry (see the
# contract test `hot_expired_key_get_vs_get_with_expiry_check_contract`,
# store/hashmap.rs), so a handler using them serves a key past its deadline —
# which the execution seam has *already* counted as a keyspace miss and
# announced via `keymiss`. Use StoreTypedExt (get_typed / get_typed_mut /
# check_typed / get_or_create_typed and their per-family wrappers), or
# `exists_for_write` for a write-path existence probe, or
# `get_with_expiry_check` for a genuinely polymorphic read. Clippy's
# disallowed_methods cannot scope a trait method to one crate, so a grep gate
# is the honest tool.
lint-store-expiry-seam:
    …grep -rEn 'store\.(get_unchecked|contains)\(' {{server-dir}}/crates/commands/src/…
```

**Scope is an orchestrator ruling, not settled here.** Rev 1 scoped it to `crates/commands/src`
alone, matching `lint-no-typed-unwrap`'s rationale (*"Scoped to crates/commands so store
internals stay unconstrained"*, `Justfile:1010`) — and rev 1 justified that with a false premise:
that `core` had only three raw-`get` uses. §Problem 7 counts **26 production sites in
`core/src`**, three of which (`shard/blocking.rs:763`, `:790`, `:793`) are the *same live defect
class* the gate exists to prevent, on the blocked-client path where no execution-seam purge has
run. A gate that cannot see the twin of the bug it was written for is a gate that certifies the
wrong thing.

Two options, ruling requested:

- **(i) `crates/commands/src` only.** Cheapest; matches the existing precedent verbatim; leaves
  the `blocking.rs` twin permanently un-gated after H1b fixes it, so it can regress silently.
- **(ii) `crates/commands/src` **plus** `crates/core/src/shard`, allowlisting exactly two files.**
  `shard/rollback.rs` (warm-unspill snapshot — must be expiry-blind by design, §Problem 7) and
  `shard/active_expiry.rs` (the expiry cycle itself; purging inside it is circular). Every other
  `shard` site is either migrated by (c) or is a defect. **Recommended.** Cost: one extra grep
  path and a two-entry allowlist — which contradicts "no allowlist" below, so state it as two
  *by-design* exemptions with the reason in the gate comment, not as a decay list.

Under either option, `store/hashmap.rs` and `store/typed.rs` stay out of scope by construction —
they *are* the seam.

**No allowlist beyond the two by-design exemptions under option (ii).** After (c) the count in
`commands` is zero, and a legitimate future need has three named alternatives in the error
message. An open-ended allowlist is the mechanism by which a gate's guarantee decays into a list;
two exemptions whose reason is written into the gate comment (*snapshot must be expiry-blind*,
*this is the expiry cycle*) are not that.

### (e) Widen `lint-no-typed-unwrap` to match what it says (this is hotfix H3)

Two edits, to `Justfile:1016` and `:1026` (rev 1 said `:1017`/`:1027` — off by one):

```diff
-    unwrap_pattern='as_[a-z_]+_mut\(\)[[:space:]]*\.unwrap\(\)|get_mut\([^)]*\)[[:space:]]*\.unwrap\(\)'
+    unwrap_pattern='as_[a-z_]+(_mut)?\(\)[[:space:]]*\.unwrap\(\)|get(_mut|_unchecked)?\([^)]*\)[[:space:]]*\.unwrap\(\)'
-    wrongtype_pattern='\.ok_or(_else)?\([^)]*WrongType'
+    wrongtype_pattern='\.ok_or(_else)?\([^)]*WrongType|Err\(CommandError::WrongType\)'
```

The widened patterns fail on 8 + 39 = 47 sites today, so **H3 must land with or after the
migration commit that removes them** — or, to keep it independently landable, as a *count pin*
(`must be exactly 47 and falling`) that the migration then drives to zero. §Effort states both.

### The invariant, once, where it belongs

> **I1 — a command never observes a key past its deadline.** Every read of a value from
> `frogdb-commands` goes through `StoreTypedExt` (`get_typed`, `get_typed_mut`, `check_typed`,
> `get_or_create_typed`, `exists_for_write`, or their 56 family wrappers) or through
> `Store::get_with_expiry_check`. `Store::get_unchecked` and `Store::contains` have **no**
> callers in `frogdb-commands`.

Enforced by `lint-store-expiry-seam` on every commit; discoverable at the call site because the
bypass is named `get_unchecked`; and *anchored* by the store's own contract test
(`hashmap.rs:2104-2140`), which stops being a curiosity and becomes I1's premise.

### Alternatives considered and rejected

**(R1) Add a blanket `purge_if_expired` loop over `handler.keys()` at `execution.rs:191`.**
Seductive — it is four lines and appears to fix all 83 sites at once. **Rejected on three counts.**
First, it is wrong for the `&self` probe it would sit next to: the hit/miss snapshot must be
non-mutating (`store/mod.rs:417-419`: *"so the execution seam can probe it without perturbing the
value a command's own read observes"*), so the purge would have to be a second pass, and the two
passes could then disagree. Second, `handler.keys()` is not every key a command reads — `SORT`'s
`BY`/`GET` patterns (`sort.rs:169-198`), `BITOP`'s sources, `ZUNION`'s operands and `COPY`'s
source are all resolved *inside* the handler from argument text, and none appear in the key spec.
Third, and decisively: it would make **every** command a mutating command with respect to
expiry, firing lazy-purge effects — `expired` notification, tracking invalidation, version bump,
XREADGROUP drain (`worker.rs:688-701`) — for keys a read-only handler never touched. It buys a
partial fix by making the effect surface strictly larger than Redis's. **It is machinery to
preserve the problem.**

**(R2) Make `Store::get` itself expiry-checking.** The single most tempting one-liner, and the
one the codebase has already ruled out in writing. `rollback.rs:36`/`:59-62` uses `get` precisely
because it must unspill without mutating the keyspace it is about to restore; `check_typed` and
`get_typed_mut` use it as a post-purge probe and would double-purge; and
`hot_expired_key_get_vs_get_with_expiry_check_contract` (`hashmap.rs:2104-2140`) is a standing
test that this distinction exists. Deleting a documented, tested distinction to avoid 83 call-site
edits is the trade this proposal exists to refuse.

**(R3) Change `Store` to take `&self` for reads.** **Not this proposal, and explicitly not
proposed.** A prior round rejected a "`Store` `&self` reads" candidate; nothing here changes any
method's receiver. `get_unchecked` keeps `&mut self` (warm unspill needs it), `exists_for_write`
is `&mut` because reaping is a mutation, and `exists_unexpired`/`contains` keep `&self`. The
change is *which door commands walk through*, not what the doors' signatures are.

**(R4) Gate only; skip the rename.** Viable, and the fallback if the orchestrator will not spend
a locked-crate touch (§Risks). It loses the call-site legibility — `store.get(k)` still reads as
innocuous to a human reviewer and is only caught by CI — but it keeps all the correctness. If
taken, the gate greps `store\.get\(` with the three `core` sites out of scope by the existing
crate scoping.

**(R5) Fix only the LIVE handlers (H1 + H1b + H2) and stop.** These are the hotfixes and they
should land first. They are not the proposal because they fix four of the eighty-three, leave
`commands/src/blocking.rs` with fourteen more of the same shape and no tests, leave
`core/src/shard/blocking.rs` with six more, and leave the eighty-fourth handler free to be
written tomorrow — a gate is the only thing that changes that. **Rev 2 is itself the evidence
against R5**: rev 1 applied R5's reasoning to one file and shipped a fix that missed the
identical bug sitting in a same-named file one crate over.

### Deletion test

- **`StoreTypedExt` + `typed_family_accessors!` (`typed.rs:114-284`)** — delete them and the
  `WrongType` invariant, the COW-before-type-check ordering, and lazy expiry each scatter to
  ~300 call sites. **Earns its keep — it is the seam, and this proposal only widens its
  membership.**
- **`Store::get` / `get_unchecked`** — delete it and `rollback.rs:62` cannot snapshot a warm key
  without expiring it (corrupting the rollback), and `check_typed`/`get_typed_mut` must
  double-purge to probe a type. **Earns its keep — as an internal primitive with a name that
  says so. That is the whole change to it.**
- **The 69 raw `ctx.store.get` calls** — delete them and 69 typed calls reappear, ~350 lines
  shorter, with expiry handled. **Do not earn their keep.**
- **The 60 `as_*()` downcasts and 39 `Err(CommandError::WrongType)` constructions in
  `frogdb-commands`** (both re-counted at rev 2 and both exact: 60 = the 28 accessors generated by
  `impl_value_accessors!` at `types/src/types/mod.rs:97-112` plus the two hand-written
  `as_vectorset[_mut]` at `:116`/`:124`, counted across `crates/commands/src`; 39 =
  `grep -rn 'Err(CommandError::WrongType)'`) — delete them and **nothing** reappears: the typed accessor returns the
  projected type and propagates `WrongTypeError` through `?` (`typed.rs:57-61`). **Do not earn
  their keep — they exist only because of the raw read above them.**
- **`Store::contains`'s 14 command call sites** — delete them and 14 `exists_for_write` calls
  reappear. `Store::contains` itself stays: `hashmap.rs` uses it internally and `exists_unexpired`
  is defined in terms of it (`store/mod.rs:422`). **The trait method earns its keep; its command
  callers do not.**
- **`Store::exists_unexpired`** — delete it and the execution seam's hit/miss accounting has no
  non-mutating expiry-aware probe, so either the counters lie or the probe perturbs the keyspace.
  **Earns its keep; untouched.**
- **`hot_expired_key_get_vs_get_with_expiry_check_contract` (`hashmap.rs:2104-2140`)** — delete
  it and I1 loses its premise. **Earns its keep; kept verbatim, renamed only.**
- **`lint-no-typed-unwrap`'s success message** — delete it (or fix it, which is H3) and a reader
  stops being told an invariant holds that does not. **Does not earn its keep as written.**

## Testability improvement

**1. The whole defect class becomes one parameterised test instead of eighty-three unwritten
ones.** Today "no command serves an expired key" is not testable as a property — there is no
seam to assert at, so proving it means one integration test per command per read path. After
the change there is exactly one seam, and the property is already half-tested: `typed.rs:484-543`
(`typed_read_honors_ttl`) asserts all five expiry behaviours of the four generic methods. The
addition is a **command-level table test**: seed a key of each family, expire it with
`DEBUG SET-ACTIVE-EXPIRE 0`, and drive every read command against it asserting the empty-key
answer. That test is writable *today* — and this proposal's claim is that it fails today at
every row in §Problem 2's Class A table, which is the evidence the reviewer should demand first.

**2. `blocking.rs` gets its first tests, and they are the ones that matter.** 905 lines, 0 tests,
8 handlers, 14 raw gets. B1 and B2 need a live server (the crate has no harness), so they land in
`redis-regression/tests/expire_tcl.rs` next to the existing TTL fixtures:
`blpop_on_an_expired_string_key_blocks_rather_than_wrongtype` and
`blmove_into_an_expired_destination_does_not_drop_the_element`. **Both fail before H1.** The
second asserts all three observables — the reply, `LLEN src`, and `EXISTS dst` — because a test
that checks only the reply passes against the bug.

**2b. And the `frogdb-core` twin needs its own test, which is a different test.** The
`commands/blocking.rs` rows above exercise the *fast path* — a BLMOVE that finds data already
present and never parks. `core/src/shard/blocking.rs:763`/`:790` is only reached when the client
**parked and was later woken by a push** (§Problem 3c), so a test that does not block first
cannot reach it. The row is
`blmove_woken_by_a_push_into_an_expired_destination_does_not_drop_the_element`: issue BLMOVE on
an empty source, wait until the client is parked (`BLOCKED_CLIENTS` in `INFO clients` ≥ 1),
`SET dst <string>` with a 50 ms TTL and let it lapse, then `RPUSH src v`. Correct: the woken
client replies `v` and `EXISTS dst` = 1 with `dst` a one-element list. Today: `store.get(dest)`
sees the expired string corpse, `as_list()` is `None`, and the handler returns WRONGTYPE for a
key `EXISTS` says is gone — and on the sibling path at `:790`/`:793` the element is dropped.
**Fails before H1b.** This is the row that proves the two `blocking.rs` files are separate
defects and that fixing only the command-crate one leaves the bug reachable.

**3. Twelve regression rows that cost nothing to write and cannot be written today.**
`expire_tcl.rs` is 908 lines and its only structural read against a TTL'd key
(`tcl_expire_write_on_expire_should_work` `:74-87`) uses `LRANGE` on a **live** key; the
expired-key assertions at `:71-72` use `GET` and `EXISTS` — the two commands that already route
correctly. That is the coverage shape that let this survive: every expiry test was written
through the one door that works. The twelve new rows drive the other door: `LLEN`, `LRANGE`,
`LINDEX`, `STRLEN`, `GETRANGE`, `GETBIT`, `BITCOUNT`, `ZUNION`, `SORT`, `JSON.MGET`, `XCLAIM`,
`BF.RESERVE`. (Rev 1 listed `JSON.GET` and `XPENDING`; both are wrong — `JSON.GET`'s spec at
`json/basic.rs:132` has no raw `get`, the raw read is `JSON.MGET`'s at `:323`, and `XPENDING`'s
spec at `stream/pending.rs:21` likewise has none — the raw stream reads are `XCLAIM`'s at `:262`
and `XAUTOCLAIM`'s at `:362`/`:420`.)

**3b. Plus one row for H2 that is not a read at all.** `expire_seti_on_an_expired_key_returns_0`:
`SET k v PX 50`, let it lapse with active expiry off, then `EXPIRE k 100`. Correct: `0`, and
`EXISTS k` stays `0`. Today: `ctx.store.contains(k)` at `expiry.rs:283` sees the corpse, the
ladder reaches `set_expiry`, and the command returns `1` — **resurrecting a key that had already
been reported gone**, permanently, because the new deadline overwrites the lapsed one. The same
row repeats for the other seven `contains` guards (`:372`, `:449`, `:533`, `:611`, `:664`,
`:749`, `:799`) as a table.

**4. The gate turns a 69-site audit into a compile-free grep.** `lint-store-expiry-seam` runs in
`lint-gates` — sub-second, unconditional on every commit via lefthook, and in the `seam-gates` CI
job. The regression that reintroduces site 70 is caught before it is pushed, which is the
difference between this and a one-time cleanup.

**5. Two contradictions become assertable in one process.** `EXISTS k` = 0 while `LLEN k` = 3, and
`EXISTS bf` = 0 while `BF.RESERVE bf` = `ERR Key already exists`, are each a two-command test with
no timing dependence beyond `DEBUG SET-ACTIVE-EXPIRE 0`. They are better regression tests than the
per-command ones because they assert *internal consistency* rather than a memorised Redis answer —
they stay correct even if a future compatibility ruling changes what the right answer is.

**6. Mutation exposure improves where it is measured.** The 39 hand-rolled `WrongType` branches
are 39 separately-mutable arms in `frogdb-commands`; folding them into one `?` propagation through
`WrongTypeError` (`typed.rs:57-61`) removes 39 mutation targets and concentrates the surviving one
in `frogdb-core`, which has the tests for it (`typed.rs:549-562`). Neither crate is locked, so
this is a quality note, not a gate obligation.

## Risks / scope boundaries vs siblings

**vs proposal 93 (hash field TTL: two books).** **Adjacent, complementary, genuinely
independent — and the pair is worth stating precisely because both name `typed.rs`.**
93 owns **field-level** expiry: it hoists `HashValue` out of `typed_family_accessors!`
(`typed.rs:269`) and hand-writes `get_hash`/`get_hash_mut`/`check_hash`/`get_or_create_hash` with
a `purge_expired_hash_fields` prelude. 97 owns **key-level** expiry, and changes **no existing
method in `typed.rs`** — it adds `exists_for_write` and migrates callers into the seam. Ordering:
**either order works.** If 93 lands first, 97 finds four hand-written hash methods where the macro
used to generate them and migrates its one `hash.rs` call site (`:2006`) into whichever exists. If
97 lands first, 93 hoists as designed. The one sentence 93 deletes (`typed.rs:162-164`, *"Hash
field-level TTL is a separate concern…"*) is in a doc block 97 also edits (the `# Expiry` paragraph
of `get_typed`) — a textual conflict of two adjacent lines, not a semantic one. Both edit
`store/mod.rs` (93 deletes `:646-667`, 97 renames `:398-402`) and `store/hashmap.rs` (93 edits
`:610-630`/`:1364-1382`/`:1392-1415`, 97 renames `:934`) at ranges 200+ lines apart. **Suggested
order: 93 then 97**, purely so 97's mechanical rename passes over a settled `typed.rs`.

**vs proposal 83 (lazy expiry: `ExpiryReport` and one removal authority).** **The most important
edge, and it is a coupling rather than a conflict.** 83's finding is that when a lazy purge *does*
fire, its effect set is hand-mirrored and incomplete — **no WAL delete, no dirty bump**
(`worker.rs:738-851`). 97 does not touch that code. But 97 **materially increases how often lazy
purge fires**: today only the ~10 commands routed through `get_with_expiry_check` and the typed
seam trigger it; after 97, every read in `frogdb-commands` can. **97 therefore amplifies 83's
defect rather than causing or fixing it.** Consequences, stated plainly for the orchestrator:

- **HARD ORDERING: 83 lands before 97. This is a constraint, not a preference.** Rev 1 wrote
  *"preferred order"* and offered a 97-first fallback; that was too weak, and the reason is
  concrete rather than stylistic. Two effects are missing from `drain_lazy_purge_effects`
  (`worker.rs:738`): the **WAL delete** and the **dirty/version bump**. 97's whole purpose is to
  route ~69 additional read sites through the purging door, so it multiplies the rate at which
  purges fire on a pipeline that loses both. Concretely, before 83:
  - a lazily-purged key produces **no WAL record**, so a replica or a recovering node that
    replays the log still holds the key — divergence that only the boot sweep repairs, and only
    on the node that reboots;
  - `bump_versions_for(...)` is called for tracking invalidation but the *dirty* counter is not
    bumped, so **`WATCH`ed keys reaped by a purge that 97 newly triggers may not abort the
    `MULTI`** that the key's disappearance should abort. That is a correctness regression in a
    transaction path, not a cosmetic one, and it is exactly the class of failure that grows with
    purge frequency.

  A 97-first merge therefore widens a replication-divergence and a WATCH-abort window across
  every read command in the crate. **If the orchestrator nonetheless schedules 97 first, it must
  be with an explicit written ruling** naming those two exposures; it is not a fallback to fall
  into by accident.
- **No file conflict, with one caveat that matters for H1b.** 83 edits `worker.rs`,
  `post_execution.rs`, `event_loop.rs`, `dispatch_core.rs`, **and `core/src/shard/blocking.rs`**;
  97 edits `commands/src/*`, `store/typed.rs`, `store/mod.rs`, `store/hashmap.rs`, `Justfile` —
  **plus `core/src/shard/blocking.rs`, as of rev 2's hotfix H1b** (§Problem 3c). That file is now
  **shared** between 83 and 97 rather than disjoint. The overlap is small (H1b inserts two
  `store.purge_if_expired(dest);` lines above `:763` and `:790`, inside `LmpopSatisfier::satisfy`)
  but it is real, and it is a second reason to land 83 first: H1b then rebases onto a settled
  file instead of the reverse. The other shared files stay read-vs-rename on disjoint lines:
  `store/mod.rs` (83 reads `:522-573`, 97 renames `:402`) and `store/hashmap.rs` (83 reads
  `:480-498`, 97 renames `:934`).
- **Note for 83: `commands/src/blocking.rs` and `core/src/shard/blocking.rs` are different
  files.** 83 cites the latter; 97 rev 1 edited only the former, and rev 2 edits **both** (H1
  and H1b respectively — they are two instances of the same defect, not one). 84 and 94 both
  make this distinction too.

**vs proposal 90 (`CommandSpec::DEFAULT`, solo-last sweep over `frogdb-commands`).** **Hard
ordering constraint: 97 lands before 90's commit 3.** 90 rewrites the 296 `static SPEC:
CommandSpec` literals across every file in the crate and is solo-last by its own scoping; 97
edits `execute` bodies in ~24 of those files. **97 changes no spec literal** — no `wal:`,
`lookup:`, `access:`, `reindex:` or `event:` value moves, because routing a read through the
expiry-honouring door does not change what the command *declares* it does. Verified by inspecting
every handler 97 touches: every edit is inside `fn execute`. The regions are structurally disjoint
(90 works between `static SPEC` and `&SPEC`; 97 works below `fn execute`), so 90's rebase over 97
is mechanical. **97 must not be scheduled after 90's sweep begins**, since 97 touches 24 files 90
holds.

**vs proposal 91 (`CommandContext` narrowing).** **One shared line.** 91 fixes `typed.rs:107` — a
doc reference to `CommandContextCore::store` that dangles once that type dies. 97 rewrites the
`typed.rs` module banner and the `StoreTypedExt` doc block, which contains `:107`. **One-line
textual conflict, no semantic overlap.** 91 also rewrites `core/src/command.rs:1035-1088` and
`:1260-1377`; 97 reads `:1059`/`:1262` as evidence only and edits nothing there. If 91 lands
first, 97 writes the banner around whatever 91 left; if 97 lands first, 91's one-line doc fix
applies unchanged.

**vs proposal 92 (`ExpiryDecision` table).** **Boundary is the probe vs the ladder, and they
touch adjacent lines in `expiry.rs`.** 92 owns the decision: given `(exists, current_deadline,
requested_deadline, NX/XX/GT/LT)`, produce `(action, reply)` — the five ladders at
`expiry.rs:287-321`, `:376-408`, `:453-492`, `:537-576` and `hash.rs:1062-1123`. 97 owns the
**input** to `exists`: the eight `if !ctx.store.contains(key) { return … }` guards at
`expiry.rs:283`, `:372`, `:449`, `:533`, `:611`, `:664`, `:749`, `:799`, each sitting **4 lines
above** one of 92's ladders. Real textual adjacency; zero semantic overlap. **One shared warning
in both directions: 92 must not pin the current NX/XX/GT/LT behaviour against a `contains` input
that is wrong for expired keys**, and 97 must not change the ladder while replacing the guard. If
92 lands first, 97's edit applies to whatever 92 named the guard. Neither blocks the other.

**vs proposals 80, 84, 94 (all cite `commands/src/blocking.rs`).** Verified line by line:

| Proposal | Its lines in `commands/src/blocking.rs` | Overlap with 97's 14 |
|---|---|---|
| **84** (`BlockingOp`/`Direction` dedupe) | `:12`, `:230`, `:231`, `:365` — declared **read-only evidence**, 84 does not edit this file | **None.** 84's `:230`/`:231` sit 4 lines above 97's `:235`; adjacent, not shared. |
| **80** (response wire fold) | `:95`, `:177`, `:294`, `:435`, `:520`, `:605`, `:749`, `:852` — the 8 `Response::BlockingNeeded` producers, "mechanical churn only" | **None semantically.** Every one is the *fall-through* below the fast path 97 rewrites; the nearest pair is 80's `:95` vs 97's `:67` (28 lines). |
| **94** (RESP3 shape once) | `:512`, `:597`, `:725` — the three `score_response(score, is_resp3)` calls | **Same function bodies, ~20 lines apart** (97's `:491`, `:576`, `:704`). Semantically disjoint (94 owns the reply shape, 97 owns the read), but **97 shortens those bodies by ~7 lines each, so 94's line numbers move**. Whichever lands second re-derives three line numbers. Flagged as the only real rebase cost among the three. |

**vs the `frogdb-core` sites.** Rev 1 claimed there were three and that all three were benign.
Both halves were wrong: §Problem 7 inventories **26 production sites**, and three of them
(`shard/blocking.rs:763`, `:790`, `:793`) are a **LIVE** instance of the very defect this
proposal exists to close, on the woken-blocked-client path. Rev 2's disposition, per site class:

- `shard/blocking.rs:763`/`:790`/`:793` — **LIVE, fixed by hotfix H1b**, and inside the gate's
  scope under option (ii) in §(d).
- `search_hook.rs:75`, `:128`, `:159` (`refresh_key`, `reindex_hash_key`, `reindex_json_key`) —
  **LATENT**; they run post-write on a key the command just wrote, so the key is live by
  construction.
- `shard/blocking.rs:430`, `:445`, `:474` (`apply_restore`) — **LATENT**; re-create-then-fill on
  a key the same statement just wrote.
- `shard/rollback.rs:62` — out of scope by *design*, and the reason `get_unchecked` survives at
  all: a rollback snapshot must not expire the key it is snapshotting.
- `shard/active_expiry.rs:223`, `:229` — the expiry cycle itself; purging inside it is circular.
- `store/typed.rs:138`, `:185`, `:203` — inside the seam, after its own purge.
- **`shard/search/query.rs:115`, `:208`, `:756` — LIVE-suspect and explicitly out of this
  proposal's scope.** FT.SEARCH materialises result documents through raw `get`, and unlike
  `search_hook.rs` it reads keys it did **not** just write, so an expired-but-unreaped key can be
  returned in a result set that `EXISTS` denies. Diagnosing whether the index itself already
  excludes such keys requires reading the index maintenance path, which is a different proposal.
  **Issue to file: "FT.SEARCH may materialise expired keys through raw `Store::get`
  (`shard/search/query.rs:115`, `:208`, `:756`)" — investigate, not covered by 97.**

### Other risks

- **This is a behaviour change across ~40 commands, and that is the largest risk in the
  proposal.** Every change in §Problem 2's table moves toward Redis, but each is a change, and
  each needs a regression row before merge. This is why the effort is **L** rather than **M** and
  why the migration is staged per family (§Effort) rather than landed as one sweep.
- **LRU/LFU accounting changes for the migrated reads.** `get_with_expiry_check` touches
  `metadata.touch()` and `lfu_log_incr` (`hashmap.rs:1139-1148`); raw `get` does not. So
  `OBJECT IDLETIME` / `OBJECT FREQ` answers change for LLEN, LRANGE, STRLEN, ZUNION, … and those
  keys become harder to evict. **This is a fix** — Redis's `lookupKeyRead` updates LRU for exactly
  these commands, and `GET` already does it here — but it is an eviction-behaviour change under
  `maxmemory` and belongs in the merge notes. `suppress_touch` (`hashmap.rs:190`) already exists
  for the paths that must not touch.
- **Locked-crate touch, priced.** The rename in commit 5 edits **3 test-only lines** in
  `frogdb-recovery` (`src/tests.rs:204`, `:624`, `:1454`) — pure identifier substitution, no
  production code, no behaviour. `cargo mutants` mutates production functions, so the score cannot
  move; but the push discipline (*"before pushing changes that touch a locked crate: `just
  mutants-diff <crate>`"*) attaches formally. **Mitigation: commit 5 is the rename and nothing
  else**, so the locked-crate diff is three lines a reviewer reads in ten seconds and
  `mutants-diff frogdb-recovery` runs on a diff with no production hunks. **Alternative R4 drops
  the rename entirely** at the cost of call-site legibility; the orchestrator should rule.
- **`exists_for_write` makes several read-shaped handlers mutating — and rev 1's claim that no
  `READONLY` command is affected was false.** The *"Key already exists"* family itself checks
  out — all seven probes (`bloom.rs:95`, `cuckoo.rs:84`, `topk.rs:103`, `tdigest.rs:96`,
  `cms.rs:41`, `cms.rs:106`, `timeseries.rs:134`) sit inside a `CommandFlags::WRITE` spec:
  `BF.RESERVE` `bloom.rs:25`, `CF.RESERVE` `cuckoo.rs:26`, `TOPK.RESERVE` `topk.rs:23`,
  `TDIGEST.CREATE` `tdigest.rs:51`, `CMS.INITBYDIM` `cms.rs:23`, `CMS.INITBYPROB` `cms.rs:88`,
  `TS.CREATE` `timeseries.rs:115`. **The false step is the inference**: Class C is ~21 sites, of
  which the family is 7, and two `READONLY` commands elsewhere in it *do* gain a mutation:
  - **`OBJECT` (`generic.rs:337`, `CommandFlags::READONLY`)** — `OBJECT REFCOUNT`'s probe at
    `generic.rs:466` is a `contains`. Migrating it to `exists_for_write` makes a `READONLY`
    command reap.
  - **`BITFIELD_RO` (`bitmap.rs:466-483`, `CommandFlags::READONLY`, `WalStrategy::NoOp`,
    `LookupSpec::None`)** — it shares `execute_bitfield` (`bitmap.rs:492`) with the `WRITE`
    `BITFIELD` (`bitmap.rs:418`), differing only in the `readonly: bool` argument. The
    `key_is_new` probe at `bitmap.rs:503` and the second read at `:506` are on the shared path,
    so a naive migration purges under `BITFIELD_RO` too.

  **Consequence for hotfix H2 and for Class C generally: the purge must be gated `if !readonly`
  on any shared handler, and `OBJECT REFCOUNT` must keep a non-mutating probe**
  (`exists_unexpired`, `store/mod.rs:422`) rather than `exists_for_write` — a `READONLY` command
  that mutates the keyspace breaks the flag's contract, and `WalStrategy::NoOp` means the reap
  would not even be journalled. Beyond that, the lazy-purge report buffers are populated on paths
  that previously never populated them; they are drained after every command regardless
  (`execution.rs`, `worker.rs:679-686`), so no new drain seam is needed, but this should be
  verified rather than assumed at implementation time.
- **Pre-existing defect 97 inherits but does not cause: `get_or_create_typed` under CLIENT
  PAUSE.** `typed.rs:198-213` calls `self.purge_if_expired(key)` and **discards the return
  value**, then probes with `self.get(key)`. Normally the purge removed the corpse and the probe
  sees `None`. But `check_and_delete_expired` (`hashmap.rs:480-487`) has an early return: when
  `self.expiry_suppressed` is set — i.e. during `CLIENT PAUSE` — it reports the key expired
  **without deleting it**. `purge_if_expired` then returns `true` while the corpse is still in
  the map, `self.get(key)` returns the expired value, and `T::from_value(&v)` sees the *old*
  type: a `get_or_create_list` on a paused-and-expired string key returns `WrongTypeError`
  instead of creating a fresh list. This is live in the seam **today**, independent of 97; 97
  merely routes far more traffic through it. The fix is one line (branch on the `bool`
  `purge_if_expired` already returns), but it belongs to whoever owns the pause/expiry
  interaction. **Issue to file: "`get_or_create_typed` returns WRONGTYPE for an expired key while
  `CLIENT PAUSE` suppresses physical deletion (`store/typed.rs:198-213` ×
  `store/hashmap.rs:480-487`)."** Not implemented here.
- **Feature-gated families.** `bloom`, `cuckoo`, `topk`, `tdigest`, `cms`, `timeseries`, `json`,
  `geo` and `stream` are behind `full`/`cmd-full`, and only docs-gen and allowlisted tooling may
  request those (linted). **~18 of the 69 sites are therefore not compiled by `just test
  frogdb-commands` under `core-profile`.** The migration must not alternate feature flags in an
  iteration loop (cache thrash); do the `core-profile` families first (commits 1-3), then one
  `cmd-full` pass (commit 4).
- **Mutation-score exposure of the core change: none.** Edited crates are `frogdb-commands`,
  `frogdb-core`, plus `Justfile` and one regression test file. None of the four locked pairs
  (txn/vll, persistence/recovery, replication/replication-runtime, cluster/cluster-runtime; ADRs
  `0002`-`0004`) is edited by commits 1-4.
- **Docs.** The Redis-compatibility deltas page should gain nothing — every behaviour change
  removes a delta rather than adding one — but the page should be checked for any entry that
  documents the *current* wrong answer as intentional.

## Spec / gates

- **Failure-mode specs: silent.** No `FM-` row governs command-level lazy key expiry.
  `grep -rn 'lookupKeyRead\|lazy expiry\|expired-but'` over `.scratch/hardening/specs/*.md`
  returns only replication/persistence rows about snapshot export
  (`replication-failure-modes.md:128`, `:1135`), replica-independent expiry (`:659`,
  `FM-REPLICATION-030`), and recovery counting (`persistence-failure-modes.md:494`). **No edited
  file contains an `FM-` tag** — `grep -rn 'FM-' crates/commands/src` returns nothing; the only
  `FM-` strings in the edited core files are three `FM-PERSISTENCE-044` comments at
  `hashmap.rs:2608`, `:2638`, `:2666`, in test regions this proposal does not touch. `just
  lint-failure-modes` (`scripts/failure-modes.py`) sees no renamed, moved or retagged test.
  **No spec-first obligation attaches.**
- **`lint-no-typed-unwrap` (`Justfile:998-1039`) — in scope, and currently overclaiming.**
  Reconciled in full at §Problem 6: it passes today, its success message asserts more than its two
  patterns check, and all 39 hand-rolled `WrongType` sites plus 8 immutable check-then-unwraps
  survive it. §(e)/**H3** fixes it.
- **`lint-store-expiry-seam` — new, §(d).** Fifteenth member of `lint-gates` (which lists
  fourteen today), compile-free, so it runs on every commit and in the `seam-gates` CI job.
  Follows `agents/seam-lints.md`'s existing conventions (grep gate, path-scoped, error message
  naming the correct alternative). Its scope is an open ruling — §(d).
- **The other fourteen gates: unaffected.** `lint-clock-seam` — the change adds **no** clock read
  to `frogdb-commands`; the purge's `crate::clock::now()` lives in `hashmap.rs` and is reached
  through existing code. `lint-metrics-chokepoint` — `expired_keys` (`hashmap.rs:480-498`) and the
  lazy-purge buffers are bumped from the same statements as today; only the *frequency* changes.
  `lint-error-sanitize` (`scripts/error-sanitize.py`) — a distinct gate from `lint-no-typed-unwrap`
  despite the brief conflating the two; it concerns error-message sanitisation and no message text
  changes here. `lint-keyspace-notify-routing` — no new emit site; the `expired` notification for a
  newly-reaped key routes through the existing coordinator drain. The remaining ten (redirects,
  pubsub confirmation, failover atomicity, INFO, float format, durable-ack, nested config,
  script gating, continuation lock, turmoil) are unreachable from this diff.
- **Feature profile.** See §Risks — 18 sites need `cmd-full`.

## Effort

**L**, staged as five commits. **M** if commits 4 and 5 are deferred (drop the eight
`cmd-full`-only families and the rename), which leaves the LIVE defects fixed, the seam
established for every `core-profile` command, and the gate running.

| # | Commit | Files | Size | Depends on |
|---|---|---|---|---|
| 1 | **H1 + H1b + H2 + their regression tests** | `commands/src/blocking.rs`, **`core/src/shard/blocking.rs`**, `commands/src/bitmap.rs`, `redis-regression/tests/expire_tcl.rs` | ~7 production lines, ~90 test lines | — (H1b prefers 83 first — §Risks) |
| 2 | **H3: fix the gate** | `Justfile` | ~4 lines | — (as a count pin) or 3 (as a hard zero) |
| 3 | **`exists_for_write` + migrate the `core-profile` families** (list, string, bitmap, set, sorted_set, hash, generic, basic, sort, blocking) | `store/typed.rs`, ~12 command files | ~51 sites, net ≈ −300 lines | 1 |
| 4 | **Migrate the `cmd-full` families** (bloom, cuckoo, topk, tdigest, cms, timeseries, json, geo, stream) | ~11 command files | ~18 sites + 14 `contains`, net ≈ −90 lines | 3 |
| 5 | **Rename `get` → `get_unchecked` + add `lint-store-expiry-seam`** | `store/mod.rs`, `store/hashmap.rs`, `store/typed.rs`, **26 production + 23 test sites across `core/src`** (§Problem 7 — `shard/{rollback,search_hook,blocking,active_expiry}.rs`, `shard/search/query.rs`, …), **`recovery/src/tests.rs` (locked, 3 lines)**, `Justfile` | **~45 lines** (rev 1 said ~25; §Problem 7 re-counts), all mechanical | 3, 4 |

Net across all five: ≈ **−390 production lines** in `frogdb-commands`, **+40** in
`frogdb-core`, **+35** in `Justfile`, **+~200** test lines. The size is breadth — 83 call sites in
24 files — not depth: once I1 is stated, every edit is forced.

### Independently-landable hotfix H1 — LIVE, data loss, 2 lines

**`BLMOVE`/`BRPOPLPUSH` drop the moved element when the destination is an expired list**
(§Problem 3, B2). Purge the destination before the existence check, so the create-if-absent
branch sees the truth `get_mut` is about to enforce:

- `commands/src/blocking.rs`, immediately above `:259` (`if ctx.store.get(dest).is_none()`):
  `ctx.store.purge_if_expired(dest);`
- `commands/src/blocking.rs`, immediately above `:827` (the `BRPOPLPUSH` twin): the same line.

No design change, no interface change, no dependency on the rest of this proposal.
`Store::purge_if_expired` is already on the trait (`store/mod.rs:506`) and already used from
`core/src/shard/blocking.rs:671`, `:914`, `:1079` for the structurally identical reason
(*"so a blocker woken by a write doesn't receive a stale value from a just-expired key"*,
`store/mod.rs:502-505`).

**Regression test** (`redis-regression/tests/expire_tcl.rs`, new):
`blmove_into_an_expired_destination_does_not_drop_the_element` — `DEBUG SET-ACTIVE-EXPIRE 0`;
`RPUSH src a`; `RPUSH dst old`; `PEXPIRE dst 50`; sleep 100 ms; `BLMOVE src dst LEFT RIGHT 0`
asserts `a`; then **`LLEN dst` must be 1 and `LRANGE dst 0 -1` must be `["a"]`**. Fails before the
fix (`dst` does not exist). Assert the destination, not the reply — the reply is correct today.

### Independently-landable hotfix H1b — LIVE, same defect, different file, 2 lines

**The woken-blocked-client path in `frogdb-core` has the same two bugs** (§Problem 3c). Rev 1
missed it because it scoped the audit to `frogdb-commands`; the two files share a name and
nothing else. `core/src/shard/blocking.rs` runs when a client **parked** and a later push wakes
it, so H1's fix in the command crate does not cover it — neither fix covers the other's path.

- `core/src/shard/blocking.rs`, immediately above `:763` (`let dest_is_wrong_type = store.get(dest)`):
  `store.purge_if_expired(dest);`
- `core/src/shard/blocking.rs`, immediately above `:790` (`if store.get(dest).is_none()`): the
  same line — or hoist one purge above `:762` to cover both, which is the cleaner shape since
  `:763`, `:790` and `:793` are all in `LmpopSatisfier::satisfy` and all read `dest`.

The file already does exactly this for the **source** key at `:671` (`if store.purge_if_expired(key)
{ return KeyReady::No; }`, with the comment *"Lazily purge an expired key so a blocker woken by a
write doesn't observe a stale value. Load-bearing for reblock-after-expire."*), and at `:914` and
`:1079` for the zset and stream satisfiers. **The destination was simply never given the same
treatment.** So H1b is not a new mechanism — it is the existing, commented, load-bearing pattern
applied to the one key it was omitted for.

**Regression test:** the `blmove_woken_by_a_push_into_an_expired_destination_does_not_drop_the_element`
row in §Testability 2b — it must actually park the client before expiring `dst`, or it exercises
H1's path instead of H1b's.

**Sequencing:** proposal 83 also edits `core/src/shard/blocking.rs`. Land 83 first (§Risks, HARD
ordering) and H1b rebases onto a settled file.

### Independently-landable hotfix H2 — LIVE, permanent data resurrection, 1 line

**`BITFIELD` reads an expired string, mutates it, and writes it back with the TTL erased**
(§Problem 4a). Add, as the first statement of `execute_bitfield` (`commands/src/bitmap.rs`,
above `:503`):

```rust
// BITFIELD is a lookupKeyWrite: an elapsed key must be reaped before it is
// read, or its bytes are carried into the fresh value and `Store::set` clears
// the deadline that would have removed them. Gated on `!readonly`: BITFIELD_RO
// shares this body (bitmap.rs:487) and is CommandFlags::READONLY with
// WalStrategy::NoOp (bitmap.rs:466-483) — it must not mutate the keyspace.
if !readonly {
    ctx.store.purge_if_expired(key);
}
```

**The `if !readonly` guard is load-bearing, and rev 1 omitted it.** `execute_bitfield`
(`bitmap.rs:492-495`) is called twice: from `BITFIELD` at `:418` with `false`, and from
`BITFIELD_RO` at `:487` with `true`. An ungated purge would make a `READONLY`,
`WalStrategy::NoOp`, `LookupSpec::None` command delete a key — an un-journalled keyspace
mutation from a command declared not to have any. `BITFIELD_RO` reaching the same expired bytes
is a *read* bug, closed by the Class-A migration (it should route through the typed seam and see
an absent key), not by a purge.

**Regression tests:** `bitfield_on_an_expired_key_starts_from_zero` — seed via
`BITFIELD counter SET u8 0 200`, `PEXPIRE counter 50`, sleep, then
`BITFIELD counter INCRBY u8 0 1` must reply `1` (not `201`). Fails before the fix. Plus
`bitfield_ro_on_an_expired_key_does_not_delete_it` — same seed, then `BITFIELD_RO counter GET u8 0`,
and assert the *store* is unchanged from `BITFIELD_RO`'s perspective (no `expired_keys` bump
attributable to it). That second row is what pins the guard.

### Independently-landable hotfix H3 — gate correctness, 4 lines, no production code

**`lint-no-typed-unwrap` prints an assertion it does not check** (§Problem 6, §(e)). Two pattern
widenings in `Justfile:1016` and `:1026`. As a hard gate it fails on 47 sites, so it must land
**with or after** commit 3; to keep it independently landable **today**, land it as a *count pin*
in the same shape as `lint-continuation-lock`'s per-enum arm counts:

```
expected_immutable_unwraps=8       # hash.rs:2006-2007, stream/pending.rs:262/362/420
expected_hand_wrongtype=39         # driven to 0 by proposal 97 commits 3-4
```

A new site moves the count and fails the gate; the migration drives both to zero and the pins are
deleted with the last one. Either way, **the success message stops asserting something false**,
which is the point.

**H1, H1b, H2 and H3 close** three data-integrity defects (two of them the same defect in two
files) and one gate that lies. They do **not** close: the 67 remaining raw reads in
`frogdb-commands`, the 14 `contains` probes, `BLPOP`'s permanent `WRONGTYPE` on an expired string
(B1, which needs the Class-B migration, not a purge line) and its `frogdb-core` twin B1c, the
`COPY`-resurrects-an-expired-source path, the seven `Key already exists` refusals, the eight
`EXPIRE`-family `contains` guards that resurrect a lapsed key (§Testability 3b), the FT.SEARCH
question (`shard/search/query.rs`, filed as its own issue), the `get_or_create_typed` ×
`CLIENT PAUSE` interaction (filed, not implemented), or the ability for the eighty-fourth handler
to reach past the seam tomorrow. **That is what the proposal is for.**
