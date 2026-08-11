# Proposal 96 — the GEO `STORE` effect is spelled eight times; the copy that disagrees with the other seven is the one Redis agrees with, and both `_RO` commands honor `STORE` — including its **delete** branch

Round 38 · lane: commands + types · candidate **H6** · effort **M** (the unification)
+ **S** (one independently-landable hotfix, a **LIVE** unreplicated write-**and-delete** bug) ·
**no locked crate edited** (`frogdb-commands`, `frogdb-server/crates/redis-regression`,
`testing/fuzz`) · **zero `FM-` tags anywhere in `crates/commands/src`** (verified: `grep -rn
"FM-" frogdb-server/crates/commands/src` returns nothing) · **one commands-scoped seam gate
(`lint-no-typed-unwrap`) verified unaffected** (§Gate check).

**Verified at HEAD `3bcf83455e7126e435f3c67eb0d5ca1949a67329`** (worktree
`arch-round-38-99`). Re-verified at `02a1be93` — `git diff 3bcf8345 HEAD --
frogdb-server/crates/commands/src/geo.rs` is **empty**, so every `geo.rs:line` below is still
exact. `geo.rs` was last modified by `38ce99d1` and is clean in the shared working tree
(`git status --short` lists only `.scratch/` files). Nothing is inherited from the lane brief —
the brief's line ranges were re-derived and are corrected below, and its headline ruling is
**refuted**.

## Corrections to the lane brief

| Brief claim | Verified at HEAD |
|---|---|
| "store-into-dest logic duplicated ×3 at `geo.rs:429-456`, `:531-557`, `:695-722`" | **Adjusted, and understated.** The store-and-notify blocks are `:439-456`, `:543-557`, `:708-722` (the brief's starts are 8–13 lines early and land mid-`if`). More importantly the *effect* has **eight** sites, not three: three store-and-notify blocks **plus five clear-and-notify blocks** (`:421-426`, `:432-437`, `:534-542`, `:669-674`, `:699-707`). Unifying only the three would leave the more numerous half behind. |
| "GEOSEARCHSTORE replies `results.len()`, the other two reply `dest_zset.len()`" | **Verified.** `:450` vs `:552` vs `:717`. `git blame` dates the drift: `:450` from `813dac97b` (2026‑01‑21), `:552`/`:717` from `b66f6f952` (2026‑02‑25) — five weeks and one copy-paste apart. |
| "these differ when results contain duplicate members **or dest pre-exists**" | **Refuted, both halves.** A pre-existing dest cannot contribute: every site builds a **fresh** `SortedSetValue::new()` (`:440`, `:543`, `:708`) and `Store::set` replaces the entry wholesale with fresh metadata (`core/src/store/hashmap.rs:945-951` — which also clears any TTL, matching Redis `setKey(...,0)`). Duplicate members cannot occur either — proof in §Problem 2. **Ruling: LATENT, not LIVE.** |
| "Redis semantics = dest cardinality" | **Refuted.** `geo.c georadiusGeneric` ends `addReplyLongLong(c, returned_items)`, where `returned_items` is the **result-array length after COUNT truncation**, not `zsetLength(zobj)`. So `results.len()` — the GEOSEARCHSTORE spelling — is the Redis-faithful one, and the fix direction is the opposite of the brief's. §Redis compatibility note. |
| "2 parallel option parsers" | **Verified.** `parse_geosearch_options` `:866-1026`, `parse_georadius_options` `:1029-1084`. Seven arms are token-identical (mechanical diff receipt in §Problem 5). |
| "duplicated 14-field `GeoSearchOptions` struct ×2" | **Adjusted.** There is **one** `GeoSearchOptions`, with **13** fields (`:845-860`), and one `GeoRadiusOptions` with 9 (`:832-843`) — different structs, not two copies. What *is* duplicated ×2 is the **15-line construction** of the former from the latter: `:514-528` and `:679-693` are **byte-identical** (mechanical diff receipt below). |
| — (not in the brief) | **A LIVE bug the store audit surfaced: `GEORADIUS_RO` and `GEORADIUSBYMEMBER_RO` accept `STORE`/`STOREDIST`, and the effect they perform is a write *or an unconditional `DELETE` of the destination*** — from a `READONLY`-flagged, `WalStrategy::NoOp`, `KeySpec::First` command. Neither is WAL'd, replicated, routed, ACL-checked, nor WATCH-invalidating. §Problem 3. This, not the reply count, is H6's live centerpiece. |
| — (not in the brief) | **A second, adjacent hole this proposal does *not* close:** the read-only **script** gate uses a hardcoded name list whose geo entries are only `"GEOADD" \| "GEOSEARCHSTORE"` (`core/src/scripting/bindings.rs:65-66`), so `EVAL_RO`/`FCALL_RO` admit `GEORADIUS`, `GEORADIUSBYMEMBER` **and** both `_RO` variants with `STORE`. §Problem 3b — **file separately**. |

## Summary

GEO's "store the search result into a destination key" is one rule with five parts —
*empty result ⇒ delete the dest and fire `del` only if something was deleted; non-empty ⇒
build a zset, `set` it, fire the family's event, reply the count*. That rule is written out
**eight times** in one file, by three commands, and the copies have already drifted in the
one place a copy can drift silently: the count. GEOSEARCHSTORE replies `results.len()`
(`:450`); the two legacy commands reply `dest_zset.len()` (`:552`, `:717`). The two
expressions are equal today — but only because of a four-step, cross-crate argument through
private bit arithmetic in `frogdb-types` that nothing states and nothing tests (§Problem 2).

The same eight-way spread hides a live defect one level up. The legacy option parser has no
way to say "this caller may not use `STORE`" — the modern one already has exactly that
knob for `STOREDIST` (`allow_storedist`, `:870`, `:988-990`) — so `GeoradiusRoCommand`,
whose entire body is `GeoradiusCommand.execute(ctx, args)` (`:791`), inherits the write
path wholesale. `GEORADIUS_RO src lon lat r unit STORE dest` writes `dest` today. Redis
answers `-ERR syntax error`.

And the destructive half is worse than the constructive one. Because the `STORE` tail is
*empty-result ⇒ `ctx.store.delete(dest)`*, a `READONLY`-flagged command that matches nothing
performs **silent, unlogged, unreplicated key destruction**: `GEORADIUS_RO src 0 0 1 m STORE
victim` reaches `geo.rs:538` (`:703` for `GEORADIUSBYMEMBER_RO`) and deletes `victim`
outright. On a primary the deletion is *real and permanent in memory* yet leaves no WAL
record and never reaches a replica — so the replica keeps `victim` and the pair diverges in
the direction that survives a failover. On a replica the command is accepted at all, because
the `-READONLY` refusal keys off `CommandFlags::WRITE`. Data loss through a command whose
name ends in `_RO` is the sharpest edge in this file.

The change is one module-internal seam plus two adapters: `store_geo_results` (the effect,
once), one shared flag grammar behind an `allow_store` flag, and a `GeoSearchOptions`
constructor that collapses the two 15-line literals. Eight effect sites become eight
one-line calls; the three commands become what they should have been — parse, search, hand
the result to the seam. Net ≈ −90 lines in `geo.rs`, with the store rule stated once, next
to its `geo.c` citation.

**Scope, stated plainly:** `allow_store = false` closes the `_RO`-command half of the
read-only hole. It does **not** close the read-only-*script* half — `EVAL_RO`/`FCALL_RO`
admit `GEORADIUS … STORE` because the script gate consults a hardcoded name list, not the
registry's `CommandFlags` (§Problem 3b). That is a separate defect in a different crate with
a different fix, and it is **to be filed as its own issue**, not folded in here.

## Files involved

Line counts at `3bcf8345`. Paths under `frogdb-server/crates/` unless noted.

| Path | Lines | Role in this change |
|---|---|---|
| `commands/src/geo.rs` | 1372 | **The change, and the only production file edited.** New `store_geo_results` + `build_dest_zset` + `parse_shared_geo_flag` + `GeoSearchOptions::from_legacy`; eight effect sites (`:421-426`, `:432-437`, `:439-456`, `:534-542`, `:543-557`, `:669-674`, `:699-707`, `:708-722`) become calls; two 15-line literals (`:514-528`, `:679-693`) become one line each; `parse_georadius_options` `:1029-1084` gains `allow_store`; `GeoradiusRoCommand::execute` `:789-792` and `GeoradiusbymemberRoCommand::execute` `:821-824` become adapters that pass `allow_store = false`. **Zero `FM-` tags; behind the `geo` cargo feature** (`commands/Cargo.toml:26`, `full` `:41`; server maps `cmd-geo = ["frogdb-commands/geo"]`, `server/Cargo.toml:86`). |
| `types/src/geo.rs` | 725 | **Read-only evidence, plus one fuzz-adjacent invariant.** `geohash_calculate_areas` `:597-620`; `geohash_neighbors` `:494-519`; `geohash_move_x/_y` `:448-490` (Morton ±1 with wraparound masking — the torus that makes the LATENT ruling hold); `geohash_estimate_steps_by_radius` `:426-444` (`step.clamp(1, 26)` `:443`); `geohash_score_range` `:415-422` (half-open `[bits<<sh, (bits+1)<<sh)` — the disjointness). Not edited. |
| `redis-regression/tests/geo_regression.rs` | 110 | **Primary test target.** FrogDB-specific geo regressions; **contains no `STORE` coverage at all** (`grep -in store` → zero hits). Home for the `_RO` red-green tests and the reply-count pins. |
| `redis-regression/tests/geo_tcl.rs` | 2285 | **Read-only evidence + six one-line additions.** Six STORE ports (`tcl_georadius_store_plain_usage :1898`, `tcl_georadiusbymember_store_storedist_plain :1935`, `tcl_geosearchstore_plain_usage :2001`, `tcl_georadius_storedist_plain_usage :2039`, `tcl_geosearchstore_storedist_plain_usage :2081`, `tcl_georadius_storedist_count_asc_desc :2125`) verify the **destination contents** via `ZRANGE`/`ZCARD` and **not one of them asserts the STORE reply**. The file *does* assert integers — via its own `assert_integer_eq` helper, 14 times (`:188`, `:220`, `:263`, `:293`, `:392`, `:408`, `:425`, `:461`, `:498`, `:526`, `:551`, `:640`, `:2159`, `:2183`) — so the "no integer assertions" claim must be stated precisely: **four of the 14 do pin a STORE reply, and all four pin the `Integer(0)` empty branch** (`:188`/`:263` GEORADIUS…STORE ⇒ site 4; `:220`/`:293` GEOSEARCHSTORE ⇒ sites 1 and 2); the other ten target `GEOADD` (×8) and `ZCARD` (×2). **The non-empty count — the expression that actually drifted — is pinned nowhere.** |
| `server/tests/integration_pubsub.rs` | 6247 | **Read-only evidence.** All seven geo pubsub tests: `test_geoadd_emits_zadd :3251`, `test_geoadd_noop_emits_nothing :3278`, `test_geosearchstore_notifies_destination :3314`, `test_geosearchstore_empty_result_emits_del :3367`, `test_geosearchstore_empty_missing_dest_silent :3409`, `test_georadius_store_notifies_georadiusstore :3586`, `test_georadius_store_empty_result_emits_del :3624`. **Three of the family's three notify behaviours are pinned for GEOSEARCHSTORE, two of three for GEORADIUS (the silent-missing-dest case is absent), and zero for GEORADIUSBYMEMBER** — the asymmetry that let the copies diverge. Added tests restore parity. |
| `testing/fuzz/fuzz_targets/geo_ops.rs` (repo root) | 166 | **One added assertion.** `Op::CalculateAreas :104-113` already fuzzes `geohash_calculate_areas` and asserts `areas.len() == 9` `:108` and `1 <= step <= 26` `:110`. Adding *"every repeat of a `(bits, step)` in the returned sequence is contiguous"* turns §Problem 2's hand proof into a machine-checked invariant, in the file that already exists, at zero runtime cost. |
| `core/src/store/hashmap.rs` | 2977 | **Read-only evidence.** `fn set :945-951` — `replace_entry(key, value, KeyMetadata::new(0))`: overwrite is wholesale and **clears the TTL**. Identical for all three commands; the brief's "dest pre-exists ⇒ merge" hypothesis dies here. |
| `core/src/shard/execution.rs` | 2132 | **Read-only, load-bearing for the LIVE bug.** `:304-305` — `let meta = if is_write { effects.into_write_meta(handler) … } else { None }`, with `is_write` = `CommandFlags::WRITE` (`:156`, `:434`). A non-WRITE command's store mutation produces **no** `WriteCommandMeta`: no WAL, no replication, no keyspace event (the deposits at `command.rs:1427-1429` are simply dropped), no dirty bump. |
| `core/src/shard/post_execution.rs` | 1907 | **Read-only.** `WriteEffectKind::VersionIncrement :322-340`, whose `warranted` condition is the `match scope` at **`:327-332`** (`dirty_delta >= 0` for `Command`/`ScatterPart`/`InternalRemoval`) — and `dirty_delta` is `0` for all three geo commands (`grep -n dirty geo.rs` → nothing); `update_dirty_counter :690-699` (a `0` delta counts as exactly **1** change, `:696`); `invalidate_keys_all_modes :686` (client-tracking, from `handler.keys(args)`). Source of §Problem 4. |
| `core/src/scripting/gate.rs` | — | **Read-only, load-bearing for §Problem 3b.** `:236-239` — the read-only-script refusal is `is_write_command(&name)`, a **name-list** consult, not `spec().flags`. `:518-521` — the scripted-write *recorder* gates on `handler.flags().contains(CommandFlags::WRITE)`, so a scripted `GEORADIUS … STORE` is neither recorded nor replicated. |
| `core/src/scripting/bindings.rs` | — | **Read-only, load-bearing for §Problem 3b.** `pub fn is_write_command :44-73`; the geo arm is `"GEOADD" \| "GEOSEARCHSTORE" => true` (`:65-66`). `GEORADIUS`, `GEORADIUSBYMEMBER` and both `_RO` variants are absent ⇒ `_ => false` ⇒ admitted by `EVAL_RO`/`FCALL_RO`. |
| `server/src/connection/guards.rs` | 1886 | **Read-only, load-bearing for the LIVE bug.** The pre-dispatch write ladder is gated on `CommandFlags::WRITE` at `:262` (`-READONLY` on a replica), `:293` (`-MISCONF`), `:306` (self-fence/quorum), `:328` (`min-replicas-to-write`). A `READONLY`-flagged command that writes walks through all four. |
| `core/src/command_spec.rs` | — | **Read-only.** `KeySpec::First` `:65-68` is a panic-safe `match args.first() { Some(k) => vec![k.as_ref()], None => Vec::new() }` — it yields **at most the first key**, which is why `GEORADIUS_RO`'s destination is invisible to ACL key checks, cluster slot validation, WATCH invalidation and `COMMAND GETKEYS`. |
| `server/src/server/register.rs` | 922 | **Read-only.** The registry parity gates: `every_write_command_declares_event :311-323`, `every_write_command_declares_wal :328-340`, `no_read_command_declares_reindex :908-921`. All three test **spec facts**; none can see a `READONLY` command that mutates the store at runtime — §Problem 3 explains why the gate family cannot be extended cheaply here and the fix belongs at the parser. |
| `commands/src/utils.rs` | 1241 | **Read-only.** `format_float` / `parse_f64` / `parse_usize` / `NxXxOptions`, imported at `geo.rs:23`. Untouched. |
| `commands/src/lib.rs` | 473 | **Read-only.** `registry.register(geo::GeoradiusRoCommand)` `:305` and siblings — the `_RO` commands are really registered, so the LIVE path is reachable in a default `full` build. |

## Problem

### 1. One effect, eight spellings

The GEO STORE rule, in full, as `geo.c` states it once:

> non-empty result ⇒ build a zset from the results (score = distance under `STOREDIST`,
> geohash score otherwise), `setKey` the destination (dropping its TTL), notify
> `geosearchstore`/`georadiusstore` under `NOTIFY_ZSET`, reply the result count; empty
> result ⇒ `dbDelete` the destination, and **only if that deleted something** notify `del`
> under `NOTIFY_GENERIC` and count a change; reply 0.

`geo.rs` says it eight times:

| # | Site | Command | Shape | Reply | Event | Reply pinned? |
|---|---|---|---|---|---|---|
| 1 | `:421-426` | GEOSEARCHSTORE, source key missing | clear | `Integer(0)` `:426` | `"del"` `:424` | yes — `geo_tcl.rs:220` |
| 2 | `:432-437` | GEOSEARCHSTORE, empty result | clear | `Integer(0)` `:436` | `"del"` `:434` | yes — `geo_tcl.rs:293` |
| 3 | `:439-456` | GEOSEARCHSTORE, non-empty | store | **`results.len()`** `:450` | `"geosearchstore"` `:454` | **no** |
| 4 | `:534-542` | GEORADIUS STORE, empty result *(also the missing-source path — `GeoradiusCommand` has no separate `None` arm)* | clear | `Integer(0)` `:541` | `"del"` `:539` | yes — `geo_tcl.rs:188`, `:263` |
| 5 | `:543-557` | GEORADIUS STORE, non-empty | store | **`dest_zset.len()`** `:552` | `"georadiusstore"` `:556` | **no** |
| 6 | `:669-674` | GEORADIUSBYMEMBER STORE, source key missing | clear | `Integer(0)` `:673` | `"del"` `:671` | **no** — and **Redis-unreachable**, see below |
| 7 | `:699-707` | GEORADIUSBYMEMBER STORE, empty result | clear | `Integer(0)` `:706` | `"del"` `:704` | **no** |
| 8 | `:708-722` | GEORADIUSBYMEMBER STORE, non-empty | store | **`dest_zset.len()`** `:717` | `"georadiusstore"` `:721` | **no** |

**Two of the eight sites have no Redis counterpart at all**, and the shared helper's
doc-comment must not pretend otherwise:

- **Site 6 is unreachable in Redis.** `georadiusbymemberCommand` resolves the member *first*
  — `longLatFromMember` → `zsetScore` on a `NULL` `zobj` → `C_ERR` → `-ERR could not decode
  requested zset member`. Control never reaches the `storekey` tail, so Redis has no
  "BYMEMBER with a missing source key + STORE" behaviour to be parity with. FrogDB instead
  returns `Integer(0)` (with STORE) / empty array (without).
- **Site 1's `FROMMEMBER` sub-case is likewise unreachable**: the member lookup runs inside
  Redis's option loop, before the `zobj == NULL` check. Site 1's `FROMLONLAT` sub-case *is*
  reachable and *does* match Redis.

These two are **FrogDB-only entries tracked separately** (§Residue, third bullet). The
`store_geo_results` doc-comment cites `geo.c` for the six sites that have a `geo.c`
counterpart and says "FrogDB-only, no upstream counterpart — see proposal 96 §Problem 1" for
these. Citing `geo.c` parity for behaviour Redis cannot produce is exactly the kind of false
provenance a single shared comment makes permanent.

Mechanical receipts (whitespace-normalized line comparison, run at HEAD):

- **Sites 5 and 8 are identical, all 26 lines** (`:533-558` ≡ `:698-723`) — including the
  three-line `geo.c` comment.
- **Sites 3, 5, 8 share an identical 9-line zset-build body** (`:440-448` ≡ `:543-551` ≡
  `:708-716`). They differ in exactly two tokens each: the count expression and the event
  name.
- **Sites 2 and 4 differ only in the binding name** (`destkey` vs `dest`); comments aside,
  the diff is two lines, both renames.

Nothing in the test suite distinguishes the copies **where they differ**. The six regression
STORE ports check what landed in the destination with `ZRANGE`/`ZCARD` (`geo_tcl.rs:1929-1930`,
`:1965-1967`, `:1985-1997`, `:2033-2034`, `:2071`, `:2115`, `:2159`, `:2183`) and never the
reply. Four `assert_integer_eq` calls *do* pin a STORE reply (`:188`, `:220`, `:263`, `:293`) —
but all four assert `0`, i.e. sites 1, 2 and 4, the three sites whose reply is a literal.
Sites 3, 5 and 8 — the three that carry the drifted expression — and sites 6 and 7 are
unpinned. `integration_pubsub.rs` pins three notify behaviours for GEOSEARCHSTORE, two for
GEORADIUS (`:3586`, `:3624`) and **none** for GEORADIUSBYMEMBER. Eight copies, zero tests that
discriminate the copies from each other: the duplication is invisible to CI, which is why a
five-week-old drift is still here.

### 2. The reply divergence: ruled **LATENT** — and why that is not reassuring

Can `results.len() != dest_zset.len()`?

1. **The destination is always fresh.** Every store site constructs `SortedSetValue::new()`
   (`:440`, `:543`, `:708`) and hands it to `ctx.store.set` (`:451`, `:553`, `:718`), which
   is `replace_entry(key, value, KeyMetadata::new(0))` (`hashmap.rs:945-951`) — a wholesale
   replacement that also drops the old TTL. A pre-existing destination therefore contributes
   **zero** members. So `dest_zset.len()` = the number of **distinct** members in `results`,
   and the two counts differ **iff `results` contains the same member twice**.
2. **A member can be yielded twice only if the same geohash area is scanned twice.**
   `execute_geosearch` (`:1121-1249`) scans ≤ 9 areas, each with
   `zset.range_by_score(Inclusive(min), Exclusive(max))` (`:1165-1170`) over
   `geohash_score_range` (`types/src/geo.rs:415-422`), which is the half-open interval
   `[bits << (52-2·step), (bits+1) << (52-2·step))`. All nine areas carry the **same**
   `step` (neighbors preserve it, `types/src/geo.rs:494-519`), so distinct `bits` ⇒ disjoint
   score ranges; a member has exactly one score.
3. **The nine areas repeat only contiguously.** `geohash_move_x` / `geohash_move_y`
   (`types/src/geo.rs:448-490`) are Morton-code ±1 with wraparound masking — i.e. exactly
   `(x ± 1 mod 2^step, y ± 1 mod 2^step)`; there is no pole clamping and no zeroing. With
   `M = 2^step` and iteration order center, N, S, E, W, NE, NW, SE, SW (`:616-619`, and the
   doc comment at `geo.rs:1114-1120`), the nine offsets are
   `(0,0),(0,1),(0,-1),(1,0),(-1,0),(1,1),(-1,1),(1,-1),(-1,-1)`:
   - `step >= 2` ⇒ `M >= 4` ⇒ all nine offsets are pairwise distinct mod `M` ⇒ nine distinct
     areas.
   - `step == 1` ⇒ `M = 2` ⇒ `+1 ≡ -1`, and the sequence collapses to
     `C, A, A, B, B, D, D, D, D` — **every repeat is contiguous**. `step` cannot go below 1
     (`clamp(1, 26)`, `types/src/geo.rs:443`; the decrease-step branch is guarded by
     `step > 1`, `:610`).
4. **Contiguous repeats are skipped.** The dedupe at `geo.rs:1154-1162` compares each area
   against the *last processed* one and `continue`s on a match.

⇒ No area is scanned twice ⇒ `results` is duplicate-free ⇒ the two expressions are **always
equal**. **The divergence is LATENT. It is not the hotfix.** (The hotfix is §Problem 3.)

That is the honest ruling, and it is also the argument for the change. The two spellings
agree only because of a four-step proof that crosses a crate boundary into private bit
arithmetic (`geohash_move_y` is not `pub`), depends on an iteration order documented in a
comment, and is stated nowhere and tested nowhere. Redis needed extra machinery for the same
property — `geohashGetAreasByRadiusWGS84` **zeroes** the out-of-range latitude neighbours
(`GZERO(neighbors.south)` …) and `membersOfAllNeighbors` skips `HASHISZERO` areas; FrogDB
has no equivalent and instead wraps across the pole, relying on the bbox/haversine filter
(`geo.rs:1177`, `:1183-1192`) to discard the wrapped candidates. Add one pole-clamping
branch to `geohash_move_y`, or one area to the list, and the sequence acquires a
**non-contiguous** repeat — at which point GEOSEARCHSTORE and GEORADIUS…STORE start giving
**different answers to the same question**, and only one of them (the GEOSEARCHSTORE one)
would still match Redis. Two spellings of one rule, kept equal by an unstated invariant three
modules away, is the architectural defect whether or not it has fired yet.

### 3. The `_RO` commands are adapters that forgot they are read-only — they **write and they delete** — **LIVE**

`GeoradiusRoCommand::execute` is, in full (`:789-792`):

```rust
    fn execute(&self, ctx: &mut CommandContext, args: &[Bytes]) -> Result<Response, CommandError> {
        // Same logic as GEORADIUS
        GeoradiusCommand.execute(ctx, args)
    }
```

`GeoradiusbymemberRoCommand::execute` `:821-824` is the same shape. The delegate parses
options with `parse_georadius_options` (`:500`, `:637`), whose `STORE` / `STOREDIST` arms
(`:1063-1067`) are **unconditional** — the legacy parser has no caller-side gate, while the
modern parser has had exactly that shape of gate for `STOREDIST` since it was written
(`allow_storedist: bool` `:870`, enforced `:987-991`). Nothing else stops it: the only
STORE-related validation on the path is the WITHCOORD/WITHDIST/WITHHASH incompatibility
check (`:502-508`), which a plain `STORE dest` passes.

So `GEORADIUS_RO src lon lat r unit STORE dest` reaches `:553` and **writes `dest`**.

**And the empty-result branch deletes.** The `STORE` tail is not "write or do nothing" — it
is "write, *or delete the destination*". A search that matches nothing takes
`ctx.store.delete(&dest)` at **`:538`** (`:703` for `GEORADIUSBYMEMBER_RO`, `:670` for its
missing-source arm), so:

```
GEORADIUS_RO {g}src 0 0 1 m STORE {g}victim     # matches nothing
```

**destroys `{g}victim`** — from a command flagged `READONLY`, with `WalStrategy::NoOp`. The
deletion is real in memory, produces no WAL record, and is never replicated, so the replica
*keeps* `{g}victim` while the primary has lost it: divergence in the direction that survives
a failover *and* the direction that survives a restart (the key comes back on the primary,
having never been logged as deleted). A replica also accepts the command outright, because
the `-READONLY` refusal keys off `CommandFlags::WRITE`. Silent unlogged key destruction
through a command whose name ends in `_RO` is the sharpest edge here — sharper than the
unreplicated write, because a lost write is a missing key and a lost delete is a *resurrected*
one.

The command's spec (`:771-785`) says `CommandFlags::READONLY`, `KeySpec::First`,
`AccessSpec::Uniform`, `WalStrategy::NoOp`, `EventSpec::NotApplicable`. Consequences of that
mismatch — they apply identically to the write and to the delete — each cited:

| Consequence | Why |
|---|---|
| **The effect is never persisted.** | `execution.rs:304-305` builds a `WriteCommandMeta` only when `CommandFlags::WRITE`. No meta ⇒ no WAL record ⇒ a stored destination vanishes on restart, **and a deleted destination comes back**. |
| **The effect is never replicated.** | Same gate: the replication broadcast is driven from the write record (`post_execution.rs:73-110`). Primary and replica silently diverge — the replica missing a store, or *retaining* a key the primary destroyed. |
| **A replica accepts it.** | The `-READONLY` refusal is `flags.contains(CommandFlags::WRITE)` (`guards.rs:262`). A client can mutate — or **delete from** — a read-only replica's keyspace with `GEORADIUS_RO`. The `-MISCONF`, self-fence and `min-replicas-to-write` gates (`:293`, `:306`, `:328`) are bypassed identically, as is the shard-side OOM check (`execution.rs:156`, `check_memory_for_write`). |
| **The destination is unrouted and unauthorized.** | `KeySpec::First` yields only the source (`command_spec.rs:65-68`), and `GeoradiusRoCommand` does **not** override `dynamic_keys` the way `GeoradiusCommand` does (`:563-575`). ACL key permissions, cluster slot validation and `COMMAND GETKEYS` all see one key. In cluster mode the destination is written into whichever node owns the **source** — where no lookup will ever find it, because clients are redirected to the destination's real owner. |
| **WATCH and client-side caching miss it.** | The slot version bump and `invalidate_keys_all_modes` both hang off the write record (`post_execution.rs:322-340`, `:686`). A `WATCH dest` … `MULTI/EXEC` survives a concurrent `GEORADIUS_RO … STORE dest`. |
| **The keyspace event is silently dropped.** | `ctx.notify_event` only deposits into `CommandEffects` (`command.rs:1427-1429`); the deposits are discarded when no write meta is built. |

Redis rejects the command outright: `georadiusroCommand` calls
`georadiusGeneric(c, RADIUS_COORDS|RADIUS_NOSTORE)` and the `store`/`storedist` option arms
are guarded by `!(flags & RADIUS_NOSTORE)`, so the token falls through to the syntax-error
arm. Coverage gap that let it through: `geo_tcl.rs` ports **two** `_RO` tests —
`tcl_georadius_ro_simple_sorted :752-770` and `tcl_georadiusbymember_ro_simple_sorted
:1229-1248` — and between them they exercise a single option token, the sort order `asc`
(`:765`); neither passes `STORE`, `STOREDIST`, `COUNT` or any `WITH*`. `introspection2_tcl.rs:7`
records that the upstream `GEORADIUS`/`GEORADIUS_RO` movablekeys tests were **excluded**.

The registry gates cannot catch this class: `every_write_command_declares_event`
(`register.rs:311-323`), `every_write_command_declares_wal` (`:328-340`) and
`no_read_command_declares_reindex` (`:908-921`) check spec facts against spec facts, and here
the spec is *correct* — `GEORADIUS_RO` genuinely is a read command. The defect is that the
command's **body** is an adapter over a write command's body with no gate in between. The
structural fix is the one the modern parser already demonstrates: make "may this caller use
`STORE`?" a parameter of the grammar, so the read-only variants get their `-ERR syntax error`
for free.

### 3b. The read-only *script* gate has the same hole, and 96 does **not** close it

`allow_store = false` fixes the `_RO`-command path. It does nothing for `EVAL_RO` / `FCALL_RO`,
because the script gate does not consult `CommandFlags` at all:

```rust
// core/src/scripting/gate.rs:236-239
let is_write = is_write_command(&name);
if self.read_only && is_write {
    return Err("ERR Write commands are not allowed from read-only scripts".to_string());
}
```

`is_write_command` (`core/src/scripting/bindings.rs:44-73`) is a hardcoded uppercase-name
match whose **entire geo arm** is:

```rust
// Geo writes                                   // bindings.rs:65-66
"GEOADD" | "GEOSEARCHSTORE" => true,
```

`GEORADIUS`, `GEORADIUSBYMEMBER`, `GEORADIUS_RO` and `GEORADIUSBYMEMBER_RO` all fall to
`_ => false`. So a read-only script may call `redis.call('GEORADIUS', src, …, 'STORE', dest)`
and write. Worse, the **scripted-write recorder** gates on the same flag as the direct path —
`handler.flags().contains(CommandFlags::WRITE)` (`gate.rs:518-521`) — so a scripted
`GEORADIUS … STORE` from *any* script, read-only or not, is never turned into a script write
record and therefore never replicated. Two independent name/flag mismatches stacked on one
command.

**This is a separate defect, in a different crate, with a different fix** (drive the script
gate off the registry's `CommandFlags` instead of a hand-maintained list — the same class of
drift `lint-script-gate` exists to police). 96 does not touch it. **To be filed as its own
issue.**

### 4. `dirty_delta` is `0` at all eight sites — which is wrong twice, in opposite directions

Nothing in `geo.rs` touches `dirty` (`grep -n dirty geo.rs` → nothing), so every one of the
eight sites deposits `dirty_delta = 0` and none sets `write_was_noop`. That single omission
produces two distinct parity breaks, and they are **not** "all eight sites wrong the same
way" — they partition the eight cleanly:

**(a) The five clear sites over-report, but only when the delete was a no-op.** Sites 1, 2, 4,
6, 7 call `ctx.store.delete(dest)` and correctly gate the `del` *notification* on its `bool`.
They do not gate anything else. In the sub-case where `delete` returned `false` — the
destination never existed, **nothing at all happened** — `dirty_delta >= 0` still makes
`WriteEffectKind::VersionIncrement` `warranted` (`post_execution.rs:327-332`), so the
destination's slot version bumps, client-tracking invalidates (`:686`), `update_dirty_counter`
counts the no-op as **one** change (`:690-699`), and `replication_forms` propagates the
command verbatim (`:87-110`). Redis does the opposite: no `dbDelete` ⇒ no `signalModifiedKey`,
no `server.dirty++`, no propagation. When `delete` returns `true` FrogDB and Redis agree, and
**the three store sites are correct on this axis** (a store always is a change). Observable:
`WATCH dest` … `GEOSEARCHSTORE dest src …` (empty result, `dest` absent) … `EXEC` returns
nil-abort in FrogDB and succeeds in Redis.

**(b) The three store sites under-report, always.** `update_dirty_counter` maps a `0` delta to
exactly `1` (`post_execution.rs:696`, `1 // Default: most write commands count as 1 dirty
change`). Redis's store branch does `server.dirty += returned_items`. So a `GEOSEARCHSTORE`
that stores 10,000 members credits **1** where Redis credits **10,000** — an under-count of
9,999 per call, straight into `rdb_changes_since_last_save`, which is what
`save <seconds> <changes>` thresholds and every "is a snapshot due" heuristic read. A workload
whose only writes are GEO stores can run essentially forever without tripping a change-based
save point.

Both are one-line fixes **in the same helper**, which is why they belong here:

```rust
// (a) in the clear branch, when delete() returned false:
ctx.effects.write_was_noop = true;
// (b) in the store branch:
ctx.effects.dirty_delta = results.len() as i64;
```

Neither is a drift *between* the three commands, so neither is the hotfix — both are
correctness follow-ups that the unification makes trivially cheap (state once, fix eight).
The `write_was_noop` pattern is already in this file: `GeoaddCommand` at `:155-157`.

### 5. Two parsers, seven identical arms, and two byte-identical adapters

`parse_geosearch_options` `:866-1026` (modern grammar: `FROMMEMBER` `:894-926`,
`FROMLONLAT` `:927-942`, `BYRADIUS` `:943-953`, `BYBOX` `:954-966`, bare `STOREDIST` flag
`:987-991`) and `parse_georadius_options` `:1029-1084` (legacy grammar: `STORE <key>`
`:1063-1064`, `STOREDIST <key>` `:1065-1067`) share seven arms verbatim:

| Option | Modern | Legacy | Diff |
|---|---|---|---|
| `WITHCOORD` | `:967-968` | `:1042-1043` | none (`if` vs `} else if` at the seam) |
| `WITHDIST` | `:969-970` | `:1044-1045` | none |
| `WITHHASH` | `:971-972` | `:1046-1047` | none |
| `COUNT [ANY]` | `:973-978` | `:1048-1053` | comment wording only |
| bare `ANY` (error) | `:979-982` | `:1054-1058` | comment wording only |
| `ASC` | `:983-984` | `:1059-1060` | none |
| `DESC` | `:985-986` | `:1061-1062` | none |

A whitespace-normalized, comment-stripped comparison of `:967-986` against `:1042-1062`
differs in **one** line — the `if` / `} else if` at the head. Six of the seven options are
also duplicated a *third* time, structurally, in `georadius_store_dest` `:1095-1112`, which
re-walks the same grammar to find the destination for `dynamic_keys` (this one is
deliberate and correct — it must run before `execute` — and stays; §Risks).

The two parsers then meet in two **byte-identical** 15-line struct literals, `:514-528` and
`:679-693` (verified: identical after whitespace normalization, all 13 fields), whose only
real content is `center` — every other field is a field-for-field copy out of
`GeoRadiusOptions`. That is the adapter the type system should be writing.

### 6. Residue

- `#[allow(dead_code)]` on `GeoRadiusOptions` (`:832`) is **stale**: every one of the nine
  fields has a read site in this file (`with_coord` `:503`,`:520`; `with_dist` `:503`,`:521`;
  `with_hash` `:503`,`:522`; `count` `:523`,`:688`; `any` `:524`,`:689`; `asc` `:525`,`:690`;
  `desc` `:526`,`:691`; `store` `:533`,`:669`,`:698`; `store_dist` `:502`,`:527`,`:639`,`:692`).
  Delete the attribute; the compiler is the verification.
- The comment at `:418-419` — *"use a special mode that skips FROMMEMBER member lookup on
  missing keys"* — describes a parameter that does not exist. The argument being passed
  `true` there is `allow_storedist` (`:870`); the missing-key skip is **unconditional**
  (`:921-925`, the `None =>` arm that substitutes a `(0,0)` dummy center). Two different
  facts fused into one wrong sentence, one line above the code that proves it.
- *Adjacent, out of scope, unverified against a real server:* that same unconditional dummy
  center means `GEOSEARCH missing FROMMEMBER m BYRADIUS …` returns an empty array where
  Redis appears to answer `-ERR could not decode requested zset member` (its `longLatFromMember`
  runs inside the option loop, before the `zobj == NULL` check, and `zsetScore` returns
  `C_ERR` on a NULL object). This proposal **preserves the current behaviour verbatim** and
  files the question; it is a command-semantics issue, not a store-path one.

## Redis compatibility note (`geo.c`)

Stated from code knowledge of `geo.c` (Redis 7/8 `georadiusGeneric`), **real-server
verification outstanding** and cheap (three `redis-cli` lines):

1. **Reply of the STORE path.** After collecting the `geoArray`, Redis computes
   `returned_items = (count == 0 || result_length < count) ? result_length : count` and, on
   the store path, ends with `addReplyLongLong(c, returned_items)` — the **result-array
   length**, never `zsetLength(zobj)`. It also does `server.dirty += returned_items` on the
   store branch and `server.dirty++` only when `dbDelete` succeeded on the empty branch.
   ⇒ FrogDB's `results.len()` (GEOSEARCHSTORE, `:450`) is faithful; `dest_zset.len()`
   (`:552`, `:717`) is a coincidence that currently agrees. **Unify on `results.len()`.**
   Because the two agree at HEAD (§Problem 2), this is a direction-of-fix ruling, not an
   observable-parity claim — no user-visible reply changes.
   **And Redis treats the coincidence as an invariant, not an accident:** its store loop
   inserts each result with a raw `zslInsert` plus `serverAssert(dictAdd(zs->dict,
   gp->member, &znode->score) == DICT_OK)`. A duplicate member would fail `dictAdd` and
   **crash the server** — upstream would rather panic than silently let `returned_items`
   diverge from the destination's cardinality. That makes "results are duplicate-free" a
   *shared* contract of the geo search, not a FrogDB implementation detail that happens to
   hold, and it is the strongest argument for pinning it (§Testability 1 and 7) rather than
   leaving it to a four-step hand proof.
2. **Event names.** `notifyKeyspaceEvent(NOTIFY_ZSET, flags & GEOSEARCH ? "geosearchstore" :
   "georadiusstore", storekey, …)` on the store branch; `notifyKeyspaceEvent(NOTIFY_GENERIC,
   "del", storekey, …)` on a successful `dbDelete`. FrogDB matches at all eight sites
   (`:424`, `:434`, `:454`, `:539`, `:556`, `:671`, `:704`, `:721`) — and the specs already
   record why this must be `EventSpec::Dynamic` rather than a static `EmitsAt` (`:394-400`,
   `:476-481`, `:614-619`). **No change.**
3. **TTL on the destination.** `setKey(c, c->db, storekey, zobj, 0)` — flags `0`, so the
   destination's expire is removed. FrogDB's `Store::set` does the same
   (`hashmap.rs:945-951`). **No change.**
4. **`_RO` variants.** `georadiusroCommand` / `georadiusbymemberroCommand` pass
   `RADIUS_NOSTORE`, and both option arms are guarded by `!(flags & RADIUS_NOSTORE)`, so
   `STORE`/`STOREDIST` fall through to the generic syntax-error arm ⇒ `-ERR syntax error`.
   FrogDB performs the write instead (§Problem 3). **This is the parity break.** Verify with
   `redis-cli georadius_ro k 0 0 1 km store d` against a stock 8.x server before landing the
   error text.

## Proposed change

All of it lives inside `commands/src/geo.rs`. No public API, no crate boundary, no spec
field, no wire format.

### A. `store_geo_results` — the STORE effect, once

```rust
/// The GEO STORE effect (geo.c `georadiusGeneric`'s storekey tail), stated once for
/// GEOSEARCHSTORE / GEORADIUS STORE / GEORADIUSBYMEMBER STORE.
///
/// Empty results clear the destination: `del` (NOTIFY_GENERIC) fires only when a key was
/// actually removed, and a clear that removed nothing declares the write a no-op so the
/// effect pipeline — WATCH bump, propagation, dirty — is skipped (Redis: no dbDelete, no
/// signalModifiedKey, no server.dirty++). Non-empty results replace the destination
/// wholesale (dropping its TTL, matching `setKey(..., 0)`), fire `event` under NOTIFY_ZSET,
/// and deposit `dirty_delta = results.len()` (Redis: `server.dirty += returned_items`).
/// The reply is the *result count*, not the destination's cardinality — Redis replies
/// `returned_items` (§Redis compatibility note 1).
///
/// Two callers reach this from a state Redis cannot: GEORADIUSBYMEMBER with a missing
/// source key, and GEOSEARCHSTORE FROMMEMBER with a missing source key. Redis resolves the
/// member first and errors (`-ERR could not decode requested zset member`), so it never
/// reaches this tail. Those two paths are **FrogDB-only behaviour, tracked separately**
/// (proposal 96 §Problem 1) — do not read the geo.c citation above as parity for them.
fn store_geo_results(
    ctx: &mut CommandContext,
    dest: &Bytes,
    results: &[GeoSearchResult],
    store_dist: bool,
    event: &'static str,
) -> Response
```

Eight call sites, each one line:

- `:421-426` → `return Ok(store_geo_results(ctx, destkey, &[], opts_store_dist, GEOSEARCHSTORE_EVENT));`
- `:432-437`, `:534-542`, `:669-674`, `:699-707` → same shape with the site's dest/event
- `:439-456`, `:543-557`, `:708-722` → `store_geo_results(ctx, dest, &results, opts.store_dist, EVENT)`

The `&'static str` event stays a plain parameter — it is exactly what `ctx.notify_event`
takes (`command.rs:1427`), and two module constants (`GEOSEARCHSTORE_EVENT`,
`GEORADIUSSTORE_EVENT`) keep the two literals from being retyped.

### B. `build_dest_zset` — pure, and where the invariant finally gets written down

```rust
/// Build the destination zset from search results. Duplicate members collapse (a sorted
/// set holds each member once) while the *reply* counts results — see `store_geo_results`.
/// `execute_geosearch` cannot currently produce a duplicate (disjoint per-area score
/// ranges + the contiguous-repeat dedupe at the area loop), and the fuzz target pins the
/// area-sequence half of that argument; this function is the place that survives if it
/// ever stops holding.
fn build_dest_zset(results: &[GeoSearchResult], store_dist: bool) -> SortedSetValue
```

Nine duplicated lines (`:440-448` ≡ `:543-551` ≡ `:708-716`) become one function with **no
`ctx`**, so it is unit-testable with zero harness — including the duplicate-member case,
which is how the latent divergence stops being latent-and-unstated and becomes
pinned-and-documented.

### C. One grammar, and an `allow_store` flag

```rust
/// Consume one option token shared by both GEO grammars (WITHCOORD, WITHDIST, WITHHASH,
/// COUNT [ANY], bare ANY → error, ASC, DESC). Returns false if the token belongs to the
/// caller's own grammar.
fn parse_shared_geo_flag(
    parser: &mut ArgParser,
    out: &mut GeoResultOptions,
) -> Result<bool, CommandError>
```

Both loops call it first and keep only their own arms — modern: `FROMMEMBER`/`FROMLONLAT`/
`BYRADIUS`/`BYBOX`/bare `STOREDIST`; legacy: `STORE <key>`/`STOREDIST <key>`. And the legacy
parser gains the gate its sibling already has:

```rust
fn parse_georadius_options(args: &[Bytes], allow_store: bool) -> Result<GeoRadiusOptions, CommandError> {
    …
    } else if parser.try_flag(b"STORE") {
        if !allow_store { return Err(CommandError::SyntaxError); }
        store = Some(parser.next_arg()?.clone());
    } else if parser.try_flag(b"STOREDIST") {
        if !allow_store { return Err(CommandError::SyntaxError); }
        …
```

### D. `GeoSearchOptions` grows a constructor; the two 15-line literals collapse

Split the 13 fields into the two things they actually are — `GeoShape { center, radius_m,
width_m, height_m, unit }` (what to search) and `GeoResultOptions { with_coord, with_dist,
with_hash, count, any, asc, desc, store_dist }` (what to return) — and keep
`GeoSearchOptions { shape, result }` so `execute_geosearch` `:1121` and
`format_geosearch_results` `:1252` keep their signatures (field accesses become
`opts.shape.radius_m` / `opts.result.with_dist`; ~20 mechanical renames inside two
functions). `:514-528` and `:679-693` become:

```rust
let opts = GeoSearchOptions::radius(coords, radius, unit, radius_opts.result);
```

Alternative considered and rejected: keep the flat struct and add
`GeoSearchOptions::from_legacy(center, radius, unit, &GeoRadiusOptions)`. It removes the
same 28 lines and touches nothing else — **strictly smaller, and a valid de-scope if a
reviewer wants the minimum**. It is rejected as the default only because it leaves the
"search shape" and "result shape" concepts fused, which is what made the legacy commands
need a hand-written adapter in the first place.

### E. The `_RO` commands become honest adapters

```rust
fn georadius_exec(ctx: &mut CommandContext, args: &[Bytes], allow_store: bool) -> Result<Response, CommandError>
```

`GeoradiusCommand::execute` → `georadius_exec(ctx, args, true)`; `GeoradiusRoCommand::execute`
→ `georadius_exec(ctx, args, false)`. Same for the BYMEMBER pair. The `_RO` specs are already
correct and stay untouched — with `STORE` rejected, `KeySpec::First` and `WalStrategy::NoOp`
become true statements instead of lies.

## Deletion test

For each duplicated construct: delete it, route the caller through the shared one — does any
existing test fail?

| Construct | Deleting it breaks… | Verdict |
|---|---|---|
| `dest_zset.len()` at `:552` / `:717` → `results.len()` | nothing. The suite's only STORE-reply assertions are the four `assert_integer_eq(…, 0)` at `geo_tcl.rs:188`, `:220`, `:263`, `:293`, all on the **empty** branch (sites 1/2/4); the six STORE ports assert destination contents (`ZRANGE`/`ZCARD`), never the reply; `geo_regression.rs` has no STORE coverage at all | **Invisible to CI.** Ships with a new pin. |
| The 9-line zset build ×3 (`:440-448`, `:543-551`, `:708-716`) | nothing — behaviour identical by construction | **Pure duplication.** Delete 18 lines. |
| The clear-and-notify block ×5 | more than the row used to claim: the four `Integer(0)` assertions above pin sites 1, 2 and 4's *reply* through a relocation, and `integration_pubsub.rs:3367`, `:3409`, `:3624` pin their notify behaviour | **Pure duplication**, with a real (if partial) net under it; one behaviour change intended (`write_was_noop`, §Problem 4a) which arrives with its own test. Sites 6 and 7 remain net-free — added tests cover them. |
| The 15-line struct literal ×2 (`:514-528` ≡ `:679-693`) | nothing; byte-identical | **Pure duplication.** Delete ~28 lines. |
| The 7 shared parser arms in `parse_georadius_options` | nothing; one-line diff from the modern parser | **Pure duplication.** Delete ~20 lines. |
| `#[allow(dead_code)]` `:832` | nothing — every field has a read site | **Dead annotation.** Delete 1 line. |
| `georadius_store_dest` `:1095-1112` | **all six** `keys()` unit tests, not four: `georadius_keys_*` ×4 (`:1304`, `:1312`, `:1320`, `:1339`) **and** `georadiusbymember_keys_*` ×2 (`:1358`, `:1366`) — `GeoradiusbymemberCommand::dynamic_keys` calls the same helper at `:735`, as GEORADIUS does at `:570` | **Not duplication — keep.** It must run before `execute`, from `dynamic_keys`, and its safety net is larger than first credited. |

Net: ≈ −150 lines deleted, ≈ +60 added (helper + constructor + docs) ⇒ `geo.rs` ≈ 1372 →
≈ 1285, with the STORE effect stated **once** and the count expression existing **once**.

## Testability improvement

Today the STORE family has: zero unit tests (the six tests at `:1303-1371` all test `keys()`),
reply-integer coverage for the `Integer(0)` branch only (four assertions, `geo_tcl.rs:188`,
`:220`, `:263`, `:293` — sites 1, 2, 4; **nothing on the count expression that drifted**),
notify coverage for 2 of 3 commands, and one behaviour (`_RO` + `STORE`) that no test
exercises in any form.

After:

1. **Unit, no harness** — `build_dest_zset` is pure. Table-driven cases: `STOREDIST` scores
   are distances / geohash scores; **a duplicate member in `results` collapses to one entry
   while the reply stays `results.len()`** (the Redis rule, asserted directly instead of
   inferred).
2. **Unit, no harness** — `parse_shared_geo_flag` + both parsers: the seven shared arms get
   one table each instead of two hand-copied loops; `allow_store = false` ⇒ `SyntaxError` for
   both `STORE` and `STOREDIST`.
3. **Regression, red-green, the hotfix** — in `geo_regression.rs`:

   ```rust
   #[tokio::test]
   async fn georadius_ro_rejects_store() {
       let server = TestServer::start_standalone().await;
       let mut client = server.connect().await;
       client.command(&["GEOADD", "{g}src", "13.361389", "38.115556", "Palermo"]).await;

       // Redis: -ERR syntax error (RADIUS_NOSTORE guards the STORE arm).
       assert_error_prefix(
           &client.command(&["GEORADIUS_RO", "{g}src", "13.361389", "38.115556",
                             "200", "km", "STORE", "{g}dest"]).await,
           "ERR",
       );
       // …and, whatever the reply, the read-only command must not have written.
       assert_eq!(unwrap_integer(client.command(&["EXISTS", "{g}dest"]).await), 0);
   }
   ```

   At HEAD both assertions fail: the reply is `:1` and `EXISTS {g}dest` is `1`. The second
   assertion is the load-bearing one — it fails even if a future reply shape changes. Same
   test for `GEORADIUSBYMEMBER_RO … STORE`.
3b. **Regression, red-green, the destructive half** — `GEOADD {g}victim …` then
   `GEORADIUS_RO {g}src 0 0 1 m STORE {g}victim` (matching nothing), then assert
   `EXISTS {g}victim` is still `1`. At HEAD it is `0`: the read-only command destroyed the
   key. This is the test the write-side one does **not** subsume, and it is the one that
   would have caught §Problem 3's sharpest edge.
4. **Regression, the count** — add `assert_integer_eq` on the STORE reply to the six existing
   STORE ports (`geo_tcl.rs:1898`, `:1935`, `:2001`, `:2039`, `:2081`, `:2125`), which today
   assert only destination contents. Both helpers exist in the harness —
   `test-harness/src/response.rs:56` (`unwrap_integer`) and `:72` (`assert_integer_eq`) —
   and `geo_tcl.rs` already uses the latter, so match it. This is the pin whose absence let
   the drift live five weeks.
5. **Integration, notify parity** — `test_georadiusbymember_store_notifies_georadiusstore`,
   mirroring `:3586`; `test_georadiusbymember_store_empty_result_emits_del`, mirroring
   `:3624`; and the silent-missing-dest case for **both** legacy commands (mirroring `:3409`,
   which exists only for GEOSEARCHSTORE). Three behaviours × three commands = 9 cells; 5 are
   filled today (GEOSEARCHSTORE 3, GEORADIUS 2), and one helper makes the other 4 worth
   writing.
6. **Integration, §Problem 4** — `WATCH dest` + no-op `GEOSEARCHSTORE` + `EXEC` succeeds
   (4a), and `INFO persistence`'s `rdb_changes_since_last_save` advances by the stored count,
   not by 1, after a multi-member `GEOSEARCHSTORE` (4b).
7. **Fuzz, the invariant behind the LATENT ruling** — one assertion in
   `testing/fuzz/fuzz_targets/geo_ops.rs:104-113`, beside the two that are already there:

   ```rust
   // Repeats in the 9-area sequence must be contiguous: the search loop dedupes only
   // against the previously processed area, so a non-contiguous repeat would rescan a
   // cell and yield duplicate members (proposal 96 §Problem 2).
   for w in areas.windows(2) { /* … */ }
   let mut seen: Vec<(u64, u8)> = Vec::new(); // first-seen order, contiguity check
   ```

   This is the cheapest possible durable statement of the property the whole reply-count
   analysis rests on, in a target that already fuzzes this exact function.

## Locked-area clearance and gate check

- **Locked crates:** none edited. `frogdb-commands`, `redis-regression` and `testing/fuzz`
  are outside all four boundaries (txn / persistence / replication / cluster; ADRs
  `adr/0002`–`0004`). No `.scratch/hardening/specs/*-failure-modes.md` row mentions `geo`,
  `GEORADIUS`, `GEOSEARCH` or `store_geo` (verified by grep), and there are **zero `FM-`
  tags in `frogdb-server/crates/commands/src`**, so `just lint-failure-modes` is unaffected
  and no mutation gate applies to the edited crates.
- **`lint-no-typed-unwrap`** (`Justfile:1012-1039`) is the **only gate targeting
  `crates/commands/src` exclusively** (`:1017`, `:1027`). It bans `as_*_mut().unwrap()` /
  `get_mut(..).unwrap()` and hand-rolled `.ok_or(…WrongType)`. `geo.rs` has neither today,
  and nothing proposed introduces either — the helper takes `&mut CommandContext` and calls
  `ctx.store.set` / `ctx.store.delete`, the same calls that are there now. Verified clean by
  grep at HEAD. Three further gates *do* sweep this file, as part of sweeping the whole
  `frogdb-server/crates` tree — `lint-redirect-seam` (`:452`), `lint-metrics-chokepoint`
  (`:1201`), `lint-format-float` (`:1252`), each of which sets
  `crates="{{server-dir}}/crates"` — and **none is threatened**: this proposal adds no
  `Response::error("CROSSSLOT`, no raw metric emission, and no `fn format_float`.
- **`lint-format-float`** (`:1249-1269`) forbids *defining* `fn format_float` outside
  `protocol/src/format.rs`. `geo.rs` imports it (`:23`) and separately defines
  `fn format_distance` (`:27-29`, `%.4f`, Redis `addReplyDoubleDistance`) — a different
  name, not matched by the gate, and **not touched by this proposal**.
- **`lint-keyspace-notify-routing`** (`:1051-1067`) scopes `core/src/shard`; geo notifies via
  `ctx.notify_event`, which is the sanctioned deposit. **`lint-clock-seam`**: `geo.rs` reads
  no clock. The remaining eleven gates scope cluster/config/protocol/scripting paths.
- **Feature note for the implementer:** `geo` is its own cargo feature
  (`commands/Cargo.toml:26`) and is **not** part of `core-profile` (`:18`, an empty marker
  feature; `default = ["core-profile"]` at `:15`). Unit tests added to `geo.rs` run only under
  `--features geo` (or `full`); `redis-regression` already builds `full`. Per CLAUDE.md, do
  not alternate feature sets inside the iteration loop — pick `--features geo` for the
  crate-local loop and run the regression crate once at the end.

## Risks / scope boundaries vs siblings

| Sibling | Footprint | Edge |
|---|---|---|
| **90 — `CommandSpec::DEFAULT`** (heading `:507`; ordering ruling `:522-528`: land **solo and last** in `frogdb-commands`) | Rewrites the interior of all 296 spec statics, including the **ten** in `geo.rs` (`:48-71`, `:172-186`, `:234-248`, `:292-306`, `:347-361`, `:385-406`, `:468-487`, `:606-625`, `:771-785`, `:803-817`) — a count 90's own per-feature table confirms (`90:426`, "`geo` \| 10") | **Same-file conflict, disjoint hunks.** 96 edits `execute` bodies, helpers and the parser — **zero lines inside any `static SPEC` block** (the `_RO` fix touches `:789-792`/`:821-824`, not `:771-785`/`:803-817`). Ordering: 90 does **not** name 96 — its sibling table predates this proposal and lists only 67/70/80/89 (`90:513-520`). The applicable ruling is therefore 90's *general* principle at `:522-528` ("a mechanical sweep should be the round's last commands-crate writer", because 90 is regenerable and its siblings are not), which by inference puts **96 first, 90 re-deriving its awk sweep from the merged tree**. **Ask 90 to add a 96 row on its next revision** so the inference becomes an explicit ruling. 96 does not change the count of spec statics. |
| **94 — RESP3 shape once** | RESP3 reply shaping | **No edge. `geo.rs` contains no `is_resp3` and no `protocol_version` branch** (verified by grep at HEAD) — every geo reply is built from `Response::Array`/`bulk`/`Integer` and shaped downstream. If 94 later re-shapes GEOPOS/GEOSEARCH replies, it does so in the protocol crate, not here. |
| **91 — CommandContext narrowing** | 7 `#[cfg(test)]` `Box::leak` ctx helpers in `commands/src` (`basic.rs:906`, `bloom`, `cuckoo`, `generic`, `hash`, `sort`, `string`) | **No edge — `geo.rs` has no such helper**, and 91 explicitly changes no handler-code lines in this crate. Consequence for 96: a unit test needing a real `CommandContext` would have to add the 8th such helper into territory 91 is retiring, which is precisely why §Testability puts the new unit tests on **`build_dest_zset` (pure, no ctx)** and the effect behaviour in the regression/integration suites. |
| **89 — probabilistic chunk codec** | `commands/src/bloom.rs`, `cuckoo.rs` | No overlap. |
| **95 — BF.INFO field table**, **92**, **93** | bloom / expiry / hash-field-expiry | No overlap (no `geo` or `GEO` mention in any of them; verified by grep). |

Other risks:

- **Behaviour changes are proposed inside a "pure refactor".** The `write_was_noop` fix
  (§Problem 4a) changes when a WATCH aborts; the `dirty_delta = results.len()` fix
  (§Problem 4b) changes `rdb_changes_since_last_save` and therefore when a change-triggered
  save fires. Both are called out separately, carry their own tests, and can be dropped from
  the refactor commit and filed as follow-ups if a reviewer wants the unification to be
  strictly behaviour-preserving. The **reply-count unification is behaviour-preserving**
  (§Problem 2 proves the values coincide) and the **`_RO` fix is deliberately
  behaviour-changing** (that is the point).
- **The `_RO` fix is client-breaking for anyone relying on the bug.** They are relying on a
  write that is lost on restart and invisible to replicas — or on a **delete** that is lost
  on restart and invisible to replicas; the "compatibility" being broken is with a defect,
  and FrogDB is pre-production.
- **96 leaves a known hole open.** The read-only-script path still admits `GEORADIUS … STORE`
  (§Problem 3b). Closing the `_RO`-command half without the script half is still a strict
  improvement, but the residual must be **filed, not forgotten** — otherwise the next reader
  sees `allow_store` and concludes read-only geo writes are impossible.
- **`GeoSearchOptions` split ripples into two functions.** ~20 mechanical field-access
  renames in `execute_geosearch` (`:1121-1249`) and `format_geosearch_results`
  (`:1252-1293`). Contained, compiler-verified; and option D names a strictly smaller
  fallback that avoids the ripple entirely.
- **`georadius_store_dest` `:1095-1112` stays a second grammar walk.** It runs from
  `dynamic_keys` (`:563-575`, `:728-740`) *before* `execute`, so it cannot reuse the parser's
  result. Its divergence risk is real but pre-existing, is pinned by **six** tests
  (`:1304`, `:1312`, `:1320`, `:1339`, `:1358`, `:1366`), and folding it in would mean
  restructuring key extraction — out of scope, and squarely inside 90's and 91's territory.
- **Unverified-at-runtime claims are flagged as such.** The `-ERR syntax error` text for the
  `_RO` rejection, the `geo.c` reply semantics, the `serverAssert(dictAdd(...) == DICT_OK)`
  in the store loop, and the `longLatFromMember` ordering behind the two Redis-unreachable
  sites are all from code knowledge of `geo.c` — **there is no Redis checkout in this
  repository to grep**. Each is cheap to confirm against a stock 8.x server, and none changes
  the shape of the proposal if the wording differs. Every FrogDB-side `file:line` in this
  document was executed against the tree.

## Effort

**M.** One production file. Suggested commit order, each independently green:

1. **Hotfix (S, independently landable, ship first):** `allow_store` on
   `parse_georadius_options` + `georadius_exec` + the two `_RO` adapters + **three** red-green
   regression tests (rejection, no-write, **no-delete**). ~15 production lines, ~55 test
   lines. Fixes a `READONLY` command that performs an unreplicated, unpersisted, unrouted
   **write or key deletion**, and that a replica accepts.
2. `store_geo_results` + `build_dest_zset`; eight sites become calls; reply unified on
   `results.len()`; `#[allow(dead_code)]` and the wrong comment at `:418-419` deleted; unit
   tests + the six reply-count pins + the GEORADIUSBYMEMBER notify tests.
3. `parse_shared_geo_flag` + the `GeoShape`/`GeoResultOptions` split; the two 15-line
   literals collapse; parser unit tables.
4. `write_was_noop` on the no-op clear (§4a) + `dirty_delta = results.len()` on the store
   branch (§4b) + their WATCH / `rdb_changes_since_last_save` tests + the fuzz contiguity
   assertion.

Commit 1 stands alone and should not wait for the rest. Commits 2–4 want to land **before**
90's sweep. **Separately, file the read-only-script hole (§Problem 3b)** — it is not part of
any commit here.
