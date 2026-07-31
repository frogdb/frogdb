# frogdb-commands, core Redis data types — testing gap audit (round 2)

## Scope

`frogdb-server/crates/commands/src/`: `basic.rs`, `string.rs`, `hash.rs`, `list.rs`, `set.rs`,
`sorted_set/{basic,count,pop,range,rank,scan,set_ops,store_remove}.rs`, `generic.rs`, `expiry.rs`,
`scan.rs`, `sort.rs`, `blocking.rs`, `utils.rs`, `lib.rs`. Out of scope (sibling agent): the
probabilistic/JSON/stream/geo/timeseries/vectorset/bitmap modules.

- **Source LOC**: 15,424 across 20 files.
- **Instrumented lines**: 7,281; **covered 89.1%**; regions 89.3%. (Whole `commands` crate is
  13,712 lines / 85.0%.) Per the brief, percentage is not the signal here — 89% is near the top of
  the workspace and the area still contains client-triggerable panics and silent data loss.
- **Depth classes** (730 unique functions, after merging the duplicate per-monomorphisation rows in
  `depth.json` — the raw array double-counts codegen instances and overstates `untested` ~10×):

  | class | count |
  |---|---|
  | well-covered (≥5 tests) | 422 |
  | single-test | 156 |
  | covered | 54 |
  | untested | 54 |
  | monoculture | 44 |

- **Largest untested functions**: `scan.rs:67 ScanCommand::execute` (63 regions, 0 covered),
  `hash.rs:1168 execute_httl_common` (48), `utils.rs:150 hash_cursor_scan::<Bytes,Empty<Bytes>>`
  (41), `basic.rs:839 flags_match_acl_category` (27), `utils.rs:774 LimitOptions::parse` (19),
  `scan.rs:134 KeysCommand::execute` (15), `scan.rs:151 parse_key_type` (15).
- **Notable single-test/monoculture**: `expiry.rs:427 ExpireatCommand::execute` (77 regions, one
  test), `expiry.rs:511 PexpireatCommand::execute` (77 regions, 3 tests, one suite),
  `sorted_set/range.rs:264 Zrevrangebyscore` (52, one test), `:374 Zrevrangebylex` (45, one test),
  `:321 Zrangebylex` (45, monoculture), `utils.rs:96 simple_glob_match` (34, monoculture —
  `scan_tcl` only), `store_remove.rs:260 Zremrangebylex` (31, one test).
- **In-crate tests**: 8 inline `#[cfg(test)]` modules, ~99 tests. `set.rs` (1127 LOC),
  `expiry.rs` (790), `blocking.rs` (905) and all 8 `sorted_set/*` files (2049) have **zero**
  in-crate tests — 4,871 LOC whose only coverage arrives from `redis-regression` over a socket.

## Summary

Coverage in this area is broad but almost entirely *end-to-end and prefix-asserted*: ~95% of it
comes from `redis-regression` driving a real socket, where essentially every negative-path
assertion is `assert_error_prefix(.., "ERR")`. That shape proves "Redis parity on the happy path"
well and proves almost nothing about **validation ordering, argument-range arithmetic, and
effect-pipeline side effects** — which is exactly where the bugs are. Three distinct bug families
escape today: (1) **client-triggerable panics** from unguarded integer/`Instant`/`Duration`
arithmetic on argument values (`EXPIREAT k i64::MAX` panics the shard worker); (2) **destructive
validation ordering**, where a command deletes a key or pops an element *before* validating the
rest of its arguments (`ZRANGESTORE`, `ZINTERSTORE`, `BLMOVE`, the `EXPIRE … GT` family);
(3) **expiry-blindness**, where a command reaches past the logical-expiry check into the raw store
and resurrects a dead key or a dead hash field (`PERSIST` is the worst — it makes an expired key
permanently immortal and diverges primary from replica). All three are cheap to test and none are
reachable by adding another parity test. Separately, the `intentional-incompatibility:encoding`
exclusion bucket dropped **86 upstream test bodies in this area alone** on the basis of a
`- $encoding` name suffix, and spot-checking shows several of them tested encoding-independent
behaviour we genuinely do not cover.

**On the crate shape**: the crate having no `tests/` directory is *correct*, but not for the reason
it currently gets away with. A `tests/` dir here would need a dev-dep on `frogdb-core`, which
compiles core twice and produces E0308 (documented in `core/tests/shard_driver/harness.rs`). The
right home for command-semantics tests is **boundary 3, `core/tests/shard_driver/`** — it gives
real dispatch, real `ShardWorker`, real store, real WAL/effects/notification seams, and real
multi-shard routing via `ShardDriver::new(n)`, with no socket. Inline units should be kept for pure
parsers/formatters only. Today the inline units all use `Box::leak(HashMapStore::new())` with
`num_shards = 1` hardcoded (`sort.rs:553+`), which structurally hides every cross-shard and every
effect-pipeline bug in this document. See the OPTIONS block on F0.

## Existing test inventory

| surface | covers | strengths | blind spots |
|---|---|---|---|
| `redis-regression` (`zset_tcl` 3167 LOC, `list_tcl` 3071, `string_tcl` 2333, `hash_field_expire_tcl` 1718, `hash_tcl` 1501, `set_tcl` 1099, `scan_tcl` 924, `expire_tcl` 908, `sort_tcl` 880 + `*_regression` siblings) | ~95% of this area's line coverage; genuine Redis parity on happy paths and common errors | real ported upstream assertions; broad command matrix | error assertions are `assert_error_prefix(.., "ERR")` — message text and *which* error fires are unpinned; keys are hash-tagged (`{t}`) so cross-shard behaviour is invisible; 86 excluded `- $encoding`/`- $type` bodies; cannot express FrogDB-only semantics or effect-pipeline state |
| inline units — `sort.rs:553` (22), `string.rs:1561` (20), `hash.rs:2090` (19), `utils.rs:980` (18), `basic.rs:889` (12), `generic.rs:650` (3), `list.rs:1098` (3), `scan.rs:165` (2) | expire-grammar wire pins, SORT behaviour, scan-request parse/reply, COPY/SET no-ops | `utils.rs scan_seam_tests` and `sort.rs` are good boundary-1/2 work | `hash.rs`'s 19 tests are *only* expire-message pins — zero hash data behaviour; `list.rs`'s 3 assert `spec()` metadata only; all use `Box::leak` + `num_shards=1`; no effects/notification/WAL visibility |
| `core/tests/shard_driver/` (s1–s8 + `driver_tests`) | blocking waiter races, WATCH+waiter, XREAD waiters, keyspace-notify ordering | exactly the right boundary; `execute`, `execute_conn`, `exec_transaction`, `watch_keys`, `block_wait`, `tick_expiry`, `tick_waiter_timeout`, `capture_keyspace`, `memory_check`, `expiry_index_check` all exist | **no scenario drives a blocking command through `commands/src/blocking.rs::execute()`** — `block_wait` bypasses arg parsing; no scenario file for plain data-type semantics; `execute` hardcodes RESP3 |
| `server/tests/integration_{strings,hashes,lists,sets,sorted_sets,…}.rs`, `proptest_commands.rs` | RESP-level shapes; parser fuzzing | proptest exists for *parsers* | `proptest_commands.rs` fuzzes parsing, never execution — no "no command panics on adversarial args" property anywhere |

**Round 1 residue check**: issue 01 (SPOP replication rewrite) is fixed and proven end-to-end
(`set.rs:977 rewrite_propagation` ← `integration_replication.rs:4298-4432`); issue 02, 07, 31, 52
done. Issue 24 covered the *keyspace* SCAN cursor in `core/src/store/hashmap.rs` only — the
collection cursor in `utils.rs hash_cursor_scan` (HSCAN/SSCAN/ZSCAN) is untouched residue (F10).

---

## Findings

### F0: Where command-semantics tests should live (structural — not scored)

Not a bug; the decision that gates every finding below. The crate cannot host a `tests/` dir
(dev-dep cycle → core compiled twice → E0308, per `core/tests/shard_driver/harness.rs`). The
choice is between extending the inline `Box::leak(HashMapStore)` pattern and adding a
`scenario_commands_*.rs` family under `core/tests/shard_driver/`.

- **OPTIONS**:
  - **(a) inline `#[cfg(test)]` with a leaked `HashMapStore`** (today's pattern, `sort.rs:553`).
    Cheapest, fastest, no new files. But `CommandContext::new(store, senders, 0, /*num_shards*/ 1,
    …)` is hardcoded to one shard, and there is no effects pipeline, no WAL, no keyspace
    notifications, no waiter wake — so it structurally cannot express F1, F4, F6, F7, F8, F12.
  - **(b) `core/tests/shard_driver/scenario_commands_{expiry,zset,list,string,hash,blocking}.rs`**
    (boundary 3). Real registry, real `ShardWorker`, real store + effects + WAL + notify seams,
    real N-shard routing. Costs one `tick_expiry`/`drain` call per scenario and a small harness
    addition (RESP2 selection). Covers every finding here except the pure parsers.
  - **(c) keep pushing everything into `redis-regression`** (boundary 4). Best parity fidelity,
    but slow, prefix-only in its error assertions, and cannot express cross-shard or effect state
    at all.
  - **Recommendation: (b) as the default home**, with (a) retained *only* for pure functions
    (`utils.rs` parsers, `format_float`, `simple_glob_match`, cursor codec). Do not add new
    inline tests that assert store state.

---

### F1: `PERSIST` / `RENAME` / `RENAMENX` / `TYPE` / `EXPIRETIME` read past the logical-expiry check and resurrect dead keys

- **Severity** 5 — `PERSIST` on a key whose deadline has passed but which the sampler has not yet
  swept clears `expires_at` **and removes it from the expiry index**, making the key permanently
  immortal. That is silent, unbounded data resurrection and it diverges primary from replica (the
  replica expires independently), i.e. a consistency violation.
- **Likelihood** 4 — default config. Active expiry in Redis-style engines is sampled, so a
  post-deadline pre-sweep window always exists; `PERSIST` after a `SET … EX` is an ordinary
  cache-pinning pattern.
- **Effort** 2 — `shard_driver` scenario: set with a short TTL, advance past it without ticking,
  `PERSIST`, `TTL`, `EXISTS`, then `tick_expiry` and re-assert.
- **Priority** 21
- **Evidence**: `commands/src/expiry.rs:697-700` — `PersistCommand::execute` is three lines,
  `ctx.store.persist(key)` with no expiry check. `core/src/store/hashmap.rs:1239-1247` — `persist`
  matches only on `expires_at.is_some()`, sets it to `None` and calls `self.expiry_index.remove`.
  Sibling `touch` at `:1249` is commented "Check and delete if expired first", proving the guard is
  the house style and its absence here is an oversight. Same class: `generic.rs:95` (`RENAME` uses
  `store.get`), `:182` (`RENAMENX` uses `store.contains`), `:48` (`TYPE` uses `store.key_type`) —
  while `generic.rs:293` (`UNLINK`) correctly uses `get_with_expiry_check` *with a comment saying
  why*. `expiry.rs:738-744` / `:782-788` (`EXPIRETIME`/`PEXPIRETIME`) lack the already-expired `-2`
  guard that `TTL`/`PTTL` have at `:603` / `:656`.
- **Proposed test**: for each of PERSIST/RENAME/RENAMENX/TYPE/EXISTS/EXPIRETIME, set a key with a
  1ms TTL, sleep past it, issue the command *without* `tick_expiry`, assert the command observes
  the key as gone (`0`/`none`/`-2`/error), then `tick_expiry` and assert `EXISTS == 0`. Assert
  `expiry_index_check()` reports no orphan after `PERSIST` on an expired key.
- **Boundary** 3 — needs the real store's expiry index and the shard's expiry tick; a leaked
  `HashMapStore` has neither.

### F2: `EXPIRE`/`PEXPIRE`/`EXPIREAT`/`PEXPIREAT` apply the past-deadline delete *before* evaluating `GT`/`LT`

- **Severity** 5 — `EXPIRE k -10 GT` deletes the key. Redis returns 0 and leaves the key and its
  TTL untouched (a past TTL is never "greater than" the current one). Silent data loss on a
  default-config command, and it replicates as a `DEL`.
- **Likelihood** 3 — the pattern that hits it is clamping a TTL derived from an external
  timestamp with `GT` as a safety net; the `GT` is *there precisely so the write is safe*, and it
  is the thing that fails.
- **Effort** 1 — four `shard_driver` `execute` calls.
- **Priority** 20
- **Evidence**: `commands/src/expiry.rs:449-463` (`EXPIREAT`) — `if timestamp < 0 { delete; return }`
  then `unix_secs_to_instant(...)` then `if expires_at <= Instant::now() { delete; return }` at
  `:456-459`, and only *then* the GT/LT comparison at `:462+`. Identical ordering at `:282`/`:291`
  (EXPIRE), `:371`/`:379` (PEXPIRE), `:533`/`:547` (PEXPIREAT). `ExpireatCommand::execute` is
  `single-test` (77 regions, reached only by
  `main::expire_tcl::tcl_expireat_check_for_expire_alike_behavior`); `PexpireatCommand::execute`
  is `monoculture` (77 regions, 3 tests, `expire_tcl` only).
- **Proposed test**: `SET k v EX 100`; `EXPIRE k -10 GT` → assert reply `0`, `EXISTS k == 1`,
  `TTL k ≈ 100`. Repeat with `LT` (must delete, reply 1), with `XX`/`NX`, and for all four
  commands. Also assert `EXPIREAT k <past> GT` on a key with no TTL.
- **Boundary** 3 — asserts store state and key survival after the effect pipeline runs.

### F3: Hash-field expiry — seven commands skip the field purge, and "last field expires ⇒ key is deleted" is asserted nowhere

- **Severity** 4 — logically-expired fields leak into results and into writes: `HSETNX` refuses to
  set over a dead field, `HDEL` reports a deletion of something already gone, `HINCRBY` increments
  from a stale value. A hash whose last field has expired but which is never purged stays visible
  to `EXISTS`/`DBSIZE`/`SCAN`, gets written to RDB, and is replicated.
- **Likelihood** 3 — needs a field past its deadline on one of the seven non-purging paths; the
  read paths purge, so it is the write paths that leak.
- **Effort** 2 — `shard_driver` + `tick_expiry`.
- **Priority** 16
- **Evidence**: `commands/src/hash.rs:124-136` (HSETNX), `:215-244` (HDEL), `:609-617` (HINCRBY),
  `:651-661` (HINCRBYFLOAT), `:987-1165` (`execute_hexpire_common`), `:1556-1615` (HPERSIST),
  `:1961-2088` (HSETEX) all operate without calling `purge_expired_hash_fields`, while
  `types/src/types/hash.rs:310-315` (`get`) and `:341-346` (`contains`) are expiry-blind by
  contract (`core/src/store/typed.rs:162-164`). `hash.rs:1168 execute_httl_common` is **untested**
  (48 regions, 0 covered) in one instantiation and `monoculture` (2 tests,
  `hash_field_expire_tcl` only) in the other. `hash_field_expire_tcl.rs:529` asserts `HEXISTS`
  after the last field expires but never `EXISTS`.
- **Proposed test**: `HSET h f v`, `HPEXPIRE h 1 FIELDS 1 f`, sleep, then for each of the seven
  commands assert it treats `f` as absent; separately assert that after the last field expires
  `EXISTS h == 0`, a `del` keyspace notification fired (`capture_keyspace`), and `memory_check()`
  reports no leaked hash.
- **Boundary** 3 — requires the real HFE index and the shard expiry tick.
- **Cross-area**: the expiry-blind `HashValue::get`/`contains` accessors themselves belong to the
  types-crate agent; the fix may well be there rather than in seven call sites.

### F4: `SORT … BY`/`GET` pattern keys resolve only against the local shard

- **Severity** 3 — wrong ordering and nil `GET` columns, silently; with `STORE` the wrong result is
  written durably. Not detected by any existing test because every `sort_tcl` weight test either
  hash-tags all keys (`sort_tcl.rs:595 tcl_sort_by_external_key` uses `{t}`) or passes by hashing
  luck (`:622 tcl_sort_by_external_key_with_limit` uses untagged `tosort`/`weight_*`).
- **Likelihood** 5 — `SORT mylist BY weight_*` on a **default 4-shard standalone** is ordinary
  usage and returns wrong results.
- **Effort** 2 — `ShardDriver::new(4)` and pick keys that provably land on different shards.
- **Priority** 17
- **Evidence**: `commands/src/sort.rs:143 resolve_pattern` reads `ctx.store`, i.e. the local shard
  only; `compute_sort_key` (`:209-248`) silently substitutes `Numeric(0.0)` / `Alpha("")` for a
  key it cannot see, so the failure is a wrong answer rather than an error.
  `SortCommand::dynamic_keys` (`:485`) and `dynamic_keys_with_flags` (`:500`, **untested**, 19
  regions) declare only `args[0]` (R) and `sort_store_dest(args)` (OW) — never the BY/GET pattern
  keys, so neither the router nor ACL nor CROSSSLOT sees them. `utils.rs:964 require_same_shard` is
  applied by `sorted_set/set_ops.rs:170,340,560,663` and `sorted_set/pop.rs:198` but **not** by
  SORT, and the rejection path itself is **untested** (0 regions covered).
- **Proposed test**: with `ShardDriver::new(4)`, seed `tosort` on shard A and `weight_1..N` spread
  across all shards; assert `SORT tosort BY weight_*` returns the same order as the single-shard
  case, and that `SORT tosort BY weight_* GET pat_* STORE dst` writes identical content. Add the
  negative: assert whatever the chosen policy is (error vs. correct cross-shard fetch) is *pinned*.
- **Boundary** 3 — cross-shard routing is exactly what shard_driver's N-shard mode exists for; a
  server integration test would need non-tagged keys and could still pass by luck.
- **OPTIONS**:
  - **(a)** Fix it (declare BY/GET keys in `dynamic_keys_with_flags`, fetch cross-shard) and test
    at boundary 3 with `ShardDriver::new(4)`. Correct, matches Redis-standalone semantics.
  - **(b)** Apply `require_same_shard` to SORT, making cross-shard BY/GET an error, and test that
    it errors. Cheap and honest but breaks working single-shard-tested user code.
  - **(c)** Declare it a documented incompatibility and pin only the current behaviour.
  - **Recommendation: (a)**; (b) only if the cross-shard fetch is judged too invasive for now, and
    then it must be a documented incompatibility, not a silent wrong answer.

### F5: Unguarded integer/time arithmetic on argument values — client-triggerable panics

- **Severity** 4 — a panic on the single-writer shard task. Unauthenticated-reachable (post-AUTH),
  no special config, and it takes out every key on that shard's worker. This is a one-line DoS.
- **Likelihood** 3 — hostile or buggy clients, and anything that forwards user-supplied numbers
  into `EXPIREAT`/`LREM`/`SETRANGE`. Not "normal operation", but not contrived either.
- **Effort** 2 — one shard_driver scenario per site, plus a registry-wide proptest (see below).
- **Priority** 16
- **Evidence**:
  - `commands/src/expiry.rs:14-29 unix_secs_to_instant` — `Some(now_instant + duration)` with no
    `checked_add`; `expiry.rs:454` calls it with an unvalidated `parse_i64` result, so
    `EXPIREAT k 9223372036854775807` panics on `Instant` overflow. Every sibling guards this:
    `EXPIRE` hand-rolls a range check at `:248-263`, `SET`/`GETEX EXAT` route through
    `utils.rs:559 checked_expire_value`, `HEXPIREAT` caps at `HFE_MAX_ABS_TIME_MSEC`
    (`hash.rs:1006`). `ExpireatCommand::execute` is `single-test`.
  - `numkeys + 1` overflow → slice index panic: `blocking.rs:354`, `:358`, `:661`, `:665`
    (BLMPOP/BZMPOP); `sorted_set/set_ops.rs:161,247,331,439,552,654,746`; `sorted_set/pop.rs:190`.
    `types/src/args.rs:316 parse_usize` accepts `usize::MAX`, and key extraction
    (`core/src/command_spec.rs:101-117`) uses `.take(count)` so it does **not** reject first.
    (`list.rs:995-1043` LMPOP was checked and is safe.)
  - `blocking.rs:869-885 parse_timeout` has no upper bound → `BLPOP k 1e300` reaches
    `Duration::from_secs_f64` at `server/src/connection/blocking.rs:44` and panics.
  - `sorted_set/pop.rs:343` guards the negative-count `Vec::with_capacity` **only when
    `with_scores`** → `ZRANDMEMBER key -9223372036854775808` panics
    (`types/src/types/sorted_set.rs:874-875`).
  - `LREM key -9223372036854775808 v` → `(-count) as usize` at `types/src/types/list.rs:284`; same
    class in `normalize_index` (`:64`) and `resolve_range` (`:97-107`).
  - `SETRANGE k 18446744073709551615 v` → `offset + value.len()` unchecked at `string.rs:334,339`,
    wrapping past the 512MB guard into `current.resize(usize::MAX, 0)`
    (`types/src/types/string_value.rs:159`).
- **Proposed test**: (i) targeted shard_driver asserts — each of the above returns a
  `CommandError`, not a panic, and the shard is still serving afterwards (`d.execute(0,"PING",…)`
  succeeds); (ii) a **registry-wide proptest**: for every command in `CommandRegistry`, generate
  arg vectors from a corpus of adversarial scalars (`""`, `"0"`, `i64::MIN`, `i64::MAX`,
  `u64::MAX`, `"1e400"`, `"nan"`, `"-0"`, non-UTF8 bytes) at each arity position and assert
  "returns a `Response` or a `CommandError`, never unwinds". That single harness closes this class
  permanently and catches the next one.
- **Boundary** 3 for the property harness (needs the real registry + a real store to reach past
  the parsers), 1 for the individual arithmetic helpers.
- **Cross-area**: the actual overflow sites in `types/src/types/{list,sorted_set,string_value}.rs`
  and `server/src/connection/blocking.rs:44` belong to other agents; the *test* belongs here
  because the reachability is a command-argument property.

### F6: `*STORE` commands destroy the destination before validating the rest of their arguments

- **Severity** 4 — the destination key is deleted and `0` returned where Redis errors and leaves it
  intact. Silent destruction of a key the client never intended to touch.
- **Likelihood** 3 — needs a missing/wrong-typed source, which is exactly the case a retry loop or
  a typo'd key produces.
- **Effort** 2.
- **Priority** 16
- **Evidence**: `sorted_set/store_remove.rs:85-88` — `let Some(zset) = ctx.store.get_zset(src)?
  else { ctx.store.delete(&dest); return Ok(Response::Integer(0)) }` fires **before** any
  `parse_score_bound`/`parse_lex_bound`/`parse_i64` at `:91-119`, so
  `ZRANGESTORE dst missing garbage garbage BYSCORE` deletes `dst` and returns 0 instead of
  erroring. Same ordering: `set_ops.rs:457-463` (ZINTERSTORE) and `:764-770` (ZDIFFSTORE) delete
  `dest` before type-checking later sources — `ZINTERSTORE dst 2 missing somestring` destroys
  `dst` where Redis returns WRONGTYPE. ZUNIONSTORE is correctly ordered, proving the intent.
  Separately, the *legitimate* empty-result-deletes-destination contract has **no assertion
  anywhere** for `ZUNIONSTORE`/`ZINTERSTORE`/`ZDIFFSTORE` (`set_ops.rs:285-289`, `:506-510`,
  `:794-798`) or `SINTERSTORE`/`SUNIONSTORE`/`SDIFFSTORE` (`set.rs:542`, `:594/605/617`,
  `:669/688`).
- **Proposed test**: pre-populate `dst`; for each of the six STORE commands, issue the malformed /
  wrong-typed / missing-source variant and assert `EXISTS dst == 1` with the original content plus
  the correct error. Then the positive contract: an empty result must delete `dst` **and** emit a
  `del` keyspace notification (`capture_keyspace`).
- **Boundary** 3 — needs post-command store state and the notification stream; `redis-regression`
  can assert the first half but not the notification.

### F7: `BLMOVE`/`BRPOPLPUSH` immediate path pops and deletes the source before checking the destination type — element is lost

- **Severity** 4 — the popped element is destroyed with no rollback when the destination is the
  wrong type. Silent data loss on a normal reliable-queue pattern.
- **Likelihood** 3 — the wrong-type destination is the classic queue-migration mistake; the
  immediate (non-blocking) path is the common one since the source is usually non-empty.
- **Effort** 2.
- **Priority** 16
- **Evidence**: `commands/src/blocking.rs:243-265` (BLMOVE) and `:814-833` (BRPOPLPUSH) pop from
  the source and delete it when empty *before* touching the destination, with no undo. The
  non-blocking sibling gets this right: `list.rs:831` calls `check_list(dest)?` first. The
  **blocked** path is guarded by the `Undo` in `core/src/shard/blocking.rs:562-590` and *is* tested
  (`list_tcl.rs:1895`, `:1926`) — so the invariant is known and only the immediate path is
  unprotected. The immediate-path test at `list_tcl.rs:1582-1594` asserts the error prefix and
  **never re-reads the source list**. `blocking.rs` has **zero** in-crate tests across 905 LOC.
- **Proposed test**: `RPUSH src a`; `SET dst notalist`; `BLMOVE src dst LEFT RIGHT 0` → assert
  WRONGTYPE **and** `LRANGE src 0 -1 == ["a"]` **and** `EXISTS src == 1`. Same for BRPOPLPUSH, and
  for the single-element case where the source is deleted by the pop.
- **Boundary** 3 — must go through `blocking.rs::execute()`; the existing `block_wait` harness API
  bypasses argument parsing entirely, so a new scenario file (not a new harness) is needed.

### F8: No-op writes in `string.rs` dirty the WATCH version and count as writes

- **Severity** 3 — a `SETNX` that returns 0 bumps the key's version, so a concurrent
  `WATCH k; …; EXEC` aborts spuriously. Optimistic-concurrency clients see phantom conflicts and
  livelock; the dirty counter and replication stream also record a write that did not happen.
- **Likelihood** 4 — `WATCH` + `SETNX` is the canonical distributed-lock recipe.
- **Effort** 2 — `exec_transaction` + `watch_keys` already exist in the harness.
- **Priority** 15
- **Evidence**: `grep write_was_noop crates/commands/src/string.rs` → **zero hits**, while
  `basic.rs:641` (SET NX/XX miss), `basic.rs:828`, and `generic.rs:109/183/577` all set it. The
  unmarked no-op returns are `string.rs:68` (`SETNX` NotSet), `:918` (`MSETNX` busy), `:1509` and
  `:1519` (`MSETEX` NX/XX miss), `:1387` and `:1353` (`DELEX` condition not met). Semantics
  defined at `core/src/command.rs:1105-1110`.
- **Proposed test**: `SET k v`; conn A `WATCH k`; conn B `SETNX k other` (returns 0); conn A
  `EXEC` → assert the transaction **succeeds**. Repeat for MSETNX/MSETEX/DELEX. Assert the
  `dirty` delta is 0 for each.
- **Boundary** 3 — `write_was_noop` only has an observable consequence once the effect pipeline
  and the WATCH version bump run; inline units cannot see it.

### F9: `LCS` — unbounded DP allocation, wrong-type keys treated as empty, illegal option combinations accepted

- **Severity** 4 — `LCS` allocates `(m+1)*(n+1)` `usize` with no guard; two 100KB strings is ~80GB
  and aborts the process. Also silently wrong: a wrong-typed key becomes `""` instead of WRONGTYPE.
- **Likelihood** 2 — LCS on large strings is uncommon, but a single request does it.
- **Effort** 1 — pure unit test on the size guard, one shard_driver call for the rest.
- **Priority** 15
- **Evidence**: `string.rs:1054` — the DP matrix is allocated from the two input lengths with no
  ceiling. `string.rs:1022-1031` uses `.unwrap_or_default()` on the key fetch, so a list or hash at
  either key is silently an empty string. `string.rs:1069` returns the bare `LEN` integer before
  reaching `:1074`, so `LCS k1 k2 LEN IDX` returns an integer instead of erroring;
  `:1015-1019` rejects `WITHMATCHLEN` without `IDX`, which Redis accepts. Related, in the same
  family: `APPEND` has no 512MB ceiling at all (`string.rs:196-213`) whereas `SETRANGE` has one,
  and that one is a hardcoded `MAX_STRING_LEN` const (`string.rs:338`) rather than the
  live-mutable `proto-max-bulk-len`.
- **Proposed test**: assert `LCS` on two strings whose product exceeds a configured budget returns
  an error rather than allocating; assert `LCS` against a list key returns WRONGTYPE; assert
  `LEN IDX` errors and `WITHMATCHLEN` without `IDX` matches the chosen policy; assert `APPEND`
  past the string ceiling errors.
- **Boundary** 1 for the allocation guard (pure), 3 for the type/option matrix.

### F10: `hash_cursor_scan` — per-call cost is O(N log N) over the whole collection, `COUNT` counts *matches* not *scans*, and a hash-collision group larger than `COUNT` livelocks the cursor

- **Severity** 4 — the shard worker is single-threaded, so an HSCAN over a large hash with a
  selective `MATCH` occupies it unboundedly (the loop only stops once `COUNT` **matching** items
  are emitted, so a non-matching pattern scans everything). The collision case never advances the
  cursor at all — the client loops forever.
- **Likelihood** 2 — needs a large collection or precomputed collisions, but `DefaultHasher::new()`
  uses fixed public keys, so the collisions are computable offline by anyone.
- **Effort** 2.
- **Priority** 14
- **Evidence**: `utils.rs:134-141 scan_hash` (`DefaultHasher::new()`, `if h == 0 { 1 }`);
  `utils.rs:150-193 hash_cursor_scan` collects **all** items, `sort_unstable_by_key`s them by hash,
  `partition_point`s to the cursor, then emits until `emitted >= count` where `emitted` counts only
  MATCH-passing items (`:184`), setting `new_cursor` to the boundary item's hash — which does not
  advance if the whole `COUNT` window shares one hash. `hash_cursor_scan::<Bytes, Empty<Bytes>>` is
  **untested** (41 regions, 0 covered). Distinct from round-1 issue 24, which covered only the
  keyspace cursor in `core/src/store/hashmap.rs`.
- **Proposed test**: property test — for a hash of N random fields, repeatedly HSCAN to completion
  and assert (a) termination in ≤ N/COUNT + ε calls, (b) every field present at both start and end
  is returned at least once, (c) no field is returned more than K times. Plus a unit test with two
  synthetic keys that collide under `scan_hash`, asserting the cursor strictly advances.
- **Boundary** 1 for the collision/termination property (`hash_cursor_scan` is pure over an
  iterator), 3 for the HSCAN-under-concurrent-mutation guarantee.

### F11: HSCAN/SSCAN/ZSCAN `MATCH` uses a different, weaker glob engine than SCAN/KEYS

- **Severity** 3 — `HSCAN h 0 MATCH "[ab]*"` and `SCAN 0 MATCH "[ab]*"` give different answers for
  the same pattern. Character classes, ranges, negation and backslash escapes silently match
  nothing (or match literally) in the collection scans.
- **Likelihood** 3 — bracket patterns are ordinary glob usage and work in SCAN, so the divergence
  is invisible until someone uses HSCAN.
- **Effort** 1 — a differential proptest between the two functions.
- **Priority** 14
- **Evidence**: `utils.rs:96-124 simple_glob_match` handles only `*` and `?` — no `[abc]`,
  `[a-z]`, `[^abc]`, no `\` escape. It is `monoculture` (34 regions, 4 tests, `main::scan_tcl`
  only). `scan.rs:141` and `server/src/connection/scatter.rs` use the full
  `frogdb_core::glob_match` (`types/src/glob.rs`), which is already proptested.
- **Proposed test**: differential property test — for random patterns and subjects,
  `simple_glob_match(p, s) == glob_match(p, s)`. It will fail immediately; the fix is to delete
  `simple_glob_match` and call `glob_match`, after which the property is the regression test.
- **Boundary** 1 — two pure functions; extend the existing `proptest_glob.rs`.

### F12: "Empty collection deletes the key" is unasserted across the immediate pop paths, LTRIM, and SREM

- **Severity** 3 — a leaked empty key is visible to `EXISTS`/`DBSIZE`/`SCAN`/`TYPE`, is persisted
  to RDB, and breaks blocking-wake logic (a waiter can be woken for a key that has no elements).
- **Likelihood** 3 — the paths are ordinary, and only the *blocked* variants are covered today.
- **Effort** 1.
- **Priority** 14
- **Evidence**: the immediate-path deletes at `blocking.rs:79-81`, `:162-164`, `:256`, `:415`,
  `:503-505`, `:588-590`, `:732`, `:824` are only exercised via the blocked path served by
  `core/src/shard/blocking.rs`. `set_tcl.rs:251-263` is literally named
  `tcl_srem_variadic_version_with_more_args_needed_to_destroy_the_key` and drops upstream's
  `EXISTS` assertion. `list_tcl.rs:869-905` never asserts `EXISTS` after an LTRIM that empties the
  list, though `types/src/types/list.rs:148-152` implements it correctly.
- **Proposed test**: a table-driven scenario over every command that can empty a collection
  (LPOP/RPOP/BLPOP-immediate/BRPOP-immediate/LREM/LTRIM/SREM/SPOP/ZREM/ZREMRANGEBY*/HDEL/
  BLMPOP-immediate/BZMPOP-immediate): after the emptying operation assert `EXISTS == 0`, a `del`
  notification fired, and `memory_check()` shows no residue.
- **Boundary** 3 — the notification and memory assertions need the real shard.

### F13: Blocking commands' negative-argument surface is almost entirely unexercised

- **Severity** 4 — includes the `BLPOP k 1e300` panic (see F5) and six unreached `WrongType` arms.
- **Likelihood** 2 — malformed blocking calls are not routine, but a blocking command that errors
  incorrectly can leave a client hung.
- **Effort** 2.
- **Priority** 14
- **Evidence**: `blocking.rs:349-387` — BLMPOP's entire argument-validation block has **zero**
  coverage, while BZMPOP's equivalent is covered at `zset_tcl.rs:1939-2054`. The `WrongType` arms
  at `:151` (BRPOP), `:236` (BLMOVE), `:392` (BLMPOP), `:492` (BZPOPMIN), `:577` (BZPOPMAX),
  `:705` (BZMPOP) are unreached. `numkeys == 0` yields `SyntaxError` for BLMPOP/BZMPOP
  (`:354`, `:661`) but `"numkeys can't be non-positive value"` for LMPOP/ZMPOP
  (`list.rs:999-1003`, `pop.rs:185-189`) — and Redis says `"numkeys should be greater than 0"`, so
  all three differ. `blocking.rs` has zero in-crate tests.
- **Proposed test**: for each of the six blocking commands, a table of malformed invocations
  (numkeys 0 / negative / `usize::MAX`, missing LEFT|RIGHT, timeout `-1`/`nan`/`inf`/`1e300`,
  wrong-typed key) asserting the exact error and that the connection is **not** left blocked
  (`wait_queue_info()` is empty afterwards).
- **Boundary** 3 — the "not left blocked" assertion requires the real waiter registry.

### F14: The `intentional-incompatibility:encoding` exclusion bucket dropped 86 upstream test bodies in this area, several of which tested encoding-independent behaviour

- **Severity** 3 — each recovered test is a wrong answer we currently ship. Verified uncovered:
  `ZINCRBY` with hexadecimal input (and we *diverge* — `types/src/args.rs:328-336` rejects `0x10`,
  Redis's `strtod` accepts it), `ZINCRBY against invalid incr value`, `ZRANGEBYLEX with LIMIT`
  (**zero** BYLEX+LIMIT invocations exist repo-wide), `$cmd with NaN weights` (`set_ops.rs:85-89`
  is unexecuted), `ZDIFFSTORE with a regular set`, `ZADD incomplete pair` (`basic.rs:54-56`
  unexecuted), `ZUNIONSTORE/ZINTERSTORE with AGGREGATE MIN/MAX` (only the non-STORE forms are
  tested, `zset_tcl.rs:1712`, `:1755`), the `SPOP new implementation: code path #1/#2/#3` trio
  (`set_tcl.rs:16-18` — three algorithmic branches at `types/src/types/set.rs:285-300`, only the
  count>cardinality one is recovered), and the list `- $encoding` LTRIM/LREM/LINSERT >128-element
  variants.
- **Likelihood** 3.
- **Effort** 2 — the exclusions are itemised; recovery is mechanical.
- **Priority** 13
- **Evidence**: 86 of the workspace's 100 `intentional-incompatibility:encoding` exclusion bullets
  are in this area (zset 25, list 24, hash 21, set 12, scan 3, sort 1). In-repo precedent that the
  bucketing is unreliable: `zset_tcl.rs:44-50` documents "Reviewed and un-excluded (issue 54):
  `$cmd with +inf/-inf scores - $encoding` was previously bucketed under 'internal-encoding' purely
  because of its `- $encoding` name suffix, but its actual assertions … are portable protocol
  behavior". Separately, `hash_field_expire_tcl.rs:1-21` uses a **prose** exclusion header naming
  **zero** upstream tests — there is no auditable list for a 41-test port of a much larger upstream
  suite. Bonus: `zset_tcl.rs:63-64` excludes `ZRANGE invalid syntax` / `ZRANGESTORE invalid syntax`
  as "Redis-internal syntax-error format", but those bodies cover LIMIT-without-BY, BYLEX+
  WITHSCORES and validation ordering — i.e. exactly where F6's destructive bug lives.
- **Proposed test**: an audit pass over the 86 bullets classifying each as
  *genuinely-encoding-specific* / *compensated-elsewhere* / *recover*, then port the recover set.
- **Boundary** 4 for direct ports (they are already written against a socket); 3 for the ones whose
  assertions are pure semantics.
- **OPTIONS**:
  - **(a)** Port all recoverable bodies back into `redis-regression` verbatim (level 4). Highest
    fidelity, keeps the exclusion ledger honest, slowest to run.
  - **(b)** Re-express the recovered assertions as `shard_driver` scenarios (level 3). Faster, but
    diverges from upstream text and the exclusion ledger stays wrong.
  - **(c)** Leave the bucket alone and write targeted level-3 tests only for the ~10 confirmed
    uncovered behaviours listed above.
  - **Recommendation: (a) for the ledger correction + (c) for immediate coverage** — fix the
    exclusion classification (it is a standing source of false confidence) but do not block on the
    full port.

### F15: `OBJECT ENCODING` synthesises encodings from hardcoded thresholds and emits a doubled error prefix

- **Severity** 2 — wrong observability output; per the house rule, misleading data is not OK.
  `OBJECT ENCODING` is what operators use to diagnose memory blowups, and it currently ignores the
  live-mutable config entirely.
- **Likelihood** 4 — default config; any operator running `OBJECT ENCODING` on a list of 65–128
  elements gets the wrong answer, and no long string ever reports `raw`.
- **Effort** 1.
- **Priority** 13
- **Evidence**: `generic.rs:336-400` — `Value::SortedSet` → `"listpack"` if `len() <= 128` else
  `"skiplist"`; `Value::List` → `"listpack"` if `len() <= 64` else `"quicklist"` (Redis's default
  `list-max-listpack-size` is **128**); `Value::String` → `"int"` or `"embstr"`, **never** `"raw"`.
  Hash and Set correctly consult `is_listpack()`, proving the pattern. `generic.rs:414-416` passes
  the string `"ERR no such key"` into `CommandError::InvalidArgument`, which
  `types/src/error.rs:119` renders as `"ERR {message}"` → **`ERR ERR no such key`**; the sibling at
  `generic.rs:99` correctly passes `"no such key"`. All of this is hidden because the `$encoding`
  parity tests are excluded (F14) and error assertions are prefix-only (F16).
- **Proposed test**: assert `OBJECT ENCODING` tracks the *live* config (`CONFIG SET
  list-max-listpack-size 8`, push 9 elements, assert `quicklist`); assert a >44-byte string reports
  `raw`; assert `OBJECT ENCODING missing` returns exactly `ERR no such key`.
- **Boundary** 3 — needs live config mutation plus real value construction.

### F16: Error message text is unpinned across the entire area, and several messages are already wrong

- **Severity** 2 — clients (and the parity suite itself) branch on message text; wrong text is a
  compatibility break that no test can currently catch. Two of the messages below are *swappable*
  without any test failing, which means the error-selection logic is untested, not just the text.
- **Likelihood** 4 — every negative-path assertion in the area is prefix-only, so this is the
  default state.
- **Effort** 1.
- **Priority** 13
- **Evidence**: confirmed divergences — `utils.rs:322,329` renders malformed score bounds as
  `"ERR value is not a valid float"` vs Redis's `"ERR min or max is not a float"`;
  `utils.rs:339-341` returns `SyntaxError` for an empty lex bound vs
  `"ERR min or max not valid string range item"`; `utils.rs:428,436,500` say `"GT and LT options…"`
  / `"GT, LT, and NX options…"` vs Redis's `"GT, LT, and/or NX options…"`; `pop.rs:50,117` say
  `"value must be positive"` vs `"value is out of range, must be positive"`; `set_ops.rs:552-554`
  returns `"ERR syntax error"` where Redis says `"Number of keys can't be greater than number of
  args"`. **No test anywhere asserts an exact LIST or SET error message** — `LSET`'s
  `"no such key"` (`list.rs:526`) and `"index out of range"` (`:538`) could be swapped and
  `list_tcl.rs:939-942` / `:950-953` would still pass.
- **Proposed test**: a golden table of `(command, args) -> exact error string` for the ~60 distinct
  error sites in this area, asserted with equality rather than prefix.
- **Boundary** ambiguous — see OPTIONS.
- **OPTIONS**:
  - **(a)** A golden table in `redis-regression` (level 4). Closest to the parity mandate; the
    table sits next to the upstream text it mirrors. Slow, and needs a socket for a pure-string
    property.
  - **(b)** Per-command exact asserts in `shard_driver` scenarios (level 3). Fast, and it is where
    the error-selection logic actually runs. Scatters the table across scenario files.
  - **(c)** A `CommandError`-rendering unit table (level 1) over `types/src/error.rs`. Cheapest, but
    only pins *rendering*, not *which* error a command chooses — it would not have caught the
    swappable-LSET case.
  - **Recommendation: (b)** — one `scenario_error_text.rs` holding the whole table, because it pins
    both selection and rendering at the cheapest level that can do both. Reject (c) as insufficient.

### F17: Missing expiry-option conflict guards in `SET`/`GETEX`

- **Severity** 3 — `SET k v EX 10 PX 10000` is silently accepted with last-wins semantics where
  Redis errors; a client that sends both because of a config bug gets a TTL it did not ask for.
  `GETEX k EXAT <past>` silently no-ops instead of deleting the key.
- **Likelihood** 2.
- **Effort** 1.
- **Priority** 12
- **Evidence**: `basic.rs:561-592` parses the expiry options in a loop with **only** the
  `keep_ttl && expiry` conflict checked (`:599`) — EX+PX, EX+EXAT, PX+PXAT all silently
  last-win. `MSETEX` *does* guard this (`string.rs:1478`), proving the intended behaviour.
  `string.rs:451-464` (GETEX validator) and `:489-525` (apply loop) accept `EX 10 PERSIST`
  together; `:509` / `:517` make a past `EXAT`/`PXAT` a no-op rather than a delete.
- **Proposed test**: a matrix over `{EX,PX,EXAT,PXAT,KEEPTTL,PERSIST}` pairs for SET and GETEX
  asserting error vs. accept, plus `GETEX k EXAT <past>` → `EXISTS k == 0` and a `del`
  notification.
- **Boundary** 3 — the past-EXAT case asserts key deletion and a notification.

### F18: `SETRANGE` empty-value no-op is gated on the wrong condition, and its size check precedes its type check

- **Severity** 3 — `SETRANGE k 10 ""` creates (or pads) a 10-byte NUL-filled string where Redis is
  a no-op that leaves the key absent. Fabricated data.
- **Likelihood** 2.
- **Effort** 1.
- **Priority** 12
- **Evidence**: `string.rs:355` gates the no-op on `offset == 0 && value.is_empty()`; Redis gates
  on the empty value alone. Separately `string.rs:338-343` (size limit) runs before `:345-351`
  (type check), so `SETRANGE <hash-key> <huge-offset> v` reports a size error instead of WRONGTYPE.
- **Proposed test**: `SETRANGE k 10 ""` → reply 0 and `EXISTS k == 0`; on an existing 5-byte
  string, reply 5 and value unchanged; `SETRANGE <hash> 536870911 v` → WRONGTYPE.
- **Boundary** 3 (key-existence assertion); the value-formatting half could be boundary 1.

### F19: Cheap uncovered parity divergences in single commands

- **Severity** 3 — each is a wrong answer.
- **Likelihood** 2.
- **Effort** 1 — one `execute` call each.
- **Priority** 12
- **Evidence**:
  - `LPOS key el MAXLEN 0` returns nothing; Redis treats 0 as unlimited.
    `types/src/types/list.rs:180` does `maxlen.unwrap_or(self.len())`, taking `Some(0)` literally —
    contrast `COUNT 0` → `usize::MAX` at `list.rs:752`, which gets it right.
  - `INCR`/`INCRBY`/`DECR` accept `"+5"` and `"007"` because
    `types/src/types/string_value.rs:58,179` use Rust's `str::parse::<i64>`; Redis's `string2ll`
    rejects both.
  - `LMOVE`/`RPOPLPUSH` with `src == dst` on a **one-element** list deletes then recreates the key,
    losing its TTL (`list.rs:843-849`, `:936-947`); the ≥2-element rotation path is fine.
  - `LPUSHX`/`RPUSHX` declare `EventSpec::Suppressed` (`list.rs:124`, `:165`) with no justification
    comment, violating the contract at `core/src/command_spec.rs:182-216`; Redis emits
    `lpush`/`rpush` keyspace events.
  - Trailing garbage is silently ignored by `ZRANK k m FOO` (`rank.rs:39`, `:96`),
    `ZRANDMEMBER k 2 FOO` (`pop.rs:333-334`), `ZDIFF 1 k FOO` (`set_ops.rs:665-666`), and
    `HRANDFIELD` (all 14 call sites use no arg or a literal WITHVALUES).
  - `ZRANGE missing foo bar` returns `[]` instead of erroring — the missing-key early return at
    `range.rs:86-88` precedes bound parsing at `:94-121` (the read-only sibling of F6).
  - Per-element NaN is not zeroed in zset set-ops: `set_ops.rs:180,270,353,367` push `score*weight`
    raw and `apply_aggregate` (`:116-123`) only zeroes the *final* NaN, so AGGREGATE MIN/MAX
    diverge from Redis on `inf * 0` inputs.
- **Proposed test**: one assertion per bullet in a `scenario_commands_parity.rs` table.
- **Boundary** 3 for the TTL/notification/existence ones, 1 for the `string2ll` parse (extend the
  `types` unit tests).

### F20: `commands/src/scan.rs` contains a dead second implementation of SCAN and KEYS that has already drifted from the live one

- **Severity** 2 — not a live bug, but a fix applied to the obvious-looking file has no effect, and
  the two parsers already return different errors for the same input.
- **Likelihood** 3 — a maintainer adding a `TYPE` value or a COUNT bound will edit the dead copy.
- **Effort** 1 — deleting dead code plus one test on the live path.
- **Priority** 11
- **Evidence**: `scan.rs:62` declares `ExecutionStrategy::ServerWide(ServerWideOp::Scan)` and
  `:129` `ServerWideOp::Keys`, so `server/src/connection/dispatch.rs:229-230` routes to
  `self.handle_scan(args)` / `handle_keys(args)` in `server/src/connection/scatter.rs:43` and
  `:197` — which **hand-roll a second parser** (cursor via `str::from_utf8`+`parse`, its own
  MATCH/COUNT/TYPE loop at `:68-118`, its own duplicate `KeyType` match at `:100-113`).
  `ScanCommand::execute` (63 regions), `KeysCommand::execute` (15) and `parse_key_type` (15) all
  have **0 regions covered** — genuine dead code, verified by dispatch grep, not a coverage
  artefact. Already divergent: `scan.rs:88` returns `"unknown type: X"` vs `scatter.rs:107`'s
  `"ERR unknown type: X"`; `scan.rs:84` uses `ArgParser::try_flag_usize` for COUNT while
  `scatter.rs` hand-parses and returns `"ERR value is not an integer or out of range"`.
- **Proposed test**: delete `ScanCommand::execute`/`KeysCommand::execute`/`parse_key_type` bodies
  (make them `unreachable!()` or remove the impls), and add the `TYPE`/`COUNT`/`MATCH` negative
  matrix against the *live* scatter path.
- **Boundary** 4 for the live path (it only exists in the connection layer), 1 for the retained
  cursor codec.
- **Cross-area**: `server/src/connection/scatter.rs` belongs to the server-area agent; flag the
  duplication to them.

---

## Deprioritised

- **`DefaultHasher` cursor instability across Rust releases** (`utils.rs:134`) — HSCAN cursors are
  not portable across a rebuild, but a cursor never survives a restart anyway (the connection
  dies), so there is no observable failure to test.
- **`COMMAND DOCS <unknown>` fabricates an entry** (`basic.rs:158-178`) and `COMMAND INFO`
  hardcodes `arity = -1`, `first/last/step = 0,0,0` (`basic.rs:240-247`) despite real key specs
  existing — introspection-only, and `COMMAND`/`ACL` output is another agent's area.
- **`flags_match_acl_category`** (`basic.rs:839`, 27 regions, untested) — belongs to the ACL
  agent's surface, not to data-type semantics.
- **Lex-range commands filter the whole zset globally instead of seeking** — a performance
  redesign, not a test gap; a benchmark would document it but not prevent anything.
- **`HSETEX … FVS` keyword absence** — an unimplemented feature, not a missing test.
- **`format_float` direct unit coverage** (`utils.rs:31`) — untested as a function but its output
  is transitively pinned by hundreds of zset parity assertions; low marginal value.
- **`parse_expire_conditions_from_slice`** (`expiry.rs:198-209`) duplicates `:172-183` verbatim and
  only the first is tested — real, but the fix is deduplication rather than a second test, and the
  behaviour is already covered through the first copy.
- **RESP2 vs RESP3 shape differences for this area's commands** — genuinely a level-4 concern
  (connection protocol version) and covered by the protocol agent.

## Cross-area notes

- **Shared infrastructure — the biggest ask**: a **registry-wide argument-fuzz property harness**
  built on `shard_driver` (F5): for every registered command, drive adversarial scalars into every
  arity position and assert "never unwinds". One harness closes an entire bug class across all
  ~250 commands, not just this area. Recommend this be built once and owned centrally rather than
  per-area.
- **`shard_driver` harness gaps** to fix before the scenarios above can be written:
  `ShardDriver::execute` hardcodes `ProtocolVersion::Resp3` (RESP2 shape assertions need a
  variant), and there is no wrapper that drives a *blocking* command through
  `blocking.rs::execute()` — `block_wait` enters at the waiter layer and skips argument parsing
  entirely (F7, F13 both need this).
- **`server/src/connection/scatter.rs:43-217`** — the live SCAN/KEYS implementation and the
  duplicate-parser problem (F20) belong to the server-area agent.
- **`types/src/types/{list,set,hash,sorted_set,string_value}.rs`** — the actual overflow sites
  (F5), the expiry-blind `HashValue::get`/`contains` (F3), `LPOS MAXLEN 0` and the `string2ll`
  parse laxity (F19) are all in the types crate; the *reachability* is a command-argument property
  so the tests belong here, but the fixes are theirs.
- **`core/src/store/hashmap.rs:1239`** (`persist` without an expiry check, F1) and
  **`core/src/shard/blocking.rs:562-590`** (the `Undo` path that the immediate path in F7 lacks)
  belong to the core agent.
- **`server/src/connection/blocking.rs:44`** — `Duration::from_secs_f64` on an unbounded timeout
  (F5) belongs to the server agent.
- **`redis-regression` exclusion ledger** (F14) — the `intentional-incompatibility:encoding`
  bucket and the un-itemised prose header at `hash_field_expire_tcl.rs:1-21` are a repo-wide
  process issue; 86 of the 100 bullets are in this area but the classification bug is not.
