# commands — core types — residual test gaps (15 findings)

Status: ready-for-agent
Type: AFK
Origin: round-2 testing audit 2026-07-28 — 15 parallel area audits, `.scratch/testing-improvements-round2/`
Source: proposals/06 — residual findings after promotion to issues 19–76
Score: 15 findings, priority range 11–16 (F0 is structural and unscored)
Area: `frogdb-server/crates/commands/src/` — strings, hashes, lists, sets, sorted sets, generic, expiry, scan, sort, blocking, utils

## Context

This area is the core Redis data-type command surface: 15,424 source LOC across 20 files,
**7,281 instrumented lines, 89.1% covered** (regions 89.3%), with depth classes over 730
unique functions of 422 `well-covered`, 156 `single-test`, 54 `covered`, 54 `untested`, 44
`monoculture`. `set.rs`, `expiry.rs`, `blocking.rs` and all eight `sorted_set/*` files —
4,871 LOC — have **zero** in-crate tests. The proposal's verdict on the shape of that
coverage: it is broad but almost entirely **end-to-end and prefix-asserted**, ~95% of it
arriving from `redis-regression` over a socket where essentially every negative-path
assertion is `assert_error_prefix(.., "ERR")` — which proves Redis parity on the happy path
and proves almost nothing about validation ordering, argument-range arithmetic, or
effect-pipeline side effects, i.e. exactly where the bugs are.

## Promoted elsewhere

- F1 → issue 57, `.scratch/testing-improvements-round2/issues/` (`PERSIST` on a past-deadline key makes it permanently immortal) **and** issue 22, `.scratch/testing-improvements-round2/issues/` (theme T4 — expiry not consistently checked before reads)
- F2 → issue 58, `.scratch/testing-improvements-round2/issues/` (`EXPIRE k -10 GT` deletes a key Redis keeps)
- F4 → issue 59, `.scratch/testing-improvements-round2/issues/` (`SORT BY`/`GET` patterns resolve local-shard-only)
- F6 → issue 24, `.scratch/testing-improvements-round2/issues/` (theme T6 — `*STORE` commands destroy the destination before validating)
- F7 → issue 24, `.scratch/testing-improvements-round2/issues/` (theme T6 — `BLMOVE`/`BRPOPLPUSH` pop and delete the source before type-checking the destination)
- F9 → issue 70, `.scratch/testing-improvements-round2/issues/` (unbounded allocations — the LCS DP matrix)

## Residual findings

### F0 — Where command-semantics tests should live (structural — not scored)

Listed first because the proposal states it "gates every finding below"; it carries no
severity/likelihood/effort/priority line. This is decision D1 in `MASTER.md` §7, filed as
issue 29, `.scratch/testing-improvements-round2/issues/` — settle it before writing the
scenarios the findings below assume.

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

### F3 — Hash-field expiry: seven commands skip the field purge, and "last field expires ⇒ key is deleted" is asserted nowhere

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

### F5 — Unguarded integer/time arithmetic on argument values — client-triggerable panics

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

### F8 — No-op writes in `string.rs` dirty the WATCH version and count as writes

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

### F10 — `hash_cursor_scan`: per-call cost is O(N log N) over the whole collection, `COUNT` counts *matches* not *scans*, and a hash-collision group larger than `COUNT` livelocks the cursor

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

### F11 — HSCAN/SSCAN/ZSCAN `MATCH` uses a different, weaker glob engine than SCAN/KEYS

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

### F12 — "Empty collection deletes the key" is unasserted across the immediate pop paths, LTRIM, and SREM

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

### F13 — Blocking commands' negative-argument surface is almost entirely unexercised

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

### F14 — The `intentional-incompatibility:encoding` exclusion bucket dropped 86 upstream test bodies in this area, several of which tested encoding-independent behaviour

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

### F15 — `OBJECT ENCODING` synthesises encodings from hardcoded thresholds and emits a doubled error prefix

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

### F16 — Error message text is unpinned across the entire area, and several messages are already wrong

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

### F17 — Missing expiry-option conflict guards in `SET`/`GETEX`

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

### F18 — `SETRANGE` empty-value no-op is gated on the wrong condition, and its size check precedes its type check

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

### F19 — Cheap uncovered parity divergences in single commands

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

### F20 — `commands/src/scan.rs` contains a dead second implementation of SCAN and KEYS that has already drifted from the live one

Overlaps the dead-code sweep (issue 34, `.scratch/testing-improvements-round2/issues/`).
`MASTER.md` §5 names `scan.rs`'s SCAN/KEYS impl among the dead code to delete, but cites no
finding numbers, so it claims nothing on its own — the deletion half should land with that
sweep, and the negative-matrix test against the *live* scatter path stays here.

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

## Acceptance criteria

- [ ] F0: a written decision records where command-semantics tests live (the proposal recommends option (b), a `core/tests/shard_driver/scenario_commands_*.rs` family), and no new inline `#[cfg(test)]` test in this crate asserts store state.
- [ ] F3: a test asserts that each of the seven non-purging hash paths (HSETNX, HDEL, HINCRBY, HINCRBYFLOAT, `execute_hexpire_common`, HPERSIST, HSETEX) treats a field past its deadline as absent; and that after the last field expires `EXISTS h == 0`, a `del` keyspace notification fired, and `memory_check()` reports no leaked hash.
- [ ] F5: a test asserts each named site (`EXPIREAT k 9223372036854775807`, `numkeys`-overflow forms of BLMPOP/BZMPOP and the `sorted_set` set-ops, `BLPOP k 1e300`, `ZRANDMEMBER key -9223372036854775808`, `LREM key -9223372036854775808 v`, `SETRANGE k 18446744073709551615 v`) returns a `CommandError` rather than panicking and that the shard still serves a subsequent `PING`; plus a registry-wide proptest asserting every command returns a `Response` or `CommandError` and never unwinds for adversarial scalars at every arity position.
- [ ] F8: a test asserts `WATCH k` on conn A followed by a no-op `SETNX`/`MSETNX`/`MSETEX`/`DELEX` on conn B leaves conn A's `EXEC` **succeeding**, and that the `dirty` delta is 0 for each.
- [ ] F10: a property test asserts HSCAN over N random fields terminates in ≤ N/COUNT + ε calls, returns every field present at both start and end at least once, and returns no field more than K times; plus a unit test with two fields colliding under `scan_hash` asserting the cursor strictly advances.
- [ ] F11: a differential property test asserts `simple_glob_match(p, s) == glob_match(p, s)` over random patterns and subjects.
- [ ] F12: a table-driven scenario asserts, for every command that can empty a collection (LPOP/RPOP/BLPOP-immediate/BRPOP-immediate/LREM/LTRIM/SREM/SPOP/ZREM/ZREMRANGEBY*/HDEL/BLMPOP-immediate/BZMPOP-immediate), that `EXISTS == 0` afterwards, a `del` notification fired, and `memory_check()` shows no residue.
- [ ] F13: a table of malformed invocations for each of the six blocking commands asserts the exact error string and that `wait_queue_info()` is empty afterwards (the connection is not left blocked).
- [ ] F14: every one of the 86 `intentional-incompatibility:encoding` bullets in this area is classified *genuinely-encoding-specific* / *compensated-elsewhere* / *recover* in the ledger, and each behaviour in the "recover" set has a test that executes it.
- [ ] F15: a test asserts `OBJECT ENCODING` tracks live config (`CONFIG SET list-max-listpack-size 8`, push 9 elements → `quicklist`), that a >44-byte string reports `raw`, and that `OBJECT ENCODING missing` returns exactly `ERR no such key` (not `ERR ERR no such key`).
- [ ] F16: a golden table asserts the **exact** error string, by equality not prefix, for the ~60 distinct error sites in this area — including `LSET`'s `"no such key"` vs `"index out of range"`, so swapping them fails.
- [ ] F17: a matrix over `{EX,PX,EXAT,PXAT,KEEPTTL,PERSIST}` pairs for `SET` and `GETEX` asserts error-vs-accept for each pair, and `GETEX k EXAT <past>` yields `EXISTS k == 0` plus a `del` notification.
- [ ] F18: a test asserts `SETRANGE k 10 ""` replies 0 with `EXISTS k == 0`, that on an existing 5-byte string it replies 5 with the value unchanged, and that `SETRANGE <hash> 536870911 v` returns WRONGTYPE (not a size error).
- [ ] F19: one assertion exists per listed divergence — `LPOS … MAXLEN 0` unlimited; `INCR`/`INCRBY`/`DECR` rejecting `"+5"` and `"007"`; `LMOVE`/`RPOPLPUSH` with `src == dst` on a one-element list preserving the TTL; `LPUSHX`/`RPUSHX` keyspace events; trailing garbage rejected by `ZRANK`/`ZRANDMEMBER`/`ZDIFF`/`HRANDFIELD`; `ZRANGE missing foo bar` erroring; per-element NaN zeroed in zset set-ops.
- [ ] F20: `ScanCommand::execute`, `KeysCommand::execute` and `parse_key_type` are gone (or `unreachable!()`), and a `TYPE`/`COUNT`/`MATCH` negative matrix asserts the exact errors of the **live** `scatter.rs` path.

## Depends on

- issue 01, `.scratch/testing-improvements-round2/issues/` (I1 — `shard_driver` harness extension; F13 needs the wrapper that drives a blocking command through `blocking.rs::execute()` because `block_wait` enters at the waiter layer and skips argument parsing, and F0's recommended home needs the `ProtocolVersion` parameter since `ShardDriver::execute` hardcodes RESP3)
- issue 11, `.scratch/testing-improvements-round2/issues/` (I11 — registry-wide argument-fuzz property harness, described by this area's author as "the biggest ask"; it is the second half of F5's proposed test and itself depends on issue 01)

## Re-triage 2026-08-06

**Verdict: still-valid** — 0/15 findings discharged.

| finding | verdict | evidence on today's tree |
|---|---|---|
| F0 where command-semantics tests should live (structural) | still-valid | gated on issue 01 (harness), owned elsewhere |
| F3 seven commands skip the hash-field purge | still-valid | `purge_expired_hash_fields` now has 13 call sites but still none in `HsetnxCommand::execute` (`commands/src/hash.rs:125`), `HdelCommand::execute` (`:216`), `HincrbyCommand::execute` (`:610`), `HincrbyfloatCommand::execute` (`:652`) |
| F5 unguarded integer/time arithmetic | still-valid | `commands/src/expiry.rs:37-47` `unix_secs_to_instant` still `Some(now_instant + duration)`, no `checked_add`; `commands/src/blocking.rs:869-885` `parse_timeout` still rejects only nan/inf/negative, so `1e300` passes |
| F8 no-op writes dirty the WATCH version | still-valid | zero `write_was_noop` uses in `commands/src/string.rs` |
| F10 `hash_cursor_scan` cost / COUNT semantics / livelock | still-valid | `commands/src/utils.rs:98` `scan_hash`, `:114` `hash_cursor_scan` unchanged |
| F11 HSCAN/SSCAN/ZSCAN MATCH uses a weaker glob engine | still-valid | `commands/src/utils.rs:60` `simple_glob_match` still handles only `*` and `?` |
| F12 "empty collection deletes the key" unasserted | still-valid | |
| F13 blocking commands' negative-argument surface | still-valid | |
| F14 `intentional-incompatibility:encoding` dropped 86 upstream bodies | still-valid | the unfreeze restored the suite to `just test` but did not shrink the exclusion bucket |
| F15 `OBJECT ENCODING` synthesised + doubled error prefix | still-valid | `commands/src/generic.rs:431` still `message: "ERR no such key"` → renders `ERR ERR no such key` |
| F16 error text unpinned across the area | still-valid | |
| F17 missing expiry-option conflict guards in SET/GETEX | still-valid | |
| F18 `SETRANGE` empty-value no-op gated on the wrong condition | still-valid | `commands/src/string.rs:356` still `if offset == 0 && value.is_empty()` |
| F19 cheap uncovered parity divergences | still-valid | `types/src/types/list.rs:180` still `let maxlen = maxlen.unwrap_or(self.len());` |
| F20 dead second SCAN/KEYS implementation | still-valid | `commands/src/scan.rs:45` `ScanCommand`, `:112` `KeysCommand`, `:151` `parse_key_type` all still present |

The core command profile is default-on and the redis-regression compat suite was unfrozen at
campaign exit (8e90999b, 3967de82), so these commands *are* exercised again in `just test` — but
the unfreeze restores tests that already existed; it adds nothing that asserts the specific
behaviours above, and every cited defect reproduces verbatim on today's tree. No FM row in
`.scratch/hardening/specs/` covers command semantics for these types (core was not one of the four
locked areas). F0's structural recommendation stays blocked on issue 01.
