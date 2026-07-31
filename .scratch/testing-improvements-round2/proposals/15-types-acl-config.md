# Foundation crates (types, ACL, config) — testing gap audit (round 2)

## Scope

| crate | src LOC | lines cov | depth classes (functions) |
|---|---|---|---|
| `frogdb-server/crates/types/` | 21.2k | 10926/11908 = **91.8%** | untested 1064, single-test 720, monoculture 514, covered 96, well-covered 720 |
| `frogdb-server/crates/acl/` | 6.3k | 3831/4054 = **94.5%** | untested 84, single-test 136, monoculture 56, covered 24, well-covered 131 |
| `frogdb-server/crates/config/` | 7.4k | 2436/2640 = **92.3%** | untested 98, single-test 167, monoculture 69, covered 16, well-covered 388 |

None of the three crates has a `tests/` directory — 100% of their own testing is inline
`#[cfg(test)]`. Everything else that exercises them does so incidentally, through `core`,
`commands`, `server` integration, or the redis parity suite.

Worst files by `untested`+`single-test` inside scope:
`types/src/metrics/definitions.rs` (302 untested), `types/src/types/stream.rs` (114),
`types/src/types/sorted_set.rs` (92), `types/src/types/mod.rs` (51),
`config/src/param.rs` (53), `acl/src/manager.rs` (21), `acl/src/ratelimit.rs` (15),
`acl/src/parser.rs` (13).

Out of scope by dispatch: `server/src/{config,admin,runtime_config}` (sibling agent) — but
several findings below terminate there, see [Cross-area notes](#cross-area-notes).

## Summary

Line coverage in these three crates is high and almost entirely misleading. The dominant
risk is **unlinked duplicate sources of truth that no test cross-checks**: the ACL category
tables are a hand-maintained second copy of the command registry (185 registered commands
have *no* ACL category at all, so `-@admin` does not deny MONITOR/CLUSTER/FAILOVER and
`-@write` does not deny JSON.SET/TS.ADD/HEXPIRE/ZREMRANGEBYSCORE — an auth bypass shipping
today); the config golden snapshot pins 118 rows of *metadata* and zero rows of *behaviour*,
so `hash-max-listpack-entries` is flagged `noop: false` while its only consumer,
`ListpackConfig::hash_thresholds()`, has zero callers; and `Value` has 15 variants of which
6 have a serialization round-trip property test. The permission evaluator stores rules in
unordered `HashSet`s and evaluates subcommand rules first-and-unconditionally, so Redis's
"last rule wins" is not merely unimplemented, it is untestable in the current shape — and
no test looks. Secondary theme: the type-encoding layer (`hash.rs`, `set.rs`, `list.rs`,
`sorted_set.rs`, `string_value.rs`) has **zero inline tests** and hand-rolls a u16
length-prefixed listpack whose overflow guard is a config value an operator can raise.

The bug class that escapes today: *a change somewhere else* (new command, new config knob,
new `Value` variant) silently lands with no ACL category, no live-config consumer, and no
round-trip — and every existing test still passes.

## Existing test inventory

| surface | covers | strengths | blind spots |
|---|---|---|---|
| `acl/src/**` inline (9 modules) | rule parse examples, permission spot-checks, save/load smoke | NOPERM message text pinned; per-rule parse cases | no rule *ordering*, no registry cross-check, `set_requirepass` 0 tests, `deny_all`/`reset`/`reset_keys`/`reset_channels` untested, `to_acl_string`→`parse` round-trip absent |
| `config/src/**` inline (21 modules) | 118-row golden registry snapshot, per-validator examples, `ConfigParam` lifecycle on a synthetic `TestCtx` | metadata drift is genuinely caught; validators have 30 example tests | zero behavioural assertions over the *real* registry: no `parse(render(get))` law, no `mutable ⇒ SET applies`, no `noop:false ⇒ has an effect`, no defaults-vs-serde check |
| `types/src/**` inline (31 modules) | algorithmic units: glob (23), json (32), bitmap (16), hll (16), stream (24), skiplist (13), vectorset (23) | genuinely good on the probabilistic/encoding structures | `types/src/types/{hash,list,set,sorted_set,string_value}.rs` = **0 inline tests**; `memory_size` for Stream/CompressedChunk untested |
| `core/tests/proptest_serialization.rs` | round-trip for String, int-string, SortedSet, Hash, List, Set, Stream + never-panic/truncation fuzz | proper proptest, includes corruption fuzz | only 6 of 15 `Value` variants |
| `core/tests/proptest_glob.rs` | glob semantics, thorough | best property file in the repo | does not pin `MAX_STAR_COUNT` behaviour |
| `core/tests/proptest_types.rs` | `StreamId` only | — | despite the name, covers ~1 type |
| `server/tests/integration_acl.rs` | ACL over RESP | end-to-end AUTH/NOPERM shape | no category-denial matrix, no ACL SAVE/LOAD restart |
| `redis-regression/tests/acl_tcl.rs` | ACL parity | large | **header excludes all ACL LOAD/SAVE parity on a false premise** (see F17) |
| `redis-regression/tests/auth_tcl.rs` | AUTH parity | — | `tcl_protected_mode_works_as_expected` is assertion-free |

## Findings

### F1: Nothing links the command registry to the ACL category tables — 185 commands have no category, so `-@admin` / `-@write` do not deny them

- **Severity** 5 — auth bypass + cross-tenant data leak. A user given `+@all -@admin` can
  still run `MONITOR` (streams every other tenant's commands and keys), `CLUSTER`,
  `FAILOVER`, `LATENCY`. A user given `+@all -@write` can still run `JSON.SET`, `TS.ADD`,
  `HEXPIRE`, `ZREMRANGEBYSCORE`, `HGETDEL`, `DELEX`, `MIGRATE`. These are the two rules
  operators actually write.
- **Likelihood** 5 — no config needed; the tables are wrong as committed, and any new
  command lands category-less by default.
- **Effort** 2 — one crate-level test in `commands/` iterating the registry.
- **Priority** 23
- **Evidence**: `acl/src/categories/mod.rs:149-153` — `all_for_command` is
  `COMMAND_ALL_CATEGORIES.get(cmd).cloned().unwrap_or_default()`, i.e. **empty vec on
  miss**, and `acl/src/permissions.rs:215-268` reads *only* `all_for_command`, so an empty
  vec makes every `-@category` rule a no-op for that command. Mechanically extracted the
  356 `static SPEC: CommandSpec` names and diffed against `acl/src/categories/data.rs`:
  **185 registered commands** (~174 excluding test-only stubs `test`/`teststub`/`walmock`/
  `__seam*`) have no `COMMAND_ALL_CATEGORIES` row — all of JSON.*, FT.*, TS.*, TOPK.*,
  TDIGEST.*, BF.*, CF.*, CMS.*, V*, ES.*, plus `migrate`, `lcs`, `substr`, `rpoplpush`,
  `zintercard`, `zremrangeby{lex,rank,score}`, the whole HEXPIRE/HTTL family, `hgetdel`,
  `hgetex`, `hsetex`, `msetex`, `delex`, `xackdel`, `xdelex`, `georadius*_ro`, `lolwut`,
  `psync`, `replconf`, `frogdb.*`. Separately, **20 commands sit in the primary
  `COMMAND_CATEGORIES` table but are missing from `COMMAND_ALL_CATEGORIES`** — `monitor`,
  `cluster`, `failover`, `latency`, `wait`, `waitaof`, `function`, `fcall`, `fcall_ro`,
  `eval_ro`, `evalsha_ro`, `pfdebug`, `pfselftest`, `bzmpop`, `zmpop`, `zdiff`,
  `zdiffstore`, `zinter`, `zunion`, `zrangestore` — so even commands someone remembered to
  categorise are unenforceable. Coverage: `COMMAND_CATEGORIES::{closure#0}`
  (`acl/src/categories/data.rs:10-293`, 276 regions) is `untested`, `regions_covered: 0`,
  `test_count: 0`. The existing `categories/mod.rs` tests are 6 spot-checks plus
  `assert_eq!(all.len(), 21)` on the *enum* (`categories/mod.rs:250`) — **directly
  answering the dispatch question: no, there is no test that catches a new command added
  with no ACL category.**
- **Proposed test**: in `commands/tests/acl_category_coverage.rs`, build a
  `CommandRegistry`, `frogdb_commands::register_all`, iterate `registry.iter()` and assert
  for every spec name that `CommandCategory::all_for_command(name)` is non-empty, that its
  contents are consistent with the spec's own flags (`CommandFlags::WRITE ⇒ contains
  Write`, `!WRITE ⇒ contains Read` for keyed commands, `ADMIN ⇒ contains Admin`), and that
  `for_command`'s primary category is a member of `all_for_command`. Escape hatch: an
  explicit `const CATEGORY_EXEMPT: &[&str]` allowlist for internal/test stubs, so adding a
  real command forces a deliberate edit. Plus a targeted assertion pair:
  `+@all -@admin ⇒ MONITOR denied`, `+@all -@write ⇒ JSON.SET denied`.
- **Boundary**: 2 (crate-level API test in `commands`, which is the lowest crate that can
  see both the registry and — via `core`→`acl` — the tables; `acl` itself cannot, it
  depends only on `types`). No server needed to prove the predicate.

### F2: ACL rules are order-insensitive sets, and subcommand rules short-circuit every deny — `+config|get -config` leaves CONFIG GET allowed

- **Severity** 5 — permission-check bypass. Redis semantics are strictly ordered ("later
  rule wins"); FrogDB stores `allowed_commands`/`denied_commands`/`allowed_categories`/
  `denied_categories` as unordered `HashSet`s plus a `Vec<SubcommandRule>` consulted
  *first*, so the outcome of `ACL SETUSER u +config|get -config` is "allowed" — the
  operator's intent (revoke all CONFIG) is silently inverted.
- **Likelihood** 4 — writing a broad grant then a narrower revoke is the standard way
  humans author ACLs, and `-command` after `+command|subcommand` is the natural fix-up.
- **Effort** 1 — pure unit test on `PermissionSet`.
- **Priority** 22
- **Evidence**: `acl/src/permissions.rs:215-268` — `is_command_allowed` checks
  `subcommand_rules` first and `return rule.allowed` unconditionally, before
  `denied_commands`, before `denied_categories`, before `allow_all`.
  `acl/src/permissions.rs:320-326` — `deny_command` does
  `self.subcommand_rules.retain(|r| r.allowed || r.command != cmd)`, i.e. it deliberately
  **keeps** allowing subcommand rules when the parent command is denied. Also
  `-get +@read` over-denies relative to Redis (the deny set is consulted before the allow
  category, with only an `allowed_commands` escape). Zero tests in the crate apply two
  conflicting rules to the same user.
- **Proposed test**: table-driven unit test over rule *sequences*:
  `[("+@all -@admin", "monitor", false), ("+config|get -config", "config get", false),
  ("-get +@read", "get", true), ("+@read -get +get", "get", true),
  ("-@all +get", "get", true)]` — each row applies rules in order to a fresh
  `PermissionSet` and asserts the final verdict. Companion proptest: generate a random rule
  sequence, evaluate against a trivially-correct ordered reference implementation (a fold
  over the rule list), assert agreement. If the current data model cannot satisfy the
  table, that *is* the finding.
- **Boundary**: 1 — `PermissionSet` is pure; a socket adds nothing.
- **OPTIONS**:
  - *(a)* Unit test on `PermissionSet` (level 1) — fastest, pins the exact semantics, but
    asserts on a structure a fix would likely replace with an ordered `Vec<Rule>`.
  - *(b)* Crate-level test through `AclManager::set_user` + `FullAclChecker` (level 2) —
    survives the refactor to an ordered rule list, still no server, marginally slower.
  - *(c)* Parity test in `redis-regression/tests/acl_tcl.rs` (level 4) — proves Redis
    equivalence, which is the real spec, but slow and it will not run until the semantics
    are fixed.
  - **Recommendation: (b)**, plus one (c) row per bypass shape once fixed. The
    ordered-rule-list refactor is the likely fix, so do not pin `PermissionSet` internals.

### F3: `Value` round-trip is tested for 6 of 15 variants — a constructible-but-not-round-trippable variant is silent data loss

- **Severity** 5 — silent data loss across restart and replica fan-out.
- **Likelihood** 3 — depends which variants are broken; the untested ones (JSON,
  TimeSeries, VectorSet, Bloom, Cuckoo, CMS, TopK, TDigest, Histogram) are exactly the
  newer, less-exercised ones.
- **Effort** 2 — extend the existing proptest file with `Arbitrary` impls.
- **Priority** 19
- **Evidence**: `types/src/types/mod.rs:34-259` defines 15 `Value` variants;
  `core/tests/proptest_serialization.rs` (398 lines) round-trips only String,
  integer-string, SortedSet, Hash, List, Set, Stream.
  `persistence/src/serialization/mod.rs:76,115` is the `serialize`/`deserialize` pair the
  missing variants would flow through.
- **Proposed test**: a proptest that generates *any* `Value` (an `Arbitrary` for the whole
  enum, not per-variant strategies) and asserts
  `deserialize(serialize(v, &md)).unwrap() == (v, md)` — structural equality, not just
  "no error". Include a totality guard: an exhaustive `match` over `Value` in the strategy
  so adding a 16th variant fails to compile until a generator exists.
- **Boundary**: 2 — `core/tests/proptest_serialization.rs` already exists at this level and
  is the natural home; `types` cannot host it (serialization lives in `persistence`).
- **Note**: overlaps the concurrently running "Serialization round-trip coverage" agent —
  the *types-side* residue is the compile-time totality guard over the `Value` enum, which
  is what stops the gap from reopening. Dedupe on the proptest body, keep the guard.

### F4: ACL file persistence is non-atomic and lossy — `ACL SAVE` then restart can drop or mangle every rule

- **Severity** 5 — silent authorization drift across restart, or a server that refuses to
  boot. A truncate-in-place write with no fsync means a crash mid-`ACL SAVE` leaves a
  half-file; `load()` then fails the *whole* file on any single bad line.
- **Likelihood** 3 — needs `aclfile` configured plus a restart/crash, but `ACL SAVE`/`LOAD`
  are wired and reachable.
- **Effort** 2 — crate-level test with `tempfile`.
- **Priority** 19
- **Evidence**: `acl/src/manager.rs:256` — `save()` is `File::create(aclfile)` +
  `write_all`, **no temp file, no rename, no fsync**. `acl/src/manager.rs:280` — `load()`
  aborts the entire file on the first parse error. `acl/src/parser.rs` `parse_acl_line`
  splits on `line.split_whitespace()`, so any key pattern or password containing a space
  round-trips to a different rule set (or an unparseable line → the server cannot load its
  own ACL file). `acl/src/user.rs:167-173` `to_acl_string` emits password hashes by
  iterating a `HashSet<[u8;32]>`, so file content is **nondeterministic between saves**.
  The one existing test (`acl/src/manager.rs` test module, ~545-623) asserts user
  *existence* and a single `authenticate`, not permission equivalence. Depth:
  `acl/src/manager.rs` has 21 untested + 26 single-test functions.
- **Proposed test**: (i) round-trip property — build N users with adversarial content
  (patterns with spaces, `~`/`%R~`/`%W~` mixes, unicode, `&` channels, ratelimit rules,
  multiple passwords), `save()`, fresh `AclManager::load()`, assert for every user that the
  **full permission verdict matrix** (a fixed list of (command, subcommand, key, channel)
  probes) is identical, not merely that the user exists; (ii) determinism — `save()` twice,
  assert byte-identical files; (iii) crash-atomicity — write a truncated file, assert
  `load()` leaves the in-memory ACL untouched and reports an error rather than partially
  applying.
- **Boundary**: 2 — `AclManager` + a tempdir is the whole behaviour; a server adds only the
  RESP wrapper. One additional level-4 test (`ACL SETUSER` → `ACL SAVE` → restart
  `TestServer` → assert the NOPERM matrix) is worth having because it proves the wiring.

### F5: `Value::memory_size()` drives eviction but its accuracy is asserted almost nowhere

- **Severity** 4 — a wrong size estimate makes `maxmemory` evict the wrong keys or fail to
  evict at all (OOM kill), and it feeds tiered-storage spill accounting.
- **Likelihood** 4 — `maxmemory` + an eviction policy is ordinary production config.
- **Effort** 2 — unit tests in `types` plus one accounting-conservation test in `core`.
- **Priority** 18
- **Evidence**: `types/src/types/mod.rs:205` `memory_size()`;
  `core/src/store/hashmap.rs:300-305` `hot_entry_memory_size` = `key.len() +
  value.memory_size() + size_of::<KeyMetadata>() + size_of::<Entry>()`, and
  `core/src/store/hashmap.rs:750-783` captures `old_accounted =
  entry.metadata.memory_size` and calls `self.resize(...)` on the spill path — so any drift
  between the value's *reported* size and the size recorded at insert leaks or
  double-counts the accounted total forever. Only two `memory_size` tests exist in `types`
  (`types/src/types/mod.rs:1492,1679`, both `memory_size_listpack_smaller`).
  `StreamEntry::memory_size` and `CompressedChunk::memory_size` are untested.
- **Proposed test**: (i) unit — for each variant, assert `memory_size()` is monotone
  non-decreasing as elements are added and strictly decreases on removal, and that it is
  within a stated factor of a hand-computed lower bound (`Σ element bytes`);
  (ii) conservation — in `core`, run a randomized insert/update/delete workload and assert
  the store's accounted total equals `Σ hot_entry_memory_size` recomputed from scratch
  (this catches the update-path drift, which is the one that actually bites).
- **Boundary**: 1 for (i); 2 for (ii) — the conservation invariant belongs to the store,
  not to `types`, and needs no shard worker.

### F6: Listpack length prefixes are `u16`, and the overflow guard is an operator-settable config value

- **Severity** 5 — silent corruption or panic. `hash-max-listpack-value` /
  `set-max-listpack-entries` are live-mutable `u64` params with
  `validate: ConfigParam::no_validate`. Set either above 65535 and a large field/value
  stays in listpack encoding, where `(value.len() as u16)` truncates the length prefix —
  the buffer is then unparseable and `lp_hash_get` indexes `old_buf[pos..pos+flen]` past
  the end.
- **Likelihood** 2 — requires an operator raising the threshold (a plausible tuning move,
  and Redis accepts arbitrary values here), which is why this is 2 and not 1.
- **Effort** 1 — pure unit test.
- **Priority** 18
- **Evidence**: `types/src/types/hash.rs:88-125` (`lp_hash_set`),
  `types/src/types/hash.rs:129-155` (`lp_hash_remove`),
  `types/src/types/hash.rs:207-237` (`from_entries`, also `len: entries.len() as u16`),
  `types/src/types/set.rs:113-124,164-172` — every length is `as u16` with no checked
  conversion. The only guard is `field.len() > thresholds.max_value_bytes`
  (`types/src/types/hash.rs:274-277`), and
  `ListpackThresholds { max_entries: usize, max_value_bytes: usize }`
  (`types/src/types/mod.rs:559-577`) is unbounded.
  `server/src/runtime_config.rs:2112-2145` registers the params with
  `validate: ConfigParam::no_validate`. `types/src/types/hash.rs` and `set.rs` have
  **0 inline tests**.
- **Proposed test**: unit — `HashValue::from_entries` and `set()` with
  `ListpackThresholds { max_entries: 200_000, max_value_bytes: 1_000_000 }` and a 70 KiB
  value; assert `get()` returns the exact value and `len()` is correct (today: expect
  corruption). Same for `SetValue`. Plus a proptest over random thresholds and entries
  asserting the listpack and hashmap encodings produce identical observable content.
- **Boundary**: 1 — pure encoding; exactly the case the brief names as the anti-pattern to
  test through a client.

### F7: ACL rate-limit refill truncates to zero and clamps non-atomically — legitimate traffic can be rejected forever

- **Severity** 4 — availability. `refill()` CASes `last_refill_us` to `now` *before*
  computing the credit; if the elapsed window is short enough that
  `cps * SCALE * elapsed_us / 1_000_000` truncates to 0, the elapsed time is consumed and
  **zero tokens are credited**. With `cps = 100` that happens for every call spaced under
  10 µs — trivially reached by a pipelining client or several concurrent connections. Once
  the bucket drains it never refills.
- **Likelihood** 3 — requires `ratelimit:cps=N` to be configured, but the failure is then
  ordinary-load, not adversarial.
- **Effort** 1 (deterministic unit) — the shuttle variant below is effort 4.
- **Priority** 17
- **Evidence**: `acl/src/ratelimit.rs:188-232` — CAS-then-credit ordering, integer-division
  truncation, and a clamp implemented as `fetch_add(add)` followed by
  `fetch_sub(new_val - cap)` with no atomicity between them, so a concurrent `try_acquire`
  can observe an over-cap bucket or have its own spend subtracted twice.
  `acl/src/ratelimit.rs:107-142` `try_acquire` likewise spends via `fetch_sub` and refunds
  via `fetch_add`, so a concurrent caller can see a transiently negative bucket and reject
  spuriously. `acl/src/ratelimit.rs`: 15 untested + 20 single-test functions;
  `acl/src/parser.rs:446` `parse_ratelimit` is untested. Enforcement is real:
  `server/src/connection/guards.rs:123` and `server/src/connection/transaction.rs:170`.
- **Proposed test**: (i) unit with an injectable clock — drain the bucket, advance the
  clock in 1 µs steps 1000 times, assert the credited total equals the credit for 1 ms of
  elapsed time (today: 0); (ii) unit — assert steady-state throughput over a simulated
  second is within ±5% of `cps` for `cps ∈ {1, 10, 100, 1000, 100_000}`; (iii) shuttle —
  two threads acquiring concurrently, assert total granted ≤ cap and that no acquire is
  rejected while tokens remain. Requires making `now_us` injectable
  (`acl/src/ratelimit.rs:23`).
- **Boundary**: 1 for (i)/(ii); shuttle (level-5 flavoured) for (iii) — the clamp race is an
  interleaving bug and nothing below shuttle finds it deterministically.

### F8: `parse_i64` / `parse_f64` diverge from Redis's `string2ll` and accept `nan`

- **Severity** 3 — wrong answer on a data path a user notices; `nan` reaching a sorted-set
  score or an INCRBYFLOAT corrupts ordering invariants downstream.
- **Likelihood** 4 — clients send `+5`, `007`, and `nan` more often than one would hope.
- **Effort** 1 — pure unit/proptest.
- **Priority** 16
- **Evidence**: `types/src/args.rs` — `parse_i64` is `s.parse()`, which accepts a leading
  `+` and leading zeros that Redis's `string2ll` rejects; `parse_f64` special-cases
  `inf`/`+inf`/`-inf` case-insensitively and then falls through to `s.parse()`, which
  **accepts `nan`, `NaN`, and `+nan`**. NaN rejection is left to each call site and is
  inconsistent. `types/src/args.rs`: 19 untested + 25 single-test functions.
- **Proposed test**: table-driven unit test pinning the intended contract for
  `["", " 1", "1 ", "+1", "007", "-0", "9223372036854775808", "0x10", "1e3", "inf",
   "Infinity", "nan", "-nan", "1.0"]` for both parsers, plus a proptest asserting
  `parse_i64(format!("{n}")) == n` for all `i64` and that `parse_f64` never returns NaN.
  Cross-check the table against `redis-regression` expectations before pinning.
- **Boundary**: 1 — pure functions.

### F9: No `parse(render(get(ctx))) == get(ctx)` law over the real config registry — the `"00"`-for-`"0"` corruption class is unguarded

- **Severity** 4 — a render/parse asymmetry corrupts persisted state (this exact class
  already shipped once: `"00"` written for `"0"`), and CONFIG REWRITE turns it into an
  unbootable server.
- **Likelihood** 3 — every new param is a fresh chance; CONFIG REWRITE is an ordinary ops
  action.
- **Effort** 2.
- **Priority** 16
- **Evidence**: `config/src/param.rs` documents the invariant explicitly — `get` is "Read
  the live typed value back (for CONFIG GET). **Must round-trip with `render`**" — and
  every test in the module uses a synthetic `TestCtx` u64 param, never the real registry.
  `config/src/param.rs`: 53 untested + 30 single-test functions. `DynParam::set` is
  parse→validate→apply, so the law is mechanically checkable for every registered param.
- **Proposed test**: iterate `config_param_registry()` (all 118 rows) and for each param
  with a live context assert `set(render(get(ctx)))` succeeds and leaves `get(ctx)`
  bit-identical; then a second pass asserting idempotence. Add adversarial values per type
  (`0`, `-1`, `MAX`, empty string, strings containing quotes/newlines/spaces) where the
  param accepts them.
- **Boundary**: 2 if a `ConfigManager` can be constructed inside `config`'s tests; 4
  otherwise (the real registry lives in `server/src/runtime_config.rs`). See cross-area.
- **OPTIONS**:
  - *(a)* In `config` against a test `ConfigManager` (level 2) — fast, but the real 118-row
    registry is assembled in `server`, so this would cover only the derived-section params.
  - *(b)* In `server/tests/` over the real registry, no socket (level 2-in-server) — covers
    everything, costs a `server` compile.
  - *(c)* Over RESP via `CONFIG GET`/`CONFIG SET` on a `TestServer` (level 4) — also proves
    the wire encoding, but slow and it conflates two layers.
  - **Recommendation: (b)** — the registry is the unit under test and it lives in `server`;
    coordinate with the admin/config agent so this is written once.

### F10: `types/src/types/{hash,list,set,sorted_set,string_value}.rs` have zero inline tests — encoding transitions are only exercised through commands

- **Severity** 3 — wrong data returned after an encoding promotion (or a promotion that
  never fires, so memory blows up).
- **Likelihood** 4 — every hash/set crossing 128 entries hits this on default config.
- **Effort** 1.
- **Priority** 16
- **Evidence**: inline test counts by file: `hash.rs` 0, `list.rs` 0, `set.rs` 0,
  `sorted_set.rs` 0, `string_value.rs` 0 (vs `glob.rs` 23, `json.rs` 32). Depth:
  `types/src/types/sorted_set.rs` 92 untested functions, `list.rs` 29 untested, `hash.rs`
  18 untested + 13 single-test. `types/src/types/hash.rs:261-296` `set()` contains the
  promotion decision; there is **no demotion path** (matching Redis), which is itself
  unasserted.
- **Proposed test**: per type, a proptest that applies a random operation sequence to both
  the real value and a `BTreeMap`/`Vec` reference model, forcing crossings of the
  entry-count and value-length thresholds in both directions, asserting observable
  equivalence (`get`, `len`, `contains`, iteration as a set) *and* that encoding never
  demotes back to listpack after promotion. Cheap, pure, and it also covers F6.
- **Boundary**: 1 — these types are pure; testing them through `HSET` over a socket is
  exactly the anti-pattern the brief calls out.

### F11: The 118-row config golden snapshot pins metadata only — `noop: false` params with no consumer pass it

- **Severity** 3 — a documented, `CONFIG GET`-readable, `CONFIG SET`-accepting knob that
  does nothing. Operators tune it, observe no effect, and misdiagnose.
- **Likelihood** 4 — already true for four params as committed.
- **Effort** 2.
- **Priority** 15
- **Evidence**: `config/src/params.rs:500` `GOLDEN_SNAPSHOT` +
  `config/src/params.rs:1339,1357,1372` — the assertions are over
  `ConfigParamInfo { name, section, field, mutable, noop }` and a row count of 118. Nothing
  asserts effect. Concretely: `config/src/params.rs:72-112` marks
  `set-max-listpack-entries`, `set-max-listpack-value`, `hash-max-listpack-entries`,
  `hash-max-listpack-value` as `noop: false` (while `list-max-listpack-size` and
  `zset-max-listpack-*` are honestly `noop: true`), and
  `server/src/runtime_config.rs:2104-2145` stores them into
  `mgr.listpack.hash_max_entries` atomics — but `ListpackConfig::hash_thresholds()` and
  `set_thresholds()` (`core/src/command.rs:46-62`) have **zero callers** repo-wide, and
  `commands/src/hash.rs:77,131,293,615,658,2045` passes the hardcoded
  `ListpackThresholds::DEFAULT_HASH`. So `CONFIG SET hash-max-listpack-entries 512` is
  accepted, reflected by `CONFIG GET`, pinned by the golden test as non-noop, and has no
  effect on `OBJECT ENCODING`.
- **Proposed test**: extend the golden test with a *behavioural* companion: for every row
  with `noop: false`, require the param to appear in an explicit `NOOP_FALSE_OBSERVED`
  list, forcing whoever adds a param either to wire an observation or to mark it
  `noop: true`. Add one direct assertion:
  `CONFIG SET hash-max-listpack-entries 3` → HSET 4 fields → `OBJECT ENCODING` is
  `hashtable`.
- **Boundary**: 2 for the registry-completeness half; 4 for the `OBJECT ENCODING`
  assertion, which genuinely needs CONFIG SET to reach a live shard.
- **OPTIONS**:
  - *(a)* Curated allowlist in `config` (level 2) — cheap, catches the *next* inert param,
    does not prove the existing ones work.
  - *(b)* Per-param harness in `server` driving `CONFIG SET` then a param-specific
    observation (level 4) — proves effect, but needs ~90 observation closures and half
    will be skipped in practice.
  - **Recommendation: (a) now** (it is what stops regression) plus (b) for the ~10 params
    with a cheap observable (`maxmemory`, `appendfsync`, the listpack thresholds,
    `timeout`, `maxclients`).

### F12: ACL key/channel pattern permissions (`%R~`, `%W~`, `%RW~`, `&`) and the reset paths are untested

- **Severity** 4 — read/write pattern confusion is a data-exposure bug (a `%W~`-only user
  reading keys), and `reset_keys`/`reset_channels` failing to clear leaves stale grants
  after `ACL SETUSER ... resetkeys`.
- **Likelihood** 2 — `%R~`/`%W~` are used by security-conscious operators, not by default.
- **Effort** 1.
- **Priority** 15
- **Evidence**: `acl/src/permissions.rs:437` `reset_keys`, `:443` `reset_channels`,
  `:210` `deny_all`, `:269` `reset`, and `ChannelPattern::to_rule_string` are all
  `untested` per depth data. `acl/src/checker.rs` maps the bool verdicts to `AclError` but
  its key/channel paths are `single-test`.
- **Proposed test**: matrix unit test over (`~*`, `~a*`, `%R~a*`, `%W~a*`, `%RW~a*`) ×
  (read op, write op, RW op such as GETSET) × (matching key, non-matching key), asserting
  the exact verdict; then `resetkeys` and assert every previously-granted key is denied.
  Same shape for `&` channel patterns with SUBSCRIBE/PUBLISH/PSUBSCRIBE.
- **Boundary**: 1 — `PermissionSet`/`ChannelPattern` are pure. Add one level-4 smoke that a
  `%R~` user gets `NOPERM ... keys` on a write, proving the checker is consulted on the
  write path.

### F13: `AclManager::set_requirepass` has zero tests and `get_requirepass` always returns `""`

- **Severity** 4 — password rotation is the most common auth operation; a broken
  `set_requirepass` either locks everyone out or leaves the old password valid.
- **Likelihood** 3 — rotation is a routine ops event.
- **Effort** 3 — needs a server to observe AUTH behaviour end-to-end.
- **Priority** 15
- **Evidence**: `acl/src/manager.rs:338` `set_requirepass` — 0 tests per depth data;
  `acl/src/manager.rs:361` `get_requirepass` returns `""` unconditionally (so
  `CONFIG GET requirepass` cannot reflect reality). The parity suite's
  `redis-regression/tests/auth_tcl.rs:100-144`
  `tcl_auth_fails_when_binary_password_is_wrong` re-sets `requirepass` to the *same* value,
  so it never exercises a change, and `tcl_protected_mode_works_as_expected` has **no
  assertions at all** — its body starts a server, connects, and returns, with a 5-point
  comment describing what it "verifies".
- **Proposed test**: (i) unit — `set_requirepass("a")`, assert
  `authenticate("default","a")` ok and `authenticate("default","b")` fails, then
  `set_requirepass("b")` and assert the verdicts swap (and assert-or-fix
  `get_requirepass`); (ii) level-4 — over RESP: AUTH with the old password,
  `CONFIG SET requirepass new`, assert the **existing** connection stays authenticated and
  a **new** connection is rejected with the old password; (iii) give
  `tcl_protected_mode_works_as_expected` real assertions or delete it.
- **Boundary**: 1 for (i), 4 for (ii) — connection-lifetime auth state is genuinely a
  connection-layer behaviour.

### F14: Config validators have no coverage of *rejection* across the parameter space

- **Severity** 3 — an out-of-range value accepted at startup surfaces later as a crash or
  as silently-clamped behaviour.
- **Likelihood** 3.
- **Effort** 2.
- **Priority** 13
- **Evidence**: `config/src/validators/` — 30 tests across 6 files, all example-shaped
  positives/negatives on cross-field rules. `Config::validate()`
  (`config/src/lib.rs:298-365`) is a hand-written sequence of checks; nothing asserts that
  *every* numeric param rejects negative/zero/overflow where it should, and registry rows
  overwhelmingly use `validate: ConfigParam::no_validate`
  (e.g. `server/src/runtime_config.rs:2120,2138`).
- **Proposed test**: (i) a `no_validate` audit — assert every registry row either has a
  non-trivial `validate` or appears in an explicit `VALIDATION_EXEMPT` list, forcing a
  decision per param; (ii) fuzz `Config::validate()` with a `Config` mutated one field at a
  time to a boundary value, asserting no panic and that documented-invalid values are
  rejected.
- **Boundary**: 2 — `Config` and the validators are the crate's public surface.

### F15: `ConfigParam::default` is dead code, so the "file default == CONFIG default" invariant is documented but unenforced

- **Severity** 3 — a divergence means the value after a fresh start differs from the value
  after a rewrite, which is how config drift becomes a production incident.
- **Likelihood** 3.
- **Effort** 2.
- **Priority** 13
- **Evidence**: `config/src/param.rs` documents `default` as "ideally the same fn serde's
  `#[serde(default)]` uses, so the file default and the CONFIG default cannot diverge", and
  `(p.default)()` is referenced at exactly one site repo-wide — `config/src/param.rs:260`,
  inside the module's own test. Nothing calls it in production, so nothing checks it.
- **Proposed test**: for every registry row backed by a config field, assert
  `render((param.default)()) == render(get(&ConfigManager::from(Config::default())))`.
  Rows where this cannot hold get an explicit exemption with a comment.
- **Boundary**: 2 — same placement question as F9; write them together.

### F16: `glob_match` returns "no match" when a pattern exceeds `MAX_STAR_COUNT`

- **Severity** 3 — `KEYS`, `SCAN MATCH`, `PSUBSCRIBE`, ACL key patterns, and
  `CONFIG GET <glob>` all silently return *fewer* results rather than erroring. A user
  believes a key is absent when it exists.
- **Likelihood** 2 — needs >100 `*` in a pattern; contrived but reachable via generated
  patterns.
- **Effort** 1.
- **Priority** 12
- **Evidence**: `types/src/glob.rs:18` `const MAX_STAR_COUNT: usize = 100;` and
  `types/src/glob.rs:65` `if star_count > MAX_STAR_COUNT { return false }` — a silent false
  negative, not an error. `core/tests/proptest_glob.rs` (310 lines, genuinely thorough)
  does not pin this behaviour. Note the *positive* finding: the matcher is an iterative
  two-pointer greedy backtracker (O(n·m)), so **there is no catastrophic-backtracking DoS**
  here — the dispatch's concern is already addressed by design.
- **Proposed test**: pin the behaviour explicitly — assert a 101-star pattern that *should*
  match returns `false` today, with a comment stating whether that is intended. If the
  intent is "error", this is a bug report rather than a test.
- **Boundary**: 1.

### F17: The ACL parity suite excludes ACL LOAD/SAVE on a premise that is no longer true

- **Severity** 2 — not a bug itself, but it is why F4 is invisible; the exclusion comment
  actively discourages the next person from adding the coverage.
- **Likelihood** 3.
- **Effort** 1 — delete the comment, unskip the tests, fix what fails.
- **Priority** 11
- **Evidence**: `redis-regression/tests/acl_tcl.rs:1-40` states "FrogDB does not implement
  ACL file persistence — ACL state lives in the FrogDB config DSL, not in a `.acl` file
  loaded via `aclfile`" and excludes all ACL LOAD/SAVE parity tests. But
  `acl/src/manager.rs:256,280` implement `save`/`load`, and
  `server/src/connection/acl_conn_command.rs:93-94` dispatch `"SAVE" => acl_save(ctx)` /
  `"LOAD" => acl_load(ctx)`. The feature is shipped and reachable; only the tests believe
  otherwise.
- **Proposed test**: re-enable the excluded parity cases and correct the header comment.
- **Boundary**: 4 — it is a parity suite; that is where it lives.

## Deprioritised

- `types/src/metrics/definitions.rs` (302 untested functions) — the single largest untested
  block in scope, but it is declarative metric metadata; a wrong entry is a wrong dashboard
  label, severity 1-2. A cheap "every definition has a unique name and a non-empty help
  string" unit test would close most of the number if someone wants it moved.
- `types/src/sync.rs` (53 untested) — lock-poison helper extensions
  (`read_or_panic`/`try_read_err`). Exercising them means poisoning a lock on purpose; the
  payoff is a clearer panic message. Not worth it.
- `types/src/traits/metrics.rs` (60 untested) — trait default methods and blanket impls;
  covered transitively wherever the traits are used.
- `types/src/redis_version.rs`, `types/src/lib.rs` — constants and re-exports.
- ACL selectors (`(...)`, `clearselectors`) — the parser returns `ParseError` by design
  (removed in commit 8121bfee). A test pinning "selectors are rejected with a clear error"
  is two lines and worth folding into F2's table, but does not merit its own finding.
- Full `redis-regression` ACL parity expansion — high value but enormous; F1/F2/F4 close
  the actual bypasses at a fraction of the cost, and F17 unblocks the parity path when
  someone has the budget.
- Property-testing `CommandSpec::validate()` itself — the agents auditing `commands` are
  better placed; noted only because F1's cross-check test would sit next to it.

## Cross-area notes

- **F1's test lives in `commands/`, not `acl/`.** `acl` depends only on `types`; `core`
  depends on `acl`; `commands` depends on `core`. `commands/` currently has **no `tests/`
  directory** — creating one is a small shared-infrastructure cost that at least three
  proposed tests (F1, plus the spec-consistency checks other agents will want) can share.
  `CommandRegistry::iter()` (`core/src/registry.rs:256`) already provides the iteration.
- **F1's real fix is architectural and belongs to whoever owns `CommandSpec`**: the ACL
  category should be a field on `CommandSpec` (like `flags`, `keys`, `access`) rather than a
  hand-maintained parallel table in `acl/src/categories/data.rs`. The proposed test is the
  guard rail either way; flag this to the commands/core agents.
- **F9 and F11 terminate in `server/src/runtime_config.rs`**, explicitly the admin/config
  agent's area. The 118-row registry is *defined* there even though the metadata snapshot
  lives in `config/src/params.rs`. The `parse/render/get` round-trip test (F9) and the
  `noop:false ⇒ observable` test (F11) should be written **once**, in `server/tests/`, not
  duplicated. Recommend the coordinator assign both to the admin/config agent with these
  findings as the specification.
- **F11's inert-param bug (`ListpackConfig::hash_thresholds()` has zero callers,
  `commands/src/hash.rs` hardcodes `DEFAULT_HASH`) is a `commands`-crate defect**, not a
  config one. Route to the core-type-commands agent.
- **F3 overlaps the running "Serialization round-trip coverage" agent** — dedupe on the
  proptest body; the residue I am claiming is the compile-time totality guard over the
  `Value` enum so a 16th variant cannot land untested.
- **F5's conservation half belongs to `core/src/store/hashmap.rs`** (memory accounting on
  the update/spill path) — coordinate with the core-engine agent so the invariant is
  asserted once, in `core`, with `types` owning only the per-variant `memory_size` bounds.
- **F7 needs an injectable clock in `acl/src/ratelimit.rs:23` (`now_us`)** — a small
  production-code seam; the shuttle variant also needs `acl` added to the shuttle feature
  matrix (`types` already has shuttle plumbing via `types/src/sync.rs`).
