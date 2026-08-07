# types / acl / config — residual test gaps (11 findings)

Status: ready-for-agent
Type: AFK
Origin: round-2 testing audit 2026-07-28 — 15 parallel area audits, `.scratch/testing-improvements-round2/`
Source: proposals/15 — residual findings after promotion to issues 19–76
Score: 11 findings, priority range 11–19
Area: `frogdb-server/crates/types/`, `frogdb-server/crates/acl/`, `frogdb-server/crates/config/`

## Context

These are the three foundation crates every other crate depends on: the `Value` enum and its
encodings, argument parsing, glob matching, metrics definitions; the ACL user/permission model,
parser, file persistence and rate limiter; and the config parameter registry, validators and
golden snapshot. `types` is 21.2k LOC at **10926/11908 = 91.8%**, `acl` 6.3k LOC at
**3831/4054 = 94.5%**, `config` 7.4k LOC at **2436/2640 = 92.3%**. **None of the three has a
`tests/` directory** — 100% of their own testing is inline `#[cfg(test)]`, and everything else
exercises them incidentally through `core`, `commands`, `server` integration or the redis
parity suite. The proposal's verdict on the shape of that coverage: "Line coverage in these
three crates is high and almost entirely misleading. The dominant risk is **unlinked duplicate
sources of truth that no test cross-checks**" — and the bug class that escapes is "*a change
somewhere else* (new command, new config knob, new `Value` variant) silently lands with no ACL
category, no live-config consumer, and no round-trip — and every existing test still passes."
Secondary theme: the type-encoding layer (`hash.rs`, `set.rs`, `list.rs`, `sorted_set.rs`,
`string_value.rs`) has **zero inline tests** and hand-rolls a u16 length-prefixed listpack whose
overflow guard is an operator-settable config value.

The proposal's `## Deprioritised` section carries no F-numbers, so nothing there is a finding;
it records `types/src/metrics/definitions.rs` (302 untested, declarative metric metadata),
`types/src/sync.rs` lock-poison helpers, `types/src/traits/metrics.rs` blanket impls,
`redis_version.rs`/`lib.rs` constants, ACL selectors (rejected by design since commit
`8121bfee`; a two-line "selectors are rejected with a clear error" case folds into 15/F2's
table, filed as issue 36, `.scratch/testing-improvements-round2/issues/`), full
`redis-regression` ACL parity expansion, and property-testing `CommandSpec::validate()` itself
as deliberately not filed.

## Promoted elsewhere

- F1 → issue 35, `.scratch/testing-improvements-round2/issues/` (ACL category enforcement is largely inert — 185 of 356 registered commands have no category, so `-@admin` / `-@write` do not deny them) **and** issue 19, `.scratch/testing-improvements-round2/issues/` (theme T1 — hand-maintained parallel tables drift from `CommandSpec`).
- F2 → issue 36, `.scratch/testing-improvements-round2/issues/` (ACL rules are order-insensitive sets, and subcommand rules short-circuit every deny — `+config|get -config` leaves CONFIG GET allowed).
- F4 → issue 75, `.scratch/testing-improvements-round2/issues/` (ACL file persistence is non-atomic, lossy and nondeterministic — `ACL SAVE` then restart can drop or mangle every rule).
- F7 → issue 68, `.scratch/testing-improvements-round2/issues/` (ACL rate-limit refill truncates to zero and clamps non-atomically → permanent starvation).
- F11 → issue 21, `.scratch/testing-improvements-round2/issues/` (theme T3 — the 118-row config golden snapshot pins metadata only, so `noop: false` params with no consumer pass it).
- F13 → issue 33, `.scratch/testing-improvements-round2/issues/` (§4 tests that cannot fail — `tcl_protected_mode_works_as_expected` asserts nothing; the `set_requirepass`/`get_requirepass` half of this finding rides with that sweep).

## Residual findings

### F3 — `Value` round-trip is tested for 6 of 15 variants — a constructible-but-not-round-trippable variant is silent data loss

- **Severity** 5 — silent data loss across restart and replica fan-out.
- **Likelihood** 3 — depends which variants are broken; the untested ones (JSON, TimeSeries, VectorSet, Bloom, Cuckoo, CMS, TopK, TDigest, Histogram) are exactly the newer, less-exercised ones.
- **Effort** 2 — extend the existing proptest file with `Arbitrary` impls.
- **Priority** 19
- **Evidence**: `types/src/types/mod.rs:34-259` defines 15 `Value` variants; `core/tests/proptest_serialization.rs` (398 lines) round-trips only String, integer-string, SortedSet, Hash, List, Set, Stream. `persistence/src/serialization/mod.rs:76,115` is the `serialize`/`deserialize` pair the missing variants would flow through.
- **Proposed test**: a proptest that generates *any* `Value` (an `Arbitrary` for the whole enum, not per-variant strategies) and asserts `deserialize(serialize(v, &md)).unwrap() == (v, md)` — structural equality, not just "no error". Include a totality guard: an exhaustive `match` over `Value` in the strategy so adding a 16th variant fails to compile until a generator exists.
- **Boundary**: 2 — `core/tests/proptest_serialization.rs` already exists at this level and is the natural home; `types` cannot host it (serialization lives in `persistence`).
- **Note**: overlaps the concurrently running "Serialization round-trip coverage" agent — the *types-side* residue is the compile-time totality guard over the `Value` enum, which is what stops the gap from reopening. Dedupe on the proptest body, keep the guard.

The counterpart is 13/F2 and 13/F19 in issue 89,
`.scratch/testing-improvements-round2/issues/` (persistence residual test gaps). Write the
proptest body once; this issue owns the compile-time totality guard.

### F5 — `Value::memory_size()` drives eviction but its accuracy is asserted almost nowhere

- **Severity** 4 — a wrong size estimate makes `maxmemory` evict the wrong keys or fail to evict at all (OOM kill), and it feeds tiered-storage spill accounting.
- **Likelihood** 4 — `maxmemory` + an eviction policy is ordinary production config.
- **Effort** 2 — unit tests in `types` plus one accounting-conservation test in `core`.
- **Priority** 18
- **Evidence**: `types/src/types/mod.rs:205` `memory_size()`; `core/src/store/hashmap.rs:300-305` `hot_entry_memory_size` = `key.len() + value.memory_size() + size_of::<KeyMetadata>() + size_of::<Entry>()`, and `core/src/store/hashmap.rs:750-783` captures `old_accounted = entry.metadata.memory_size` and calls `self.resize(...)` on the spill path — so any drift between the value's *reported* size and the size recorded at insert leaks or double-counts the accounted total forever. Only two `memory_size` tests exist in `types` (`types/src/types/mod.rs:1492,1679`, both `memory_size_listpack_smaller`). `StreamEntry::memory_size` and `CompressedChunk::memory_size` are untested.
- **Proposed test**: (i) unit — for each variant, assert `memory_size()` is monotone non-decreasing as elements are added and strictly decreases on removal, and that it is within a stated factor of a hand-computed lower bound (`Σ element bytes`); (ii) conservation — in `core`, run a randomized insert/update/delete workload and assert the store's accounted total equals `Σ hot_entry_memory_size` recomputed from scratch (this catches the update-path drift, which is the one that actually bites).
- **Boundary**: 1 for (i); 2 for (ii) — the conservation invariant belongs to the store, not to `types`, and needs no shard worker.
- **Cross-area**: the proposal routes (ii) to the core-engine agent so the invariant is asserted once, in `core`, with `types` owning only the per-variant `memory_size` bounds.

### F6 — Listpack length prefixes are `u16`, and the overflow guard is an operator-settable config value

- **Severity** 5 — silent corruption or panic. `hash-max-listpack-value` / `set-max-listpack-entries` are live-mutable `u64` params with `validate: ConfigParam::no_validate`. Set either above 65535 and a large field/value stays in listpack encoding, where `(value.len() as u16)` truncates the length prefix — the buffer is then unparseable and `lp_hash_get` indexes `old_buf[pos..pos+flen]` past the end.
- **Likelihood** 2 — requires an operator raising the threshold (a plausible tuning move, and Redis accepts arbitrary values here), which is why this is 2 and not 1.
- **Effort** 1 — pure unit test.
- **Priority** 18
- **Evidence**: `types/src/types/hash.rs:88-125` (`lp_hash_set`), `types/src/types/hash.rs:129-155` (`lp_hash_remove`), `types/src/types/hash.rs:207-237` (`from_entries`, also `len: entries.len() as u16`), `types/src/types/set.rs:113-124,164-172` — every length is `as u16` with no checked conversion. The only guard is `field.len() > thresholds.max_value_bytes` (`types/src/types/hash.rs:274-277`), and `ListpackThresholds { max_entries: usize, max_value_bytes: usize }` (`types/src/types/mod.rs:559-577`) is unbounded. `server/src/runtime_config.rs:2112-2145` registers the params with `validate: ConfigParam::no_validate`. `types/src/types/hash.rs` and `set.rs` have **0 inline tests**.
- **Proposed test**: unit — `HashValue::from_entries` and `set()` with `ListpackThresholds { max_entries: 200_000, max_value_bytes: 1_000_000 }` and a 70 KiB value; assert `get()` returns the exact value and `len()` is correct (today: expect corruption). Same for `SetValue`. Plus a proptest over random thresholds and entries asserting the listpack and hashmap encodings produce identical observable content.
- **Boundary**: 1 — pure encoding; exactly the case the brief names as the anti-pattern to test through a client.

### F8 — `parse_i64` / `parse_f64` diverge from Redis's `string2ll` and accept `nan`

- **Severity** 3 — wrong answer on a data path a user notices; `nan` reaching a sorted-set score or an INCRBYFLOAT corrupts ordering invariants downstream.
- **Likelihood** 4 — clients send `+5`, `007`, and `nan` more often than one would hope.
- **Effort** 1 — pure unit/proptest.
- **Priority** 16
- **Evidence**: `types/src/args.rs` — `parse_i64` is `s.parse()`, which accepts a leading `+` and leading zeros that Redis's `string2ll` rejects; `parse_f64` special-cases `inf`/`+inf`/`-inf` case-insensitively and then falls through to `s.parse()`, which **accepts `nan`, `NaN`, and `+nan`**. NaN rejection is left to each call site and is inconsistent. `types/src/args.rs`: 19 untested + 25 single-test functions.
- **Proposed test**: table-driven unit test pinning the intended contract for `["", " 1", "1 ", "+1", "007", "-0", "9223372036854775808", "0x10", "1e3", "inf", "Infinity", "nan", "-nan", "1.0"]` for both parsers, plus a proptest asserting `parse_i64(format!("{n}")) == n` for all `i64` and that `parse_f64` never returns NaN. Cross-check the table against `redis-regression` expectations before pinning.
- **Boundary**: 1 — pure functions.

### F9 — No `parse(render(get(ctx))) == get(ctx)` law over the real config registry — the `"00"`-for-`"0"` corruption class is unguarded

**Ownership note (from `INFRASTRUCTURE.md`, I12)**: area 15 asks that **area 05 (server admin /
config / INFO, issue 81, `.scratch/testing-improvements-round2/issues/`) own** this test and the
`noop:false ⇒ observable` test (15/F11, promoted to issue 21,
`.scratch/testing-improvements-round2/issues/`), written **once** in `server/tests/`, with 15's
findings as the specification. Do not duplicate it in `config`.

- **Severity** 4 — a render/parse asymmetry corrupts persisted state (this exact class already shipped once: `"00"` written for `"0"`), and CONFIG REWRITE turns it into an unbootable server.
- **Likelihood** 3 — every new param is a fresh chance; CONFIG REWRITE is an ordinary ops action.
- **Effort** 2.
- **Priority** 16
- **Evidence**: `config/src/param.rs` documents the invariant explicitly — `get` is "Read the live typed value back (for CONFIG GET). **Must round-trip with `render`**" — and every test in the module uses a synthetic `TestCtx` u64 param, never the real registry. `config/src/param.rs`: 53 untested + 30 single-test functions. `DynParam::set` is parse→validate→apply, so the law is mechanically checkable for every registered param.
- **Proposed test**: iterate `config_param_registry()` (all 118 rows) and for each param with a live context assert `set(render(get(ctx)))` succeeds and leaves `get(ctx)` bit-identical; then a second pass asserting idempotence. Add adversarial values per type (`0`, `-1`, `MAX`, empty string, strings containing quotes/newlines/spaces) where the param accepts them.
- **Boundary**: 2 if a `ConfigManager` can be constructed inside `config`'s tests; 4 otherwise (the real registry lives in `server/src/runtime_config.rs`). See cross-area.
- **OPTIONS**:
  - *(a)* In `config` against a test `ConfigManager` (level 2) — fast, but the real 118-row registry is assembled in `server`, so this would cover only the derived-section params.
  - *(b)* In `server/tests/` over the real registry, no socket (level 2-in-server) — covers everything, costs a `server` compile.
  - *(c)* Over RESP via `CONFIG GET`/`CONFIG SET` on a `TestServer` (level 4) — also proves the wire encoding, but slow and it conflates two layers.
  - **Recommendation: (b)** — the registry is the unit under test and it lives in `server`; coordinate with the admin/config agent so this is written once.

### F10 — `types/src/types/{hash,list,set,sorted_set,string_value}.rs` have zero inline tests — encoding transitions are only exercised through commands

- **Severity** 3 — wrong data returned after an encoding promotion (or a promotion that never fires, so memory blows up).
- **Likelihood** 4 — every hash/set crossing 128 entries hits this on default config.
- **Effort** 1.
- **Priority** 16
- **Evidence**: inline test counts by file: `hash.rs` 0, `list.rs` 0, `set.rs` 0, `sorted_set.rs` 0, `string_value.rs` 0 (vs `glob.rs` 23, `json.rs` 32). Depth: `types/src/types/sorted_set.rs` 92 untested functions, `list.rs` 29 untested, `hash.rs` 18 untested + 13 single-test. `types/src/types/hash.rs:261-296` `set()` contains the promotion decision; there is **no demotion path** (matching Redis), which is itself unasserted.
- **Proposed test**: per type, a proptest that applies a random operation sequence to both the real value and a `BTreeMap`/`Vec` reference model, forcing crossings of the entry-count and value-length thresholds in both directions, asserting observable equivalence (`get`, `len`, `contains`, iteration as a set) *and* that encoding never demotes back to listpack after promotion. Cheap, pure, and it also covers F6.
- **Boundary**: 1 — these types are pure; testing them through `HSET` over a socket is exactly the anti-pattern the brief calls out.

### F12 — ACL key/channel pattern permissions (`%R~`, `%W~`, `%RW~`, `&`) and the reset paths are untested

- **Severity** 4 — read/write pattern confusion is a data-exposure bug (a `%W~`-only user reading keys), and `reset_keys`/`reset_channels` failing to clear leaves stale grants after `ACL SETUSER ... resetkeys`.
- **Likelihood** 2 — `%R~`/`%W~` are used by security-conscious operators, not by default.
- **Effort** 1.
- **Priority** 15
- **Evidence**: `acl/src/permissions.rs:437` `reset_keys`, `:443` `reset_channels`, `:210` `deny_all`, `:269` `reset`, and `ChannelPattern::to_rule_string` are all `untested` per depth data. `acl/src/checker.rs` maps the bool verdicts to `AclError` but its key/channel paths are `single-test`.
- **Proposed test**: matrix unit test over (`~*`, `~a*`, `%R~a*`, `%W~a*`, `%RW~a*`) × (read op, write op, RW op such as GETSET) × (matching key, non-matching key), asserting the exact verdict; then `resetkeys` and assert every previously-granted key is denied. Same shape for `&` channel patterns with SUBSCRIBE/PUBLISH/PSUBSCRIBE.
- **Boundary**: 1 — `PermissionSet`/`ChannelPattern` are pure. Add one level-4 smoke that a `%R~` user gets `NOPERM ... keys` on a write, proving the checker is consulted on the write path.

### F14 — Config validators have no coverage of *rejection* across the parameter space

- **Severity** 3 — an out-of-range value accepted at startup surfaces later as a crash or as silently-clamped behaviour.
- **Likelihood** 3.
- **Effort** 2.
- **Priority** 13
- **Evidence**: `config/src/validators/` — 30 tests across 6 files, all example-shaped positives/negatives on cross-field rules. `Config::validate()` (`config/src/lib.rs:298-365`) is a hand-written sequence of checks; nothing asserts that *every* numeric param rejects negative/zero/overflow where it should, and registry rows overwhelmingly use `validate: ConfigParam::no_validate` (e.g. `server/src/runtime_config.rs:2120,2138`).
- **Proposed test**: (i) a `no_validate` audit — assert every registry row either has a non-trivial `validate` or appears in an explicit `VALIDATION_EXEMPT` list, forcing a decision per param; (ii) fuzz `Config::validate()` with a `Config` mutated one field at a time to a boundary value, asserting no panic and that documented-invalid values are rejected.
- **Boundary**: 2 — `Config` and the validators are the crate's public surface.

### F15 — `ConfigParam::default` is dead code, so the "file default == CONFIG default" invariant is documented but unenforced

Overlaps the dead-code sweep (issue 34, `.scratch/testing-improvements-round2/issues/`) —
`MASTER.md` §5 lists `ConfigParam::default`. §5 cites no finding numbers, so it claims nothing
on its own; note the tension: this finding's proposal is to give `default` a *production*
consumer in the form of an enforced invariant, which is the opposite of deleting it. Decide
together.

- **Severity** 3 — a divergence means the value after a fresh start differs from the value after a rewrite, which is how config drift becomes a production incident.
- **Likelihood** 3.
- **Effort** 2.
- **Priority** 13
- **Evidence**: `config/src/param.rs` documents `default` as "ideally the same fn serde's `#[serde(default)]` uses, so the file default and the CONFIG default cannot diverge", and `(p.default)()` is referenced at exactly one site repo-wide — `config/src/param.rs:260`, inside the module's own test. Nothing calls it in production, so nothing checks it.
- **Proposed test**: for every registry row backed by a config field, assert `render((param.default)()) == render(get(&ConfigManager::from(Config::default())))`. Rows where this cannot hold get an explicit exemption with a comment.
- **Boundary**: 2 — same placement question as F9; write them together.

### F16 — `glob_match` returns "no match" when a pattern exceeds `MAX_STAR_COUNT`

- **Severity** 3 — `KEYS`, `SCAN MATCH`, `PSUBSCRIBE`, ACL key patterns, and `CONFIG GET <glob>` all silently return *fewer* results rather than erroring. A user believes a key is absent when it exists.
- **Likelihood** 2 — needs >100 `*` in a pattern; contrived but reachable via generated patterns.
- **Effort** 1.
- **Priority** 12
- **Evidence**: `types/src/glob.rs:18` `const MAX_STAR_COUNT: usize = 100;` and `types/src/glob.rs:65` `if star_count > MAX_STAR_COUNT { return false }` — a silent false negative, not an error. `core/tests/proptest_glob.rs` (310 lines, genuinely thorough) does not pin this behaviour. Note the *positive* finding: the matcher is an iterative two-pointer greedy backtracker (O(n·m)), so **there is no catastrophic-backtracking DoS** here — the dispatch's concern is already addressed by design.
- **Proposed test**: pin the behaviour explicitly — assert a 101-star pattern that *should* match returns `false` today, with a comment stating whether that is intended. If the intent is "error", this is a bug report rather than a test.
- **Boundary**: 1.

### F17 — The ACL parity suite excludes ACL LOAD/SAVE on a premise that is no longer true

- **Severity** 2 — not a bug itself, but it is why F4 is invisible; the exclusion comment actively discourages the next person from adding the coverage.
- **Likelihood** 3.
- **Effort** 1 — delete the comment, unskip the tests, fix what fails.
- **Priority** 11
- **Evidence**: `redis-regression/tests/acl_tcl.rs:1-40` states "FrogDB does not implement ACL file persistence — ACL state lives in the FrogDB config DSL, not in a `.acl` file loaded via `aclfile`" and excludes all ACL LOAD/SAVE parity tests. But `acl/src/manager.rs:256,280` implement `save`/`load`, and `server/src/connection/acl_conn_command.rs:93-94` dispatch `"SAVE" => acl_save(ctx)` / `"LOAD" => acl_load(ctx)`. The feature is shipped and reachable; only the tests believe otherwise.
- **Proposed test**: re-enable the excluded parity cases and correct the header comment.
- **Boundary**: 4 — it is a parity suite; that is where it lives.

Pairs with the promoted F4 (issue 75, `.scratch/testing-improvements-round2/issues/`): F17 is
why F4 is invisible today, so unskipping the parity cases will surface F4's non-atomic, lossy,
nondeterministic `save()` rather than pass.

## Acceptance criteria

- [ ] F3: a proptest generates *any* `Value` via an `Arbitrary` for the whole enum and asserts `deserialize(serialize(v, &md)).unwrap() == (v, md)` structurally; and the strategy contains an exhaustive `match` over `Value` so that adding a 16th variant fails to compile until a generator exists.
- [ ] F5: a test asserts, per `Value` variant, that `memory_size()` is monotone non-decreasing as elements are added, strictly decreases on removal, and is within a stated factor of `Σ element bytes`; `StreamEntry::memory_size` and `CompressedChunk::memory_size` are among them; and a `core` conservation test asserts the store's accounted total equals `Σ hot_entry_memory_size` recomputed from scratch after a randomized insert/update/delete workload.
- [ ] F6: a test with `ListpackThresholds { max_entries: 200_000, max_value_bytes: 1_000_000 }` and a 70 KiB value asserts `HashValue::from_entries`/`set()` and `SetValue` return the exact value from `get()` with a correct `len()` (no `as u16` truncation); plus a proptest over random thresholds asserting listpack and hashmap encodings produce identical observable content.
- [ ] F8: a table-driven test pins the verdict of `parse_i64` and `parse_f64` for each of `["", " 1", "1 ", "+1", "007", "-0", "9223372036854775808", "0x10", "1e3", "inf", "Infinity", "nan", "-nan", "1.0"]`; plus a proptest asserting `parse_i64(format!("{n}")) == n` for all `i64` and that `parse_f64` never returns NaN.
- [ ] F9: a test iterates the real 118-row `config_param_registry()` and asserts for every param with a live context that `set(render(get(ctx)))` succeeds and leaves `get(ctx)` bit-identical, that a second pass is idempotent, and that adversarial per-type values (`0`, `-1`, `MAX`, empty string, quotes/newlines/spaces) round-trip where accepted — written once in `server/tests/`, not duplicated in `config`.
- [ ] F10: each of `hash.rs`, `list.rs`, `set.rs`, `sorted_set.rs`, `string_value.rs` has a proptest applying a random operation sequence to both the real value and a `BTreeMap`/`Vec` reference model, crossing the entry-count and value-length thresholds in both directions, asserting observable equivalence (`get`, `len`, `contains`, iteration as a set) and that encoding never demotes back to listpack after promotion.
- [ ] F12: a matrix test over (`~*`, `~a*`, `%R~a*`, `%W~a*`, `%RW~a*`) × (read op, write op, GETSET) × (matching, non-matching key) asserts the exact verdict, and that after `resetkeys` every previously-granted key is denied; the same shape exists for `&` channel patterns with SUBSCRIBE/PUBLISH/PSUBSCRIBE; plus one level-4 smoke that a `%R~` user gets `NOPERM ... keys` on a write.
- [ ] F14: a test asserts every registry row either has a non-trivial `validate` or appears in an explicit `VALIDATION_EXEMPT` list; and a fuzz test mutates `Config` one field at a time to boundary values, asserting `Config::validate()` never panics and rejects documented-invalid values.
- [ ] F15: a test asserts, for every registry row backed by a config field, that `render((param.default)()) == render(get(&ConfigManager::from(Config::default())))`, with any row that cannot hold carrying an explicit exemption and a comment.
- [ ] F16: a test pins `glob_match`'s behaviour for a 101-star pattern that should match — asserting the current `false` with a comment stating whether that is intended, or asserting an error if the decision is that it should error.
- [ ] F17: the exclusion header at `redis-regression/tests/acl_tcl.rs:1-40` is corrected and the ACL LOAD/SAVE parity cases run rather than being skipped.

## Depends on

Nothing.

The only infrastructure item this area asked for is I3 (issue 03,
`.scratch/testing-improvements-round2/issues/` — injectable clock seam) in its smallest slice,
`acl/src/ratelimit.rs:23 now_us`, and it serves 15/F7, which is **promoted** to issue 68,
`.scratch/testing-improvements-round2/issues/`. No residual finding here needs it, so it is not
listed as a dependency of this issue — schedule it with issue 68.

Two coordination dependencies that are not infra items: F9 is owned by area 05 / issue 81,
`.scratch/testing-improvements-round2/issues/` per `INFRASTRUCTURE.md`'s I12 note (write it once
in `server/tests/`, with this finding as the spec), and F15 shares that placement question.
F5's conservation half belongs to `core/src/store/hashmap.rs` and the core-engine agent, issue
77, `.scratch/testing-improvements-round2/issues/`.

## Re-triage 2026-08-06

**Verdict: partially-fixed** — 0/11 findings fully discharged; F3 and F9 partially.

| F | verdict | evidence (verified today) |
|---|---|---|
| F3 | partially-fixed | The *encoding* half largely landed on the persistence side: `every_marker_round_trips` (`persistence/src/serialization/registry.rs:562+`) round-trips at least one sample per `TypeMarker` over `samples_for` (`:415`), whose `match` is **exhaustive with no wildcard** and is documented as such — so a new marker cannot land without a sample. Unmet: the guard keys off `TypeMarker`, not `Value`, and there is still no proptest generating an arbitrary `Value`; `core/tests/proptest_serialization.rs` still constructs only String / integer-string / SortedSet / Hash / List / Set / Stream. |
| F5 | still-valid | `Value::memory_size` (`types/src/types/mod.rs:205-222`) fans out to all 15 variants; the only inline tests remain the two listpack cases plus `test_stream_value_memory_size` (`:1167`). No monotonicity/lower-bound suite, no store-side conservation test. |
| F6 | still-valid | `types/src/types/{hash,set}.rs` still have **no `mod tests` at all** and every length is still an unchecked `as u16`; the registry rows still use `validate: ConfigParam::no_validate`. |
| F8 | still-valid | `types/src/args.rs:308-311` `parse_i64` is still bare `s.parse()`; `parse_f64` (`:328-337`) still special-cases inf and then falls through to `s.parse()`, which accepts `nan`. |
| F9 | partially-fixed | The specific `"00"`-for-`"0"` corruption was fixed and is now guarded by example — `to_toml_value_string_never_coerces_to_bool_or_integer` (`server/src/runtime_config.rs:4434`), `to_toml_value_*` siblings, `test_rewrite_config_output_is_valid_toml_value` (`:4662`), `min_replicas_max_lag_round_trips_without_losing_a_sub_second_window` (`:4474`). The finding's ask — a registry-wide `set(render(get(ctx)))` law over all 118 rows — is still absent: `test_param_registry_consistency` (`:4135`) was narrowed to guard only `noop ⟺ NoopParam`, everything else having become compile-time-enforced. |
| F10 | still-valid | `hash.rs`, `list.rs`, `set.rs`, `sorted_set.rs`, `string_value.rs` still contain zero `mod tests`. |
| F12 | still-valid | `acl/src/permissions.rs:437` `reset_keys` / `:443` `reset_channels` still have no direct test; the only callers are the parser arms (`acl/src/parser.rs:508,518`). |
| F14 | still-valid | 47 `no_validate` rows in `server/src/runtime_config.rs`; no `VALIDATION_EXEMPT` list exists anywhere. |
| F15 | still-valid | `(p.default)()` is still referenced at exactly one site repo-wide — `config/src/param.rs:260`, inside the module's own test. |
| F16 | still-valid | `types/src/glob.rs:18` `MAX_STAR_COUNT = 100` and `:65` still `return false` on overflow, unpinned. |
| F17 | still-valid | `redis-regression/tests/acl_tcl.rs:1-20` still excludes "ACL LOAD/SAVE tests (file-based ACL persistence)" and still asserts FrogDB does not implement it, while `acl/src/manager.rs` ships `save`/`load`. |

None of these three crates is a locked campaign area, so no FM spec discharges anything here. The
two landed rounds that touched the area moved less than expected: the config-mutability round
(26 params live-mutable, golden at 118 rows) fixed the `"00"` bug and added per-param round-trip
examples but no registry-wide law (F9), and the `frogdb-types` clock/epoch work (d92a7c20,
8b62120f) is orthogonal to F5/F6/F8/F16. Overlaps rather than duplicates: F3's proptest body is
89/F1+F19's, and 89/F1 records the same partial state from the persistence side; F9 and F15 are
still owned by issue 81 per INFRASTRUCTURE.md I12. No live production bug newly confirmed — F6's
`as u16` truncation remains reachable only by an operator raising a listpack threshold above
65535, exactly as filed.
