# Round 2 testing audit — consolidated findings

15 parallel audits, 249 findings, covering every core-database crate. Scored
`Priority = 3·Severity + 2·Likelihood − Effort` per `BRIEF.md`.

Per-area proposals: [`proposals/`](proposals/) (01–15).

**This document is the audit record; the actionable form is [`issues/open/`](issues/open/)** —
95 issues, number ranges mapped in [`README.md`](README.md). §2 themes are issues 19–26, the §3
defect table is issues 35–76 in table order, §4 and §5 are issues 33 and 34, §7 decisions are
issues 29–32, and the §6/`INFRASTRUCTURE.md` items are issues 01–18.

---

## 1. Headline

The audit was commissioned to find **testing gaps**. It found those, but the dominant
result is different and more urgent: **roughly 40 suspected live defects**, most of them
silent-data-loss or consistency-violation shaped, in code that is *nominally covered*.

Workspace line coverage is 85%. That number is not wrong, and it is not protective. The
recurring pattern across all 15 areas is a **test that executes the defective path and
asserts something that stays true when the path is broken** — a `+OK`, an absence of
error, a `len()`, an epsilon comparison, or nothing at all.

This is why coverage percentage was explicitly not used as the ranking signal.

### Data-quality caveat (verified independently)

Two defects in the coverage pipeline itself, both found because agents distrusted their
inputs, both since confirmed by direct measurement:

| artifact | defect | measured |
|---|---|---|
| `target/llvm-cov/lcov.info` | contains essentially no test-execution data; only `config-derive` (a build-time proc macro) has nonzero counts | FNDA 29 nonzero / 34 644 (0.1%); DA 323 / 128 130 (0.3%). **FIXED (issue 27):** root cause was the recipe aborting before writing (missing `target/llvm-cov/` dir → stale artifact consumed); fixed recipe measures 86.6%, DA 128 637 / 147 408 nonzero |
| `target/llvm-cov/depth/depth.json` | `class_counts` computed over duplicate function records (monomorphisations + `::<_>` generic placeholders, one copy zeroed) | `untested` predicted 14 849 raw → ~2 163 span-deduped. **FIXED (issue 28):** measured 15 791 raw → **2 414 span-deduped**; total 39 811 records → 17 115 source functions |

Consequences: `coverage-nightly.yml` consumes `just coverage-lcov`, so **the CI coverage
number is meaningless**; and the claim that the deduplicated figure "matches `llvm-cov export
--format=lcov` exactly" is false. *(Attribution corrected during filing: that string is not in
`docs/agents/coverage-depth.md` — it is emitted into every generated report by
`scripts/coverage-depth.py:839`, with a matching comment at `:667`. The doc is what has to be
corrected; the script is what says it.)* The per-file
`line_counts` in `depth.json` are sound, and the strongest findings below are anchored on
those or on read source, not on depth classes.

Both are filed as their own issues — 27 and 28 in [`issues/open/`](issues/open/) — independent
of this audit. Until fixed, no coverage number from this repo should be quoted.

Every finding whose sole evidence was a depth class was re-checked against **span-deduped**
data before being listed below. All survived, and two came out sharper:

| function | span-deduped | reading |
|---|---|---|
| `failure_detector.rs:330 trigger_auto_failover` | 0 tests, **0/104 regions** | genuinely never executed *(04/F2)* |
| `routing.rs:197 execute_cross_shard_copy` | 0 tests, **0/102 regions** | genuinely never executed *(03/F2)* |
| `strategies.rs:153 merge_sum_integers` | 2 tests, 10/10 regions | full region coverage, both tests all-success *(03/F1)* |
| `acl/categories/mod.rs:149 all_for_command` | **3551 tests, 233 560 execs, 7/7 regions** | the single best illustration in this audit: maximally covered, and the covered path is `unwrap_or_default()` returning `[]` for 185 of 356 commands *(15/F1)* |
| `shard/types.rs:479 invalidate_keys_all_modes` | 37 tests, 16/18 regions | well tested — and the lazy-expiry callsite calls the *other* function *(02/F1)* |

The last two are the argument against coverage-as-signal, stated numerically: a function
can be the most-executed line in the workspace and still be inert, and a function can be
well tested and still never called from the place that matters.

---

## 2. Cross-cutting themes

These recur across areas and are each **one piece of work, not N**. Prefer them over
individual findings — they are where the leverage is.

### T1 — Hand-maintained parallel tables drift from `CommandSpec`

Three areas independently found the same shape: a second, hand-written copy of
command metadata that silently disagrees with the registry.

- **ACL categories** — 185 of 356 registered commands have no `COMMAND_ALL_CATEGORIES`
  row; 20 more (`monitor`, `cluster`, `failover`, `latency`) exist only in the primary
  table. *(15/F1 — verified, see §3)*
- **Scripting write-flags** — `bindings.rs:44-77`'s `is_write_command` list omits JSON.\*,
  TS.\*, PFADD, SETBIT, BITFIELD, BITOP, `SORT…STORE`, HEXPIRE. *(09/F1)*
- **WAL declared keys** — nothing proves a handler's mutations are confined to the keys
  its `WalStrategy::actions()` declares. *(01/F4)*

One registry-consistency test module closes all three and every future instance.
`CommandRegistry::iter()` (`core/src/registry.rs:256`) is the seam.

### T2 — Failure of a *derived* structure is reported as success

- Failed RocksDB spill → real delete + replicated `DEL` + `evicted` notification *(01/F2)*
- Failed warm-tier read → key reads absent but stays in `data`/`expiry_index`/DBSIZE *(01/F3)*
- Mid-iteration RocksDB error indistinguishable from end-of-CF → shard silently truncated,
  returns `Ok` *(13/F8)*
- Search index absent from snapshot + full-sync → `Index::open_or_create` builds an empty
  index and reports `RecoveryOutcome::Recovered{num_docs: 0}` as success *(10/F3, 10/F4)*
- HLL encode failure → persists an empty sketch, returns `Ok` *(13/F9)*

Search's proposal names the right general fix: **one conservation invariant**
(`index_docs ≡ {store keys matching prefix, type, unexpired}`) asserted at quiescent
points of workloads that already exist, living in `crates/testing/` beside the existing
conservation checkers. The same shape generalises to store-vs-expiry-index and
store-vs-DBSIZE.

### T3 — Config that parses, sets, and does nothing

Advertised knobs with no consumer: `o`/`c` keyspace-event classes, `latency-monitor-threshold`
(NoopParam), `slowlog-max-arg-len`, `ListpackConfig::hash_thresholds()`/`set_thresholds()`
(zero callers — `commands/src/hash.rs` hardcodes `DEFAULT_HASH`), 4 further `noop:false`
rows. The 118-row golden pins *metadata only*, so a param can be added, advertised, and
inert without any test noticing. *(02/F12, 05/F10, 15/F11)*

One "every advertised CONFIG SET has an observable effect" audit, over the real registry.

### T4 — Expiry is not consistently checked before reads

`PERSIST`/`RENAME`/`RENAMENX`/`TYPE`/`EXPIRETIME` read past logical expiry and resurrect
dead keys; `HashMapStore::persist` (`core/src/store/hashmap.rs:1239-1247`) clears
`expires_at` with no expiry check while its sibling `touch` at `:1249` has the guard, and
`UNLINK` does it correctly *with an explanatory comment*. Replica-side independent expiry
plus primary `PERSIST` diverges permanently. Search returns expired-unreaped keys with
stale content. *(06/F1, 14/F10, 10/F9)*

### T5 — Partial failure reported as total success

`merge_sum_integers` filters error replies out of the sum; MGET maps a failed shard to
`nil`; `MSetStrategy::merge` returns `+OK` ignoring `shard_results` entirely. Every merge
test feeds only successful shards. *(03/F1)* Same shape in scatter error classification
*(03/F8)* and `TS.MADD` partial failure *(07/F17)*.

### T6 — Errors do not roll back their effects

`execution.rs:240-243` builds a `WriteCommandMeta` even when a handler returns `Err`
(only `write_was_noop` suppresses), so **failed commands still version, persist, and
propagate**. `rollback.rs` covers WAL failure only. Instances: JSON multi-path mutation
drops its rollback snapshot on the error path *(07/F3)*; `*STORE` commands destroy the
destination before validating *(06/F6)*; `BLMOVE`/`BRPOPLPUSH` pop and delete the source
before type-checking the destination *(06/F7)*; script timeout commits and replicates
partial effects *(09/F4)*.

One table-driven invariant over the registry: *command returns `Err` ⟹ no version bump,
no WAL record, no propagation.*

### T7 — Determinism of propagated writes is unverified

`VADD` REDUCE projection is seeded by `rand::random()`; `TOPK.ADD` decay uses `rand`.
Both propagate verbatim → replica divergence. *(07/F4, 07/F13)* Wants one shared
primary/replica determinism table, with those two as its first rows.

### T8 — Five independent `format_float` implementations

`commands/src/utils.rs:31` (ryu, correct), `types/src/types/string_value.rs:338`,
`protocol/src/response.rs:876`, `core/src/shard/timeseries_execution.rs:352`,
`commands/src/timeseries.rs:1370`. Already causes a live divergence: `INCRBYFLOAT` replies
with one rendering and stores another *(08/F1)*. Collapse to one impl + a test asserting
no second definition exists.

---

## 3. Suspected live defects

Found by reading, not by test failure. **None of these are test gaps** — the proposed
tests fail against today's code. Ordered by consequence.

**One** was verified directly by the coordinator (marked ✅); the rest carry the auditing
agent's file:line evidence and need confirmation before or during fixing. *(This line read
"Two" until filing; only one row was ever marked. Two further rows — 03/F1 and 02/F1 — are
independently corroborated by the span-deduped table in §1, which is weaker than a direct
verification and is cited as such in their issues.)*

### Security

| # | defect | evidence |
|---|---|---|
| ✅ | **ACL category enforcement is largely inert.** `permissions.rs:236` is the sole enforcement consumer and calls `all_for_command` → `unwrap_or_default()`. 185/356 commands have no row. `+@all -@admin` still permits MONITOR/FAILOVER/LATENCY; `-@write` still permits JSON.SET/TS.ADD/HEXPIRE. The only two tests of that function use `"GET"`/`"SET"`, which *are* in the table. | 15/F1 |
| | **ACL rules have no ordering.** `is_command_allowed` consults `subcommand_rules` first and returns unconditionally; `deny_command` deliberately retains allowing subcommand rules; rules live in a `HashSet`. `+config\|get -config` allows `CONFIG GET`. Zero tests apply two conflicting rules to one user. | 15/F2 |
| | **Lua sandbox escape.** `sandbox.rs:416-419` claims `_G.__frogdb_backing`/`__frogdb_protected` are hidden; metatables only fire on *absent* raw keys, so user Lua reads/writes `_real_G` and `_protected`. `setmetatable(_G,{})` then sticks permanently in the long-lived per-shard VM. | 09/F3 |
| | **RESP frame injection.** Client-controlled command name interpolated raw into `-ERR unknown command '{cmd_name}'` at six sites; `gen_error` writes it verbatim. Redis sanitises via `sdsmapchars(s,"\r\n","  ",2)`; no equivalent in the workspace. A name containing `\r\n+OK\r\n` puts three frames on the wire where the client expects one. | 08/F3 |
| | **`HELLO … AUTH` password leaks into the MONITOR feed.** | 03/F10 |
| | **Admin HTTP bearer gate untested, default-open.** The middleware guarding `/admin/shutdown\|transfer-leader\|cluster\|role\|nodes` has no test; `// No token configured — allow all` is the default path; `http.bind=0.0.0.0` with no token produces neither warning nor refusal. | 05/F2, 05/F6 |

### Silent data loss

| # | defect | evidence |
|---|---|---|
| | **Failed spill deletes the key and replicates the `DEL`.** A disk write failure destroys data on primary *and* replicas. | 01/F2 |
| | **RocksDB iteration error → silent shard truncation.** `rocks/columns.rs:41-45` `.and_then(\|r\| r.ok())` makes a mid-iteration error indistinguishable from end-of-CF; recovery returns `Ok`. | 13/F8 |
| | **`ES.SNAPSHOT` discards non-UTF-8 state, replies OK.** `snapshot.rs:46` `from_utf8(state).unwrap_or("")`. `ES.REPLAY` then reports version N with empty state → consumer skips every event ≤ N. Unrecoverable. | 07/F1 |
| | **`TS.CREATERULE` rules die on every restart.** `TimeSeriesValue.rules` populated but absent from `serialization/timeseries.rs`; `from_raw` hardcodes `rules: Vec::new()`. Downsampling silently stops. *Found independently by two agents from opposite directions.* | 07/F2, 13/F7 |
| | **`FT.ALTER` on an `ON JSON` index destroys every document.** `index_mgmt.rs:128-141` rescan closure has no `IndexSource::Json` branch; `index.rs:1010` wipes the dir first. Sibling `create.rs:36-52` handles it correctly. All 5 existing ALTER tests are HASH. | 10/F5 |
| | **Restored/attached nodes get a permanently empty search index.** Neither snapshots nor full-sync ship `<data_dir>/search`; `recover` creates an empty index and reports success. Replica answers every FT.SEARCH with 0 hits, forever, no error. | 10/F3, 10/F4 |
| | **Write to a MIGRATING slot returns `+OK`, then is discarded.** `guards.rs:750` gates TRYAGAIN on `keys.len() >= 2`; `:152` converts to ASK only post-execution on a nil reply. Redis `-ASK`s here. | 04/F1 |
| | **FUNCTION libraries never replicated, absent from RDB/full-sync** → FCALL dead after failover. | 09/F5 |
| | **Operator config silently ignored.** `loader.rs:91` merges a discovered `./frogdb.toml` with figment `.nested()`, reinterpreting top-level tables as profiles; `config_source_path` still points at it, so a later CONFIG REWRITE writes to a file that was never read. | 05/F1 |

### Consistency violations

| # | defect | evidence |
|---|---|---|
| | **MULTI/EXEC bypasses the VLL continuation gate.** `dispatch_core.rs:95` has `conn_id` in scope and never calls `can_execute_during_lock`; siblings `Execute` (`:20`) and `ScatterRequest` (`:49`) both do. A transaction mutates shards a cross-shard script believes it holds exclusively. | 12/F2 |
| | **Replica adopts replid+offset before the snapshot arrives.** Only installer failure rewinds; transport/checksum/no-installer failures leave the replica advertising an identity for a dataset it never received → next reconnect gets `+CONTINUE` onto a stale keyspace. The doc comment at `:270-280` claims the opposite. | 14/F1 |
| | **Backlog eviction between PSYNC grant and tail re-extraction truncates replay.** Floor checked at grant time; `replica_session.rs:652` re-extracts with no lower-bound re-check; the `debug_assert` checks ordering, not contiguity. | 14/F2 |
| | **Stale Raft log-reader cache serves overwritten-term entries.** `get_log_reader` clones the cache into a detached handle that never receives `invalidate_cache_range`; openraft creates the reader once at startup. Raft log divergence, deterministic repro. | 11/F3 |
| | **BCAST client-side-caching trackers never invalidated on lazy expiry.** `worker.rs:732,758` call the default-mode-only `invalidate_keys`, not `invalidate_keys_all_modes`. Stale read, forever. Round 1's regression test is default-mode, so it passes. Camouflaged by `has_tracking_clients()` and `has_any_tracking_clients()` being the same function. | 02/F1, 02/F3 |
| | **`INCRBYFLOAT` stores a different rendering than it replies.** `SET k 0; INCRBYFLOAT k 0.1` replies `0.1`, `GET k` returns `0.10000000000000001`. Redis agrees on both. | 08/F1 |
| | **Non-deterministic writes propagated verbatim** (`VADD` REDUCE, `TOPK.ADD`). | 07/F4, 07/F13 |
| | **`PERSIST` on a past-deadline key makes it permanently immortal**, and diverges primary from replica. | 06/F1 |
| | **`EXPIRE k -10 GT` deletes a key Redis keeps** — past-deadline delete applied before the GT/LT comparison. | 06/F2 |
| | **`SORT BY`/`GET` patterns resolve local-shard-only** → wrong ordering on a default 4-shard standalone; missing keys silently sort as `0.0`/`""`. Existing tests pass only because they hash-tag or get lucky. | 06/F4 |
| | **Script timeout commits and replicates partial effects** — FrogDB's own kill path returns `Unkillable` for exactly this case. | 09/F4 |
| | **Scatter merges discard per-shard errors** → partial failure replies as success. | 03/F1 |
| | **`RemoveSlots` ignores its `node_id`** → unassigns another node's slots. **`RemoveNode`** leaves dangling migrations → slot assigned to a ghost node. | 11/F9, 11/F5 |

### Availability / resource

| # | defect | evidence |
|---|---|---|
| | **`FT.SEARCH … LIMIT 0 0` panics the shard worker.** `fetch_limit = 0` → tantivy `assert_ne!(limit, 0)`. No clamp upstream, no `catch_unwind` on the shard loop. This is *the* standard RediSearch count-only idiom; zero occurrences in the repo. Second vector: `COMBINE RRF 0`. | 10/F1 |
| | **VLL continuation drain deadlock.** `vll/src/shard.rs:247-262` awaits inside `dispatch_message(msg).await`, so the queue it waits on cannot drain → guaranteed 2 s shard-wide freeze then `LockTimeout` whenever a scatter is in flight. | 12/F5 |
| | **Disconnect while blocked leaks connection + shard waiter + maxclients slot forever.** `BLPOP k 0` registers `deadline: None`, which the shard never GCs; `fetch_sub` is a bare statement after `run()`. | 03/F3 |
| | **Tracking table `lru_order` grows unbounded**, outside `maxmemory`; compaction only inside `evict_lru`, gated at 1M keys. | 02/F2 |
| | **`_protected` gains one strong entry per EVAL/FCALL** → unbounded per-shard Lua heap. | 09/F2 |
| | **Rate-limit refill truncates to 0** → permanent starvation. | 15/F7 |
| | **Replication frame-size mismatch wedges the link.** `PROTO_MAX_BULK_LEN` 512 MB vs `MAX_FRAME_SIZE` 64 MB, enforced only on decode; encode uses an unchecked `as u32`. | 14/F4 |
| | **Unbounded allocations**: LCS DP matrix, FT deep-offset `2×(offset+limit)`, `BF/CF.LOADCHUNK` `usize` wrap, FUNCTION LOAD capture VM with `memory_limit_bytes: 0`, unbounded RESP nesting depth (stack overflow → abort). | 06/F9, 10/F6, 07/F14, 09/F12, 08/F4 |

### Durability

| # | defect | evidence |
|---|---|---|
| | **`durable_sequence` advances on `sync=false`** — "durable" means "handed to RocksDB". | 13/F3 |
| | **No on-disk format version or magic**; `flags` byte written 0, read into `_flags`. Nothing can refuse a format it doesn't understand. | 13/F5 |
| | **Raft `append` acks durability without fsync** (while `save_vote` does flush). | 11/F10 |
| | **Stager fsyncs neither `metadata.json` nor any directory** before promoting `latest`. | 13/F20 |
| | **ACL `save()` is non-atomic** (`File::create` + `write_all`, no temp/rename/fsync), whitespace-lossy, and nondeterministic (`HashSet` iteration). | 15/F4 |
| | **Replicas ACK on receipt, not apply** → `WAIT` overstates durability. | 14/F7 |

---

## 4. Tests that cannot fail

Found while auditing; each is worse than no test, because it reads as coverage.

- Two zero-assertion cluster-replication tests; a third counts `MOVED` as "found" *(04/F3)*
- The flagship cross-shard atomicity test passes when every reply is an error *(12/F4)*
- Vacuous assertions throughout `crash_recovery_tests.rs` *(13/F11)*
- Round-1's WAL-mode pin is tautological; uses `eprintln!` where an assert belongs *(13/F18)*
- Cluster tests structurally incapable of failing: early-return, `eprintln`, `matches!`-only *(11/F14)*
- All sketch assertions are lower-bound-only (`>= N`) — garbage passes *(07/F15)*
- Double-comparison tests use `< 1e-10` everywhere, hiding rendering changes *(08/F10)*
- 15 COPY integration tests never exercise cross-shard COPY: `crc16("src")%16384%4 == crc16("dst")%16384%4 == 2` *(03/F2)*
- `trigger_auto_failover`'s own named test leaves `auto_failover` at its default `false` *(04/F2)*
- The protected-mode test asserts nothing *(15/F13)*
- `core/tests/concurrency.rs:641` asserts partial cross-shard visibility is *acceptable*, contradicting VLL's stated contract — rename or delete *(12)*

## 5. Dead code found (delete, do not test)

> **Correction, recorded during filing.** The heading overstates the consensus: **five of the ten
> items below are contested by the proposal that found them**, which recommends wiring them up
> rather than deleting them. `CrashTestHarness` (13/F15 wants the API *used*) · `ConfigParam::default`
> (15/F15 wants the invariant wired) · `set_running_function` (09/F15 — deleting it makes
> `FUNCTION STATS running_script` permanently null, which is the actual bug) · `new_replication_id`
> (14 — it is the function replid rotation will need, blocked on
> `.scratch/replication-cluster-rework/`) · `PageCacheSink` (13/F14 recommends *exporting* it under
> `test-support`). Issue 34, [`issues/open/`](issues/open/), marks each contested item as such and
> does not resolve them. Do not treat this section as a delete list.

`connection/builder.rs` (175 lines, zero call sites) · `CrashTestHarness` byte-level verify
API (13 fns, zero call sites) · `scan.rs` SCAN/KEYS impl duplicating the live `scatter.rs`
and already divergent · `SlotMigrationCoordinator::is_migrating`/`migration_for` ·
`types/src/geo.rs:335-352 geohash_range_for_bbox` · `Response::Attribute` (no producer
anywhere) · `ConfigParam::default` · `set_running_function` (zero callers, which is why
FUNCTION STATS `running_script` is always null) · `new_replication_id` (no production
caller) · `PageCacheSink` (unreachable — `WriteSink` is `pub(super)`).

---

## 6. Infrastructure prerequisites

> **Superseded by [`INFRASTRUCTURE.md`](INFRASTRUCTURE.md).** The table below was a lossy
> first pass: it collapsed ~17 requested items into 10, dropping I11 (registry-wide
> argument-fuzz harness, described by its author as "the biggest ask" in that area), I12–I18,
> and it carried no LOE. The companion doc has the full set, tiered by cost, with measured
> LOE and per-item attribution. The `I<N>` labels are a consolidation artifact and do not
> appear in the proposals themselves.
>
> Headline correction from measuring: **I1 ≈ 1–2 days** (every builder option it needs
> already exists and is simply not forwarded), while **I2 ≈ 1–2 weeks** and **I3** is a
> 313-call-site refactor with no existing abstraction. They are not peers and should not be
> scheduled as a block.

Requested independently by multiple agents. A large fraction of proposed tests drop one or
two effort levels once they exist, and several are impossible without them.

| # | infrastructure | unblocks | asked by |
|---|---|---|---|
| I1 | **`shard_driver` harness extension** — `with_eviction(EvictionConfig)`, optional warm/persistent store, `drive_register_tracking` (mirroring `drive_capture_keyspace`), a blocking-command entry wrapper, configurable `ProtocolVersion` | ~20 findings; pulls eviction, tracking, blocking and command-semantics tests from level 4 → 3 | 01, 02, 06, 12 |
| I2 | **Subprocess-SIGKILL crash primitive** in `frogdb-test-harness` | the entire crash-consistency class; today the suite *cannot express* a crash at an arbitrary byte offset. Note `ClusterNode::kill()` is a **graceful** shutdown despite its name | 13, 11, 14 |
| I3 | **Injectable clock seam** | TTL drift, expiry, replication timeouts, cluster elections, rate-limit refill — currently wall-clock-dependent and flaky | 01, 13, 14, 15 |
| I4 | **Conservation checker** in `crates/testing/` for derived structures (index/store/expiry-index/DBSIZE), asserted at quiescent points of existing workloads | T2 entirely — 4 search findings collapse into one invariant | 10 |
| I5 | **"Shard busy running a script" fixture** | 09/F4, 09/F8, 09/F15 — nothing today starts a long EVAL and talks to the same shard on a second connection | 09 |
| I6 | **Live-link fault primitive** — current `fault_injection` only mangles *recorded histories*; and a resync-boundary signal on the replica frame channel (needed to test *and* fix 14/F3, 14/F5) | replication failure-path coverage | 14 |
| I7 | **`ScatterHeavy` workload profile** — the lock-leak checker, probe and runner all already exist and are wired; the profiles simply emit no MGET/MSET/DEL | 12/F1, the cheapest severity-5 item in the audit | 12 |
| I8 | **Virtual-time / injectable-timeout primitive for shuttle** | 12/F7 deterministic interleaving | 12 |
| I9 | **TLS harness**: `TestServerConfig.tls_watch_certs`/`tls_additional_certs`; `TlsFixture` ECDSA variant + in-place regeneration | 03/F9, 03/F13 | 03 |
| I10 | **Fuzz CI** — `fuzz.py` shows the nightly cron was deliberately removed and the PR `corpus-replay` gate is `-runs=0` restore-only, so it silently no-ops on a cold cache. "Continuous fuzzing" is currently not running for any of the 34 targets | round-1 issue 40 residue; protocol + every decoder | 08, 13 |

Tool-choice conclusions, where agents were asked to judge:

- **Cluster** — model checking beats turmoil decisively. `apply_command` is pure,
  synchronous, `BTreeMap`-only → proptest at level 1–2 with zero new infrastructure, and it
  catches 11/F5 and 11/F9 as a generalisation. Round 1 already hit an upstream turmoil
  0.7.1 port leak that makes indefinite partitions impossible, killing the majority-loss
  scenario that would justify more turmoil spend.
- **VLL** — loom is the wrong tool: no atomics, no `UnsafeCell`, no interior mutability;
  state machines are `&mut self` single-owner, all cross-task comms are tokio channels.
  The nondeterminism is message-arrival order → shuttle's domain.
- **Protocol** — property and fuzz should be primary, not example tests. Nine named
  invariants are enumerated in `proposals/08-protocol.md`.

Structural note: **the real RESP decoder is not in the protocol crate.** `FrogDbResp2`
lives in `server/src/connection/{codec,frame_io,util}.rs`, which is why protocol's 85.6%
flatters it. Relocating it drops two findings from effort 2 → 1.

---

## 7. Open decisions

~30 `OPTIONS` blocks were raised; each is recorded in its area proposal. Four are
structural — they change what gets written everywhere else and should be settled first.

**D1 — Home for command-semantics tests.** The `commands` crate has no `tests/` dir (and
genuinely cannot have one: dev-dep cycle → core compiled twice → E0308), has 22 inline
tests across ~2.5k LOC, and `core/tests/shard_driver/` — which has what these tests need —
is used by **zero** tests in this area. Options: extend inline `Box::leak(HashMapStore)`
units (cheapest, but hardcodes `num_shards=1` and has no effects/WAL/notify pipeline, so it
structurally cannot express 6 of the findings); a new `scenario_commands_*.rs` family under
`shard_driver` (real dispatch/store/effects/N-shard routing, no socket); or keep pushing
into `redis-regression` (level 4). *Both command agents recommend the shard_driver family.*

**D2 — Bugs before tests, or tests before bugs.** ~40 suspected live defects. Writing a
pinning test first documents current (wrong) behaviour; fixing first risks unverified
fixes. Several proposed tests *cannot be written at all* until a semantics decision is made
(e.g. what a failed spill should do; whether script-timeout writes survive; the scatter
partial-failure contract; per-shard vs global `slowlog-max-len`).

**D3 — Coverage tooling.** `lcov.info` is empty of test data and CI consumes it;
`depth.json` class counts are ~7× inflated. Fix before or after the testing work.

**D4 — Infrastructure first, or findings first.** I1–I10 unblock and cheapen a large
fraction of the 249 findings, but deliver no coverage on their own.

Remaining decisions requiring a semantics call before their test can assert anything, in
priority order: failed-spill behaviour *(01/F2)* · declared-vs-actual WAL key enforcement
*(01/F4)* · scatter partial-failure contract *(03/F1)* · `slowlog-max-len` per-shard vs
global *(02/F10)* · `o`/`c` event classes — implement or reject *(02/F12)* · script-timeout
write policy *(09/F4)* · cross-shard `SORT` — fix, guard, or document *(06/F4)* ·
`SO_REUSEPORT` release-only gate *(05/F4)* · search write-visibility seam *(10/F7)* ·
INFO fields that are currently fabricated constants — omit rather than fake *(05/F11)*.

---

## 8. Area index

| # | area | findings | proposal |
|---|---|---:|---|
| 01 | core engine (shard/store/eviction) | 16 | [`01-core-engine.md`](proposals/01-core-engine.md) |
| 02 | core state / pubsub / observability | 15 | [`02-core-state-observability.md`](proposals/02-core-state-observability.md) |
| 03 | server net / connection / TLS | 18 | [`03-server-net-connection.md`](proposals/03-server-net-connection.md) |
| 04 | server cluster / slot migration | 14 | [`04-server-cluster-slotmigration.md`](proposals/04-server-cluster-slotmigration.md) |
| 05 | server admin / config / INFO | 17 | [`05-server-admin-config.md`](proposals/05-server-admin-config.md) |
| 06 | commands — core types | 21 | [`06-commands-core-types.md`](proposals/06-commands-core-types.md) |
| 07 | commands — extended types | 20 | [`07-commands-extended-types.md`](proposals/07-commands-extended-types.md) |
| 08 | protocol | 12 | [`08-protocol.md`](proposals/08-protocol.md) |
| 09 | scripting | 16 | [`09-scripting.md`](proposals/09-scripting.md) |
| 10 | search | 15 | [`10-search.md`](proposals/10-search.md) |
| 11 | cluster | 16 | [`11-cluster.md`](proposals/11-cluster.md) |
| 12 | vll | 18 | [`12-vll.md`](proposals/12-vll.md) |
| 13 | persistence | 20 | [`13-persistence.md`](proposals/13-persistence.md) |
| 14 | replication | 14 | [`14-replication.md`](proposals/14-replication.md) |
| 15 | types / acl / config | 17 | [`15-types-acl-config.md`](proposals/15-types-acl-config.md) |

Blocked on the unreviewed PRDs in `.scratch/replication-cluster-rework/` (WAIT-cluster,
EXEC-slot, promotion-replid, epoch-fold), reported as blocked rather than proposed:
cluster-mode data replication assertions *(04/F3)*, purge-then-restart topology loss
*(11/F1)*, promoted-node PSYNC / replid rotation *(14)*. Open round-1 issue 66
(minimal-RDB full-sync carries no dataset) may share a root cause with 09/F5.
