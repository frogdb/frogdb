# Advertised config params parse, set, and do nothing — the golden snapshot pins metadata only

Status: ready-for-agent
Type: AFK
Origin: round-2 testing audit 2026-07-28 — 15 parallel area audits, `.scratch/testing-improvements-round2/`
Source: MASTER.md §2 T3
Score: aggregate of 3 findings
Area: frogdb-config · frogdb-server / runtime_config · frogdb-core / keyspace events

## Context

Three areas independently found knobs that `CONFIG SET` accepts, `CONFIG GET` echoes, and the
118-row golden snapshot certifies as `noop: false` — and that no production code consumes. The
operator tunes a limit, observes no effect, and misdiagnoses.

This is **one piece of work, not N param fixes**: a single "every advertised `CONFIG SET` has an
observable effect" audit driven over the real config registry, so that adding a new inert param
fails the build rather than shipping. The individual inert params are the audit's first failures,
not separate tasks.

## Evidence

- **The golden snapshot cannot see effect at all.** *(15/F11)* `config/src/params.rs:500`
  `GOLDEN_SNAPSHOT` and the assertions at `config/src/params.rs:1339,1357,1372` are over
  `ConfigParamInfo { name, section, field, mutable, noop }` and a row count of 118. Nothing asserts
  a consequence.
- **Listpack thresholds: `noop: false`, zero consumers.** *(15/F11)* `config/src/params.rs:72-112`
  marks `set-max-listpack-entries`, `set-max-listpack-value`, `hash-max-listpack-entries`,
  `hash-max-listpack-value` as `noop: false` (while `list-max-listpack-size` and
  `zset-max-listpack-*` are honestly `noop: true`), and `server/src/runtime_config.rs:2104-2145`
  stores them into `mgr.listpack.hash_max_entries` atomics — but `ListpackConfig::hash_thresholds()`
  and `set_thresholds()` (`core/src/command.rs:46-62`) have **zero callers repo-wide**, and
  `commands/src/hash.rs:77,131,293,615,658,2045` passes the hardcoded
  `ListpackThresholds::DEFAULT_HASH`. So `CONFIG SET hash-max-listpack-entries 512` has no effect
  on `OBJECT ENCODING`.
- **`o` (`OVERWRITTEN`) and `c` (`TYPE_CHANGED`) keyspace-event classes parse and are never
  emitted.** *(02/F12)* `crates/core/src/keyspace_event.rs` defines and parses both;
  `rg 'OVERWRITTEN|TYPE_CHANGED' crates/ --glob '*.rs'` returns **only** that file — zero emission
  sites, in contrast to `NEW` (emitted at `core/src/shard/keyspace_notify.rs:103`, gated by
  `new_events_enabled()`) and `MISS`, which round-1 issue 09 wired up. `keyspace_event.rs` is at
  100% line coverage, which is exactly why percentage is not the signal. Round-1 issue 09 residue.
- **Live-mutable params are proven live against injected collaborators, never against the real
  `Server`.** *(05/F10)* `server/src/runtime_config.rs` publication setters
  `set_snapshot_coordinator`, `set_replication_lag_thresholds`, `set_replication_self_fence`,
  `set_log_reload_handle`, `set_tls_runtime`, `set_config_file_path` are each documented "called at
  most once; a second call is ignored", with production call sites at `server/init.rs:256,328,452`,
  `server/mod.rs:283,288`, `server/cluster_init.rs:116`, `role_manager.rs:685`. Every live-effect
  test (`snapshot_interval_set_reaches_the_published_coordinator`,
  `self_fence_sets_reach_the_published_quorum_checker`, …) builds its own `ConfigManager` and calls
  the setter itself, so **none of them can fail if a production call site is deleted**.
- Also named by MASTER.md §2 T3 as instances of the same shape: `latency-monitor-threshold`
  (a `NoopParam`), `slowlog-max-arg-len`, and 4 further `noop: false` rows.

## What to fix

1. Extend the golden test with a *behavioural* companion: every row with `noop: false` must appear
   in an explicit `NOOP_FALSE_OBSERVED` list, forcing whoever adds a param either to wire an
   observation or to mark it `noop: true`.
2. Add a publication-completeness test on a **booted `Server`**: assert every `set_*` seam in
   `runtime_config.rs` has actually been populated by the real startup path.
3. Add end-to-end spot checks for the params with a cheap observable — the audit names
   `maxmemory`, `appendfsync`, the listpack thresholds, `timeout`, `maxclients`, plus
   `CONFIG SET tracing-sampling-rate 0` ⇒ no spans emitted,
   `CONFIG SET status-memory-warning-percent 1` ⇒ `/status/json` reports a warning,
   `CONFIG SET snapshot-interval-secs 1` ⇒ a snapshot file appears.
4. For `o`/`c`: either wire the emission sites and assert them via the existing
   `core/tests/shard_driver/notify_capture.rs` seam, or make `CONFIG SET` **reject** them so the
   config cannot silently lie. Which one is a semantics call — see issue 30.

## Acceptance criteria

- [ ] A test enumerates the real registry and fails for any `noop: false` row absent from
      `NOOP_FALSE_OBSERVED`; it fails today for the four listpack params.
- [ ] `CONFIG SET hash-max-listpack-entries 3` then `HSET` of 4 fields ⇒ `OBJECT ENCODING` is
      `hashtable`. Fails today.
- [ ] A booted-`Server` test asserts every `runtime_config.rs` publication seam is populated, via
      an `is_published()` accessor or a single publication bitmask, and fails if a production call
      site is removed.
- [ ] At least the audit's named cheap-observable params each have one end-to-end assertion over
      RESP.
- [ ] `notify-keyspace-events "KEoc"` is either observed (`overwritten` / `type_changed` keyevents
      arrive, asserted through `notify_capture.rs`) or rejected by `CONFIG SET`; the test pins
      whichever was decided.

## Test boundary

**Level 2 for the registry-completeness half, level 4 for the observations.** The completeness
assertion is a pure iteration over the config registry and belongs in `config`/`server` unit tests.
The wiring gap 05/F10 names sits precisely between `Server::new` and `ConfigManager`, which no
lower level contains, so the publication-completeness and spot-check assertions genuinely need a
booted server. The `o`/`c` assertion is **level 3** — `notify_capture.rs` gives exact emission-order
assertions without a socket, which is what it was built for.

## Depends on

Issue 12, `.scratch/testing-improvements-round2/issues/` (config observability seams: the
`is_published()` accessor, the `ConfigPersister` IO seam, the protected-route const list, and a
`TestServer` restart-in-place helper). The `o`/`c` implement-or-reject call is tracked in issue 30,
`.scratch/testing-improvements-round2/issues/`. Note `INFRASTRUCTURE.md` I12 records that area 15
asks area 05 to own the registry round-trip and the `noop:false ⇒ observable` tests, written
**once** in `server/tests/`, with 15's findings as the spec.
