# frogdb-core client state, pub/sub, and observability — testing gap audit (round 2)

## Scope

Audited (`frogdb-server/crates/core/src/`): `client_registry/{mod,info,stats}.rs` (1951 L),
`pubsub.rs` (1508 L), `tracking.rs` (682 L), `keyspace_event.rs` (286 L),
`keyspace_stats.rs` (165 L), `hotkeys.rs` (308 L), `slowlog.rs` (446 L), `latency.rs`,
`latency_histogram.rs`, `metrics.rs` (189 L), `probes.rs` (166 L),
`observability/{mod,wal}.rs` (238 L).

Read as *seams* (owned by other agents, cited only where the bug lives at the call site):
`shard/{tracking→types.rs, keyspace_notify.rs, keyspace_coordinator.rs, dispatch_pubsub.rs,
post_execution.rs, worker.rs}`, `server/src/connection/{hotkeys,slowlog,lifecycle}.rs`,
`server/src/info/sections.rs`.

- **Line coverage for the area: 93.7%** (3878 covered regions). Percentage is not the signal.
- **Depth classes** (deduplicated across test binaries by taking max `test_count` per
  `(file, line_start, name)` — `depth.json` lists every function once per binary, so a raw
  read reports functions as `untested` that are in fact `well-covered`):
  `well-covered` 151, `monoculture` 41, `untested` 30, `single-test` 17, `covered` 5.

**Round-1 residue.** Issues 27–30, 58, 62 (cross-shard keyevent routing, STORE-family
destination-only notifications, lazy-expiry `expired` keyevent, notifications-disabled) and
issue 09 (`n`/`m` classes inert) are all genuinely closed: `integration_pubsub.rs` now has
`test_cross_shard_{keyevent,keyspace,expired}_...` (:207/:262/:309), 40+
`test_*_notifies_*` destination-only tests, and `regression_lazy_expiry_emits_expired_keyevent`
(:366). Residue: (a) issue 09 fixed `n`/`m` but left `o` (`OVERWRITTEN`) and `c`
(`TYPE_CHANGED`) parseable-but-never-emitted; (b) every round-1 fix landed on the
**keyspace-notification** half; the **client-side-caching** half got exactly one regression
test (`regression_lazy_expiry_invalidates_tracked_key`, `integration_client.rs:1307`) and it
covers **default mode only** — F1 below is the same bug class, still live, in BCAST mode.

## Summary

Two distinct silent-stale-read surfaces dominate this area. First, client-side-caching
invalidation has *two* modes (default key-interest, BCAST prefix) and the codebase built a
single seam (`ShardTracking::invalidate_keys_all_modes`, "so the two modes can never drift
apart again") but the lazy-expiry drain in `shard/worker.rs` does not use it — a BCAST
tracker never learns that a key expired lazily, and the existing regression test does not
notice because it is default-mode. The guard that makes this look correct
(`has_tracking_clients()`) is misnamed: it is true for BCAST clients too, so the call site
reads as mode-aware and is not. Second, two tracking/observability structures are unbounded
by construction — `TrackingTable::lru_order` never has entries removed outside the eviction
path (which only runs at 1M keys/shard), and `HotkeySession::entries` is an uncapped HashMap
fed by a live sampling session. Beyond that, the observability data itself is in places
knowingly wrong: `INFO` hardcodes `pubsub_channels`/`pubsub_patterns`/`pubsubshard_channels`/
`tracking_*` to `0` while the Prometheus gauges next to them carry real values;
`slowlog-max-len` is enforced per shard so `SLOWLOG LEN` returns up to `4×` the configured
bound on a default server; and `HOTKEYS ... METRIC cpu` ranks by access count. The bug that
escapes today is: **a correct-looking write path that silently skips one of the two tracking
modes, or an operator-facing counter that is confidently zero.**

## Existing test inventory

| Surface | Covers | Strengths | Blind spots |
|---|---|---|---|
| `core/src/**` inline `#[cfg(test)]` | `keyspace_event` flag parse/format (100%), `keyspace_stats` (100%, incl. RESETSTAT baseline), `latency_histogram` buckets, `slowlog` ring/truncation, `pubsub` sub/unsub/publish counts | Genuinely asserts values, not "no error"; `keyspace_coordinator` has a 4-test suite incl. mailbox-full drop + metric assertion | `tracking.rs` tests never exercise both modes together; no test of `lru_order` length; `hotkeys` tests never assert ranking order |
| `core/tests/shard_driver/` + `notify_capture.rs` | Real `ShardWorker`, real dispatch, no socket; **has a keyspace-notification capture seam** (`drive_capture_keyspace`) used by `scenario_s8` | The right boundary for notification-order and expiry-interleaving assertions already exists | No tracking/invalidation capture seam — nothing registers a `TrackedConnection` at this level, so every invalidation test is forced up to level 4 |
| `server/tests/integration_pubsub.rs` (160 tests, 6196 L) | Cross-shard routing, per-command notification identity, blocking-pop notifications, slow-subscriber overflow teardown (:4691) | Very strong on *which* event fires for *which* command | Overflow teardown tested only for plain channel subscribers; nothing for pattern/sharded subs or the invalidation channel |
| `server/tests/integration_client.rs` (28 tracking tests, :1108–:1877) | OPTIN/OPTOUT/NOLOOP/REDIRECT, BCAST prefix filter/accumulation/overlap-reject, scatter MSET/DEL invalidation, FLUSHDB (default mode) | BCAST prefix *registration* rules are well pinned | No BCAST test for: lazy expiry, active expiry, eviction, FLUSHALL/FLUSHDB. No test of tracking-table capacity eviction end-to-end |
| `server/tests/integration_hotkeys.rs` (144 L, 2 tests) | Reply *shape* only | — | Zero accuracy coverage: no test asserts the returned keys are the hot ones, or that `METRIC` changes the ranking |
| `redis-regression/tests/` | `slowlog_tcl`, `tracking_tcl`, `pubsub_tcl`, `keyspace_tcl`, `introspection_tcl`, `latency_monitor_tcl` | Parity-checked against upstream expectations | `latency_monitor_tcl` documents the whole LATENCY subsystem as `intentional-incompatibility:observability`; `tracking_tcl` excludes everything needing `CONFIG SET tracking-table-max-keys` (which does not exist) |
| `telemetry/tests/` | Prometheus/OTel exporter plumbing | Exporter-level | Nothing cross-checks an exporter gauge against the `INFO` field claiming the same quantity |

## Findings

### F1: Lazy expiry invalidates only default-mode trackers; BCAST trackers get nothing

- **Severity** 5 — a BCAST client caches `k`, `k` expires lazily on the next touch, the client
  is never invalidated and serves the stale value indefinitely. Silent stale read, the exact
  failure mode client-side caching exists to prevent.
- **Likelihood** 4 — BCAST + TTLs is an ordinary configuration; lazy expiry is the *default*
  reclamation path for any key not swept by the active cycle first.
- **Effort** 3
- **Priority** 20
- **Evidence**: `crates/core/src/shard/worker.rs:732-733` and `:758-759` call
  `self.tracking.invalidate_keys(&[key.as_ref()], 0)` — the **default-mode-only** entry point
  (`shard/types.rs:471`, delegates to `tracking_table` only). The both-modes seam
  `invalidate_keys_all_modes` (`shard/types.rs:479`, documented as "The single seam every write
  path uses so the two modes can never drift apart again") is used by `post_execution.rs:686`
  and reached by active expiry via `run_internal_removal_effects`
  (`event_loop.rs:247`) — but **not** by these two lazy-expiry drains. A BCAST client's keys
  live in `broadcast_table`, never in `tracking_table`, so `invalidate_keys` sends nothing.
  The existing round-1 regression `integration_client.rs:1307
  regression_lazy_expiry_invalidates_tracked_key` uses default mode and therefore passes.
- **Proposed test**: `CLIENT TRACKING on BCAST PREFIX k` on a RESP3 connection; `SET k v PX 50`;
  wait past expiry; touch `k` from a second connection to force the lazy drain; assert an
  `invalidate` push carrying `k` arrives within a bounded wait. Mirror-test for the `emptied`
  branch (`worker.rs:758` — hash-field death emptying the container).
- **Boundary**: 4 (server integration, `TestServer` + RESP3) — the defect is a *call-site*
  choice inside `ShardWorker`, so a unit test on `ShardTracking` cannot catch it; the shard
  driver has no seam to register a `TrackedConnection` (see F5 OPTIONS / Cross-area notes).
  Cheapest correct placement is beside the existing default-mode regression in
  `integration_client.rs`.

### F2: `TrackingTable::lru_order` grows without bound; only the 1M-key eviction path compacts it

- **Severity** 4 — unbounded RSS growth outside `maxmemory` accounting, ending in OOM-kill /
  crash-loop on a long-lived tracking connection.
- **Likelihood** 4 — needs only one RESP3 tracking client on a churning keyspace; every
  read of a *new* key pushes an entry and nothing pops it until the table reaches `max_keys`.
- **Effort** 1
- **Priority** 19
- **Evidence**: `crates/core/src/tracking.rs:103-177`. `record_read` pushes to `lru_order` on
  every first-sight key. `invalidate_keys` removes from `key_to_clients` with the comment
  "*we don't remove from lru_order here — stale entries are cleaned lazily during eviction*",
  and `remove_connection` (`:191-204`) has the same "*Stale lru_order entries cleaned lazily
  during eviction*" note. Compaction lives **only** inside `evict_lru` (`:207`), which is
  called only from `while self.key_to_clients.len() > self.max_keys`. A read-then-write
  workload keeps `key_to_clients` small forever, so `evict_lru` never runs and `lru_order`
  grows monotonically with the number of *distinct keys ever read*. No test asserts
  `lru_order.len()`; `evict_lru` is `single-test`.
- **Proposed test**: unit test on `TrackingTable` — register one tracked conn, loop 10_000×
  {`record_read(key_i)`, `invalidate_keys(&[key_i])`} with `max_keys` at its 1M default, then
  assert an exposed length/capacity accessor stays O(live keys), not O(iterations). (Add a
  `#[cfg(test)]`-visible `lru_len()` or make compaction observable via `memory_usage()`.)
- **Boundary**: 1 (pure unit) — a pure data-structure invariant with no engine involvement;
  driving 10k reads through a socket would be strictly worse.

### F3: `has_tracking_clients()` and `has_any_tracking_clients()` are provably equivalent — the guard that made F1 look correct

- **Severity** 3 — not a bug on its own, but it is the *mechanism* by which F1 escaped review
  and will re-escape: every call site reading `has_tracking_clients()` looks mode-scoped and is
  not.
- **Likelihood** 4 — any future write path added by copy-paste from `worker.rs:732` inherits F1.
- **Effort** 1
- **Priority** 16
- **Evidence**: `shard/types.rs:433` `has_tracking_clients() = !invalidation_registry.is_empty()`;
  `:439` `has_any_tracking_clients() = has_tracking_clients() || !broadcast_table.is_empty()`.
  But `register_broadcast` (`:451`) calls `invalidation_registry.register(conn_id, conn)`
  *before* `broadcast_table.register(...)` — so a BCAST client always makes the registry
  non-empty. The second disjunct at `:440` is unreachable and the two predicates are the same
  function. Consequences: (a) `post_execution.rs:672`'s early-return guard is fine by accident;
  (b) `invalidate_keys_all_modes`'s inner `if self.has_tracking_clients()` (`:483`) is
  always-true dead branching; (c) `worker.rs:732` reads as "only if someone is tracking by key"
  and is actually "if anyone is tracking at all".
- **Proposed test**: unit test on `ShardTracking` asserting `register_broadcast` alone makes
  **both** predicates true and `unregister` makes both false — which documents the equivalence
  and forces a rename/removal (`has_tracking_clients` → `has_default_mode_tracking_clients`,
  implemented as `!tracking_table.is_empty()`, would make F1 a compile-visible mistake).
- **Boundary**: 1 (pure unit) — pure predicate algebra on `ShardTracking`.

### F4: Tracking-table capacity is a hardcoded 1M keys *per shard*, unconfigurable, and its eviction path is `single-test`

- **Severity** 4 — 4 shards × 1M `Bytes` keys + two index maps is multi-GB of untracked
  memory before a single invalidation-on-eviction fires; the operator has no knob to lower it.
- **Likelihood** 3 — needs a large tracked keyspace, but Redis ships
  `tracking-table-max-keys` precisely because operators hit this.
- **Effort** 3
- **Priority** 15
- **Evidence**: `crates/core/src/tracking.rs:17` `DEFAULT_TRACKING_TABLE_MAX_KEYS = 1_000_000`,
  wired at `shard/types.rs:425` **per `ShardTracking`** (per shard). Grep across `crates/`
  finds no `tracking-table-max-keys` config parameter — the only hit is
  `redis-regression/tests/tracking_tcl.rs:6`, which *excludes* the upstream tests on the
  grounds that `CONFIG SET tracking-table-max-keys` does not exist. `evict_lru` is
  `single-test`; nothing asserts that eviction delivers an invalidation (it does —
  `tracking.rs:212-218` — but that correctness is unpinned, and it is the *only* thing
  preventing eviction from being a silent stale read).
- **Proposed test**: unit test — `TrackingTable::with_max_keys(2)`, one registered conn,
  `record_read(a,b,c)`; assert exactly one invalidation for `a` was delivered and
  `key_to_clients` holds `{b,c}`. Plus a config test once the parameter exists, asserting the
  bound is per-shard-divided or documented as per-shard.
- **Boundary**: 1 (pure unit) for the eviction-invalidation invariant; 4 only for the config
  wiring once added.

### F5: FLUSHALL/FLUSHDB × BCAST tracking is untested, and correctness rests on a subtle registry-wide iteration

- **Severity** 4 — if the FLUSH branch stopped notifying BCAST clients, every BCAST client
  would serve its entire cache stale after a FLUSHALL. Currently correct, entirely unpinned.
- **Likelihood** 3 — FLUSHALL is a plausible ops event; the regression would be invisible.
- **Effort** 3
- **Priority** 15
- **Evidence**: `shard/post_execution.rs:679-686` branches
  `if has_flush && self.tracking.has_tracking_clients() { flush_all_tracking() } else {
  invalidate_keys_all_modes(&all_keys, ...) }`. `flush_all_tracking` (`types.rs:492`) calls
  only `tracking_table.flush_all(registry)` — which is correct for BCAST *only because*
  `flush_all` (`tracking.rs:180-184`) iterates `registry.connections` rather than
  `key_to_clients` ("*Send FlushAll to every registered connection (not just those with tracked
  keys)*"). Note the `else` branch is a trap: FLUSHALL extracts **zero** keys, so if the
  guard ever narrowed to default-mode-only (see F3), BCAST clients would fall into
  `invalidate_keys_all_modes(&[])`, which returns immediately at `types.rs:481`. Existing
  coverage: `integration_client.rs:1393 test_tracking_flushdb` — default mode only.
- **Proposed test**: RESP3 conn A `CLIENT TRACKING on BCAST PREFIX ""`, conn B `CLIENT TRACKING
  on` + `GET k`; write keys, `FLUSHALL` from conn C; assert **both** A and B receive a
  flush-style invalidation (RESP3 null-array invalidate), not just B.
- **Boundary**: 4 (server integration) — the `InvalidationMessage::FlushAll` → RESP3 null-array
  encoding is part of the contract being asserted, and lives in the connection layer.

### F6: Replica-side keyspace notifications and tracking invalidation are entirely untested

- **Severity** 3 — a read-scaling replica with keyspace notifications or tracking clients
  either double-fires, mis-fires, or silently fires nothing; a subscriber on a replica gets a
  wrong answer on a user-visible path.
- **Likelihood** 4 — subscribing to `__keyevent@0__:expired` on a replica is a standard
  cache-invalidation pattern, and replicas are ordinary deployment.
- **Effort** 3
- **Priority** 14
- **Evidence**: `server/src/replication/executor.rs` applies replicated commands through
  `CoreMsg::Execute` with `REPLICA_INTERNAL_CONN_ID`, so the whole post-execution pipeline —
  including `emit_keyspace_notifications_for_command` and `invalidate_written_keys` — runs on
  the replica. `rg 'keyevent|keyspace@|TRACKING' crates/server/tests/integration_replication.rs`
  → **0 hits**. Redis's actual semantics here are subtle (a replica does not expire keys itself;
  `expired` on a replica fires only when the primary's `DEL` arrives), and nothing pins which
  semantics FrogDB has chosen.
- **Proposed test**: `TestServer::start_primary` + `start_replica`; subscriber on the replica to
  `__keyevent@0__:set` and `:expired`; `SET k v PX 50` on the primary; assert the replica
  subscriber sees exactly one `set` and exactly one `expired`/`del` (whichever the chosen
  semantics are), and that the replica does **not** emit a second `expired` of its own.
- **Boundary**: 5 (multi-node harness) — replication is intrinsic to the behaviour; there is no
  lower level at which "what does a replica emit" is a meaningful question.

### F7: `HotkeySession::entries` is an unbounded `HashMap<Vec<u8>, HotkeyEntry>`

- **Severity** 3 — a HOTKEYS session with `SAMPLE 1` (or default) and no `DURATION` stores an
  entry per distinct key touched, for as long as the session runs; `DURATION 0` means unlimited.
- **Likelihood** 3 — HOTKEYS is a diagnostic an operator reaches for *during* an incident,
  which is exactly when the keyspace is churning.
- **Effort** 1
- **Priority** 14
- **Evidence**: `crates/core/src/hotkeys.rs` — `entries: HashMap<Vec<u8>, HotkeyEntry>` with no
  cap; `config.count` (validated 1..=100 at `server/src/connection/hotkeys.rs:~200`) bounds only
  the *reply* length, applied in `top_keys()` via `truncate(self.config.count)` after collecting
  and sorting the whole map. `DURATION` accepts any `u64` with 0 meaning unlimited.
- **Proposed test**: unit test — start a session with `count: 10`, record 100_000 distinct keys,
  assert a bounded-memory invariant (either `entries.len()` is capped by a count-derived
  reservoir, or `memory_usage()` stays under a stated bound). This test is expected to **fail**
  and drive a fix (count-min sketch / bounded top-K, which is what Redis's `--hotkeys` and
  DragonflyDB both do rather than an exact map).
- **Boundary**: 1 (pure unit) — pure accumulator behaviour, no engine.

### F8: `HOTKEYS ... METRIC <m>` is accepted but ranking always sorts by access count

- **Severity** 3 — an operator hunting a CPU hotspot with `METRIC cpu` is handed the
  most-*accessed* keys, a different key set. The reply even attaches `total-cpu-time-us` per
  key, which makes the wrong ranking look authoritative. Wrong answer on a user-visible path.
- **Likelihood** 3 — anyone who reads the docs and passes a non-default `METRIC`.
- **Effort** 2
- **Priority** 13
- **Evidence**: `crates/core/src/hotkeys.rs` —
  `top_keys()` does `entries.sort_by(|a, b| b.1.access_count.cmp(&a.1.access_count))` then
  `truncate(count)`, unconditionally, with no reference to the session's selected metric.
  `server/src/connection/hotkeys.rs:370-470` renders `total-cpu-time-us` / `total-net-bytes`
  onto that list. `integration_hotkeys.rs` (144 L) asserts reply *shape* only.
- **Proposed test**: drive a session where key `A` is accessed 100× cheaply and key `B` is
  accessed 3× expensively (large value → net bytes; or a costly command → cpu us); assert
  `METRIC accesses` ranks `A` first and `METRIC cpu`/`net` ranks `B` first.
- **Boundary**: 2 (crate-level API on `HotkeySession`) — the ranking is a pure function of
  recorded samples; feeding samples directly is deterministic, whereas driving real CPU-time
  differences through a socket is inherently flaky. Recommend a shape-only smoke test stays at
  level 4 and the ranking assertions move to level 2.

### F9: `INFO` hardcodes `pubsub_*` and `tracking_*` to `0` while the Prometheus gauges beside them carry real values

- **Severity** 2 — wrong operator-facing fields, but flatly contradicted by the same server's
  `/metrics`. Per repo convention, misleading observability is a real bug; the rubric caps it
  at 2.
- **Likelihood** 5 — default config, every `INFO` call.
- **Effort** 3
- **Priority** 13
- **Evidence**: `server/src/info/sections.rs` — `ClientsSection` emits `tracking_clients: 0`;
  the Stats section emits `pubsub_channels: 0`, `pubsub_patterns: 0`,
  `pubsubshard_channels: 0`, `tracking_total_keys/items/prefixes: 0` (with the comment
  "*FrogDB does not yet count tracked keys (previously this misreported the db size)*").
  Duplicated at `server/src/commands/info.rs:350-352` and `:363-365`. Meanwhile
  `shard/diagnostics.rs:376-390` feeds `PubsubChannels::set(.., subscriptions.unique_channel_count())`,
  `PubsubPatterns::set(.., unique_pattern_count())`, `PubsubSubscribers::set(..,
  total_subscription_count())` — the data exists and is already scatter-gatherable
  (`PubSubMsg::PubSubIntrospection` at `shard/dispatch_pubsub.rs:104` already serves
  `PUBSUB CHANNELS`/`NUMSUB`).
- **Proposed test**: subscribe 2 conns to 3 channels + 1 pattern, enable tracking on one;
  assert `INFO stats` `pubsub_channels == 3`, `pubsub_patterns == 1`, `INFO clients`
  `tracking_clients == 1`, and that `PUBSUB CHANNELS`'s length agrees with the INFO field.
  Add a cross-check that the Prometheus gauge for the same quantity matches the INFO field.
- **Boundary**: 4 (server integration) — INFO rendering plus the scatter-gather across shards
  is the behaviour; there is no lower level where "does INFO agree with PUBSUB CHANNELS" exists.

### F10: `slowlog-max-len` is enforced per shard — retention and `SLOWLOG LEN` scale with `num_shards`

- **Severity** 2 — `SLOWLOG LEN` over-reports up to `num_shards×` the configured bound, and
  slowlog memory is `num_shards×` what the operator budgeted.
- **Likelihood** 5 — default config: `TestServer` and the shipped default both use 4 shards,
  and any keyed workload spreads entries across all of them.
- **Effort** 3
- **Priority** 13
- **Evidence**: `SlowLog` is a per-`ShardObservability` field (`shard/types.rs:149`), capped by
  `set_max_len` applied per shard (`shard/dispatch_observability.rs:26`). `SLOWLOG LEN`
  (`server/src/connection/observability_conn_command.rs:229`) scatter-gathers and **sums**;
  `SLOWLOG GET` gathers, sorts by global monotonic `id` descending and truncates (correct
  ordering — the global `slowlog_next_id` at `shard/types.rs:652` makes the merge sound).
  The existing parity test `redis-regression/tests/slowlog_tcl.rs:136` asserts
  `SLOWLOG LEN == 10` after 100 `PING`s with `slowlog-max-len 10` — it passes only because
  `PING` is not key-routed and lands on one shard. `slowlog_tcl.rs:5` already acknowledges
  "*Each shard has its own...*".
- **Proposed test**: `CONFIG SET slowlog-log-slower-than 0`, `slowlog-max-len 10`,
  `SLOWLOG RESET`, then 100 `SET` commands on keys chosen to hash across all 4 shards; assert
  `SLOWLOG LEN` respects the documented semantics (either `== 10` after a fix that divides the
  budget, or a test that pins and documents `<= 10 * num_shards` deliberately).
- **Boundary**: 4 (server integration) — the defect is the aggregation across shards, which
  only exists above the shard layer. **This finding needs a semantics decision first** (see
  OPTIONS).
- **OPTIONS**:
  1. *Divide the budget* (`max_len / num_shards`, min 1) — restores Redis parity for LEN and
     memory; loses entries unevenly when the workload is skewed to one shard.
  2. *Keep per-shard budget, fix `SLOWLOG LEN` to report `min(sum, max_len)`* — LEN matches
     Redis, memory is still `num_shards×`.
  3. *Document per-shard semantics and pin it* — cheapest; leaves the memory surprise.
  **Recommendation: (1)**, matching the operator's mental model for a bounded diagnostic
  buffer; the test above then asserts `== 10` and the parity test at `slowlog_tcl.rs:136`
  becomes meaningful rather than accidentally green.

### F11: `PubsubSubscribers` gauge is documented as subscribers but fed subscription counts

- **Severity** 2 — a dashboard/alert on "pub/sub subscribers" reads 10 for one client
  subscribed to 10 channels. Misleading, and a capacity alert built on it fires at the wrong
  point.
- **Likelihood** 4 — any deployment scraping the shipped dashboard.
- **Effort** 1
- **Priority** 13
- **Evidence**: `crates/types/src/metrics/definitions.rs:262-296` documents
  `PubsubSubscribers` as "Total pub/sub subscribers per shard";
  `shard/diagnostics.rs:376-390` sets it from `self.subscriptions.total_subscription_count()`,
  which is `channel_subs` + `pattern_subs` + `sharded_subs` entry counts
  (`pubsub.rs:504-517` — each is a `(channel → conn → sender)` entry, so one conn on N channels
  contributes N).
- **Proposed test**: unit test on `ShardSubscriptions` — one conn subscribed to 3 channels and
  1 pattern; assert `unique_channel_count() == 3`, `unique_pattern_count() == 1`, and a
  (to-be-added) `unique_subscriber_count() == 1`, distinct from `total_subscription_count() == 4`.
  Then either rename the gauge or feed it the unique count.
- **Boundary**: 1 (pure unit) — pure counting over `ShardSubscriptions`.

### F12: `o` (`OVERWRITTEN`) and `c` (`TYPE_CHANGED`) event classes parse and configure but are never emitted

- **Severity** 2 — `CONFIG SET notify-keyspace-events "KEoc"` succeeds and `CONFIG GET` echoes
  it, so the operator believes a subscription is armed that will never fire. Round-1 issue 09
  residue.
- **Likelihood** 3 — a plausible ops event: anyone porting a Redis 7.x config that uses these
  classes.
- **Effort** 1
- **Priority** 11
- **Evidence**: `crates/core/src/keyspace_event.rs` defines `OVERWRITTEN` and `TYPE_CHANGED`
  and parses `o`/`c`. `rg 'OVERWRITTEN|TYPE_CHANGED' crates/ --glob '*.rs'` returns **only**
  `keyspace_event.rs` — zero emission sites, in contrast to `NEW` (emitted at
  `shard/keyspace_notify.rs:103`, gated by `new_events_enabled()`) and `MISS`, both of which
  round-1 issue 09 wired up. `keyspace_event.rs` is at 100% line coverage, which is exactly why
  percentage is not the signal here.
- **Proposed test**: `CONFIG SET notify-keyspace-events "KEA"` then `"KEoc"`; `SET k v1`;
  `SET k v2` (overwrite); `LPUSH k2 x` then `DEL k2; SET k2 v` (type change); assert the
  `overwritten` / `type_changed` keyevents arrive. Expected to fail and drive implementation —
  or, if these classes are deliberately unimplemented, a negative test asserting `CONFIG SET`
  *rejects* `o`/`c` so the config cannot silently lie.
- **Boundary**: 3 (`shard_driver` + the existing `notify_capture.rs` seam) — the capture seam
  at `core/tests/shard_driver/notify_capture.rs` already exists and gives exact emission-order
  assertions without a socket; this is precisely what it was built for.

### F13: Slow-subscriber overflow teardown is tested only for plain channel subscribers

- **Severity** 3 — an overflowed *invalidation* channel silently stops sending invalidations
  (`InvalidationRegistry` senders are `let _ = ...send(...)` everywhere:
  `tracking.rs:183, 216, 328`), which is a stale read rather than a disconnect. An overflowed
  pattern/sharded subscriber's teardown path is unexercised.
- **Likelihood** 2 — needs a genuinely slow consumer.
- **Effort** 3
- **Priority** 10
- **Evidence**: `server/src/connection.rs:577` handles `Drained::Overflowed` by
  `disconnect_overflowed_subscriber(); break;`. The one test for it,
  `integration_pubsub.rs:4691-4798`, uses a plain `SUBSCRIBE` and a shrunken budget (its doc
  comment carefully explains the socket-buffer determinism problem). Nothing covers
  `PSUBSCRIBE`/`SSUBSCRIBE` overflow, and the invalidation path does not share the
  `OutputBudget` machinery at all — its sends are unconditionally discarded on failure.
- **Proposed test**: (a) parameterise the existing overflow test over
  `SUBSCRIBE`/`PSUBSCRIBE`/`SSUBSCRIBE`; (b) new test: RESP3 tracking client that stops
  reading, flood invalidations, assert the client is **disconnected** rather than left
  silently un-invalidated (expected to fail — currently it is left stale).
- **Boundary**: 4 (server integration) — the behaviour is socket back-pressure; it does not
  exist below the connection layer. Reuse the shrunken-budget technique from `:4702-4717`.

### F14: `to_flag_string` never emits `A`, so `CONFIG GET notify-keyspace-events` diverges from what was `CONFIG SET`

- **Severity** 1 — cosmetic/parity; the semantics are preserved, the string is not.
- **Likelihood** 4 — anyone who sets `AKE` and reads it back, and any config-management tool
  that diffs desired vs. actual.
- **Effort** 1
- **Priority** 10
- **Evidence**: `crates/core/src/keyspace_event.rs` — `ALL_TYPES` (`A`) is
  `g$lshzxet` *excluding* `MODULE`; `to_flag_string` emits `K`/`E` first then each type flag
  individually and has no `A` collapse branch. Redis's `keyspaceEventsFlagsToString` emits `A`
  when the full `NOTIFY_ALL` set is present, so a config-drift detector sees a permanent diff.
- **Proposed test**: property/round-trip test — for every subset of flags,
  `from_string(to_flag_string(f)) == f` (already likely true), **plus** the specific pin
  `to_flag_string(parse("AKE")) == "AKE"` (or the documented alternative).
- **Boundary**: 1 (pure unit / proptest) — pure string↔bitflag algebra. A `proptest` over
  flag subsets is the right tool and is cheap; example tests would miss the `A`-collapse
  interaction with `m`/`n`.

### F15: `OutputBudget` overflow latch + wakeup has no interleaving test

- **Severity** 3 — a lost `overflow_notify` wakeup leaves a subscriber connection parked
  forever on `recv_or_overflow()` with a full budget: the client hangs instead of being torn
  down.
- **Likelihood** 2 — a narrow interleaving between a shard's `send` and the connection's drain.
- **Effort** 4
- **Priority** 9
- **Evidence**: `crates/core/src/pubsub.rs` — `OutputBudget` carries `queued_bytes`, a 32 MiB
  `hard_limit`, an `overflowed` latch and an `overflow_notify`; `PubSubSender::send` decides
  drop-vs-enqueue against `queued_bytes` while the receiver concurrently decrements it in
  `recv_or_overflow`. Every existing test drives this from a single task; the only end-to-end
  test (`integration_pubsub.rs:4691`) is explicitly written to make the outcome *deterministic*
  by shrinking the budget, i.e. it deliberately avoids the racy region.
- **Proposed test**: `shuttle` (or `loom`) model over one sender task and one receiver task
  sharing an `OutputBudget` with a 2-message limit; assert the invariant "either the receiver
  observes `Drained::Overflowed`, or `queued_bytes` returns to 0 and no message is lost" under
  all interleavings.
- **Boundary**: 2 (crate-level API) run under the existing `crates/testing/` shuttle harness —
  `OutputBudget`/`PubSubSender`/`PubSubReceiver` are a self-contained public trio; no shard or
  socket is needed, which is what makes exhaustive interleaving tractable.

## Deprioritised

- **LATENCY subsystem is inert** — `LatencyMonitor::record`/`record_with_timestamp` have no
  production caller (only `latest()`/`history()`/`reset()` are called, from
  `shard/dispatch_observability.rs:59-70`), and `latency-monitor-threshold` is a `NoopParam`
  (`server/src/runtime_config.rs:2361`). Already declared
  `intentional-incompatibility:observability` with an explicit exclusion list in
  `redis-regression/tests/latency_monitor_tcl.rs`. Deliberate non-feature, correctly
  documented — testing it would pin a decision, not catch a bug. (Worth one cheap negative
  test only if the team wants `latency-monitor-threshold` to *reject* rather than accept.)
- **`SlowLog::set_max_arg_len` / `SlowLog.max_arg_len` are dead code** — argument truncation
  actually happens at the connection layer (`server/src/connection/slowlog.rs:47-49` calls
  `SlowLog::truncate_args` with `config_manager.slowlog_max_arg_len()`) before the entry
  reaches the shard. The `untested` class on `set_max_arg_len` is correct and the fix is
  deletion, not a test.
- **`BroadcastTable::invalidate_matching` emits duplicate key entries under overlapping
  prefixes** (`tracking.rs:305-323` builds `conn_keys` by pushing one `Bytes` per matching
  prefix despite the "*deduplicating per connection*" comment) — unreachable in practice
  because `CLIENT TRACKING BCAST` rejects overlapping prefixes
  (`integration_client.rs:1544 test_tracking_bcast_overlap_with_accumulated_prefix_rejected`).
  Fix the comment; no test.
- **`NOLOOP` is ignored on the FLUSHALL path** — `flush_all` (`tracking.rs:180-184`) sends
  `FlushAll` to every registered connection without consulting `tracked.noloop`. Matches
  Redis's `trackingInvalidateKeysOnFlush`, which also notifies the caller. Correct as-is.
- **`unique_pattern_count()` allocates a `HashSet` per call** from
  `check_thresholds_after_subscribe` on every SUBSCRIBE — a perf smell, not a correctness gap;
  belongs in a benchmark, not a test.
- **Threshold-warning latches** (`warned_total_90`/`warned_channels_90`/`warned_patterns_90`) —
  I expected a stuck-latch bug; `dispatch_pubsub.rs:111` calls
  `reset_thresholds_if_needed()` on `ConnectionClosed`. Correct.
- **`ShardSubscriptions::remove_connection`** — checked for a leaked `PubSubSender` on
  disconnect; senders are stored inline in `channel_subs`/`pattern_subs`/`sharded_subs`
  (`pubsub.rs:504-517`) and all three are cleaned (`:770-785`). No leak. Similarly
  `in_pubsub_mode()` is `!subscriptions.is_empty() || !patterns.is_empty() ||
  !sharded_subscriptions.is_empty()`, so the `notify_connection_closed` fan-out guard
  (`server/src/connection/lifecycle.rs:190`) does not miss RESP3 subscribers.
- **Keyspace-notification drop under a saturated coordinator mailbox** — `Sharded` topology
  drops + counts (`shard/keyspace_coordinator.rs:106-116`) and has a genuine unit test
  asserting both the non-delivery and the metric (`:259-286`). An end-to-end saturation test
  would need a new fault primitive (effort 4-5) for a documented best-effort path.

## Cross-area notes

- **Shared infrastructure request — a tracking/invalidation capture seam in `shard_driver`.**
  `core/tests/shard_driver/notify_capture.rs` already provides exactly this for keyspace
  notifications, via the worker's `drive_capture_keyspace` seam, and it is why F12 can be
  tested at level 3. There is **no equivalent for client-side caching**: nothing at the
  shard-driver level can register a `TrackedConnection` / `InvalidationRegistry` entry, so
  F1, F5, and F13(b) are all forced to level 4 (server integration + RESP3 push parsing),
  which is slower and indirect for what is fundamentally a shard-worker call-site question.
  A `drive_register_tracking(conn_id, mode, prefixes) -> InvalidationReceiver` seam mirroring
  `drive_capture_keyspace` would move F1/F5 from effort 3 to effort 2 and make the whole
  eviction/expiry/flush invalidation matrix cheap to cover exhaustively. **Recommend the
  coordinator treat this as a shared prerequisite**, since the eviction and expiry agents
  almost certainly want the same seam.
- **`shard/` ownership overlap.** F1, F3, and F5 have their *defect* in
  `core/src/shard/{worker,types,post_execution}.rs`, which is another agent's crate region.
  They are reported here because the invariant being violated belongs to `tracking.rs` and the
  BCAST/default mode split is unintelligible without it. Flag to whoever owns `core/src/shard/`.
- **`server/src/info/` ownership.** F9's fix is in `server/src/info/sections.rs` and
  `server/src/commands/info.rs` (the hardcoded zeros are duplicated in both), but the data
  source is `pubsub.rs`/`tracking.rs`. Also worth a shared convention: **any `INFO` field and
  the Prometheus gauge claiming the same quantity should be cross-asserted in one test** —
  F9 and F11 are both instances of the two drifting, and the telemetry crate's tests
  (`crates/telemetry/tests/`) currently check exporter plumbing without ever comparing an
  exported value to the `INFO` field that claims the same thing.
- **Config-parameter inertness is a recurring pattern in this area** — `o`/`c` keyspace classes
  (F12), `latency-monitor-threshold` (deprioritised), missing `tracking-table-max-keys` (F4),
  dead `slowlog-max-arg-len` on the shard side (deprioritised). A generic
  "every advertised `CONFIG SET` value has an observable effect" audit would catch this class
  repo-wide and probably belongs to whoever owns `crates/config`.
