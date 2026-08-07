# core state / pubsub / observability — residual test gaps (11 findings)

Status: ready-for-agent
Type: AFK
Origin: round-2 testing audit 2026-07-28 — 15 parallel area audits, `.scratch/testing-improvements-round2/`
Source: proposals/02 — residual findings after promotion to issues 19–76
Score: 11 findings, priority range 9–15
Area: frogdb-core — `client_registry/**`, `pubsub.rs`, `tracking.rs`, `keyspace_event.rs`, `keyspace_stats.rs`, `hotkeys.rs`, `slowlog.rs`, `latency*.rs`, `metrics.rs`, `probes.rs`, `observability/**`

## Context

This area is `frogdb-core`'s client-facing state and telemetry: the client registry, pub/sub
subscription tables and output budgets, client-side-caching (tracking) tables in both default and
BCAST modes, keyspace-event flags and stats, hotkeys sampling, the slowlog, latency histograms and
the metrics/probes plumbing. **Line coverage for the area is 93.7 %** (3878 covered regions) over
deduplicated depth classes `well-covered` 151, `monoculture` 41, `untested` 30, `single-test` 17,
`covered` 5 — and the proposal is explicit that "percentage is not the signal": `keyspace_event.rs`
sits at 100 % line coverage while two of its event classes have zero emission sites. The proposal's
verdict on the shape of that coverage: the bug that escapes today is "**a correct-looking write
path that silently skips one of the two tracking modes, or an operator-facing counter that is
confidently zero**", with two tracking/observability structures unbounded by construction beside it.

## Promoted elsewhere

- F1 → issue 54, `.scratch/testing-improvements-round2/issues/` (BCAST client-side-caching trackers
  never invalidated on lazy expiry — `worker.rs:732,758` call the default-mode-only
  `invalidate_keys`).
- F3 → issue 54, `.scratch/testing-improvements-round2/issues/` (same defect —
  `has_tracking_clients()`/`has_any_tracking_clients()` are provably the same function and are the
  camouflage that let F1 escape review).
- F2 → issue 66, `.scratch/testing-improvements-round2/issues/` (`TrackingTable::lru_order` grows
  without bound; compaction lives only inside `evict_lru`, gated at 1M keys/shard).
- F12 → issue 21, `.scratch/testing-improvements-round2/issues/` (theme T3 — config that parses,
  sets and does nothing; the `o` `OVERWRITTEN` and `c` `TYPE_CHANGED` keyspace-event classes).

## Residual findings

### F4 — Tracking-table capacity is a hardcoded 1M keys *per shard*, unconfigurable, and its eviction path is `single-test`

- **Severity** 4 — 4 shards × 1M `Bytes` keys + two index maps is multi-GB of untracked memory before a single invalidation-on-eviction fires; the operator has no knob to lower it.
- **Likelihood** 3 — needs a large tracked keyspace, but Redis ships `tracking-table-max-keys` precisely because operators hit this.
- **Effort** 3
- **Priority** 15
- **Evidence**: `crates/core/src/tracking.rs:17` `DEFAULT_TRACKING_TABLE_MAX_KEYS = 1_000_000`, wired at `shard/types.rs:425` **per `ShardTracking`** (per shard). Grep across `crates/` finds no `tracking-table-max-keys` config parameter — the only hit is `redis-regression/tests/tracking_tcl.rs:6`, which *excludes* the upstream tests on the grounds that `CONFIG SET tracking-table-max-keys` does not exist. `evict_lru` is `single-test`; nothing asserts that eviction delivers an invalidation (it does — `tracking.rs:212-218` — but that correctness is unpinned, and it is the *only* thing preventing eviction from being a silent stale read).
- **Proposed test**: unit test — `TrackingTable::with_max_keys(2)`, one registered conn, `record_read(a,b,c)`; assert exactly one invalidation for `a` was delivered and `key_to_clients` holds `{b,c}`. Plus a config test once the parameter exists, asserting the bound is per-shard-divided or documented as per-shard.
- **Boundary**: 1 (pure unit) for the eviction-invalidation invariant; 4 only for the config wiring once added.

### F5 — FLUSHALL/FLUSHDB × BCAST tracking is untested, and correctness rests on a subtle registry-wide iteration

- **Severity** 4 — if the FLUSH branch stopped notifying BCAST clients, every BCAST client would serve its entire cache stale after a FLUSHALL. Currently correct, entirely unpinned.
- **Likelihood** 3 — FLUSHALL is a plausible ops event; the regression would be invisible.
- **Effort** 3
- **Priority** 15
- **Evidence**: `shard/post_execution.rs:679-686` branches `if has_flush && self.tracking.has_tracking_clients() { flush_all_tracking() } else { invalidate_keys_all_modes(&all_keys, ...) }`. `flush_all_tracking` (`types.rs:492`) calls only `tracking_table.flush_all(registry)` — which is correct for BCAST *only because* `flush_all` (`tracking.rs:180-184`) iterates `registry.connections` rather than `key_to_clients` ("*Send FlushAll to every registered connection (not just those with tracked keys)*"). Note the `else` branch is a trap: FLUSHALL extracts **zero** keys, so if the guard ever narrowed to default-mode-only (see F3), BCAST clients would fall into `invalidate_keys_all_modes(&[])`, which returns immediately at `types.rs:481`. Existing coverage: `integration_client.rs:1393 test_tracking_flushdb` — default mode only.
- **Proposed test**: RESP3 conn A `CLIENT TRACKING on BCAST PREFIX ""`, conn B `CLIENT TRACKING on` + `GET k`; write keys, `FLUSHALL` from conn C; assert **both** A and B receive a flush-style invalidation (RESP3 null-array invalidate), not just B.
- **Boundary**: 4 (server integration) — the `InvalidationMessage::FlushAll` → RESP3 null-array encoding is part of the contract being asserted, and lives in the connection layer.

### F6 — Replica-side keyspace notifications and tracking invalidation are entirely untested

- **Severity** 3 — a read-scaling replica with keyspace notifications or tracking clients either double-fires, mis-fires, or silently fires nothing; a subscriber on a replica gets a wrong answer on a user-visible path.
- **Likelihood** 4 — subscribing to `__keyevent@0__:expired` on a replica is a standard cache-invalidation pattern, and replicas are ordinary deployment.
- **Effort** 3
- **Priority** 14
- **Evidence**: `server/src/replication/executor.rs` applies replicated commands through `CoreMsg::Execute` with `REPLICA_INTERNAL_CONN_ID`, so the whole post-execution pipeline — including `emit_keyspace_notifications_for_command` and `invalidate_written_keys` — runs on the replica. `rg 'keyevent|keyspace@|TRACKING' crates/server/tests/integration_replication.rs` → **0 hits**. Redis's actual semantics here are subtle (a replica does not expire keys itself; `expired` on a replica fires only when the primary's `DEL` arrives), and nothing pins which semantics FrogDB has chosen.
- **Proposed test**: `TestServer::start_primary` + `start_replica`; subscriber on the replica to `__keyevent@0__:set` and `:expired`; `SET k v PX 50` on the primary; assert the replica subscriber sees exactly one `set` and exactly one `expired`/`del` (whichever the chosen semantics are), and that the replica does **not** emit a second `expired` of its own.
- **Boundary**: 5 (multi-node harness) — replication is intrinsic to the behaviour; there is no lower level at which "what does a replica emit" is a meaningful question.

### F7 — `HotkeySession::entries` is an unbounded `HashMap<Vec<u8>, HotkeyEntry>`

- **Severity** 3 — a HOTKEYS session with `SAMPLE 1` (or default) and no `DURATION` stores an entry per distinct key touched, for as long as the session runs; `DURATION 0` means unlimited.
- **Likelihood** 3 — HOTKEYS is a diagnostic an operator reaches for *during* an incident, which is exactly when the keyspace is churning.
- **Effort** 1
- **Priority** 14
- **Evidence**: `crates/core/src/hotkeys.rs` — `entries: HashMap<Vec<u8>, HotkeyEntry>` with no cap; `config.count` (validated 1..=100 at `server/src/connection/hotkeys.rs:~200`) bounds only the *reply* length, applied in `top_keys()` via `truncate(self.config.count)` after collecting and sorting the whole map. `DURATION` accepts any `u64` with 0 meaning unlimited.
- **Proposed test**: unit test — start a session with `count: 10`, record 100_000 distinct keys, assert a bounded-memory invariant (either `entries.len()` is capped by a count-derived reservoir, or `memory_usage()` stays under a stated bound). This test is expected to **fail** and drive a fix (count-min sketch / bounded top-K, which is what Redis's `--hotkeys` and DragonflyDB both do rather than an exact map).
- **Boundary**: 1 (pure unit) — pure accumulator behaviour, no engine.

### F8 — `HOTKEYS ... METRIC <m>` is accepted but ranking always sorts by access count

- **Severity** 3 — an operator hunting a CPU hotspot with `METRIC cpu` is handed the most-*accessed* keys, a different key set. The reply even attaches `total-cpu-time-us` per key, which makes the wrong ranking look authoritative. Wrong answer on a user-visible path.
- **Likelihood** 3 — anyone who reads the docs and passes a non-default `METRIC`.
- **Effort** 2
- **Priority** 13
- **Evidence**: `crates/core/src/hotkeys.rs` — `top_keys()` does `entries.sort_by(|a, b| b.1.access_count.cmp(&a.1.access_count))` then `truncate(count)`, unconditionally, with no reference to the session's selected metric. `server/src/connection/hotkeys.rs:370-470` renders `total-cpu-time-us` / `total-net-bytes` onto that list. `integration_hotkeys.rs` (144 L) asserts reply *shape* only.
- **Proposed test**: drive a session where key `A` is accessed 100× cheaply and key `B` is accessed 3× expensively (large value → net bytes; or a costly command → cpu us); assert `METRIC accesses` ranks `A` first and `METRIC cpu`/`net` ranks `B` first.
- **Boundary**: 2 (crate-level API on `HotkeySession`) — the ranking is a pure function of recorded samples; feeding samples directly is deterministic, whereas driving real CPU-time differences through a socket is inherently flaky. Recommend a shape-only smoke test stays at level 4 and the ranking assertions move to level 2.

### F9 — `INFO` hardcodes `pubsub_*` and `tracking_*` to `0` while the Prometheus gauges beside them carry real values

- **Severity** 2 — wrong operator-facing fields, but flatly contradicted by the same server's `/metrics`. Per repo convention, misleading observability is a real bug; the rubric caps it at 2.
- **Likelihood** 5 — default config, every `INFO` call.
- **Effort** 3
- **Priority** 13
- **Evidence**: `server/src/info/sections.rs` — `ClientsSection` emits `tracking_clients: 0`; the Stats section emits `pubsub_channels: 0`, `pubsub_patterns: 0`, `pubsubshard_channels: 0`, `tracking_total_keys/items/prefixes: 0` (with the comment "*FrogDB does not yet count tracked keys (previously this misreported the db size)*"). Duplicated at `server/src/commands/info.rs:350-352` and `:363-365`. Meanwhile `shard/diagnostics.rs:376-390` feeds `PubsubChannels::set(.., subscriptions.unique_channel_count())`, `PubsubPatterns::set(.., unique_pattern_count())`, `PubsubSubscribers::set(.., total_subscription_count())` — the data exists and is already scatter-gatherable (`PubSubMsg::PubSubIntrospection` at `shard/dispatch_pubsub.rs:104` already serves `PUBSUB CHANNELS`/`NUMSUB`).
- **Proposed test**: subscribe 2 conns to 3 channels + 1 pattern, enable tracking on one; assert `INFO stats` `pubsub_channels == 3`, `pubsub_patterns == 1`, `INFO clients` `tracking_clients == 1`, and that `PUBSUB CHANNELS`'s length agrees with the INFO field. Add a cross-check that the Prometheus gauge for the same quantity matches the INFO field.
- **Boundary**: 4 (server integration) — INFO rendering plus the scatter-gather across shards is the behaviour; there is no lower level where "does INFO agree with PUBSUB CHANNELS" exists.

### F10 — `slowlog-max-len` is enforced per shard — retention and `SLOWLOG LEN` scale with `num_shards`

**BLOCKED on the `slowlog-max-len` per-shard vs global semantics call** — `MASTER.md` §7 lists this
among the decisions requiring a semantics call before its test can assert anything; the decision
issues are 29–32, `.scratch/testing-improvements-round2/issues/`. Record the chosen option on this
issue before writing the test; the finding itself stays actionable and `ready-for-agent`.

- **Severity** 2 — `SLOWLOG LEN` over-reports up to `num_shards×` the configured bound, and slowlog memory is `num_shards×` what the operator budgeted.
- **Likelihood** 5 — default config: `TestServer` and the shipped default both use 4 shards, and any keyed workload spreads entries across all of them.
- **Effort** 3
- **Priority** 13
- **Evidence**: `SlowLog` is a per-`ShardObservability` field (`shard/types.rs:149`), capped by `set_max_len` applied per shard (`shard/dispatch_observability.rs:26`). `SLOWLOG LEN` (`server/src/connection/observability_conn_command.rs:229`) scatter-gathers and **sums**; `SLOWLOG GET` gathers, sorts by global monotonic `id` descending and truncates (correct ordering — the global `slowlog_next_id` at `shard/types.rs:652` makes the merge sound). The existing parity test `redis-regression/tests/slowlog_tcl.rs:136` asserts `SLOWLOG LEN == 10` after 100 `PING`s with `slowlog-max-len 10` — it passes only because `PING` is not key-routed and lands on one shard. `slowlog_tcl.rs:5` already acknowledges "*Each shard has its own...*".
- **Proposed test**: `CONFIG SET slowlog-log-slower-than 0`, `slowlog-max-len 10`, `SLOWLOG RESET`, then 100 `SET` commands on keys chosen to hash across all 4 shards; assert `SLOWLOG LEN` respects the documented semantics (either `== 10` after a fix that divides the budget, or a test that pins and documents `<= 10 * num_shards` deliberately).
- **Boundary**: 4 (server integration) — the defect is the aggregation across shards, which only exists above the shard layer. **This finding needs a semantics decision first** (see OPTIONS).
- **OPTIONS**:
  1. *Divide the budget* (`max_len / num_shards`, min 1) — restores Redis parity for LEN and memory; loses entries unevenly when the workload is skewed to one shard.
  2. *Keep per-shard budget, fix `SLOWLOG LEN` to report `min(sum, max_len)`* — LEN matches Redis, memory is still `num_shards×`.
  3. *Document per-shard semantics and pin it* — cheapest; leaves the memory surprise.

  **Recommendation: (1)**, matching the operator's mental model for a bounded diagnostic buffer; the test above then asserts `== 10` and the parity test at `slowlog_tcl.rs:136` becomes meaningful rather than accidentally green.

### F11 — `PubsubSubscribers` gauge is documented as subscribers but fed subscription counts

- **Severity** 2 — a dashboard/alert on "pub/sub subscribers" reads 10 for one client subscribed to 10 channels. Misleading, and a capacity alert built on it fires at the wrong point.
- **Likelihood** 4 — any deployment scraping the shipped dashboard.
- **Effort** 1
- **Priority** 13
- **Evidence**: `crates/types/src/metrics/definitions.rs:262-296` documents `PubsubSubscribers` as "Total pub/sub subscribers per shard"; `shard/diagnostics.rs:376-390` sets it from `self.subscriptions.total_subscription_count()`, which is `channel_subs` + `pattern_subs` + `sharded_subs` entry counts (`pubsub.rs:504-517` — each is a `(channel → conn → sender)` entry, so one conn on N channels contributes N).
- **Proposed test**: unit test on `ShardSubscriptions` — one conn subscribed to 3 channels and 1 pattern; assert `unique_channel_count() == 3`, `unique_pattern_count() == 1`, and a (to-be-added) `unique_subscriber_count() == 1`, distinct from `total_subscription_count() == 4`. Then either rename the gauge or feed it the unique count.
- **Boundary**: 1 (pure unit) — pure counting over `ShardSubscriptions`.

### F13 — Slow-subscriber overflow teardown is tested only for plain channel subscribers

- **Severity** 3 — an overflowed *invalidation* channel silently stops sending invalidations (`InvalidationRegistry` senders are `let _ = ...send(...)` everywhere: `tracking.rs:183, 216, 328`), which is a stale read rather than a disconnect. An overflowed pattern/sharded subscriber's teardown path is unexercised.
- **Likelihood** 2 — needs a genuinely slow consumer.
- **Effort** 3
- **Priority** 10
- **Evidence**: `server/src/connection.rs:577` handles `Drained::Overflowed` by `disconnect_overflowed_subscriber(); break;`. The one test for it, `integration_pubsub.rs:4691-4798`, uses a plain `SUBSCRIBE` and a shrunken budget (its doc comment carefully explains the socket-buffer determinism problem). Nothing covers `PSUBSCRIBE`/`SSUBSCRIBE` overflow, and the invalidation path does not share the `OutputBudget` machinery at all — its sends are unconditionally discarded on failure.
- **Proposed test**: (a) parameterise the existing overflow test over `SUBSCRIBE`/`PSUBSCRIBE`/`SSUBSCRIBE`; (b) new test: RESP3 tracking client that stops reading, flood invalidations, assert the client is **disconnected** rather than left silently un-invalidated (expected to fail — currently it is left stale).
- **Boundary**: 4 (server integration) — the behaviour is socket back-pressure; it does not exist below the connection layer. Reuse the shrunken-budget technique from `:4702-4717`.

### F14 — `to_flag_string` never emits `A`, so `CONFIG GET notify-keyspace-events` diverges from what was `CONFIG SET`

- **Severity** 1 — cosmetic/parity; the semantics are preserved, the string is not.
- **Likelihood** 4 — anyone who sets `AKE` and reads it back, and any config-management tool that diffs desired vs. actual.
- **Effort** 1
- **Priority** 10
- **Evidence**: `crates/core/src/keyspace_event.rs` — `ALL_TYPES` (`A`) is `g$lshzxet` *excluding* `MODULE`; `to_flag_string` emits `K`/`E` first then each type flag individually and has no `A` collapse branch. Redis's `keyspaceEventsFlagsToString` emits `A` when the full `NOTIFY_ALL` set is present, so a config-drift detector sees a permanent diff.
- **Proposed test**: property/round-trip test — for every subset of flags, `from_string(to_flag_string(f)) == f` (already likely true), **plus** the specific pin `to_flag_string(parse("AKE")) == "AKE"` (or the documented alternative).
- **Boundary**: 1 (pure unit / proptest) — pure string↔bitflag algebra. A `proptest` over flag subsets is the right tool and is cheap; example tests would miss the `A`-collapse interaction with `m`/`n`.

### F15 — `OutputBudget` overflow latch + wakeup has no interleaving test

- **Severity** 3 — a lost `overflow_notify` wakeup leaves a subscriber connection parked forever on `recv_or_overflow()` with a full budget: the client hangs instead of being torn down.
- **Likelihood** 2 — a narrow interleaving between a shard's `send` and the connection's drain.
- **Effort** 4
- **Priority** 9
- **Evidence**: `crates/core/src/pubsub.rs` — `OutputBudget` carries `queued_bytes`, a 32 MiB `hard_limit`, an `overflowed` latch and an `overflow_notify`; `PubSubSender::send` decides drop-vs-enqueue against `queued_bytes` while the receiver concurrently decrements it in `recv_or_overflow`. Every existing test drives this from a single task; the only end-to-end test (`integration_pubsub.rs:4691`) is explicitly written to make the outcome *deterministic* by shrinking the budget, i.e. it deliberately avoids the racy region.
- **Proposed test**: `shuttle` (or `loom`) model over one sender task and one receiver task sharing an `OutputBudget` with a 2-message limit; assert the invariant "either the receiver observes `Drained::Overflowed`, or `queued_bytes` returns to 0 and no message is lost" under all interleavings.
- **Boundary**: 2 (crate-level API) run under the existing `crates/testing/` shuttle harness — `OutputBudget`/`PubSubSender`/`PubSubReceiver` are a self-contained public trio; no shard or socket is needed, which is what makes exhaustive interleaving tractable.

## Acceptance criteria

- [ ] F4: a unit test asserts that `TrackingTable::with_max_keys(2)` + `record_read(a,b,c)` delivers exactly one invalidation, for `a`, and leaves `key_to_clients == {b,c}`; plus a config test once `tracking-table-max-keys` exists asserting the bound is per-shard-divided or documented as per-shard.
- [ ] F5: a test asserts that after `FLUSHALL`, **both** a BCAST-tracking RESP3 connection and a default-mode tracking connection receive a flush-style (RESP3 null-array) invalidation.
- [ ] F6: a test asserts what a replica emits for `SET k v PX 50` applied from the primary — exactly one `set` keyevent and exactly one `expired`/`del` per the chosen semantics, and no second replica-originated `expired`.
- [ ] F7: a unit test asserts a bounded-memory invariant on `HotkeySession` after recording 100_000 distinct keys with `count: 10` (capped `entries.len()` or `memory_usage()` under a stated bound) — expected to fail against today's unbounded `HashMap`.
- [ ] F8: a test asserts `METRIC accesses` ranks the frequently-but-cheaply-accessed key first while `METRIC cpu`/`net` ranks the rarely-but-expensively-accessed key first.
- [ ] F9: a test asserts `INFO stats` `pubsub_channels == 3` / `pubsub_patterns == 1` and `INFO clients` `tracking_clients == 1` for a known subscriber/tracker set, that `PUBSUB CHANNELS`'s length agrees, and that the Prometheus gauge for the same quantity matches the INFO field.
- [ ] F10: a test asserts `SLOWLOG LEN` against the *decided* semantics after 100 `SET`s spread across all shards with `slowlog-max-len 10` — `== 10` under option 1/2, or an explicit `<= 10 * num_shards` pin under option 3 — with the chosen option recorded on this issue first.
- [ ] F11: a unit test on `ShardSubscriptions` asserts `unique_channel_count() == 3`, `unique_pattern_count() == 1` and `unique_subscriber_count() == 1` for one connection on 3 channels + 1 pattern, distinct from `total_subscription_count() == 4`, and the `PubsubSubscribers` gauge is fed the unique count (or renamed).
- [ ] F13: the overflow-teardown test is parameterised over `SUBSCRIBE`/`PSUBSCRIBE`/`SSUBSCRIBE`, and a further test asserts a non-reading RESP3 tracking client is disconnected rather than left silently un-invalidated.
- [ ] F14: a proptest asserts `from_string(to_flag_string(f)) == f` over flag subsets **and** the pin `to_flag_string(parse("AKE")) == "AKE"`.
- [ ] F15: a shuttle/loom model over one sender and one receiver sharing a 2-message `OutputBudget` asserts that under all interleavings either the receiver observes `Drained::Overflowed` or `queued_bytes` returns to 0 with no message lost.

## Depends on

- Infrastructure I1 (`shard_driver` harness extension — specifically
  `drive_register_tracking(conn_id, mode, prefixes) -> InvalidationReceiver`, a fifth `drive_*`
  seam mirroring the existing `drive_capture_keyspace`) — issue 01,
  `.scratch/testing-improvements-round2/issues/`. Needed by F5 and F13(b), which are forced to
  level 4 (server integration + RESP3 push parsing) today because nothing at the shard-driver level
  can register a `TrackedConnection` / `InvalidationRegistry` entry. The proposal recommends the
  coordinator treat this as a shared prerequisite; it would move F5 from effort 3 to effort 2 and
  make the whole eviction/expiry/flush invalidation matrix cheap to cover exhaustively.

## Re-triage 2026-08-06

**Verdict: still-valid** — 0/11 findings discharged.

| finding | verdict |
|---|---|
| F4 tracking-table capacity hardcoded 1M/shard | still-valid |
| F5 FLUSHALL/FLUSHDB × BCAST tracking untested | still-valid |
| F6 replica-side notifications + tracking invalidation untested | still-valid |
| F7 `HotkeySession::entries` unbounded | still-valid |
| F8 `HOTKEYS METRIC <m>` accepted but ignored | still-valid |
| F9 INFO hardcodes `pubsub_*` / `tracking_*` to 0 | still-valid |
| F10 `slowlog-max-len` enforced per shard | still-valid |
| F11 `PubsubSubscribers` gauge fed subscription counts | still-valid |
| F13 slow-subscriber teardown only for channel subscribers | still-valid |
| F14 `to_flag_string` never emits `A` | still-valid |
| F15 `OutputBudget` overflow latch + wakeup no interleaving test | still-valid |

Nothing in the hardening campaign touched this surface: core was not one of the four locked areas
and no FM row in `.scratch/hardening/specs/` covers tracking, hotkeys, slowlog or the observability
gauges. Spot-verified on today's tree: F9's fabricated constants are still literal — `INFO` emits
`tracking_clients` 0 at `crates/server/src/info/sections.rs:132` and `pubsub_channels` 0 at `:351`
(line drift only from the body's cited numbers); F14's `to_flag_string`
(`crates/core/src/keyspace_event.rs:97-121`) still pushes only `K E g $ l s h z …` and has no `A`
fold-up branch. The clock-seam sweep (2fb1051c, 0fe2dd0a) and the driven shard ticks (e7827926,
f475839c) changed how time and sweeps are *driven* in this crate but discharge none of these
findings, all of which are about state accounting and reported values.
