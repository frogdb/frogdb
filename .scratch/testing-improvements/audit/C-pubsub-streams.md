# Area C: Pub/Sub / Streams / Blocking / Keyspace Notifications (agent report, verified evidence)

### `keyspace-new-and-keymiss-events-never-emitted`
`n` (new-key) + `m` (keymiss) classes parse (`crates/core/src/keyspace_event.rs:72-74`, round-trip :133-139, unit :272-274) so `CONFIG SET notify-keyspace-events Knm` succeeds — but NO emission site anywhere in crates/commands/src or crates/core/src (grep all emit callers + NEW/MISS refs → nothing). Accepted-but-inert; no test asserts firing or non-firing.
- expected (Redis 8.6): keymiss on read miss (6.0+); `new` on first creation (7.4+). Implement or reject flags — silent swallow = surprise.
- tests: integration KEm + GET missing → assert event (or flag rejected, pinning policy); KEn + SET newkey.
- **L3 / C2.**

### `keyspace-stream-event-class-t-unasserted`
Stream-class events ARE emitted (commands/src/stream/basic.rs:28-30 xadd; :271; :320; consumer_groups.rs:29) but integration_pubsub.rs covers $/g/l/s/h/z/x only, never stream events; pubsub_tcl.rs:22 excluded upstream stream-events test as intentional-incompatibility:config. Regression stopping XADD emission unnoticed.
- tests: subscribe keyspace+keyevent under KEA; run each stream mutator; assert names (xadd/xtrim/xsetid/xgroup-create...). Port excluded upstream test.
- **L2 / C2.**

### `pubsub-subscription-cap-enforcement-untested`
admit_subscriptions (server/src/connection/state.rs:685-731): LimitReached at cap, one-shot 80% warning latch, consulted in subscribe_kind (pubsub_conn_command.rs:315-321), distinct errors per kind. Zero tests (grep error strings/LimitReached/crossed_80 in server tests + redis-regression → empty). Untested subtleties: (a) all-or-nothing batch admission (SUBSCRIBE a b c w/ 2 slots rejects all); (b) admit counts args.len() incl. duplicates but add_subscription inserts HashSet → re-subscribing held channel can trip limit without growing set.
- tests: unit (at-limit, one-below, 80% once, unsubscribe re-arm); integration per-kind cap w/ exact error + prior subs survive; duplicate-at-limit.
- **L3 / C2.**

### `resp2-resp3-subscribe-mode-command-gate-underspecified`
test_pubsub_mode_restrictions (integration_pubsub.rs:768) asserts only RESP2 GET-rejected + PING-works. Gate (guards.rs:196-213): RESP3 branch returns true for EVERY command (:197-199) — untested; RESP2 allow-set = strategy PubSub|ConnectionState + PING/QUIT — boundary untested; RESET accepted+exits (error text :322 advertises) untested.
- expected: RESP2 subscribed = 9 commands only; RESP3 = all allowed + push delivery.
- tests: RESP3 subscribed SET/GET normal replies while receiving pushes; RESP2 RESET exits mode; representative data command rejected.
- **L2 / C2.**

### `pubsub-slow-subscriber-unbounded-no-output-buffer-limit`
PubSubSender = mpsc::UnboundedSender (core/src/pubsub.rs:22); per-conn receiver unbounded (pubsub_conn_command.rs:270). No client-output-buffer-limit config (grep config+server → empty), no slow-subscriber test. Stalled subscriber → unbounded server memory (DoS). (Cross-shard keyspace-notification hop IS bounded: try_send+drop+KeyspaceNotificationsDropped metric, unit test keyspace_coordinator.rs:261 — client-facing path is the unguarded one.)
- expected: Redis client-output-buffer-limit pubsub 32mb 8mb 60 → disconnect. Least surprise: bound + disconnect/drop.
- tests: integration/turmoil — non-reading subscriber + flood; assert bounded memory / disconnect. Part feature-gap, part test-gap; test pins current policy either way.
- **L2 / C3.**

### `subscriber-disconnect-cross-shard-deregistration-untested`
ShardSubscriptions::remove_connection unit-tested on single map (core/src/pubsub.rs:517, test :911). Real disconnect must deregister broadcast (shard 0) + sharded (per owning shard). No integration test: kill/close subscriber then assert PUBSUB CHANNELS/NUMSUB/SHARDNUMSUB drop + PUBLISH count falls (grep → empty).
- tests: subscriber w/ broadcast+pattern+sharded across ≥2 shards; CLIENT KILL + ungraceful-close variants; assert zero counts all shards.
- **L2 / C2.**

### `multiwaiter-multikey-exactly-once-no-deterministic-reproducer`
Known issue-11 MultiWaiter loss has NO deterministic/shuttle regression guard. seed_sweep_nightly pins OPS_PER_CLIENT=75 w/ comment that ≥~90 fails nearly every seed (concurrency_workload.rs:216-222). multi_waiter_exact_fifo_is_clean (:143) only proves checker wiring at low ops. No shuttle model targets multi-key wake/pop interleaving (shuttle already linked: core/Cargo.toml:15, server/Cargo.toml:19). Once issue 11 fixed, nothing prevents silent regression. NOT a re-file of bug — this is the missing deterministic test.
- tests: shuttle — N waiters overlapping key sets (BLMPOP), concurrent pushes, exactly-once conservation + FIFO wake over all interleavings; raise nightly cap once green.
- **L3 / C3.**

### `blocking-xread-spurious-wake-on-xtrim-xdel-unpinned`
try_satisfy_stream_waiters documented to run after XADD/DEL/UNLINK/SET/XGROUP DESTROY/RENAME (core/src/shard/blocking.rs:198-199) — deliberately not XTRIM/XDEL. Correct-by-construction, zero tests pinning no-wake.
- tests: block XREAD $; XTRIM/XDEL from other client; assert still blocked; XADD wakes w/ exactly new entry.
- **L1 / C2.**

### `blocking-multikey-crossslot-rejection-untested`
blocking_nil_shape_regression.rs:103 hash-tags keys onto one slot "so the command validates" — negative path never asserted. Only CROSSSLOT test = MSET (integration_cluster.rs:7736). No blocking multi-key CROSSSLOT coverage.
- tests: cluster BLPOP/BLMPOP/BZMPOP/BLMOVE keys in different slots → immediate CROSSSLOT, no block.
- **L2 / C2.**

## Verified NOT gaps
Stream ID edges/overflow/XSETID/NOMKSTREAM (stream_tcl.rs); XAUTOCLAIM cursor + tombstones (stream_cgroups_tcl.rs:2093,:2427); XDELEX/XACKDEL (integration_streams.rs:498-1060); single-key blocking FIFO (list_tcl.rs:2426, zset_tcl.rs:2407); CLIENT UNBLOCK both modes (list_tcl.rs:2758-2818); MULTI/Lua nil for blocking (multi_regression.rs:517, scripting_tcl.rs:462); per-publisher ordering+conservation turmoil (concurrency_pubsub.rs + pubsub_oracle.rs); sharded-pubsub slot-migration SUNSUBSCRIBE (integration_pubsub.rs:1274).
