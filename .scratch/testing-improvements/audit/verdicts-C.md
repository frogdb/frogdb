# Verdicts C (pubsub/streams/blocking/notifications)
keyspace-new-and-keymiss-events-never-emitted: CONFIRMED L3/C2. Exhaustive grep: no EventSpec carries NEW/MISS; notify_event only GENERIC/LIST/SET/STRING/ZSET; no read-path keymiss site. Redis versions check out (keymiss 6.0, new 7.4). Distinct from issues 09/10.
keyspace-stream-event-class-t-unasserted: CONFIRMED L2/C2 (leans L1 — declarative specs — but defensible).
pubsub-subscription-cap-enforcement-untested: ADJUSTED L2/C1. "Zero tests" FALSE — state.rs unit tests subscribe_limit_and_80pct_latch + subscribe_rejects_over_limit exist (finder missed in-crate unit tests). Missing: integration error text, all-or-nothing batch admission, duplicate-counts quirk.
resp2-resp3-subscribe-mode-command-gate-underspecified: CONFIRMED L2/C2. RESP3 tests assert reply shape only; is_allowed_in_pubsub_mode no unit test; RESET property tests never enter pubsub mode.
pubsub-slow-subscriber-unbounded-no-output-buffer-limit: ADJUSTED L2/C2. Refutation failed — no output-buffer-limit (INFO stub =0); writer awaits full socket while publishers send() into unbounded mpsc → unbounded memory. C2 per rubric (availability/DoS, not data loss).
subscriber-disconnect-cross-shard-deregistration-untested: CONFIRMED L2/C2. Real ungraceful-close path (lifecycle.rs:191-193 ConnectionClosed broadcast to all shards) has no integration assertion.
multiwaiter-multikey-exactly-once-no-deterministic-reproducer: CONFIRMED L3/C3. Genuinely additive to issue 11: shuttle waiter tests (concurrency.rs:1516-1654) use MockStreamWaitQueue single-key mock, not real ShardWorker multi-key BLMPOP path; issue 11 has no deterministic post-fix guard (nightly capped at 75 < threshold).
blocking-xread-spurious-wake-on-xtrim-xdel-unpinned: CONFIRMED L1/C2 (stream_tcl 1106/1169 cover net-zero XADD+DEL tx, not standalone XTRIM/XDEL non-wake).
blocking-multikey-crossslot-rejection-untested: CONFIRMED L2/C2.
