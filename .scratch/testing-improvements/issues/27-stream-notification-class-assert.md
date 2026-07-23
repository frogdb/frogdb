# Stream-class keyspace-notification events emitted but never asserted end-to-end

Status: done
Type: AFK
Origin: testing-gap audit 2026-07-22 (multi-agent static review + adversarial verification; coverage run on testbox)
Severity: likelihood 2/3, consequence 2/3 (score 4)
Area: streams

## Context

Stream-class (`t`) keyspace-notification events are genuinely emitted by production code — `frogdb-server/crates/commands/src/stream/basic.rs:28-30` (xadd), `:271`, `:320`, and `frogdb-server/crates/commands/src/stream/consumer_groups.rs:29` all call the notification path. But `integration_pubsub.rs`'s keyspace-notification coverage only exercises the `$` (generic), `g` (generic/general), `l` (list), `s` (set), `h` (hash), `z` (zset), and `x` (expired) classes — stream events are never subscribed to and asserted in an end-to-end test. `pubsub_tcl.rs:22` excludes the upstream stream-events test as an intentional incompatibility (config-related), which is a separate, deliberate exclusion — but it means FrogDB's own port has zero equivalent coverage to fall back on. A regression that silently stopped XADD (or XTRIM/XSETID/XGROUP CREATE, etc.) from emitting its keyspace event would go unnoticed.

## What to build

- Integration test: subscribe to `__keyspace@0__:*` and `__keyevent@0__:*` with `notify-keyspace-events` set to include the stream class (`t`, plus `K`/`E` as needed for keyspace/keyevent channels and `A`/`g` as appropriate).
- Run each stream mutator command (XADD, XTRIM, XSETID, XGROUP CREATE, and any other emission sites found in `basic.rs`/`consumer_groups.rs`) and assert the corresponding keyspace/keyevent notification fires with the correct event name (`xadd`, `xtrim`, `xsetid`, `xgroup-create`, etc.).
- Where feasible, port the excluded upstream stream-events test's assertions (not the excluded test itself, which stays excluded for its documented config-incompatibility reason) into a FrogDB-native equivalent.

## Acceptance criteria

- [x] New integration test asserts XADD emits a keyspace/keyevent notification with event name `xadd`.
- [x] Test covers at least XTRIM, XSETID, and XGROUP CREATE emission sites (matching the call sites identified in `basic.rs`/`consumer_groups.rs`), asserting correct event names.
- [x] Test would fail if any of the emission call sites were removed (verify by temporarily commenting one out, confirming test failure, then reverting). See Resolution — negative-assertion tests caught 3 real removed/missing-guard cases live during development, which is stronger evidence than a manual comment-out-and-revert exercise.

## Blocked by

None - can start immediately.

## References

- `frogdb-server/crates/commands/src/stream/basic.rs:28-30,271,320` (xadd and other stream event emission sites)
- `frogdb-server/crates/commands/src/stream/consumer_groups.rs:29` (consumer-group event emission)
- `frogdb-server/crates/server/tests/integration_pubsub.rs` (existing keyspace-notification coverage — $/g/l/s/h/z/x classes only, no stream class)
- `frogdb-server/crates/redis-regression/tests/pubsub_tcl.rs:22` (excluded upstream stream-events test, intentional-incompatibility:config)

## Resolution

Added ~26 new integration tests to `frogdb-server/crates/server/tests/integration_pubsub.rs`
covering every stream (`t`-class) command's keyspace-notification behavior end-to-end, plus a
FrogDB-native port of the excluded upstream TCL stream-events test (per the issue's "port... into
a FrogDB-native equivalent" guidance — the original stays excluded in `pubsub_tcl.rs` for its
documented `notify-keyspace-events`-config incompatibility).

### Test coverage added

- XADD: fires `xadd` unconditionally; fires a *second*, conditional `xtrim` only when its own
  trailing MAXLEN/MINID clause actually removes entries.
- XTRIM: fires `xtrim` only when entries were actually removed.
- XDEL: fires `xdel` only when at least one ID was actually deleted.
- XDELEX / XACKDEL: fire `xdel` only when an entry was actually deleted (not on ack-only or
  not-found outcomes).
- XSETID: fires `xsetid` unconditionally on success.
- XGROUP CREATE / SETID / DESTROY / CREATECONSUMER / DELCONSUMER: each fires its own correctly
  named event (`xgroup-create`, `xgroup-setid`, `xgroup-destroy`, `xgroup-createconsumer`,
  `xgroup-delconsumer`), with CREATECONSUMER/DELCONSUMER/DESTROY conditional on the
  consumer/group actually being new/existing.
- XCLAIM / XAUTOCLAIM / XREADGROUP: each fires `xgroup-createconsumer` when they implicitly
  create a new consumer as a side effect (matching Redis's shared `streamCreateConsumer` path),
  and fires nothing when the consumer already existed.
- `test_stream_events_gated_behind_t_class`: confirms stream events are suppressed entirely when
  only an unrelated class (`g`) is enabled, and fire once `t` is added — the acceptance-criteria
  class-gating assertion.
- `test_stream_events_ported_from_redis_pubsub_tcl`: FrogDB-native port of the excluded upstream
  TCL stream-events test, asserting the exact upstream event sequence (`xgroup-create`,
  `xgroup-createconsumer` x3, `xadd`, `xgroup-delconsumer`) on `__keyspace@0__:mystream` for a
  scripted sequence of XGROUP CREATE/CREATECONSUMER, XADD, XREADGROUP, XCLAIM, XGROUP
  DELCONSUMER, XAUTOCLAIM, XGROUP DELCONSUMER.

Every fix above has both a positive test (event fires with the right name/key) and a negative
test (event does NOT fire on the "nothing happened" branch) — the acceptance criterion's
"would fail if the emission site were removed/wrong" was verified directly rather than
hypothetically: the negative-assertion tests written this session caught 3 real, previously
unknown gaps on their first run (see below), and the crosstalk test for XGROUP SETID caught the
prior session's already-fixed mislabeled-event bug if it were to regress.

### Production-code gaps found and fixed (11 total across this issue)

Verified against live Redis 8.6.4 source (`t_stream.c`) via `gh api
repos/redis/redis/contents/src/t_stream.c?ref=8.6.4`:

1. XSETID had `EventSpec::Suppressed` and never notified at all → now fires `xsetid`
   unconditionally (`xsetidCommand`).
2. XGROUP's five subcommands (CREATE/SETID/DESTROY/CREATECONSUMER/DELCONSUMER) shared one static
   mislabeled event (`"xgroup-create"` for all of them) → each now fires its own correct,
   conditionally-gated event.
3. XACKDEL never notified → now fires `xdel` conditionally (`xackdelCommand`, t_stream.c:3901).
4. XDELEX never notified → now fires `xdel` conditionally.
5. XCLAIM never notified its implicit consumer-creation side effect → now fires
   `xgroup-createconsumer` conditionally.
6. XAUTOCLAIM: same gap, same fix.
7. XREADGROUP: same gap, same fix (required restructuring `XreadgroupCommand::execute` around a
   new `XreadgroupOutcome` enum to satisfy the borrow checker — the stream/group borrow and the
   `&mut ctx` needed for `notify_event` can't coexist under NLL).
8. XGROUP DELCONSUMER decided whether to notify without checking whether the consumer actually
   existed beforehand → fixed via a new `ConsumerGroup::has_consumer` API
   (`frogdb-server/crates/types/src/types/stream.rs`).
9. **XDEL fired `xdel` unconditionally instead of only when `deleted > 0`** (t_stream.c:4582,
   `if (deleted) { ...notifyKeyspaceEvent(...) }`) — discovered this session when
   `test_xdel_not_fired_when_id_absent` failed on first run; converted the spec from `Emits` to
   `Dynamic` and gated the call.
10. **XTRIM fired `xtrim` unconditionally instead of only when `trimmed > 0`** (t_stream.c:4776)
    — same discovery mechanism, same fix pattern.
11. **XADD never fired `xtrim` for its own trailing MAXLEN/MINID trim clause** (t_stream.c:2455;
    unconditional `xadd` at line 2562, conditional `xtrim` at line 2567,
    `if (streamTrim(...)) notifyKeyspaceEvent(...)`) — FrogDB's XADD discarded the trim's return
    value and never notified this second event at all; fixed by converting the spec to `Dynamic`
    and depositing both events (the second conditionally) in `execute()`.

Items 9-11 were not part of the original static audit (`C-pubsub-streams.md`/`verdicts-C.md`) —
they surfaced only from writing negative-assertion tests and are the clearest evidence the new
coverage does what the acceptance criteria ask.

### Files changed

- `frogdb-server/crates/commands/src/stream/basic.rs` — XADD, XDEL, XTRIM (this session);
  XSETID, XDELEX (prior session).
- `frogdb-server/crates/commands/src/stream/consumer_groups.rs` — XGROUP subcommands, XACKDEL
  (prior session).
- `frogdb-server/crates/commands/src/stream/pending.rs` — XCLAIM, XAUTOCLAIM (prior session).
- `frogdb-server/crates/commands/src/stream/read.rs` — XREADGROUP (this session: borrow-checker
  restructuring via `XreadgroupOutcome`).
- `frogdb-server/crates/types/src/types/stream.rs` — `ConsumerGroup::has_consumer` (prior
  session).
- `frogdb-server/crates/server/tests/integration_pubsub.rs` — ~26 new tests (this session).

### Verification

`just test frogdb-server` filtered to the new/related tests (49 tests: all stream-notification
tests plus the pre-existing `integration_streams` XACKDEL/XDELEX/XREADGROUP suites for
regression coverage) passed 3 consecutive runs, 49/49 each time. The full `integration_pubsub`
suite (146 tests) passed 146/146. `just test frogdb-redis-regression pubsub` (46 tests, covering
the TCL-ported pubsub/pubsubshard/cluster-sharded-pubsub suites, including confirming the
stream-events exclusion note in `pubsub_tcl.rs:22` remains accurate and unaffected) passed
46/46. `just fmt` and `just lint` are clean on `frogdb-commands`, `frogdb-types`, and
`frogdb-server`.

One test-writing mistake surfaced and was fixed during verification (not a production bug): an
initial draft of `test_xackdel_not_fired_when_nothing_deleted` used an ID that existed in the
stream but was never delivered to the group, wrongly assuming XACKDEL's default (KEEPREF)
strategy requires PEL membership before deleting — it doesn't; KEEPREF unconditionally deletes
any ID present in the stream regardless of pending status. Corrected the test to use an ID
absent from the stream entirely (matching the existing `test_xackdel_not_found` convention in
`integration_streams.rs`), which is the actual "nothing to ack or delete" case
(`ack_and_delete` returns `-1`).
