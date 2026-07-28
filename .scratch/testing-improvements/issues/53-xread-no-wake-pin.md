# Pin blocking-XREAD no-wake property for XTRIM/XDEL

Status: done
Type: AFK
Origin: testing-gap audit 2026-07-22 (multi-agent static review + adversarial verification; coverage run on testbox)
Severity: likelihood 1/3, consequence 2/3 (score 2)
Area: Pub/Sub / Streams

## Context

`try_satisfy_stream_waiters` is documented to run only after XADD, DEL, UNLINK, SET, `XGROUP
DESTROY`, and RENAME (`core/src/shard/blocking.rs:198-199`) — deliberately **not** after `XTRIM` or
`XDEL`. This is correct by construction: trimming or deleting existing entries never produces a new
entry that a client blocked on `XREAD $` (or similar) should wake up for. However, zero tests pin
this no-wake property, so a future change that adds XTRIM/XDEL to the wake-trigger list would
silently reintroduce spurious wakes with nothing catching it.

Existing coverage (`stream_tcl.rs:1106,1169`) only covers net-zero XADD+DEL transactions bundled
together, not a standalone XTRIM or XDEL against a blocked reader. Verdict CONFIRMED L1/C2.

## What to build

Integration tests that block a client on `XREAD BLOCK` against a stream, then perform XTRIM and XDEL
from a second connection and assert the blocked client does **not** wake; finally perform an XADD and
assert the client wakes with exactly the new entry.

## Acceptance criteria

- [x] Integration test: client blocks on `XREAD BLOCK <timeout> STREAMS s $` (or equivalent
      last-delivered-id form); a second connection issues `XTRIM` on the same stream; assert the
      blocked client remains blocked past a bounded wait window (no spurious wake).
- [x] Sibling test (or extension) for `XDEL`: entries deleted from the stream while a client is
      blocked; assert no wake.
- [x] Follow-up `XADD` from the second connection wakes the blocked client with exactly the newly
      added entry (not a stale or trimmed one).
- [x] Test comments cite `core/src/shard/blocking.rs:198-199` as the documented wake-trigger list
      being pinned.

## Blocked by

None - can start immediately

## References

- .scratch/testing-improvements/audit/C-pubsub-streams.md (`blocking-xread-spurious-wake-on-xtrim-xdel-unpinned`)
- .scratch/testing-improvements/audit/verdicts-C.md (CONFIRMED L1/C2)
- core/src/shard/blocking.rs:198-199
- frogdb-server/crates/redis-regression/tests/stream_tcl.rs:1106,1169

## Resolution

Verified the property is correct-by-construction and previously untested, exactly as the audit
found — no divergence, no fix needed. Confirmed at the command-spec level (not just the
`try_satisfy_stream_waiters` doc comment):

- `XaddCommand::spec()` (`frogdb-server/crates/commands/src/stream/basic.rs`) sets
  `wakes: WaiterWake::Kind(WaiterKind::Stream)`.
- `XdelCommand::spec()` and `XtrimCommand::spec()` (same file) both set `wakes: WaiterWake::None`
  — neither is wired to `satisfy_waiters_for_command` /
  `try_satisfy_stream_waiters` (`frogdb-server/crates/core/src/shard/blocking.rs`,
  `frogdb-server/crates/core/src/shard/post_execution.rs:701-726`).

Added integration tests pinning the no-wake side of the property plus the follow-up wake:

- `frogdb-server/crates/redis-regression/tests/stream_tcl.rs`:
  - `tcl_xread_xtrim_should_not_awake_client` — blocks `XREAD BLOCK 20000 STREAMS s1trim $`,
    `XTRIM`s the stream from a second connection (trimming 4 of 5 entries), asserts the blocker
    is still blocked after a bounded 200ms wait and `blocked_client_count() == 1`, then `XADD`s
    and asserts the blocker wakes with exactly the new entry.
  - `tcl_xread_xdel_should_not_awake_client` — same shape for `XDEL` (deletes a middle entry by
    ID), asserting no wake, then a follow-up `XADD` wakes with exactly the new entry.
- `frogdb-server/crates/redis-regression/tests/stream_cgroups_tcl.rs`:
  - `tcl_xreadgroup_xtrim_xdel_should_not_awake_client` — XREADGROUP BLOCK analogue: blocks
    `XREADGROUP GROUP mygroup Alice BLOCK 0 STREAMS s1cg >`, exercises both `XTRIM` and `XDEL`
    against existing entries in sequence (neither wakes it, checked via bounded wait +
    `blocked_client_count()`), then `XADD` wakes it with exactly the new entry.

All three tests cite `try_satisfy_stream_waiters` / `core/src/shard/blocking.rs` in their doc
comments as the documented wake-trigger list being pinned (line numbers in that file have since
drifted from the 198-199 cited at filing time, so the tests reference the function/doc-comment
rather than a stale line number).

Test runs: `just test frogdb-redis-regression "stream_tcl::|stream_cgroups_tcl::"` — 3 consecutive
runs, all `114 tests run: 114 passed, 2176 skipped` (no flakes). `just fmt frogdb-redis-regression`
made no changes; `cargo clippy -p frogdb-redis-regression --tests -- -D warnings` clean.

No divergence found — XTRIM/XDEL do not wake blocked XREAD/XREADGROUP waiters, matching Redis
semantics. No production code changes required.
