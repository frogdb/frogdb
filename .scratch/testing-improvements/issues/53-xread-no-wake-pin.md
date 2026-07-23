# Pin blocking-XREAD no-wake property for XTRIM/XDEL

Status: needs-triage
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

- [ ] Integration test: client blocks on `XREAD BLOCK <timeout> STREAMS s $` (or equivalent
      last-delivered-id form); a second connection issues `XTRIM` on the same stream; assert the
      blocked client remains blocked past a bounded wait window (no spurious wake).
- [ ] Sibling test (or extension) for `XDEL`: entries deleted from the stream while a client is
      blocked; assert no wake.
- [ ] Follow-up `XADD` from the second connection wakes the blocked client with exactly the newly
      added entry (not a stale or trimmed one).
- [ ] Test comments cite `core/src/shard/blocking.rs:198-199` as the documented wake-trigger list
      being pinned.

## Blocked by

None - can start immediately

## References

- .scratch/testing-improvements/audit/C-pubsub-streams.md (`blocking-xread-spurious-wake-on-xtrim-xdel-unpinned`)
- .scratch/testing-improvements/audit/verdicts-C.md (CONFIRMED L1/C2)
- core/src/shard/blocking.rs:198-199
- frogdb-server/crates/redis-regression/tests/stream_tcl.rs:1106,1169
