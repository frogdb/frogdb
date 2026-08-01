# Unbounded RESP nesting depth on both decode and encode — stack overflow aborts the process

Status: needs-triage
Type: AFK
Origin: round-2 testing audit 2026-07-28 — 15 parallel area audits, `.scratch/testing-improvements-round2/`
Source: proposals/08 F4 · `MASTER.md` §3 (availability / resource) — split from issue 70
Score: severity 4 · likelihood 2 · effort 2 · priority 14
Area: frogdb-server connection codec / frogdb-protocol response encoding

## Context

Both the RESP decode and encode paths recurse on nesting depth with no bound. A client sending
sufficiently nested arrays, or a Lua script returning a sufficiently nested table, overflows the
stack — which is a `SIGSEGV`/abort, not an unwind, so it takes the whole process down rather than
failing one connection. Redis is structurally immune here: `processMultibulkBuffer` is iterative
and rejects a non-`$` element outright, so nesting is *unrepresentable* in a client request.

**This is a suspected live defect found by reading, not by test failure — the proposed test fails
against today's code.** The evidence is the auditing agent's and needs confirmation before or
during the fix.

Split out of issue 70, `.scratch/testing-improvements-round2/issues/`, which carries the other
four sites from the same `MASTER.md` §3 row. This is the only one of the five with an unresolved
`OPTIONS:` block; the other four are `ready-for-agent` and should not wait on this decision.

## Evidence

The codec's `*` handling validates only the element *count* and then falls through —
`crates/server/src/connection/codec.rs:141-163` — and `scan_for_oversized_bulk` bails out on a
non-`$` element (`codec.rs:262`: `if pos >= buf.len() || buf[pos] != b'$' { return None; }`), so
nested `*` elements go straight to `self.inner.decode(src)` at `codec.rs:207`. Upstream is plainly
recursive with no depth parameter (`redis-protocol-6.0.0/src/resp2/decode.rs`:
`d_parse_array_frames` → `nom_count(d_parse_frame, len)`).

Encode has the same shape: `crates/protocol/src/response.rs:234` and `:255-262` recurse through
`Array`/`Map`/`Set` with no depth bound, reachable from a Lua script returning a deeply nested
table (`crates/core/src/scripting/executor.rs:393`).

Stack overflow is `SIGSEGV`/abort, not an unwind.

## Options

Reproduced verbatim from proposals/08 F4:

1. *Bounded-stack thread unit test* (recommended). Deterministic, fast, no process risk.
   Cost: the 256 KB figure is a magic number that must be documented, and it proves
   "bounded" only relative to that stack.
2. *`#[should_panic]`/abort-capturing integration test.* Honest about the real failure
   mode but a stack overflow aborts rather than panics, so it needs a subprocess
   harness — new infrastructure for one assertion.
3. *Fuzz-only* — let the `FrogDbResp2` fuzz target from F5 find it. Zero incremental
   cost, but non-deterministic and gives no regression pin.

**Recommendation: (1) as the pin, with (3) as the ongoing net.**

The decision is which of these to adopt — and, if (2), whether the subprocess harness is worth
building for one assertion or should wait for I2 (issue 02, same directory), which builds a
subprocess crash primitive anyway.

## What to fix

1. Add a depth cap to the decode path. The natural home is the codec's own `*` handling
   (`codec.rs:141-163`), which already walks the frame — the upstream crate takes no depth
   parameter, so the bound has to live on this side.
2. Add the mirrored cap to the encode path (`response.rs:234`, `:255-262`), which is reachable
   from script output and is not covered by any decode-side guard.
3. Pick the depth limit deliberately and document it against the stack size it assumes.

## Acceptance criteria

- [ ] On a `stack_size(256 * 1024)` thread, `"*1\r\n".repeat(N)` makes `decode` return
      `Err(DecodeError)` rather than aborting. Red today.
- [ ] The mirrored assertion holds for `to_resp2_frame` and `to_resp3_frame`. Red today.
- [ ] A Lua script returning a table nested past the cap gets a clean error, and the *next*
      command on that shard still works — i.e. the shard worker survives.
- [ ] The chosen depth limit is a named constant with a comment stating the stack size it is
      safe against, not a bare literal.
- [ ] The option selected above is recorded in this issue with its rationale.

## Test boundary

Level 1 — a bounded-stack thread over the decode/encode functions directly. Deliberately *not*
level 4: a socket-level version risks killing the test harness process itself, which is the whole
difficulty this finding describes.

## Depends on

Nothing hard. Issue 10, `.scratch/testing-improvements-round2/issues/` (fuzz CI) is the ongoing
net under option (3); issue 02, same directory (subprocess-SIGKILL primitive) would supply the
harness option (2) needs. Sibling: issue 70, same directory.
