# 38 — Two live RESP3 wire incompatibilities in the zset reply shapes (A1 legacy range trio, A3 ZADD INCR)

Status: needs-triage

## What to build

Reply shape in FrogDB is decided independently at 27 handler sites in `frogdb-commands`. Two of
those independent decisions disagree with upstream Redis on the wire. Proposal 94 rev 1 rated
all three of its in-tree asymmetries as "pinned asymmetries, preserved not fixed"; the
adversarial review **re-rated A1 and A3 as LIVE Redis-compat defects** — A1 because nothing in
the tree pins the FrogDB behavior at all (so it is an unnoticed incompatibility, not a
preserved decision), and A3 because the thing that pins it is a golden row whose own doc
comment concedes the divergence. A2 (`ZSCORE` vs `ZINCRBY` on non-finite scores) stays as a
pinned-both-ways asymmetry and is deliberately **not** in scope here.

**A1 — the three legacy range commands are flat in RESP3 where `ZRANGE` is nested.**
`ZRANGE … WITHSCORES` branches on the connection protocol at
`frogdb-server/crates/commands/src/sorted_set/range.rs:125-129` and emits nested
`[member, Double(score)]` pairs under RESP3. Its three legacy siblings call `scored_array`
unconditionally and emit a flat member/score array in **both** protocols:
`ZRANGEBYSCORE` at `range.rs:185`, `ZREVRANGE` at `:234`, `ZREVRANGEBYSCORE` at `:291`.
Upstream nests all four: `genericZrangebyscoreCommand` and friends compute
`should_emit_array_length = (resp > 2 && withscores)` from the **connection**, not from the
command, so `ZREVRANGEBYSCORE key max min WITHSCORES` on a RESP3 connection is an array of
pairs in Redis and a flat array in FrogDB. No test in the tree pins the FrogDB shape — the
RESP3 zset pins in `redis-regression/tests/zset_tcl.rs` cover ZINTER/ZRANDMEMBER/ZPOP*/ZMPOP,
not the legacy range trio — so a client library that decodes RESP3 pairs structurally gets the
wrong result today, silently.

**A3 — `ZADD … INCR` never reads the protocol version.**
`frogdb-server/crates/commands/src/sorted_set/basic.rs:131` is
`Ok(Response::bulk(Bytes::from(format_float(new_score))))`, unconditionally, with no
`ctx.protocol_version` consultation anywhere in the INCR branch. So `ZADD k INCR 3 m` is
`$1\r\n3\r\n` on a RESP3 connection while the semantically identical `ZINCRBY k 3 m`
(`basic.rs:410`) is `,3\r\n`. Upstream `zaddGenericCommand` calls `addReplyDouble`, which
emits `,3\r\n` under RESP3. The FrogDB behavior is pinned by
`frogdb-server/tests/resp3.rs:890-895` (`test_zadd_incr_resp3_finite_wire_bytes`), and that
pin's own doc comment says it is *"flagged … as a RESP3-consistency gap relative to ZINCRBY,
out of scope for this encoder/dispatch-change-free pin task"* — i.e. it pins the divergence
knowingly, in the wrong direction.

Both are LIVE on main today for any RESP3 client, and both are one-line fixes at the cited
sites: A1 = branch on `is_resp3 && with_scores` into `scored_array_resp3` at `:185`, `:234`,
`:291` exactly as `:125` already does; A3 = emit `Response::Double(new_score)` under RESP3
(subject to the same finiteness policy `ZINCRBY` uses today, so A2 is not perturbed). Fixing
A3 flips the `resp3.rs:890-895` golden row and its neighbours at `:897` / `:914`, which must be
updated with a comment recording that the pin was deliberately inverted. Fixing A1 adds golden
rows where none exist. Proposal 94's fold (one shape authority in the two encoders) makes both
asymmetries structurally inexpressible, but it explicitly does **not** fix them — it preserves
current behavior byte-for-byte — so these must land as their own change, in either order
relative to 94.

## Acceptance criteria

- [ ] **A1**: on a RESP3 connection, `ZRANGEBYSCORE`/`ZREVRANGE`/`ZREVRANGEBYSCORE` with
      `WITHSCORES` reply with nested `[member, Double(score)]` pairs, matching `ZRANGE` and
      upstream `should_emit_array_length = (resp > 2 && withscores)`. RESP2 output is
      byte-identical to today.
- [ ] **A3**: on a RESP3 connection, `ZADD key INCR <n> <member>` replies with a RESP3 double
      (`,3\r\n`), matching `ZINCRBY`. RESP2 output is byte-identical to today.
- [ ] Wire-bytes regression `test_zrange_family_resp3_withscores_nested_wire_bytes` in
      `frogdb-server/tests/resp3.rs` covering all four range commands × RESP2/RESP3 ×
      WITHSCORES/no-WITHSCORES. The three legacy rows fail at HEAD.
- [ ] `test_zadd_incr_resp3_finite_wire_bytes` (`frogdb-server/tests/resp3.rs:890-895`) is
      inverted to assert `,3\r\n`, with a comment recording that the previous assertion pinned
      a known divergence.
- [ ] A2 (`ZSCORE` +inf bulk vs `ZINCRBY` +inf Double) is untouched — `resp3.rs:858` and
      `:775` still pass unchanged.
- [ ] `just test frogdb-server resp3` green

## Blocked by

None - can start immediately

## Source

Round 38-99 adversarial review of proposal 94
(`.scratch/arch-deepening/proposals/94-resp3-shape-once.md`), §Problem 3 defects **A1** and
**A3**, re-rated from "pinned asymmetry" to LIVE compat defect by the review (proposal
§Problem 3 "Filing note for the orchestrator").

## Comments
