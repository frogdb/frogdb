# 15: Wrong-type drain covers plain XREAD waiters — no unleavable blocked state

Status: ready-for-agent

## Origin

Distsys-review MAJ-12 (`.scratch/formal-spec/2026-08-13-independent-distsys-review.md`),
ruled **accept-plus (fix semantics, not just the row)** by the user 2026-08-14
([rulings ledger](../../../formal-spec/2026-08-13-distsys-review-rulings.md)).

## What is wrong

TR-BLOCKING-019 claims "Every waiter on that key sharing the failing condition is
drained and replied to … rather than left parked to time out". The code contradicts it:
both drain arms (`core/src/shard/blocking.rs:493-505` `DrainWrongType`, `:512-519`
`DrainNoGroup`) loop only on `pop_oldest_xreadgroup_waiter` — plain `XREAD` waiters are
deliberately left parked (comments `:489-492`, `:510-511` say so).

Consequence: `XREAD BLOCK 0 STREAMS s $` then `SET s foo` parks the client forever —
no deadline, and nothing will ever re-signal that key as a stream. An unleavable
blocked state (compounded by CRIT-4/5 pre-restructure: unkillable too, except via
`CLIENT UNBLOCK`). The spec's false drain claim is exactly what hides it from review.

Redis comparison: Redis 7 replies `-UNBLOCKED the stream key no longer exists`
(`unblockDeletedStreamReadgroupClients`) — but only to readgroup clients; plain XREAD
waiters stay parked in Redis too. FrogDB replies `NOGROUP …`/`WRONGTYPE …`; neither
the text divergence nor the behavior is recorded in the deviations table.

## Ruled shape

Fix the semantics, don't just make the row honest:

- **Wrong-type drain covers all stream waiters on the key** — XREAD and XREADGROUP
  both — because a wrong-typed key makes *every* stream wait unsatisfiable. Reply
  with a pinned error text.
- **`DrainNoGroup` stays XREADGROUP-only** — plain XREAD needs no group; its wait
  remains satisfiable (group creation is irrelevant to it). This asymmetry is stated
  in the spec, not implied.
- Draining parked XREAD clients on type conflict is a deviation from Redis's
  park-forever behavior: document as deviation-as-improvement (no unleavable
  blocked state), including the error-text difference from Redis's `-UNBLOCKED …`.

## What to build (spec-first)

1. Split TR-BLOCKING-019 into per-arm postconditions:
   - Wrong-type: all stream waiters on the key (XREAD + XREADGROUP) drained, error
     text pinned verbatim in the row.
   - NoGroup: XREADGROUP waiters for the missing group drained; plain XREAD waiters
     explicitly stay parked (still satisfiable); error text pinned.
2. Code: extend the `DrainWrongType` arm to pop plain XREAD waiters too (new or
   generalized pop beside `pop_oldest_xreadgroup_waiter`); reply path per the pinned
   text.
3. Forcing tests (each arm, currently `DrainWrongType` has none):
   - `XREAD BLOCK` waiter + `SET` on the key → waiter drained with pinned error
     (fails pre-fix: waiter stays parked).
   - `XREADGROUP BLOCK` waiter + wrong-type overwrite → drained (pins existing
     behavior).
   - `XREAD BLOCK` waiter + `XGROUP DESTROY`-shaped NoGroup condition → waiter
     *stays parked* and is satisfied by a later `XADD` (pins the asymmetry).
4. Deviations table row: behavior + error-text divergence from Redis, improvement
   rationale.

## Cross-references

- [Issue 13](13-blocking-wait-becomes-a-run-loop-state.md): same machinery — the
  CRIT-4/5 restructure moves the wait into the run loop. Land 13 first or coordinate;
  the drain-scope fix must survive the restructure (drain = state transition, not
  channel push, post-13).
- [Issue 08](08-blocking-command-rows.md): blocking row family; keep row vocabulary
  consistent.

## Acceptance criteria

- [ ] TR-BLOCKING-019 split; error texts pinned; `just lint-spec` green
- [ ] Wrong-type drain pops XREAD + XREADGROUP waiters; NoGroup unchanged scope
- [ ] Forcing tests fail pre-fix where marked, pass post-fix
- [ ] Deviations table row landed

## Blocked by

None to start the spec rows; coordinate code with
[issue 13](13-blocking-wait-becomes-a-run-loop-state.md) if in flight.
