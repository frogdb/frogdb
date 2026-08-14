# 16: Deny-blocking contexts return the op-aware nil — MULTI/EXEC shape fixed, rowed

Status: ready-for-agent

## Origin

Distsys-review MAJ-13 (`.scratch/formal-spec/2026-08-13-independent-distsys-review.md`),
ruled accept-and-file by the user 2026-08-14
([rulings ledger](../../../formal-spec/2026-08-13-distsys-review-rulings.md)).

## What is wrong

A blocking command executed where it cannot block (inside MULTI/EXEC, inside Lua) has
no spec row, and the code path is wrong for the RESP shape. The conversion at
`frogdb-server/crates/core/src/shard/execution.rs:623-630`:

```rust
let response = if matches!(&response, Response::BlockingNeeded { .. }) {
    Response::Null
} else { response };
```

collapses every op to `Response::Null` (`$-1`), discarding the `op` sitting in the
matched variant. `MULTI; BLPOP k 1; EXEC` on an empty list returns `$-1` where Redis
returns `*-1` — a third instance of the wrong-shape family FM-BLOCKING-002 polices,
on a path FM-BLOCKING-002's `into_response` fix does not reach.

Redis comparison: under `CLIENT_DENY_BLOCKING`, `blpop`/`brpop`/`bzpopmin`/
`XREAD BLOCK` call `addReplyNullArray`; only `BLMOVE`/`BRPOPLPUSH` call
`addReplyNull`. Op-awareness is preserved exactly as FM-BLOCKING-002 requires.

The Lua path (`scripting/bindings.rs:184`, `Boolean(false)`) is confirmed correct —
Lua flattens both nil shapes; do not touch it.

## What to build (spec-first)

1. TR row in `specs/blocking.md`: "a blocking command in a deny-blocking context
   (MULTI/EXEC, Lua) resolves immediately with the op-aware timeout nil, registers no
   `WaitEntry`, and sets no blocked flag." Cite FM-BLOCKING-002 as the shape
   authority.
2. Code: `op.timeout_reply()` at the conversion site — the op is already in scope in
   the matched `BlockingNeeded` variant.
3. Forcing tests, one per op family (fails pre-fix on the array-nil families):
   - `MULTI; BLPOP k 1; EXEC` empty list → `*-1`
   - `MULTI; BZPOPMIN k 1; EXEC` → `*-1`
   - `MULTI; BLMOVE src dst LEFT LEFT 1; EXEC` → `$-1` (pins the scalar-nil family)
   - `MULTI; XREAD BLOCK 1 STREAMS s $; EXEC` → the op's non-blocking nil shape
   - WAIT inside MULTI keeps its existing pinning test
     (`test_wait_inside_multi_nonzero_timeout_does_not_block`), cited by the row.

## Cross-references

- FM-BLOCKING-002: same wrong-shape family; the row should name this as the
  deny-blocking instance.
- [Issue 13](13-blocking-wait-becomes-a-run-loop-state.md): the run-loop restructure
  may move the conversion site — the TR row and tests must survive the move (they pin
  behavior, not the site).
- [Issue 08](08-blocking-command-rows.md): blocking row family; keep vocabulary
  consistent.

## Acceptance criteria

- [ ] TR row landed; `just lint-spec` green
- [ ] Conversion site uses `op.timeout_reply()`
- [ ] Forcing tests fail pre-fix (array-nil families), pass post-fix
- [ ] Lua path untouched and still green

## Blocked by

None — can start immediately.
