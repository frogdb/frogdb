# The client-visible scatter error replies have no tests

Status: ready-for-agent
Type: bug (test gap) — client-contract strings unpinned
Severity: likelihood 2/3 (every VLL degradation path a client can hit), consequence 2/3 (a reply
string or a code can change without any test noticing; clients dispatch retry policy on the code) —
score 4
Area: server / scatter coordinator, VLL spec

## Problem

`ScatterExecutor::scatter_error_to_response` (`server/src/scatter/executor.rs:141-190`) is the sole
translator from `ScatterError` to the reply a client sees for every cross-shard degradation:
shard-unavailable, lock-failed, lock-channel-closed, lock-timeout, result-channel-closed,
result-timeout. The VLL spec preamble (`specs/vll.md:15-18`) names
it explicitly as where the client's reply lives.

Two of the strings it emits appear **nowhere else in the tree**:

| string | occurrences outside `executor.rs` |
|---|---|
| `ERR VLL lock acquisition failed` | **0** (4 hits, all inside the function) |
| `ERR shard dropped VLL result` | **0** (2 hits, both inside the function) |

The third, `BUSY shard busy with continuation lock; retry`, is named as the client-visible
`Observable` of both FM-VLL-001 and FM-VLL-004, but their witness
(`sca_lock_request_rejected_while_continuation_held`, `vll/src/shard.rs`) asserts
`ShardReadyResult::Failed(ShardBusy)` and `enqueue_failed` at the *shard* layer. Nothing asserts
that the shard-layer variant becomes that string at the client. The rows say "the client sees X";
no test observes a client.

So the mapping from internal error variant to wire reply — the part clients actually key retry
behaviour off — is entirely unpinned. Change `BUSY` to `ERR` on the busy arm and the suite stays
green while every well-behaved client stops retrying.

This is the concrete instance of the campaign-2 witness audit's H7 heuristic (a row names a RESP
error code that appears in no test named by the row); H7 flags 10 rows, and this one has the
largest client-facing blast radius.

## Fix

A table test over `scatter_error_to_response` — one case per `ScatterError` variant, asserting the
exact reply bytes — plus at least one end-to-end case that drives a real client into the busy arm
and asserts `BUSY shard busy with continuation lock; retry` off the wire, since that is what the FM
rows claim. Then cite the tests in FM-VLL-001 and FM-VLL-004's `Forced by`.

The function is a private method on `ScatterExecutor`; if constructing one in a unit test is
awkward, lift the `match` into a free `fn scatter_error_reply(err: &ScatterError, …) -> Response`
that the method delegates to. Prefer that over making the method public.

While there: `continuation_error_to_response` (`server/src/connection/scripting/eval.rs`) is the
other translator the VLL spec preamble names, and should get the same treatment in the same change.

## Comments

Found by the campaign-2 witness audit, 2026-08-07, while measuring heuristic H7. Related: the VLL
spec's own "not yet rowed" note (`vll-failure-modes.md:20-25`) already observes that the scatter
phases collapse to a generic `-ERR VLL lock acquisition failed` — that generic string is one of the
two with zero tests.
