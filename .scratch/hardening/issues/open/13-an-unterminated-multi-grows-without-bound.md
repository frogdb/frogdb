# An unterminated MULTI in the replication stream grows without bound

Status: needs-triage
Type: bug (availability)
Severity: likelihood 1/3 (needs a primary that streams `MULTI` and then never `EXEC`s — a bug or a
hostile peer, not normal operation), consequence 3/3 (replica OOMs with no metric moving and no log
line) — score 3
Area: replication / apply

## Problem

`PendingTxn` (`frogdb-server/crates/replication/src/apply.rs:137`) accumulates every command
between `MULTI` and `EXEC`:

```rust
struct PendingTxn {
    shard_id: u16,
    commands: Vec<ParsedCommand>,
    ...
}
```

and the apply loop pushes into it unconditionally (`apply.rs:427`):

```rust
if let Some(txn) = pending.as_mut() {
    txn.commands.push(cmd);
    txn.bytes += frame_bytes;
}
```

Nothing bounds `commands.len()` or `txn.bytes`. A primary that opens a `MULTI` and never closes it
turns the replica's apply path into an unbounded buffer: every subsequent frame is retained instead
of applied, so the replica simultaneously stops making progress *and* grows until the allocator
gives up. There is no threshold, no counter, no warning — the only external symptom is that
`master_repl_offset` keeps advancing on the primary while the replica's applied offset stalls, and
RSS climbs.

The epoch guard next to it already establishes the pattern for abandoning a group (a group is
dropped when the frames that follow belong to a newer history), so the machinery for giving up
mid-transaction exists; it just has no size trigger.

Redis has the same structural exposure on `MULTI` from a master link, but bounds the input side
with `proto-max-bulk-len` and `client-query-buffer-limit`; FrogDB's replica link has neither
applied to the accumulated group.

## Candidate fix

Bound the group on both axes, mirroring the backlog's two-axis bound: a maximum command count and a
maximum accumulated byte size, both config-backed with defaults generous enough that no legitimate
transaction trips them (a replicated `MULTI` is bounded by whatever the primary accepted from its
own client). On breach, log at `error`, increment a counter, abandon the group, and tear down the
link so a resync re-establishes a clean stream — the same disposition as the epoch mismatch, since
in both cases the accumulated state cannot be trusted.

Open sub-question: whether breach should also mark the replica for full resync rather than
partial, since the stream position after an abandoned group is only as trustworthy as the peer that
produced it.

## Forcing tests

An apply-seam test that feeds `MULTI` followed by N+1 commands with no `EXEC` and asserts the group
is abandoned, the counter moved, and the link torn down — not that the process survives, which is
not a decidable assertion. A second test at the byte bound with a small number of large commands.
A third asserting a legitimate large-but-under-bound transaction still applies atomically.
