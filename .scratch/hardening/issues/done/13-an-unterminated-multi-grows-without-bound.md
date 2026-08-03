# An unterminated MULTI in the replication stream grows without bound

Status: done
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

## Resolution

Fixed as proposed. `ReplicaTxnBound` (`apply.rs`) carries both ceilings and an `AtomicU64` of
abandoned groups, shared behind an `Arc` so the count is the node's rather than one link's.
`exceeded(commands, bytes)` is a strict `>` on each axis, checked immediately after every push onto
the open group; on breach the loop drops the group, counts it, logs at `error` with both sizes and
both limits, and calls `stint.admit_divergence(epoch)`.

Config: `replication.replica_txn_max_commands` (default 1e6) and `replication.replica_txn_max_bytes`
(default 1 GiB), both `#[param(skip)]` — TOML-only internal sizing knobs, alongside
`split_brain_buffer_size`/`split_brain_buffer_max_mb`, so no CONFIG surface, no `params.rs` golden
churn, no generated website JSON. `validate()` rejects 0 on either axis: "0 = unlimited" is the bug
itself, so it is not an available reading.

### The open sub-question: full resync, not partial

Decided **full**, for three reasons:

1. It is what the existing mechanism already gives. `admit_divergence` latches on the stint, the
   connection wakes through `AppliedOffset::divergence()` and runs `abandon_diverged_link()`, which
   resets the received head to 0 — so `psync_request_args` sends `PSYNC ? -1` and the primary can
   only answer `+FULLRESYNC`. No second teardown path to build or keep correct.
2. A partial resync has no honest resume point. The abandoned group's bytes were consumed from the
   stream but deliberately never claimed (same as any group that never reached a shard), so the
   applied head does not describe the position the primary would resume from. A `+CONTINUE` from it
   would either redeliver the same unterminated group forever or splice the surviving half of a
   transaction into the keyspace.
3. It matches Redis, which kills the link outright when `client-query-buffer-limit` is breached
   rather than negotiating a resume.

### One claim in the issue that was wrong

"The epoch guard next to it already establishes the pattern for abandoning a group … the machinery
for giving up mid-transaction exists." The epoch guard only *discards* — it sets `pending = None`
and continues on the same link. What actually tears the link down is `admit_divergence`, used by the
failed-apply path (issue 08), and that is what this fix reuses. A discard-and-continue disposition
would have left the replica reading the rest of a stream it had just proved it could not trust.

Forcing tests: `an_unterminated_multi_is_abandoned_at_the_command_ceiling`,
`an_unterminated_multi_is_abandoned_at_the_byte_ceiling`,
`a_large_transaction_under_the_bound_still_applies_atomically` (a group sitting exactly *on* both
ceilings, asserting it still applies as one atomic group and claims its whole byte span), plus
`zero_replicated_txn_ceilings_are_rejected` in `frogdb-config`. Spec row **FM-REPLICATION-045**.
