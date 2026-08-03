# `INFO replication` renders only streaming replicas, so `state` is always `online`

Status: done
Type: gap (Redis parity / observability)
Severity: likelihood 3/3 (every `INFO replication` during any sync), consequence 2/3 (a replica
that is mid-transfer is invisible, and the one field that would say so can only say `online`) —
score 6
Area: replication / INFO

## Problem

Closing issue 16 replaced the hardcoded `state=online` in the `slaveN:` line with a real projection:
`ReplicaLine.state` is `ReplicaState::from(phase)`, total over `Phase`
(`WaitingBgsave` → `wait_bgsave`, `SendingBulk` → `send_bulk`, `Streaming` → `online`,
`Disconnecting` → `offline`). That is FM-REPLICATION-049 and it is real code with real tests.

But **no client can observe any value except `online`**, because both renderers feed from a
streaming-only source:

- `frogdb-server/crates/server/src/connection/info_handler.rs:129` — `tracker.get_streaming_replicas()`
- `frogdb-server/crates/server/src/commands/info.rs:408` — same call
- `frogdb-server/crates/replication/src/tracker.rs:193` — `get_streaming_replicas()` filters
  `matches!(s.phase(), Phase::Streaming)`; `get_all_replicas()` at `:179` is the unfiltered twin

So a replica that is being fed a checkpoint right now — the exact situation an operator opens
`INFO replication` to see — appears in neither `connected_slaves` nor any `slaveN:` line. It is not
misreported; it is absent.

Redis reports it. `genInfoSectionDict`'s replication section walks `server.slaves` and emits a line
for every replica whose state is `wait_bgsave`, `send_bulk` or `online`, and `connected_slaves` is
`listLength(server.slaves)` — the whole list, syncing replicas included.

## Why this is not simply a bug to fix in place

Widening the feed is **wire-visible in two ways at once**, which is why it is an issue and not a
one-line change:

1. `connected_slaves` grows to include replicas that hold none of the primary's data yet. Anything
   downstream that reads `connected_slaves` as "replicas that can serve reads" or "replicas that
   could be failed over to" changes meaning silently.
2. FM-REPLICATION-043's NOT-observable clause currently *forbids* a `slaveN:` line for a
   non-streaming replica, precisely because `connected_slaves` and the line count must agree
   (FM-REPLICATION-001 is the failure this protects against: a replica advertised as a failover
   target while it is still empty). Widening the feed means rewriting that clause deliberately,
   not sliding past it.

What FM-REPLICATION-049 already bought is that the widening is now *safe to make*: the render no
longer has to be edited in lockstep with the filter, because the state is projected from the
replica rather than assumed by the caller. That was GAP-2's actual complaint.

## Suggested remedy

1. Feed both renderers from `get_all_replicas()` and drop `Phase::Disconnecting` entries at the
   render boundary (Redis has no spelling for that phase and skips such slaves) — so the set of
   rendered lines is "every replica in a phase Redis names", which is Redis's own rule.
2. Make `connected_slaves` the count of *rendered* lines, from the same vector, so the two cannot
   drift regardless of what the filter admits. That keeps FM-REPLICATION-043's real invariant
   (count == line count) while changing what both of them count.
3. Rewrite FM-REPLICATION-043's NOT-observable clause and FM-REPLICATION-049's Observable/deviations
   text in the same change, and re-point anything that reads `connected_slaves` as a readiness
   signal (`WAIT` does not — it counts acked offsets — but check the operator and cluster paths).

## Tests that should exist

- `a_replica_being_fed_a_checkpoint_is_rendered_as_send_bulk` — register a replica, hold it in
  `SendingBulk`, assert the `slaveN:` line exists and says `send_bulk`.
- `connected_slaves_counts_every_rendered_line` — mixed phases, assert the count equals the number
  of `slaveN:` lines in the same render (the FM-REPLICATION-043 invariant, restated over the wider
  set).
- `a_disconnecting_replica_is_not_rendered` — the one phase that stays filtered.

## Spec impact

FM-REPLICATION-043 (Observable + NOT observable), FM-REPLICATION-049 (Observable + the GAP-2
closure note + the `Disconnecting`/`offline` row in the Redis-deviations table, which is marked
latent-today and would become live). All four places currently state that `online` is the only
value on the wire; closing this issue means rewriting all four.

## Resolution

Fixed as specified, plus one thing the issue did not know about.

**The fix.** A single shared feed, `info::rendered_replicas(tracker)`
(`frogdb-server/crates/server/src/info/mod.rs`), maps `tracker.get_all_replicas()` through
`ReplicaLine::from_replica`, which now returns `Option<Self>`. `ReplicaState::from(Phase)` became
`ReplicaState::from_phase(Phase) -> Option<Self>` and the `Offline` variant was **deleted**:
`Disconnecting` maps to `None`, so a replica being torn down drops out of the lines *and* the count
in one `filter_map`. `connected_slaves` is `replicas.len()` in both renderers
(`connection/info_handler.rs`, `commands/info.rs`), so remedy step 2 is structural rather than a
convention — the count cannot be computed from a different traversal than the lines.

This makes FrogDB match Redis exactly (`genInfoSectionDict` emits a line only for `wait_bgsave`,
`send_bulk`, `online`) and *removes* a deviation rather than adding one: the old `offline` spelling
was a word no Redis client parses, kept only because the count was computed separately.

**Ordering — not in the issue, found by making the feed wider.** Neither `get_all_replicas` nor
`get_streaming_replicas` sorted; the registry is a `HashMap`, so `slaveN:` indices came out in
iteration order and could reshuffle between two renders that observed no change. Both getters now
`sort_unstable_by_key(|r| r.id)` (`frogdb-server/crates/replication/src/tracker.rs`); ids come from a
monotonic counter, so id order is attach order. The sort was added to `get_streaming_replicas` too
because `ROLE`'s replica array is positional on the wire and had the same defect.

**Wire-visible changes, taken deliberately** (issue's point 1): `connected_slaves` now includes
syncing replicas, and `wait_bgsave` / `send_bulk` now reach clients. `WAIT` is unaffected — it counts
acked offsets over `get_streaming_replicas()`, which keeps its `Phase::Streaming` filter. The only
in-repo consumer of `connected_slaves` is `frogctl/src/commands/replication.rs`, which iterates
`0..connected_slaves` reading `slaveN:` lines; that stays correct precisely because count == line
count. No cluster path reads it as a readiness signal.

## Spec impact (done)

- New row **FM-REPLICATION-060** — "every attached replica INFO can name is rendered, in the state it
  is actually in".
- **FM-REPLICATION-043**: Observable rewritten (lines are per phase-Redis-names, not per streaming
  replica; ordering is attach order); NOT-observable rewritten — the old blanket prohibition on a
  line for a non-streaming replica became a prohibition on `state=online` for one, plus a new clause
  naming the invisible-syncing-replica failure and one naming the `connected_slaves`-as-readiness
  misreading; Invariant rewritten onto the shared feed.
- **FM-REPLICATION-049**: Observable, NOT-observable and Invariant rewritten (`from_phase`, the
  `Option`, the deleted `offline`); the "Why `Disconnecting` renders a line at all" note inverted to
  "Why `Disconnecting` renders no line".
- **GAP-2** closure paragraph rewritten: GAP-2 is closed by 049 + 060 together, and the "be exact
  about what a client sees today" caveat is replaced by a statement of what
  `get_streaming_replicas()` is still for.
- Redis-deviations table: the FM-REPLICATION-049 `offline` row (marked "latent today") is replaced by
  an FM-REPLICATION-060 row recording convergence rather than deviation.

## Tests

`frogdb-server/crates/server/src/info/sections.rs`, all tagged `// FM-REPLICATION-060`
(`connected_slaves_counts_every_rendered_line` also carries `// FM-REPLICATION-043`):

- `a_replica_being_fed_a_checkpoint_is_rendered_as_send_bulk`
- `connected_slaves_counts_every_rendered_line`
- `a_disconnecting_replica_is_not_rendered`
- `rendered_replicas_are_ordered_by_attach` (not in the issue's list — see Ordering above)

Red proof: written against a deliberately streaming-only `rendered_replicas`; 3 of the 4 failed. The
fourth (`a_disconnecting_replica_is_not_rendered`) passes in both worlds by design — it guards the
one filter that stays, and would fail if a later change widened the feed to the raw registry.
