# 19 — the self-fence arms only on the write path

Status: needs-triage

## Parent

[PRD](../../PRD.md) §3 W5. Found by `DEBUG REPLICATION CHECK`
([issue 03](03-debug-replication-check.md)) on its first run against a live pair — the first
state the new surface rejected, and a `Tier::Hard` one.

## What was found

`ReplicationQuorumChecker` latches its arming lazily, from the write path only:

- `arm_if_streaming` (`frogdb-server/crates/replication-runtime/src/quorum.rs`) sets
  `armed` when `tracker.has_streaming_replica()` is true
- its **only** caller is `QuorumChecker::has_quorum`, which runs per write command
- nothing on the session lifecycle arms it — a replica reaching `Phase::Streaming` leaves
  the checker unarmed until the primary's next write

Two consequences, one cosmetic and one not.

**The surface.** A primary whose replica is streaming but which has served no write since
reports `INV-FENCE-1` — "sessions [1] are streaming but the self-fence checker is unarmed".
That is the invariant's dead-detector clause firing on a node nobody would call broken, at
`Tier::Hard`, so `check_hard` (the hook tier) would fail a quiesce there too.

**The behaviour.** The window is not only a reporting artifact. If the streaming replica is
then *lost* and its session torn down before the primary's first write:

1. the write calls `has_quorum` → `arm_if_streaming`
2. `tracker.has_streaming_replica()` is now false, so `armed` stays false
3. `!armed` short-circuits to "allow", and the fence never engages — not for that write and
   not for any later one, because nothing will ever arm it again until a *new* replica
   streams

So a primary that took a replica and served no write while it was attached keeps accepting
writes after silently losing it, with `self-fence-on-replica-loss` at its default `yes`.
That is precisely the failure FM-REPLICATION-041/062 exist to prevent, reachable whenever a
partition or crash lands in a read-only lull — narrow, but not exotic (a freshly promoted
primary that is being caught up before traffic is cut over sits in exactly this state).

The arming comment says arming is tracked "even while fencing is disabled, so enabling the
toggle on a primary that has already served replicas fences immediately rather than granting
a fresh grace period" — the same reasoning applies to a primary that has already *had* a
streaming replica but no writes, which is the case the current placement misses.

## Precedent

Redis's `min-replicas-to-write` is evaluated per write against the live good-replica count
with no arming latch at all: there is no "has this primary ever had a replica" memory, so
the read-only lull cannot desynchronise the two. FrogDB's latch exists to distinguish
"never had a replica" from "lost its replicas", which Redis does not attempt — so the fix is
FrogDB-specific rather than a compatibility question.

## Ruling needed

- (a) arm on the session transition to `Streaming` (the tracker/registration path) rather
  than on the write path, keeping `has_quorum`'s lazy `arm_if_streaming` as a
  belt-and-braces catch-up. INV-FENCE-1 stays `Tier::Hard` and stops false-positiving
- (b) keep the write-path latch and restate INV-FENCE-1 so it only fires once a write has
  been served (needs a "has written" bit in the view that does not exist today), accepting
  the unfenced-after-a-read-only-lull behaviour as documented

## Acceptance criteria

- [ ] Ruling recorded here with its reasoning
- [ ] Behaviour implemented, with a forcing test in `frogdb-replication-runtime` (locked
      crate, gate 0.85) — the "lost the replica without ever writing" path, asserting the
      write is refused
- [ ] `debug_replication_check_is_clean_on_a_primary_before_its_first_write` un-ignored
      under (a), or deleted with the ruling written into INV-FENCE-1's comment under (b)
- [ ] `debug_replication_check_renders_a_violating_states_id_and_detail` re-pointed at a
      state that still violates under the ruling
- [ ] INV-FENCE-1's tier/claim updated if (b) wins

## Witness

`frogdb-server/crates/server/tests/integration_debug_introspection.rs` —
`debug_replication_check_is_clean_on_a_primary_before_its_first_write`, `#[ignore]`d against
this issue. Today's behaviour is pinned (deliberately) by
`debug_replication_check_renders_a_violating_states_id_and_detail` in the same file.
