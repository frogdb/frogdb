# 23 — a demotion drops the arming latch but keeps the departure record

Status: ready-for-agent

## Parent

[PRD](../../PRD.md) §3 W2. Found by property R6
([issue 05](../done/05-properties-r2-r6.md)) on its first run — the second state the new
`(replica set, departure code)` projection rejected, after the one
[issue 19](19-self-fence-arms-only-on-the-write-path.md) already covers.

## What was found

`ReplicationTrackerImpl` keeps `last_streaming_departure` — the record of how this node's
last streaming replica left — and `ReplicationQuorumChecker` keeps `armed`, the latch that
says a session reached `Streaming` at all. The catalog treats the second as the independent
witness for the first (`INV-SESSION-3`: "a recorded streaming departure implies a session
actually reached Streaming"), and `INV-FENCE-1`'s second clause says the same thing from the
other end.

`RoleManager::demote` (`frogdb-server/crates/server/src/role_manager.rs`) clears one and not
the other:

1. `end_primary_stint()` asks the replica sessions to disconnect; each one's `run_exit`
   records a departure — `Lost` for a dropped link — and deregisters
2. `checker.reset_arming()` drops the latch, deliberately, so a later re-promotion does not
   inherit a fence from replicas that belonged to the stint that just ended
3. nothing clears `last_streaming_departure`

The node is then a replica carrying `departure = Lost` with `armed = false`, which is
exactly the state both entries call a violation, at `Tier::Hard`. It is not a transient: the
latch is only cleared here and the departure record is only cleared by a *new* replica
reaching `Streaming` (`ReplicaSession`'s PSYNC path calls
`tracker.clear_streaming_departure()`), so the pair stays incoherent for the whole time this
node is a replica.

Two consequences.

**The surface.** `DEBUG REPLICATION CHECK` ([issue 03](../done/03-debug-replication-check.md)) on any
demoted node whose replica link was lost reports `INV-SESSION-3` and `INV-FENCE-1`, and
`check_hard` would fail a quiesce there. Same shape as issue 19's cosmetic half, different
cause: there the latch was never installed, here it was installed and then withdrawn.

**The behaviour.** The departure record is not only the catalog's input — the issue's own
comment on `inv_session_3` says it is "what a promotion and the self-fence checker read to
decide whether this node ever had a follower". A node demoted with `Graceful` latched and
then re-promoted reads a clean departure from the *previous* stint until its first new
replica streams. `departed_cleanly()` is only consulted once `armed` is true, and the same
`reset_arming` left `armed` false, so the self-fence cannot act on the stale value today —
but that is one call-ordering accident away from being load-bearing, and the promotion path
reads the record on its own terms.

## Precedent

Redis has no arming latch and no departure record: `min-replicas-to-write` is evaluated per
write against the live good-replica count, so there is no pair to keep coherent. The latch
and the record are both FrogDB's, added together for FM-REPLICATION-041/062, so this is
about keeping two pieces of FrogDB's own state in step rather than a compatibility question.

## Ruling needed

- (a) clear the departure record alongside the latch: `reset_arming` (or the demotion path
  around it) calls `tracker.clear_streaming_departure()`, on the grounds that both describe
  the stint that just ended and neither survives it. Smallest change, and it makes the
  invariant true as written
- (b) leave the record and narrow both entries so they only fire while this node is a
  primary, on the grounds that the record is a *primary's* history and a replica has no
  business being judged on it (needs `ViewField::Role` on two more entries; note the fence
  view is captured on every node, since the checker is installed at boot regardless of role)
- (c) leave both and demote the two entries to `Tier::DocumentedException` citing this file

Note that (a) and (b) differ observably after a re-promotion: under (a) the re-promoted node
starts with no departure history, under (b) it inherits the old stint's.

## Acceptance criteria

- [ ] Ruling recorded here with its reasoning
- [ ] Behaviour implemented, with a forcing test in `frogdb-replication-runtime` (locked
      crate, gate 0.85) — demote with a `Lost` departure latched, then assert the projection
      is clean
- [ ] The R6 muzzle arm citing this file removed from
      `frogdb-server/crates/replication-runtime/src/properties.rs`, and
      `pinned_issue_23_a_demotion_keeps_the_departure_it_disarmed` deleted or inverted
- [ ] `INV-SESSION-3` / `INV-FENCE-1` claim or tier updated if (b) or (c) wins

## Witness

`frogdb-server/crates/replication-runtime/src/properties.rs` —
`pinned_issue_23_a_demotion_keeps_the_departure_it_disarmed`, a `#[should_panic]` witness,
plus the `known_defect` arm that keeps property R6 from failing on this shape.

## Ruling (2026-08-13)

**Option a: clear with the latch.** Demotion clears `last_streaming_departure` alongside `checker.reset_arming()` — re-promotion starts with a clean departure history, making the invariants true as written.

## Addendum (2026-08-13, anti-pattern review)

Review finding A7: "clear" must write the sentinel `0`/`None` (unknown), never a synthesized
`Graceful` departure — the tracker's `AtomicU8` treats `0`/unknown as "keep fencing", and the
row itself calls the permissive reading "the one failure of this seam that silently un-fences a
primary". Reading the ruling's "clean departure history" as `Some(Graceful)` would hand a
re-promoted node a pre-disarmed fence — the opposite of the safe default. The forcing test's
assertion must be `last_streaming_departure() == None` after demotion, not
`Some(Graceful)`. Add a line to the FM row: a demote/re-promote cycle deliberately forgets that
this node ever had followers, so the self-fence is not a guarantee that survives a role flap.
