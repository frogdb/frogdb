# 21 — A reconnecting replica streams beside the session it replaces

Status: ready-for-agent

## Parent

Found by [issue 04](04-proptest-generator-and-r1.md)'s stateful proptest generator
(`frogdb-server/crates/replication/src/properties.rs`), filed under the campaign's rule that a
generator counterexample is pinned and filed rather than fixed inline.
[PRD](../../PRD.md) §3 W2.

## What the property found

Property R1 (`r1_every_action_leaves_the_catalog_clean`) reaches a registry holding **two**
sessions for one announced replica identity, both in `Phase::Streaming`. The forcing sequence is
`pinned_issue_22_a_reconnect_streams_beside_the_session_it_replaces` in
`frogdb-server/crates/replication/src/properties.rs`:

1. A replica attaches, announces `REPLCONF listening-port 6480`, and reaches `Streaming`.
2. Its link dies in a way that produces no bytes and no error on the primary's side — the peer's
   host is partitioned or power-cycled. The primary's session is still registered and still
   `Streaming`; nothing has told it otherwise.
3. The replica comes back on a **new ephemeral port** and announces the same listening port.
4. It reaches `Streaming` too.

The HARD entry `INV-SESSION-2` fires: "replica identity 127.0.0.1:6480 is streaming on sessions
[1, 2]". This is spec GAP-5.

## Where the dedupe is missing

- `ReplicationTrackerImpl::register_announced_replica` (`tracker.rs:183`) mints a fresh id and
  inserts. It never looks at the announcement it is handed, so an identity that is already in the
  map is not noticed.
- `ReplicaSession::start_streaming` does not kick a same-identity predecessor either. Nothing
  anywhere in the crate compares two sessions' `announced_id`; the only consumer of that pair is
  the invariant projection.
- The stale session is only reaped when *its own* socket finally errors or its write times out —
  `write_timeout_ms` for a session with nothing queued behind it is not a liveness bound on a dead
  peer, because a TCP write to a black-holed peer sits in the send buffer.

## Consequences in release builds

The seam hook that catches this in debug is whichever registry-wide seam runs next — in the pinned
sequence, `ingest_replica_ack`. In release the duplicate simply persists:

- `INFO replication` reports the replica twice, with two different offsets, so `connected_slaves`
  over-counts and an orchestrator reading it believes it has more redundancy than it does.
- `WAIT` counts both sessions toward its replica quorum
  (`WaitCoordinator` counts acked sessions, not identities), so `WAIT 2 0` can be satisfied by one
  physical replica acking on its live session while the corpse's last ack is still on the books.
  This is the same class of unbacked durability claim as [issue 21](21-ack-above-live-head.md),
  reached a different way.
- `count_good_replicas` / the lag disconnect machinery both operate per session, so the corpse also
  suppresses nothing and is disconnected on its own schedule.

## Why this is filed rather than fixed

The dedupe policy is a ruling, and the wrong choice makes reconnect storms worse:

1. **Kick the predecessor at `start_streaming`.** When a session reaches `Streaming` with an
   announced identity, disconnect any other session already streaming on that identity. This is
   what Redis effectively gets for free (its replica list is keyed by the connection, but a dead
   peer's connection is reaped by `repl-timeout`, and the new one arrives after). Risk: a replica
   that legitimately opens two links (it does not today) would flap.
2. **Refuse the second registration.** Reject the new handshake while an identity is already
   streaming. Wrong on its own — the *new* link is the live one and the old one is the corpse, so
   refusing it strands the replica until the corpse times out.
3. **Do nothing structural; rely on `repl-timeout`.** Shorten the ack-freshness disconnect so the
   corpse is reaped quickly. This narrows the window rather than closing it, and the window is
   exactly a `WAIT` correctness window.

Option 1 is the shape the invariant implies, but it needs a decision on the unannounced case
(sessions with `listening_port == 0` are *unknown*, not "port 0", and `INV-SESSION-2` deliberately
does not compare them — so they cannot be deduped and must not be).

Whichever option is ruled, the fix needs a failure-mode row (a new `FM-REPLICATION-NNN` in
`specs/replication.md`) naming the forcing test, per the locked
crate's spec-first rule.

## Pin

`frogdb-server/crates/replication/src/properties.rs`:
`pinned_issue_22_a_reconnect_streams_beside_the_session_it_replaces`
(`#[should_panic(expected = "INV-SESSION-2")]`).

The generator is muzzled for exactly the resolved shape "this PSYNC would take a second *announced*
session to `Streaming` while an earlier one on the same identity is still there" —
`known_defect`'s second arm — and `the_muzzle_only_covers_the_pinned_shapes` asserts the near
misses stay in the property: the first PSYNC on an identity, a reconnect after the predecessor
really did deregister, and an unannounced pair. Fixing this issue turns the witness red.

## Ruling (2026-08-13)

**Option: kick predecessor.** A session reaching `Phase::Streaming` disconnects any other session streaming with the same announced identity (addr:port) — an immediate, explicit version of Redis's implicit newest-wins. Sessions with `listening_port == 0` (unknown identity) are NEVER deduped — INV-SESSION-2's exclusion stands. Log each kick. New FM-REPLICATION row.
