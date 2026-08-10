# 07 — Decision/IO seam extraction, tiers (i) and (ii)

Status: needs-triage

## Parent

[PRD](../../PRD.md) §3 W3 (the precondition deliverable); scope ruled in §8 D2 — "land (i) and
(ii) first as stepping stones".

## What to build

The models in issues 08 and 09 need a pure decision function, and today the decisions live inside
async methods that own sockets, shard senders, timers and files. W3's first deliverable is
therefore a **decision/IO split**: extract the decision half of each modelled transition into a
pure `fn(&ReplicationView, Action) -> Outcome` and leave the async method as the I/O half that
calls it. This is not a novel shape for the area — it is the shape
`PartialSyncReplay::handle_partial_sync_request` (`primary/replay.rs:350`) already has: state plus
a request plus the current offset in, a `ReplayDecision` out, no I/O.

**Tier (i) — what the two models need.** The promotion transition around
`PrimaryReplicationHandler::begin_primary_stint` (`primary/mod.rs:389`), which is already
synchronous and already transactional in character, so it is the first candidate; and the
feed-gate transition (`ReplicaFeedGate::publish`, `feed_gate.rs:75`). `AppliedOffset::{freeze,
claim, land, admit_divergence}` are synchronous too and entangled only with their own atomics, so
they come along cheaply. The session loop is untouched by this tier.

**Tier (ii) — the symmetry win.** Split the PSYNC arm selection out of `ReplicaConnection::psync`
(`replica/connection.rs:224`) into a pure function beside `handle_partial_sync_request`. This is
good architecture independent of stateright: it deletes the asymmetry ADR 0004 implicitly created,
where the primary side has a pure decision function and the replica side does not, and it is
bounded to one function.

Tier (iii) — the `replica_session.rs` restructure — is authorized by D2 but is **issue 10**, not
this one. Keep the boundary.

Locked-crate discipline applies (`frogdb-replication`, gate 0.85): every step spec-first against
`.scratch/hardening/specs/replication-failure-modes.md`, where rows may move their file:line
citations but not their meaning, and `just mutants-diff` on each touched locked crate before push.
This is a refactor, not a behavior change — no FM row's claim changes.

Soft coupling worth coordinating rather than blocking on: the extracted functions take
`&ReplicationView` from issue 02. If 02 has not landed, extract against the concrete component
state and re-type onto the view when it exists; the split itself is what matters.

## Acceptance criteria

- [ ] Promotion decision extracted as a pure function; `begin_primary_stint` becomes the I/O half
      that calls it, and the W1 hook still fires on the seam
- [ ] Feed-gate transition extracted the same way, with `ReplicaFeedGate::publish` reduced to the
      I/O half
- [ ] PSYNC arm selection is a pure function beside `handle_partial_sync_request`;
      `ReplicaConnection::psync` selects nothing itself and does I/O only
- [ ] No behavior change: every `replication-failure-modes.md` row keeps its meaning (citations
      may move), `just lint-failure-modes` green
- [ ] `just test frogdb-replication` and `just test frogdb-server` green; `just mutants-diff` on
      each touched locked crate triaged before push

## Blocked by

None — can start immediately.
