# 29 — `replication-durability = quorum` write mode

Status: needs-triage

**This needs a brainstorm/design round before implementation** — it touches the ack path, the
`WAIT` machinery, and the config surface, and several open design questions (below) need to be
settled first. Do not pick up as `ready-for-agent` until a design ruling exists.

## Parent

Design issue arising from the 2026-08-13 anti-pattern review of the LOCKED cluster spec,
finding H2
(`.scratch/formal-spec/reviews/2026-08-13-antipattern/spec-review-cluster.md`, H2) — read that
finding for context on the unplanned-failover data-loss window it identifies. Cross-reference
[cluster-correctness issue 26](../../cluster-correctness/issues/) (planned-failover barrier),
which handles the *planned* path; this issue is the roadmap path for the *unplanned* one.

## Ruling (2026-08-13)

**File the design issue now as the roadmap path to lossless UNPLANNED failover: a
`replication-durability = quorum` mode where a write is acked to the client only after a
replica/quorum has received it (CRDB/etcd Raft-commit shape; Redis per-write `WAIT`
generalized to a mode). Latency cost per write when enabled. Design questions to settle in the
design round: quorum definition (one replica vs majority), interaction with WAIT,
timeout/degraded behavior when replicas lag or are absent, per-command vs global scope,
interplay with `durability sync` (persistence) semantics. Cross-reference cluster-correctness
issue 26 (planned-failover barrier), which handles the planned path.**

## Why this matters

FrogDB's current durability story acks a write once it lands locally (per
`FM-REPLICATION-008`'s `landed <= claimed <= received` triple — deliberately honest about what
an ack means, and stronger than Redis's receive-based ack). But a write acked and only locally
landed is lost if that node fails before any replica received it and an *unplanned* failover
promotes a replica that never saw it. `WAIT n timeout` lets a client opt into waiting for
replication per-call, but nothing acks-gates a write on quorum receipt by default or as a
durable mode — the client has to remember to call `WAIT` after every write it cares about, and
even then a raced failover between the local ack and the `WAIT` return is a client-visible gap.

CockroachDB and etcd/Raft close this by construction: a write is not acknowledged until it is
committed to a quorum of the replication group, so any leader that can be elected already holds
every acked write. Redis's per-write `WAIT` is the closest analogue in Redis-compatible systems,
but it is opt-in and per-call rather than a durability mode.

## Design questions for the brainstorm round

- **Quorum definition.** One replica ack vs. a majority of the replica set. Interacts with
  cluster shard replica counts, which can be as low as one.
- **Interaction with `WAIT`.** Does `replication-durability = quorum` subsume per-call `WAIT`,
  coexist with it, or make it redundant for writes issued under the mode?
- **Timeout / degraded behavior.** What happens when replicas lag or are absent — block
  indefinitely (CP), time out and ack anyway (weakens the guarantee), or refuse the write
  (unavailable)? Needs a documented choice, not an implicit default.
- **Scope.** Per-command opt-in (a flag on the write) vs. a global server/database mode.
- **Interplay with `durability sync`.** How this replication-side mode composes with the
  existing local-persistence durability knob — are they independent axes or does one imply
  constraints on the other?
- **Relationship to planned failover.** Cluster-correctness issue 26 already handles the
  *planned* failover barrier (slot handoff, orderly promotion). This mode's job is the
  *unplanned* case (primary failure with no barrier to run). The design round should state the
  boundary between the two explicitly so they don't overlap or leave a gap.

## Acceptance criteria (design round, not implementation)

- [ ] Design questions above settled with a recorded ruling
- [ ] Relationship to cluster-correctness issue 26 stated explicitly (boundary between planned
      and unplanned paths)
- [ ] Issue re-triaged to `ready-for-agent` (or split into implementation sub-issues) once the
      design ruling lands
