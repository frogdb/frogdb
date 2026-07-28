# Replication & Cluster Rework

Umbrella for four interlocking design PRDs spawned by the testing-gap audit
(`.scratch/testing-improvements/`). These are architectural changes / bug fixes, not test
work — moved here per the one-feature-per-directory convention.

Status: draft — all four await user review before implementation.

## PRDs

- [wait-cluster-mode.md](wait-cluster-mode.md) — cluster-mode data replication is entirely
  inert (primary machinery gated on the standalone config role string); build one
  replication plane keyed on data-path role, after which WAIT needs no cluster-specific
  code. Origin: issue 37.
- [exec-slot-revalidation.md](exec-slot-revalidation.md) — EXEC on a former slot owner
  silently commits orphan writes; re-validate the whole queue against a slot snapshot at
  EXEC entry. Subsumes the issue-33 EXECABORT divergence. Origin: issue 55.
- [promotion-replid-psync.md](promotion-replid-psync.md) — manual promotion mints no
  replid, rejects PSYNC, and leaves the shard broadcaster as a frozen NoopBroadcaster;
  shared replication-identity cell + ordered promotion sequence. Origin: issue 34.
- [epoch-fold-redesign.md](epoch-fold-redesign.md) — stop folding the raft term into
  `cluster_current_epoch`; expose `cluster_raft_term` separately; fix the vacuous
  harness `get_cluster_info`; persist raft snapshots. Origin: issues 47 + 16.

## Interlocks

- wait-cluster-mode depends on the promotion bridge designed in promotion-replid-psync
  and on landed issue 61 (runtime checkpoint install).
- wait-cluster-mode flips the issue-48 no-chained-replication pin and the issue-34
  promoted-node-rejects-PSYNC pin — both need explicit re-decision.
- epoch-fold T1 (AddNode counter ratchet) already landed via issue 64.
