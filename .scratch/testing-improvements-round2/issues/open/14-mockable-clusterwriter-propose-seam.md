# Mockable `ClusterWriter` / propose seam — `ProposeError::Redirect` is unreachable in tests

Status: ready-for-agent
Type: AFK
Origin: round-2 testing audit 2026-07-28 — 15 parallel area audits, `.scratch/testing-improvements-round2/`
Source: INFRASTRUCTURE.md I14
LOE: 1–2 days (estimated)
Tier: B
Area: frogdb-cluster / `cluster/src/network.rs`, `server/src/cluster_bus.rs`
Asked by: 04 → 11. **Dropped from `MASTER.md` §6.**

## Context

Server-side code handles `ProposeError::Redirect`, but no server-side test can produce one —
reaching it today means standing up a real multi-node cluster and engineering a leader change.
A seam that lets a test inject the propose outcome collapses that to a crate-API test. The
same file carries a second, related debt: the RPC error taxonomy is consumed by
string-matching, so the two should be fixed together if that file is touched.

## Evidence

- **Effect**: `ProposeError::Redirect` is unreachable from server-side tests today. A seam
  drops 04/F7 from **boundary 5 to boundary 2** and helps several other server-side callers.
- **Related, same file**: `MAX_FRAME_SIZE` and `parse_rpc_message`'s error taxonomy live in
  `cluster/src/network.rs`. 04/F10 needs those errors *typed* rather than string-matched —
  `server/src/cluster_bus.rs:167-181` currently string-matches. If that file is refactored,
  do both at once.

## What to build

1. A mockable `ClusterWriter` / propose seam so a server-side test can inject any
   `ProposeError`, including `Redirect`, without a live cluster.
2. Rewrite 04/F7's coverage against the seam at boundary 2.
3. In the same pass, give `parse_rpc_message` a typed error taxonomy in
   `cluster/src/network.rs` and replace the string-matching at
   `server/src/cluster_bus.rs:167-181` with matches on those variants.

## Acceptance criteria

- [ ] A server-side test injects `ProposeError::Redirect` and asserts the caller's handling,
      with no cluster harness and no multi-node setup.
- [ ] The seam is used by at least one caller other than 04/F7's test.
- [ ] `parse_rpc_message` returns a typed error enum covering `MAX_FRAME_SIZE` violations and
      the other parse failures.
- [ ] `server/src/cluster_bus.rs:167-181` matches on those variants; no string comparison of
      error text remains in that block.

## Test boundary

Level 2 — that is the whole point of the seam: propose-outcome handling is server-side logic,
and today it can only be reached at level 5. With the seam it is a crate-API test with no
network and no leader election.

## Depends on

Nothing.
