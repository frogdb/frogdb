# 03 — `DEBUG REPLICATION CHECK`

Status: needs-triage

## Parent

[PRD](../../PRD.md) §3 W5; §5 sequencing item 2 (land inside W1's tail or W4's first PR, not as a
separate late workstream).

## What to build

A mirror of `DEBUG CLUSTER CHECK`: server side at
`frogdb-server/crates/server/src/connection/debug_conn_command.rs:226` / `:302` with the response
formatter beside `format_cluster_check_response` (`:819`), and the seam beside
`DebugProvider::cluster_check` in `frogdb-core/src/conn_command.rs:603`.

Reply shape is a RESP array of `{id, detail}` maps, one per violation; **an empty array is
"clean"**, not a sentinel string — the Jepsen checker's clean test reads emptiness. ADMIN-flag
gated like every sibling DEBUG subcommand, and **always compiled** (not behind
`cfg(test, debug_assertions)`) because Jepsen runs release binaries: the catalog's `check_all`
entry point therefore cannot be test-only even though the seam hooks are.

This is the one place, alongside the W4 quiesce path, that assembles a **complete**
`ReplicationView` — every `Option<T>` that §8 D7 leaves to the caller gets filled here (the
runtime crate's `ReplicationQuorumChecker`, the server's `RoleManager`, the client registry's
`ReplicaFeedGate`), so INV-FENCE-1 and INV-ROLE-1 are actually evaluated at this surface even
though the in-crate hooks usually skip them.

One deliberate divergence from the cluster command, and it should be documented as deliberate:
**it answers in every mode.** `DEBUG CLUSTER CHECK` errors in standalone because an empty array
would misread as "clean"; a node with no replication link still has a replid, an offset triple
and a persisted state that can be wrong, so replication has no not-applicable case to
distinguish. Standalone, primary and replica all get a real answer.

Documentation for the new subcommand goes on the website with the other DEBUG commands.

## Acceptance criteria

- [ ] `DEBUG REPLICATION CHECK` always compiled, ADMIN-gated, replying with a RESP array of
      `{id, detail}` maps and an empty array when clean
- [ ] Answers in standalone, primary and replica modes — no error path for "not applicable" —
      with a test per mode
- [ ] Assembles a complete `ReplicationView` (all §8 D7 `Option<T>` fields filled) so INV-FENCE-1
      and INV-ROLE-1 are evaluated here
- [ ] Test asserting a deliberately violating state renders its id and detail, and a clean node
      returns the empty array
- [ ] Website DEBUG command reference updated

## Blocked by

- Issue 02 (`.scratch/replication-correctness/issues/`) — renders the catalog over the view it
  builds.
