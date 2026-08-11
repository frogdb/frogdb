# 03 — `DEBUG REPLICATION CHECK`

Status: done

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

- [x] `DEBUG REPLICATION CHECK` always compiled, ADMIN-gated, replying with a RESP array of
      `{id, detail}` maps and an empty array when clean
- [x] Answers in standalone, primary and replica modes — no error path for "not applicable" —
      with a test per mode
- [x] Assembles a complete `ReplicationView` (all §8 D7 `Option<T>` fields filled) so INV-FENCE-1
      and INV-ROLE-1 are evaluated here
- [x] Test asserting a deliberately violating state renders its id and detail, and a clean node
      returns the empty array
- [x] Website DEBUG command reference updated

## Blocked by

- Issue 02 (`.scratch/replication-correctness/issues/`) — renders the catalog over the view it
  builds.

## Resolution (2026-08-11)

Built as specified, mirroring `DEBUG CLUSTER CHECK` end to end.

### Shape

- `DebugProvider::replication_check` beside `cluster_check` in `frogdb-core/src/conn_command.rs`,
  implemented in `connection/debug_handler.rs`, dispatched from `connection/debug_conn_command.rs`
  under `DEBUG REPLICATION CHECK` with a `HELP` entry. ADMIN gating comes free from `DEBUG_SPEC`'s
  `CommandFlags::ADMIN`; nothing is behind a `cfg`.
- `format_cluster_check_response` was generalised to `format_check_response(violations,
  not_applicable)` and now serves both catalogs — one renderer, one `{id, detail}` map shape.
- The seam returns `Option<Vec<Violation>>`, but `None` does **not** mean standalone: it means no
  replication seams are wired at all, which never happens in a running server (`init_replication`
  builds `PrimaryReplicationHandler` on every role). Standalone, primary and replica all get a
  real array. `None` renders as `ERR This instance has replication support disabled` so an
  un-wired build cannot be misread as "clean".

### Completing the view

`PrimaryReplicationHandler::view()` already supplies state, offsets, apply gate, backlog,
replicas, departure and feed gate on every role. Three groups are filled at this surface:

- **feed gate** — re-taken with `frogdb_core::HANDOFF_BARRIER_MS` as the barrier budget, which the
  replication crate cannot know, so `INV-GATE-1`'s budget clause is live here
- **fence** — from `ReplicationQuorumChecker`, carried un-erased on `ClusterDeps`
  (`replication_self_fence`) because `dyn QuorumChecker` only answers `has_quorum()` while
  `INV-FENCE-1` needs the arming latch and the two settings behind it
- **role** — from the live `is_replica` flag plus `RoleController::primary_target()` for the
  upstream (`ClusterDeps::role_controller`)

Both new deps are wired in every mode in `server/subsystems.rs`: a group that went missing in
standalone would silently stop evaluating the two entries nothing else checks. The identity is
also back-filled from the shared state when `handler.view()`'s `try_read` loses the race, so a
contended sample cannot silently skip every `INV-REPLID-*` claim. No locked crate was modified.

### Defect found

The violating-state test found one on its first run against a live pair — filed as
[issue 19](../open/19-self-fence-arms-only-on-the-write-path.md). `ReplicationQuorumChecker`
latches its arming inside `has_quorum()`, on the write path only, so a primary whose replica is
streaming but which has served no write reports `INV-FENCE-1` at `Tier::Hard`. Not only a
reporting artifact: lose that replica before the first write and `arm_if_streaming` finds nothing
to arm from, so the fence never engages — the failure FM-REPLICATION-041/062 exist to prevent.
Muzzled witness `debug_replication_check_is_clean_on_a_primary_before_its_first_write`; today's
behaviour is pinned deliberately by the violating-state test, which is also the end-to-end proof
that the fence group is filled (the in-crate hooks skip `INV-FENCE-1` for want of it).

`INV-ROLE-1` has no end-to-end forcing path: chained replication is refused by the `PSYNC` role
gate and demotion tears its downstreams down (`test_chained_replication_rejected_…`,
`test_demoted_primary_stops_serving_psync_to_its_downstream`), which is why the entry is a
`DocumentedException` in the first place. It is evaluated rather than skipped here — the role
group is filled — and the catalog's own unit tests force the violating branch.

### Evidence

- `just test frogdb-server replication_check` — 7/7 (3 dispatch unit tests, 4 integration),
  plus `debug_replication_unknown_subcommand_errors`; the muzzled witness is the only skip.
- `just check frogdb-server --all-targets` green; lefthook seam-gates + fmt green on both commits.
- Commits `8dca1c74` (surface) and `d452a732` (tests, issue 19, docs) on
  `replication-03-debug-replication-check`.
