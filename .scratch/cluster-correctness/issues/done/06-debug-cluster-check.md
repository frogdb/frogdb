# 06 — DEBUG CLUSTER CHECK

Status: done

## Parent

[PRD](../../PRD.md) §3 W5; exposure ruled in §8 D3 (always compiled, admin/DEBUG gated).

## What to build

`DEBUG CLUSTER CHECK`: admin/debug command running the issue-02 catalog against the
node's current `ClusterStateInner` and returning violations as a RESP array (empty =
clean). Always compiled — Jepsen runs release binaries — gated behind the existing
admin/DEBUG surface like its DEBUG siblings. Read-only.

Update the command docs (website) and any DEBUG command enumeration the docs generate.

## Acceptance criteria

- [ ] Command returns `id + detail` per violation, empty array when clean, in release
      builds
- [ ] Gated exactly as sibling DEBUG commands (no new auth surface)
- [ ] Integration test: inject a violating state via test hooks, command reports it
- [ ] Docs updated

## Blocked by

- Issue 02 (`.scratch/cluster-correctness/issues/`) — the catalog is what it runs.

## Resolution

`DEBUG CLUSTER CHECK` implemented, always compiled (no cfg gating — Jepsen runs
release binaries), gated exactly like sibling DEBUG subcommands (ADMIN flag only, no
new auth surface).

- `frogdb-cluster/src/state.rs`: `ClusterState::check_invariants()` — read-locks
  `ClusterStateInner` and runs `invariants::check_all()` (all tiers, including
  documented exceptions). Locked-crate change; `just mutants-diff frogdb-cluster`
  ran clean: 2 mutants in diff, 1 caught (killed by
  `check_invariants_reports_an_injected_dangling_slot_owner`), 1 unviable
  (doesn't compile) — zero survivors, no triage needed.
- `frogdb-core`: `DebugProvider::cluster_check()` trait seam + flat
  `frogdb_core::Violation` re-export.
- `frogdb-server`: `ConnectionHandler::cluster_check()` impl (`debug_handler.rs`),
  `DEBUG CLUSTER CHECK` dispatch + RESP formatting (`debug_conn_command.rs`) —
  `id`/`detail` map per violation, plain empty `Response::Array(vec![])` when
  clean (not a sentinel bulk string, so Jepsen's non-empty-reply check for
  "clean" reads correctly), and an explicit "cluster support disabled" error in
  standalone mode (an empty array there would misread as "clean" rather than
  "not applicable").
- Tests: 4 dispatch-level unit tests (`cluster_check_reports_an_empty_array_when_clean`,
  `cluster_check_reports_violations_as_id_detail_maps`,
  `cluster_check_reports_cluster_disabled_error_in_standalone_mode`,
  `cluster_unknown_subcommand_errors`) in `debug_conn_command.rs`; 1 wire-level
  integration test (`debug_cluster_check_errors_outside_cluster_mode`) in
  `integration_debug_introspection.rs`; 3 pre-existing `frogdb-cluster` unit
  tests cover `check_invariants()` itself (clean state, injected dangling-slot
  violation, all-tiers-including-documented-exceptions).
- Docs: `website/src/content/docs/architecture/debugging.md` — new row in the
  FrogDB-specific DEBUG subcommands table (hand-written, not generated).
- Verification: `just test frogdb-cluster` (268/268), `just test frogdb-core`
  (915/915), `just test frogdb-server debug_conn_command` (21/21) and
  `cluster_check` (4/4), `just check frogdb-core`/`frogdb-server`/`frogdb-cluster`,
  `just lint frogdb-cluster` (clippy on `frogdb-cluster` and dependent
  `frogdb-server --features turmoil --tests`, clean), `just lint-failure-modes`
  (278 failure modes, 1382/1382 tags matched), `just scratch-check`, `just fmt`.
- Commits: `34de2424` (invariants seam + ClusterState helper), `0c8d759d`
  (DEBUG CLUSTER CHECK executor + docs), `f32047b5` (fix: second `StubDebug`
  fixture in `frogdb-core` missed on first pass, caught by
  `just lint-failure-modes`).
