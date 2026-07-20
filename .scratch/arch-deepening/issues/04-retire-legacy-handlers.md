# 04 — Retire the legacy `handlers/` directory (~35 files)

Status: needs-triage

## What to build

Proposal 03 part (a). The `ConnMutation` seam (round-8, `ab340681`) finished the dispatch-side
unification, and doc-comments now honestly state that `handlers/` bodies remain live behind
`ServerWideOp`/`ScatterGatherOp` and the EXEC/scripting/search helpers. This issue removes the
two-worlds split: every remaining `handlers/` body either migrates to its op's exhaustive-match
arm / a `*_conn_command.rs` module, or is deleted as dead.

Phased (single issue, land per phase, each phase independently green):

1. **Inventory** — map each `handlers/*.rs` body to its live entry points (ServerWideOp arm,
   scatter strategy, EXEC orchestration, scripting/search helper); mark dead code.
2. **Mechanical migrations** — bodies with exactly one caller move next to that caller
   (dispatch arm or conn_command module); delete dead ones.
3. **Structural stragglers** — `execute_transaction` (EXEC orchestration),
   `handlers/scripting`, `handlers/search`: re-home as named modules (`transaction.rs` etc.)
   outside `handlers/`, since EXEC is deliberately not a narrow-ConnCtx leaf.
4. **Delete `handlers/`** — directory gone; fix the stale category index doc in
   `handlers/mod.rs` by deleting it with the module; final doc-comment sweep so no text
   references the retired layout.

## Acceptance criteria

- [ ] Phase 1 inventory table committed (in this issue's Comments or a sibling doc)
- [ ] `frogdb-server/crates/server/src/connection/handlers/` no longer exists
- [ ] No behavior change: full server suite green at each phase boundary
- [ ] Doc-comments/module docs reference only the current layout
- [ ] `cargo clippy -p frogdb-server --all-targets -- -D warnings` clean

## Blocked by

None - can start immediately

## Source

`.scratch/arch-deepening/proposals/03-conn-command-unification.md` part (a); round-8 P03 agent report.
