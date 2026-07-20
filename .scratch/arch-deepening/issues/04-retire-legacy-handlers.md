# 04 — Retire the legacy `handlers/` directory (~35 files)

Status: in-progress

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

## Comments

### 2026-07-20 — Phase 1 inventory

All remaining `handlers/` bodies are `impl ConnectionHandler` methods (or trait impls on
`ConnectionHandler`) dispatched from the data-driven `dispatch.rs` seam or a `*_conn_command.rs`
executor. The round-8 `ConnMutation` work finished dispatch-side unification; these bodies are the
shard-routed / server-wide / EXEC-orchestration work the proposal explicitly keeps behind
`ServerWideOp` / `route_and_execute`. None is dead. Disposition = flatten each out of the
`handlers/` ghetto to a sibling module under `connection/` (next to its `dispatch.rs` /
`*_conn_command.rs` caller), or fold trivially-tiny single-caller bodies into the caller file.

| handlers file | body / contents | live entry point (sole caller) | disposition |
|---|---|---|---|
| `admin.rs` | `handle_shutdown` | `dispatch_server_wide` (`ServerWideOp::Shutdown`) | fold into `dispatch.rs` |
| `pubsub.rs` | `BROADCAST_SHARD` const | cluster_bus, lifecycle, pubsub_conn_command | fold const into `pubsub_conn_command.rs` |
| `slowlog.rs` | `maybe_log_slow_query` | `connection.rs` command loop | → `connection/slowlog.rs` |
| `timeseries_scatter.rs` | `handle_ts_{queryindex,mget,mrange}` | `dispatch_server_wide` (`Ts*`) | → `connection/timeseries_scatter.rs` |
| `info.rs` | `InfoProvider` impl + `gather_info_sources` | `InfoProvider::render` trait | → `connection/info_handler.rs` |
| `blocking.rs` + `blocking/coordinator.rs` | `handle_blocking_wait`, `handle_wait_command` + coordinator | `dispatch.rs` | → `connection/blocking.rs` + `connection/blocking/` |
| `cluster.rs` | `handle_raft_command`, `handle_slot_migration`, `handle_reset_command` | `dispatch.rs` (`handle_internal_action`) | → `connection/cluster.rs` |
| `persistence.rs` | `handle_migrate_command`, `handle_migrate` | `dispatch.rs` | → `connection/persistence_handler.rs` |
| `scatter.rs` | `handle_{scan,keys,dbsize,randomkey,flushdb,flushall}` + `scatter_gather` | `dispatch_server_wide` | → `connection/scatter.rs` |
| `debug.rs` | `DebugProvider` impl | `DebugProvider` trait | → `connection/debug_handler.rs` |
| `hotkeys.rs` | `HOTKEYS_CONN_COMMAND` + `maybe_record_hotkeys` | `register.rs`, `connection.rs` | → `connection/hotkeys.rs` |
| `transaction.rs` | `handle_exec`, `execute_transaction`, … | `transaction_conn_command` (`handle_exec`) | Phase 3: → `connection/transaction.rs` |
| `scripting/{mod,eval,function,script}.rs` | `handle_{eval,evalsha,script,fcall,function}` + parse util | `scripting_conn_command.rs` | Phase 3: → `connection/scripting/` |
| `search/*.rs` (17 files) | `handle_ft_*`, `handle_es_all` + merge/helpers | `dispatch_server_wide` (`Ft*`/`EsAll`) | Phase 3: → `connection/search/` |

Summary: 15 bodies migrated (relocated beside caller), 2 folded into caller (`admin`, `pubsub`
const), 0 deleted-dead. Names disambiguated where a `*_conn_command.rs` sibling would collide
(`info_handler.rs`, `debug_handler.rs`, `persistence_handler.rs`).
