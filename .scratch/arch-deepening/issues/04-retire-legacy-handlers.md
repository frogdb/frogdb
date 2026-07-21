# 04 — Retire the legacy `handlers/` directory (~35 files)

Status: ready-for-human

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

- [x] Phase 1 inventory table committed (in this issue's Comments or a sibling doc)
- [x] `frogdb-server/crates/server/src/connection/handlers/` no longer exists
- [x] No behavior change: full server suite green at each phase boundary
- [x] Doc-comments/module docs reference only the current layout
- [x] `cargo clippy -p frogdb-server --all-targets -- -D warnings` clean

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

### 2026-07-20 — Phases 2-4 done, retired

`handlers/` is gone. Every body now lives beside its caller as a sibling module under
`connection/`, or (trivial single-caller cases) folded into the caller. No logic changed.

Per-phase commits (each independently green — `just check frogdb-server` + pre-commit clippy):

| Phase | Commit | What |
|---|---|---|
| 1 inventory | `cc654ea7` | inventory table (above) |
| 2A | `20c43a99` | scatter/timeseries_scatter/cluster/persistence(→`persistence_handler`) → `connection/`; SHUTDOWN inlined into the `ServerWideOp::Shutdown` arm; `admin.rs` dropped |
| 2B | `82d78c1b` | blocking(+coordinator)/hotkeys/slowlog → `connection/`; info/debug → `{info,debug}_handler.rs`; pubsub `BROADCAST_SHARD` folded into `pubsub_conn_command.rs`; Justfile audit guards retargeted |
| 3+4 | `efc25398` | transaction/scripting/search re-homed under `connection/`; `handlers/mod.rs` + `pub mod handlers` deleted; dead scripting `util` module removed (8 items, zero callers — the old `pub mod` chain had masked the dead-code lint); doc-comment sweep |

Final layout (what replaced `handlers/`) — all now direct children of `connection/`:
`blocking.rs` + `blocking/`, `cluster.rs`, `debug_handler.rs`, `hotkeys.rs`, `info_handler.rs`,
`persistence_handler.rs`, `scatter.rs`, `scripting/`, `search/`, `slowlog.rs`,
`timeseries_scatter.rs`, `transaction.rs`. (`admin.rs` and `pubsub.rs` did not survive as files;
their content folded into the caller / `pubsub_conn_command.rs`.)

Inventory tally: 13 files/dirs relocated, 2 folded into caller (admin SHUTDOWN, pubsub const),
1 dead module deleted (scripting `util`). No body could not be migrated.

Verification:
- `just check` (full workspace, `--all-targets`): green.
- `cargo clippy -p frogdb-server --all-targets -- -D warnings` (pre-commit hook on `efc25398`): clean.
- Targeted tests by affected family (all pass): transactions/EXEC 27; scripting 18; functions/FCALL
  21; search/FT.* 146; database (DBSIZE/FLUSHDB/KEYS scatter) 4; lists (BLPOP blocking) 30;
  timeseries (TS.MGET/MRANGE scatter) 8; info 8; persistence/MIGRATE 26; replication/WAIT 137;
  cluster (RAFT/slot-migration/RESET) 144; hotkeys 2. Total 571, 0 failures.
