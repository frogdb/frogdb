# Per-subcommand ADMIN flags — audit + settled design input (2026-08-04)

Design input for [issues/open/05-cluster-admin-gating-breaks-client-bootstrap.md](issues/open/05-cluster-admin-gating-breaks-client-bootstrap.md).
Triage decision (user): **Option 1 — per-subcommand flags — full sweep** of blanket-ADMIN
container commands, mirroring Redis's per-subcommand admin marks. One client-breaking change.

## Settled scope decisions (orchestrator, from the audit below)

- **In sweep**: `CLUSTER`, `CONFIG`, `ACL`, `CLIENT`, `SLOWLOG`, `LATENCY`, `HOTKEYS`, `MEMORY`.
  (`LATENCY`/`SLOWLOG`/`HOTKEYS` weren't named in the issue but are blanket-ADMIN container
  commands with the identical defect shape; user chose "full sweep of blanket-ADMIN commands".)
- **DEBUG stays blanket-ADMIN** — Redis marks every DEBUG subcommand `@admin @dangerous`; no
  subcommand-level exception exists there, and FrogDB's custom DEBUG subcommands are all
  operator/test tooling. The mechanism may still express DEBUG as "all subcommands admin".
- **MEMORY**: not ADMIN-flagged today at all (issue premise wrong — spec is `READONLY|RANDOM`,
  `observability_conn_command.rs:284`). Sweep *adds* admin to `MEMORY PURGE` only (Redis parity);
  everything else stays non-admin.
- **CONFIG**: Redis marks GET as well as SET/RESETSTAT/REWRITE `@admin @dangerous` — behavior
  likely unchanged, but express it through the per-subcommand table so the mechanism is uniform.
  Verify against a real redis-server (`COMMAND INFO config|get` style checks) before finalizing.
- FrogDB-specific subcommands with no Redis analogue — classify by mutation semantics:
  mutating/server-affecting → admin (`HOTKEYS START/STOP/RESET`), informational → non-admin
  (`HOTKEYS GET`, `MEMORY MALLOC-SIZE`, `CLIENT STATS`, `LATENCY BANDS/HISTOGRAM`). Flag any
  judgment calls in the final report for orchestrator review.
- All UNCERTAIN rows in the parity table below must be verified against real Redis 7.x
  (redis-server is in the Brewfile) before the flag tables are committed.

## Audit report (read-only agent, 2026-08-04)

### 1. Current machinery

- `CommandSpec`: `frogdb-server/crates/core/src/command_spec.rs:471-509`. One `flags:
  CommandFlags` for the whole command — no per-subcommand field exists.
- `CommandFlags` bitflags (incl. `ADMIN`): `frogdb-server/crates/core/src/command.rs:887-960`.
- Admin-port guard: `frogdb-server/crates/server/src/connection/guards.rs:391-400` — runs on
  `cmd_name` alone, before args are inspected, returns `-NOADMIN`.
- `extract_subcommand(command, args) -> Option<String>`:
  `frogdb-server/crates/server/src/connection/util.rs:253-263`, keyed on the static
  `CONTAINER_COMMANDS` list (`util.rs:246-250`: ACL, CLIENT, CONFIG, CLUSTER, DEBUG, HOTKEYS,
  MEMORY, MODULE, OBJECT, SCRIPT, SLOWLOG, XGROUP, XINFO, COMMAND, PUBSUB, FUNCTION, LATENCY,
  STATUS, SELECT). Already used by the ACL check at `guards.rs:402-411` — the template to
  replicate for the admin gate.
- Subcommand dispatch everywhere is a hand-written `match` on `args[0]` inside `execute()` —
  no declarative table exists. Per-subcommand flags need a new structure (e.g. a
  `&'static [(&'static str, CommandFlags)]` on/next to `CommandSpec`, or a
  `subcommand_flags()` method) consulted by the guard and ideally docs-gen.
- Precedent: ACL's user-authored per-subcommand permission rules
  (`frogdb-server/crates/acl/src/permissions.rs:110-142, 180-370, 432-433`;
  `is_command_allowed(command, subcommand)`), proving the `command|subcommand` shape is
  idiomatic here. Also check `frogdb-server/crates/acl/src/categories/mod.rs`
  (`CommandCategory::Admin`, lines 62-195) for overlap before adding a third source of truth.

Registration sites / dispatch:

| Command | Spec | Registered | Dispatch |
|---|---|---|---|
| CLUSTER | `server/src/commands/cluster/mod.rs:86-103` | `register.rs:142` (shard-side `Command`, Standard) | match `mod.rs:118-216` |
| CONFIG | `connection/conn_command.rs:210-227` | `register.rs:97` | match `conn_command.rs:258-268` |
| ACL | `connection/acl_conn_command.rs:33-47` | `register.rs:122` | match `acl_conn_command.rs:82-107` |
| CLIENT | `connection/client_conn_command.rs:34-51` | `register.rs:92` | match `client_conn_command.rs:77-101` |
| DEBUG | `connection/debug_conn_command.rs:46-64` | `register.rs:105` | byte-slice match `debug_conn_command.rs:114+` |
| MEMORY | `connection/observability_conn_command.rs:281-295` | `register.rs:174` | match `observability_conn_command.rs:322-333` |
| SLOWLOG | `observability_conn_command.rs:122` | — | same file |
| LATENCY | `observability_conn_command.rs:514` | — | same file |
| HOTKEYS | `connection/hotkeys.rs:39` | — | same file |

### 2. Full ADMIN inventory (production commands)

Container commands (have subcommands): ACL, CLIENT, CONFIG, DEBUG, CLUSTER, SLOWLOG, LATENCY,
HOTKEYS — all blanket-ADMIN today except MEMORY (not ADMIN). MODULE (stubbed, blanket-ADMIN).

No-subcommand ADMIN commands (unaffected by the split, keep as-is): FROGDB.HOTSHARDS, MONITOR,
BGSAVE, SHUTDOWN, FROGDB.FINALIZE, SAVE (stub), BGREWRITEAOF (stub), SYNC (stub), REPLICAOF,
SLAVEOF, REPLCONF, PSYNC, PFDEBUG, PFSELFTEST. Note PFDEBUG's first arg is subcommand-shaped
but PFDEBUG is not in CONTAINER_COMMANDS — leave alone.

Not ADMIN today (and out of scope unless flags are being added): LASTSAVE, STATUS, WAIT, ROLE,
OBJECT, SCRIPT, XGROUP, XINFO, COMMAND, PUBSUB, FUNCTION, SELECT.

### 3. Redis 7.x parity table (verify UNCERTAIN rows against real redis-cli)

- **CLUSTER** non-admin: INFO, NODES, MYID, SLOTS, SHARDS, KEYSLOT, COUNTKEYSINSLOT,
  GETKEYSINSLOT, LINKS, HELP. Admin: MEET, FORGET, REPLICATE, FAILOVER, RESET, SAVECONFIG,
  SET-CONFIG-EPOCH, SETSLOT, ADDSLOTS, DELSLOTS, ADDSLOTSRANGE, DELSLOTSRANGE, BUMPEPOCH,
  FLUSHSLOTS.
- **CONFIG**: GET, SET, RESETSTAT, REWRITE all `@admin @dangerous` in Redis (GET included —
  verify). HELP non-admin.
- **ACL** non-admin: WHOAMI, LIST, USERS, CAT, GENPASS, GETUSER, LOG (read form), HELP.
  Admin: SETUSER, DELUSER, LOAD, SAVE, LOG RESET. DRYRUN: UNCERTAIN (verify).
- **CLIENT** non-admin: ID, GETNAME, SETNAME, INFO, LIST, TRACKINGINFO, GETREDIR, CACHING,
  TRACKING, REPLY, SETINFO, HELP. Admin: KILL, PAUSE, UNPAUSE. UNCERTAIN: UNBLOCK, NO-EVICT,
  NO-TOUCH (verify — likely admin). STATS is FrogDB-specific.
- **DEBUG**: all admin (stays blanket).
- **MEMORY** non-admin: DOCTOR, HELP, MALLOC-STATS, STATS, USAGE. Admin: PURGE.
  MALLOC-SIZE FrogDB-specific → non-admin (informational).
- **SLOWLOG**: RESET admin; GET, LEN, HELP non-admin.
- **LATENCY**: RESET admin; HISTORY, LATEST, DOCTOR, GRAPH, HELP non-admin; BANDS, HISTOGRAM
  FrogDB-specific → non-admin.
- **HOTKEYS** (FrogDB-only): START, STOP, RESET admin; GET non-admin.

### 4. Consumers of `CommandFlags::ADMIN`

| Consumer | Location | Note |
|---|---|---|
| Admin-port guard | `guards.rs:391-400` | the redesign site |
| Guard unit test | `guards.rs:1329-1345` | `run_pre_checks("DEBUG", &[])` with NO args — per-subcommand scheme needs a "no subcommand given" default (safe default: gate) |
| COMMAND INFO flags | `commands/src/basic.rs:214-216` | emits "admin" whole-command |
| COMMAND LIST FILTERBY ACLCAT | `commands/src/basic.rs:843` | whole-command |
| ACL categories (parallel system) | `acl/src/categories/mod.rs:62-195` | check overlap |
| docs-gen | `ops/docs-gen/src/main.rs:487-517` | dumps whole-command flag names into `website/src/data/commands.json` |
| Harness | `test-harness/src/cluster_harness.rs:360-391` `try_send_admin_aware` | see §6 |
| Pin test (invert, don't delete) | `server/tests/integration_admin_port.rs:117-156` | CLUSTER discovery NOADMIN pin; references issue 05 by name |
| Other admin-port tests | `integration_admin_port.rs:14-102, 162-220` | DEBUG SLEEP, CONFIG SET, SHUTDOWN, CONFIG-GET (that one admits it doesn't test CONFIG GET). No pin tests exist for ACL/CLIENT/MEMORY — NEW tests needed |

`NOADMIN` appears nowhere else (dispatch.rs:426 is a comment).

### 5. Goldens / docs-gen coupling

- `website/src/data/commands.json` is a golden enforced by the `docs-gen-check` CI job
  (`test.yml:325-349`, required at `:478`). Current: ACL `["ADMIN"]`, CLIENT
  `["NOSCRIPT","LOADING","STALE","ADMIN"]`, CLUSTER `["STALE","ADMIN"]`, CONFIG
  `["NOSCRIPT","LOADING","STALE","ADMIN"]`, DEBUG `[...,"ADMIN","MOVABLEKEYS"]`, MEMORY
  `["READONLY","RANDOM"]`. Any flag change ⇒ regenerate with `just docs-gen` (regenerating
  website data files is routine/mechanical — precedent commit c8db823c).
- `CommandInfo` (docs-gen) has no `subcommands` field; `COMMAND DOCS`/`INFO` RESP output is a
  stub with no subcommands array. Exposing per-subcommand flags there is OPTIONAL follow-up,
  not required for issue 05 acceptance.

### 6. Harness call sites for `try_send_admin_aware`

7 call sites. Discovery (simplify to plain send after the split): `cluster_harness.rs:752`
(CLUSTER INFO), `:876` (CLUSTER NODES). Mutating (keep admin-aware): `integration_cluster.rs:
2771, 3241, 12584` (FAILOVER TAKEOVER), `:12885` (REPLICATE), `cluster_harness.rs:1180`
(REPLICATE). Keep the helper itself; update its CLUSTER-specific justification comment
(`:362-368`).
