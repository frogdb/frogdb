# FUNCTION libraries are never replicated and never enter the RDB

Status: done
Type: AFK
Origin: round-2 testing audit 2026-07-28 — 15 parallel area audits, `.scratch/testing-improvements-round2/`
Source: proposals/09 F5 · MASTER.md §3
Score: severity 4 · likelihood 4 · effort 3 · priority 17
Area: frogdb-server / connection scripting (FUNCTION)

## Context

`FUNCTION LOAD`/`DELETE`/`FLUSH`/`RESTORE` mutate the local registry and write
`<data_dir>/functions.fdb`, and that is all: there is no propagation to replicas and no
representation in the snapshot or full-sync payload. A replica therefore has no functions, a
promoted replica fails every `FCALL` with "function not found" until an operator re-loads by hand,
and with persistence disabled the libraries are lost outright. Failover is an ordinary ops event
and building a fresh replica is the normal way to add capacity, so applications built on FUNCTION
break wholesale at exactly the moment they need to keep working.

**This is a suspected live defect found by reading, not by test failure — the proposed test fails
against today's code.** The evidence is the auditing agent's and needs confirmation before or
during the fix.

## Evidence

- `FUNCTION` is a `ConnectionLevel` command handled entirely in
  `server/src/connection/scripting/function.rs`; `handle_function_load`/`_delete`/`_flush`/
  `_restore` mutate `self.admin.function_registry` and then call `persist_functions()`, which
  writes `<data_dir>/functions.fdb` **only when `config_manager.persistence_enabled()`**.
- There is no propagation call on any of those paths — `rg` for `FunctionLoad|function_load` under
  `core/src/replication`, `core/src/persistence` and `crates/persistence` returns nothing, and
  `rg -l 'function_registry|FunctionLibrary' crates/persistence/` is empty, so the snapshot format
  carries no libraries either.
- Recovery reads the file back at `server/src/recovery/functions.rs:20-21`, which is a
  *local-restart* path only.
- **Why the existing tests pass anyway**: the three restart tests in
  `server/tests/functions.rs:652-792` cover exactly that local-restart path and nothing else;
  `functions_tcl.rs` excludes replication as "needs:repl".

## What to fix

1. Propagate `FUNCTION LOAD`/`DELETE`/`FLUSH`/`RESTORE` to replicas on the replication stream.
2. Include the function registry in the snapshot / full-sync payload so a freshly attached replica
   starts with the libraries it needs.
3. Define and document the persistence-disabled behaviour rather than leaving it undefined.

## Acceptance criteria

- [x] New multi-node test: `FUNCTION LOAD` on the primary, wait for sync, assert `FUNCTION LIST` on
      the replica contains the library and `FCALL_RO` on the replica returns the expected value.
      **Fails today.**
- [x] Promote the replica and assert `FCALL` still works.
- [x] Second test: with persistence disabled, `FUNCTION LOAD` then restart — asserts the documented
      behaviour (whatever is chosen) rather than leaving it undefined.
- [x] A freshly attached replica (full sync, no prior state) has the libraries without any local
      `functions.fdb`.

## Test boundary

**5** (multi-node) — replication and promotion *are* the behaviour; there is no lower level at
which "the replica has the library" is expressible. The existing `start_primary`/`start_replica`
harness is sufficient.

## Depends on

Round-1 issue 66 (minimal-RDB full-sync carries no dataset), `.scratch/testing-improvements/issues/`
— `MASTER.md` §8 flags it as possibly sharing a root cause with this finding; check before
designing the snapshot half.

## Resolution (2026-08-02)

**Premise confirmed, not stale.** `FUNCTION LOAD/DELETE/FLUSH/RESTORE` mutated
`admin.function_registry` and wrote `functions.fdb`, and nothing else: no propagation call on any
of the four paths, and no representation in the full-sync payload. A replica answered
"Function not found" for every library, and a promoted replica kept answering that forever. Proven
red before the fix: with the propagation disabled, four of the five new integration tests fail
(the fifth covers the `-READONLY` gate, which is a separate hole found on the way — see below).

### Root cause

The registry is *process-wide state that lives beside the keyspace*: one `SharedFunctionRegistry`
per node, persisted to its own `functions.fdb`, never touched by the write path that carries keys.
Every mechanism that crosses the link — shard-tagged write frames, the checkpoint/live-dataset
full-sync envelope — is keyspace-shaped, so there was no lane the registry could travel in and
none was built.

### Fix shape

1. **A single owner.** New `frogdb-server/src/function_store.rs` — `FunctionStore` is the only
   mutator of the registry. The connection handlers and the replica's apply loop call the same four
   methods, so "what propagates" and "what a replica applies" cannot drift apart.
   `MUTATING_SUBCOMMANDS` is the one list both sides read.
2. **A control lane for the steady state.** The four subcommands broadcast a `CONTROL_SHARD`
   (`u16::MAX`) tagged `FUNCTION` frame — `PrimaryReplicationHandler::broadcast_control_command`
   (the old `broadcast_command`, renamed for what it does). Shard-tagging was rejected: the
   registry is per-process, so a shard id would make the frame undeliverable the moment the two
   nodes' shard counts differ. The replica applies it through a new **synchronous**
   `ControlApplier` trait, invoked inline in `consume_frames` (after `REPLCONF`/`FROGDB.FINALIZE`,
   before MULTI reconstruction), so a control frame cannot be reordered against the write stream
   nor block it on I/O; a rejected apply is `ApplyError::ControlRejected` and diverges the link
   rather than being skipped. Propagation is post-hoc: an errored mutation returns before it.
3. **A snapshot for the full sync.** `set_function_snapshot_hook` on the primary handler, called
   from `handle_full` immediately after `snapshot_offset` is captured, broadcasts one whole-registry
   `FUNCTION RESTORE <dump> FLUSH`. Being *after* the capture is what puts it in the
   `(snapshot_offset, current]` range `start_streaming` replays before the live tail. `FLUSH`
   policy (not `APPEND`/`REPLACE`) so a replica that booted with its own `functions.fdb` converges
   on exactly the primary's set; `restore` flushes and loads under one write lock, so no
   intermediate set is observable.
4. **An ordering lock.** A process-global `PROPAGATION_ORDER` mutex is held across
   *mutate-then-broadcast* on the client path and *snapshot-then-broadcast* on the sync path.
   Without it a snapshot read taken before a concurrent `FUNCTION LOAD` could be broadcast *after*
   that load's own frame and silently erase it on the replica.
5. **A `-READONLY` gate** on the four mutating subcommands (reads still served) — see divergences.

### Divergences from the issue text, called out

* **The full-sync half is a replicated command, not part of the payload.** The issue asks to
  "include the function registry in the snapshot / full-sync payload". `FullSyncMetadata` is a
  strict four-part colon-joined trailer and the `$FROGDB_CHECKPOINT` file list is staged for
  RocksDB to open (the `LiveDataset` branch has no file list at all), so neither has room for an
  unrelated blob. Carrying it as a frame instead makes one mechanism serve both sync flavours and
  the steady state. Cost: the dump is re-sent on every resync, and it lands just *after* the
  dataset rather than atomically with it — a few-millisecond window where a synced replica has the
  keys but not the libraries. Recorded in the spec's Redis-deviations table.
* **New client-visible behaviour: `FUNCTION LOAD/DELETE/FLUSH/RESTORE` on a replica now answer
  `-READONLY`.** Not in the issue. Found while fixing it: `FUNCTION_SPEC` carries only
  `CommandFlags::NOSCRIPT`, so the generic WRITE-flag gate in `guards.rs` never fired and a client
  could load a library straight onto a replica — invisible upstream, and promotable into the
  authoritative set by a failover. Redis draws the same line (`function|load` is flagged a write,
  `function|list` is not). Read subcommands are unaffected.
* **Criterion 3's "documented behaviour" was chosen, not just asserted.** With persistence
  disabled the libraries do not survive a restart and no `functions.fdb` is written at all —
  Redis parity (the registry rides the RDB there, so no RDB means no libraries) and, more to the
  point, `persistence disabled` keeps meaning exactly one thing. A persistence-disabled replica
  gets its libraries from the primary's resync instead.

### One self-inflicted bug, found and fixed during verification

The first wiring parked a whole `FunctionStore` inside the primary handler's full-resync hook. The
store holds an `Arc<ConfigManager>`, and the config manager transitively owns the snapshot
coordinator and the shard notifier — i.e. the storage engine — so the hook closed a reference cycle
that kept RocksDB open past `shutdown()`. Every in-process restart in the suite then failed its
second boot with `IO error: lock hold by current process ... LOCK: No locks available` (28 tests:
all of `integration_persistence`'s restart set, three `integration_replication` identity tests, the
cluster restart/rolling tests, and `search::test_ft_survives_restart`). Bisected against a clean
HEAD, narrowed to the hook, and fixed by making `snapshot_command_args` a free function over the
registry alone — the snapshot needs no config, so the hook now captures nothing that can reach the
storage engine. The reasoning is recorded at the function so it is not re-introduced.

### Tests

`frogdb-server/crates/server/tests/integration_replication_functions.rs` (new, level 5):

* `a_function_loaded_on_the_primary_reaches_an_attached_replica` — FM-REPLICATION-054
* `function_delete_removes_one_library_on_the_replica_and_keeps_the_rest` — FM-REPLICATION-054
* `a_replica_that_full_syncs_receives_the_primarys_existing_libraries` — FM-REPLICATION-055
* `a_promoted_replica_keeps_the_libraries_it_replicated` — FM-REPLICATION-056
* `a_client_can_not_load_a_function_on_a_replica` — FM-REPLICATION-057

`frogdb-server/crates/server/src/function_store.rs`:
`only_the_four_state_changing_subcommands_replicate` — FM-REPLICATION-054.

`frogdb-server/crates/server/tests/functions.rs`:
`test_functions_are_lost_on_restart_when_persistence_is_disabled` (criterion 3).

### Spec

Four new rows in `.scratch/hardening/specs/replication-failure-modes.md` — FM-REPLICATION-054
(state-changing subcommands cross the link, and only those), -055 (a full resync carries the whole
registry), -056 (a promoted replica keeps them), -057 (a replica refuses client mutations) — plus a
"Scope, part three — the control lane" paragraph and one Redis-deviations row.

### Depends-on

Round-1 issue 66 (minimal-RDB full sync carries no dataset) is already fixed on main
(FM-REPLICATION-001), and the fix here does not touch the payload path, so there is no shared root
cause left to coordinate.
