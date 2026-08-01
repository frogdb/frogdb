# FUNCTION libraries are never replicated and never enter the RDB

Status: ready-for-agent
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

- [ ] New multi-node test: `FUNCTION LOAD` on the primary, wait for sync, assert `FUNCTION LIST` on
      the replica contains the library and `FCALL_RO` on the replica returns the expected value.
      **Fails today.**
- [ ] Promote the replica and assert `FCALL` still works.
- [ ] Second test: with persistence disabled, `FUNCTION LOAD` then restart — asserts the documented
      behaviour (whatever is chosen) rather than leaving it undefined.
- [ ] A freshly attached replica (full sync, no prior state) has the libraries without any local
      `functions.fdb`.

## Test boundary

**5** (multi-node) — replication and promotion *are* the behaviour; there is no lower level at
which "the replica has the library" is expressible. The existing `start_primary`/`start_replica`
harness is sufficient.

## Depends on

Round-1 issue 66 (minimal-RDB full-sync carries no dataset), `.scratch/testing-improvements/issues/`
— `MASTER.md` §8 flags it as possibly sharing a root cause with this finding; check before
designing the snapshot half.
