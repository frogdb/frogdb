# Boundary signatures — compact, high-signal test suite

State: active

Design session 2026-09-02. Classify every command by the shape it presents at the four
application boundaries (WAL sink, replica stream, cluster routing / txn queueing, blocking
waiters) as a read-only projection of `CommandSpec`; test each distinct shape once per boundary
in `sig_*` binaries plus the boundary crates; select the whole set with one nextest filter
(`just test-core`); keep it honest with `specs/signatures.md` under `spec-lint`.

- [`PRD.md`](PRD.md) — the design, decisions log (§9), deferred work (§10), issue order (§11)
- Issues 01–09 are the build; 10 (crate split) and 11 (`KeyspaceEffect` extraction) are
  deferred follow-ups filed at the user's request

Issues: [open](issues/open/) / [done](issues/done/)
