# 23: account the replica feed under NetworkOutput (D4 second half)

Status: ready-for-agent
Type: AFK
Origin: issue 18's review, 2026-09-01 — [PRD.md](../../PRD.md) D4
Area: frogdb-replication / frogdb-replication-runtime (LOCKED) + frogdb-memory + server connection handoff
Phase: 5 — after issue 18 lands

## Why

D4 rules that replica feed buffers account under `Subsystem::NetworkOutput` with issue 18's
`replica` output-limit class. Issue 18 delivered the class and the seam, but the PSYNC
handoff (`connection.rs` `run()` → `PrimaryReplicationHandler`) takes
`self.framed.into_inner()`, drops the `ConnectionHandler` and with it the
`OutputBufferAccount`'s `Charge`. From that point the feed writes with its own buffers and
no `Subsystem::` charge exists anywhere in `crates/replication*`. Net effect today:

- The `replica` class in `client-output-buffer-limit` is live only in the REPLCONF→PSYNC
  window, where nothing large is ever buffered — the `replica 256mb 64mb 60` limit is
  decorative.
- Replica feed bytes are charged **nowhere** — a slow replica's buffered feed is invisible
  to the budget breakdown and to `CLIENT LIST` omem.

Issue 18's fix round documents this window at the spec row, config docs, and handoff code;
this issue closes the gap for real.

## What to build

Spec-first — replication is a LOCKED area (`specs/replication.md`), so the behavior change
starts as a failure-mode row (slow replica → feed buffer charged, visible, limited →
disconnect at the replica-class hard limit / soft window), then a failing test, then the
implementation:

1. Charge the primary→replica feed buffers to `Subsystem::NetworkOutput` at a single seam
   in the feed path (same absolute-figure `set_buffered` style issue 18 used — no matched
   +/- bookkeeping). Backlog stays `ReplicationBacklog`, WAL stays `WalChannel` (D4).
2. Enforce the `replica` class limits on the feed: hard → disconnect replica; soft +
   window → disconnect after window. Redis semantics (`client-output-buffer-limit slave`).
3. `CLIENT LIST`/`INFO` omem for a replica connection reflects the feed buffer.
4. Carry the charge across the PSYNC handoff instead of dropping it, or open a fresh one in
   the replication handler — either way no uncharged gap between handoff and first feed
   write.
5. Update the FM-MEMORY-001 row cell and config docs written by issue 18's fix round to
   drop the "pre-PSYNC window only" caveat.

## Acceptance criteria

- [ ] New FM row(s) in `specs/replication.md` (and/or `specs/memory.md` budget-seam side)
      with forcing tests in the owning crate; `just lint-spec` green.
- [ ] Slow-replica e2e test: feed backs up → omem nonzero → hard limit disconnects.
- [ ] Budget breakdown shows replica feed bytes under `NetworkOutput` under load.
- [ ] `just mutants-diff frogdb-replication` run before push (LOCKED-area discipline).
- [ ] Issue 18's window caveats removed.

## Out of scope

Changing backlog/WAL subsystem attribution (D4 rules them settled), feed flow control or
partial-sync semantics, new config knobs beyond the existing `replica` class line.

## Depends on

Issue 18 landed. [Issue 19](../) landed 2026-09-03 without touching feed buffer ownership
(triaged 2026-09-04: no coordination needed). [Issue 21](../) landed 2026-09-04 and added
the replica-side `TxnBuffering` charge (FM-REPLICATION-045) in `apply.rs` — a different
subsystem; this issue's `NetworkOutput` seam sits on the primary's feed path.
