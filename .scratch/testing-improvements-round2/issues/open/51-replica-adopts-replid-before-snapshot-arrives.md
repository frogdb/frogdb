# A replica adopts the primary's replid + offset before any snapshot byte arrives

Status: ready-for-agent
Type: AFK
Origin: round-2 testing audit 2026-07-28 — 15 parallel area audits, `.scratch/testing-improvements-round2/`
Source: proposals/14 F1 · MASTER.md §3
Score: severity 5 · likelihood 4 · effort 2 · priority 21
Area: frogdb-replication / replica connection

## Context

The replica writes the primary's replication id and offset immediately after parsing
`+FULLRESYNC`, before receiving a single byte of the snapshot. Only one of four failure modes
rewinds it. A transport failure, a checksum mismatch, or a missing installer therefore leaves the
replica advertising an identity and offset for a dataset it never received; the next reconnect is
granted `+CONTINUE` and streams deltas onto the *old* keyspace — permanent, silent divergence with
`master_link_status:up`. Any interrupted full resync lands here, and checkpoint transfers are the
long pole of replication: a network blip, a primary restart mid-transfer, or a disk-full on the
staging dir all qualify. The doc comment on the function asserts the opposite of what the code does.

**This is a suspected live defect found by reading, not by test failure — the proposed test fails
against today's code.** The evidence is the auditing agent's and needs confirmation before or
during the fix.

## Evidence

- `frogdb-server/crates/replication/src/replica/connection.rs:191-192` —
  `self.state.write().await.replication_id = new_repl_id.clone(); self.offsets.reset_to(new_offset);`
  runs immediately after parsing `+FULLRESYNC`, *before* `receive_rdb` / `receive_checkpoint`.
- The only rewind is `:329` (`self.offsets.reset_to(0)`) inside `install_staged_checkpoint`,
  reached only when the injected installer returns `Err`. A failure in `receive_checkpoint_files`
  (`:294`, transport) or `stager.commit` (`:298`, checksum mismatch) returns `Err` with the adopted
  pair intact, and a missing installer (`:319-326`) returns `Ok(())` and then adopts the staged
  offset over an untouched keyspace.
- The doc-comment at `:270-280` asserts the opposite ("The offset is adopted only after the install
  succeeds") — it describes `receive_checkpoint`'s second adopt and is blind to the first.
- `reset_to` writes the handler-owned shared atomic (`replica/offset.rs:53-55`), so the adoption
  survives the reconnect.
- **Why the existing tests pass anyway**: `connection.rs:182` and `:207` (the two `psync` error
  closures) are `untested`; the three existing checkpoint tests (`:522`, `:546`, `:588`) all take
  the success or installer-failure path.

## What to fix

1. Defer adopting `replication_id` and the offset until the snapshot is fully installed — stage the
   parsed pair and commit it at the same point `install_staged_checkpoint` succeeds.
2. Make every failure path (transport, checksum, missing installer) leave the pre-attempt pair
   intact, so the next `psync_request_args` produces `PSYNC ? -1` (or the pre-attempt pair).
3. Fix the doc comment at `:270-280`, which currently documents behaviour the code does not have.

## Acceptance criteria

- [ ] New table-driven crate test: for each of {EOF mid-file-transfer, checksum mismatch,
      malformed `+FULLRESYNC` (2 fields), non-numeric offset, `-ERR` PSYNC reply}, assert that
      after the failed attempt `ReplicaOffset::current()` and `ReplicationState::replication_id`
      are **unchanged from before the attempt**. **Fails today** for the transport, checksum and
      missing-installer rows.
- [ ] The same test asserts `psync_request_args` then produces `PSYNC ? -1` (or the pre-attempt
      pair) on the next connect — never the primary's freshly minted pair.
- [ ] The `connection.rs:182`/`:207` psync error closures gain coverage.
- [ ] The doc comment at `:270-280` matches the post-fix behaviour.

## Test boundary

**2** (crate-level API) — the behaviour is entirely inside `ReplicaConnection` against an injected
stream + installer, both of which the crate already has. Not level 5: a server would add a socket
and a real RocksDB checkpoint without exercising anything extra, and would make the failure
injection non-deterministic.

## Depends on

Nothing — the crate already has a scripted fake stream and an injectable installer, so this is a
table of scripted responses. Related: issue 52, `.scratch/testing-improvements-round2/issues/`
(backlog eviction between PSYNC grant and tail re-extraction) — both concern the full-sync handoff
and should be reviewed together.
