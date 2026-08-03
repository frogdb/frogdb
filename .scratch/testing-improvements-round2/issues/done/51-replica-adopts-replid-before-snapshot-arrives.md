# A replica adopts the primary's replid + offset before any snapshot byte arrives

Status: done
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

- [x] New table-driven crate test: for each of {EOF mid-file-transfer, checksum mismatch,
      malformed `+FULLRESYNC` (2 fields), non-numeric offset, `-ERR` PSYNC reply}, assert that
      after the failed attempt `ReplicaOffset::current()` and `ReplicationState::replication_id`
      are **unchanged from before the attempt**. **Fails today** for the transport, checksum and
      missing-installer rows.
- [x] The same test asserts `psync_request_args` then produces `PSYNC ? -1` (or the pre-attempt
      pair) on the next connect — never the primary's freshly minted pair.
- [x] The `connection.rs:182`/`:207` psync error closures gain coverage.
- [x] The doc comment at `:270-280` matches the post-fix behaviour.

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

## Resolution

**Half the premise was already fixed; the other half was live and is fixed here.**

*Already fixed* — the **offset** half. Commit `1ea25181` ("a persistence-disabled primary ships
its live dataset on full sync") replaced the early `self.offsets.reset_to(new_offset)` with
`reset_to(0)`, so `psync` has not adopted the granted offset for some time. That commit also moved
both receive paths to "install, then adopt" (`install_payload` runs before
`adopt_replication_history` + `reset_to(metadata offset)` in `receive_snapshot` and
`receive_checkpoint`) and made `install_payload` rewind to 0 on installer error. The doc comment
the issue calls out as lying (`receive_checkpoint`'s "**Install before adopt**") therefore now
describes what the code does. The issue's line numbers are stale by the same commit.

*Live* — the **identity** half. `psync` still ran
`self.state.write().adopt_replication_history(new_repl_id)` immediately after parsing
`+FULLRESYNC`, before a single payload byte. That is two mutations, both wrong before the dataset
lands: the node advertises the *incoming* primary's replid over a keyspace it has not received, and
`adopt_replication_history` clears the failover window (`secondary_id`/`secondary_offset`) that
describes the keyspace it is still serving. Every failure past the grant — socket death, an
envelope marker it cannot install, a non-payload line, a truncated transfer, a checksum mismatch,
a missing installer — left that adoption behind.

Redis is the precedent for the fix, not for the old code:
`slaveTryPartialResynchronization` parks the granted id in `server.master_replid` — a
*cached-master* field, not the node's own history — and only `readSyncBulkPayload`, after the RDB
has loaded, runs `memcpy(server.replid, server.master->replid, ...)` followed by
`clearReplicationId2()`.

### Fix

`frogdb-server/crates/replication/src/replica/connection.rs` — deleted the psync-time
`adopt_replication_history` call (the `reset_to(0)` beside it stays) and rewrote the comment to
state the whole rule: on `+FULLRESYNC` neither half of the granted pair is adopted; the id comes
from the payload's own trailer, via the receive paths, after the install. No staging field was
needed (what the issue's step 1 proposed) — the trailer already carries the id, so the granted one
is simply not needed before then.

### Tests

- `a_full_sync_that_never_delivers_a_dataset_leaves_the_old_history_alone` (table-driven, six rows:
  socket death on the grant, an uninstallable payload marker, a non-payload envelope line, a
  malformed grant with no offset, a non-numeric granted offset, and a `-ERR` PSYNC reply). Each row
  asserts the replid, `secondary_id` and `secondary_offset` are all unchanged, that the granted
  offset was not adopted, that the connection did not reach `Streaming`, and that the *next*
  `psync_request_args` is exactly the pair the row implies — `("?", -1)` for rows that failed after
  the grant was understood (the head has been rewound), the pre-attempt pair for rows that failed
  before it (nothing about this node changed). The three post-grant rows are **red** against the
  old code (`left: "0123…"` vs `right: "old-primary-replid…"`); they also cover the two previously
  untested psync error closures, along with the `-ERR` and malformed-grant rows.
- `a_checkpoint_that_dies_mid_transfer_leaves_the_old_history_alone` — the issue's
  "EOF mid-file-transfer" row at the layer that owns it (`receive_checkpoint` over a truncated
  body). Not red pre-fix (it never runs `psync`); it pins the same invariant one layer down, where
  the trailer that carries the id never arrives. The checksum-mismatch and missing-installer rows
  were already covered by `receive_snapshot_rejects_a_corrupted_dataset` and
  `receive_snapshot_without_an_installer_fails_the_sync`.

### Failure-mode spec

FM-REPLICATION-001 extended rather than split — this is the same invariant the row already
carries ("a granted full resync always carries the primary's dataset"), stated for identity as well
as data: the "NOT observable" cell now names the premature replid adoption and the dropped failover
window, the "Invariant" cell states that `psync` adopts *neither* half of the granted pair, and both
new tests are in "Forced by".
