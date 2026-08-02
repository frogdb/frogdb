# Staged replication metadata is consumed even when persisting the reconciled state failed

Status: done
Type: bug (data safety / replication)
Severity: likelihood 1/3 (needs a write failure on the state file in the window after a full sync),
consequence 3/3 (a replica resumes from an offset that does not match its dataset, silently) —
score 3
Area: recovery / replication state

## Problem

Recovery phase 5 adopts the replication id and offset that shipped inside an installed full-sync
checkpoint, persists the reconciled state, then deletes the staging file
(`frogdb-server/crates/recovery/src/replication.rs:49-54`):

```rust
repl_state.apply_staged_metadata(&meta);
if let Err(e) = repl_state.save(&state_path) {
    warn!(error = %e, "Failed to persist reconciled replication state");
}
// Consume the staging file so later restarts use the state file.
frogdb_core::consume_staged_replication_metadata(inputs.data_dir);
```

The save is warn-only; the consume is unconditional. If the save fails — a full disk, a permissions
change, a transient IO error — this boot still runs correctly (the in-memory `repl_state` is
right), but the on-disk state file keeps the node's *pre-full-sync* offset while the staging file
that could have corrected it is gone.

On the next restart the node loads that stale offset and pairs it with the dataset the full sync
installed. Because the replication id it carries is now the primary's (the staged id was adopted
in memory but, by assumption, not saved — so the id is the *old* one, and a valid `PSYNC <replid>
<offset>` is attempted against whichever id survived), the node can be granted a **partial** resync
from a position its data does not correspond to. That is strictly worse than the full resync a
zeroed offset would have forced: the missing range is never shipped and the divergence is silent.

The asymmetry is the bug, not the warn: everything else on this path is careful to fall back to
"offset 0, force a full resync" rather than trust a doubtful position (corrupt metadata is treated
as absent, an unparseable state file is regenerated — FM-PERSISTENCE-038). Only this window can
leave a *plausible but wrong* offset behind.

Current behavior noted in FM-PERSISTENCE-039 in
`.scratch/hardening/specs/persistence-failure-modes.md`.

## Candidate fixes

1. **Only consume after a successful save.** Two lines: make the consume conditional on
   `save(...)` returning `Ok`. A leftover staging file is re-adopted on the next boot, which is
   idempotent — the metadata describes a dataset that is already installed — so the retry is safe
   and self-healing. Preferred.
2. **Fail the phase.** Promote the save failure to a `RecoveryError` and refuse to start. Correct
   but harsh: the node is presently able to serve, and a full disk becomes an outage instead of a
   degraded boot.
3. **Zero the offset on save failure.** Keep the consume, but make the in-memory state fall back to
   offset 0 so the next sync is full. Safe, but throws away a good offset for *this* boot's
   replication as well.

## Forcing test

A `frogdb-recovery` seam test that stages a `replication_metadata.json`, makes the state file path
unwritable (a read-only file, or a directory where the file should be), runs `recover`, and asserts
the staging file still exists afterwards. The existing
`staged_replication_metadata_is_adopted_and_consumed` test has all the fixtures.

## Resolution

Candidate fix 1 (only consume after a successful save), as filed. In
`frogdb-server/crates/recovery/src/replication.rs` the consume is now the `Ok` arm of the save:

```rust
match repl_state.save(&state_path) {
    Ok(()) => frogdb_core::consume_staged_replication_metadata(inputs.data_dir),
    Err(e) => warn!(
        error = %e,
        "Failed to persist reconciled replication state; keeping the staged metadata so the \
         next boot re-adopts it"
    ),
}
```

The boot still proceeds on the correct in-memory state — a full disk stays a degraded boot, not an
outage — but the staging file survives, so the next boot re-adopts it. Re-adoption is idempotent:
the metadata describes a dataset that is already installed.

Forcing test: `staged_metadata_survives_a_failed_state_save` (`frogdb-recovery`), which blocks the
atomic write by creating a *directory* where `ReplicationState::save` needs its temp file, then
asserts the staging file is still present after `recover` and that the in-memory state adopted the
staged replid/offset anyway.

Spec: FM-PERSISTENCE-039 retitled to "…consumed only once it is durable"; the failed-save case is
now stated in Observable/NOT observable and the consumption ordering in Invariant.
