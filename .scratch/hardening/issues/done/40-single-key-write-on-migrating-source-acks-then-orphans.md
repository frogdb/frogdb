# Single-key write on a migrating source acks +OK locally, then SETSLOT orphans it (acked-write loss, fault-free)

Status: done
Type: AFK
Origin: issue-31 jepsen forensics, 2026-08-05 ("Bug X" — slot-migration-partition suite: writes
90..120 acked and read back, then every post-finalization read returns 89; loss occurred 1.9s
BEFORE the nemesis partition started, i.e. in a healthy cluster)
Severity: likelihood 3/3 (every single-key write to a migrating slot whose key was already
migrated), consequence 3/3 (acked writes silently destroyed at handoff)
Area: Cluster / migration routing — Phase 4 scope

## Mechanism (traced in code)

- `route_with_snapshot` (`frogdb-server/crates/server/src/slot_migration/routing.rs:139-146`)
  returns `LocalServeMigrating` from ownership alone — never consults key presence.
- `check_migrating_multikey` (`connection/guards.rs:871-873`) bails on `keys.len() < 2`, so
  single-key commands never get the migrating-source presence probe.
- `migrating_ask_for_nil` (`guards.rs:181-183`) fires only on a **nil reply** — `GET` of a
  migrated key gets `-ASK`, but `SET` replies `+OK` and executes locally, recreating the key
  on the source.
- `CLUSTER SETSLOT NODE` hands the slot away; the source's recreated copy is the orphan the
  jepsen `read-orphans` checker reports.

Redis checks key presence pre-execution for ALL arities on the migrating side
(`cluster.c:1436-1461`: all-present → serve, none-present → -ASK, partial → -TRYAGAIN).
FrogDB's EXEC/batch path is already correct (`ProbeMigratingSource`); only the single-key
path lacks the probe.

## Fix direction

Drop the `keys.len() < 2` gate so single-key commands on a migrating source take the same
presence probe (absent → -ASK before execution); retire the reply-side `migrating_ask_for_nil`
hack once the pre-execution probe covers reads too. **FM-CLUSTER-028 currently documents the
buggy behavior and self-contradicts between its Observable and NOT-observable cells — rewrite
the row first (spec-first), then failing test, then fix.** The failing test shape: migrate a
key to the target, then SET it via the source; expect -ASK, not +OK; then finalize and prove
the write is not orphaned.

## Related

- Issue 31 (root-cause analysis, evidence, and the harness-side follow-ups)
- NOT the FM-CLUSTER-037 commit→apply window; NOT fixed by rework 02's barrier (writes were
  accepted seconds before finalization).

## Resolution (2026-08-05)

Fixed. The MIGRATING source now decides by key presence, before execution, at every arity —
plain, scripted, inside or outside `MULTI`.

**Spec first.** FM-CLUSTER-028 was rewritten to the correct contract; its Observable /
NOT-observable contradiction is resolved, and the residual paragraph under FM-CLUSTER-030 that
recorded the un-hoisted probe as a known gap now records it as closed.

**Shape of the fix.**

- `route_migrating_source` (`slot_migration/routing.rs`) is the new shared routing half:
  "we own the slot, a migration off it is open, and the ASK address renders" → `Some(target)`.
  It reuses `route_with_snapshot`'s `LocalServeMigrating` arm and `migration_target_addr`, so
  the per-command path and the batch path cannot drift on *who* to redirect to.
- `KeyPresence::migrating_source_reply` (`connection/guards.rs`) is the single presence→reply
  policy: present → serve, absent → `-ASK`, mixed → `-TRYAGAIN`, unavailable → refuse. Both
  `check_migrating_source` and `validate_queued_batch`'s `ProbeMigratingSource` arm route
  through it; the EXEC path's behavior is unchanged, byte for byte.
- `check_migrating_multikey` → `check_migrating_source`: the `keys.len() < 2` gate is gone,
  cluster-exempt (node-scoped) commands are skipped as they are for slot validation, and slot
  state is read before any keyspace lookup so a non-migrating slot still costs zero probes.
- `migrating_ask_for_nil`, `is_nil_response` and `ask_response` are deleted. The hack was
  over-inclusive (`HGET` on a live hash, `LPOS`, a `BLPOP` timeout all reply nil while the key
  is still here → spurious `ASK` to a node holding nothing) and under-inclusive (every write),
  so it was unsound for writes by construction.
- `DispatchStage::MigratingTryAgain` → `MigratingSourceProbe`, hoisted in `PRE_DISPATCH_ORDER`
  to sit directly after `ClusterSlotValidation` and ahead of `ConnectionCommand`/`ServerWide`,
  pinned by three new `MUST_PRECEDE` pairs. Without the hoist a bare `EVAL` writing an
  already-migrated key still acked on the source — the same orphan, reached through the
  scripting family.

**Forced by** — unit: `migrating_source_probe_names_the_importing_node`,
`migrating_source_probe_is_none_without_an_open_migration`,
`migrating_source_probe_is_none_when_the_ask_address_is_unknown` (routing);
`migrating_source_asks_a_single_key_write_whose_key_moved`,
`migrating_source_serves_a_single_key_command_still_held_here`,
`migrating_source_tryagain_when_the_keys_straddle_the_slot`,
`migrating_source_probes_the_scripting_family`,
`migrating_source_refuses_when_the_shard_does_not_answer`,
`migrating_source_probe_is_skipped_when_the_slot_is_not_migrating`,
`migrating_source_probe_is_skipped_for_node_scoped_commands` (guards). Integration:
`test_single_key_write_on_migrating_source_asks_and_never_orphans` — with the arity gate
temporarily restored it fails exactly as the jepsen run did
(`got Simple(b"OK")` where an `ASK` was required).

That integration test also carries the end-to-end no-orphan proof (the source's physical
`DBSIZE` is `0` both before and after `SETSLOT NODE`, and the target's migrated values survive
untouched), which is what the `slot-migration` workload's `:orphaned-keys` gate can later be
tightened against. Issue 31 stays open — its P2/P3 harness follow-ups are unaffected by this.
