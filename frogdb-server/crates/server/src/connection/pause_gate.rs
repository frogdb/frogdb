//! The slot dimension of the `PauseGate` dispatch stage.
//!
//! A pause is either **node-global** — the operator's `CLIENT PAUSE`, Redis
//! semantics, unchanged — or **slot-scoped**: armed on one CRC16 hash slot by
//! the slot-migration finalization barrier, parking only the commands that can
//! reach that slot. This module holds the two decisions that scoping needs, as
//! pure functions over the command registry so they are testable without a
//! socket:
//!
//! - [`command_pause_slot`] — which slot a command's keys pin it to, if any.
//! - [`queue_pause_slot`] — the same question for a whole queued transaction,
//!   which is what `EXEC` asks through the [`TxnHost`](frogdb_txn::TxnHost)
//!   seam.
//! - [`exempt_from_slot_pause`] — the commands a slot-scoped pause must never
//!   park, on pain of deadlocking the very handover that armed it.
//!
//! The mode decision (what `PauseMode::Write` blocks) stays on the handler in
//! `lifecycle.rs`, where the function registry lives.

use bytes::Bytes;
use frogdb_core::CommandRegistry;

use crate::slot_migration::SlotValidator;

/// Commands a *slot-scoped* pause must never park.
///
/// Both entries are hazards the pause-barrier brief calls out by name, and both
/// are self-deadlocks rather than mere inconveniences:
///
/// - `MIGRATE` is `WRITE`-flagged and runs over an ordinary client connection on
///   the source node. It is how the source finishes copying the slot's keys to
///   the target — the work the barrier is waiting to complete. Parking it stalls
///   the handover behind a barrier that only the handover can release.
/// - `CLUSTER` carries the whole control plane, including `CLUSTER SETSLOT
///   <slot> STABLE`, the operator's escape hatch for cancelling a migration
///   that has gone wrong. A barrier that can swallow its own cancel command is
///   unrecoverable without a restart.
///
/// This is an exemption from *slot-scoped* pauses only. A node-global `CLIENT
/// PAUSE` still blocks both exactly as it does today: that is an operator
/// deliberately quiescing the node, and changing it would be a visible
/// deviation from Redis for no gain.
pub(crate) fn exempt_from_slot_pause(cmd_name: &str) -> bool {
    matches!(cmd_name, "CLUSTER" | "MIGRATE")
}

/// The hash slot a command is pinned to, for slot-scoped pause matching.
///
/// `Some(slot)` when every key the command names hashes to that one slot.
/// `None` means **the command cannot be pinned** — it names no keys at all
/// (`FLUSHALL`, a `numkeys 0` script), or it names keys in more than one slot —
/// and callers must treat that fail-closed, as "may touch the barriered slot".
/// A keyless write is not slot-free in effect: `FLUSHALL` erases the migrating
/// slot along with everything else.
///
/// Deliberately *not* the same question as `ClusterSlotValidation` asks. That
/// stage decides where a command should run and answers `-CROSSSLOT` for a
/// straddling key set; this one only asks whether the barrier can safely let the
/// command past, so a straddling key set collapses into the same `None` as a
/// keyless one instead of an error.
pub(crate) fn command_pause_slot(
    registry: &CommandRegistry,
    cmd_name: &str,
    args: &[Bytes],
) -> Option<u16> {
    let keys = registry.get_entry(cmd_name)?.keys(args);
    SlotValidator::same_slot(&keys).ok().flatten()
}

/// The hash slot a *queued transaction* is pinned to, for slot-scoped pause
/// matching at `EXEC`.
///
/// The batch's answer is the fold of its commands' answers: `Some(slot)` only
/// when every queued command pins to that same slot. One unpinnable command, or
/// two commands pinning to different slots, collapses the whole batch to `None`
/// — "may touch the barriered slot" — and callers park it on any armed
/// slot-scoped pause. An empty queue never reaches here (`EXEC` returns early),
/// but folds to `None` for the same fail-closed reason.
///
/// A batch is *not* the union of its commands' key sets for `-CROSSSLOT`
/// purposes — a transaction may legitimately straddle slots where a single
/// command may not. That difference is deliberate: this function is only asked
/// whether a barrier can safely let the batch past, and a straddling batch can
/// reach the barriered slot just as surely as a single straddling command can.
pub(crate) fn queue_pause_slot(
    registry: &CommandRegistry,
    queue: &[frogdb_protocol::ParsedCommand],
) -> Option<u16> {
    let mut pinned = None;
    for cmd in queue {
        let name = cmd.name_uppercase();
        let name = std::str::from_utf8(&name).ok()?;
        let slot = command_pause_slot(registry, name, &cmd.args)?;
        match pinned {
            None => pinned = Some(slot),
            Some(seen) if seen == slot => {}
            Some(_) => return None,
        }
    }
    pinned
}

#[cfg(all(test, not(feature = "turmoil")))]
mod tests {
    use super::*;
    use frogdb_core::slot_for_key;

    fn registry() -> CommandRegistry {
        let mut registry = CommandRegistry::new();
        crate::register_commands(&mut registry);
        registry
    }

    fn arg(s: &str) -> Bytes {
        Bytes::copy_from_slice(s.as_bytes())
    }

    // FM-CLUSTER-080 FM-CLUSTER-081
    #[test]
    fn only_migrate_and_cluster_are_slot_pause_exempt() {
        assert!(exempt_from_slot_pause("MIGRATE"));
        assert!(exempt_from_slot_pause("CLUSTER"));
        assert!(!exempt_from_slot_pause("SET"));
        assert!(!exempt_from_slot_pause("EVAL"));
        assert!(!exempt_from_slot_pause("FLUSHALL"));
    }

    // FM-CLUSTER-079
    #[test]
    fn single_key_command_pins_to_its_key_slot() {
        let registry = registry();
        assert_eq!(
            command_pause_slot(&registry, "SET", &[arg("foo"), arg("v")]),
            Some(slot_for_key(b"foo"))
        );
        assert_eq!(
            command_pause_slot(&registry, "GET", &[arg("foo")]),
            Some(slot_for_key(b"foo"))
        );
    }

    // FM-CLUSTER-079
    #[test]
    fn hash_tagged_keys_in_one_slot_pin_together() {
        let registry = registry();
        assert_eq!(
            command_pause_slot(
                &registry,
                "MSET",
                &[arg("{t}a"), arg("1"), arg("{t}b"), arg("2")]
            ),
            Some(slot_for_key(b"{t}a"))
        );
    }

    // FM-CLUSTER-079
    #[test]
    fn unpinnable_commands_answer_none() {
        let registry = registry();
        // Cross-slot key set: not an error here, just "cannot be pinned".
        assert_eq!(
            command_pause_slot(&registry, "MSET", &[arg("a"), arg("1"), arg("b"), arg("2")]),
            None
        );
        // Keyless write — still able to erase the barriered slot.
        assert_eq!(command_pause_slot(&registry, "FLUSHALL", &[]), None);
        // Unknown command: nothing to resolve.
        assert_eq!(command_pause_slot(&registry, "NOSUCHCOMMAND", &[]), None);
    }

    fn queued(name: &str, args: &[&str]) -> frogdb_protocol::ParsedCommand {
        frogdb_protocol::ParsedCommand {
            name: Bytes::copy_from_slice(name.as_bytes()),
            args: args.iter().map(|a| arg(a)).collect(),
        }
    }

    // FM-CLUSTER-096
    #[test]
    fn a_batch_whose_commands_share_a_slot_pins_to_it() {
        let registry = registry();
        let queue = vec![
            queued("SET", &["{t}a", "1"]),
            queued("INCR", &["{t}b"]),
            queued("GET", &["{t}c"]),
        ];
        assert_eq!(
            queue_pause_slot(&registry, &queue),
            Some(slot_for_key(b"{t}a"))
        );
    }

    // FM-CLUSTER-096
    #[test]
    fn a_batch_straddling_slots_cannot_be_pinned() {
        let registry = registry();
        let queue = vec![
            queued("SET", &["alpha", "1"]),
            queued("SET", &["beta", "2"]),
        ];
        assert_ne!(slot_for_key(b"alpha"), slot_for_key(b"beta"));
        assert_eq!(queue_pause_slot(&registry, &queue), None);
    }

    // FM-CLUSTER-096
    #[test]
    fn one_unpinnable_command_unpins_the_whole_batch() {
        let registry = registry();
        // FLUSHALL names no key but erases the barriered slot along with the
        // rest, so the batch must park on any barrier.
        let queue = vec![queued("SET", &["{t}a", "1"]), queued("FLUSHALL", &[])];
        assert_eq!(queue_pause_slot(&registry, &queue), None);
    }

    // FM-CLUSTER-096
    #[test]
    fn an_empty_batch_is_unpinnable() {
        let registry = registry();
        assert_eq!(queue_pause_slot(&registry, &[]), None);
    }

    // FM-CLUSTER-096
    #[test]
    fn queued_command_names_are_matched_case_insensitively() {
        let registry = registry();
        let queue = vec![queued("set", &["foo", "1"])];
        assert_eq!(
            queue_pause_slot(&registry, &queue),
            Some(slot_for_key(b"foo")),
            "the queue holds the client's own casing; pinning must not depend on it"
        );
    }
}
