# `REPLCONF frogdb-version` is parsed, answered `+OK`, and discarded

Status: done
Type: bug (dead field / lying comment)
Severity: likelihood 3/3 (every replica sends it on every handshake), consequence 2/3 (the
rolling-upgrade version check it exists to feed has no data; the field reads `None` for every
replica) — score 6
Area: replication / handshake

## Problem

This is issue 16 (`listening-port` and `capa` parsed then discarded) a third time, on the third
`REPLCONF` option — found only after 16 was closed, because 16's fix covered the two options it
named and not this one.

The replica sends it. `frogdb-server/crates/replication/src/replica/connection.rs:200` puts
`frogdb-version` on the wire during the handshake.

The primary parses it, logs it, and drops it.
`frogdb-server/crates/server/src/commands/replication.rs:345-362`:

```rust
tracing::debug!(version = %version, "REPLCONF frogdb-version");
// Version will be stored in replica connection state by the
// connection handler (similar to listening-port and capa).
Ok(Response::ok())
```

The comment names a writer that does not exist. `SessionInner.replica_version`
(`replica_session.rs:278`) is initialised to `None` at `:339`, is copied into `ReplicaInfo` at
`:406`, and is read back by `replica_version()` at `:374` — and **nothing anywhere in the workspace
assigns it**. `rg replica_version` returns six hits, all of them the declaration, the initialiser,
the copy and the getter.

So `ReplicaInfo::replica_version` is `None` for every replica that has ever connected, and the
stated purpose — "so the primary can check all replica versions during finalization" — has no
input. A rolling upgrade that is supposed to refuse to finalise while an old replica is still
attached cannot see the old replica.

## Why it matters more than a dead field

`listening-port` (issue 16) was a *wrong* value rendered with confidence. This one is `None`, which
is honest as far as it goes — the danger is the consumer side. Any check written as
"if every known replica version is >= X, finalise" is vacuously true over an empty set, so the gate
opens rather than closes. A version gate that fails open during a rolling upgrade is worse than no
gate, because the operator believes one is there. `frogdb_version_gate_active`
(`types/src/metrics/definitions.rs:501`) exists as a gauge, which suggests something already
believes it is being enforced.

## Suggested remedy

1. Store it exactly where issue 16 stored the port: a per-connection announcement carried into the
   session at `PSYNC`, never a global side channel. `ReplicaAnnouncement` already exists and
   already threads the port and the capabilities through `register_announced_replica`; add the
   version to it rather than inventing a second path.
2. Delete the comment that claims someone else does this, whichever way the fix goes.
3. Decide and write down what an *absent* version means (a replica old enough not to send the
   option at all). `None` must be distinguishable from "not yet parsed" at the point the gate reads
   it, or the gate fails open in a second way.
4. Then find the finalization check the comment refers to and confirm it actually reads the field —
   if no such check exists, that is a third issue, not a closed one.

## Tests that should exist

- `a_replica_that_announced_its_version_is_recorded_with_it` — the twin of
  `a_psync_carries_the_announcement_into_the_registry`.
- `a_replica_that_announced_no_version_is_recorded_as_unknown` — and the gate treats unknown as
  blocking, not as satisfied.
- `re_announcing_the_version_does_not_clear_the_port_or_capabilities` — the overwrite-only-its-own-
  kind rule FM-REPLICATION-049 already specifies for the other two options.

## Spec impact

FM-REPLICATION-049 covers the announcement recording for `listening-port` and `capa` and its
NOT-observable half is written in terms of those two. Closing this issue extends that row to the
third option (or adds a row for the version gate itself, if the gate turns out to be real).

## Resolution

Fixed as far as this issue reaches: the **writer** now exists. FM-REPLICATION-049 extended to the
third option (Trigger, Observable, NOT-observable, Invariant, Outcome variant, Forced by, Bug refs)
— no new row, because it is the same failure mode on the same mechanism.

- `frogdb-server/crates/replication/src/replica_session.rs` — `ReplicaAnnouncement.version:
  Option<String>`, `AnnouncedOption::Version(String)` with a `frogdb-version` parse arm,
  `AnnouncementError::{MissingVersion, InvalidVersionEncoding}`, and `ReplicaSession::announced`
  seeding `replica_version` from the announcement instead of hardcoding `None`.
- `frogdb-server/crates/server/src/commands/replication.rs` — the hand-rolled `"frogdb-version"`
  arm and its lying comment are **deleted**; the subcommand joins `"listening-port" | "capa"` on
  the shared parser, and `announcement_error` maps the two new variants to the exact wire text the
  deleted arm produced.
- No change was needed in `connection/dispatch.rs`: the handshake stage already absorbs whatever
  `AnnouncedOption::parse` returns, so the option started being recorded the moment the parser knew
  it. Wire behaviour is unchanged (`+OK`; same two errors) — what changed is that the value survives.

**Remedy step 1** taken as written (the existing `ReplicaAnnouncement`, no second path).
**Step 2** taken (comment deleted).
**Step 3**: absent version is `None` = *unknown*, documented on `ReplicaAnnouncement::version`,
`ReplicaInfo::replica_version` and `ReplicaSession::replica_version()`, and pinned by
`a_replica_that_announced_no_version_is_recorded_as_unknown`. The spec states that a consumer must
treat unknown as blocking and that defaulting it (to `"0.0.0"`, `""`, or the primary's own version)
is a second way to fail open.

**Step 4 — the gate does not exist, so this is issue 27.** `ClusterCommand::FinalizeUpgrade`
validates the **raft topology's** node versions, not the replicas attached over `PSYNC`, and
`version_gate::is_gate_active` keys on the cluster's finalized `active_version`. Neither reads a
replica announcement, and a non-cluster primary-replica pair is covered by no gate at all. Filed as
`.scratch/hardening/issues/open/27-no-gate-reads-the-announced-replica-version.md`, with the design
choice written up under an Open question heading (extend `FinalizeUpgrade` vs. a replication-level
readiness surface, plus whether unknown blocks with no override).

### Divergences worth naming

- `ReplicaAnnouncement` and `AnnouncedOption` lost their `Copy` derive (they carry a `String` now)
  and are `Clone` instead. The one by-value handoff site (`connection.rs:833`, which consumes
  `self`) was unaffected.
- The version is stored **verbatim and unparsed** — deliberately not a `semver::Version`. This
  records what the peer said, and a peer can say anything; validating at the handshake would make a
  malformed version an announcement error rather than a fact the gate gets to refuse.

### Tests

Red-first (the API did not exist, so these were compile-red; the end-to-end assertion below was a
genuine assertion-red once the type existed).

- `a_replica_that_announced_its_version_is_recorded_with_it`
- `a_replica_that_announced_no_version_is_recorded_as_unknown`
- `re_announcing_the_version_does_not_clear_the_port_or_capabilities`
- `an_unreadable_frogdb_version_is_refused` (missing value, non-UTF-8, case-insensitive subcommand,
  and a non-semver value recorded verbatim)
- `a_psync_carries_the_announcement_into_the_registry` extended: the announcement now carries a
  version and the registered `ReplicaInfo` is asserted to hold it (this is the assertion that was
  red against the old `replica_version: None` hardcode)
- `announcement_errors_keep_their_wire_shape` extended with the two new error mappings
