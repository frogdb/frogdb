# 06 — Frozen wire-frame fixtures for `ReplicationFrame` and `FullSyncMetadata`

Status: needs-triage

## Parent

[PRD](../../PRD.md) §3 W2 (the non-property deliverable).

## What to build

Golden byte fixtures, checked in, for the two encodings that cross the wire between a primary and
a replica: `ReplicationFrame` (`frogdb-server/crates/replication/src/frame.rs`) and
`FullSyncMetadata` (`fullsync.rs`). Round-trip is asserted in both directions — decoding the
golden bytes yields the expected value, and encoding that value reproduces the golden bytes
exactly.

The gap this closes is narrow and real: `version_compat.rs` gates *majors* at the handshake, and
nothing pins the byte layout *within* a major. A silent field reordering is therefore a
rolling-upgrade wire break that the current tests cannot see — every existing test encodes and
decodes with the same build.

Because of that, the failure message matters as much as the assertion: a layout change inside a
major is a wire break, so the fixture test must tell the reader the fix is a version bump, not a
regenerated fixture. Cover each variant and each field the wire actually carries, not one
happy-path frame.

Fixtures live in `frogdb-replication` so the mutation gate sees the encoders they pin. No
dependency on the view or the catalog — this can start immediately and land in parallel.

## Acceptance criteria

- [ ] Golden byte fixtures checked in for `ReplicationFrame` and `FullSyncMetadata`, covering
      every variant and field the wire carries
- [ ] Both directions asserted against the goldens (decode → expected value, encode → exact bytes)
- [ ] Failure message states the rule explicitly: within a major, a layout change is a wire break
      — bump the version, do not regenerate the fixture
- [ ] Fixtures and tests live in `frogdb-replication`; `just mutants-diff frogdb-replication`
      triaged before push

## Blocked by

None — can start immediately.
