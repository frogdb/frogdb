# 06 — Frozen wire-frame fixtures for `ReplicationFrame` and `FullSyncMetadata`

Status: done

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

- [x] Golden byte fixtures checked in for `ReplicationFrame` and `FullSyncMetadata`, covering
      every variant and field the wire carries
- [x] Both directions asserted against the goldens (decode → expected value, encode → exact bytes)
- [x] Failure message states the rule explicitly: within a major, a layout change is a wire break
      — bump the version, do not regenerate the fixture
- [x] Fixtures and tests live in `frogdb-replication`; `just mutants-diff frogdb-replication`
      triaged before push

## Blocked by

None — can start immediately.

## Resolution (2026-08-10)

Added `frogdb-server/crates/replication/src/wire_golden.rs` (`#[cfg(test)] mod wire_golden;` in
`lib.rs`), modeled on `frogdb-cluster`'s `encoding_golden.rs` but using hex-text fixture files
(`testdata/wire/*.hex`) rather than JSON, since the pinned formats are binary/plain-text wire
grammars, not self-describing structures.

Fixture inventory:
- 11 `ReplicationFrame` fixtures: one per flag bit (`NONE`, `COMPRESSED`, `END_OF_BATCH`,
  `REQUIRE_ACK`), all three flags combined, `CONTROL_SHARD` vs. a tagged shard, `sequence` at
  `u64::MAX`, empty payload, a payload with embedded control/binary bytes, and a "kitchen sink"
  combining every field at distinct, asymmetric byte values (to catch a field swap, not just a
  single-field regression).
- 4 `FullSyncMetadata` fixtures: a typical value, all-zero, all-`u64::MAX`/`0xFF`, and a
  realistic-looking replication id — covering the colon-delimited format's field and hex-checksum
  boundaries.

Both directions are asserted for every fixture: encoding the fixture's Rust value must reproduce
the checked-in hex byte-for-byte, and decoding the checked-in hex must reproduce every field of
the fixture value exactly (manual field comparison — neither `ReplicationFrame` nor
`FullSyncMetadata` gained a `PartialEq` derive, to keep this change additive-tests-only in a
locked crate).

A mismatch panics via `wire_break_message`, which is itself pinned by a dedicated unit test
(`wire_break_message_tells_the_developer_to_bump_the_version`) rather than left to eyeballing: it
names `version_compat.rs` as the gate that does *not* cover this layout, states the "same major"
rule, and tells the developer to bump `FRAME_VERSION` (or the handshake major for
`FullSyncMetadata`, which carries no version field of its own) — explicitly forbidding a silent
`UPDATE_GOLDEN=1` regeneration as the fix for an unintentional diff. Manually verified end-to-end
by corrupting a checked-in fixture and confirming the panic fires with this message, then
restoring it.

No production code in `frogdb-replication` changed — additive tests and checked-in fixtures only.

Verification:
- `just check frogdb-replication` — clean.
- `just test frogdb-replication wire_golden` — 5/5 new tests pass against the checked-in goldens
  (fixture-name-uniqueness x2, frame round-trip, metadata round-trip, failure-message content).
- `just test frogdb-replication` (full crate suite) — 414/414 pass, nothing else affected.
- `just lint-failure-modes` — green (`OK: 279 failure modes ... 1401 test references, 1401 tags`);
  the new tests were deliberately left untagged (no `FM-REPLICATION-NNN` comment lines) so the
  locked failure-modes spec did not need editing.
- `just mutants-diff frogdb-replication` — `INFO No mutants to filter` (the diff is test-only;
  trivially green as expected).
