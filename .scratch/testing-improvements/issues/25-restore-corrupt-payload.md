# RESTORE corrupt-payload contract untested for payloads that still parse

Status: needs-triage
Type: AFK
Origin: testing-gap audit 2026-07-22 (multi-agent static review + adversarial verification; coverage run on testbox)
Severity: likelihood 2/3, consequence 2/3 (score 4)
Area: basic-commands

## Context

The DUMP/RESTORE frame format (`frogdb-server/crates/persistence/src/serialization/mod.rs:84-110`, `build_frame`) has no CRC64 and no version footer — unlike Redis, which validates and rejects on checksum/version mismatch. `deserialize` (same file, :113-160) validates only header size and length bounds. All existing RESTORE tests are happy-path round-trips (`frogdb-server/crates/server/tests/integration_dump_restore.rs:57-384`: REPLACE, BUSYKEY, TTL; `dump_tcl.rs`: existing-key/syntax/TTL cases) — none feed corrupted, truncated, bit-flipped, or type-mismatched payloads. The `deserialize.rs` fuzz target exercises memory safety of the low-level parsing function, not the RESTORE command's error contract end-to-end. Upstream Redis's `corrupt-dump-fuzzer.tcl` was correctly excluded as RDB-format-specific, but that exclusion also dropped FrogDB's own robustness coverage for its own frame format — nothing replaced it.

**Verdict adjustment (must be reflected in scope):** RESTORE already fails closed on payloads that fail to parse — `deserialize`'s error is mapped to a clean `CommandError::InvalidArgument` ("DUMP payload version or checksum are wrong: ...") in `frogdb-server/crates/server/src/commands/persistence.rs:122-124`, *before* the value ever reaches `store.set`. So the "crashes or materializes garbage on totally malformed input" framing is not the live risk. The real untested residue is narrower: a payload that is corrupted (bit-flipped type byte, truncated mid-payload, garbage bytes) but still *parses successfully* under the per-type deserializer — that case is not rejected by the fail-closed path, because parsing didn't fail, and would silently materialize a wrong-type or wrong-content key. No test exercises this residue, and a regression in a per-type validator that made it accept more garbage than it should would go undetected.

## What to build

- For each supported type, DUMP a valid value, then construct corrupted variants and RESTORE them, asserting behavior in each case:
  - (a) truncated payload (cut mid-way through the type-specific body) — expect it to fail parsing and hit the existing fail-closed path (regression guard on current behavior).
  - (b) flipped type-marker byte (payload body left as-is, mismatched against a different type's expected layout) — the interesting case: does it still parse (silently materializing a wrong-type/garbage key) or fail? Assert whichever is true today, and flag it as a residue for follow-up if it silently succeeds.
  - (c) corrupted length field within the frame — expect fail-closed.
  - (d) fully garbage payload bytes — expect fail-closed.
- Add a `restore_payload` fuzz target that drives the actual RESTORE command path (not just `deserialize`) so mutations that "still parse" but produce wrong data are caught by fuzzing over time, not just the fixed test cases above.

## Acceptance criteria

- [ ] Per-type (string, list, hash, set, zset, stream, at minimum) test: DUMP, corrupt via truncation, RESTORE, assert clean error and no key materialized.
- [ ] Per-type test: DUMP, flip the type-marker byte, RESTORE, assert and document actual behavior (error, or — if it still parses — explicitly flag as known residue with a comment/follow-up reference; do not let it pass silently uncommented).
- [ ] Per-type test: corrupted length field and garbage payload, RESTORE, assert clean error and no key materialized.
- [ ] New `restore_payload` fuzz target exists and drives the RESTORE command path end-to-end (not just the low-level `deserialize` function).
- [ ] Task description/PR references that the fail-closed parse-failure path (`persistence.rs:122-124`) already existed and was verified correct — new tests must not re-claim this as newly fixed.

## Blocked by

None - can start immediately.

## References

- `frogdb-server/crates/persistence/src/serialization/mod.rs:84-110` (`build_frame`, no CRC64/version footer)
- `frogdb-server/crates/persistence/src/serialization/mod.rs:113-160` (`deserialize`, header/length bounds only)
- `frogdb-server/crates/server/src/commands/persistence.rs:122-124` (fail-closed mapping of `deserialize` error to `CommandError::InvalidArgument`, confirmed present, before `store.set`)
- `frogdb-server/crates/server/tests/integration_dump_restore.rs:57-384` (existing happy-path-only coverage)
- Existing `deserialize.rs` fuzz target (memory safety only, not command-path contract)
