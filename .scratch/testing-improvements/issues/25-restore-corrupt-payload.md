# RESTORE corrupt-payload contract untested for payloads that still parse

Status: done
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

- [x] Per-type (string, list, hash, set, zset, stream, at minimum) test: DUMP, corrupt via truncation, RESTORE, assert clean error and no key materialized.
- [x] Per-type test: DUMP, flip the type-marker byte, RESTORE, assert and document actual behavior (error, or — if it still parses — explicitly flag as known residue with a comment/follow-up reference; do not let it pass silently uncommented).
- [x] Per-type test: corrupted length field and garbage payload, RESTORE, assert clean error and no key materialized.
- [x] New `restore_payload` fuzz target exists and drives the RESTORE command path end-to-end (not just the low-level `deserialize` function).
- [x] Task description/PR references that the fail-closed parse-failure path (`persistence.rs:122-124`) already existed and was verified correct — new tests must not re-claim this as newly fixed.

## Blocked by

None - can start immediately.

## References

- `frogdb-server/crates/persistence/src/serialization/mod.rs:84-110` (`build_frame`, no CRC64/version footer)
- `frogdb-server/crates/persistence/src/serialization/mod.rs:113-160` (`deserialize`, header/length bounds only)
- `frogdb-server/crates/server/src/commands/persistence.rs:122-124` (fail-closed mapping of `deserialize` error to `CommandError::InvalidArgument`, confirmed present, before `store.set`)
- `frogdb-server/crates/server/tests/integration_dump_restore.rs:57-384` (existing happy-path-only coverage)
- Existing `deserialize.rs` fuzz target (memory safety only, not command-path contract)

## Resolution

Resolved 2026-07-23. Added per-type corrupt-payload contract tests plus a
`restore_payload` fuzz target. No panic or partial-state bug was found; the
pre-existing fail-closed path holds. One expected residue was confirmed and is
documented (not a regression).

### Fail-closed path re-confirmed (not newly fixed)

`commands/persistence.rs:122-124` already maps a `deserialize` error to a clean
`CommandError::InvalidArgument` ("DUMP payload version or checksum are wrong: ...")
*before* `store.set`. The new tests are a regression guard on this existing behavior;
they do not claim to introduce it.

### Tests added — `frogdb-server/crates/server/tests/integration_dump_restore.rs`

Corruption matrix over 9 value kinds (string, integer-string, list, hash, set, zset,
stream, HLL, JSON):

- `test_restore_truncated_payload_fails_closed_per_type` — (a) truncation. Two
  variants: *consistent-length* (body halved, length field rewritten to match → the
  per-type deserializer must reject the short body) and *dangling-length* (buffer
  truncated, length field left pointing past the end → frame-level bounds check
  rejects). Structured types + the fixed-width integer string fail closed; the raw
  string legitimately parses a shorter body (safety invariant asserted instead).
- `test_restore_type_flip_invariant_per_type` — (b) type-marker flip to a neighbouring
  valid marker, plus an out-of-range (unknown) marker. Asserts no panic / no partial
  state either way, and records which kinds silently materialize a wrong-type key.
  Unknown marker is always rejected via `TypeMarker::from_byte`.
- `test_restore_corrupt_length_field_fails_closed_per_type` — (c) oversized length
  field (`u64::MAX`) → frame-level bounds rejection for every type.
- `test_restore_garbage_body_no_panic_per_type` — (d) garbage body. Structured types
  reject cleanly; the raw string (any bytes) and the integer string (any 8-byte body
  is a valid `i64`) legitimately parse — safety invariant asserted for those.
- `test_restore_fully_random_bytes_fails_closed` — wholly random buffers: sub-header
  lengths are guaranteed-rejected; >= header length asserts the safety invariant.

Every rejection case asserts: clean protocol error, `EXISTS` == 0 (no partial state),
and the server still answers `PING` (no panic / wedged connection).

### Documented residue (acceptance criterion b)

With no CRC64/version footer, a flipped type-marker byte whose body happens to parse
under the neighbouring type's deserializer materializes a **wrong-type** key rather
than erroring. Empirically (testbox run, `--no-capture`) this silently succeeds for:

    ["list", "hash", "stream"]

i.e. flipping SortedSet(2)->Hash(3) with a zset body, Hash(3)->List(4) with a hash
body, and Stream(6)->Bloom(7) with a stream body each parse and store a wrong-type
value. This is inherent to a checksum-free frame format and is **not** a regression;
it is the residue this issue flagged. The invariant that still holds (and is tested):
the key is either fully absent (clean error) or fully materialized — never partial
state, never a panic. The `test_restore_type_flip_invariant_per_type` test logs the
residue set to stderr each run so a change in which flips succeed is visible in CI.

Closing this gap for real would require adding a checksum/version footer to the DUMP
frame (Redis parity) — a format change, out of scope for a testing-gap task. Left as
a potential follow-up.

### Fuzz target added — `testing/fuzz/fuzz_targets/restore_payload.rs`

Drives the RESTORE command path end-to-end rather than just `deserialize`: it seeds a
genuine value with real write commands, DUMPs it via `serialize`, bit-mutates the
frame, then mirrors RESTORE's handler body (`deserialize` -> on `Ok`, `store.set`) and
exercises the materialized (possibly wrong-type/garbage) key through the real command
registry (TYPE/GET/LLEN/SCARD/ZCARD/HLEN/... via `register_all`), plus a re-`serialize`
round-trip. RESTORE's handler lives in the server crate (not `frogdb-commands`, which
the fuzz workspace depends on), so its body is mirrored; a comment documents this.
Registered as `[[bin]] restore_payload` in `testing/fuzz/Cargo.toml`. Built with
`cargo +nightly fuzz` and smoke-run 10s with no crashes.

### Verification

- `just test frogdb-server test_restore_` (testbox): 5 tests run, 5 passed, 1852
  skipped.
- `just fuzz restore_payload 10`: builds and smoke-runs clean (no crash/leak).
