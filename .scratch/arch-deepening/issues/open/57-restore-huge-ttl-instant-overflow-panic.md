# 57 — `RESTORE` with a huge TTL panics the shard worker (unchecked `Instant + Duration`)

Status: needs-triage

**PARKED per user (security)** — do not implement without user ruling.

## What to build

`RestoreCommand::execute`
(`frogdb-server/crates/server/src/commands/persistence.rs:90`) parses the TTL argument with
`parse_i64(&args[1])` at `:92` and applies it with **no upper bound**:

```
:134    clock::now() + Duration::from_millis(ttl_ms as u64)
```

`clock::now()` returns a real `std::time::Instant`
(`frogdb-server/crates/types/src/clock.rs:31`), and `impl Add<Duration> for Instant` panics —
`"overflow when adding duration to instant"` — when the sum does not fit the platform
timespec. `ttl_ms` can be up to `i64::MAX` (≈ 9.2e18 ms ≈ 2.9e11 years), so
`RESTORE k 9223372036854775807 <payload>` reaches the add and aborts the thread. The same
unchecked pattern appears twice more on the `ABSTTL` branch, in `unix_ms_to_instant`:
`UNIX_EPOCH + Duration::from_millis(unix_ms as u64)` at `:162` (`SystemTime` `Add` panics
identically) and `now_instant + duration` at `:168`.

**Reachability is real, not theoretical.** The TTL is client-supplied and the only gate ahead
of the add is `deserialize(serialized_value)` at `:121`, which any client satisfies by first
running `DUMP` on a key it owns and replaying the bytes. No admin privilege, no cluster role,
no special config — an ordinary authenticated client with write access to one key can panic a
shard worker. There is no `catch_unwind` anywhere on the command dispatch path (the crate's
only occurrence is inside a test, `connection/transaction_conn_command.rs:621`), so the panic
takes the worker down rather than being converted into an error reply. That makes this a
denial-of-service reachable from untrusted input, hence the security parking above.

Redis does not have this failure mode: `restoreCommand` validates the TTL up front
(`if (ttl < 0) → "Invalid TTL value, must be >= 0"`) and stores the expiry as a plain
`long long` mstime, so an absurd TTL yields an absurd-but-harmless expire time, never a crash.
The fix direction is to bound and saturate rather than to panic: reject out-of-range TTLs with
a `CommandError` (matching Redis's error surface for the negative case) and use
`Instant::checked_add` / `SystemTime::checked_add` with a saturating "effectively never
expires" fallback for the in-range-but-enormous case. All three sites (`:134`, `:162`, `:168`)
must be converted together — fixing only the relative branch leaves `ABSTTL` panicking.

**Adjacent divergence found while verifying, worth folding in or splitting out:** the
`else` branch at `:141-144` treats a *negative* TTL as "remove the expiry" and returns `OK`.
Redis rejects negative TTLs with an error. Whatever ruling the panic gets, this silent
acceptance is a separate compatibility bug in the same twenty lines.

## Acceptance criteria

- [ ] `RESTORE k <i64::MAX> <valid payload>` returns an error reply (or a saturated
      never-expiring key, per ruling) — never panics; same for the `ABSTTL` form
- [ ] All three unchecked time-arithmetic sites converted to checked/saturating forms:
      `persistence.rs:134`, `:162`, `:168`
- [ ] Negative-TTL behavior ruled and pinned: either Redis-compatible error, or the current
      remove-expiry semantics documented as a deliberate deviation
- [ ] Regression test `restore_rejects_out_of_range_ttl_without_panicking` in
      `frogdb-server`: drives `DUMP` then `RESTORE` at `i64::MAX`, `i64::MIN`, `u32::MAX`-ish
      and `0`, over both the relative and `ABSTTL` forms, asserting replies and asserting the
      server still answers `PING` afterwards (proving the worker survived)
- [ ] The `RESTORE` payload/TTL pair is added to the fuzz corpus seeds so the class cannot
      regress silently
- [ ] `just test frogdb-server restore` green

## Blocked by

None - can start immediately. Security-parked: needs a user ruling before implementation.

## Source

Round 38-99 adversarial review, follow-up suspect list. Filed after direct verification of the
code (the suspect was stated as unconfirmed; reading
`frogdb-server/crates/server/src/commands/persistence.rs:90-170` and
`frogdb-server/crates/types/src/clock.rs:31` confirms it holds, and turned up two further
unchecked sites on the `ABSTTL` path plus the negative-TTL divergence).

## Comments
