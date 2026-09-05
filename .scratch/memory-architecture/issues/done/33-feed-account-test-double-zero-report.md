# 33: the replica-feed test double keeps a zero report, like the real account

Status: done
Type: AFK
Origin: whole-branch re-review nit, 2026-09-05 (D7)
Area: frogdb-replication (test support)
Phase: 6 — polish

## Parent

[PRD.md](../../PRD.md), decision D7.

## Why

`replica_session.rs` reports zero buffered bytes after the written live dataset is released
and asserts the verdict is `Keep`:

```rust
let verdict = report_feed(&self.feed, 0, "the written live dataset");
debug_assert!(verdict.is_ok(), "reporting zero buffered bytes must be FeedVerdict::Keep, got {verdict:?}");
```

The comment above it is right about the production account: `FeedOutputAccount` in
`frogdb-server/crates/server/src/connection/output_buffer.rs:404` shrinks the charge on a
smaller report, so `set_buffered(0)` cannot breach a hard, soft or budget bound. The test
double `RecordingFeedAccount` in `frogdb-server/crates/replication/src/feed_account.rs:324`
does not share that property:

```rust
if total_bytes < self.shed_at { return FeedVerdict::Keep; }
state.over_limit_reports += 1;
```

With `shed_at == 0` a zero report counts as over the limit and, once `over_limit_reports`
passes `shed_after`, the double returns `Shed` — the assert would fire in a debug test
build. No current test constructs the double with `shed_at == 0`, so nothing fails today;
the first test that does will fail for the wrong reason. The double should model the real
account's invariant, not just its limit.

## What to build

1. In `RecordingFeedAccount::set_buffered`, a report of `0` returns `FeedVerdict::Keep`
   before the limit comparison, and does **not** increment `over_limit_reports`. It is still
   pushed onto `reports` (tests read the sequence of reports).
2. Document the invariant on the method in one sentence: a zero report is a release, never a
   shed, matching the production account.
3. Add a unit test beside the double: `RecordingFeedAccount::shedding_at(0, "…")` with
   `shed_after == 0`, report `0` → `Keep`; report `1` → `Shed`. The test names the assert in
   `replica_session.rs` it protects.

## Acceptance criteria

- [ ] `set_buffered(0)` on the double returns `Keep` for every `shed_at`, including `0`.
- [ ] `reports` still records the zero.
- [ ] New unit test RED before the change, GREEN after.
- [ ] `just test frogdb-replication` green (note: `quint` must be on PATH — run
      `export PATH="$(dirname "$(mise which quint)"):$PATH"` first if the 24
      `quint_conformance` tests fail with "`quint` is not on PATH").

## Files likely touched

- `frogdb-server/crates/replication/src/feed_account.rs`

## Out of scope

The production account; `replica_session.rs`; anything about the limits themselves.

## Blocked by

None.

## Decisions

D7

## Resolution

Landed 2026-09-05 on `mem-arch-integration` as `cde0fc33` (pick of `6eec7e6c`, one file).

`RecordingFeedAccount::set_buffered` returns `FeedVerdict::Keep` for a zero report before the
`shed_at` comparison, without touching `over_limit_reports`, and still records the zero in
`reports`. The invariant is stated at the check: a zero report is a release, never a shed,
matching the production `FeedOutputAccount`. New unit test
`a_zero_report_is_always_kept_even_at_a_zero_limit` builds the double with
`shedding_at(0, "hard_limit")` (so `shed_after == 0`), asserts report 0 → `Keep` and report 1
→ `Shed`, and names the `replica_session.rs` `debug_assert!` it protects. RED before, GREEN
after; `just test frogdb-replication` 636/636.

Review (sonnet, one round): Approved, no Critical/Important. One Minor left as-is: the
invariant sentence sits inline above the `if` rather than as a doc comment on the method.
