# 23: account the replica feed under NetworkOutput (D4 second half)

Status: done
Type: AFK
Origin: issue 18's review, 2026-09-01 — [PRD.md](../../PRD.md) D4
Area: frogdb-replication / frogdb-replication-runtime (LOCKED) + frogdb-memory + server connection handoff
Phase: 5 — after issue 18 lands

## Why

D4 rules that replica feed buffers account under `Subsystem::NetworkOutput` with issue 18's
`replica` output-limit class. Issue 18 delivered the class and the seam, but the PSYNC
handoff (`connection.rs` `run()` → `PrimaryReplicationHandler`) takes
`self.framed.into_inner()`, drops the `ConnectionHandler` and with it the
`OutputBufferAccount`'s `Charge`. From that point the feed writes with its own buffers and
no `Subsystem::` charge exists anywhere in `crates/replication*`. Net effect today:

- The `replica` class in `client-output-buffer-limit` is live only in the REPLCONF→PSYNC
  window, where nothing large is ever buffered — the `replica 256mb 64mb 60` limit is
  decorative.
- Replica feed bytes are charged **nowhere** — a slow replica's buffered feed is invisible
  to the budget breakdown and to `CLIENT LIST` omem.

Issue 18's fix round documents this window at the spec row, config docs, and handoff code;
this issue closes the gap for real.

## What to build

Spec-first — replication is a LOCKED area (`specs/replication.md`), so the behavior change
starts as a failure-mode row (slow replica → feed buffer charged, visible, limited →
disconnect at the replica-class hard limit / soft window), then a failing test, then the
implementation:

1. Charge the primary→replica feed buffers to `Subsystem::NetworkOutput` at a single seam
   in the feed path (same absolute-figure `set_buffered` style issue 18 used — no matched
   +/- bookkeeping). Backlog stays `ReplicationBacklog`, WAL stays `WalChannel` (D4).
2. Enforce the `replica` class limits on the feed: hard → disconnect replica; soft +
   window → disconnect after window. Redis semantics (`client-output-buffer-limit slave`).
3. `CLIENT LIST`/`INFO` omem for a replica connection reflects the feed buffer.
4. Carry the charge across the PSYNC handoff instead of dropping it, or open a fresh one in
   the replication handler — either way no uncharged gap between handoff and first feed
   write.
5. Update the FM-MEMORY-001 row cell and config docs written by issue 18's fix round to
   drop the "pre-PSYNC window only" caveat.

## Acceptance criteria

- [ ] New FM row(s) in `specs/replication.md` (and/or `specs/memory.md` budget-seam side)
      with forcing tests in the owning crate; `just lint-spec` green.
- [ ] Slow-replica e2e test: feed backs up → omem nonzero → hard limit disconnects.
- [ ] Budget breakdown shows replica feed bytes under `NetworkOutput` under load.
- [ ] `just mutants-diff frogdb-replication` run before push (LOCKED-area discipline).
- [ ] Issue 18's window caveats removed.

## Out of scope

Changing backlog/WAL subsystem attribution (D4 rules them settled), feed flow control or
partial-sync semantics, new config knobs beyond the existing `replica` class line.

## Depends on

Issue 18 landed. [Issue 19](../) landed 2026-09-03 without touching feed buffer ownership
(triaged 2026-09-04: no coordination needed). [Issue 21](../) landed 2026-09-04 and added
the replica-side `TxnBuffering` charge (FM-REPLICATION-045) in `apply.rs` — a different
subsystem; this issue's `NetworkOutput` seam sits on the primary's feed path.

## Resolution

Landed 2026-09-05 on `mem-arch-integration` (picks `bc501fdc5`..`be50c836f`, 14 commits).

What shipped: a `FeedOutputAccount` seam in `frogdb-replication` (`feed_account.rs`) — the
crate names one absolute `u64` and gets back a `#[must_use] FeedVerdict {Keep, Shed}`; budget,
class, clock, `omem`, metric and log line all stay in `frogdb-server`. Four report points in
`replica_session.rs` cover every buffer the feed holds (staged live dataset before the store
call, released at `SendLiveDataset`; backlog handoff tail decremented per frame;
`FeedSequencer::buffered_bytes()` — an incrementally maintained `held_bytes` — after every
step), with a single release in `SessionDriver::exit` and `Charge`'s RAII drop covering the
`handle_psync` paths that return before a driver exists. The connection's
`OutputBufferAccount` is carried across the PSYNC handoff (class `Replica`) rather than
re-opened, so there is no uncharged gap; `ReplicaFeedAccount` wraps it, republishes `omem`
through `ClientRegistry::update_memory` only when the figure changes (`last_published`
elision), and counts sheds in
`frogdb_client_output_buffer_disconnects_total{class="replica",reason=...}`. The `replica`
soft window is **polled, not sampled**: a 500 ms ticker per feed (over a `Weak`, exits on
release) re-judges the last figure and delivers a shed out of band through a
`ShedGuardedStream` armed over the socket before the full sync — so a session parked inside
`write_all` for a dead replica is dropped when the window expires, as Redis does from
`serverCron`. Spec: FM-REPLICATION-069 (ten forcing tests, eight in the replication crate) +
a Redis-deviations row for the 500 ms granularity; FM-MEMORY-001/002 cells and issue 18's
"pre-PSYNC window only" caveats removed from `config/src/server.rs`, the generated
`config-reference.json`, and `website/.../architecture/connection.md`. No new config knobs;
backlog/WAL attribution untouched; `frogdb-memory` untouched.

Review: round 0 (3 Important, 6 Minor) — the soft limit was inert on a stalled feed (window
advanced only on the next report), the registry write lock was taken on every feed step, and
`buffered_bytes()` was an O(held) fold inside the barrier window. Fix round 1 (9 commits):
periodic re-judge + out-of-band shed (RED shown twice, ticker disabled then restored),
`last_published` elision, incremental `held_bytes` with a step-by-step equivalence test,
`released` short-circuit, omem test reads the replica's own `CLIENT LIST` row, spec cells
narrowed to what the tests force; two test defects the implementer found while forcing
(the soft-window e2e was draining the window it tested; the stalled-feed unit test waited
unbounded) fixed. Re-review r1: all findings addressed, no new Critical/Important (two new
Minors, carried below). Gates: full `frogdb-server` suite 2151/2151 on a quiet box (216 s),
`frogdb-replication` 635/635, workspace clippy `-D warnings`, lint-spec (319 rows / 1802
refs), lint-gates, spec-gen/docs-gen no drift, `mutants-diff frogdb-replication` 45: 20
caught / 0 missed / 25 unviable (all genuinely non-compiling — `Poll::new()`,
`Default::default()` for `io::Error`/`FeedAction`/...).

Deviations from the brief, for human sign-off:

- **`website/src/content/docs/architecture/connection.md` is outside the literal file
  boundary.** The brief allowed "the config/operations page carrying issue 18's caveat"; the
  caveat actually sat on the architecture → connection page. Prose only, corrected a
  now-false published claim.
- **The round-0 `mutants-diff` baseline timed out under load** (3+ agents compiling); re-run
  with nextest `slow-timeout.period=60s` / `terminate-after=4`, mutant set unchanged. Round 1
  ran with the plain recipe.
- **Soft window granularity is 500 ms**, not Redis's `serverCron` `hz`. One tokio task per
  connected replica waking twice a second; recorded in the Redis-deviations table so a change
  is a visible spec edit. `ShedGuardedStream` adds a `Box` indirection plus a one-shot poll per
  feed read/write; neither is benchmarked.

Known gaps carried (follow-up material): the ticker's `released` check is not atomic with
`re_judge`, so a tick landing in the sub-tick window before `release()` can emit one spurious
`soft_limit` warn + counter increment for a link already ending (metric-only); the
`last_published` elision is not ordered against a concurrent second reporter (only reachable
through the aborted-write-task race, self-heals on the next distinct figure, worst case a
stale `omem` on a connection about to be unregistered); the `flush`/`end` decrements of
`held_bytes` carry no mutant (cargo-mutants does not mutate assignments) and rest on the
unit test; the frame currently on the wire (`FeedAction::Send` payload and the tail's
`encoded` copy) is uncharged for the duration of its write, one frame in the conservative
direction.
