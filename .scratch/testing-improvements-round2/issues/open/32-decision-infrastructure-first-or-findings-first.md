# Decision D4 — shared test infrastructure first, or findings first

Status: ready-for-human
Type: decision
Origin: round-2 testing audit 2026-07-28 — 15 parallel area audits, `.scratch/testing-improvements-round2/`
Source: MASTER.md §7 D4 · MASTER.md §6 · INFRASTRUCTURE.md
Area: crates/testing · frogdb-test-harness · `core/tests/shard_driver/`

## Context

Eighteen shared-infrastructure items were requested independently by multiple area audits
(`INFRASTRUCTURE.md`, filed as issues 01–18,
`.scratch/testing-improvements-round2/issues/`). They unblock and cheapen a large fraction of the
249 findings — a proposed test typically drops one or two effort levels once the item it needs
exists, and several findings are impossible without one. But they deliver **no coverage on their
own**.

The framing in MASTER.md §6 has one correction that matters to this decision: I1, I2 and I3 are
**not peers and should not be scheduled as a block**.

- **I1** (`shard_driver` harness extension) — **1–2 days, measured**. Every builder option it needs
  already exists on `ShardWorkerBuilder` and is simply not forwarded (`with_eviction:207`,
  `with_persistence:225`, `with_replication:201`, `with_wal_mode:219`, `with_fake_wal_failure:286`,
  `with_scripting:213`). Unblocks ~20 findings and pulls eviction, tracking, blocking and
  command-semantics tests from level 4 to level 3.
- **I2** (subprocess-SIGKILL crash primitive) — **1–2 weeks with CI-flake risk, measured**.
  `TestServer` is entirely in-process; there is zero `Command::new` in `test-harness/src`.
- **I3** (injectable clock seam) — **3–5 days scoped to expiry, multi-week full, measured**. 313
  raw `SystemTime::now()`/`Instant::now()` call sites and **no existing abstraction whatsoever** to
  adopt.
- **I7** (`ScatterHeavy` workload profile) — **~0.5 day, measured**; the checker, probe and runner
  all already exist and need no changes. Described by its author as "the single cheapest
  high-severity item in the audit".

`INFRASTRUCTURE.md` already tiers all eighteen: **A** = cheap, unblocks a lot, existing foothold
(I1, I7, I16, I17); **B** = real work, build when a scheduled finding needs it (I4, I5, I9, I11,
I12, I13, I14, I15, I18); **C** = expensive or viral, needs its own design decision first (I2, I3,
I6, I8, I10).

## Options

**(a) Build all the infrastructure first.** *Consequence:* every subsequent test is written at its
natural boundary and nothing gets written twice. But it front-loads a multi-week programme —
including I2 and I3, whose own scoping questions are unresolved — before a single defect closes,
and the audit's dominant result is ~40 suspected silent-data-loss defects.

**(b) Findings first, build infrastructure only when a scheduled finding is blocked.** *Consequence:*
the highest-severity defects close soonest and no speculative harness work is done. But several
tests get written at a higher boundary than they should be (slower, more brittle) and later have to
be moved down, and the audit warns that some findings simply *cannot be expressed* today — the
crash-consistency class in particular, because the suite cannot express a crash at an arbitrary byte
offset.

**(c) Tier A first, then findings, with tiers B and C pulled in on demand.** *Consequence:* matches
how `INFRASTRUCTURE.md` is already organised. Tier A is ~2.5–3.5 days total and unblocks the
largest single block of findings (I1 alone: ~20). Tiers B and C stay demand-driven, so I2's 1–2
weeks and I3's viral 313-call-site refactor are only paid for if a scheduled finding needs them.
Cost: a short delay before the first finding closes, and someone must own the tier-A items.

**(d) Split by owner — infrastructure and findings in parallel tracks.** *Consequence:* nothing
blocks, but the two tracks must coordinate on `crates/testing/` and `test-harness/`, which I4, I5,
I9 and I18 all touch, and I3 carries an explicit rule from its author: *whoever builds one first
owns it; nobody adds a second.*

## Two design questions inside tier C that this decision surfaces

- **I2**: is "process dies mid-fsync with OS buffers still in flight" — the only residue
  `CrashTestHarness`'s existing byte-level truncation does not cover — worth 1–2 weeks plus ongoing
  CI flake, or is truncation-level crash testing sufficient for production readiness?
- **I3**: full clock seam, or scoped to the expiry path (~30–40 of the 313 sites, covering theme T4
  and the TTL findings) leaving replication and election timeouts on the wall clock? The smallest
  useful slice is a single production-code seam: `acl/src/ratelimit.rs:23 now_us`.

## Recommendation

None. MASTER.md §7 records no recommendation for D4. What the audit does state, and any choice must
respect: I1, I2 and I3 are not peers and should not be scheduled as a block; and `INFRASTRUCTURE.md`
supersedes MASTER.md §6, which was a lossy first pass that collapsed ~17 items into 10 and dropped
I11 — described by its author as "the biggest ask" in that area — along with I12–I18.

## Depends on

Nothing blocks this decision. It sequences issues 01–18,
`.scratch/testing-improvements-round2/issues/`, against everything else in the round-2 backlog.
Take it together with issue 30, same directory (bugs-before-tests), which is the orthogonal
scheduling decision.
