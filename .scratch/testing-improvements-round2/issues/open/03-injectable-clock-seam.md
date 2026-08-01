# Decide the scope of an injectable clock seam — 313 raw `now()` sites, no abstraction exists

Status: needs-triage
Type: decision
Origin: round-2 testing audit 2026-07-28 — 15 parallel area audits, `.scratch/testing-improvements-round2/`
Source: INFRASTRUCTURE.md I3
LOE: 3–5 days scoped to expiry / multi-week full (measured)
Tier: C
Area: workspace-wide / time abstraction (expiry, TTL, rate limit, election timeouts)
Asked by: 13 (F16), 14 (item 3), 15 (F7)

## Context

Three areas independently need to control time: persistence wants deterministic TTL/expiry
behaviour across restart, replication wants election and timeout behaviour without wall-clock
sleeps, and ACL rate limiting cannot be tested at all without it. There is nothing to adopt —
no trait, no helper — so the first builder defines the pattern for the whole workspace. That
makes the scope question load-bearing rather than incidental.

## Evidence

- **313** raw `SystemTime::now()` / `Instant::now()` call sites, and **no existing
  abstraction whatsoever** — no `trait Clock`, no `now_ms()`, nothing to adopt.

  | crate | sites | | crate | sites |
  |---|---:|---|---|---:|
  | core | 121 | | types | 18 |
  | server | 51 | | replication | 11 |
  | persistence | 44 | | vll | 4 |
  | commands | 31 | | acl, scripting | 3 each |

  cluster, search and protocol have zero.
- **Smallest useful slice**: 15/F7 needs only `acl/src/ratelimit.rs:23 now_us` — a single
  production-code seam, and `types` already has shuttle plumbing via `types/src/sync.rs`.

## Options

> **Decision needed**: full seam, or scoped to the expiry path (~30–40 sites, covers theme T4
> and the TTL findings) leaving replication and election timeouts wall-clock?
>
> **Rule**: whoever builds one first owns it; nobody adds a second (13's explicit request).

## What to build

1. Take the scope decision above and record it on this issue.
2. Build the seam at the chosen scope, in one place, with the "one seam only" rule written
   into its module docs so a second abstraction cannot be added later by accident.
3. Convert the smallest useful slice first — `acl/src/ratelimit.rs:23 now_us` — as the proof
   the seam is usable, reusing the existing `types/src/sync.rs` shuttle plumbing.

## Acceptance criteria

- [ ] A scope decision (full vs expiry-only) is recorded in a `## Resolution` section.
- [ ] Exactly one time abstraction exists in the workspace; its module docs state the
      "nobody adds a second" rule and name the owner.
- [ ] `acl/src/ratelimit.rs` reads time through the seam, and a test drives its refill
      behaviour with no wall-clock sleep.
- [ ] Every call site inside the chosen scope reads through the seam; sites outside the scope
      are listed explicitly in the docs as deliberately wall-clock.

## Test boundary

Level 1–2 — the point of the seam is to pull time-dependent behaviour *down* from
sleep-based level-4 tests to deterministic unit and crate-API tests. The rate-limit slice is
level 1; the expiry slice lands at level 2–3 via `shard_driver`.

## Depends on

Nothing.
