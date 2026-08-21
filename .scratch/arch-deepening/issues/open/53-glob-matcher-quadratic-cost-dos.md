# 53 — [SEC] Canonical `glob_match` has no work bound; three amplifiers make it a DoS, one now live on main

Status: needs-triage

**PARKED per user (security)** — do not implement without user ruling.

## What to build

`frogdb_types::glob_match` (`frogdb-server/crates/types/src/glob.rs:23`) is an iterative
two-pointer backtracking matcher whose cost is `O(n·m)` with **both** `n` (key/channel length)
and `m` (pattern length) attacker-chosen and neither capped. The guard that looks like a bound is
not one: `MAX_STAR_COUNT = 100` (`glob.rs:18`) is incremented only in the `b'*'` arm
(`glob.rs:63-67`), which then advances `pi` past the star run and records `star_pi = pi`
(`glob.rs:73`). Every backtrack restores `pi = star_pi` — i.e. to the position *after* the star —
so a star is counted **once, on first encounter, no matter how many times it is backtracked
through**. `MAX_STAR_COUNT` therefore bounds the number of `*` **characters** in a pattern, not
the number of backtracking steps: a **two-star** pattern can backtrack `O(n·m)` times and never
approach the cap. Measured: a single match at `n = 100k`, `k = 10k`, `star_count = 2` took
**752 ms** on one shard thread.

**Escalated by this review, because the amplifier is now LIVE on `origin/main`.** The finding was
originally written as conditional on the H/S/ZSCAN glob unification landing. It landed (HF-B, in
the `d48e1b44` batch): `hash_cursor_scan` now calls the canonical matcher **per item**
(`frogdb-server/crates/commands/src/utils.rs:116`), and — per the sibling cost finding — the item
loop is not bounded by `COUNT` for non-matching items (`utils.rs:110-119` applies `MATCH` *after*
the `emitted >= count` break). Per-call cost therefore multiplies by collection size, on
`origin/main`, reachable by any authenticated client issuing `HSCAN`/`SSCAN`/`ZSCAN`. The shard
worker is single-threaded, so this is shard occupancy, not one slow client.

The other two amplifiers:

- **PSUBSCRIBE / PUBLISH.** The hot loop is `frogdb-server/crates/core/src/pubsub.rs:741-742`:
  `for (pattern, compiled, _, sender) in &self.pattern_subs { if compiled.matches(channel) … }`
  walks the whole `pattern_subs: Vec<(Bytes, GlobPattern, ConnId, PubSubSender)>` (declared
  `pubsub.rs:523`) on **every** PUBLISH. Cite the loop, not `pubsub.rs:487` — that is the one-line
  body of `GlobPattern::matches`, the callee, and citing it alone makes the cost look like a
  single call when it is `patterns × publishes`. A client registers a pattern once; every
  subsequent PUBLISH pays. Two further sites exist on the introspection path (`channels()`
  `pubsub.rs:810`, `shard_channels()` `:834`), matching an operator-supplied pattern against every
  registered channel. The doc at `pubsub.rs:466-467` — *"an iterative O(nm) algorithm with no
  catastrophic backtracking"* — is **true and misleading**: `O(n·m)` with both terms attacker-chosen
  and uncapped *is* the finding, and the phrase invites the reader to stop there.
- **ACL.** `frogdb-server/crates/acl/src/permissions.rs:73` (key patterns) and `:101` (channel
  patterns) match on the command path. Operator-supplied, so lower severity, but a third amplifier.

**Nothing in the test surface can find this.** `testing/fuzz/fuzz_targets/glob_match.rs` is 13
lines, checks only for panics, and structurally cannot reach these inputs:
`let split = data[0] as usize % data.len()` bounds the *pattern* by `data[0]`, i.e. **≤ 255
bytes**. No amount of fuzzing time produces a 20 KB pattern. Note also that
`.scratch/testing-improvements-round2/issues/open/91-types-acl-config-residual-test-gaps.md`
finding **F16** records the opposite conclusion — *"there is no catastrophic-backtracking DoS
here — the dispatch's concern is already addressed by design"* — on the strength of the matcher
being iterative. That conclusion should be corrected when this issue is ruled on: iterative
removes *exponential* blowup, not quadratic cost, and quadratic on a single-threaded shard with
per-item amplification is the whole problem.

**This needs a user ruling before any code moves.** Three options, none of them proposed here:
(a) a **work budget** inside `glob_match` — a step counter that refuses past a limit; changes
semantics for legitimate large patterns, needs a compat decision, and additionally interacts with
`MAX_STAR_COUNT`'s already-unverified relationship to Redis's `GLOB_MATCH_MAX_RECURSION` (Redis
bounds recursion depth in a recursive matcher; FrogDB bounds star count in an iterative one — the
mirroring is structural, not behavioral, and whether the two agree on a given adversarial pattern
is unverified). (b) a **pattern-length cap at the command boundary** (PSUBSCRIBE, the SCAN family's
`MATCH`, ACL rule creation), leaving `glob_match` untouched. (c) **accept and document**, with the
`pubsub.rs:466-467` doc corrected to say the cost is unbounded rather than merely
non-catastrophic. Until that ruling lands, the correct action is none.

## Acceptance criteria

- [ ] A user ruling is recorded in this issue selecting (a) work budget, (b) boundary
      pattern-length cap, or (c) accept-and-document, with the Redis/Valkey behavior for the same
      adversarial pattern established first
- [ ] Whichever option is chosen, a benchmark or test pins the worst case: a two-star pattern at
      `n = 100k` / `k = 10k` completes within a stated bound (options a/b) or its measured cost is
      recorded in the docs (option c)
- [ ] The `pubsub.rs:466-467` doc comment no longer reads as a safety claim, regardless of which
      option is chosen
- [ ] `testing/fuzz/fuzz_targets/glob_match.rs` splits pattern and key without the `data[0]`
      ≤ 255-byte bound, so long-pattern inputs are reachable at all
- [ ] `.scratch/testing-improvements-round2/issues/open/91-types-acl-config-residual-test-gaps.md`
      F16's "no catastrophic-backtracking DoS" note is corrected or annotated to point here
- [ ] `just test frogdb-types glob`

## Blocked by

None - can start immediately

## Source

Round 38-99 adversarial review of proposal 98 (`.scratch/arch-deepening/proposals/98-scan-grammar-unify.md`),
ruling R1 (escalated).

## Comments
