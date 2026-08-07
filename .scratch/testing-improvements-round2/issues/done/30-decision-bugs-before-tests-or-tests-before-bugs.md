# Decision D2 — bugs before tests, or tests before bugs (and the ten semantics calls that gate specific tests)

Status: done
Type: decision
Origin: round-2 testing audit 2026-07-28 — 15 parallel area audits, `.scratch/testing-improvements-round2/`
Source: MASTER.md §7 D2
Area: whole-workspace scheduling

## Context

The audit was commissioned to find testing gaps. It found those, but its dominant result is roughly
**40 suspected live defects**, most of them silent-data-loss or consistency-violation shaped, in
code that is nominally covered. None of them was found by a test failure; all were found by
reading, and the proposed tests fail against today's code.

That inverts the usual ordering question. Writing a pinning test first documents current — wrong —
behaviour; fixing first risks unverified fixes. And a third case exists that neither ordering
resolves: **several proposed tests cannot be written at all until a semantics decision is made**,
because there is no correct answer to assert.

## Options

**(a) Tests first — pin current behaviour, then fix.** *Consequence:* every fix lands with a test
that demonstrably changed from red to green, and the suite gains a regression net immediately. But
~40 tests get written asserting behaviour that is known-wrong, each has to be inverted when its fix
lands, and a reviewer reading the suite mid-flight sees data-loss behaviour presented as
intentional. It also delays the fixes themselves, and the audit classifies most of these as
silent-data-loss or consistency violations.

**(b) Fixes first — repair, then test.** *Consequence:* the highest-severity defects close soonest.
But nothing proves the pre-fix behaviour was what the audit claimed, so a mis-read finding produces
a "fix" for a non-bug, and there is no red-to-green evidence for any of them. The audit explicitly
warns that only two of the ~40 were verified directly by the coordinator (15/F1 and the coverage
caveat); the rest carry the auditing agent's file:line evidence and "need confirmation before or
during fixing".

**(c) Per-defect: pin first only where the fix is contested or the semantics are unsettled;
otherwise fix-with-test in one change.** *Consequence:* avoids writing ~40 throwaway pins while
keeping red-to-green evidence for each fix, at the cost of a triage pass over the defect list and
a less uniform process.

**(d) Neither, for the ten decision-gated items below** — they cannot proceed under any of (a),
(b) or (c) until their semantics call is made. Their tests have nothing to assert.

## The ten semantics calls that gate specific tests

Reproduced from MASTER.md §7 in the audit's priority order. Each blocks at least one proposed test:

1. failed-spill behaviour *(01/F2)* — delete-and-replicate-`DEL` as today, or reject the write with
   OOM and keep the key. Blocks issue 20, `.scratch/testing-improvements-round2/issues/`.
2. declared-vs-actual WAL key enforcement *(01/F4)* — property test, compile-time/derive check, or
   documented best-effort. Blocks criterion 4 of issue 19, same directory.
3. scatter partial-failure contract *(03/F1)* — fail loudly / partial-with-distinguishable-reply /
   status-quo-documented. Blocks issue 23, same directory.
4. `slowlog-max-len` per-shard vs global *(02/F10)*.
5. `o` / `c` keyspace-event classes — implement or reject *(02/F12)*. Blocks the last criterion of
   issue 21, same directory.
6. script-timeout write policy *(09/F4)* — no writes survive, or writes survive and replicate
   identically. Blocks the last criterion of issue 24, same directory.
7. cross-shard `SORT` — fix, guard, or document *(06/F4)*.
8. `SO_REUSEPORT` release-only gate *(05/F4)*.
9. search write-visibility seam *(10/F7)*.
10. INFO fields that are currently fabricated constants — omit rather than fake *(05/F11)*.

## Recommendation

None. MASTER.md §7 records no recommendation for D2; the audit deliberately leaves the ordering to
the maintainer. Two constraints any choice must satisfy, both stated by the audit:

- Only 15/F1 and the two coverage-pipeline defects were verified directly by the coordinator. Every
  other defect needs confirmation **before or during** fixing, whichever ordering is chosen.
- The ten items above are not scheduling preferences — their tests cannot assert anything until the
  call is made, so they need answers regardless of which of (a)/(b)/(c) is picked.

## Depends on

Nothing blocks this decision. It gates issues 19, 20, 21, 23 and 24,
`.scratch/testing-improvements-round2/issues/` (via the specific calls listed above) and the
scheduling of the defect issues in the 35–76 range, same directory. Issue 32, same directory
(infrastructure-first or findings-first), is the orthogonal scheduling decision and should be taken
together with this one.

## Re-triage 2026-08-06

**Verdict: superseded**

The foundation-hardening campaign (2026-07-31 → 2026-08-05, all four areas LOCKED) settled the
ordering as option (c) and wrote it down as a standing rule: *"Bug fixes follow spec-first order:
failure-mode row → failing test → fix"* (`docs/agents/hardening-campaign.md:90`), enforced two-way
by `just lint-failure-modes`. The retrospective (`e3e51a52`,
`.scratch/hardening/retrospective-2026-08-05.md` §"What worked" #3) records the same practice as
one of the five things that worked. No throwaway pins were written; each of the ~30 defects the
campaign closed landed as row → red test → fix in one change. Of the ten decision-gated semantics
calls, three are now answered — #1 failed spill (keep the key, `0d727d05`, issue 41 → `done/`),
#6 script-timeout write policy (issue 60 → `done/`, pinned by
`shard-harness/tests/script_timeout_effects.rs`), #10 fabricated INFO constants (retrospective
standing rule: *"no plausible-looking constants in INFO/logs/stats"*). The remaining seven stay
with their own issues (19, 21, 23, 59 and the un-filed 02/F10, 05/F4, 10/F7) rather than with this
decision.
