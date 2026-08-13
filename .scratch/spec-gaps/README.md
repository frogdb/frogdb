# Spec gaps — 2026-08-13 anti-pattern review follow-through

State: active

Ten issues filed against `specs/persistence.md`, `specs/txn.md`, `specs/vll.md`, and
`specs/blocking.md`, sourced from the 2026-08-13 adversarial anti-pattern reviews in
[`.scratch/formal-spec/reviews/2026-08-13-antipattern/`](../formal-spec/reviews/2026-08-13-antipattern/):

- [`spec-review-persistence.md`](../formal-spec/reviews/2026-08-13-antipattern/spec-review-persistence.md)
  — 1 CRITICAL, 4 HIGH, 5 ADVISORY on `specs/persistence.md` (LOCKED), plus 3 HIGH / 3 ADVISORY
  hazards in the issue-24 ruling's persistence-layer follow-through.
- [`spec-review-txn-vll-blocking.md`](../formal-spec/reviews/2026-08-13-antipattern/spec-review-txn-vll-blocking.md)
  — 1 CRITICAL + 1 HIGH + 4 ADVISORY on `specs/txn.md`; 4 HIGH + 2 ADVISORY on `specs/vll.md`;
  3 HIGH on `specs/blocking.md`.

Every issue's `## Ruling (2026-08-13)` section carries the settled decision for that finding,
recorded the same day the reviews landed. All issues are spec-first: a row lands only alongside
its forcing test (`just lint-spec` enforces row↔test agreement), and mutation coverage on touched
locked crates is triaged via `just mutants-diff` before push.

Issues 01-05 are the persistence findings; 06-10 are the txn/vll/blocking findings.

Issues: [open](issues/open/) / [done](issues/done/)
