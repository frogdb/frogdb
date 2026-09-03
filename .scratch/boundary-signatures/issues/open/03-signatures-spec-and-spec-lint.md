# `specs/signatures.md` + `spec-lint` `SIG-` support, census agreement, location rule

Status: ready-for-agent

Size: M

## Why

The census (issue 01 in this directory) says which boundary shapes exist. Nothing yet forces each
shape to have a test at each boundary, and nothing notices when a command's signature drifts
because someone changed its `WalStrategy`. This issue builds the honesty gate: a hand-authored
spec in the same grammar as the locked specs, plus the `spec-lint` checks that keep it, the
census and the tests in agreement.

See [PRD §6](../../PRD.md#6-the-honesty-gate-specssignaturesmd--spec-lint).

## What to build

**1. `specs/signatures.md`**, written from `website/src/data/signatures.json`. Same grammar as
the locked specs, but **no `Status: LOCKED` line and no `Status:` line at all** (decision Q10 in
[PRD §9](../../PRD.md#9-decisions-log-design-session-2026-09-02)) — there is no mutation gate
behind it; it is a coverage contract.

One `## SIG-<NAME>` section per census signature, shaped as in
[PRD §6](../../PRD.md#6-the-honesty-gate-specssignaturesmd--spec-lint):

```markdown
## SIG-UPSERT-VERBATIM-1KEY-NOWAKE

| | |
|---|---|
| Axes | persist=Upsert, propagate=Verbatim, route=SingleKey, wake=None |
| Representative | `SET` |
| Forced by (persistence) | `sig_persistence::upsert_1key_survives_restart` |
| Forced by (replication) | `sig_replication::upsert_1key_reaches_replica_verbatim` |
| Forced by (cluster) | `sig_cluster::upsert_1key_moved_on_foreign_slot` |
| Forced by (txn) | `sig_txn::upsert_1key_queued_and_applied_on_exec` |
```

Member lists are **not** repeated in the spec — they live in `signatures.json` (decision Q2) —
**except** for `Dynamic` signatures (`PersistShape::Dynamic`, `RouteShape::DynamicKeys`), whose
sections list every member with its own per-boundary `Forced by` rows, per
[PRD §3.4](../../PRD.md#34-dynamic-buckets-are-the-one-exception-to-representative-only).

Every `Forced by` row lands in this issue as `MISSING ([gap: ...](...))` pointing at the area
issue that will close it:

- persistence rows and wake rows → issue 04 in this directory
- replication rows → issue 05
- cluster rows → issue 06
- txn rows → issue 07

Use the `MISSING ([gap: <file>](<link>))` form `spec-lint` already understands (see
`MISSING_GAP_RE` near line 285 of `scripts/spec-lint.py`); the link resolves relative to the spec
and the target file must exist. Not every signature needs a row at every boundary; the rule is
fixed in [PRD §6](../../PRD.md#6-the-honesty-gate-specssignaturesmd--spec-lint) ("Which
boundaries a section must force"): persistence row iff `persist ≠ None`, replication row iff
`propagate ≠ None`, cluster and txn rows always, wake row iff `wake ≠ None`. Wake rows are
closed by issue 04 (the `sig_wake` binary). State the rule at the top of the spec and have the
lint derive the requirement from the `Axes` cell: a row present when not required is an error,
a row absent when required is `MISSING`.

**2. `scripts/spec-lint.py` changes.**

- **`SIG-` prefix** in the section and tag regexes. The script already handles `FM-`, `TR-`,
  `LV-` and `CO-`; `SIG-` sections are `## SIG-<NAME>` and `SIG-` tags are comment lines that are
  nothing but the id (`// SIG-UPSERT-VERBATIM-1KEY-NOWAKE`), matching the existing `TAG_LINE_RE`
  discipline: a comment that merely *mentions* a name in prose is a cross-reference, not a tag.
  `SIG-` rows use their own field schema (`Axes`, `Representative`, `Forced by (<boundary>)`), not
  `REQUIRED_FIELDS` — do not force the FM schema onto them.
- **Census agreement**: every name in `website/src/data/signatures.json` has a section in
  `specs/signatures.md` and vice versa; each section's `Axes` cell matches the census entry's
  axes; each section's `Representative` is a member of that signature in the census; a `Dynamic`
  section's member list equals the census member list exactly.
- **Location rule** from [PRD §5](../../PRD.md#5-the-compact-suite-location-not-tooling): a
  `// SIG-` tag outside the compact suite is an error — "move the test or drop the tag". The
  compact suite is the boundary crates (`frogdb-txn`, `frogdb-vll`, `frogdb-persistence`,
  `frogdb-recovery`, `frogdb-replication`, `frogdb-replication-runtime`, `frogdb-cluster`,
  `frogdb-cluster-runtime`) plus any test file whose binary name starts with `sig_`.
- **Existing machinery reused**: `Forced by` resolution against `cargo nextest list`, the
  `MISSING ([gap: ...])` warn-instead-of-fail handling, and the gap-issue allowlist.
- **Gap-issue allowlist**: the allowlist today accepts only `.scratch/hardening/issues/` (the
  rule is stated in the module docstring around lines 29–30 of `scripts/spec-lint.py`, and
  enforced where `MISSING_GAP_RE` matches are resolved, near line 285). Extend it to also accept
  `.scratch/boundary-signatures/issues/`. Keep it an allowlist — an arbitrary path is still an
  error.

**3. Fixture tests.** `spec-lint` has a fixture suite at `scripts/tests/test_spec_lint.py`, run
by `just test-spec-lint` (and as a `lint-spec` prerequisite, and in CI's "Seam Lint Gates" job).
Every new check needs fixtures pinning the *failing* direction, not just the passing one:
a census name with no section, a section with no census name, a wrong `Representative`, a
`Dynamic` section whose member list disagrees with the census, a `// SIG-` tag on a test outside
the compact suite, and a `MISSING ([gap: ...])` pointing outside the allowlist.

**4. Website publication.** `website/scripts/spec-gen.py` globs `specs/*.md` and exits with
"no AREAS entry for ..." for an unknown spec, so add a `signatures` entry to its `AREAS` table
(order, display name, blurb) and run `just spec-gen`. `just spec-gen-check` (CI job "Spec Docs
Generation Check") must pass.

## Acceptance criteria

- [ ] `specs/signatures.md` exists with one `## SIG-<NAME>` section per census signature, no
      `Status:` line, `Axes` / `Representative` / per-boundary `Forced by` rows
- [ ] `Dynamic` signature sections list every member with per-member `Forced by` rows (PRD §3.4)
- [ ] Every `Forced by` row is `MISSING ([gap: ...])` citing issue 04, 05, 06 or 07 in this
      directory by boundary
- [ ] `spec-lint` recognises `SIG-` sections and `// SIG-` tags
- [ ] `spec-lint` fails on: census/spec name mismatch either direction, axes mismatch, a
      `Representative` that is not a census member, a `Dynamic` member-list mismatch, a
      `Forced by (<boundary>)` row present when the `Axes` cell says that boundary has nothing
      to force (PRD §6 rule)
- [ ] `spec-lint` fails on a `// SIG-` tag outside the boundary crates and `sig_*` binaries
- [ ] The gap-issue allowlist accepts `.scratch/boundary-signatures/issues/` in addition to
      `.scratch/hardening/issues/`, and still rejects anything else
- [ ] New fixtures in `scripts/tests/test_spec_lint.py` pin each new check's failing direction;
      `just test-spec-lint` green
- [ ] `just lint-spec` green (warnings for the `MISSING` gaps, no errors)
- [ ] `just spec-gen-check` green after adding the `signatures` `AREAS` entry

## Blocked by

Issues 01 and 02 in this directory.
