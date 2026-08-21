# 21 — `AdminConfig`'s doc comment claims `#[param(skip)]` on every field; all three are registered params

Status: needs-triage

## What to build

`frogdb-server/crates/config/src/admin.rs:10-11` carries this comment, directly above the
`#[derive(… ConfigParams)]` on `AdminConfig`:

```rust
// No fields are exposed as CONFIG GET/SET parameters; each carries an explicit
// `#[param(skip)]` to satisfy the per-field coverage guarantee.
```

Not one of the three fields carries `#[param(skip)]`. All three carry `#[param(name = …)]`:
`admin-enabled` (`admin.rs:18`), `admin-port` (`:23`), `admin-bind` (`:28`). All three are
registered into the golden parameter table — `frogdb-server/crates/config/src/params.rs:383`
extends the row list from `AdminConfig::PARAMS` with the inline comment
`// admin-enabled, admin-port, admin-bind`, and the golden rows themselves sit at `params.rs:1061`
(`admin-enabled`), `:1068` (`admin-port`), and `:1075` (`admin-bind`), each with
`mutable: false`. The comment states the exact opposite of the code it sits on, and it is the
comment that is stale: the params are real, boot-fixed, and `CONFIG GET`-readable.

This is documentation-only and latent — no runtime behavior is wrong — but it is a lie about the
security-relevant admin surface sitting three lines above the struct that defines it, and it is
the kind of comment a reader trusts instead of re-deriving. It misleads anyone auditing which
config keys are reachable over `CONFIG GET` from an authenticated connection.

The drift has a known origin: issue **13-01** (`.scratch/arch-deepening/issues/open/`,
promote-or-justify audit of the `#[param(skip)]` fields) promoted `admin.enabled`, `admin.port`,
and `admin.bind` to *promote-immutable* in Pass 2a (see its `admin.rs — AdminConfig` table). The
attributes and golden rows were updated; this comment was not. That makes this a residue of
13-01, not a duplicate of it — 13-01 is the audit ledger and its status is `ready-for-human`,
while this is a one-line correction at the code.

Fix direction: delete the comment, or replace it with the truth (three params, all
`mutable: false`, `CONFIG GET`-only, boot-fixed). While there, check whether the same stale
"per-field coverage guarantee" phrasing was copy-pasted onto other config sections that have
since been promoted.

## Acceptance criteria

- [ ] `config/src/admin.rs:10-11` no longer claims `#[param(skip)]`; the comment either goes away
      or accurately describes three registered, immutable, `CONFIG GET`-only params
- [ ] A sweep confirms no other `ConfigParams`-deriving struct carries the same stale
      "each carries an explicit `#[param(skip)]`" claim while registering named params
- [ ] Regression: the existing golden-parameter snapshot test in `frogdb-config` continues to
      pin the three `admin-*` rows (`params.rs:1061`, `:1068`, `:1075`) with `mutable: false`, so
      the comment and the table cannot silently diverge again
- [ ] `just test frogdb-config param` green

## Blocked by

None - can start immediately

## Source

Round 38-99 adversarial review of proposal 76
(`.scratch/arch-deepening/proposals/76-observability-extractors.md`) — review non-blocking item
**N15** (bonus finding), promoted by the author into §Problem 8 (`:319-331`) and hotfix candidate
**H6** (`:791-797`), ruled "issue candidate only" because 76 does not edit `config/`. N15 and H6
are the same defect at the same two lines; this is one issue covering both.

## Comments
