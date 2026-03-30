---
name: doc-sync
description: >
  Sync docs with code, update docs, doc drift, are docs up to date, audit documentation,
  spec is wrong, doc is stale, check documentation, verify specs, docs out of date,
  after code changes affecting documented behavior.
---

# Documentation Sync

Detect and fix drift between FrogDB's documentation and the actual codebase. Documentation
lives in `website/src/content/docs/` (architecture, operations, guides), plus project docs
(`CLAUDE.md`, `README.md`, `AGENTS.md`), and `testing/jepsen/README.md`.

## When To Use

- **After code changes**: Renamed a crate, added workspace members, changed a dependency version,
  modified a trait/struct, updated config keys — any change that docs may reference
- **User reports stale docs**: "this spec is wrong", "doc says X but code does Y"
- **Periodic audit**: Systematic scan for all drift across the doc corpus

## Trigger Modes

### 1. Change-Triggered

When specific code changes have been made (you can see them in `git diff` or the user describes them):

1. Run `git diff --name-only` (or use the user's description) to identify changed files
2. Consult `references/doc-map.md` to find which docs reference those code areas
3. Run targeted checks on those docs only
4. Fix obvious drift automatically; ask about ambiguous cases

### 2. User-Described

When the user says "I renamed X to Y" or "I changed X":

1. Grep all docs for the old name/value
2. Present findings
3. Fix or ask depending on classification (see below)

### 3. Audit

Systematic scan of all documentation in priority order. Use `references/checkable-assertions.md`
as a checklist of machine-verifiable claims.

**Priority order:**
1. **P1 Structural** — workspace members, crate names, edition, dep versions, cross-doc links
2. **P2 API surface** — struct/trait/enum definitions in code blocks vs actual code, config keys
3. **P3 Behavioral** — spot-check key behavioral claims, metric names, Justfile recipes

## Check Categories

### P1: Structural (check first — fastest, highest signal)

| Check | Source of Truth | Docs That Reference It |
|-------|----------------|----------------------|
| Workspace members list | `Cargo.toml` `[workspace] members` | REPO.md, ARCHITECTURE.md, INDEX.md |
| Crate count | `Cargo.toml` member count | ARCHITECTURE.md ("5 crates"), INDEX.md |
| Crate names | `Cargo.toml` `[workspace.dependencies]` internal crates | All docs mentioning `frogdb-*` |
| Edition | `Cargo.toml` `[workspace.package] edition` | REPO.md |
| Dependency versions | `Cargo.toml` `[workspace.dependencies]` | REPO.md code blocks |
| Cross-doc links | `](*.md)` patterns in all docs | All spec docs |
| File paths in docs | Paths like `crates/lua/` | REPO.md, ARCHITECTURE.md |

**How to check:**
- Read `Cargo.toml` for ground truth
- Grep docs for the old/expected values
- Compare

### P2: API Surface (check second — medium effort)

| Check | Source of Truth | Docs That Reference It |
|-------|----------------|----------------------|
| Struct/trait definitions in code blocks | Actual source files | ARCHITECTURE.md, EXECUTION.md, STORAGE.md, AUTH.md |
| Config keys and defaults | `crates/server/src/config/` | CONFIGURATION.md, DEPLOYMENT.md |
| Command lists by type | `crates/commands/src/*.rs` | types/*.md, COMMANDS.md |
| Public API re-exports | `lib.rs` files | REPO.md |

**How to check:**
- Read the source file containing the type definition
- Compare against the code block in the doc
- Flag differences (added fields, renamed fields, changed types)

### P3: Behavioral (check last — highest effort, lowest signal)

| Check | Source of Truth | Docs That Reference It |
|-------|----------------|----------------------|
| Justfile recipe names | `Justfile` | REPO.md, CLAUDE.md, TESTING.md |
| Metric names | `crates/telemetry/src/definitions.rs` | OBSERVABILITY.md |
| Info section names | `crates/telemetry/src/status.rs` | OBSERVABILITY.md |
| Default config values | Config struct defaults | CONFIGURATION.md |

## Discrepancy Classification

### Code Authoritative (fix doc automatically)

These are cases where the code is clearly correct and the doc is stale:

- **Renamed crate**: doc says `frogdb-lua`, code has `frogdb-scripting` → fix doc
- **Old version**: doc says `edition = "2021"`, Cargo.toml has `"2024"` → fix doc
- **Old dep version**: doc says `thiserror = "1"`, Cargo.toml has `"2"` → fix doc
- **Wrong member count**: doc says "5 crates", workspace has 28 members → fix doc
- **Stale file path**: doc says `crates/lua/`, actual path is `crates/scripting/` → fix doc
- **Added struct fields**: code has new fields not in doc code block → add them to doc
- **Removed struct fields**: doc shows fields that no longer exist → remove from doc

### Ambiguous (ASK the user)

These require human judgment:

- **Doc describes unimplemented behavior**: Could be aspirational spec (intentional) or stale
  promise (should be removed). Present both interpretations.
- **Doc says "not supported" but code implements it**: Feature may have been added after doc was
  written. Ask if doc should be updated.
- **Doc describes constraint code doesn't enforce**: Could be an intentional spec the code should
  eventually enforce, or a stale constraint.
- **Behavioral claim that's hard to verify**: e.g., "operations are linearizable" — don't try to
  prove this from code.

**Ambiguity presentation format:**

```
REVIEW NEEDED: [brief description]

Doc (file:line): "[quoted text from doc]"
Code (file:line): [what the code actually shows]
Assessment: [your analysis of what likely happened]

Options:
  A) Update doc to match code
  B) Keep doc as-is (aspirational spec)
  C) Something else — please describe
```

## What NOT To Check

- **Prose descriptions**: Don't verify narrative/explanatory text against code
- **Mermaid diagrams**: Flow diagrams are illustrative, not machine-verifiable
- **Illustrative code examples**: Snippets marked as pseudocode or simplified examples
- **ROADMAP.md phases**: These are historical/aspirational, not claims about current code
- **SOURCES.md external links**: Don't check if external URLs are live
- **docs/todo/\***: These are explicitly future/unimplemented work
- **Comments about design rationale**: "We chose X because Y" — don't verify the reasoning

## Output Format

### For automatic fixes

```
FIXED: [description]
  File: [path:line]
  Was: [old text]
  Now: [new text]
```

### For items needing review

Use the ambiguity presentation format above.

### Audit summary (end of audit mode)

```
Audit Summary
─────────────
Checked: [N] assertions across [M] files
Fixed:   [X] (auto-applied)
Review:  [Y] (need your input)
Clean:   [Z] (no drift found)
```

## Reference Files

| File | When to Read |
|------|-------------|
| `references/doc-map.md` | Change-triggered mode: map changed code → affected docs |
| `references/checkable-assertions.md` | Audit mode: systematic checklist of verifiable claims |

Read these files at the start of the relevant mode. They contain pre-computed mappings
that avoid scanning all 50+ docs for every check.

## Scope Boundaries

**In scope:**
- `website/src/content/docs/architecture/*.md` (internals/contributor docs)
- `website/src/content/docs/operations/*.md` (operator docs)
- `website/src/content/docs/guides/*.md` (user docs)
- `CLAUDE.md` (project instructions)
- `README.md` (project overview)
- `AGENTS.md` (agent guidelines)
- `testing/jepsen/README.md` (Jepsen test docs)
- `testing/redis-compat/README.md` (redis-compat docs)

**Out of scope:**
- `docs/todo/**` — these are explicitly unimplemented/future work
- `.claude/skills/*/SKILL.md` — maintained by the skill-creator skill
- Source code comments — not documentation drift
- Git history / commit messages
- External URLs / third-party docs

**Defer to other skills:**
- Redis compatibility issues → `/redis-compat`
- Architecture changes → `/db-architect`
- Build/test issues → `/check`
