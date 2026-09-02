# `workflow_gen` cannot see the locked-areas manifest — give it one helper that reads it

Status: done
Type: mechanism (interface slice, carved from 15 and 16)
Severity: n/a — enabling slice; 15 and 16 both need it and would otherwise each invent it
Area: campaign mechanism / CI
Blocked by: 13

## Problem

Issues 15 (`mutants-diff` job) and 16 (weekly full gate) both derive their CI matrix from the
locked-areas manifest (issue 13: spec header key blocks, parsed by `scripts/locked_areas.py`).
`workflow_gen` is a uv project under `.github/workflows/workflow_gen/` with its own `sys.path`;
it cannot `import locked_areas` today. Two issues adding the same shim in parallel is a
guaranteed merge conflict on `helpers.py`.

## What to build

One function in `.github/workflows/workflow_gen/src/workflow_gen/helpers.py`:

```python
def locked_areas() -> list[Spec]:
    """Locked-area specs from `specs/*.md` headers, via scripts/locked_areas.py.

    Only LOCKED areas are returned (DRAFT specs carry no gate or crates).
    Raises if the manifest fails validation — a bad header must break generation,
    not silently drop a crate from a CI matrix.
    """
```

- Locate the repo root the way `_read_rust_version` does (walk `Path(__file__).parents` for a
  marker — reuse the same loop, do not duplicate it: extract `_repo_root()` and use it in both).
- `sys.path.insert(0, str(root / "scripts"))` then `import locked_areas`; call
  `locked_areas.load(...)` / `validate(...)` per its API (read the module — do not re-parse
  headers). Any validation error → raise `RuntimeError` with the parser's messages joined.
- Return only specs with `is_locked`. Order = the parser's order (stable across runs, or
  `workflow-gen --check` will flap).
- No caller yet. Add a unit test in `workflow_gen`'s own test layout if one exists (check
  `pyproject.toml` for pytest); otherwise a `__main__`-level smoke is not needed — the forcing
  test below suffices.

## Acceptance criteria

- [ ] `helpers.locked_areas()` returns the five LOCKED specs (`txn`, `vll`, `persistence`,
      `replication`, `cluster`) with `.crates` and `.gate` populated, from a `uv run --project
      .github/workflows/workflow_gen python -c` one-liner.
- [ ] A spec header with a bad `Gate:` (edit a copy in a temp checkout, or monkeypatch) makes
      the call raise, not return a shorter list.
- [ ] `just workflow-gen --check` still passes (no generated output changes — this issue adds no
      caller).
- [ ] `just lint-py` and `just fmt-py-check` clean.

## Files likely touched

- .github/workflows/workflow_gen/src/workflow_gen/helpers.py

## Decisions

D1

## Resolution

Landed as `23da7cb9` (merge `41351567`) on `locked-areas-mechanical/impl` (2026-09-02).
`helpers.locked_areas()` returns the parser's `Spec` list for LOCKED areas only, sorted by
spec path, raising `RuntimeError` on any manifest validation error. The repo-root walk is
one `_repo_root()` shared with `_read_rust_version`. `workflow_gen` has no test layout, so
the raise path is demonstrated in the report rather than pinned by a test. Review left four
Minor items (prefer `sys.path.append`, private `_ROOT_MARKER`, no caching — callers call it
once, no self-explaining error when `scripts/` is missing); the first two are folded into
issue 15's brief since it edits the same file next. `Spec.area` is uppercase — job ids and
matrix keys in 15/16 use `.area.lower()`.
