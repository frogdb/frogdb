# FrogDB

## Build System

This project uses `just` (see `Justfile`) as the command runner. Always use `just` instead of running `cargo` directly.

Run `just` with no arguments to see all available recipes. Common examples:

```bash
just check-crate frogdb-core        # type-check a single crate
just test-crate frogdb-server       # run tests for a specific crate
just test-one test_publish          # run a specific test by name (with --nocapture)
just lint-crate frogdb-persistence  # clippy on a specific crate
just fmt                            # format code
just fmt-check                      # check formatting (CI)
just concurrency                    # run Shuttle + Turmoil concurrency tests
just proptest                       # run property-based tests
```

- this project uses multiple git worktrees; to clean stale artifacts across all worktrees run `just clean-worktrees`
- if theres a script/tool, create a Justfile target for it

## Design Documentation

The `spec/` directory contains design documentation. Search/Consult it before making architectural changes.

## Agent Guidelines

- check that altered packages/tools build, lint, and pass tests before finishing a task
- Prefer `just` commands over raw CLI tools
- If a `just` command doesn't work for a task, we should fix it so it does
