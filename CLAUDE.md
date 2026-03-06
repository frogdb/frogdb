# FrogDB

FrogDB is unreleased, pre-production software. Breaking changes are acceptable — sweeping changes that would normally be prohibitive for production software are encouraged here when they improve implementation efficiency.

## Build System

This project uses `just` (see `Justfile`) as the command runner. Always use `just` instead of running `cargo` directly.

Run `just` with no arguments to see all available recipes. Common examples:

```bash
just check                              # type-check the workspace
just check frogdb-core                  # type-check a single crate
just test                               # run all tests
just test frogdb-server                 # run all tests for a specific crate
just test frogdb-server test_publish    # run tests matching a pattern (with --nocapture)
just lint                               # clippy on the workspace
just lint frogdb-persistence            # clippy on a specific crate
just lint-py                            # ruff check
just fmt                                # format Rust code
just fmt frogdb-core                    # format a single crate
just fmt-py                             # format Python code
just concurrency                        # run Shuttle + Turmoil concurrency tests
```

- when running a single test, target the owning crate to avoid rebuilding the entire workspace: `just test frogdb-server test_name` or `cargo test -p frogdb-server test_name -- --nocapture`
- `cargo test` accepts substring patterns (not regex) for test name filtering; use `cargo nextest run -E 'test(/pattern/)'` for regex
- this project uses multiple git worktrees; to clean stale artifacts in the current worktree run `just clean-stale`
- if theres a script/tool, create a Justfile target for it

## Design Documentation

The `spec/` directory contains design documentation. Search/Consult it before making architectural changes.

## Agent Guidelines

- check that altered packages/tools build, lint, and pass tests before finishing a task
- Prefer `just` commands over raw CLI tools
- If a `just` command doesn't work for a task, we should fix it so it does
- For sweeping mechanical changes (renaming identifiers, text substitution, etc.) with many instances, use text manipulation tools like `awk` or `sed` rather than editing files individually
