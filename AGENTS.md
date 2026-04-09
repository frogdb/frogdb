# FrogDB

FrogDB is a Redis-compatible database written in Rust.

- It is unreleased, pre-production software.
- Breaking/sweeping changes are acceptable

## Build/Task System

This project uses `just` (see `Justfile`) for performing almost all tasks in the development
lifecycle. 

The `Justfile` has recipes for tasks like:
- tests
  - unit
  - concurrency
  - web
  - fuzzing
  - jepsen
  - browser
  - load/memtier
  - regression/compatibility
- linting
- type checking
- building
  - caching/sccache/config
  - cross-compilation
- formatting
- benchmarking
- profiling
- docker
- debug server
- website
- code generation
  - docs
  - helm
  - grafana
  - debian
- github runner
- cleaning/disk space

Common examples:

```bash
just check frogdb-core                  # type-check a single crate
just test frogdb-server                 # run all tests for a specific crate
just test frogdb-server test_publish    # run tests matching a regex pattern
just lint frogdb-persistence            # clippy on a specific crate
just lint-py                            # ruff check
just fmt frogdb-core                    # format a single crate
just fmt-py                             # format Python code
```

**IMPORTANT**: Check the `Justfile` for a task/recipe before running commands like `cargo` directly.

- **BAD**: `cargo test ...`
- **GOOD**: `just test ...`

- Run tests/checks against the specific crate/package/file so save time
- If you encounter an error with `sccache`, rerun the command prefixed with `RUSTC_WRAPPER=""`

## Agent Guidelines

- Write simple code
- Follow Rust best practices
- Code architecture choices should focus on making the software easy to change in the future
- Follow idiomatic Rust patterns and use best practices
- When implementing features or making changes, think about what unit + integration + concurrency
  tests make sense to add. Consider edge cases.
- When designing features, research what implementation Redis, Valkey, and DragonflyDB use for the
  feature. This provides critical insight for decision making.
- Look for documentation to update if changing behavior
- When adding new development tools or dependencies (e.g., cargo plugins, CLI tools for testing),
  update both `Brewfile` (for macOS) and `shell.nix` (for Linux/Nix) to keep the development
  environments in sync.