# FrogDB

FrogDB is unreleased, pre-production software. Breaking changes are acceptable — sweeping changes
that would normally be prohibitive for production software are encouraged here when they improve
implementation efficiency.

## Build System

This project uses `just` (see `Justfile`) as the command runner. Always use `just` instead of
running `cargo` directly.

Run `just` with no arguments to see all available recipes. Common examples:

```bash
just check                              # type-check the workspace
just check frogdb-core                  # type-check a single crate
just test                               # run all tests
just test frogdb-server                 # run all tests for a specific crate
just test frogdb-server test_publish    # run tests matching a regex pattern
just lint                               # clippy on the workspace
just lint frogdb-persistence            # clippy on a specific crate
just lint-py                            # ruff check
just fmt                                # format Rust code
just fmt frogdb-core                    # format a single crate
just fmt-py                             # format Python code
just concurrency                        # run Shuttle + Turmoil concurrency tests
```

- When running a single test, target the owning crate to avoid rebuilding the entire workspace:
  `just test frogdb-server test_name`
- Tests are run via `cargo nextest` (not `cargo test`); pattern args use regex filter expressions
  (`-E 'test(/pattern/)'`), which `just test` handles automatically
- Nextest enforces a 15s hard timeout per test (configured in `.config/nextest.toml`)
- This project uses multiple git worktrees; to clean stale artifacts in the current worktree run
  `just clean-stale`
- If there's a script/tool, create a Justfile target
- Tests require `cargo-nextest` (`cargo install cargo-nextest`); if missing, `just test` will fail
- If you encounter an error with `sccache`, rerun the command prefixed with `RUSTC_WRAPPER=""`
- The `target/` directory can grow large (10GB+) due to debug symbols, incremental compilation
  cache, and multiple profiles. Use `just target-size` to check its size and `just clean` to reclaim
  disk space when needed.

## Documentation

The `docs/` directory contains documentation organized by audience:

- `docs/users/` — end-user guides (commands, scripting, pub/sub, event sourcing, etc.)
- `docs/operators/` — operational guides (configuration, deployment, persistence, replication, etc.)
- `docs/contributors/` — internals documentation (architecture, concurrency, storage, VLL, etc.)

Consult these docs before making architectural changes.

## Agent Guidelines

- Always ask for clarifying questions before starting to plan/research or implement. Databases
  require extreme attention to detail — overlooked edge cases can be disastrous.
- Check that altered packages/tools build, lint, and pass tests before finishing a task
- Prefer `just` commands over raw CLI tools
- If a `just` command doesn't work for a task, we should fix it so it does
- For sweeping mechanical changes (renaming identifiers, text substitution, etc.) with many
  instances, use text manipulation tools like `awk` or `sed` rather than editing files individually
- Code architecture choices should focus on making the software easy to change in the future
- Follow idiomatic Rust patterns and use best practices
- When implementing features or making changes, think about what unit + integration + concurrency
  tests make sense to add. Consider edge cases.
- When designing features, research what implementation Redis, Valkey, and DragonflyDB use for the
  feature. This provides critical insight for decision making.
- When non-trivial functionality is required, evaluate if a Rust crate is available which can help.
  When a library has a copyleft license like GPL or AGPL, prompt before including it.
- When adding new development tools or dependencies (e.g., cargo plugins, CLI tools for testing),
  update both `Brewfile` (for macOS) and `shell.nix` (for Linux/Nix) to keep the development
  environments in sync.
- Try to keep a single source of truth in documentation (DRY) using Markdown links when referencing
  a topic covered in another section.
- When renaming markdown files or moving content, fix any links that point to the affected
  file/section.
- Run `pwd` before starting and only search for code in the current directory. You may be in a
  worktree directory and not the main directory.
