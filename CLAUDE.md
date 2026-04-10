# FrogDB

FrogDB is unreleased, pre-production software. Breaking changes are acceptable — sweeping changes
that would normally be prohibitive for production software are encouraged here when they improve
implementation efficiency.

## Build System

This project uses `just` (see `Justfile`). 

Examples:

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
```

**IMPORTANT**: Check the `Justfile` for a recipe before using custom commands.

- When running a single test, target the owning crate to avoid rebuilding the entire workspace:
  `just test frogdb-server test_name`
- If you encounter an error with `sccache`, rerun the command prefixed with `RUSTC_WRAPPER=""`

## Agent Guidelines

- Check the `Justfile` before performing an action to see if there is already a target to do this
  - eg. build/tests/linting, dev servers, code generation, 
- For sweeping mechanical changes (renaming identifiers, text substitution, etc.) with many
  instances, use text manipulation tools like `awk` or `sed` rather than editing files individually
- Code architecture choices should focus on making the software easy to change in the future
- Follow idiomatic Rust patterns and use best practices
- When implementing features or making changes, think about what unit + integration + concurrency
  tests make sense to add. Consider edge cases.
- When designing features, research what implementation Redis, Valkey, and DragonflyDB use for the
  feature. This provides critical insight for decision making.
- When adding new development tools or dependencies:
  - Language runtimes and dev CLI tools (rust, python, node, just, uv, bun, cargo plugins, ...)
    live in `.mise.toml`. If the tool has a mise plugin or is available via the `cargo:`/`ubi:`
    backends, add it there.
  - System libraries and specialized packages that mise cannot manage (libclang, OpenSSL, redis,
    tcl-tk, leiningen, heaptrack, ...) still go in `Brewfile` (macOS) and `shell.nix` (Nix/Linux).
    Keep the two in sync.
  - If you bump Rust, update both `rust-toolchain.toml` and `.mise.toml`. The
    `sync-toolchain-check` lefthook job enforces that they agree.
- Try to keep a single source of truth in documentation (DRY) using Markdown links when referencing
  a topic covered in another section.
- When renaming markdown files or moving content, fix any links that point to the affected
  file/section.
- Run `pwd` before starting and only search for code in the current directory. You may be in a
  worktree directory and not the main directory.
