# FrogDB

## Build System

This project uses `just` (see `Justfile`) as the command runner. Always use `just` instead of running `cargo` directly.

Run `just` with no arguments to see all available recipes. Common examples:

```bash
just check              # check for compilation errors for all targets
just test               # run all tests
just test-crate <name>  # test a particular crate
just test-one <name>    # run an individual test
just fmt                # format all files
just lint               # clippy
just deny               # check for invalid package dependencies
just concurrency        # run Shuttle + Turmoil concurrency/chaos tests
```

- this project uses multiple git worktrees; to clean stale artifacts across all worktrees run `just clean-worktrees`

## Design Documentation

The `spec/` directory contains design documentation. Consult it before making architectural changes. Key docs:

- `INDEX.md` - Master design overview
- `ARCHITECTURE.md` - Component relationships and boundaries
- `CONCURRENCY.md` - Shard worker architecture, scatter-gather
- `CONSISTENCY.md` - Linearizability and consistency guarantees
- `PERSISTENCE.md` - Storage engine and durability
- `REPLICATION.md` - Replication protocol
- `CLUSTER.md` - Clustering design
- `TESTING.md` - Test strategy

## Redis Compatibility Tests (Conditional)

Run `just redis-compat` when changes modify command behavior, argument parsing, or error responses.
This validates compatibility with the Redis protocol and expected command semantics.

## Agent Guidelines

- **ALWAYS** ensure changed packages build, lint, and pass tests before finishing a task
- Scope checks to the crates you actually changed — don't run the full workspace suite when only one crate is affected:
  - `just check-crate <name>` instead of `just check`
  - `just lint-crate <name>` instead of `just lint`
  - `just test-crate <name>` instead of `just test`
  - For a single test file: `cargo test -p <crate> --test <test_name>`
- Only run workspace-wide `just check` / `just lint` / `just test` when changes span multiple crates or you're doing a final pre-commit/pre-PR validation
- Prefer `just` commands over raw CLI tools
- If a `just` command doesn't work for a task, we should fix it so it does
