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

### Execution mode: local (default) or testbox

Builds and tests run in one of two modes. **`local` is the default.**

- **Settle the mode before the first build/test/lint/bench command of a session**, including a
  resumed one. If the prompt names a mode ("local mode", "use the testbox"), use it. Otherwise
  ask the user — never guess, and never switch mid-session on your own.
- The mode is recorded per-worktree: `just build-mode` prints it, `just build-mode testbox`
  sets it. A SessionStart hook injects the recorded value so it survives resumes; confirm it
  anyway. Record the answer so later sessions and the tb-* guard agree.
- Subagents inherit the session's mode — state it explicitly in every dispatch prompt.
- The `tb-*` recipes refuse to run in local mode (`BUILD_MODE=testbox` overrides for a one-off).

**Local mode** — everything runs on this machine, testbox untouched: full-workspace builds,
whole-suite `just test`, `just lint`, concurrency/turmoil suites, benchmarks. Heavy runs follow
the liveness rule below. Say so if a run is slow; do not reach for a testbox to fix it.

**Testbox mode** — heavy compute moves to a remote aarch64 Linux VM (matches production better
than macOS; RocksDB builds from vendored source there) so the laptop stays responsive and
parallel agents don't contend for CPU/disk/memory. Mechanics live in the `blacksmith-testbox`
skill.

- **Run remotely** (`just tb-run "<command>"`): full-workspace builds, `just test` (whole
  suite), `just lint` (clippy compiles everything), concurrency/turmoil suites, benchmarks,
  and anything else expected to take >2 minutes of compute.
- **Still local**: `just fmt`/`fmt-check` (no compilation), single-crate check/test iteration
  loops (`just check <crate>`, `just test <crate> <pattern>`), and other sub-minute commands.
- Lifecycle: `just tb-warmup` at task start (5-minute idle timeout; records the box ID so a
  SessionEnd hook cleans it up). Never call `blacksmith testbox warmup` directly — that
  bypasses the auto-cleanup. Re-warm freely after idle expiry; re-hydration restores from
  cache.
- **One `tb-run` at a time per worktree**: concurrent runs race the rsync sync. Agents in
  different worktrees get separate boxes automatically (IDs are recorded per-worktree).

### Long-Running Commands

- sometimes compilation/test commands can hang. Perform liveness checks on long-running commands to
ensure they are still progressing and not stuck.

## Agent Guidelines

- Check the `Justfile` before performing an action to see if there is already a target to do this
  - eg. build/tests/linting, dev servers, code generation, 
- Code architecture choices should focus on making the software easy to change in the future
- Follow idiomatic Rust patterns and use best practices
- When implementing features or making changes, think about what unit + integration + concurrency
  tests make sense to add. Consider edge cases.
- When designing features, research what implementation Redis, Valkey, and DragonflyDB use for the
  feature. This provides critical insight for decision making.
- When adding new development tools or dependencies:
  - Language runtimes and dev CLI tools (rust, python, node, just, uv, bun, cargo plugins, ...) live
    in `.mise.toml`. If the tool has a mise plugin or is available via the `cargo:`/`ubi:` backends,
    add it there.
  - System libraries and specialized packages that mise cannot manage (libclang, OpenSSL, redis,
    tcl-tk, leiningen, heaptrack, ...) still go in `Brewfile` (macOS) and `shell.nix` (Nix/Linux).
    Keep the two in sync.
  - If you bump Rust, update both `rust-toolchain.toml` and `.mise.toml`. The `sync-toolchain-check`
    lefthook job enforces that they agree.
- Try to keep a single source of truth in documentation (DRY) using Markdown links when referencing
  a topic covered in another section.
- When renaming markdown files or moving content, fix any links that point to the affected
  file/section.
- Run `pwd` before starting and only search for code in the current directory. You may be in a
  worktree directory and not the main directory.
- If you need a paragraph-long comment to justify why the workaround is OK, the code is wrong — fix
  the code.

## Agent skills

### Hardening campaign (ACTIVE)

Foundation-hardening campaign is in progress: core areas (transactions, persistence, replication,
cluster) are being extracted, specced, and mutation-tested; the redis-regression suite is frozen
and operator/frogctl are out of scope. **Read `docs/agents/hardening-campaign.md` before working
on core-area code.**

### Issue tracker

Issues + PRDs live as markdown under `.scratch/<feature>/`. See `docs/agents/issue-tracker.md`.

### Triage labels

Five canonical roles, default strings (`needs-triage`, `needs-info`, `ready-for-agent`,
`ready-for-human`, `wontfix`). See `docs/agents/triage-labels.md`.

### Domain docs

Multi-context: `CONTEXT-MAP.md` at root points to a per-context `CONTEXT.md`
(server / operator / cli). See `docs/agents/domain.md`.

### Coverage depth

`just coverage-depth` adds per-line exec counts and per-function test diversity on top of
plain line coverage (which function is reached by how many *distinct* tests). See
`docs/agents/coverage-depth.md`.

### Remote compilation/testing: Blacksmith testboxes

Gated behind the session's execution mode — see
[Execution mode](#execution-mode-local-default-or-testbox) for the policy and the
`blacksmith-testbox` skill for the mechanics.
