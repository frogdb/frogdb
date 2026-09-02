# FrogDB

FrogDB is a modern, Redis 8.x-compatible database written in Rust. It supports both standalone
and cluster operating modes as well as replication and configurable durability/persistence.

> **Note**: `AGENTS.md` is a symlink to this file — they are the same document.

## Goals

- Correctness
  - Specified behavior proven under various failure modes
- Redis compatible (with differences documented)
  - Deviations should be improvements
- Easy to operate
  - easy introspection and observability
  - easy to adjust configuration without downtime
- Fast
  - Should be at least as fast as competing solutions
- Scalable
  - can operate as a single node with no disk, to cluster of nodes and replicas with persistence

## Development Philosophy

- FrogDB is unreleased, pre-production software. Breaking changes are acceptable — sweeping changes
  that would normally be prohibitive for production software are encouraged here when they improve
  implementation efficiency.
- Inspiration is drawn from high-quality modern database projects like CockroachDB, ScyllaDB, FoundationDB
- Upmost care should be taken to ensure the correctness of the system. Examples include:
  - Extensive regression tests derived from the official Redis test suite to ensure compatibility
  - Extensive distributed systems and concurrency testing to ensure expected behavior during various
    failure modes like network partitions, disk failures, etc.
  - Fuzz testing for security/stability
- Easy to operate in a modern cloud environment, eg:
  - Grafana/Prometheus/OpenTelemetry/dtrace for observability
  - frogctl cli tool
  - Debug web pages
  - operational debug/profiling tools
  - kubernetes operator

## Main Components

- FrogDB
  - The database binary
- frogctl
  - cli tool for managing the database (ops)
- frogdb-operator
  - a Kubernetes operator for FrogDB
- website for info/documentation/marketing
- the assets/ folder has images for branding
- .scratch/roadmap/ contains roadmap and unfinished/follow-up items

## Build System

This project uses `just` (see `Justfile`) for performing almost all tasks required in the
development lifecycle: tests (unit, concurrency, web, fuzzing, jepsen, browser, load/memtier,
regression/compatibility), linting, type checking, building (incl. cross-compilation), formatting,
benchmarking, profiling, docker, debug server, website, code generation (docs/markdown, helm,
grafana, debian), github runner, and cleanup/disk space.

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

**IMPORTANT**: Check the `Justfile` for a recipe before using custom commands like `cargo` directly.

- **BAD**: `cargo test ...`
- **GOOD**: `just test ...`

- When running a single test, target the owning crate to avoid rebuilding the entire workspace:
  `just test frogdb-server test_name`

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

Any command expected to exceed ~2 minutes (workspace build/check, full test suites, `just lint`,
mutation runs) follows this protocol — foreground long runs stall the agent stream and get the
agent killed by the 600s watchdog:

1. **Checkpoint-commit WIP first.** A watchdog kill or API drop loses everything uncommitted.
2. **Launch with the Bash tool's `run_in_background: true`.** The harness tracks it, captures
   output to a file, and re-invokes you when it exits. NEVER detach with `nohup`/`&`/wrapper
   scripts — detached processes are untracked and nothing will ever resume you.
3. **Unthrottle the build.** macOS runs background tasks at background QoS and throttles their
   disk I/O to a crawl (fresh test binaries sit at `_dyld_start`, 0% CPU; rustc runs 10x slow).
   After launching: `pgrep -f 'rustc|cargo|nextest'`, then `taskpolicy -B -p <pid>` for each
   (both commands are sandbox-excluded and work as plain top-level Bash calls).
4. **Poll with short foreground commands** (tail the harness output file). Liveness = log
   growing or CPU accumulating (`ps -Ao pid,pcpu,etime,command`). Static log + 0% CPU for
   2+ minutes = stuck: `sample <pid>` to diagnose, kill, re-run.
5. **Never end your turn while a background command is pending** unless you have nothing left
   to do — and say so explicitly if you do.
6. **Never pipe a long run through filters** (`| grep | tail` buffers everything and hides
   progress); let the harness capture raw output.

## Website/Documentation

FrogDB has a website for documentation using Astro that is published to Github Pages.

**IMPORTANT**: Check for relevant documentation to update when making API/behavior changes

## Code generation

Many markup files (yaml, json) in the repo are generated from Python or Rust scripts.

- Check files for indications that these are generated
- Make changes in the generator code, **not** the generated yaml/json.

Examples:
- github actions
- helm charts
- grafana
- some documentation/markdown

## Web/HTTP/HTML

- **IMPORTANT**: use `bun` for Javascript/Typescript build/test/run/dev/install. **NOT**
  npm/npx/yarn

## Agent Guidelines

- Check the `Justfile` before performing an action to see if there is already a target to do this
  - eg. build/tests/linting, dev servers, code generation, 
- Write simple code, avoid unnecessary complexity
- Code architecture choices should focus on making the software easy to change in the future
- Follow idiomatic Rust patterns and use best practices
- When discovering a bug, write a regression test. Think about how we might prevent new bugs from occurring in a systematic fashion.
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
- When marking todo items complete in markdown files or elsewhere, don't mark them as completed or
  strike them out `~~`, just remove them

## Agent skills

### Locked core areas

Several core areas are **locked** behind failure-mode specs and mutation gates. Which areas,
at what gate, over which crates is declared by each spec's header key block (`Status:` /
`Gate:` / `Crates:` at the top of `specs/<area>.md`) and printed by **`just locked-areas`** —
that manifest is what the tooling reads, so it is never restated here. Boundary ADRs:
`adr/0002`–`0004`; `just lint-locked-areas` keeps the headers honest.

- The specs (`specs/<area>.md`, header `Status: LOCKED`)
  are the contract: behavior changes are **spec-first** (failure-mode row → failing test →
  fix). `just lint-spec` enforces spec↔test agreement (every `FM-<AREA>-NNN` row
  names its forcing tests; every tagged test matches a row) and runs in `just lint`.
- CI runs `just mutants-diff` for every locked crate a PR or a push to main touches and fails on
  any missed mutant; run it locally to iterate faster. Full runs:
  `just mutants <crate>` + `just mutants-gate <crate>` (the gate comes from the spec header). A
  surviving mutant no test can kill is documented *at the code* with why it is unobservable —
  never a blanket skip.
- Put the forcing test in the mutated crate: `cargo mutants -p <crate>` runs only that
  package's own tests, so a row forced solely from `frogdb-server` integration tests
  contributes nothing to the owning crate's score.
- Command families are cargo features (`frogdb-commands`): `core-profile` (common families) is the
  dev default, kept small to keep iteration builds and the build cache fast. Exotic families
  (`json`, `stream`, `geo`, ...) live behind `full`/`cmd-full`. Every distributable artifact —
  Docker image, cross-built binaries, macOS tarballs, deb, Homebrew — builds `cmd-full`
  (ADR-0005 ruling 1), enforced by the `lint-ship-cmd-full` seam lint (`agents/seam-lints.md`).
  Don't alternate feature flags between commands in an iteration loop — it thrashes the build
  cache.

### Issue tracker

Issues + PRDs live as markdown under `.scratch/<feature>/`. See `agents/issue-tracker.md`.

### Triage labels

Five canonical roles, default strings (`needs-triage`, `needs-info`, `ready-for-agent`,
`ready-for-human`, `wontfix`). See `agents/triage-labels.md`.

### Domain docs

Multi-context: `CONTEXT-MAP.md` at root points to a per-context `CONTEXT.md`
(server / operator / cli). See `agents/domain.md`.

### Coverage depth

`just coverage-depth` adds per-line exec counts and per-function test diversity on top of
plain line coverage (which function is reached by how many *distinct* tests). Reports land in
`.scratch/testing-improvements/audit/`; `just coverage-depth-calibrate <crate>` sizes a run,
`just test-coverage-depth` tests the pipeline itself.

### Seam lints

Fifteen chokepoint gates encode "every X must go through Y" invariants (clock reads, metrics
emission, redirect replies, durable-ack writes, ...). `just lint-gates` runs the compile-free subset on every commit
(lefthook, unconditional) and in CI (`seam-gates` job); the full family runs in `just lint`. See
`agents/seam-lints.md`.

### Remote compilation/testing: Blacksmith testboxes

Gated behind the session's execution mode — see
[Execution mode](#execution-mode-local-default-or-testbox) for the policy and the
`blacksmith-testbox` skill for the mechanics.
