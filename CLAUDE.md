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

### Remote execution: Blacksmith testboxes

Heavy compute belongs on a remote Linux testbox, not this machine — it keeps the laptop
responsive and stops parallel agents from contending for CPU/disk/memory. See the
`blacksmith-testbox` skill for details.

- **Run remotely** (`just tb-run "<command>"`): full-workspace builds, `just test` (whole
  suite), `just lint` (clippy compiles everything), concurrency/turmoil suites, benchmarks,
  and anything else expected to take >2 minutes of compute.
- **Run locally**: `just fmt`/`fmt-check` (no compilation), single-crate check/test iteration
  loops (`just check <crate>`, `just test <crate> <pattern>`), and other sub-minute commands.
- Lifecycle: `just tb-warmup` at task start (5-minute idle timeout; records the box ID so a
  SessionEnd hook cleans it up). Never call `blacksmith testbox warmup` directly — that
  bypasses the auto-cleanup. Re-warm freely after idle expiry; re-hydration restores from
  cache.
- **One `tb-run` at a time per worktree**: concurrent runs race the rsync sync. Agents in
  different worktrees get separate boxes automatically (IDs are recorded per-worktree).
- The box is aarch64 Linux (matches production better than macOS); RocksDB builds from
  vendored source there, not Homebrew.

### Long-Running Commands (watchdog rules)

- **Known issue (root-caused 2026-07-12)**: background shell tasks are spawned at Darwin
  background QoS (`ps` STAT `SN`, nice 5), and macOS throttles their disk I/O to a trickle. Fresh
  debug test binaries then take **minutes** to page in — processes sit at `_dyld_start` with 0%
  CPU (nextest `--list` phase looks hung; rustc crawls; a small crate compiled in 5m that takes
  33s un-throttled). Foreground runs are unaffected. Fix for a live tree:
  `taskpolicy -B -p <pid>` on each process (clears the background band); or run heavy
  builds/tests in the foreground. Diagnostic signature: `ps -o stat` shows `SN` and `sample`
  shows only `_dyld_start + 0`.
- **Second `_dyld_start` failure mode (observed 2026-07-16): system-wide exec-validation wedge.**
  Distinct from the QoS throttle: processes hang at `_dyld_start` even at normal QoS (`ps` STAT
  `S`, foreground-launched), `taskpolicy -B`/`renice` do nothing, and even a freshly-compiled
  hello-world hangs on exec — while previously-executed binaries run instantly. Root cause
  signature: `syspolicyd` spinning at ~100% CPU with a huge accumulated TIME (hours), often with
  a freshly-respawned `amfid` (recent PID = it crashed). Every *new* binary's code-signature
  validation queues behind the wedged daemon forever. Check:
  `ps -Ao pid,stat,pcpu,time,command | grep -E "syspolicyd|amfid"`. Observed behavior
  (2026-07-16): the wedge sometimes self-clears in ~10-15 min as the queue drains, but a
  parallel nextest `--list` storm (30+ big test binaries at once) re-crashes amfid and re-wedges
  it; `rustc` also queues (it dlopens freshly built proc-macro dylibs). Serial exec of one
  binary at a time works but is slow (~2 min/binary validation tax). Real fix requires the
  user: `sudo pkill syspolicyd` (launchd respawns it) or a reboot. Don't burn time on
  taskpolicy/renice/codesign once this signature is confirmed; probe with a freshly `cp`'d
  hello-world binary (new inode = fresh validation) to detect recovery.
  Additional findings (2026-07-17):
  - Probe correctly: the probe must be a freshly **compiled** binary (`clang hello.c`). A `cp` of
    a platform binary (`/bin/echo`) is SIGKILLed in ms by the trust cache — false "still wedged".
  - Orphaned requests: any process (test binary or `rustc`) whose validation request was in-flight
    when `syspolicyd` died hangs at `_dyld_start`/0% CPU **forever**, even after a healthy daemon
    respawns. Kill those processes and relaunch; incremental compile resumes cheaply.
  - Working recovery recipe: kill the exec storm → `sudo pkill syspolicyd` → serially pre-validate
    each test binary (`<bin> --list`, one at a time; enumerate them without exec via
    `cargo test --no-run --message-format json | jq -r 'select(.profile.test==true) | .executable'`)
    → then run nextest. Note nextest may still rebuild a few crates (feature unification) and
    re-storm; per-crate `just test <crate>` runs are small enough to absorb.
  - Symptom in results: wedge-frozen trivial unit tests die at exactly the 15s nextest cap
    (`TIMEOUT [15.00Ns]`) — environmental, rerun the affected crates, don't debug the tests.
  - Permanent machine-level fix (user action): add the terminal to System Settings → Privacy &
    Security → Developer Tools (Gatekeeper exemption for processes it launches).
- Run any command expected to take >2 minutes in the background with **raw output redirected to a
  log file** (`cmd > /path/to/log 2>&1`). Never pipe a long run through `grep`/`sort`/`tail` —
  filters buffer output and hide all progress.
- After launching a long background command, **verify liveness within ~3 minutes** and re-check
  every few minutes: the log file must be growing (`wc -c`), or its processes must accumulate CPU
  time (`ps -Ao pid,pcpu,etime,command`). A static log plus 0% CPU for 2+ minutes means stuck —
  diagnose with `sample <pid>` (macOS), then kill and rerun (usually unsandboxed).
- Do not passively wait more than ~5 minutes on a long run without performing a liveness check.

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

### Issue tracker

Issues + PRDs live as markdown under `.scratch/<feature>/`. See `docs/agents/issue-tracker.md`.

### Triage labels

Five canonical roles, default strings (`needs-triage`, `needs-info`, `ready-for-agent`,
`ready-for-human`, `wontfix`). See `docs/agents/triage-labels.md`.

### Domain docs

Multi-context: `CONTEXT-MAP.md` at root points to a per-context `CONTEXT.md`
(server / operator / cli). See `docs/agents/domain.md`.
