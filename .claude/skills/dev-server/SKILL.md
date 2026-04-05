---
name: dev-server
description: >
  Manage the FrogDB development server lifecycle: start, restart, and stop.
  Start the server with continuous low-volume background traffic for testing
  metrics, debug pages, logs, and observability features. Automatically kills
  any existing dev server before starting a new one (one instance per worktree).
  Use when the user asks to start/stop/restart the dev server, run the server
  for development, test the debug UI, or when you've edited server code and
  need to restart to pick up changes. Also use after editing HTML/CSS/JS assets
  under frogdb-server/crates/debug/assets/ since rust-embed bakes them at
  compile time.
---

# FrogDB Dev Server

Manages a FrogDB server with continuous low-volume memtier traffic for development.
Uses random ports by default to avoid collisions across worktrees.

## Quick Reference

| Action | Command |
|--------|---------|
| Start (random port) | `just dev` |
| Start specific workload | `just dev read-heavy` |
| Start with fixed port | `just dev mixed 500 --port 6379` |
| Start release build | `just dev mixed 500 --release` |
| Check status | `cat .dev-server.json` |
| Kill | `kill $(cat .dev-server.json \| python3 -c "import sys,json; print(json.load(sys.stdin)['pid'])")` |

## Starting the Dev Server

1. **Kill any existing instance first** — the script handles this automatically via `.dev-server.json`,
   but if the state file is stale, manually check:
   ```bash
   cat .dev-server.json 2>/dev/null  # check for existing state
   ```

2. **Start in background** using Bash `run_in_background`:
   ```bash
   just dev
   ```
   This builds the server, starts it on a random port, waits for readiness, then runs
   memtier at ~500 ops/sec. The script automatically kills any previous dev server.

3. **Wait for the state file** (appears once the server is ready):
   ```bash
   cat .dev-server.json
   ```
   Contains `{"pid": <pid>, "port": <port>, "http_port": <http_port>}`.

4. **Print the URLs** using the ports from the state file:
   - Debug UI: `http://127.0.0.1:<http_port>/debug`
   - Metrics: `http://127.0.0.1:<http_port>/metrics`
   - Status: `http://127.0.0.1:<http_port>/status/json`

## When to Restart

Restart the dev server when:

- **User asks:** "restart dev server", "rebuild", "reload"
- **Rust code changed under `frogdb-server/crates/`** — any `.rs` file change requires a rebuild
- **Assets changed under `frogdb-server/crates/debug/assets/`** — `rust-embed` bakes HTML/CSS/JS
  into the binary at compile time, so asset changes also require a rebuild
- **Build failure** — kill the server, fix the issue, then restart

Do NOT restart for changes to:
- `testing/`, `docs/`, `.github/` — no server binary impact
- `justfile` load test recipes — server binary unchanged
- Python scripts — no server binary impact

### Restart Procedure

Just run `just dev` again in background — the script automatically kills the previous instance
before starting the new one.

## Stopping the Dev Server

Read the state file and kill the process:

```bash
# Read the PID from state file
cat .dev-server.json
# Kill by PID (the script cleans up the state file on normal exit)
kill <pid>
```

If the state file is missing or stale, find the process by port:

```bash
lsof -i :<port> -t | xargs kill
```

## State File

`.dev-server.json` at the repo root tracks the running instance:

```json
{
  "pid": 12345,
  "port": 52341,
  "http_port": 52342
}
```

- Written once the server is ready and accepting connections
- Cleaned up automatically on normal exit (Ctrl-C, SIGTERM)
- May be stale if the process was killed with SIGKILL — always verify the PID is alive
- Gitignored — each worktree has its own state
