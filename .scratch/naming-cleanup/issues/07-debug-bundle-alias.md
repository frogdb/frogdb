# Align "diagnostic bundle" wording to canonical "Debug Bundle"

Status: ready-for-agent

Canonical term: **Debug Bundle** (matches server config `[debug-bundle]`). Drift:

- `frogdb-server/ops/frogdb-admin/src/main.rs` — subcommand/help text says "diagnostic bundle"
  (`DiagnosticBundle` subcommand naming)
- frogctl `debug zip` — help text should say it produces a debug bundle

Rename CLI-facing wording (and the `DiagnosticBundle` enum variant if cheap); keep any HTTP
API paths as-is unless they also say "diagnostic".
