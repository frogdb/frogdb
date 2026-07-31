# Align "diagnostic bundle" wording to canonical "Debug Bundle"

Status: done

Canonical term: **Debug Bundle** (matches server config `[debug-bundle]`). Drift:

- `frogdb-server/ops/frogdb-admin/src/main.rs` — subcommand/help text says "diagnostic bundle"
  (`DiagnosticBundle` subcommand naming)
- frogctl `debug zip` — help text should say it produces a debug bundle

Rename CLI-facing wording (and the `DiagnosticBundle` enum variant if cheap); keep any HTTP
API paths as-is unless they also say "diagnostic".

## Comments

In `frogdb-server/ops/frogdb-admin/src/main.rs`: renamed the `Commands::DiagnosticBundle`
enum variant to `DebugBundle` (its doc comment, which clap uses as the subcommand help text,
now reads "Debug bundle operations."). Since the variant has no explicit `#[command(name =
...)]` override, clap's default kebab-case derivation changes the subcommand string
automatically from `diagnostic-bundle` to `debug-bundle` — no separate rename was needed.
Updated the `Generate` doc comment and the two `eprintln!` progress messages
("Generating diagnostic bundle..." → "Generating debug bundle..."). Also updated the
`BundleClient` doc comment in `client.rs` ("HTTP client for the FrogDB diagnostic bundle API."
→ "... debug bundle API.") for consistency. The HTTP API paths in `client.rs`
(`/debug/api/bundle/...`) already said "bundle", not "diagnostic" — left untouched, matching
the issue's instruction.

In `frogctl/src/commands/debug.rs`: `debug zip`'s doc comment (clap help text) changed from
"Collect a diagnostic bundle into a ZIP archive" to "Collect a debug bundle into a ZIP
archive".

Follow-up noticed but out of this issue's stated scope: `frogdb-server` proper (outside
`ops/frogdb-admin`) still has several "diagnostic bundle(s)" strings — doc comments in
`crates/core/src/conn_command.rs`, `crates/config/src/debug_bundle.rs`,
`crates/server/src/connection/debug_conn_command.rs` (including a `DEBUG BUNDLE` command help
line), `crates/server/src/connection/handlers/debug.rs`, and several spots in
`crates/debug/src/bundle/*.rs` and `crates/debug/src/web_ui/handlers.rs` (including an
`<h3>Diagnostic Bundles</h3>` heading in the web UI). `frogdb-server/CONTEXT.md` already
documents **Debug Bundle** as canonical with `_Avoid_: diagnostic bundle`, so these are
pre-existing drift against that established term — flagging for a follow-up issue rather than
fixing here, since this issue explicitly scoped the fix to `ops/frogdb-admin` and `frogctl`.

Verified with `RUSTC_WRAPPER="" just check frogdb-admin` and
`RUSTC_WRAPPER="" just check frogctl` (both clean).
