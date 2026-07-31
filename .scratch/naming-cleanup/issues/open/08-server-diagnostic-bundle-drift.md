# Server crates still say "diagnostic bundle"

Status: ready-for-agent

Issue 07 aligned frogdb-admin and frogctl to the canonical **Debug Bundle**, but
`frogdb-server` proper still says "diagnostic bundle(s)" in:

- `crates/core/src/conn_command.rs` (doc comments)
- `crates/config/src/debug_bundle.rs` (doc comments)
- `crates/server/src/connection/debug_conn_command.rs` (incl. `DEBUG BUNDLE` help line)
- `crates/server/src/connection/handlers/debug.rs`
- `crates/debug/src/bundle/*.rs`
- `crates/debug/src/web_ui/handlers.rs` (incl. `<h3>Diagnostic Bundles</h3>` web-UI heading)

Sweep all to "debug bundle" / "Debug Bundles". Wording only — no API paths or config keys
change (they already say `debug-bundle`/`bundle`).
