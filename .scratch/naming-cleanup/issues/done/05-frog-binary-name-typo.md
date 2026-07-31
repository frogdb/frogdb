# Error text says `frog`, binary is `frogctl`

Status: done

`frogctl/src/commands/debug.rs:434` — `"frog debug vll: not yet implemented"` uses `frog`
instead of `frogctl`. Fix the string; grep for other `"frog "` user-facing strings while
there.

## Comments

Fixed the `debug.rs:434` string plus every other whole-word `frog` (binary-name) occurrence
found via `grep -rn '\bfrog\b' frogctl/src/` — 39 occurrences across
`frogctl/src/commands/{debug,upgrade,backup,config,data,replication,cluster}.rs` and
`frogctl/src/ops/config.rs` (an `anyhow::bail!("frog ... not yet implemented")` pattern
repeated across most command modules, plus a generated-config comment and a few `println!`
hints in `upgrade.rs`). All were mechanically renamed `frog` → `frogctl` with
`perl -pi -E 's/\bfrog\b/frogctl/g'` (BSD `sed -E` does not support `\b`, so `sed` alone
silently no-ops on this pattern — verified before relying on it). Confirmed no double-substitution
(`frogctlctl`) resulted. Left `FrogDB`/`frogdb`/`frogctl` occurrences (already correct) and the
generic English words "diagnostics"/"diagnostic" untouched, since `\bfrog\b` only matches the
standalone binary-name typo, not substrings.

Verified with `RUSTC_WRAPPER="" just check frogctl` (clean).
