# Error text says `frog`, binary is `frogctl`

Status: ready-for-agent

`frogctl/src/commands/debug.rs:434` — `"frog debug vll: not yet implemented"` uses `frog`
instead of `frogctl`. Fix the string; grep for other `"frog "` user-facing strings while
there.
