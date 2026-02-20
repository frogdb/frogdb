# FrogDB - CLAUDE.md

## Verification Before Completing Code Changes

**IMPORTANT:** Before marking any code change as complete, you MUST run all of the following commands and confirm they pass. Do NOT skip any of these steps.

```bash
# 1. Build the entire workspace
cargo build --all

# 2. Check formatting
cargo fmt --all -- --check

# 3. Run clippy lints (must pass with no warnings)
cargo clippy --all-targets --all-features -- -D warnings

# 4. Run all tests
cargo test --all
```

If any command fails, fix the issue and re-run all commands before considering the task done.
