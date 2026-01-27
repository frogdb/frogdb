# FrogDB Development Dependencies (macOS)
# For Linux/WSL, use shell.nix instead: nix-shell

# Rust toolchain (via rustup for version management)
brew "rustup"

# Command runner (like make, but simpler)
brew "just"

# Optional: Redis CLI for manual testing
brew "redis"

# Redis compatibility testing
brew "tcl-tk"  # Required for running Redis test suite
brew "uv"      # Python script runner for test harness

# Profiling tools
brew "cargo-flamegraph"  # CPU flamegraphs
brew "samply"            # CPU profiler with UI

# Browser testing
cask "chromedriver"      # WebDriver for browser integration tests

# Cross-compilation
brew "zig"               # Cross-compiler toolchain (used by cargo-zigbuild)
