# FrogDB Development Dependencies (macOS)
# For Linux/WSL, use shell.nix instead: nix-shell
# NOTE: We may move to mise (https://mise.jdx.dev/) for toolchain management in the future.

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

# Build dependencies
brew "llvm"              # Provides libclang for bindgen (used by librocksdb-sys)

# Cross-compilation
brew "zig"               # Cross-compiler toolchain (used by cargo-zigbuild)

# Jepsen testing (distributed consistency tests)
brew "openjdk"           # Required by Leiningen/Clojure
brew "leiningen"         # Clojure build tool for Jepsen

# Optional: System RocksDB for faster builds (see `just build-fast`)
brew "rocksdb"
