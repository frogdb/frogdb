# FrogDB Development Dependencies (macOS)
# For Linux/WSL, use shell.nix instead: nix-shell
# NOTE: We may move to mise (https://mise.jdx.dev/) for toolchain management in the future.

# Rust toolchain (via rustup for version management)
brew "rustup"

# Command runner (like make, but simpler)
brew "just"

# Language server for Justfiles (IDE support)
brew "terror/tap/just-lsp"

# Optional: Redis CLI for manual testing
brew "redis"

# Redis compatibility testing
brew "tcl-tk@8"  # Required for running Redis test suite (Tcl 8.x; Redis 7.x tests are incompatible with Tcl 9)
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

# CI linting
brew "actionlint"        # Lint GitHub Actions workflow files

# Jepsen testing (distributed consistency tests)
brew "openjdk"           # Required by Leiningen/Clojure
brew "leiningen"         # Clojure build tool for Jepsen
brew "gnuplot"           # Required by Jepsen for performance graphs

# Optional: System RocksDB for faster builds (set FROGDB_SYSTEM_ROCKSDB=1)
brew "rocksdb"
