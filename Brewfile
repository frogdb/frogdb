# FrogDB system dependencies (macOS)
#
# Language runtimes and dev CLI tools live in .mise.toml — run `mise install` after brew bundle.
# This file handles system libraries and specialized packages that mise cannot manage.
#
# For Linux/Nix, use shell.nix instead: nix-shell

# mise — toolchain manager (installs rust, python, node, just, uv, bun, cargo plugins, ...)
brew "mise"

# Lefthook — git hooks manager (installed here so `lefthook install` works before mise has run)
brew "lefthook"

# Build dependencies
brew "llvm"              # Provides libclang for bindgen (used by librocksdb-sys)

# Justfile LSP for IDE support (no mise plugin)
brew "terror/tap/just-lsp"

# Redis compatibility testing
brew "redis"             # Redis CLI/server for manual testing and Redis compat suite
brew "tcl-tk@8"          # Redis 7.x test suite (Tcl 8.x; Redis 7.x tests are incompatible with Tcl 9)

# Jepsen testing (distributed consistency tests)
brew "leiningen"         # Clojure build tool (no mise plugin; also pulls in a JDK)
brew "gnuplot"           # Required by Jepsen for performance graphs

# Browser testing
cask "chromedriver"      # WebDriver for browser integration tests

# Optional: System RocksDB for faster builds (set FROGDB_SYSTEM_ROCKSDB=1)
brew "rocksdb"
