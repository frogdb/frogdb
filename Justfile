# Default recipe - show available commands
default:
    @just --list

# Build debug
build:
    cargo build

# Build release
release:
    cargo build --release

# Run all tests
test:
    cargo test --all

# Run tests for a specific crate
test-crate crate:
    cargo test -p {{crate}}

# Run a specific test
test-one name:
    cargo test {{name}} -- --nocapture

# Run property-based tests (proptest)
proptest:
    cargo test proptest --all

# Run concurrency tests (requires shuttle feature)
concurrency:
    cargo test -p frogdb-core --features shuttle --test concurrency

# Run all benchmarks
bench:
    cargo bench -p frogdb-benches

# Format code
fmt:
    cargo fmt --all

# Check formatting (CI)
fmt-check:
    cargo fmt --all -- --check

# Run clippy lints
lint:
    cargo clippy --all-targets --all-features -- -D warnings

# Run cargo-deny (license/security audit)
deny:
    cargo deny check

# Run all checks (CI)
check: fmt-check lint deny test

# Run the server (debug)
run *args:
    cargo run -p frogdb-server -- {{args}}

# Run the server (release)
run-release *args:
    cargo run --release -p frogdb-server -- {{args}}

# Show size of target directory
target-size:
    @echo "Target directory size:"
    @du -sh target 2>/dev/null || echo "No target directory found"
    @echo "\nBreakdown by subdirectory:"
    @du -sh target/*/ 2>/dev/null || echo "No subdirectories found"

# Clean build artifacts
clean:
    cargo clean

# Watch for changes and run tests (requires cargo-watch)
watch:
    cargo watch -x 'test --all'

# Generate documentation
doc:
    cargo doc --all --no-deps --open

# =============================================================================
# Redis Compatibility Testing
# =============================================================================

# Run Redis compatibility tests
redis-compat *args:
    uv run compat/redis-compat/run_tests.py {{args}}

# Run specific Redis test unit
redis-compat-unit unit *args:
    uv run compat/redis-compat/run_tests.py --single unit/{{unit}} {{args}}

# Clean Redis test cache
redis-compat-clean:
    rm -rf .redis-tests/

# Show Redis compatibility coverage
redis-compat-coverage:
    uv run compat/redis-compat/coverage.py
