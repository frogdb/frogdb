# Redis Compatibility Test Harness

This directory contains tools for running the official Redis 7.x Tcl test suite against FrogDB to
verify Redis protocol compatibility.

## Prerequisites

### macOS (Homebrew)

```bash
brew bundle  # Installs tcl-tk and uv from Brewfile
```

### Manual Installation

| Dependency | Version | Installation                                       |
| ---------- | ------- | -------------------------------------------------- |
| Tcl        | 8.5+    | `brew install tcl-tk` or system package manager    |
| uv         | Latest  | `curl -LsSf https://astral.sh/uv/install.sh \| sh` |
| Python     | 3.11+   | Managed automatically by uv                        |

## Usage

### Run All Tests

```bash
just redis-compat
```

This will:
1. Download Redis 7.2.4 source to `.redis-tests/` (cached for future runs)
2. Build FrogDB in release mode
3. Start FrogDB on port 6399
4. Run the Redis Tcl test suite in external server mode
5. Stop FrogDB and clean up

### Run Specific Test Unit

```bash
# Run string type tests
just redis-compat --single unit/type/string

# Run hash type tests
just redis-compat --single unit/type/hash

# Run set operations tests
just redis-compat --single unit/type/set
```

### Command Line Options

```bash
# Use a different port
just redis-compat --port 6400

# Verbose output
just redis-compat --verbose

# Skip rebuilding FrogDB (use existing binary)
just redis-compat --skip-build

# Keep test data directory after run
just redis-compat --keep-data

# Run with additional tag filters
just redis-compat --tags slow --tags large-memory
```

### Environment Variables

| Variable      | Default | Description                  |
| ------------- | ------- | ---------------------------- |
| `FROGDB_PORT` | 6399    | Port for FrogDB during tests |

### Clean Test Cache

```bash
just redis-compat-clean
```

This removes the `.redis-tests/` directory, including cached Redis source and test data.

### View Coverage Report

```bash
just redis-compat-coverage
```

## Skiplist Files

Tests are organized into three skiplist files:

### `skiplist-intentional.txt`

Permanent incompatibilities where FrogDB intentionally differs from Redis:

- **SELECT command** - FrogDB uses single database per instance
- **Cross-slot operations** - Multi-key ops require same hash slot
- **CONFIG REWRITE** - Configuration changes are transient
- **BITFIELD u64** - Unsigned integers limited to 63 bits
- **Strict script key validation** - All keys must be declared in KEYS array

See [spec/COMPATIBILITY.md](../../spec/COMPATIBILITY.md) for details.

### `skiplist-not-implemented.txt`

Features not yet implemented in FrogDB:

- **Blocking commands** (Phase 11) - BLPOP, BRPOP, BLMOVE, etc.
- **Streams** (Phase 13) - XADD, XREAD, XREADGROUP, etc.
- **Cluster commands** (Phase 14) - CLUSTER *, READONLY, etc.
- **DEBUG commands** - Not planned
- **MONITOR** - Not planned
- **MEMORY/LATENCY commands** - Deferred

See [spec/ROADMAP.md](../../spec/ROADMAP.md) for implementation timeline.

### `skiplist-flaky.txt`

Tests with timing or ordering issues that fail intermittently. Initially empty; populated as flaky
tests are discovered during testing.

## Adding Expected Failures

When a test fails due to a known incompatibility or unimplemented feature:

1. Identify the test file (e.g., `unit/type/string`)
2. Add it to the appropriate skiplist:
   - `skiplist-intentional.txt` for permanent differences
   - `skiplist-not-implemented.txt` for planned features
   - `skiplist-flaky.txt` for intermittent failures
3. Add a comment explaining why it's skipped

Example:
```
# unit/type/newfeature - NEWCMD not implemented (Phase 15)
unit/type/newfeature
```

## Test Categories

### Expected to Pass

- String operations (`unit/type/string`)
- List operations (`unit/type/list`)
- Set operations (`unit/type/set`)
- Hash operations (`unit/type/hash`)
- Sorted set basic operations (`unit/type/zset`)
- SCAN family commands
- EXPIRE/TTL/PERSIST
- Key operations (DEL, EXISTS, RENAME, etc.)

### Expected to Skip (Intentional)

- SELECT command tests
- CONFIG REWRITE tests
- Cross-slot multi-key tests
- BITFIELD u64 tests
- Script tests using undeclared keys

### Expected to Skip (Not Implemented)

- Blocking command tests (BLPOP, BRPOP, etc.)
- Stream tests (XADD, XREAD, etc.)
- Cluster command tests
- DEBUG command tests
- MONITOR tests

## Troubleshooting

### Port Already in Use

```bash
# Use a different port
FROGDB_PORT=6400 just redis-compat
```

### Tcl Not Found

Ensure Tcl is installed and `tclsh` is in your PATH:

```bash
which tclsh  # Should return a path
tclsh <<< 'puts $tcl_version'  # Should print 8.5+
```

### Tests Hang

If tests hang, FrogDB may not be starting correctly. Try:

```bash
# Build and test manually
cargo build --release -p frogdb-server
./target/release/frogdb-server --port 6399

# In another terminal, verify connectivity
redis-cli -p 6399 PING
```

### Viewing Test Output

For detailed test output:

```bash
just redis-compat --verbose
```

## Architecture

```
compat/redis-compat/
├── run_tests.py              # Main test runner (UV script)
├── coverage.py               # Coverage analysis tool
├── skiplist-intentional.txt  # Permanent incompatibilities
├── skiplist-not-implemented.txt  # Not yet implemented
├── skiplist-flaky.txt        # Flaky tests
└── README.md                 # This file

.redis-tests/                 # Cache directory (gitignored)
├── redis-7.2.4/              # Downloaded Redis source
├── combined_skiplist.txt     # Merged skiplists
└── frogdb-test-data/         # FrogDB data during tests
```

## References

- [Redis Test Suite Documentation](https://github.com/redis/redis/tree/7.2/tests)
- [FrogDB Compatibility Spec](../../spec/COMPATIBILITY.md)
- [FrogDB Roadmap](../../spec/ROADMAP.md)
