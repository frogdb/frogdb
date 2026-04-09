# FrogDB

FrogDB is a modern, Redis-compatible in-memory database written in Rust. It supports both standalone
and cluster operating modes. It uses RocksDB for configurable persistence.

## Philosophy

- FrogDB is unreleased, pre-production software. Breaking/sweeping large changes are acceptable
- FrogDB aims to be correct, fast, easy to operate, and scalable
- Inspiration is drawn from high-quality database projects like CockroachDB, ScyllaDB, FoundationDB
- Upmost care should be taken to ensure the correctness of the system. Examples include:
  - Extensive regression tests derived from the official Redis test suite to ensure compatibility
  - Extensive distributed systems and concurrency testing to ensure expected behavior during various
    failure modes like network partitions, disk failures, etc.
  - Fuzz testing for security/stability
- Easy to operate in a modern cloud environment, eg:
  - Grafana/Prometheus/OpenTelemetry/dtrace for observability
  - frogctl cli tool
  - Debug web pages
  - operational debug/profiling tools
  - kubernetes operator

## Main Components

- FrogDB
  - The database binary
- frogctl
  - cli tool for managing the database (ops)
- frogdb-operator
  - a Kubernetes operator for FrogDB
- website for info/documentation/marketing
- the assets/ folder has images for branding
- todo/ contains roadmap and unfinished/follow up items

## Build/Task System

The project uses `just` (see `Justfile`) for performing almost all tasks required in the development
lifecycle. 

The `Justfile` has recipes for tasks like:
- tests
  - unit
  - concurrency
  - web
  - fuzzing
  - jepsen
  - browser
  - load/memtier
  - regression/compatibility
- linting
- type checking
- building
  - cross-compilation
- formatting
- benchmarking
- profiling
- docker
- debug server
- website
- code generation
  - docs/markdown
  - helm
  - grafana
  - debian
- github runner
- cleanup/disk space

Common examples:

```bash
just check frogdb-core                  # type-check a single crate
just test frogdb-server                 # run all tests for a specific crate
just test frogdb-server test_publish    # run tests matching a regex pattern
just lint frogdb-persistence            # clippy on a specific crate
just lint-py                            # ruff check
just fmt frogdb-core                    # format a single crate
just fmt-py                             # format Python code
```

**IMPORTANT**: Check the `Justfile` for a relevant recipe before running commands like `cargo`
directly.

- **BAD**: `cargo test ...`
- **GOOD**: `just test ...`

**IMPORTANT**: Run tests/checks against the specific crate/package/file to save time

## Website/Documentation

FrogDB has a website for documentation using Astro that is published to Github Pages.

**IMPORTANT**: Check for relevant documentation to update when making API/behavior changes

## Code generation

Many markup files (yaml, json) in the repo are generated from Python or Rust scripts.

- Check files for indications that these are generated
- Make changes in the generator code, **not** the generated yaml/json.

Examples:
- github actions
- helm charts
- grafana
- some documentation/markdown

## Web/HTTP/HTML

- **IMPORTANT**: use `bun` for Javascript/Typescript build/test/run/dev/install. **NOT**
  npm/npx/yarn

## Development Guidelines

- Write simple code, avoid unnecessary complexity
- Follow Rust best practices, avoid non-standard approaches
- Architecture choices should focus on making the software easy to change in the future
- Follow idiomatic Rust patterns and use best practices
- When implementing features or making changes, think about what unit + integration + concurrency
  tests make sense to add. Consider edge cases.
- When designing features, research what implementation Redis, Valkey, and DragonflyDB use for the
  feature. This provides critical insight for decision making.
- Look for documentation to update if changing behavior
- Add required developer/build tools to Brewfile or shell.nix