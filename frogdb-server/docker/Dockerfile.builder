# FrogDB In-Docker Build
#
# Compiles FrogDB entirely inside Docker using Alpine's system RocksDB package,
# avoiding the expensive ~200-file C++ compilation of librocksdb-sys.
#
# Rust (and cargo plugins) are installed via mise from .mise.toml so the Docker
# build uses the exact same Rust version as local development and CI. The base
# image is raw alpine:3.21 (not rust:alpine) so there is no pre-baked toolchain
# to get overridden by rust-toolchain.toml.
#
# Uses cargo-chef for dependency caching: code-only changes skip dep compilation.
#
# Build context must be the repo root (Cargo.toml + Cargo.lock + .mise.toml live there).
#
# Usage:
#   docker build -f frogdb-server/docker/Dockerfile.builder -t frogdb:latest .
#   docker build -f frogdb-server/docker/Dockerfile.builder --build-arg BUILD_TARGET=debug -t frogdb:latest .

ARG BUILD_TARGET=prod

# ---------------------------------------------------------------------------
# Stage 0: Shared base — alpine + mise + pinned Rust from .mise.toml
# ---------------------------------------------------------------------------
FROM alpine:3.23 AS mise-base

# Base build tools needed to compile cargo-chef and cargo build scripts
RUN apk add --no-cache \
    ca-certificates curl bash git \
    build-base musl-dev

# Install mise into a system location so it survives USER switches and works
# from any WORKDIR. MISE_INSTALL_PATH controls where the mise binary lands.
ENV MISE_INSTALL_PATH=/usr/local/bin/mise
RUN curl -fsSL https://mise.run | sh

# Put mise shims on PATH so `cargo`, `rustc`, etc. resolve to the pinned toolchain
ENV PATH="/root/.local/share/mise/shims:${PATH}"

WORKDIR /app

# Copy tool manifests FIRST so the mise install layer is cached as long as
# .mise.toml and rust-toolchain.toml are unchanged. This is the Docker
# equivalent of the Cargo dependency-caching pattern.
#
# We install ONLY Rust here, not every tool in .mise.toml. The Docker build
# only needs cargo; the other tools in .mise.toml (python, node, java, bun,
# cargo plugins, ubi binaries, ...) target a glibc Linux dev environment and
# many either fail to build on Alpine musl (python/node build from source),
# don't publish aarch64-musl artifacts (samply/ubi), or aren't needed for the
# release build at all. Installing `rust` alone pulls cargo + the exact
# toolchain version in rust-toolchain.toml, which is all this stage requires.
COPY .mise.toml rust-toolchain.toml ./
RUN mise trust .mise.toml && mise install rust

# ---------------------------------------------------------------------------
# Stage 1: Prepare dependency recipe (cargo-chef)
# ---------------------------------------------------------------------------
FROM mise-base AS planner

RUN --mount=type=cache,target=/root/.cargo/registry \
    --mount=type=cache,target=/root/.cargo/git \
    cargo install cargo-chef --locked

COPY Cargo.toml Cargo.lock ./
COPY frogdb-server/ frogdb-server/
COPY frogctl/ frogctl/
RUN cargo chef prepare --recipe-path recipe.json

# ---------------------------------------------------------------------------
# Stage 2: Build
# ---------------------------------------------------------------------------
FROM mise-base AS builder

# System RocksDB + compression libs + jemalloc build deps
RUN apk add --no-cache \
    rocksdb-dev \
    snappy-dev \
    lz4-dev \
    zstd-dev \
    clang-dev \
    openssl-dev \
    pkgconf \
    make \
    mold

RUN --mount=type=cache,target=/root/.cargo/registry \
    --mount=type=cache,target=/root/.cargo/git \
    cargo install cargo-chef --locked

# Use clang toolchain (clang-dev is installed above; cc-rs defaults to "c++"/GCC which isn't present)
ENV CC=clang
ENV CXX=clang++
# Tell librocksdb-sys, snappy-sys, and zstd-sys to use system libraries
ENV ROCKSDB_LIB_DIR=/usr/lib
ENV SNAPPY_LIB_DIR=/usr/lib
ENV ZSTD_SYS_USE_PKG_CONFIG=1
# tikv-jemalloc-sys builds jemalloc from source (requires _rjem_ symbol prefix
# that system jemalloc doesn't provide). This is fast (~10s) unlike RocksDB.
# Dynamic musl linking (-crt-static) so build scripts can dlopen libclang for bindgen.
# Mold linker for faster linking.
ENV RUSTFLAGS="-C target-feature=-crt-static -C link-arg=-fuse-ld=mold"

# Cache dependencies (only invalidated by Cargo.toml/Cargo.lock changes)
COPY --from=planner /app/recipe.json recipe.json
RUN --mount=type=cache,target=/root/.cargo/registry \
    --mount=type=cache,target=/root/.cargo/git \
    --mount=type=cache,target=/app/target \
    cargo chef cook --profile docker --recipe-path recipe.json

# Build the actual project, verify linkage, and copy binary out of cache mount
COPY Cargo.toml Cargo.lock ./
COPY frogdb-server/ frogdb-server/
COPY frogctl/ frogctl/
RUN --mount=type=cache,target=/root/.cargo/registry \
    --mount=type=cache,target=/root/.cargo/git \
    --mount=type=cache,target=/app/target \
    cargo build --profile docker --bin frogdb-server --bin frogctl --bin frogdb-admin && \
    cp target/docker/frogdb-server /usr/local/bin/frogdb-server && \
    cp target/docker/frogctl /usr/local/bin/frogctl && \
    cp target/docker/frogdb-admin /usr/local/bin/frogdb-admin && \
    grep -q 'cargo:rustc-link-lib=dylib=rocksdb' target/docker/build/librocksdb-sys-*/output && \
    grep -q 'cargo:rustc-link-lib=dylib=snappy' target/docker/build/librocksdb-sys-*/output && \
    echo "Verified: RocksDB and Snappy linked from system libraries" && \
    (nm /usr/local/bin/frogdb-server 2>/dev/null | grep -q '_rjem_malloc' && \
    echo "Verified: jemalloc statically linked (tikv-jemalloc-sys)" || \
    echo "Warning: jemalloc symbols not found")

# ---------------------------------------------------------------------------
# Stage 3: Runtime image variants
# ---------------------------------------------------------------------------
# Production: minimal runtime with non-root user
FROM alpine:3.23 AS runtime-prod
RUN apk add --no-cache \
    rocksdb snappy lz4-libs zstd-libs libssl3 libgcc redis
RUN adduser -D -H frogdb
RUN mkdir -p /data && chown frogdb:frogdb /data
USER frogdb

# Debug: includes Jepsen/benchmarking tools, runs as root
FROM alpine:3.23 AS runtime-debug
RUN apk add --no-cache \
    rocksdb snappy lz4-libs zstd-libs libssl3 libgcc redis \
    iptables iproute2 procps bash strace
RUN mkdir -p /data
USER root

# Select variant based on BUILD_TARGET arg
FROM runtime-${BUILD_TARGET} AS runtime

# Copy built binaries
COPY --from=builder /usr/local/bin/frogdb-server /usr/local/bin/frogdb-server
COPY --from=builder /usr/local/bin/frogctl /usr/local/bin/frogctl
COPY --from=builder /usr/local/bin/frogdb-admin /usr/local/bin/frogdb-admin

# Environment variables for configuration (use __ for nested fields)
# TODO: should this be 127.0.0.1 for security by default?
ENV FROGDB_SERVER__BIND=0.0.0.0
ENV FROGDB_SERVER__PORT=6379
ENV FROGDB_PERSISTENCE__DATA_DIR=/data
ENV FROGDB_LOGGING__LEVEL=info

EXPOSE 6379

VOLUME /data

HEALTHCHECK --interval=5s --timeout=3s --start-period=5s --retries=3 \
    CMD redis-cli -p ${FROGDB_SERVER__PORT:-6379} PING || exit 1

CMD ["frogdb-server"]
