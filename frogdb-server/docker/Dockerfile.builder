# FrogDB In-Docker Build
#
# Compiles FrogDB entirely inside Docker using Alpine's system RocksDB package,
# avoiding the expensive ~200-file C++ compilation of librocksdb-sys.
#
# Uses cargo-chef for dependency caching: code-only changes skip dep compilation.
#
# Build context must be the repo root (Cargo.toml + Cargo.lock live there).
#
# Usage:
#   docker build -f frogdb-server/docker/Dockerfile.builder -t frogdb:latest .
#   docker build -f frogdb-server/docker/Dockerfile.builder --build-arg BUILD_TARGET=debug -t frogdb:latest .

ARG BUILD_TARGET=prod

# ---------------------------------------------------------------------------
# Stage 1: Prepare dependency recipe (cargo-chef)
# ---------------------------------------------------------------------------
FROM rust:alpine3.21 AS planner

RUN apk add --no-cache musl-dev
RUN cargo install cargo-chef --locked

WORKDIR /app
COPY Cargo.toml Cargo.lock ./
COPY frogdb-server/ frogdb-server/
COPY frog-cli/ frog-cli/
RUN cargo chef prepare --recipe-path recipe.json

# ---------------------------------------------------------------------------
# Stage 2: Build
# ---------------------------------------------------------------------------
FROM rust:alpine3.21 AS builder

# System RocksDB + compression libs + jemalloc + build tools
RUN apk add --no-cache \
    rocksdb-dev \
    snappy-dev \
    lz4-dev \
    zstd-dev \
    clang-dev \
    openssl-dev \
    musl-dev \
    pkgconf \
    make \
    mold

RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/usr/local/cargo/git \
    cargo install cargo-chef --locked

# Tell librocksdb-sys, snappy-sys, and zstd-sys to use system libraries
ENV ROCKSDB_LIB_DIR=/usr/lib
ENV SNAPPY_LIB_DIR=/usr/lib
ENV ZSTD_SYS_USE_PKG_CONFIG=1
# tikv-jemalloc-sys builds jemalloc from source (requires _rjem_ symbol prefix
# that system jemalloc doesn't provide). This is fast (~10s) unlike RocksDB.
# Dynamic musl linking (-crt-static) so build scripts can dlopen libclang for bindgen.
# Mold linker for faster linking.
ENV RUSTFLAGS="-C target-feature=-crt-static -C link-arg=-fuse-ld=mold"

WORKDIR /app

# Cache dependencies (only invalidated by Cargo.toml/Cargo.lock changes)
COPY --from=planner /app/recipe.json recipe.json
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/usr/local/cargo/git \
    --mount=type=cache,target=/app/target \
    cargo chef cook --profile docker --recipe-path recipe.json

# Build the actual project, verify linkage, and copy binary out of cache mount
COPY Cargo.toml Cargo.lock ./
COPY frogdb-server/ frogdb-server/
COPY frog-cli/ frog-cli/
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/usr/local/cargo/git \
    --mount=type=cache,target=/app/target \
    cargo build --profile docker --bin frogdb-server --bin frog --bin frogdb-admin && \
    cp target/docker/frogdb-server /usr/local/bin/frogdb-server && \
    cp target/docker/frog /usr/local/bin/frog && \
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
FROM alpine:3.21 AS runtime-prod
RUN apk add --no-cache \
    rocksdb snappy lz4-libs zstd-libs libssl3 libgcc redis
RUN adduser -D -H frogdb
RUN mkdir -p /data && chown frogdb:frogdb /data
USER frogdb

# Debug: includes Jepsen/benchmarking tools, runs as root
FROM alpine:3.21 AS runtime-debug
RUN apk add --no-cache \
    rocksdb snappy lz4-libs zstd-libs libssl3 libgcc redis \
    iptables iproute2 procps bash strace
RUN mkdir -p /data
USER root

# Select variant based on BUILD_TARGET arg
FROM runtime-${BUILD_TARGET} AS runtime

# Copy built binaries
COPY --from=builder /usr/local/bin/frogdb-server /usr/local/bin/frogdb-server
COPY --from=builder /usr/local/bin/frog /usr/local/bin/frog
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
