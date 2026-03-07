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

# ---------------------------------------------------------------------------
# Stage 1: Prepare dependency recipe (cargo-chef)
# ---------------------------------------------------------------------------
FROM rust:alpine3.21 AS planner

RUN apk add --no-cache musl-dev
RUN cargo install cargo-chef --locked

WORKDIR /app
COPY Cargo.toml Cargo.lock ./
COPY frogdb-server/ frogdb-server/
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
    jemalloc-dev \
    clang-dev \
    openssl-dev \
    musl-dev \
    pkgconf

RUN cargo install cargo-chef --locked

# Tell librocksdb-sys, snappy-sys, and zstd-sys to use system libraries
ENV ROCKSDB_LIB_DIR=/usr/lib
ENV SNAPPY_LIB_DIR=/usr/lib
ENV ZSTD_SYS_USE_PKG_CONFIG=1
# Tell tikv-jemalloc-sys to skip C compilation and use system jemalloc
ENV JEMALLOC_OVERRIDE=/usr/lib/libjemalloc.so.2

WORKDIR /app

# Cache dependencies (only invalidated by Cargo.toml/Cargo.lock changes)
COPY --from=planner /app/recipe.json recipe.json
RUN cargo chef cook --release --recipe-path recipe.json

# Build the actual project
COPY Cargo.toml Cargo.lock ./
COPY frogdb-server/ frogdb-server/
RUN cargo build --release --bin frogdb-server

# Verify system libraries were linked (fail the build if not)
RUN grep -q 'cargo:rustc-link-lib=dylib=rocksdb' target/release/build/librocksdb-sys-*/output && \
    grep -q 'cargo:rustc-link-lib=dylib=snappy' target/release/build/librocksdb-sys-*/output && \
    echo "Verified: RocksDB and Snappy linked from system libraries"
RUN ldd target/release/frogdb-server | grep -q 'libjemalloc' && \
    echo "Verified: jemalloc dynamically linked" || \
    echo "Warning: jemalloc not dynamically linked (may be statically linked or unused)"

# ---------------------------------------------------------------------------
# Stage 3: Runtime image variants
# ---------------------------------------------------------------------------
ARG BUILD_TARGET=prod

# Production: minimal runtime with non-root user
FROM alpine:3.21 AS runtime-prod
RUN apk add --no-cache \
    rocksdb snappy lz4-libs zstd-libs jemalloc libssl3 libgcc redis
RUN adduser -D -H frogdb
RUN mkdir -p /data && chown frogdb:frogdb /data
USER frogdb

# Debug: includes Jepsen/benchmarking tools, runs as root
FROM alpine:3.21 AS runtime-debug
RUN apk add --no-cache \
    rocksdb snappy lz4-libs zstd-libs jemalloc libssl3 libgcc redis \
    iptables iproute2 procps bash strace
RUN mkdir -p /data
USER root

# Select variant based on BUILD_TARGET arg
FROM runtime-${BUILD_TARGET} AS runtime

# Copy built binary
COPY --from=builder /app/target/release/frogdb-server /usr/local/bin/frogdb-server

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
