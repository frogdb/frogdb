# FrogDB In-Docker Build
#
# Compiles FrogDB entirely inside Docker using Alpine's system RocksDB package,
# avoiding the expensive ~200-file C++ compilation of librocksdb-sys.
#
# Uses cargo-chef for dependency caching: code-only changes skip dep compilation.
#
# Usage:
#   docker build -f Dockerfile.builder -t frogdb:latest .
#   just docker-build-full

# ---------------------------------------------------------------------------
# Stage 1: Prepare dependency recipe (cargo-chef)
# ---------------------------------------------------------------------------
FROM rust:alpine3.21 AS planner

RUN apk add --no-cache musl-dev
RUN cargo install cargo-chef --locked

WORKDIR /app
COPY . .
RUN cargo chef prepare --recipe-path recipe.json

# ---------------------------------------------------------------------------
# Stage 2: Build
# ---------------------------------------------------------------------------
FROM rust:alpine3.21 AS builder

# System RocksDB + compression libs + build tools
RUN apk add --no-cache \
    rocksdb-dev \
    snappy-dev \
    lz4-dev \
    zstd-dev \
    clang-dev \
    openssl-dev \
    musl-dev \
    pkgconf

RUN cargo install cargo-chef --locked

# Tell librocksdb-sys and snappy-sys to use system libraries
ENV ROCKSDB_LIB_DIR=/usr/lib
ENV SNAPPY_LIB_DIR=/usr/lib

WORKDIR /app

# Cache dependencies (only invalidated by Cargo.toml/Cargo.lock changes)
COPY --from=planner /app/recipe.json recipe.json
RUN cargo chef cook --release --recipe-path recipe.json

# Build the actual project
COPY . .
RUN cargo build --release --bin frogdb-server

# ---------------------------------------------------------------------------
# Stage 3: Minimal runtime image
# ---------------------------------------------------------------------------
FROM alpine:3.21

RUN apk add --no-cache \
    rocksdb \
    snappy \
    lz4-libs \
    zstd-libs \
    libssl3 \
    libgcc \
    redis

# Create non-root user
RUN adduser -D -H frogdb

# Create data directory
RUN mkdir -p /data && chown frogdb:frogdb /data

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

USER frogdb

CMD ["frogdb-server"]
