# FrogDB Docker Image
#
# Multi-stage build for minimal image size:
# 1. Builder stage: Compile FrogDB with all dependencies
# 2. Runtime stage: Copy only the binary and runtime dependencies

# ===========================================================================
# Builder Stage
# ===========================================================================
FROM rust:1.75-bookworm AS builder

# Install build dependencies
RUN apt-get update && apt-get install -y \
    libclang-dev \
    clang \
    cmake \
    libssl-dev \
    pkg-config \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy the entire project
COPY . .

# Build release binary
RUN cargo build --release --bin frogdb

# ===========================================================================
# Runtime Stage
# ===========================================================================
FROM debian:bookworm-slim

# Install runtime dependencies and tools for Jepsen testing
RUN apt-get update && apt-get install -y \
    libssl3 \
    ca-certificates \
    procps \
    iptables \
    iproute2 \
    redis-tools \
    && rm -rf /var/lib/apt/lists/*

# Create non-root user for running FrogDB
RUN useradd -r -s /bin/false frogdb

# Create data directory
RUN mkdir -p /data && chown frogdb:frogdb /data

# Copy the binary from builder
COPY --from=builder /app/target/release/frogdb /usr/local/bin/frogdb

# Set ownership
RUN chmod +x /usr/local/bin/frogdb

# Environment variables for configuration
ENV FROGDB_PORT=6379
ENV FROGDB_DATA_DIR=/data
ENV FROGDB_LOG_LEVEL=info

# Expose the default port
EXPOSE 6379

# Volume for persistent data
VOLUME /data

# Health check using redis-cli
HEALTHCHECK --interval=5s --timeout=3s --start-period=5s --retries=3 \
    CMD redis-cli -p ${FROGDB_PORT} PING || exit 1

# Run as root by default (needed for iptables in Jepsen tests)
# In production, override with --user frogdb
USER root

# Default command
CMD ["frogdb"]
