# FrogDB Docker Image
#
# Expects pre-built cross-compiled binary from:
#   cross build --release --target x86_64-unknown-linux-gnu --bin frogdb-server
# Or use: just cross-build

FROM --platform=linux/amd64 debian:bookworm-slim

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

# Copy pre-built cross-compiled binary
COPY target/x86_64-unknown-linux-gnu/release/frogdb-server /usr/local/bin/frogdb-server
RUN chmod +x /usr/local/bin/frogdb-server

# Environment variables for configuration (use __ for nested fields)
ENV FROGDB_SERVER__BIND=0.0.0.0
ENV FROGDB_SERVER__PORT=6379
ENV FROGDB_PERSISTENCE__DATA_DIR=/data
ENV FROGDB_LOGGING__LEVEL=info

# Expose the default port
EXPOSE 6379

# Volume for persistent data
VOLUME /data

# Health check using redis-cli
HEALTHCHECK --interval=5s --timeout=3s --start-period=5s --retries=3 \
    CMD redis-cli -p ${FROGDB_SERVER__PORT} PING || exit 1

# Run as root by default (needed for iptables in Jepsen tests)
# In production, override with --user frogdb
USER root

# Default command
CMD ["frogdb-server"]
