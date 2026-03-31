---
title: "Deployment"
description: "This guide covers building, packaging, and deploying FrogDB in various environments."
sidebar:
  order: 6
---
This guide covers building, packaging, and deploying FrogDB in various environments.

## Build Requirements

### Rust Toolchain

```bash
# Minimum Rust version
rustc --version  # 1.75.0 or later recommended

# Install via rustup
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
```

### System Dependencies

| Platform | Dependencies |
|----------|--------------|
| Ubuntu/Debian | `build-essential pkg-config libclang-dev` |
| RHEL/CentOS | `gcc make pkgconfig clang-devel` |
| macOS | Xcode Command Line Tools |

```bash
# Ubuntu/Debian
sudo apt-get install build-essential pkg-config libclang-dev

# macOS
xcode-select --install
```

---

## Building from Source

### Release Build

```bash
git clone https://github.com/nathanjordan/frogdb.git
cd frogdb
cargo build --release
```

Binary location: `target/release/frogdb-server`

### Build Options

```bash
# Static linking (for portable binaries)
RUSTFLAGS="-C target-feature=+crt-static" cargo build --release --target x86_64-unknown-linux-musl
```

---

## Docker

### Dockerfile

```dockerfile
# Build stage
FROM rust:1.75-bookworm as builder

WORKDIR /usr/src/frogdb
COPY . .

RUN cargo build --release

# Runtime stage
FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /usr/src/frogdb/target/release/frogdb-server /usr/local/bin/

# Create data directory
RUN mkdir -p /data && chown nobody:nogroup /data
VOLUME /data

# Run as non-root
USER nobody

EXPOSE 6379

ENTRYPOINT ["frogdb-server"]
CMD ["--bind", "0.0.0.0", "--data-dir", "/data"]
```

### Build and Run

```bash
# Build image
docker build -t frogdb:latest .

# Run container
docker run -d \
    --name frogdb \
    -p 6379:6379 \
    -v frogdb-data:/data \
    frogdb:latest

# With custom config
docker run -d \
    --name frogdb \
    -p 6379:6379 \
    -v frogdb-data:/data \
    -v $(pwd)/frogdb.toml:/etc/frogdb/frogdb.toml \
    frogdb:latest --config /etc/frogdb/frogdb.toml
```

### Docker Compose

```yaml
# docker-compose.yml
version: '3.8'

services:
  frogdb:
    image: frogdb:latest
    build: .
    ports:
      - "6379:6379"
    volumes:
      - frogdb-data:/data
      - ./frogdb.toml:/etc/frogdb/frogdb.toml:ro
    command: ["--config", "/etc/frogdb/frogdb.toml"]
    restart: unless-stopped
    healthcheck:
      test: ["CMD", "redis-cli", "-p", "6379", "ping"]
      interval: 10s
      timeout: 5s
      retries: 3
    deploy:
      resources:
        limits:
          memory: 4G
        reservations:
          memory: 1G

  prometheus:
    image: prom/prometheus:latest
    ports:
      - "9090:9090"
    volumes:
      - ./prometheus.yml:/etc/prometheus/prometheus.yml:ro
    depends_on:
      - frogdb

volumes:
  frogdb-data:
```

---

## systemd

### Service File

```ini
# /etc/systemd/system/frogdb.service
[Unit]
Description=FrogDB Server
Documentation=https://github.com/nathanjordan/frogdb
After=network.target

[Service]
Type=simple
User=frogdb
Group=frogdb
ExecStart=/usr/local/bin/frogdb-server --config /etc/frogdb/frogdb.toml
ExecReload=/bin/kill -HUP $MAINPID
Restart=on-failure
RestartSec=5

# Security hardening
NoNewPrivileges=yes
ProtectSystem=strict
ProtectHome=yes
PrivateTmp=yes
ReadWritePaths=/var/lib/frogdb

# Resource limits
LimitNOFILE=65535
LimitNPROC=65535
MemoryMax=4G

[Install]
WantedBy=multi-user.target
```

### Installation

```bash
# Create user
sudo useradd --system --home-dir /var/lib/frogdb --shell /bin/false frogdb

# Create directories
sudo mkdir -p /var/lib/frogdb /etc/frogdb
sudo chown frogdb:frogdb /var/lib/frogdb

# Install binary
sudo cp target/release/frogdb-server /usr/local/bin/
sudo chmod 755 /usr/local/bin/frogdb-server

# Install config
sudo cp frogdb.toml /etc/frogdb/
sudo chown root:frogdb /etc/frogdb/frogdb.toml
sudo chmod 640 /etc/frogdb/frogdb.toml

# Install service
sudo cp frogdb.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable frogdb
sudo systemctl start frogdb
```

### Management

```bash
# Status
sudo systemctl status frogdb

# Logs
sudo journalctl -u frogdb -f

# Restart
sudo systemctl restart frogdb

# Reload config (graceful)
sudo systemctl reload frogdb
```

---

## Kubernetes

### Basic Deployment

```yaml
# frogdb-deployment.yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: frogdb
  labels:
    app: frogdb
spec:
  replicas: 1
  selector:
    matchLabels:
      app: frogdb
  template:
    metadata:
      labels:
        app: frogdb
      annotations:
        prometheus.io/scrape: "true"
        prometheus.io/port: "9090"
        prometheus.io/path: "/metrics"
    spec:
      containers:
      - name: frogdb
        image: frogdb:latest
        ports:
        - containerPort: 6379
          name: redis
        - containerPort: 9090
          name: metrics
        resources:
          requests:
            memory: "1Gi"
            cpu: "500m"
          limits:
            memory: "4Gi"
            cpu: "2"
        volumeMounts:
        - name: data
          mountPath: /data
        - name: config
          mountPath: /etc/frogdb
          readOnly: true
        livenessProbe:
          tcpSocket:
            port: 6379
          initialDelaySeconds: 10
          periodSeconds: 10
        readinessProbe:
          exec:
            command: ["redis-cli", "-p", "6379", "ping"]
          initialDelaySeconds: 5
          periodSeconds: 5
      volumes:
      - name: data
        persistentVolumeClaim:
          claimName: frogdb-data
      - name: config
        configMap:
          name: frogdb-config
---
apiVersion: v1
kind: Service
metadata:
  name: frogdb
spec:
  selector:
    app: frogdb
  ports:
  - port: 6379
    targetPort: 6379
    name: redis
  - port: 9090
    targetPort: 9090
    name: metrics
---
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: frogdb-data
spec:
  accessModes:
    - ReadWriteOnce
  resources:
    requests:
      storage: 10Gi
---
apiVersion: v1
kind: ConfigMap
metadata:
  name: frogdb-config
data:
  frogdb.toml: |
    [server]
    bind = "0.0.0.0"
    port = 6379

    [persistence]
    data-dir = "/data"
    durability-mode = "periodic"

    [observability]
    metrics-port = 9090
```

### StatefulSet (for persistence)

For production with persistent storage:

```yaml
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: frogdb
spec:
  serviceName: frogdb
  replicas: 1
  selector:
    matchLabels:
      app: frogdb
  template:
    # ... (same as Deployment)
  volumeClaimTemplates:
  - metadata:
      name: data
    spec:
      accessModes: ["ReadWriteOnce"]
      storageClassName: "fast-ssd"
      resources:
        requests:
          storage: 50Gi
```

---

## Production Configuration Example

```toml
# /etc/frogdb/frogdb.toml

[server]
bind = "0.0.0.0"
port = 6379
num-shards = 0  # Auto-detect CPU count

[memory]
max-memory = 4294967296  # 4GB

[persistence]
data-dir = "/var/lib/frogdb"
durability-mode = "periodic"
periodic-sync-ms = 100
snapshot-interval-s = 3600

[timeouts]
scatter-gather-timeout-ms = 1000
client-timeout-s = 300

[observability]
log-level = "info"
log-format = "json"
metrics-port = 9090
otlp-endpoint = "http://otel-collector:4317"

[auth]
# Enable for production
# requirepass = "your-secure-password"
```

---

## Pre-Deployment Checklist

### System Configuration

```bash
# Increase file descriptor limit
echo "* soft nofile 65535" >> /etc/security/limits.conf
echo "* hard nofile 65535" >> /etc/security/limits.conf

# Optimize TCP for Redis-like workloads
echo "net.core.somaxconn = 65535" >> /etc/sysctl.conf
echo "net.ipv4.tcp_max_syn_backlog = 65535" >> /etc/sysctl.conf
echo "vm.overcommit_memory = 1" >> /etc/sysctl.conf
sysctl -p
```

### Memory Allocator

FrogDB uses the system allocator by default. For production workloads, **jemalloc is recommended** to reduce memory fragmentation:

```bash
# Install jemalloc development files
sudo apt-get install libjemalloc-dev  # Ubuntu/Debian
brew install jemalloc                  # macOS

# Build with jemalloc
cargo build --release --features jemalloc
```

| Allocator | Use Case | Trade-off |
|-----------|----------|-----------|
| System (default) | Development, testing | Simple, no dependencies |
| jemalloc | Production (recommended) | Best fragmentation handling |
| mimalloc | High-allocation workloads | Faster allocations, slightly higher memory |

### Verification

```bash
# Test connectivity
redis-cli -h localhost -p 6379 ping

# Check metrics
curl http://localhost:9090/metrics

# Verify persistence
redis-cli -h localhost -p 6379 set testkey testvalue
# Restart server
redis-cli -h localhost -p 6379 get testkey  # Should return "testvalue"
```

---

## Upgrade Procedure

### Single Node Upgrade

1. Create backup/snapshot
2. Stop FrogDB service
3. Replace binary
4. Start FrogDB service
5. Verify health

```bash
sudo systemctl stop frogdb
sudo cp /usr/local/bin/frogdb-server /usr/local/bin/frogdb-server.bak
sudo cp target/release/frogdb-server /usr/local/bin/
sudo systemctl start frogdb
redis-cli -p 6379 ping  # Verify
```
