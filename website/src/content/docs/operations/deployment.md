---
title: "Deployment"
description: "Deploy FrogDB with Docker, packages, or from source."
sidebar:
  order: 1
---

## Docker

```bash
docker run -d --name frogdb \
  -p 6379:6379 \
  -v frogdb-data:/data \
  -e FROGDB_SERVER__BIND=0.0.0.0 \
  frogdb/frogdb:latest
```

With custom config:

```bash
docker run -d --name frogdb \
  -p 6379:6379 -p 9090:9090 \
  -v frogdb-data:/data \
  -v $(pwd)/frogdb.toml:/etc/frogdb/frogdb.toml:ro \
  frogdb/frogdb:latest --config /etc/frogdb/frogdb.toml
```

Docker Compose:

```yaml
services:
  frogdb:
    image: frogdb/frogdb:latest
    ports:
      - "6379:6379"
      - "9090:9090"
    volumes:
      - frogdb-data:/data
      - ./frogdb.toml:/etc/frogdb/frogdb.toml:ro
    command: ["frogdb-server", "--config", "/etc/frogdb/frogdb.toml"]
    restart: unless-stopped
    healthcheck:
      test: ["CMD", "redis-cli", "-p", "6379", "ping"]
      interval: 10s
      retries: 3
    deploy:
      resources:
        limits:
          memory: 4G

volumes:
  frogdb-data:
```

## systemd

```ini
# /etc/systemd/system/frogdb.service
[Unit]
Description=FrogDB Server
After=network.target

[Service]
Type=simple
User=frogdb
Group=frogdb
ExecStart=/usr/local/bin/frogdb-server --config /etc/frogdb/frogdb.toml
ExecReload=/bin/kill -HUP $MAINPID
Restart=on-failure
RestartSec=5
NoNewPrivileges=yes
ProtectSystem=strict
ProtectHome=yes
PrivateTmp=yes
ReadWritePaths=/var/lib/frogdb
LimitNOFILE=65535

[Install]
WantedBy=multi-user.target
```

```bash
sudo useradd --system --home-dir /var/lib/frogdb --shell /bin/false frogdb
sudo mkdir -p /var/lib/frogdb /etc/frogdb
sudo chown frogdb:frogdb /var/lib/frogdb
sudo systemctl daemon-reload && sudo systemctl enable --now frogdb
```

## Kubernetes

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
    metadata:
      labels:
        app: frogdb
      annotations:
        prometheus.io/scrape: "true"
        prometheus.io/port: "9090"
    spec:
      containers:
      - name: frogdb
        image: frogdb/frogdb:latest
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
        volumeMounts:
        - name: data
          mountPath: /data
        - name: config
          mountPath: /etc/frogdb
          readOnly: true
        livenessProbe:
          httpGet:
            path: /health/live
            port: 9090
          initialDelaySeconds: 10
        readinessProbe:
          httpGet:
            path: /health/ready
            port: 9090
          initialDelaySeconds: 5
      volumes:
      - name: config
        configMap:
          name: frogdb-config
  volumeClaimTemplates:
  - metadata:
      name: data
    spec:
      accessModes: ["ReadWriteOnce"]
      resources:
        requests:
          storage: 50Gi
```

## OS Tuning

```bash
# File descriptors
echo "* soft nofile 65535" >> /etc/security/limits.conf
echo "* hard nofile 65535" >> /etc/security/limits.conf

# TCP backlog
echo "net.core.somaxconn = 65535" >> /etc/sysctl.conf
sysctl -p
```

## Production Configuration

```toml
[server]
bind = "0.0.0.0"
port = 6379
num-shards = 0  # Auto-detect CPU cores

[persistence]
enabled = true
data-dir = "/var/lib/frogdb"
durability-mode = "periodic"
sync-interval-ms = 1000

[snapshot]
snapshot-dir = "/var/lib/frogdb/snapshots"
snapshot-interval-secs = 3600

[memory]
maxmemory = 4294967296  # 4 GB
maxmemory-policy = "allkeys-lru"

[security]
requirepass = "your-secure-password"

[logging]
level = "info"
format = "json"

[metrics]
enabled = true
port = 9090
```

## Verify

```bash
redis-cli -p 6379 ping           # PONG
curl http://localhost:9090/metrics # Prometheus metrics
```
