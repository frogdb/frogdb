#!/bin/sh
# =============================================================================
# GENERATED FILE - DO NOT EDIT DIRECTLY
# =============================================================================
# Source: ops/deb/deb-gen/
# Regenerate with: just deb-gen
# =============================================================================
set -e

# Create system user/group
if ! getent group frogdb > /dev/null 2>&1; then
    addgroup --system frogdb
fi
if ! getent passwd frogdb > /dev/null 2>&1; then
    adduser --system --ingroup frogdb --home /var/lib/frogdb \
            --no-create-home --shell /usr/sbin/nologin frogdb
fi

# Create data directories
mkdir -p /var/lib/frogdb/data
mkdir -p /var/lib/frogdb/snapshots
mkdir -p /var/lib/frogdb/cluster
mkdir -p /var/log/frogdb

# Set ownership and permissions
chown -R frogdb:frogdb /var/lib/frogdb
chown -R frogdb:frogdb /var/log/frogdb
chmod 750 /var/lib/frogdb
chmod 750 /var/log/frogdb

# Enable and start service on systemd systems
if [ -d /run/systemd/system ]; then
    systemctl daemon-reload
    systemctl enable frogdb-server.service
    systemctl start frogdb-server.service || true
fi