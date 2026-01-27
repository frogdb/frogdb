#!/bin/bash
# =============================================================================
# FrogDB Restore Script
# =============================================================================
# This script restores a FrogDB backup from cloud storage.
#
# Usage:
#   ./restore.sh --namespace frogdb --pod frogdb-0 --backup s3://my-bucket/backups/frogdb-backup-20240101-120000.rdb
#
# WARNING: This will stop FrogDB, replace the data, and restart it.
#          Data will be lost if not backed up!
# =============================================================================

set -euo pipefail

# Default values
NAMESPACE="frogdb"
POD_NAME="frogdb-0"
BACKUP_URL=""
DATA_DIR="/data"
FORCE=false
CLOUD_PROVIDER=""

# Color codes
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

log_info() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

log_warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

usage() {
    echo "Usage: $0 [OPTIONS]"
    echo ""
    echo "Options:"
    echo "  -n, --namespace NAMESPACE    Kubernetes namespace (default: frogdb)"
    echo "  -p, --pod POD_NAME           FrogDB pod name (default: frogdb-0)"
    echo "  -r, --backup BACKUP_URL      Cloud storage backup URL (required)"
    echo "                               Examples: s3://bucket/path/file.rdb"
    echo "  -d, --data-dir DIR           FrogDB data directory (default: /data)"
    echo "  -f, --force                  Skip confirmation prompt"
    echo "  -h, --help                   Show this help message"
    echo ""
    echo "Examples:"
    echo "  $0 --backup s3://my-backups/frogdb/frogdb-backup-20240101.rdb"
    echo "  $0 --namespace prod --pod frogdb-0 --backup gs://prod-backups/frogdb/backup.rdb --force"
    exit 1
}

# Parse arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        -n|--namespace)
            NAMESPACE="$2"
            shift 2
            ;;
        -p|--pod)
            POD_NAME="$2"
            shift 2
            ;;
        -r|--backup)
            BACKUP_URL="$2"
            shift 2
            ;;
        -d|--data-dir)
            DATA_DIR="$2"
            shift 2
            ;;
        -f|--force)
            FORCE=true
            shift
            ;;
        -h|--help)
            usage
            ;;
        *)
            log_error "Unknown option: $1"
            usage
            ;;
    esac
done

# Validate required arguments
if [[ -z "$BACKUP_URL" ]]; then
    log_error "Backup URL is required"
    usage
fi

# Detect cloud provider from backup URL
if [[ "$BACKUP_URL" == s3://* ]]; then
    CLOUD_PROVIDER="aws"
elif [[ "$BACKUP_URL" == gs://* ]]; then
    CLOUD_PROVIDER="gcp"
elif [[ "$BACKUP_URL" == azure://* ]]; then
    CLOUD_PROVIDER="azure"
else
    log_error "Invalid backup URL. Must start with s3://, gs://, or azure://"
    exit 1
fi

log_warn "=========================================="
log_warn "         FrogDB RESTORE OPERATION"
log_warn "=========================================="
log_warn ""
log_warn "This will:"
log_warn "  1. Scale down the FrogDB StatefulSet"
log_warn "  2. Replace the data with the backup"
log_warn "  3. Scale up the FrogDB StatefulSet"
log_warn ""
log_warn "Namespace: $NAMESPACE"
log_warn "Pod: $POD_NAME"
log_warn "Backup: $BACKUP_URL"
log_warn ""
log_warn "DATA WILL BE LOST IF NOT BACKED UP!"
log_warn ""

if [[ "$FORCE" != true ]]; then
    read -p "Are you sure you want to continue? (yes/no): " CONFIRM
    if [[ "$CONFIRM" != "yes" ]]; then
        log_info "Restore cancelled"
        exit 0
    fi
fi

log_info "Starting FrogDB restore"

# Get StatefulSet name
STATEFULSET=$(kubectl get pod -n "$NAMESPACE" "$POD_NAME" -o jsonpath='{.metadata.ownerReferences[?(@.kind=="StatefulSet")].name}')

if [[ -z "$STATEFULSET" ]]; then
    log_error "Could not find StatefulSet for pod $POD_NAME"
    exit 1
fi

log_info "StatefulSet: $STATEFULSET"

# Step 1: Scale down StatefulSet
log_info "Scaling down StatefulSet..."
kubectl scale statefulset -n "$NAMESPACE" "$STATEFULSET" --replicas=0

# Wait for pods to terminate
log_info "Waiting for pods to terminate..."
kubectl wait --for=delete pod/"$POD_NAME" -n "$NAMESPACE" --timeout=120s || true

# Step 2: Download backup from cloud storage
log_info "Downloading backup from $BACKUP_URL..."

TEMP_DIR=$(mktemp -d)
trap "rm -rf $TEMP_DIR" EXIT
BACKUP_FILE="$TEMP_DIR/restore.rdb"

case $CLOUD_PROVIDER in
    aws)
        aws s3 cp "$BACKUP_URL" "$BACKUP_FILE"
        ;;
    gcp)
        gsutil cp "$BACKUP_URL" "$BACKUP_FILE"
        ;;
    azure)
        # Extract container and blob path
        CONTAINER=$(echo "$BACKUP_URL" | sed 's|azure://||' | cut -d/ -f1)
        BLOB_PATH=$(echo "$BACKUP_URL" | sed 's|azure://||' | cut -d/ -f2-)
        az storage blob download --container-name "$CONTAINER" --name "$BLOB_PATH" --file "$BACKUP_FILE"
        ;;
esac

if [[ ! -f "$BACKUP_FILE" ]]; then
    log_error "Failed to download backup"
    exit 1
fi

log_info "Backup downloaded successfully"

# Step 3: Create a temporary pod to restore the data
log_info "Creating restore pod..."

# Get PVC name
PVC_NAME=$(kubectl get statefulset -n "$NAMESPACE" "$STATEFULSET" -o jsonpath='{.spec.volumeClaimTemplates[0].metadata.name}')-"$POD_NAME"

cat <<EOF | kubectl apply -f -
apiVersion: v1
kind: Pod
metadata:
  name: frogdb-restore
  namespace: $NAMESPACE
  labels:
    app: frogdb-restore
spec:
  containers:
  - name: restore
    image: debian:bookworm-slim
    command: ["sleep", "infinity"]
    volumeMounts:
    - name: data
      mountPath: /data
  volumes:
  - name: data
    persistentVolumeClaim:
      claimName: $PVC_NAME
  restartPolicy: Never
EOF

# Wait for restore pod to be ready
log_info "Waiting for restore pod to be ready..."
kubectl wait --for=condition=Ready pod/frogdb-restore -n "$NAMESPACE" --timeout=60s

# Step 4: Copy backup to the PVC
log_info "Copying backup to PVC..."
kubectl cp "$BACKUP_FILE" "$NAMESPACE/frogdb-restore:/data/dump.rdb"

# Step 5: Clean up restore pod
log_info "Cleaning up restore pod..."
kubectl delete pod -n "$NAMESPACE" frogdb-restore --wait=true

# Step 6: Scale up StatefulSet
log_info "Scaling up StatefulSet..."
kubectl scale statefulset -n "$NAMESPACE" "$STATEFULSET" --replicas=1

# Wait for pod to be ready
log_info "Waiting for FrogDB to be ready..."
kubectl wait --for=condition=Ready pod/"$POD_NAME" -n "$NAMESPACE" --timeout=300s

# Step 7: Verify restore
log_info "Verifying restore..."
PING_RESULT=$(kubectl exec -n "$NAMESPACE" "$POD_NAME" -- redis-cli PING 2>/dev/null)

if [[ "$PING_RESULT" == "PONG" ]]; then
    log_info "FrogDB is responding to PING"
else
    log_error "FrogDB is not responding correctly"
    exit 1
fi

# Get key count
KEY_COUNT=$(kubectl exec -n "$NAMESPACE" "$POD_NAME" -- redis-cli DBSIZE 2>/dev/null | grep -oE '[0-9]+' || echo "unknown")

log_info "Restore completed successfully!"
log_info "Database contains approximately $KEY_COUNT keys"
