#!/bin/bash
# =============================================================================
# FrogDB Backup Script
# =============================================================================
# This script triggers BGSAVE on FrogDB and copies the snapshot to cloud storage.
#
# Usage:
#   ./backup.sh --namespace frogdb --pod frogdb-0 --bucket s3://my-bucket/backups
#
# Supported cloud backends: s3, gs, azure
# =============================================================================

set -euo pipefail

# Default values
NAMESPACE="frogdb"
POD_NAME="frogdb-0"
BUCKET=""
BACKUP_DIR="/data/snapshots"
TIMEOUT=300
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
    echo "  -b, --bucket BUCKET_URL      Cloud storage bucket URL (required)"
    echo "                               Examples: s3://bucket/path, gs://bucket/path, azure://container/path"
    echo "  -d, --backup-dir DIR         FrogDB backup directory (default: /data/snapshots)"
    echo "  -t, --timeout SECONDS        Timeout for BGSAVE (default: 300)"
    echo "  -h, --help                   Show this help message"
    echo ""
    echo "Examples:"
    echo "  $0 --bucket s3://my-backups/frogdb"
    echo "  $0 --namespace prod --pod frogdb-0 --bucket gs://prod-backups/frogdb"
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
        -b|--bucket)
            BUCKET="$2"
            shift 2
            ;;
        -d|--backup-dir)
            BACKUP_DIR="$2"
            shift 2
            ;;
        -t|--timeout)
            TIMEOUT="$2"
            shift 2
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
if [[ -z "$BUCKET" ]]; then
    log_error "Bucket URL is required"
    usage
fi

# Detect cloud provider from bucket URL
if [[ "$BUCKET" == s3://* ]]; then
    CLOUD_PROVIDER="aws"
elif [[ "$BUCKET" == gs://* ]]; then
    CLOUD_PROVIDER="gcp"
elif [[ "$BUCKET" == azure://* ]]; then
    CLOUD_PROVIDER="azure"
else
    log_error "Invalid bucket URL. Must start with s3://, gs://, or azure://"
    exit 1
fi

log_info "Starting FrogDB backup"
log_info "Namespace: $NAMESPACE"
log_info "Pod: $POD_NAME"
log_info "Bucket: $BUCKET"
log_info "Cloud Provider: $CLOUD_PROVIDER"

# Step 1: Trigger BGSAVE
log_info "Triggering BGSAVE..."
kubectl exec -n "$NAMESPACE" "$POD_NAME" -- redis-cli BGSAVE

# Step 2: Wait for BGSAVE to complete
log_info "Waiting for BGSAVE to complete (timeout: ${TIMEOUT}s)..."
ELAPSED=0
INTERVAL=5

while true; do
    STATUS=$(kubectl exec -n "$NAMESPACE" "$POD_NAME" -- redis-cli LASTSAVE 2>/dev/null)
    BGSAVE_IN_PROGRESS=$(kubectl exec -n "$NAMESPACE" "$POD_NAME" -- redis-cli INFO persistence 2>/dev/null | grep rdb_bgsave_in_progress | cut -d: -f2 | tr -d '\r\n')

    if [[ "$BGSAVE_IN_PROGRESS" == "0" ]]; then
        log_info "BGSAVE completed successfully"
        break
    fi

    if [[ $ELAPSED -ge $TIMEOUT ]]; then
        log_error "BGSAVE timed out after ${TIMEOUT}s"
        exit 1
    fi

    sleep $INTERVAL
    ELAPSED=$((ELAPSED + INTERVAL))
    log_info "Waiting... (${ELAPSED}s elapsed)"
done

# Step 3: Find the latest snapshot
log_info "Finding latest snapshot..."
SNAPSHOT_FILE=$(kubectl exec -n "$NAMESPACE" "$POD_NAME" -- ls -t "$BACKUP_DIR" 2>/dev/null | head -1)

if [[ -z "$SNAPSHOT_FILE" ]]; then
    log_error "No snapshot files found in $BACKUP_DIR"
    exit 1
fi

log_info "Latest snapshot: $SNAPSHOT_FILE"

# Step 4: Generate backup filename with timestamp
TIMESTAMP=$(date -u +%Y%m%d-%H%M%S)
BACKUP_FILENAME="frogdb-backup-${TIMESTAMP}.rdb"

# Step 5: Copy snapshot to cloud storage
log_info "Copying snapshot to $BUCKET/$BACKUP_FILENAME..."

# Create a temporary directory for the backup
TEMP_DIR=$(mktemp -d)
trap "rm -rf $TEMP_DIR" EXIT

# Copy from pod to local
kubectl cp "$NAMESPACE/$POD_NAME:$BACKUP_DIR/$SNAPSHOT_FILE" "$TEMP_DIR/$BACKUP_FILENAME"

# Upload to cloud storage
case $CLOUD_PROVIDER in
    aws)
        aws s3 cp "$TEMP_DIR/$BACKUP_FILENAME" "$BUCKET/$BACKUP_FILENAME"
        ;;
    gcp)
        gsutil cp "$TEMP_DIR/$BACKUP_FILENAME" "$BUCKET/$BACKUP_FILENAME"
        ;;
    azure)
        # Extract container and path from azure://container/path
        CONTAINER=$(echo "$BUCKET" | sed 's|azure://||' | cut -d/ -f1)
        BLOB_PATH=$(echo "$BUCKET" | sed 's|azure://||' | cut -d/ -f2-)
        az storage blob upload --container-name "$CONTAINER" --name "$BLOB_PATH/$BACKUP_FILENAME" --file "$TEMP_DIR/$BACKUP_FILENAME"
        ;;
esac

log_info "Backup completed successfully!"
log_info "Backup location: $BUCKET/$BACKUP_FILENAME"
