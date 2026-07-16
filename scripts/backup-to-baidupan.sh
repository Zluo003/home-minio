#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

if [[ ! -f .env ]]; then
  echo ".env not found. Copy .env.example to .env first." >&2
  exit 1
fi

set -a
source .env
set +a

MINIO_BUCKET="${MINIO_BUCKET:-waule-media}"
MINIO_API_PORT="${MINIO_API_PORT:-9000}"
BAIDUPAN_REMOTE_DIR="${BAIDUPAN_REMOTE_DIR:-NewWaule/home-minio}"
BAIDUPAN_WORK_DIR="${BAIDUPAN_WORK_DIR:-/data/backup}"
BAIDUPAN_TOOL="${BAIDUPAN_TOOL:-baidupcs}"
BYPY_BIN="${BYPY_BIN:-bypy}"
BAIDUPCS_BIN="${BAIDUPCS_BIN:-BaiduPCS-Go}"
BAIDUPCS_MAX_PARALLEL="${BAIDUPCS_MAX_PARALLEL:-16}"
BAIDUPCS_UPLOAD_NORAPID="${BAIDUPCS_UPLOAD_NORAPID:-true}"
MC_IMAGE="${MC_IMAGE:-quay.io/minio/mc:latest}"
HOME_MINIO_STATE_DB="${HOME_MINIO_STATE_DB:-./state/home-minio.sqlite}"
DRY_RUN=false

for arg in "$@"; do
  case "$arg" in
    --dry-run) DRY_RUN=true ;;
    *)
      echo "Unknown argument: $arg" >&2
      exit 1
      ;;
  esac
done

if [[ "${BAIDUPAN_BACKUP_ENABLED:-false}" != "true" && "$DRY_RUN" != "true" ]]; then
  echo "BAIDUPAN_BACKUP_ENABLED is not true. Use --dry-run to test without uploading." >&2
  exit 1
fi

command -v docker >/dev/null
if [[ "$DRY_RUN" != "true" ]]; then
  case "$BAIDUPAN_TOOL" in
    baidupcs)
      if ! command -v "$BAIDUPCS_BIN" >/dev/null; then
        echo "BaiduPCS-Go command not found: $BAIDUPCS_BIN" >&2
        exit 1
      fi
      ;;
    bypy)
      if ! command -v "$BYPY_BIN" >/dev/null; then
        echo "bypy command not found: $BYPY_BIN" >&2
        exit 1
      fi
      ;;
    *)
      echo "BAIDUPAN_TOOL must be baidupcs or bypy." >&2
      exit 1
      ;;
  esac
fi

WORK_PARENT="$(dirname "$BAIDUPAN_WORK_DIR")"
mkdir -p "$WORK_PARENT"
WORK_DIR="$(cd "$WORK_PARENT" && pwd)/$(basename "$BAIDUPAN_WORK_DIR")"
LEGACY_MIRROR_DIR="$WORK_DIR/mirror/$MINIO_BUCKET"
SPOOL_DIR="$WORK_DIR/spool"
STATE_DIR="$WORK_DIR/state"
LOG_DIR="$WORK_DIR/logs"
STATE_FILE="$STATE_DIR/baidupan-uploaded.tsv"
RUN_ID="baidupan-$(date -u +%Y%m%dT%H%M%SZ)-$$"
RUN_MANIFEST="$STATE_DIR/manifest-$RUN_ID.jsonl"
INVENTORY_PATH="$STATE_DIR/inventory-$RUN_ID.jsonl"
LOCK_FILE="$STATE_DIR/baidupan-backup.lock"

mkdir -p "$SPOOL_DIR" "$STATE_DIR" "$LOG_DIR"
touch "$STATE_FILE"

if ! command -v flock >/dev/null; then
  echo "flock is required for safe backup locking." >&2
  exit 1
fi
exec 9>"$LOCK_FILE"
if ! flock -n 9; then
  echo "Another Baidu Netdisk backup is already running." >&2
  exit 1
fi
trap 'rm -f "$INVENTORY_PATH"' EXIT

inventory_minio_bucket() {
  echo "Listing MinIO bucket metadata for $MINIO_BUCKET"
  rm -f "$INVENTORY_PATH"
  if ! docker run --rm \
    --network "container:home-minio" \
    -e MINIO_ROOT_USER="$MINIO_ROOT_USER" \
    -e MINIO_ROOT_PASSWORD="$MINIO_ROOT_PASSWORD" \
    -e MINIO_BUCKET="$MINIO_BUCKET" \
    --entrypoint sh \
    "$MC_IMAGE" \
    -eu -c '
      mc alias set home http://127.0.0.1:9000 "$MINIO_ROOT_USER" "$MINIO_ROOT_PASSWORD" >/dev/null
      mc mb --ignore-existing "home/$MINIO_BUCKET" >/dev/null
      mc ls --recursive --json "home/$MINIO_BUCKET"
    ' >"$INVENTORY_PATH"; then
    echo "MinIO inventory failed; no backup manifest was created." >&2
    return 1
  fi
}

ensure_baidupcs_ready() {
  "$BAIDUPCS_BIN" who >/dev/null
  "$BAIDUPCS_BIN" quota >/dev/null
  "$BAIDUPCS_BIN" config set -max_parallel "$BAIDUPCS_MAX_PARALLEL" >/dev/null || true
}

inventory_minio_bucket

manifest_args=(
  --run-id "$RUN_ID"
  --bucket "$MINIO_BUCKET"
  --inventory "$INVENTORY_PATH"
  --uploaded-state "$STATE_FILE"
  --manifest "$RUN_MANIFEST"
  --db "$HOME_MINIO_STATE_DB"
)
if [[ "$DRY_RUN" == "true" ]]; then
  manifest_args+=(--dry-run)
fi
node ./scripts/build-baidupan-backup-manifest.mjs "${manifest_args[@]}"

if [[ "$DRY_RUN" != "true" && "$BAIDUPAN_TOOL" == "baidupcs" ]]; then
  ensure_baidupcs_ready
fi

runner_args=(
  --run-id "$RUN_ID"
  --spool-dir "$SPOOL_DIR"
  --db "$HOME_MINIO_STATE_DB"
  --tool "$BAIDUPAN_TOOL"
  --remote-dir "$BAIDUPAN_REMOTE_DIR"
  --baidupcs-bin "$BAIDUPCS_BIN"
  --bypy-bin "$BYPY_BIN"
  --max-parallel "$BAIDUPCS_MAX_PARALLEL"
)
if [[ "$BAIDUPCS_UPLOAD_NORAPID" == "true" ]]; then
  runner_args+=(--no-rapid)
fi
if [[ "$DRY_RUN" == "true" ]]; then
  runner_args+=(--dry-run)
fi

echo "Manifest: $RUN_MANIFEST"
runner_exit=0
if node ./scripts/run-baidupan-backup-manifest.mjs "${runner_args[@]}"; then
  runner_exit=0
else
  runner_exit=$?
fi

if [[ "$DRY_RUN" != "true" && -d "$LEGACY_MIRROR_DIR" ]]; then
  if ! node ./scripts/cleanup-legacy-baidupan-mirror.mjs \
    --mirror-dir "$LEGACY_MIRROR_DIR" \
    --inventory "$INVENTORY_PATH" \
    --db "$HOME_MINIO_STATE_DB" \
    --bucket "$MINIO_BUCKET"; then
    echo "Legacy mirror cleanup failed; remaining legacy files were left in place." >&2
  fi
fi

if [[ "$runner_exit" -ne 0 ]]; then
  echo "Backup run $RUN_ID completed with errors; failed items remain queued in SQLite." >&2
  exit 1
fi
