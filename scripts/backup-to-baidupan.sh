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
BAIDUPAN_WORK_DIR="${BAIDUPAN_WORK_DIR:-./backup}"
BAIDUPAN_TOOL="${BAIDUPAN_TOOL:-baidupcs}"
BYPY_BIN="${BYPY_BIN:-bypy}"
BAIDUPCS_BIN="${BAIDUPCS_BIN:-BaiduPCS-Go}"
BAIDUPCS_MAX_PARALLEL="${BAIDUPCS_MAX_PARALLEL:-16}"
BAIDUPCS_UPLOAD_NORAPID="${BAIDUPCS_UPLOAD_NORAPID:-true}"
MC_IMAGE="${MC_IMAGE:-quay.io/minio/mc:latest}"
HOME_MINIO_STATE_DB="${HOME_MINIO_STATE_DB:-./state/home-minio.sqlite}"
DRY_RUN=false
SKIP_MIRROR=false

for arg in "$@"; do
  case "$arg" in
    --dry-run) DRY_RUN=true ;;
    --skip-mirror) SKIP_MIRROR=true ;;
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
MIRROR_DIR="$WORK_DIR/mirror/$MINIO_BUCKET"
STATE_DIR="$WORK_DIR/state"
LOG_DIR="$WORK_DIR/logs"
STATE_FILE="$STATE_DIR/baidupan-uploaded.tsv"
MIRROR_REPORT="$STATE_DIR/baidupan-mirror-changes.jsonl"
MIRROR_REPORT_NEXT="$STATE_DIR/baidupan-mirror-changes.next.jsonl"
RUN_ID="baidupan-$(date -u +%Y%m%dT%H%M%SZ)-$$"
RUN_MANIFEST="$STATE_DIR/manifest-$RUN_ID.jsonl"
LOCK_FILE="$STATE_DIR/baidupan-backup.lock"

mkdir -p "$MIRROR_DIR" "$STATE_DIR" "$LOG_DIR"
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
trap 'rm -f "$MIRROR_REPORT_NEXT" "$MIRROR_REPORT_NEXT.merged"' EXIT

mirror_minio_bucket() {
  echo "Mirroring MinIO bucket $MINIO_BUCKET to $MIRROR_DIR"
  rm -f "$MIRROR_REPORT_NEXT"
  if ! docker run --rm \
    --network "container:home-minio" \
    -v "$WORK_DIR:/backup" \
    -e MINIO_ROOT_USER="$MINIO_ROOT_USER" \
    -e MINIO_ROOT_PASSWORD="$MINIO_ROOT_PASSWORD" \
    -e MINIO_BUCKET="$MINIO_BUCKET" \
    --entrypoint sh \
    "$MC_IMAGE" \
    -eu -c '
      mc alias set home http://127.0.0.1:9000 "$MINIO_ROOT_USER" "$MINIO_ROOT_PASSWORD" >/dev/null
      mc mb --ignore-existing "home/$MINIO_BUCKET" >/dev/null
      mc mirror --json --overwrite --preserve "home/$MINIO_BUCKET" "/backup/mirror/$MINIO_BUCKET"
    ' >"$MIRROR_REPORT_NEXT"; then
    echo "MinIO mirror failed; no backup manifest was created." >&2
    return 1
  fi

  if [[ -s "$MIRROR_REPORT" ]]; then
    cat "$MIRROR_REPORT" "$MIRROR_REPORT_NEXT" >"$MIRROR_REPORT_NEXT.merged"
    mv "$MIRROR_REPORT_NEXT.merged" "$MIRROR_REPORT"
    rm -f "$MIRROR_REPORT_NEXT"
  else
    mv "$MIRROR_REPORT_NEXT" "$MIRROR_REPORT"
  fi
}

ensure_baidupcs_ready() {
  "$BAIDUPCS_BIN" who >/dev/null
  "$BAIDUPCS_BIN" quota >/dev/null
  "$BAIDUPCS_BIN" config set -max_parallel "$BAIDUPCS_MAX_PARALLEL" >/dev/null || true
}

if [[ "$SKIP_MIRROR" != "true" ]]; then
  mirror_minio_bucket
fi

if [[ ! -d "$MIRROR_DIR" ]]; then
  echo "Mirror directory does not exist: $MIRROR_DIR" >&2
  exit 1
fi

manifest_args=(
  --run-id "$RUN_ID"
  --bucket "$MINIO_BUCKET"
  --mirror-dir "$MIRROR_DIR"
  --uploaded-state "$STATE_FILE"
  --manifest "$RUN_MANIFEST"
  --db "$HOME_MINIO_STATE_DB"
)
if [[ -s "$MIRROR_REPORT" ]]; then
  manifest_args+=(--mirror-report "$MIRROR_REPORT")
fi
if [[ "$DRY_RUN" == "true" ]]; then
  manifest_args+=(--dry-run)
fi
node ./scripts/build-baidupan-backup-manifest.mjs "${manifest_args[@]}"
rm -f "$MIRROR_REPORT"

if [[ "$DRY_RUN" != "true" && "$BAIDUPAN_TOOL" == "baidupcs" ]]; then
  ensure_baidupcs_ready
fi

runner_args=(
  --run-id "$RUN_ID"
  --mirror-dir "$MIRROR_DIR"
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
if ! node ./scripts/run-baidupan-backup-manifest.mjs "${runner_args[@]}"; then
  echo "Backup run $RUN_ID completed with errors; failed items remain queued in SQLite." >&2
  exit 1
fi
