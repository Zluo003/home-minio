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
BAIDUPAN_REMOTE_DIR="${BAIDUPAN_REMOTE_DIR:-/NewWaule/home-minio}"
BAIDUPAN_WORK_DIR="${BAIDUPAN_WORK_DIR:-./backup}"
BAIDUPAN_TOOL="${BAIDUPAN_TOOL:-baidupcs}"
BYPY_BIN="${BYPY_BIN:-bypy}"
BAIDUPCS_BIN="${BAIDUPCS_BIN:-BaiduPCS-Go}"
BAIDUPCS_MAX_PARALLEL="${BAIDUPCS_MAX_PARALLEL:-16}"
MC_IMAGE="${MC_IMAGE:-quay.io/minio/mc:RELEASE.2026-06-13T12-46-12Z}"
DRY_RUN=false
UPLOAD_TO_MINIO=false

for arg in "$@"; do
  case "$arg" in
    --dry-run) DRY_RUN=true ;;
    --upload-to-minio) UPLOAD_TO_MINIO=true ;;
    *)
      echo "Unknown argument: $arg" >&2
      exit 1
      ;;
  esac
done

command -v docker >/dev/null
if [[ "$DRY_RUN" != "true" ]]; then
  case "$BAIDUPAN_TOOL" in
    baidupcs) command -v "$BAIDUPCS_BIN" >/dev/null ;;
    bypy) command -v "$BYPY_BIN" >/dev/null ;;
    *)
      echo "BAIDUPAN_TOOL must be baidupcs or bypy." >&2
      exit 1
      ;;
  esac
fi

WORK_PARENT="$(dirname "$BAIDUPAN_WORK_DIR")"
mkdir -p "$WORK_PARENT"
WORK_DIR="$(cd "$WORK_PARENT" && pwd)/$(basename "$BAIDUPAN_WORK_DIR")"
RESTORE_DIR="$WORK_DIR/restore/$MINIO_BUCKET"
REMOTE_ROOT_BAIDUPCS="/${BAIDUPAN_REMOTE_DIR#/}/$MINIO_BUCKET"
REMOTE_ROOT_BYPY="${BAIDUPAN_REMOTE_DIR%/}/$MINIO_BUCKET"

mkdir -p "$RESTORE_DIR"

download_with_baidupcs() {
  "$BAIDUPCS_BIN" who >/dev/null
  "$BAIDUPCS_BIN" quota >/dev/null
  "$BAIDUPCS_BIN" config set -max_parallel "$BAIDUPCS_MAX_PARALLEL" >/dev/null || true
  "$BAIDUPCS_BIN" download "$REMOTE_ROOT_BAIDUPCS" --saveto "$RESTORE_DIR" --ow -p "$BAIDUPCS_MAX_PARALLEL"
}

download_with_bypy() {
  "$BYPY_BIN" downdir "$REMOTE_ROOT_BYPY" "$RESTORE_DIR"
}

upload_restore_to_minio() {
  echo "Restoring $RESTORE_DIR to MinIO bucket $MINIO_BUCKET"
  docker run --rm \
    --network "container:home-minio" \
    -v "$WORK_DIR:/backup" \
    -e MINIO_ROOT_USER="$MINIO_ROOT_USER" \
    -e MINIO_ROOT_PASSWORD="$MINIO_ROOT_PASSWORD" \
    -e MINIO_BUCKET="$MINIO_BUCKET" \
    "$MC_IMAGE" \
    sh -eu -c '
      mc alias set home http://127.0.0.1:9000 "$MINIO_ROOT_USER" "$MINIO_ROOT_PASSWORD" >/dev/null
      mc mb --ignore-existing "home/$MINIO_BUCKET" >/dev/null
      mc mirror --overwrite --preserve "/backup/restore/$MINIO_BUCKET" "home/$MINIO_BUCKET"
    '
}

echo "Restore source: $([[ "$BAIDUPAN_TOOL" == "baidupcs" ]] && echo "$REMOTE_ROOT_BAIDUPCS" || echo "/apps/bypy/$REMOTE_ROOT_BYPY")"
echo "Restore target directory: $RESTORE_DIR"
if [[ "$DRY_RUN" == "true" ]]; then
  echo "Dry run only. No files downloaded."
  exit 0
fi

if [[ "$BAIDUPAN_TOOL" == "baidupcs" ]]; then
  download_with_baidupcs
else
  download_with_bypy
fi

if [[ "$UPLOAD_TO_MINIO" == "true" ]]; then
  upload_restore_to_minio
else
  echo "Downloaded only. Add --upload-to-minio to mirror restored files back into MinIO."
fi
