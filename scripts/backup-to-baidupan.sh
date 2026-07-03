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
MC_IMAGE="${MC_IMAGE:-quay.io/minio/mc:latest}"
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
RUN_MANIFEST="$STATE_DIR/manifest-$(date +%Y%m%d-%H%M%S).tsv"

mkdir -p "$MIRROR_DIR" "$STATE_DIR" "$LOG_DIR"
touch "$STATE_FILE"

mirror_minio_bucket() {
  echo "Mirroring MinIO bucket $MINIO_BUCKET to $MIRROR_DIR"
  docker run --rm \
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
      mc mirror --overwrite --preserve "home/$MINIO_BUCKET" "/backup/mirror/$MINIO_BUCKET"
    '
}

remote_path_for_file() {
  local relative="$1"
  if [[ "$BAIDUPAN_TOOL" == "baidupcs" ]]; then
    printf "/%s/%s/%s" "${BAIDUPAN_REMOTE_DIR#/}" "$MINIO_BUCKET" "$relative"
  else
    printf "%s/%s/%s" "${BAIDUPAN_REMOTE_DIR%/}" "$MINIO_BUCKET" "$relative"
  fi
}

file_signature() {
  local file="$1"
  if stat -f "%z	%m" "$file" >/dev/null 2>&1; then
    stat -f "%z	%m" "$file"
  else
    stat -c "%s	%Y" "$file"
  fi
}

is_uploaded() {
  local relative="$1"
  local size="$2"
  local mtime="$3"
  grep -Fqx "$MINIO_BUCKET	$relative	$size	$mtime" "$STATE_FILE"
}

mark_uploaded() {
  local relative="$1"
  local size="$2"
  local mtime="$3"
  printf "%s\t%s\t%s\t%s\n" "$MINIO_BUCKET" "$relative" "$size" "$mtime" >>"$STATE_FILE"
}

remote_dirname() {
  local remote="$1"
  local dir="${remote%/*}"
  [[ "$dir" == "$remote" ]] && dir="/"
  printf "%s" "$dir"
}

ensure_baidupcs_ready() {
  "$BAIDUPCS_BIN" who >/dev/null
  "$BAIDUPCS_BIN" quota >/dev/null
  "$BAIDUPCS_BIN" config set -max_parallel "$BAIDUPCS_MAX_PARALLEL" >/dev/null || true
}

upload_with_baidupcs() {
  local file="$1"
  local remote="$2"
  local remote_dir
  remote_dir="$(remote_dirname "$remote")"
  "$BAIDUPCS_BIN" mkdir "$remote_dir" >/dev/null 2>&1 || true
  "$BAIDUPCS_BIN" upload "$file" "$remote_dir" --policy rsync
}

upload_with_bypy() {
  local file="$1"
  local remote="$2"
  "$BYPY_BIN" upload "$file" "$remote"
}

upload_file() {
  local file="$1"
  local relative="${file#$MIRROR_DIR/}"
  local signature
  signature="$(file_signature "$file")"
  local size="${signature%%	*}"
  local mtime="${signature#*	}"

  printf "%s\t%s\t%s\t%s\n" "$MINIO_BUCKET" "$relative" "$size" "$mtime" >>"$RUN_MANIFEST"

  if is_uploaded "$relative" "$size" "$mtime"; then
    echo "SKIP $relative"
    return
  fi

  local remote
  remote="$(remote_path_for_file "$relative")"
  echo "$($DRY_RUN && echo DRY || echo UPLOAD) $relative -> $remote"
  if [[ "$DRY_RUN" == "true" ]]; then
    return
  fi

  if [[ "$BAIDUPAN_TOOL" == "baidupcs" ]]; then
    upload_with_baidupcs "$file" "$remote"
  else
    upload_with_bypy "$file" "$remote"
  fi
  mark_uploaded "$relative" "$size" "$mtime"
}

if [[ "$DRY_RUN" != "true" && "$BAIDUPAN_TOOL" == "baidupcs" ]]; then
  ensure_baidupcs_ready
fi

if [[ "$SKIP_MIRROR" != "true" ]]; then
  mirror_minio_bucket
fi

if [[ ! -d "$MIRROR_DIR" ]]; then
  echo "Mirror directory does not exist: $MIRROR_DIR" >&2
  exit 1
fi

while IFS= read -r -d '' file; do
  upload_file "$file"
done < <(find "$MIRROR_DIR" -type f ! -name ".DS_Store" -print0 | sort -z)

echo "Manifest: $RUN_MANIFEST"
echo "Done."
