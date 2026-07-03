#!/usr/bin/env bash
set -euo pipefail

# Example only. Run this on the home Linux server if you prefer direct OSS -> MinIO sync.
# Requires MinIO Client: https://min.io/docs/minio/linux/reference/minio-mc.html

: "${ALIYUN_OSS_ENDPOINT:?set ALIYUN_OSS_ENDPOINT, e.g. https://oss-cn-hongkong.aliyuncs.com}"
: "${ALIYUN_OSS_BUCKET:?set ALIYUN_OSS_BUCKET}"
: "${ALIYUN_OSS_ACCESS_KEY_ID:?set ALIYUN_OSS_ACCESS_KEY_ID}"
: "${ALIYUN_OSS_ACCESS_KEY_SECRET:?set ALIYUN_OSS_ACCESS_KEY_SECRET}"
: "${HOME_MINIO_ENDPOINT:?set HOME_MINIO_ENDPOINT, e.g. http://127.0.0.1:9000}"
: "${HOME_MINIO_BUCKET:=waule-media}"
: "${HOME_MINIO_ACCESS_KEY_ID:?set HOME_MINIO_ACCESS_KEY_ID}"
: "${HOME_MINIO_SECRET_ACCESS_KEY:?set HOME_MINIO_SECRET_ACCESS_KEY}"
: "${OSS_PREFIX:=}"

mc alias set aliyun "$ALIYUN_OSS_ENDPOINT" "$ALIYUN_OSS_ACCESS_KEY_ID" "$ALIYUN_OSS_ACCESS_KEY_SECRET"
mc alias set home "$HOME_MINIO_ENDPOINT" "$HOME_MINIO_ACCESS_KEY_ID" "$HOME_MINIO_SECRET_ACCESS_KEY"
mc mirror --overwrite --preserve "aliyun/$ALIYUN_OSS_BUCKET/$OSS_PREFIX" "home/$HOME_MINIO_BUCKET/$OSS_PREFIX"
