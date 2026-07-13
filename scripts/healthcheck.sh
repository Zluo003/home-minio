#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."
if [[ -f .env ]]; then
  set -a
  source .env
  set +a
fi

MINIO_ROOT_USER="${MINIO_ROOT_USER:-change-me-admin}"
MINIO_ROOT_PASSWORD="${MINIO_ROOT_PASSWORD:-change-me-long-password}"
MINIO_API_PORT="${MINIO_API_PORT:-19000}"
MINIO_BUCKET="${MINIO_BUCKET:-waule-media}"

curl -fsS "http://127.0.0.1:${MINIO_API_PORT}/minio/health/ready" >/dev/null
docker compose exec -T minio mc alias set local "http://127.0.0.1:9000" "$MINIO_ROOT_USER" "$MINIO_ROOT_PASSWORD" >/dev/null
docker compose exec -T minio mc ls "local/${MINIO_BUCKET}" >/dev/null
echo "home-minio ok"
