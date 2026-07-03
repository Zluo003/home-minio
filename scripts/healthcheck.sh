#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."
set -a
source .env
set +a

curl -fsS "http://127.0.0.1:${MINIO_API_PORT:-9000}/minio/health/ready" >/dev/null
docker compose exec -T minio mc alias set local "http://127.0.0.1:9000" "$MINIO_ROOT_USER" "$MINIO_ROOT_PASSWORD" >/dev/null
docker compose exec -T minio mc ls "local/${MINIO_BUCKET:-waule-media}" >/dev/null
echo "home-minio ok"
