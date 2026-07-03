# Home MinIO for NewWaule Cold Media

This project runs a private MinIO instance on the home Linux server. NewWaule uses it as cold media storage through the S3-compatible API.

## Recommended Network

Do not expose MinIO directly to the public internet. Put the home Linux server and the Hong Kong NewWaule server into the same private network with Tailscale or WireGuard, then point NewWaule to the private endpoint:

```env
HOME_MINIO_ENDPOINT=http://100.x.y.z:9000
```

## Start

```bash
cp .env.example .env
# edit .env secrets first
docker compose up -d
./scripts/healthcheck.sh
```

## NewWaule Environment

Set these on the Hong Kong NewWaule API/worker host:

```env
HOME_MINIO_ENABLED=true
HOME_MINIO_ENDPOINT=http://100.x.y.z:9000
HOME_MINIO_REGION=us-east-1
HOME_MINIO_BUCKET=waule-media
HOME_MINIO_ACCESS_KEY_ID=waule-newwaule
HOME_MINIO_SECRET_ACCESS_KEY=change-me-minio-secret
HOME_MINIO_FORCE_PATH_STYLE=true
HOME_MINIO_CACHE_DIR=storage/local-media
HOME_MINIO_PUBLIC_BASE_URL=https://api.example.com
```

`HOME_MINIO_PUBLIC_BASE_URL` is the public NewWaule API root. Migrated files will keep user-facing URLs under:

```text
https://api.example.com/local-media/<objectKey>
```

The file itself is stored in home MinIO. When users request the URL, NewWaule first checks local cache, then pulls the object from home MinIO and caches it on the Hong Kong server.

## Migrate Existing OSS Files

Prefer running from NewWaule so the database is updated safely after each object is uploaded and verified:

```bash
pnpm --filter @waule/api media:migrate-home-minio -- --source=OSS --older-than-days=15 --limit=100
pnpm --filter @waule/api media:migrate-home-minio -- --source=OSS --older-than-days=15 --limit=100 --execute
```

Use `--source=ALL` to include both old OSS assets and new local assets.

For a large one-time transfer, you may also mirror OSS directly from the home server with `scripts/sync-oss-to-minio.example.sh`, but still run the NewWaule migration script afterward so database records point to `HOME_MINIO`.

## Cron Example

```cron
15 3 * * * cd /home/New-Waule && pnpm --filter @waule/api media:migrate-home-minio -- --source=ALL --older-than-days=15 --limit=200 --execute >> /var/log/newwaule-home-minio-migrate.log 2>&1
```

## Backup Note

Home MinIO becomes the durable copy for cold user media. Use at least a mirrored disk, ZFS mirror, or a second backup target.
