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

Default ports are five digits to avoid common local-service conflicts:

```text
MinIO API:      19000
MinIO Console:  19001
Web API:        19090
Web Console:    19091
```

Open the local management console:

```text
http://<home-server-ip>:19091
```

The web console reads and updates `.env`, checks MinIO readiness, and can trigger Baidu Netdisk backup dry-runs or backups. If `HOME_MINIO_WEB_TOKEN` is set, the browser will ask for it on first use. Port, account, MinIO credential, and backup schedule changes require restarting compose:

```bash
docker compose up -d --force-recreate
```

Set the MinIO address that NewWaule should use. In production this is usually a Tailscale/WireGuard address reachable from the Hong Kong server:

```env
HOME_MINIO_PUBLIC_ENDPOINT=http://100.x.y.z:19000
HOME_MINIO_CONSOLE_PUBLIC_URL=http://100.x.y.z:19001
NEWWAULE_PUBLIC_BASE_URL=https://api.example.com
```

`NEWWAULE_PUBLIC_BASE_URL` is the public NewWaule API root. It is copied into NewWaule as `HOME_MINIO_PUBLIC_BASE_URL`, so migrated media URLs become:

```text
https://api.example.com/local-media/<objectKey>
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

## Baidu Netdisk Backup

This is only for disaster backup. NewWaule does not read media from Baidu Netdisk.

For SVIP accounts, use `BaiduPCS-Go` as the primary backup tool. The CLI does not create speed beyond what Baidu allows for the logged-in account, so the practical requirement is: log in with the SVIP account and use BaiduPCS-Go instead of the slower bypy API path.

Install and authorize `BaiduPCS-Go` on the home Linux server:

```bash
BaiduPCS-Go login
BaiduPCS-Go who
BaiduPCS-Go quota
```

`bypy` is kept as a fallback only:

```bash
python3 -m pip install --user bypy
bypy info
```

Backups preserve MinIO object keys exactly. If MinIO has:

```text
gateway-media/2026/07/03/example.mp4
```

Baidu Netdisk stores:

```text
/NewWaule/home-minio/<bucket>/gateway-media/2026/07/03/example.mp4
```

Enable backup in `.env`:

```env
BAIDUPAN_BACKUP_ENABLED=true
BAIDUPAN_TOOL=baidupcs
BAIDUPAN_REMOTE_DIR=NewWaule/home-minio
BAIDUPAN_WORK_DIR=./backup
BAIDUPAN_CRON_SCHEDULE=35 3 * * *
BYPY_BIN=bypy
BAIDUPCS_BIN=BaiduPCS-Go
BAIDUPCS_MAX_PARALLEL=16
```

Run a dry check first:

```bash
./scripts/backup-to-baidupan.sh --dry-run
```

Run the real backup:

```bash
./scripts/backup-to-baidupan.sh
```

The script first mirrors MinIO objects to `backup/mirror/<bucket>/`, then uploads new or changed files to Baidu Netdisk. It records uploaded file signatures in `backup/state/baidupan-uploaded.tsv`, so repeated runs skip unchanged files and do not delete anything from Baidu Netdisk.

Automatic backup is handled by the `backup-scheduler` compose service. It reads `BAIDUPAN_CRON_SCHEDULE`; the current format supports daily schedules such as `35 3 * * *`.

```bash
docker compose up -d --force-recreate backup-scheduler
```

## Restore From Baidu Netdisk

Restore downloads the same `<bucket>/<objectKey>` layout back to `backup/restore/<bucket>/`.

Dry run:

```bash
./scripts/restore-from-baidupan.sh --dry-run
```

Download backup files only:

```bash
./scripts/restore-from-baidupan.sh
```

Download and mirror them back into local MinIO:

```bash
./scripts/restore-from-baidupan.sh --upload-to-minio
```
