# Home MinIO for NewWaule Cold Media

This project runs a private MinIO instance on the home Linux server. NewWaule uses it as cold media storage through the S3-compatible API.

## Recommended Network

Do not expose MinIO directly to the public internet. Put the home Linux server and the Hong Kong NewWaule server into the same private network with Tailscale or WireGuard, then fill the private endpoint into NewWaule admin:

```text
NewWaule 管理后台 -> 系统配置 -> Home MinIO 冷存储
Endpoint: http://100.x.y.z:19000
```

## Start

```bash
docker compose up -d --build
./scripts/healthcheck.sh
```

The stack can start without manually creating or editing `.env`. Open the Home MinIO console and complete configuration from the page. Docker-level fields marked `需重启` take effect after recreating the containers.

Default ports are five digits to avoid common local-service conflicts:

```text
MinIO API:      19000
MinIO Console:  19001
Web API:        19090
Web Console:    19091
```

Published ports listen on all local interfaces by default so the console and API are reachable from both the LAN and Tailscale:

```env
HOME_MINIO_BIND_ADDRESS=0.0.0.0
```

Do not configure router port forwarding for these ports. Restrict access with the host firewall to the LAN subnet and `tailscale0`. To expose the services on only one interface, replace `0.0.0.0` with that interface's LAN or Tailscale address; use `127.0.0.1` for local-only access.

Open the local management console:

```text
http://<home-server-ip>:19091
```

The web console is the configuration entry point. Settings are split into MinIO storage, NewWaule connection, media transfer, backup, and console pages; every field includes its purpose and restart requirement. Password and token fields use `留空不修改`. Port, account, and MinIO credential changes require restarting compose:

```bash
docker compose up -d --force-recreate
```

Lifecycle state and the OSS configuration received from NewWaule are stored only in the HomeServer SQLite database. The state directory and SQLite files use owner-only permissions. No additional encryption key or secret file is required, and credential values are never returned by the API or printed in lifecycle errors.

Set the MinIO address that NewWaule should use. In production this is usually a Tailscale/WireGuard address reachable from the Hong Kong server:

```env
HOME_MINIO_PUBLIC_ENDPOINT=http://100.x.y.z:19000
HOME_MINIO_CONSOLE_PUBLIC_URL=http://100.x.y.z:19001
```

`HOME_MINIO_PUBLIC_ENDPOINT` is copied into the NewWaule admin Home MinIO form as `Endpoint`. NewWaule users do not access this endpoint directly.

## NewWaule Admin Config

NewWaule no longer uses `HOME_MINIO_*` environment variables. Configure it in:

```text
管理后台 -> 系统配置 -> Home MinIO 冷存储
```

Required fields:

```text
启用: true
Endpoint: HOME_MINIO_PUBLIC_ENDPOINT, usually a Tailscale/WireGuard/private address
Region: us-east-1
Bucket: MINIO_BUCKET
AccessKeyId: MINIO_WAULE_ACCESS_KEY
SecretKey: MINIO_WAULE_SECRET_KEY
Path Style: true
缓存目录: storage/home-minio-cache
NewWaule 公网 URL: https://api.example.com
Home MinIO 管理 API: http://100.x.y.z:19090
管理 API 令牌: 与 Home MinIO 控制台 -> 控制台设置 -> 管理 API 令牌相同
```

`NewWaule 公网 URL` is the public NewWaule API root. Migrated media URLs become:

```text
https://api.example.com/local-media/<objectKey>
```

The file itself is stored in home MinIO. When users request the URL, NewWaule first checks its local cache directory. If the object is missing, NewWaule sends a small control request to the home-minio Web API, and home-minio actively pushes the object back to the NewWaule public API cache endpoint.

Connection direction:

```text
NewWaule API/worker -> home-minio Web API control request
home-minio -> NewWaule public API cache upload
NewWaule admin -> home-minio Web API archive control
home-minio pull script -> manifest sourceUrl
home-minio lifecycle worker -> Aliyun OSS
browser/user -> NewWaule /local-media/<objectKey>
```

Configure cache push on the `NewWaule 连接` page. These are page fields; users do not need to edit `.env`:

```text
NEWWAULE_API_BASE_URL=https://api.example.com
NEWWAULE_CACHE_UPLOAD_BASE_URL=
NEWWAULE_HOME_MINIO_TOKEN=<与控制台设置页的管理 API 令牌相同>
CACHE_PUSH_CONCURRENCY=4
```

`NEWWAULE_HOME_MINIO_TOKEN` should match the `管理 API 令牌` saved in NewWaule. `NEWWAULE_CACHE_UPLOAD_BASE_URL` can stay empty when NewWaule triggers the push, because NewWaule sends the exact upload endpoint. The MinIO S3 endpoint can still stay on Tailscale/WireGuard for private control and archive verification, but large cache transfers are pushed from home-minio to NewWaule over the public NewWaule API path.

## Pull Media From NewWaule Manifest

To avoid routing OSS migration traffic through the Hong Kong NewWaule server, export a manifest from NewWaule first:

```bash
pnpm --filter @waule/api media:migrate-home-minio -- --mode=export-manifest --source=ALL --older-than-days=15 > /tmp/newwaule-media-manifest.jsonl
```

Copy the manifest to the home server path configured by:

```env
MEDIA_PULL_MANIFEST_PATH=./backup/newwaule-media-manifest.jsonl
MEDIA_PULL_WORK_DIR=./backup/pull
MEDIA_PULL_CONCURRENCY=4
MINIO_INTERNAL_ENDPOINT=http://minio:9000
```

Then pull media on the home server:

```bash
node scripts/pull-media-manifest-to-minio.mjs --dry-run
node scripts/pull-media-manifest-to-minio.mjs
```

`MEDIA_PULL_CONCURRENCY` controls how many files are downloaded and uploaded to MinIO at the same time. Start with `4`; if the home server, OSS/NewWaule source, and network are stable, `8` or `16` can improve throughput. The script streams each source URL directly into the MinIO bucket and only uses `MEDIA_PULL_WORK_DIR` for failed-record logs.

After NewWaule has `Home MinIO 管理 API` and `管理 API 令牌` configured, local-to-OSS lifecycle transfers use the two category-specific controls:

```text
管理后台 -> 系统设置 -> 上传媒体存储 -> 预览待处理 / 立即执行一次
管理后台 -> 系统设置 -> 生成媒体存储 -> 预览待处理 / 立即执行一次
```

Each category action submits at most 100 physical objects. NewWaule persists the run and Home job ID, while Home MinIO persists item state in SQLite so process restarts can resume safely.

Media that was stored directly in OSS can be moved to cold storage manually:

```text
管理后台 -> 系统设置 -> Home MinIO 冷存储 -> 手动归档 OSS 冷文件
```

This action includes only workflow uploads and generated media whose age has reached NewWaule's global `冷存储天数`. Home MinIO pulls each OSS/CDN source directly, verifies the local copy, and then NewWaule switches database references to `/local-media/{objectKey}` before deleting the matching OSS source. Avatars, invoices, director assets, tone/lighting assets, canvas style presets, templates, and ambiguous keys are excluded.

Before each lifecycle batch, NewWaule sends the current OSS bucket, region, endpoint, AccessKey, secret, and optional CDN base URL. Home MinIO fingerprints the complete config, reuses an identical immutable version, stores it in the local owner-only SQLite database, and pins running jobs to that version. Responses never contain plaintext secrets.

Default transfer tuning is:

```env
MEDIA_PULL_CONCURRENCY=4
OSS_FILE_CONCURRENCY=8
OSS_MULTIPART_CONCURRENCY=4
OSS_MAX_HTTP_CONCURRENCY=16
OSS_MULTIPART_THRESHOLD_BYTES=67108864
OSS_PART_SIZE_BYTES=16777216
```

Small objects stream concurrently from MinIO to OSS. Large objects are staged in `HOME_MINIO_TRANSFER_WORK_DIR`, verified, then uploaded with Aliyun Multipart Upload. SQLite persists the upload ID/checkpoint and part ETags for restart recovery.

The manifest endpoints and scripts below remain available as compatibility tools for old manual archive records. They are not the scheduler for the new warm/cold lifecycle.

For large archives, NewWaule uploads the manifest as `application/x-ndjson`; home-minio writes the request body directly to `MEDIA_PULL_MANIFEST_PATH` and the pull script reads it line by line. The older JSON `{ "manifest": "..." }` upload format is still accepted for compatibility.

While a pull job is running, `/api/actions/pull-manifest-status` exposes in-memory `processed`, `downloaded`, `skipped`, `failed`, and `rejected` counters. The pull process emits a compact progress record at most twice per second, so NewWaule can update Admin progress without scanning MinIO or reading the manifest again.

Legacy manifest OSS records are downloaded from the `sourceUrl` generated by NewWaule. New lifecycle jobs receive a versioned Aliyun OSS configuration from NewWaule and upload warm media directly from Home MinIO to OSS. LOCAL records are downloaded from the NewWaule public `/local-media/<objectKey>` URL, so large file transfer uses the NewWaule public network path rather than Tailscale.

If old database rows still point to OSS objects that were already deleted, those rows will return `404`. The pull script records them in `MEDIA_PULL_WORK_DIR/failed-*.jsonl` and still uploads every successfully downloaded file into MinIO, so stale rows do not block valid media from being archived. After pull finishes, update NewWaule database without downloading files through the Hong Kong server:

```bash
pnpm --filter @waule/api media:migrate-home-minio -- --mode=verify-home --source=ALL --older-than-days=15 --execute
```

This workflow first verifies that the object exists in Home MinIO, then updates NewWaule database records to point user-facing URLs at `/local-media/<objectKey>`. To actually reduce OSS storage cost or free NewWaule server disk, run the explicit source cleanup step afterward:

```bash
pnpm --filter @waule/api media:migrate-home-minio -- --mode=cleanup-source --source=ALL --older-than-days=15 --execute
```

`cleanup-source` only processes records that already point to `HOME_MINIO`. It checks Home MinIO again before deleting the previous OSS object or previous NewWaule local file, then marks the asset metadata as `homeMinioSourceDeleted=true`.

## Migrate Existing OSS Files

Use the manifest workflow so OSS traffic goes from OSS directly to the home server:

```bash
pnpm --filter @waule/api media:migrate-home-minio -- --mode=export-manifest --source=OSS --older-than-days=15 > /tmp/newwaule-oss-media.jsonl
node scripts/pull-media-manifest-to-minio.mjs
pnpm --filter @waule/api media:migrate-home-minio -- --mode=verify-home --source=OSS --older-than-days=15 --execute
pnpm --filter @waule/api media:migrate-home-minio -- --mode=cleanup-source --source=OSS --older-than-days=15 --execute
```

Use `--source=ALL` to include both old OSS assets and new local assets.

## Cron Example

```cron
15 3 * * * cd /home/New-Waule && pnpm --filter @waule/api media:migrate-home-minio -- --mode=export-manifest --source=ALL --older-than-days=15 > /tmp/newwaule-media-manifest.jsonl
30 3 * * * cd /home/home-minio && node scripts/pull-media-manifest-to-minio.mjs >> /var/log/home-minio-pull.log 2>&1
50 3 * * * cd /home/New-Waule && pnpm --filter @waule/api media:migrate-home-minio -- --mode=verify-home --source=ALL --older-than-days=15 --execute >> /var/log/newwaule-home-minio-verify.log 2>&1
10 4 * * * cd /home/New-Waule && pnpm --filter @waule/api media:migrate-home-minio -- --mode=cleanup-source --source=ALL --older-than-days=15 --execute >> /var/log/newwaule-home-minio-cleanup.log 2>&1
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

The Docker web API and backup scheduler install `BaiduPCS-Go` inside their containers at startup. They also mount the host login config into `/root/.config/BaiduPCS-Go` by default. If you logged in as `root`, keep:

```env
BAIDUPCS_CONFIG_DIR=/root/.config/BaiduPCS-Go
```

If you logged in as another Linux user, point `BAIDUPCS_CONFIG_DIR` to that user's BaiduPCS-Go config directory, for example `/home/your-user/.config/BaiduPCS-Go`. After changing this value, recreate the backup containers:

```bash
docker compose up -d --force-recreate web-api backup-scheduler
docker exec -it home-minio-web-api BaiduPCS-Go who
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

Open `备份与恢复` in the Home MinIO console and configure:

```env
BAIDUPAN_BACKUP_ENABLED=true
BAIDUPAN_TOOL=baidupcs
BAIDUPAN_REMOTE_DIR=NewWaule/home-minio
BAIDUPAN_WORK_DIR=./backup
BAIDUPAN_CRON_SCHEDULE=35 3 * * *
BYPY_BIN=bypy
BAIDUPCS_BIN=BaiduPCS-Go
BAIDUPCS_CONFIG_DIR=/root/.config/BaiduPCS-Go
BAIDUPCS_MAX_PARALLEL=16
BAIDUPCS_UPLOAD_NORAPID=true
```

`BAIDUPCS_UPLOAD_NORAPID=true` is recommended. Some valid BaiduPCS-Go sessions can run `who` and `quota`, but the rapid-upload metadata API still fails with `获取用户uk错误` or `请确保登录信息包含了STOKEN`. Skipping rapid upload avoids that API path and uses normal file upload.

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
