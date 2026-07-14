import { createReadStream, createWriteStream } from "node:fs";
import { mkdir, readFile } from "node:fs/promises";
import { dirname, resolve } from "node:path";
import { createInterface } from "node:readline/promises";
import { once } from "node:events";
import { createMinioSignedHttpClient } from "../web/backend/minio-signed-http.mjs";

const rootDir = resolve(new URL("..", import.meta.url).pathname);
const envPath = resolve(rootDir, ".env");
const progressPrefix = "HOME_MINIO_PROGRESS ";
const progressIntervalMs = 500;

function parseEnv(source) {
  const values = {};
  for (const line of source.split(/\r?\n/)) {
    const trimmed = line.trim();
    if (!trimmed || trimmed.startsWith("#") || !trimmed.includes("=")) continue;
    const index = line.indexOf("=");
    const key = line.slice(0, index).trim();
    let value = line.slice(index + 1).trim();
    if ((value.startsWith('"') && value.endsWith('"')) || (value.startsWith("'") && value.endsWith("'"))) {
      value = value.slice(1, -1);
    }
    values[key] = value;
  }
  return values;
}

async function loadEnv() {
  return parseEnv(await readFile(envPath, "utf8"));
}

function assertObjectKey(value) {
  const key = String(value || "").trim().replace(/\\/g, "/").replace(/^\/+/, "");
  if (!key || key.split("/").some((segment) => !segment || segment === "." || segment === "..")) {
    throw new Error(`Invalid objectKey: ${value}`);
  }
  return key;
}

function sourceUrlObjectKey(value) {
  try {
    const key = new URL(String(value || "")).pathname.replace(/^\/+/, "");
    return key.startsWith("local-media/") ? key.slice("local-media/".length) : key;
  } catch {
    return "";
  }
}

function recordSourceUrls(record) {
  const urls = [];
  if (Array.isArray(record.sourceUrls)) {
    for (const url of record.sourceUrls) {
      if (typeof url === "string" && url.trim()) {
        urls.push(url.trim());
      }
    }
  }
  if (typeof record.sourceUrl === "string" && record.sourceUrl.trim()) {
    urls.push(record.sourceUrl.trim());
  }
  return [...new Set(urls)];
}

function readInteger(value, fallback, { min, max }) {
  const parsed = Number.parseInt(String(value ?? ""), 10);
  if (!Number.isFinite(parsed)) return fallback;
  return Math.min(max, Math.max(min, parsed));
}

async function writeLine(stream, line) {
  if (!stream.write(`${line.replace(/\n+$/, "")}\n`, "utf8")) {
    await once(stream, "drain");
  }
}

function createMinioClient(env, bucket) {
  return createMinioSignedHttpClient(env, { bucket });
}

async function ensureBucket(client) {
  await client.ensureBucket();
}

async function headMinioObject(client, key) {
  const result = await client.headObject(key);
  return result ? { size: result.sizeBytes } : null;
}

async function uploadUrlToMinio(client, key, url) {
  const source = await fetch(url, { signal: AbortSignal.timeout(30 * 60 * 1000) });
  if (!source.ok || !source.body) {
    throw new Error(`Download failed ${source.status}: ${url}`);
  }
  const contentLength = source.headers.get("content-length");
  if (!contentLength || !Number.isFinite(Number(contentLength))) {
    throw new Error(`Source missing content-length: ${url}`);
  }
  const contentType = source.headers.get("content-type") || "application/octet-stream";
  await client.putObject(key, source.body, {
    contentLength: Number(contentLength),
    contentType,
  });
}

async function processRecord(record, { dryRun, minioClient }) {
  const key = assertObjectKey(record.objectKey);
  const sourceUrls = recordSourceUrls(record).filter((url) => sourceUrlObjectKey(url) === key);
  if (sourceUrls.length === 0) {
    console.error(`SKIP unsafe manifest record ${key} <- ${record.sourceUrl || ""}`);
    return { status: "rejected" };
  }
  const expectedSize = record.sizeBytes ? Number(record.sizeBytes) : null;
  if (dryRun) {
    console.log(`DRY ${record.storageProvider} ${key} <- ${sourceUrls[0]}`);
    return { status: "skipped" };
  }
  const existing = await headMinioObject(minioClient, key);
  if (existing && (!expectedSize || existing.size === expectedSize)) {
    console.log(`SKIP ${key}`);
    return { status: "skipped" };
  }

  console.log(`PUT ${record.storageProvider} ${key} <- ${sourceUrls[0]}`);

  const errors = [];
  try {
    for (const sourceUrl of sourceUrls) {
      try {
        await uploadUrlToMinio(minioClient, key, sourceUrl);
        return { status: "downloaded" };
      } catch (error) {
        const message = error instanceof Error ? error.message : String(error);
        errors.push(`${sourceUrl}: ${message}`);
        console.error(`FAILED ${key}: ${message}`);
      }
    }
  } catch (error) {
    errors.push(error instanceof Error ? error.message : String(error));
  }
  const message = errors.join(" | ");
  return {
    status: "failed",
    failedRecord: {
      objectKey: key,
      storageProvider: record.storageProvider,
      sourceUrl: sourceUrls[0],
      sourceUrls,
      message,
    },
  };
}

async function main() {
  const dryRun = process.argv.includes("--dry-run");
  const env = await loadEnv();
  const bucket = env.MINIO_BUCKET || "waule-media";
  const manifestPath = resolve(rootDir, env.MEDIA_PULL_MANIFEST_PATH || "./backup/newwaule-media-manifest.jsonl");
  const workDir = resolve(rootDir, env.MEDIA_PULL_WORK_DIR || "./backup/pull");
  const concurrency = readInteger(env.MEDIA_PULL_CONCURRENCY, 4, { min: 1, max: 32 });
  const minioClient = createMinioClient(env, bucket);
  if (!dryRun) {
    await ensureBucket(minioClient);
  }
  const seenKeys = new Set();
  const active = new Set();
  let records = 0;
  let uniqueRecords = 0;
  let downloaded = 0;
  let skipped = 0;
  let failed = 0;
  let rejected = 0;
  let failedPath = "";
  let failedStream = null;
  let lastProgressAt = 0;

  function emitProgress(force = false) {
    const now = Date.now();
    if (!force && now - lastProgressAt < progressIntervalMs) return;
    lastProgressAt = now;
    console.log(`${progressPrefix}${JSON.stringify({
      processed: downloaded + skipped + failed + rejected,
      downloaded,
      skipped,
      failed,
      rejected,
      records,
      uniqueRecords,
    })}`);
  }

  async function writeFailedRecord(record) {
    if (dryRun || !record) return;
    if (!failedStream) {
      failedPath = resolve(workDir, `failed-${Date.now()}.jsonl`);
      await mkdir(dirname(failedPath), { recursive: true });
      failedStream = createWriteStream(failedPath, { encoding: "utf8" });
    }
    await writeLine(failedStream, JSON.stringify(record));
  }

  async function consumeRecord(record) {
    const result = await processRecord(record, { dryRun, minioClient });
    if (result?.status === "downloaded") downloaded += 1;
    if (result?.status === "skipped") skipped += 1;
    if (result?.status === "failed") failed += 1;
    if (result?.status === "rejected") rejected += 1;
    if (result?.failedRecord) await writeFailedRecord(result.failedRecord);
    emitProgress();
  }

  emitProgress(true);

  const lineReader = createInterface({
    input: createReadStream(manifestPath, { encoding: "utf8" }),
    crlfDelay: Infinity,
  });

  for await (const rawLine of lineReader) {
    const line = rawLine.trim();
    if (!line) continue;
    const record = JSON.parse(line);
    if (!record.objectKey || recordSourceUrls(record).length === 0) continue;
    records += 1;
    const key = String(record.objectKey || "").trim().replace(/\\/g, "/").replace(/^\/+/, "");
    if (!key || seenKeys.has(key)) continue;
    seenKeys.add(key);
    uniqueRecords += 1;

    const promise = consumeRecord(record).finally(() => {
      active.delete(promise);
    });
    active.add(promise);
    if (active.size >= concurrency) {
      await Promise.race(active);
    }
  }

  await Promise.all(active);
  if (failedStream) {
    failedStream.end();
    await once(failedStream, "finish");
    console.error(`FAILED_RECORDS ${failedPath}`);
  }

  emitProgress(true);
  console.log(JSON.stringify({ dryRun, manifestPath, bucket, records, uniqueRecords, concurrency }, null, 2));
  console.log(JSON.stringify({ downloaded, skipped, failed, rejected }, null, 2));
}

main().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});
