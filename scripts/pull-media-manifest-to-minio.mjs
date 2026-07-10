import { createHash, createHmac } from "node:crypto";
import { createReadStream, createWriteStream } from "node:fs";
import { mkdir, readFile } from "node:fs/promises";
import { dirname, resolve } from "node:path";
import { createInterface } from "node:readline/promises";
import { once } from "node:events";

const rootDir = resolve(new URL("..", import.meta.url).pathname);
const envPath = resolve(rootDir, ".env");
const emptyPayloadHash = createHash("sha256").update("").digest("hex");
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

function encodePathSegment(value) {
  return encodeURIComponent(value).replace(/[!'()*]/g, (char) => `%${char.charCodeAt(0).toString(16).toUpperCase()}`);
}

function encodeObjectKeyPath(key) {
  return key.split("/").map(encodePathSegment).join("/");
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

function formatAmzDate(date) {
  return date.toISOString().replace(/[:-]|\.\d{3}/g, "");
}

function formatDateStamp(date) {
  return formatAmzDate(date).slice(0, 8);
}

function hmac(key, value, encoding) {
  return createHmac("sha256", key).update(value).digest(encoding);
}

function s3SigningKey(secretAccessKey, dateStamp, region) {
  const kDate = hmac(`AWS4${secretAccessKey}`, dateStamp);
  const kRegion = hmac(kDate, region);
  const kService = hmac(kRegion, "s3");
  return hmac(kService, "aws4_request");
}

function buildS3Request(params) {
  const endpoint = new URL(params.endpoint.replace(/\/+$/, ""));
  const date = new Date();
  const amzDate = formatAmzDate(date);
  const dateStamp = formatDateStamp(date);
  const canonicalUri = params.key
    ? `/${encodePathSegment(params.bucket)}/${encodeObjectKeyPath(params.key)}`
    : `/${encodePathSegment(params.bucket)}`;
  const url = new URL(canonicalUri, `${endpoint.origin}/`);
  const payloadHash = params.payloadHash || emptyPayloadHash;
  const signingHeaders = {
    host: endpoint.host,
    "x-amz-content-sha256": payloadHash,
    "x-amz-date": amzDate,
    ...(params.headers || {}),
  };
  const canonicalEntries = Object.entries(signingHeaders)
    .map(([key, value]) => [key.toLowerCase(), String(value).trim().replace(/\s+/g, " ")])
    .sort(([left], [right]) => left.localeCompare(right));
  const canonicalHeaders = canonicalEntries.map(([key, value]) => `${key}:${value}\n`).join("");
  const signedHeaders = canonicalEntries.map(([key]) => key).join(";");
  const canonicalRequest = [
    params.method,
    canonicalUri,
    "",
    canonicalHeaders,
    signedHeaders,
    payloadHash,
  ].join("\n");
  const credentialScope = `${dateStamp}/${params.region}/s3/aws4_request`;
  const stringToSign = [
    "AWS4-HMAC-SHA256",
    amzDate,
    credentialScope,
    createHash("sha256").update(canonicalRequest).digest("hex"),
  ].join("\n");
  const signature = hmac(s3SigningKey(params.secretAccessKey, dateStamp, params.region), stringToSign, "hex");
  const headers = {
    ...Object.fromEntries(canonicalEntries.filter(([key]) => key !== "host")),
    Authorization: `AWS4-HMAC-SHA256 Credential=${params.accessKeyId}/${credentialScope}, SignedHeaders=${signedHeaders}, Signature=${signature}`,
  };
  return { url, headers };
}

async function signedS3Fetch(params) {
  const request = buildS3Request(params);
  return fetch(request.url, {
    method: params.method,
    headers: request.headers,
    body: params.body,
    ...(params.body ? { duplex: "half" } : {}),
    signal: AbortSignal.timeout(params.timeoutMs || 30 * 60 * 1000),
  });
}

function createMinioClient(env, bucket) {
  const accessKeyId = env.MINIO_WAULE_ACCESS_KEY || env.MINIO_ROOT_USER;
  const secretAccessKey = env.MINIO_WAULE_SECRET_KEY || env.MINIO_ROOT_PASSWORD;
  if (!accessKeyId || !secretAccessKey) {
    throw new Error("Missing MinIO credentials. Set MINIO_WAULE_ACCESS_KEY/MINIO_WAULE_SECRET_KEY or MINIO_ROOT_USER/MINIO_ROOT_PASSWORD.");
  }
  return {
    endpoint: env.MINIO_INTERNAL_ENDPOINT || env.MINIO_UPLOAD_ENDPOINT || "http://minio:9000",
    region: env.MINIO_REGION || "us-east-1",
    bucket,
    accessKeyId,
    secretAccessKey,
  };
}

async function ensureBucket(client) {
  const head = await signedS3Fetch({ ...client, method: "HEAD", timeoutMs: 10_000 });
  if (head.ok) return;
  if (head.status !== 404) {
    throw new Error(`MinIO bucket check failed ${head.status}: ${await head.text().catch(() => "")}`);
  }
  const put = await signedS3Fetch({ ...client, method: "PUT", timeoutMs: 30_000 });
  if (!put.ok) {
    throw new Error(`MinIO bucket create failed ${put.status}: ${await put.text().catch(() => "")}`);
  }
}

async function headMinioObject(client, key) {
  const response = await signedS3Fetch({ ...client, method: "HEAD", key, timeoutMs: 30_000 });
  if (response.status === 404) return null;
  if (!response.ok) {
    throw new Error(`MinIO HEAD failed ${response.status}: ${key}`);
  }
  const size = Number(response.headers.get("content-length"));
  return Number.isFinite(size) ? { size } : {};
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
  const upload = await signedS3Fetch({
    ...client,
    method: "PUT",
    key,
    body: source.body,
    payloadHash: "UNSIGNED-PAYLOAD",
    headers: {
      "content-length": contentLength,
      "content-type": contentType,
    },
    timeoutMs: 30 * 60 * 1000,
  });
  if (!upload.ok) {
    throw new Error(`MinIO PUT failed ${upload.status}: ${await upload.text().catch(() => "")}`);
  }
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
