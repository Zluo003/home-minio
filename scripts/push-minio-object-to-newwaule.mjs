import { createHash, createHmac } from "node:crypto";
import { readFile } from "node:fs/promises";
import { resolve } from "node:path";

const rootDir = resolve(new URL("..", import.meta.url).pathname);
const envPath = resolve(rootDir, ".env");
const emptyPayloadHash = createHash("sha256").update("").digest("hex");

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
  return {
    ...parseEnv(await readFile(envPath, "utf8").catch(() => "")),
    ...process.env,
  };
}

function assertObjectKey(value) {
  const key = String(value || "").trim().replace(/\\/g, "/").replace(/^\/+/, "");
  if (!key || key.split("/").some((segment) => !segment || segment === "." || segment === "..")) {
    throw new Error(`Invalid objectKey: ${value}`);
  }
  return key;
}

function encodePathSegment(value) {
  return encodeURIComponent(value).replace(/[!'()*]/g, (char) => `%${char.charCodeAt(0).toString(16).toUpperCase()}`);
}

function encodeObjectKeyPath(key) {
  return key.split("/").map(encodePathSegment).join("/");
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

async function main() {
  const key = assertObjectKey(process.argv[2]);
  const env = await loadEnv();
  const bucket = env.MINIO_BUCKET || "waule-media";
  const newWauleApiUrl = String(env.NEWWAULE_API_BASE_URL || "").replace(/\/+$/, "");
  const cacheUploadBaseUrl = String(
    env.NEWWAULE_CACHE_UPLOAD_BASE_URL || (newWauleApiUrl ? `${newWauleApiUrl}/api/v1/home-minio/cache/objects` : ""),
  ).replace(/\/+$/, "");
  const token = String(env.NEWWAULE_HOME_MINIO_TOKEN || env.HOME_MINIO_WEB_TOKEN || "");
  if (!cacheUploadBaseUrl) {
    throw new Error("Missing NEWWAULE_CACHE_UPLOAD_BASE_URL or NEWWAULE_API_BASE_URL.");
  }
  if (!token) {
    throw new Error("Missing NEWWAULE_HOME_MINIO_TOKEN or HOME_MINIO_WEB_TOKEN.");
  }

  const minioClient = createMinioClient(env, bucket);
  const source = await signedS3Fetch({
    ...minioClient,
    method: "GET",
    key,
    timeoutMs: 30 * 60 * 1000,
  });
  if (!source.ok || !source.body) {
    throw new Error(`MinIO GET failed ${source.status}: ${key}`);
  }

  const contentType = source.headers.get("content-type") || "application/octet-stream";
  const contentLength = source.headers.get("content-length");
  const uploadHeaders = {
    "content-type": contentType,
    "x-home-minio-token": token,
    ...(contentLength ? { "content-length": contentLength } : {}),
  };
  const uploadUrl = `${cacheUploadBaseUrl}/${encodeObjectKeyPath(key)}`;
  console.log(`PUSH ${key} -> ${uploadUrl}`);
  const upload = await fetch(uploadUrl, {
    method: "PUT",
    headers: uploadHeaders,
    body: source.body,
    duplex: "half",
    signal: AbortSignal.timeout(30 * 60 * 1000),
  });
  if (!upload.ok) {
    throw new Error(`NewWaule cache upload failed ${upload.status}: ${await upload.text().catch(() => "")}`);
  }
  console.log(await upload.text());
}

main().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});
