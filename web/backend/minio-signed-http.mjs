import { createHash, createHmac } from "node:crypto";

const EMPTY_PAYLOAD_HASH = createHash("sha256").update("").digest("hex");
const DEFAULT_TIMEOUT_MS = 30 * 60 * 1000;

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

function combinedSignal(signal, timeoutMs) {
  const timeout = AbortSignal.timeout(timeoutMs);
  return signal ? AbortSignal.any([signal, timeout]) : timeout;
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
  const payloadHash = params.payloadHash || EMPTY_PAYLOAD_HASH;
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
  return {
    url,
    headers: {
      ...Object.fromEntries(canonicalEntries.filter(([key]) => key !== "host")),
      Authorization: `AWS4-HMAC-SHA256 Credential=${params.accessKeyId}/${credentialScope}, SignedHeaders=${signedHeaders}, Signature=${signature}`,
    },
  };
}

async function readErrorDetail(response) {
  const body = (await response.text().catch(() => "")).replace(/\s+/g, " ").trim().slice(0, 1000);
  const code = /<Code>([^<]+)<\/Code>/i.exec(body)?.[1] || "";
  const message = /<Message>([^<]+)<\/Message>/i.exec(body)?.[1] || "";
  return [code, message].filter(Boolean).join(": ") || body;
}

export class MinioHttpError extends Error {
  constructor(operation, statusCode, detail = "") {
    const suffix = detail ? ` ${detail}` : "";
    const authorizationHint = statusCode === 401 || statusCode === 403
      ? " Check MINIO_WAULE_ACCESS_KEY, MINIO_WAULE_SECRET_KEY, and rerun minio-init to attach the current policy."
      : "";
    super(`MinIO ${operation} failed with HTTP ${statusCode}.${suffix}${authorizationHint}`);
    this.name = "MinioHttpError";
    this.statusCode = statusCode;
  }
}

export function createMinioSignedHttpClient(env, options = {}) {
  const accessKeyId = options.accessKeyId || env.MINIO_WAULE_ACCESS_KEY || env.MINIO_ROOT_USER;
  const secretAccessKey = options.secretAccessKey || env.MINIO_WAULE_SECRET_KEY || env.MINIO_ROOT_PASSWORD;
  if (!accessKeyId || !secretAccessKey) {
    throw new Error("MinIO credentials are not configured.");
  }
  const client = {
    endpoint: options.endpoint || env.MINIO_INTERNAL_ENDPOINT || env.MINIO_UPLOAD_ENDPOINT || "http://minio:9000",
    region: options.region || env.MINIO_REGION || "us-east-1",
    bucket: options.bucket || env.MINIO_BUCKET || "waule-media",
    accessKeyId,
    secretAccessKey,
    fetchImpl: options.fetchImpl || globalThis.fetch,
  };

  async function request({ method, key, body, payloadHash, headers, timeoutMs = DEFAULT_TIMEOUT_MS, signal }) {
    const signed = buildS3Request({ ...client, method, key, payloadHash, headers });
    return client.fetchImpl(signed.url, {
      method,
      headers: signed.headers,
      body,
      ...(body ? { duplex: "half" } : {}),
      signal: combinedSignal(signal, timeoutMs),
    });
  }

  async function assertOk(response, operation) {
    if (response.ok) return response;
    throw new MinioHttpError(operation, response.status, await readErrorDetail(response));
  }

  return {
    async ensureBucket({ signal } = {}) {
      const head = await request({ method: "HEAD", timeoutMs: 10_000, signal });
      if (head.ok) return;
      if (head.status !== 404) throw new MinioHttpError("bucket HEAD", head.status, await readErrorDetail(head));
      await assertOk(await request({ method: "PUT", timeoutMs: 30_000, signal }), "bucket create");
    },

    async headObject(key, { signal } = {}) {
      const response = await request({ method: "HEAD", key, timeoutMs: 30_000, signal });
      if (response.status === 404) return null;
      await assertOk(response, `HEAD ${key}`);
      const sizeBytes = Number(response.headers.get("content-length"));
      return {
        sizeBytes: Number.isSafeInteger(sizeBytes) && sizeBytes >= 0 ? sizeBytes : 0,
        etag: response.headers.get("etag"),
        lastModified: response.headers.get("last-modified"),
        sha256: response.headers.get("x-amz-meta-sha256"),
        contentType: response.headers.get("content-type"),
      };
    },

    async getObject(key, { signal } = {}) {
      const response = await assertOk(
        await request({ method: "GET", key, signal }),
        `GET ${key}`,
      );
      return {
        body: response.body,
        sizeBytes: Number(response.headers.get("content-length")),
        etag: response.headers.get("etag"),
        lastModified: response.headers.get("last-modified"),
        sha256: response.headers.get("x-amz-meta-sha256"),
        contentType: response.headers.get("content-type"),
      };
    },

    async putObject(key, body, { contentLength, contentType, metadata, signal } = {}) {
      const headers = {
        "content-length": String(contentLength),
        "content-type": contentType || "application/octet-stream",
        ...Object.fromEntries(
          Object.entries(metadata || {}).map(([name, value]) => [`x-amz-meta-${name.toLowerCase()}`, String(value)]),
        ),
      };
      const response = await assertOk(
        await request({ method: "PUT", key, body, payloadHash: "UNSIGNED-PAYLOAD", headers, signal }),
        `PUT ${key}`,
      );
      return { etag: response.headers.get("etag") };
    },

    destroy() {},
  };
}
