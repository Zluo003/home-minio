import { createHash } from "node:crypto";
import { createReadStream, createWriteStream } from "node:fs";
import { mkdir, rm, stat } from "node:fs/promises";
import { dirname, resolve } from "node:path";
import { createInterface } from "node:readline";
import { Readable, Transform } from "node:stream";
import { pipeline } from "node:stream/promises";
import OSS from "ali-oss";
import { createMinioSignedHttpClient } from "./minio-signed-http.mjs";

// One initial attempt plus three retries. A failed item must not block the rest of the run.
const MAX_ITEM_ATTEMPTS = 4;
const MAX_RETRY_DELAY_MS = 6 * 60 * 60 * 1000;
const DEFAULT_REQUEST_TIMEOUT_MS = 30 * 60 * 1000;
const PERMANENT_HTTP_STATUSES = new Set([400, 401, 403, 404, 410]);

export class PermanentLifecycleTransferError extends Error {
  constructor(message, statusCode = null, failureKind = null) {
    super(message);
    this.name = "PermanentLifecycleTransferError";
    this.statusCode = statusCode;
    this.failureKind = failureKind;
  }
}

export function isPermanentLifecycleTransferError(error) {
  if (error instanceof PermanentLifecycleTransferError) return true;
  const status = Number(error?.status ?? error?.statusCode ?? error?.$metadata?.httpStatusCode);
  return Number.isInteger(status) && PERMANENT_HTTP_STATUSES.has(status);
}

function lifecycleFailureDetails(error) {
  const statusCode = Number(error?.status ?? error?.statusCode ?? error?.$metadata?.httpStatusCode);
  const normalizedStatus = Number.isInteger(statusCode) ? statusCode : null;
  if (error?.failureKind) {
    return { kind: String(error.failureKind), statusCode: normalizedStatus, retryable: false };
  }
  if (normalizedStatus === 404 || normalizedStatus === 410) {
    return { kind: "SOURCE_MISSING", statusCode: normalizedStatus, retryable: false };
  }
  if (normalizedStatus === 401 || normalizedStatus === 403) {
    return { kind: "ACCESS_DENIED", statusCode: normalizedStatus, retryable: false };
  }
  if (normalizedStatus === 400) {
    return { kind: "SOURCE_INVALID", statusCode: normalizedStatus, retryable: false };
  }
  return {
    kind: isPermanentLifecycleTransferError(error) ? "TRANSFER_REJECTED" : "TRANSFER_RETRYABLE",
    statusCode: normalizedStatus,
    retryable: !isPermanentLifecycleTransferError(error),
  };
}

function combinedSignal(signal, timeoutMs) {
  const timeout = AbortSignal.timeout(timeoutMs);
  return signal ? AbortSignal.any([signal, timeout]) : timeout;
}

function throwIfAborted(signal) {
  if (!signal?.aborted) return;
  throw signal.reason instanceof Error ? signal.reason : new Error("Lifecycle transfer cancelled.");
}

function readInteger(value, fallback, { min, max }) {
  const parsed = Number.parseInt(String(value ?? ""), 10);
  if (!Number.isFinite(parsed)) return fallback;
  return Math.min(max, Math.max(min, parsed));
}

function delay(ms) {
  return new Promise((resolveDelay) => setTimeout(resolveDelay, ms));
}

function scheduleIoTurn(callback) {
  const immediate = setImmediate(callback);
  immediate.unref?.();
}

function normalizeEtag(value) {
  return typeof value === "string" ? value.replace(/^"|"$/g, "") : null;
}

function encodeObjectKey(key) {
  return key.split("/").map((segment) => encodeURIComponent(segment).replace(/[!'()*]/g, (char) => `%${char.charCodeAt(0).toString(16).toUpperCase()}`)).join("/");
}

function decodePathname(pathname) {
  try {
    return decodeURIComponent(pathname);
  } catch {
    return pathname;
  }
}

export function redactLifecycleMessage(message, sensitiveValues = []) {
  let redacted = String(message || "");
  for (const value of sensitiveValues) {
    const secret = String(value || "");
    if (secret.length >= 4) redacted = redacted.split(secret).join("[REDACTED]");
  }
  return redacted
    .replace(/\b(access[-_ ]?key(?:id|secret)?|authorization|secret(?:access)?key|token|password)\b\s*[:=]\s*[^\s,;]+/gi, "$1=[REDACTED]")
    .slice(0, 4000);
}

export function sourceUrlMatchesObjectKey(sourceUrl, objectKey) {
  try {
    const url = new URL(sourceUrl);
    if (url.protocol !== "http:" && url.protocol !== "https:") return false;
    const path = decodePathname(url.pathname).replace(/^\/+/, "");
    const candidate = path.startsWith("local-media/") ? path.slice("local-media/".length) : path;
    return candidate === objectKey || candidate.endsWith(`/${objectKey}`);
  } catch {
    return false;
  }
}

export function takeContiguousCallbackBatch(callbacks, limit = 100) {
  const first = callbacks[0];
  if (!first) return [];
  const batch = [first];
  const maxItems = Math.max(1, Math.min(100, limit));
  for (const callback of callbacks.slice(1)) {
    if (batch.length >= maxItems) break;
    const previous = batch[batch.length - 1];
    if (callback.runId !== first.runId || callback.sequence !== previous.sequence + 1) break;
    batch.push(callback);
  }
  return batch;
}

function nodeReadable(body) {
  if (!body) throw new Error("Object response body is empty.");
  if (body instanceof Readable) return body;
  if (typeof body.transformToWebStream === "function") {
    return Readable.fromWeb(body.transformToWebStream());
  }
  if (typeof body[Symbol.asyncIterator] === "function") return Readable.from(body);
  throw new Error("Object response body is not readable.");
}

class Semaphore {
  constructor(limit) {
    this.limit = Math.max(1, limit);
    this.active = 0;
    this.waiters = [];
  }

  drain() {
    for (let index = 0; index < this.waiters.length;) {
      const waiter = this.waiters[index];
      if (this.active + waiter.weight > this.limit) {
        index += 1;
        continue;
      }
      this.waiters.splice(index, 1);
      this.active += waiter.weight;
      waiter.resolve();
    }
  }

  async use(callback, requestedWeight = 1) {
    const weight = Math.max(1, Math.min(this.limit, requestedWeight));
    if (this.active + weight > this.limit || this.waiters.length > 0) {
      await new Promise((resolveWaiter) => {
        this.waiters.push({ weight, resolve: resolveWaiter });
        this.drain();
      });
    } else {
      this.active += weight;
    }
    try {
      return await callback();
    } finally {
      this.active -= weight;
      this.drain();
    }
  }
}

function createOssClient(config) {
  const endpoint = config.endpoint.replace(/^https?:\/\//i, "").replace(/\/+$/, "");
  return new OSS({
    region: config.region.startsWith("oss-") ? config.region : `oss-${config.region}`,
    endpoint: `https://${endpoint}`,
    accessKeyId: config.accessKeyId,
    accessKeySecret: config.accessKeySecret,
    bucket: config.bucket,
    secure: true,
    timeout: "30m",
    retryMax: 3,
  });
}

function buildDirectOssUrl(config, objectKey) {
  const endpoint = config.endpoint.replace(/^https?:\/\//i, "").replace(/\/+$/, "");
  const host = endpoint.startsWith(`${config.bucket}.`) ? endpoint : `${config.bucket}.${endpoint}`;
  return `https://${host}/${encodeObjectKey(objectKey)}`;
}

async function hashFile(filePath) {
  const hash = createHash("sha256");
  for await (const chunk of createReadStream(filePath)) hash.update(chunk);
  return hash.digest("hex");
}

function minioConnectionKey(env, bucket) {
  return JSON.stringify([
    env.MINIO_INTERNAL_ENDPOINT || env.MINIO_UPLOAD_ENDPOINT || "http://minio:9000",
    env.MINIO_REGION || "us-east-1",
    bucket,
    env.MINIO_WAULE_ACCESS_KEY || env.MINIO_ROOT_USER || "",
    env.MINIO_WAULE_SECRET_KEY || env.MINIO_ROOT_PASSWORD || "",
  ]);
}

export class LifecycleTransferService {
  constructor(options) {
    this.store = options.store;
    this.fetch = options.fetchImpl || globalThis.fetch;
    this.ossClientFactory = options.ossClientFactory || createOssClient;
    this.environmentProvider = options.environmentProvider || null;
    this.minioClientFactory = options.minioClientFactory
      || ((env, bucket) => createMinioSignedHttpClient(env, { bucket }));
    this.managedMinioClient = !options.minioClient;
    this.minioClient = options.minioClient || null;
    this.minioClientKey = options.minioClient ? "injected" : null;
    this.activeItemIds = new Set();
    this.activeObjectKeys = new Set();
    this.activeTransfers = new Map();
    this.activeManifestRuns = new Map();
    this.activeCallbackRuns = new Set();
    this.pendingEnvironment = null;
    this.byteSamples = [];
    this.timer = null;
    this.stopped = true;
    this.bucketReadyPromise = null;
    this.applyEnvironment(options.env || process.env);
  }

  applyEnvironment(env) {
    const nextEnv = { ...env };
    const nextBucket = nextEnv.MINIO_BUCKET || "waule-media";
    const nextClientKey = minioConnectionKey(nextEnv, nextBucket);
    if (this.managedMinioClient && this.minioClientKey !== nextClientKey) {
      this.minioClient?.destroy?.();
      this.minioClient = this.minioClientFactory(nextEnv, nextBucket);
      this.minioClientKey = nextClientKey;
      this.bucketReadyPromise = null;
    }
    this.env = nextEnv;
    this.bucket = nextBucket;
    this.workDir = resolve(nextEnv.HOME_MINIO_TRANSFER_WORK_DIR || "./transfer-work");
    this.pullConcurrency = readInteger(nextEnv.MEDIA_PULL_CONCURRENCY, 4, { min: 1, max: 32 });
    this.ossFileConcurrency = readInteger(nextEnv.OSS_FILE_CONCURRENCY, 8, { min: 1, max: 32 });
    this.multipartConcurrency = readInteger(nextEnv.OSS_MULTIPART_CONCURRENCY, 4, { min: 1, max: 16 });
    this.maxHttpConcurrency = readInteger(nextEnv.OSS_MAX_HTTP_CONCURRENCY, 16, { min: 1, max: 64 });
    this.multipartThresholdBytes = readInteger(nextEnv.OSS_MULTIPART_THRESHOLD_BYTES, 64 * 1024 * 1024, { min: 5 * 1024 * 1024, max: 5 * 1024 * 1024 * 1024 });
    this.partSizeBytes = readInteger(nextEnv.OSS_PART_SIZE_BYTES, 16 * 1024 * 1024, { min: 100 * 1024, max: 1024 * 1024 * 1024 });
    this.pullSemaphore = new Semaphore(this.pullConcurrency);
    this.ossSemaphore = new Semaphore(this.ossFileConcurrency);
    this.workerSemaphore = new Semaphore(this.maxHttpConcurrency);
    this.httpSemaphore = new Semaphore(this.maxHttpConcurrency);
  }

  reconfigure(env) {
    if (this.activeItemIds.size > 0 || this.activeTransfers.size > 0) {
      this.pendingEnvironment = { ...env };
      return false;
    }
    this.pendingEnvironment = null;
    this.applyEnvironment(env);
    return true;
  }

  async refreshEnvironment() {
    if (!this.environmentProvider) return;
    this.reconfigure(await this.environmentProvider());
  }

  applyPendingEnvironment() {
    if (!this.pendingEnvironment || this.activeItemIds.size > 0 || this.activeTransfers.size > 0) return;
    const nextEnv = this.pendingEnvironment;
    this.pendingEnvironment = null;
    this.applyEnvironment(nextEnv);
  }

  settings() {
    return {
      pullConcurrency: this.pullConcurrency,
      ossFileConcurrency: this.ossFileConcurrency,
      multipartConcurrency: this.multipartConcurrency,
      maxHttpConcurrency: this.maxHttpConcurrency,
      multipartThresholdBytes: this.multipartThresholdBytes,
      partSizeBytes: this.partSizeBytes,
    };
  }

  recordTransferredBytes(bytes) {
    if (!Number.isFinite(bytes) || bytes <= 0) return;
    const now = Date.now();
    this.byteSamples.push({ at: now, bytes });
    this.byteSamples = this.byteSamples.filter((sample) => sample.at >= now - 60_000);
  }

  telemetry() {
    const now = Date.now();
    this.byteSamples = this.byteSamples.filter((sample) => sample.at >= now - 60_000);
    const transferredBytesLastMinute = this.byteSamples.reduce((sum, sample) => sum + sample.bytes, 0);
    const oldestAt = this.byteSamples[0]?.at ?? now;
    const elapsedSeconds = Math.max(1, Math.min(60, (now - oldestAt) / 1000));
    return {
      activeItems: this.activeItemIds.size,
      activeObjects: this.activeObjectKeys.size,
      activeHttpRequests: this.httpSemaphore.active,
      transferredBytesLastMinute,
      throughputBytesPerSecond: transferredBytesLastMinute / elapsedSeconds,
    };
  }

  async ensureBucket(signal) {
    if (!this.bucketReadyPromise) {
      const pending = this.httpSemaphore.use(() => this.minioClient.ensureBucket({ signal }));
      const checked = pending.catch((error) => {
        if (this.bucketReadyPromise === checked) this.bucketReadyPromise = null;
        throw error;
      });
      this.bucketReadyPromise = checked;
    }
    return this.bucketReadyPromise;
  }

  start() {
    if (!this.stopped) return;
    this.stopped = false;
    void this.kick();
    this.timer = setInterval(() => void this.kick(), 10_000);
    this.timer.unref?.();
  }

  async stop() {
    this.stopped = true;
    if (this.timer) clearInterval(this.timer);
    this.timer = null;
    for (const controller of this.activeManifestRuns.values()) {
      controller.abort(new Error("Lifecycle service is stopping."));
    }
    for (const transfer of this.activeTransfers.values()) {
      transfer.controller.abort(new Error("Lifecycle service is stopping."));
    }
    while (this.activeItemIds.size > 0 || this.activeManifestRuns.size > 0 || this.activeCallbackRuns.size > 0) await delay(50);
    this.minioClient.destroy?.();
  }

  async kick() {
    if (this.stopped) return;
    try {
      await this.refreshEnvironment();
    } catch (error) {
      console.error(`[home-minio] lifecycle configuration refresh failed: ${error instanceof Error ? error.message : String(error)}`);
      return;
    }
    this.kickManifestRuns();
    this.kickCallbacks();
    const available = Math.max(0, this.maxHttpConcurrency - this.activeItemIds.size);
    if (!available) return;
    const runnable = this.store.listRunnableItems(available * 2);
    for (const candidate of runnable) {
      if (
        this.activeItemIds.has(candidate.id) ||
        this.activeObjectKeys.has(candidate.objectKey) ||
        this.activeItemIds.size >= this.maxHttpConcurrency
      ) continue;
      this.activeItemIds.add(candidate.id);
      this.activeObjectKeys.add(candidate.objectKey);
      void this.workerSemaphore.use(() => this.processItem(candidate.id))
        .catch((error) => console.error(`[home-minio] lifecycle item ${candidate.id} failed: ${error instanceof Error ? error.message : String(error)}`))
        .finally(() => {
          this.activeItemIds.delete(candidate.id);
          this.activeObjectKeys.delete(candidate.objectKey);
          this.applyPendingEnvironment();
          if (!this.stopped) scheduleIoTurn(() => void this.kick());
        });
    }
  }

  async submitJob(payload) {
    await this.refreshEnvironment();
    const job = this.store.createJob(payload);
    if (!this.stopped) void this.kick();
    return job;
  }

  async submitStreamingJob(payload) {
    await this.refreshEnvironment();
    const job = this.store.createStreamingJob(payload);
    if (!this.stopped) this.kickManifestRuns();
    return job;
  }

  getJobSummary(id) {
    return this.store.getJobSummary(id);
  }

  getJobSummaryWithDiagnostics(id) {
    return this.store.getJobSummaryWithDiagnostics(id);
  }

  kickManifestRuns() {
    if (this.stopped) return;
    for (const runId of this.store.listManifestRuns(4)) {
      if (this.activeManifestRuns.has(runId)) continue;
      const controller = new AbortController();
      this.activeManifestRuns.set(runId, controller);
      void this.ingestManifest(runId, controller.signal)
        .catch((error) => console.error(`[home-minio] lifecycle manifest ${runId} failed: ${error instanceof Error ? error.message : String(error)}`))
        .finally(() => {
          this.activeManifestRuns.delete(runId);
          if (!this.stopped) scheduleIoTurn(() => void this.kick());
        });
    }
  }

  async ingestManifest(runId, signal) {
    const control = this.store.claimManifestRun(runId);
    if (!control) return;
    try {
      throwIfAborted(signal);
      const response = await this.fetch(control.manifest_url, {
        headers: { "x-media-manifest-token": control.manifest_token },
        redirect: "follow",
        signal: combinedSignal(signal, DEFAULT_REQUEST_TIMEOUT_MS),
      });
      if (!response.ok || !response.body) {
        throw new PermanentLifecycleTransferError(
          `Manifest download failed with HTTP ${response.status}.`,
          response.status,
          response.status === 404 || response.status === 410 ? "MANIFEST_MISSING" : "MANIFEST_REJECTED",
        );
      }
      const reader = createInterface({ input: nodeReadable(response.body), crlfDelay: Infinity });
      let batch = [];
      for await (const rawLine of reader) {
        throwIfAborted(signal);
        const line = String(rawLine).trim();
        if (!line) continue;
        if (line.length > 2 * 1024 * 1024) throw new PermanentLifecycleTransferError("Manifest line exceeds 2 MiB.", 400, "MANIFEST_INVALID");
        let item;
        try {
          item = JSON.parse(line);
        } catch {
          throw new PermanentLifecycleTransferError("Manifest contains invalid JSON.", 400, "MANIFEST_INVALID");
        }
        batch.push(item);
        if (batch.length >= 500) {
          this.store.appendManifestItems(runId, batch);
          batch = [];
        }
      }
      if (batch.length) this.store.appendManifestItems(runId, batch);
      this.store.completeManifest(runId);
    } catch (error) {
      const details = lifecycleFailureDetails(error);
      this.store.failManifest(runId, error instanceof Error ? error.message : String(error), details.retryable);
      throw error;
    }
  }

  kickCallbacks() {
    if (this.stopped || this.activeCallbackRuns.size > 0) return;
    const runnable = this.store.listRunnableCallbacks(100);
    const first = runnable.find((item) => !this.activeCallbackRuns.has(item.runId));
    if (!first) return;
    const group = takeContiguousCallbackBatch(
      runnable.filter((item) => item.runId === first.runId),
      100,
    );
    const claimed = this.store.claimCallbacks(group.map((item) => item.id));
    if (!claimed.length) return;
    this.activeCallbackRuns.add(first.runId);
    void this.deliverCallbacks(claimed)
      .catch((error) => console.warn(
        `[home-minio] lifecycle callback ${first.runId} sequences ${claimed[0]?.sequence}-${claimed[claimed.length - 1]?.sequence} failed: ${error instanceof Error ? error.message : String(error)}`,
      ))
      .finally(() => {
        this.activeCallbackRuns.delete(first.runId);
        if (!this.stopped) scheduleIoTurn(() => this.kickCallbacks());
      });
  }

  async deliverCallbacks(callbacks) {
    const first = callbacks[0];
    try {
      const response = await this.fetch(first.callbackUrl, {
        method: "POST",
        signal: AbortSignal.timeout(30_000),
        headers: {
          "content-type": "application/json",
          "x-home-minio-token": first.callbackToken,
        },
        body: JSON.stringify({
          runId: first.runId,
          fromSequence: callbacks[0].sequence,
          toSequence: callbacks[callbacks.length - 1].sequence,
          items: callbacks.map((callback) => ({ sequence: callback.sequence, result: callback.payload })),
        }),
      });
      if (!response.ok) {
        const responseText = await response.text().catch(() => "");
        const details = redactLifecycleMessage(responseText, [first.callbackToken]).slice(0, 1000);
        throw new Error(`NewWaule callback failed with HTTP ${response.status}${details ? `: ${details}` : "."}`);
      }
      this.store.markCallbacksDelivered(callbacks.map((item) => item.id));
    } catch (error) {
      this.store.markCallbacksRetry(callbacks.map((item) => item.id), error instanceof Error ? error.message : String(error));
      throw error;
    }
  }

  getJob(id) {
    return this.store.getJob(id);
  }

  cancelJob(id) {
    const job = this.store.cancelJob(id);
    if (!job) return null;
    this.activeManifestRuns.get(id)?.abort(new Error("Lifecycle job cancelled."));
    for (const transfer of this.activeTransfers.values()) {
      if (transfer.jobId === id) transfer.controller.abort(new Error("Lifecycle job cancelled."));
    }
    return this.store.getJob(id);
  }

  cancelStreamingJob(id) {
    const job = this.store.cancelJob(id, "Cancelled by administrator", true);
    if (!job) return null;
    this.activeManifestRuns.get(id)?.abort(new Error("Lifecycle job cancelled."));
    for (const transfer of this.activeTransfers.values()) {
      if (transfer.jobId === id) transfer.controller.abort(new Error("Lifecycle job cancelled."));
    }
    return this.store.getJobSummary(id);
  }

  resumeJob(id) {
    const job = this.store.resumeJob(id);
    if (job && !this.stopped) void this.kick();
    return job;
  }

  resumeStreamingJob(id) {
    const job = this.store.resumeStreamingJob(id);
    if (job && !this.stopped) void this.kick();
    return job;
  }

  async headHomeObject(objectKey, signal) {
    return this.httpSemaphore.use(async () => {
      const result = await this.minioClient.headObject(objectKey, { signal });
      return result ? { ...result, etag: normalizeEtag(result.etag) } : null;
    });
  }

  async copySourceToHome(item, signal) {
    throwIfAborted(signal);
    await this.ensureBucket(signal);
    const existing = await this.headHomeObject(item.objectKey, signal);
    if (existing) {
      const expectedHomeSize = item.expectedSizeBytes ?? item.home?.sizeBytes ?? null;
      const expectedHomeSha256 = item.expectedSha256 || item.home?.sha256 || null;
      const hasVerificationEvidence = expectedHomeSize !== null || Boolean(expectedHomeSha256);
      const sizeMatches = expectedHomeSize === null || existing.sizeBytes === expectedHomeSize;
      const knownSha256 = expectedHomeSha256
        ? existing.sha256 || await this.hashHomeObject(item.objectKey, signal)
        : existing.sha256;
      const hashMatches = !expectedHomeSha256 || knownSha256 === expectedHomeSha256;
      if (hasVerificationEvidence && sizeMatches && hashMatches) {
        return { ...existing, sha256: knownSha256, reused: true };
      }
    }
    if (!sourceUrlMatchesObjectKey(item.sourceUrl, item.objectKey)) {
      throw new PermanentLifecycleTransferError("Source URL path does not match objectKey.", 400, "SOURCE_URL_MISMATCH");
    }

    const copied = await this.pullSemaphore.use(() => this.httpSemaphore.use(async () => {
      const response = await this.fetch(item.sourceUrl, {
        redirect: "follow",
        signal: combinedSignal(signal, DEFAULT_REQUEST_TIMEOUT_MS),
      });
      if (!response.ok || !response.body) {
        const message = `Source download failed with HTTP ${response.status}.`;
        if (PERMANENT_HTTP_STATUSES.has(response.status)) {
          throw new PermanentLifecycleTransferError(
            message,
            response.status,
            response.status === 404 || response.status === 410 ? "SOURCE_MISSING" : "SOURCE_ACCESS_REJECTED",
          );
        }
        throw new Error(message);
      }
      const contentLength = Number(response.headers.get("content-length"));
      if (!Number.isSafeInteger(contentLength) || contentLength < 0) {
        throw new PermanentLifecycleTransferError("Source response is missing a valid Content-Length.", 400, "SOURCE_METADATA_INVALID");
      }
      if (item.expectedSizeBytes != null && contentLength !== item.expectedSizeBytes) {
        throw new PermanentLifecycleTransferError(
          `Source size mismatch: expected ${item.expectedSizeBytes}, received ${contentLength}.`,
          400,
          "CONTENT_MISMATCH",
        );
      }

      const hash = createHash("sha256");
      let copiedBytes = 0;
      const recordTransferredBytes = (bytes) => this.recordTransferredBytes(bytes);
      const counter = new Transform({
        transform(chunk, _encoding, callback) {
          copiedBytes += chunk.length;
          hash.update(chunk);
          recordTransferredBytes(chunk.length);
          callback(null, chunk);
        },
      });
      const body = nodeReadable(response.body).pipe(counter);
      const upload = await this.minioClient.putObject(item.objectKey, body, {
        contentLength,
        contentType: item.mimeType || response.headers.get("content-type") || "application/octet-stream",
        signal,
      });
      if (copiedBytes !== contentLength) {
        throw new Error(`Home MinIO copy ended at ${copiedBytes} of ${contentLength} bytes.`);
      }
      const sha256 = hash.digest("hex");
      if (item.expectedSha256 && sha256 !== item.expectedSha256) {
        throw new PermanentLifecycleTransferError("Source SHA-256 does not match the expected checksum.", 400, "CONTENT_MISMATCH");
      }
      return { contentLength, uploadEtag: normalizeEtag(upload.etag), sha256 };
    }, 2));
    const verified = await this.headHomeObject(item.objectKey, signal);
    if (!verified || verified.sizeBytes !== copied.contentLength) {
      throw new Error("Home MinIO HEAD verification failed after upload.");
    }
    return { ...verified, etag: verified.etag || copied.uploadEtag, sha256: copied.sha256, reused: false };
  }

  async hashHomeObject(objectKey, signal) {
    return this.httpSemaphore.use(async () => {
      throwIfAborted(signal);
      const response = await this.minioClient.getObject(objectKey, { signal });
      const hash = createHash("sha256");
      for await (const chunk of nodeReadable(response.body)) {
        throwIfAborted(signal);
        hash.update(chunk);
      }
      return hash.digest("hex");
    });
  }

  meteredReadable(body) {
    const recordTransferredBytes = (bytes) => this.recordTransferredBytes(bytes);
    return nodeReadable(body).pipe(new Transform({
      transform(chunk, _encoding, callback) {
        recordTransferredBytes(chunk.length);
        callback(null, chunk);
      },
    }));
  }

  async stageHomeObject(item, expectedSize, expectedSha256, signal) {
    throwIfAborted(signal);
    const digest = createHash("sha256").update(`${item.id}\n${item.objectKey}`).digest("hex");
    const filePath = resolve(this.workDir, digest.slice(0, 2), digest);
    await mkdir(dirname(filePath), { recursive: true });
    const existing = await stat(filePath).catch(() => null);
    if (existing?.isFile() && existing.size === expectedSize) {
      const sha256 = await hashFile(filePath);
      if (!expectedSha256 || sha256 === expectedSha256) return { filePath, sha256 };
      await rm(filePath, { force: true });
    }

    return this.httpSemaphore.use(async () => {
      const response = await this.minioClient.getObject(item.objectKey, { signal });
      const tempPath = `${filePath}.${process.pid}.${Date.now()}.tmp`;
      const hash = createHash("sha256");
      let copiedBytes = 0;
      const counter = new Transform({
        transform(chunk, _encoding, callback) {
          copiedBytes += chunk.length;
          hash.update(chunk);
          callback(null, chunk);
        },
      });
      try {
        await pipeline(nodeReadable(response.body), counter, createWriteStream(tempPath, { flags: "wx" }), { signal });
        if (copiedBytes !== expectedSize) throw new Error(`Staged object size mismatch: ${copiedBytes} of ${expectedSize}.`);
        const sha256 = hash.digest("hex");
        if (expectedSha256 && sha256 !== expectedSha256) throw new Error("Staged object SHA-256 mismatch.");
        await rm(filePath, { force: true });
        const { rename } = await import("node:fs/promises");
        await rename(tempPath, filePath);
        return { filePath, sha256 };
      } catch (error) {
        await rm(tempPath, { force: true }).catch(() => undefined);
        throw error;
      }
    });
  }

  async headOssObject(client, objectKey, signal) {
    return this.httpSemaphore.use(async () => {
      try {
        throwIfAborted(signal);
        const result = await client.head(objectKey);
        return {
          sizeBytes: Number(result.res?.headers?.["content-length"] ?? result.meta?.["content-length"] ?? 0),
          etag: normalizeEtag(result.res?.headers?.etag),
          sha256: result.meta?.sha256 || result.res?.headers?.["x-oss-meta-sha256"] || null,
        };
      } catch (error) {
        if (error?.status === 404 || error?.code === "NoSuchKey") return null;
        throw error;
      }
    });
  }

  async validatePublicUrl(url, expectedSize, signal) {
    for (let attempt = 0; attempt < 3; attempt += 1) {
      throwIfAborted(signal);
      const head = await this.httpSemaphore.use(() => this.fetch(url, {
        method: "HEAD",
        redirect: "follow",
        signal: combinedSignal(signal, 10_000),
      }).catch(() => null));
      if (head?.ok) {
        const rawSize = head.headers.get("content-length");
        const size = rawSize === null ? Number.NaN : Number(rawSize);
        if (Number.isSafeInteger(size) && size === expectedSize) return true;
      }
      const range = await this.httpSemaphore.use(() => this.fetch(url, {
        method: "GET",
        headers: { Range: "bytes=0-0" },
        redirect: "follow",
        signal: combinedSignal(signal, 10_000),
      }).catch(() => null));
      if (range) {
        const contentRange = range.headers.get("content-range") || "";
        const rangeTotal = /^bytes\s+0-0\/(\d+)$/i.exec(contentRange)?.[1];
        const rawRangeSize = range.headers.get("content-length");
        const rangeSize = rawRangeSize === null ? Number.NaN : Number(rawRangeSize);
        const validPartial = range.status === 206 && rangeTotal !== undefined && Number(rangeTotal) === expectedSize;
        const validFull = range.status === 200 && Number.isSafeInteger(rangeSize) && rangeSize === expectedSize;
        await range.body?.cancel?.().catch?.(() => undefined);
        if (validPartial || validFull) return true;
      }
      throwIfAborted(signal);
      if (attempt < 2) await delay(1000 * (attempt + 1));
    }
    return false;
  }

  async uploadHomeObjectToOss(item, home, signal) {
    throwIfAborted(signal);
    const job = this.store.getJob(item.jobId);
    const config = this.store.getDecryptedConfig(job.configVersionId);
    const client = this.ossClientFactory(config);
    const cancelClient = () => {
      try {
        client.cancel?.();
      } catch {
        // The persisted CANCELLED state remains authoritative even if the client has no active request.
      }
    };
    signal?.addEventListener("abort", cancelClient, { once: true });
    try {
      const existing = await this.headOssObject(client, item.objectKey, signal);
      let etag = existing?.etag || null;
      let sha256 = home.sha256 || item.expectedSha256 || null;

      if (!existing || existing.sizeBytes !== home.sizeBytes || (sha256 && existing.sha256 !== sha256)) {
        await this.ossSemaphore.use(async () => {
          throwIfAborted(signal);
          if (home.sizeBytes >= this.multipartThresholdBytes) {
            const staged = await this.stageHomeObject(item, home.sizeBytes, sha256, signal);
            sha256 = staged.sha256;
            const checkpoint = this.store.getCheckpoint(item.id);
            let lastMultipartBytes = 0;
            const result = await this.httpSemaphore.use(() => client.multipartUpload(item.objectKey, staged.filePath, {
              parallel: this.multipartConcurrency,
              partSize: this.partSizeBytes,
              checkpoint: checkpoint && checkpoint.name === item.objectKey ? { ...checkpoint, file: staged.filePath } : undefined,
              mime: item.mimeType || "application/octet-stream",
              meta: sha256 ? { sha256 } : undefined,
              progress: async (percentage, nextCheckpoint) => {
                throwIfAborted(signal);
                const uploadedBytes = Math.max(0, Math.min(home.sizeBytes, Math.round(Number(percentage || 0) * home.sizeBytes)));
                this.recordTransferredBytes(Math.max(0, uploadedBytes - lastMultipartBytes));
                lastMultipartBytes = Math.max(lastMultipartBytes, uploadedBytes);
                if (!nextCheckpoint) return;
                const serializable = { ...nextCheckpoint, file: staged.filePath };
                this.store.updateItem(item.id, { checkpoint: serializable, stage: "OSS_UPLOADING" });
                this.store.replaceMultipartParts(item.id, nextCheckpoint.doneParts, nextCheckpoint.partSize);
              },
            }), this.multipartConcurrency);
            throwIfAborted(signal);
            etag = normalizeEtag(result.etag || result.res?.headers?.etag);
            await rm(staged.filePath, { force: true }).catch(() => undefined);
          } else {
            const result = await this.httpSemaphore.use(async () => {
              throwIfAborted(signal);
              const response = await this.minioClient.getObject(item.objectKey, { signal });
              return client.putStream(item.objectKey, this.meteredReadable(response.body), {
                contentLength: home.sizeBytes,
                mime: item.mimeType || response.contentType || "application/octet-stream",
                meta: sha256 ? { sha256 } : undefined,
              });
            }, 2);
            throwIfAborted(signal);
            etag = normalizeEtag(result.res?.headers?.etag);
          }
        });
      }

      const verified = await this.headOssObject(client, item.objectKey, signal);
      if (!verified || verified.sizeBytes !== home.sizeBytes || (sha256 && verified.sha256 !== sha256)) {
        throw new Error("OSS HEAD verification failed after upload.");
      }
      const directUrl = buildDirectOssUrl(config, item.objectKey);
      const directVerified = await this.validatePublicUrl(directUrl, home.sizeBytes, signal);
      if (!directVerified) throw new Error("Direct OSS public URL verification failed.");

      let selectedUrl = directUrl;
      let cdnVerified = false;
      let warning = null;
      if (config.publicBaseUrl) {
        const cdnUrl = `${config.publicBaseUrl.replace(/\/+$/, "")}/${encodeObjectKey(item.objectKey)}`;
        cdnVerified = await this.validatePublicUrl(cdnUrl, home.sizeBytes, signal);
        if (cdnVerified) selectedUrl = cdnUrl;
        else warning = "Configured CDN URL was unavailable; direct OSS URL was selected.";
      }

      return {
        bucket: config.bucket,
        sizeBytes: verified.sizeBytes,
        etag: verified.etag || etag,
        directUrl,
        selectedUrl,
        cdnVerified,
        warning,
      };
    } finally {
      signal?.removeEventListener("abort", cancelClient);
    }
  }

  retryAt(attemptCount) {
    const delayMs = Math.min(MAX_RETRY_DELAY_MS, 60_000 * 2 ** Math.max(0, attemptCount - 1));
    return new Date(Date.now() + delayMs).toISOString();
  }

  async processItem(itemId) {
    const item = this.store.claimItem(itemId);
    if (!item) return;
    const controller = new AbortController();
    this.activeTransfers.set(item.id, { jobId: item.jobId, controller });
    try {
      this.store.updateItem(item.id, { stage: "HOME_COPYING" });
      const home = await this.copySourceToHome(item, controller.signal);
      if (controller.signal.aborted || this.store.isItemCancelled(item.id)) return;
      const verifiedAt = new Date().toISOString();
      this.store.updateItem(item.id, {
        stage: "HOME_VERIFIED",
        homeSizeBytes: home.sizeBytes,
        homeEtag: home.etag,
        homeSha256: home.sha256,
        homeReused: home.reused,
        homeVerifiedAt: verifiedAt,
      });

      if (item.targetTier === "COLD_HOME_MINIO") {
        this.store.updateItem(item.id, {
          status: "SUCCEEDED",
          stage: "COMPLETED",
          checkpoint: null,
          finishedAt: new Date().toISOString(),
        });
        this.store.enqueueItemCallback(item.id);
        this.kickCallbacks();
        return;
      }

      if (controller.signal.aborted || this.store.isItemCancelled(item.id)) return;
      this.store.updateItem(item.id, { stage: "OSS_UPLOADING" });
      const oss = await this.uploadHomeObjectToOss(item, home, controller.signal);
      if (controller.signal.aborted || this.store.isItemCancelled(item.id)) return;
      this.store.updateItem(item.id, {
        status: "SUCCEEDED",
        stage: "COMPLETED",
        ossBucket: oss.bucket,
        ossSizeBytes: oss.sizeBytes,
        ossEtag: oss.etag,
        ossDirectUrl: oss.directUrl,
        ossSelectedUrl: oss.selectedUrl,
        ossCdnVerified: oss.cdnVerified,
        ossVerifiedAt: new Date().toISOString(),
        checkpoint: null,
        warning: oss.warning,
        finishedAt: new Date().toISOString(),
      });
      this.store.enqueueItemCallback(item.id);
      this.kickCallbacks();
    } catch (error) {
      if (controller.signal.aborted || this.store.isItemCancelled(item.id)) return;
      let ossConfig = null;
      try {
        const job = this.store.getJob(item.jobId);
        if (job?.configVersionId) ossConfig = this.store.getDecryptedConfig(job.configVersionId);
      } catch {
        ossConfig = null;
      }
      const message = redactLifecycleMessage(
        error instanceof Error ? error.message : String(error),
        [
          this.env.MINIO_WAULE_ACCESS_KEY,
          this.env.MINIO_WAULE_SECRET_KEY,
          this.env.MINIO_ROOT_USER,
          this.env.MINIO_ROOT_PASSWORD,
          this.env.HOME_MINIO_WEB_TOKEN,
          ossConfig?.accessKeyId,
          ossConfig?.accessKeySecret,
        ],
      );
      const failure = lifecycleFailureDetails(error);
      const failed = item.attemptCount >= MAX_ITEM_ATTEMPTS;
      const nextRetryAt = failed ? null : this.retryAt(item.attemptCount);
      this.store.updateItem(item.id, {
        status: failed ? "FAILED" : "RETRY_WAIT",
        stage: failed ? "FAILED" : "RETRY_WAIT",
        nextRetryAt,
        error: message.slice(0, 4000),
        failureKind: failure.kind,
        failureStatusCode: failure.statusCode,
        retryable: failed ? false : true,
        finishedAt: failed ? new Date().toISOString() : null,
      });
      if (failed) {
        this.store.enqueueItemCallback(item.id);
        this.kickCallbacks();
      }
      const level = failed ? "error" : "warn";
      console[level](`[home-minio] lifecycle item ${item.id} ${failed ? "failed" : `retry at ${nextRetryAt}`}: ${message}`);
    } finally {
      this.activeTransfers.delete(item.id);
    }
  }
}
