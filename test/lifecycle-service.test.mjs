import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { Readable } from "node:stream";
import test from "node:test";
import {
  LifecycleTransferService,
  redactLifecycleMessage,
  sourceUrlMatchesObjectKey,
} from "../web/backend/lifecycle-service.mjs";
import { LifecycleStore } from "../web/backend/lifecycle-store.mjs";

class FakeMinioClient {
  constructor() {
    this.objects = new Map();
  }

  async send(command) {
    const name = command.constructor.name;
    const input = command.input;
    if (name === "HeadBucketCommand" || name === "CreateBucketCommand") return {};
    if (name === "HeadObjectCommand") {
      const value = this.objects.get(input.Key);
      if (!value) {
        const error = new Error("not found");
        error.$metadata = { httpStatusCode: 404 };
        throw error;
      }
      return { ContentLength: value.length, ETag: `"home-${value.length}"`, Metadata: {} };
    }
    if (name === "PutObjectCommand") {
      const chunks = [];
      for await (const chunk of input.Body) chunks.push(Buffer.from(chunk));
      const value = Buffer.concat(chunks);
      this.objects.set(input.Key, value);
      return { ETag: `"home-${value.length}"` };
    }
    if (name === "GetObjectCommand") {
      const value = this.objects.get(input.Key);
      if (!value) throw new Error("missing fake object");
      return { Body: Readable.from(value), ContentLength: value.length, ContentType: "application/octet-stream" };
    }
    throw new Error(`Unexpected MinIO command ${name}`);
  }

  destroy() {}
}

function createFakeOssClient(objects, metadata, multipartCalls, multipartControl) {
  return {
    async head(key) {
      const value = objects.get(key);
      if (!value) {
        const error = new Error("not found");
        error.status = 404;
        throw error;
      }
      return {
        meta: metadata.get(key) || {},
        res: { headers: { "content-length": String(value.length), etag: `"oss-${value.length}"` } },
      };
    },
    async putStream(key, body, options) {
      const chunks = [];
      for await (const chunk of body) chunks.push(Buffer.from(chunk));
      const value = Buffer.concat(chunks);
      objects.set(key, value);
      metadata.set(key, options?.meta || {});
      return { res: { headers: { etag: `"oss-${value.length}"` } } };
    },
    async multipartUpload(key, filePath, options) {
      const { readFile } = await import("node:fs/promises");
      const value = await readFile(filePath);
      const checkpoint = {
        file: filePath,
        name: key,
        fileSize: value.length,
        partSize: options.partSize,
        uploadId: "upload-1",
        doneParts: [{ number: 1, etag: "part-1" }],
      };
      await options.progress(0.5, checkpoint);
      multipartControl.attempts += 1;
      multipartCalls.push({
        key,
        parallel: options.parallel,
        partSize: options.partSize,
        checkpointUploadId: options.checkpoint?.uploadId || null,
      });
      if (multipartControl.failFirst && multipartControl.attempts === 1) {
        throw new Error("injected multipart interruption");
      }
      objects.set(key, value);
      metadata.set(key, options.meta || {});
      return { etag: `oss-${value.length}`, res: { headers: { etag: `"oss-${value.length}"` } } };
    },
  };
}

async function setupService({
  body,
  threshold = 64 * 1024 * 1024,
  fetchDelayMs = 0,
  failFirstMultipart = false,
  cdnUnavailable = false,
  maxHttpConcurrency = 16,
}) {
  const root = await mkdtemp(join(tmpdir(), "home-minio-service-"));
  const store = new LifecycleStore({ dbPath: join(root, "state.sqlite"), encryptionKey: Buffer.alloc(32, 9) });
  const minio = new FakeMinioClient();
  const ossObjects = new Map();
  const ossMetadata = new Map();
  const multipartCalls = [];
  const multipartControl = { attempts: 0, failFirst: failFirstMultipart };
  const stats = { sourceFetchCount: 0 };
  const fetchImpl = async (url, options = {}) => {
    if (String(url).startsWith("https://source.example.test/")) {
      stats.sourceFetchCount += 1;
      if (fetchDelayMs > 0) await new Promise((resolveDelay) => setTimeout(resolveDelay, fetchDelayMs));
      return new Response(body, { headers: { "content-length": String(body.length), "content-type": "image/png" } });
    }
    if (cdnUnavailable && String(url).startsWith("https://cdn.example.test/")) {
      return new Response(null, { status: 503 });
    }
    if (options.method === "HEAD") {
      return new Response(null, { status: 200, headers: { "content-length": String(body.length) } });
    }
    return new Response(Buffer.from([body[0] ?? 0]), { status: 206, headers: { "content-range": `bytes 0-0/${body.length}` } });
  };
  const service = new LifecycleTransferService({
    store,
    minioClient: minio,
    fetchImpl,
    ossClientFactory: () => createFakeOssClient(ossObjects, ossMetadata, multipartCalls, multipartControl),
    env: {
      MINIO_BUCKET: "media",
      HOME_MINIO_TRANSFER_WORK_DIR: join(root, "work"),
      MEDIA_PULL_CONCURRENCY: "4",
      OSS_FILE_CONCURRENCY: "8",
      OSS_MAX_HTTP_CONCURRENCY: String(maxHttpConcurrency),
      OSS_MULTIPART_CONCURRENCY: "4",
      OSS_MULTIPART_THRESHOLD_BYTES: String(threshold),
      OSS_PART_SIZE_BYTES: String(1024 * 1024),
    },
  });
  return { root, store, service, minio, ossObjects, multipartCalls, stats };
}

async function cleanup(context) {
  await context.service.stop();
  context.store.close();
  await rm(context.root, { recursive: true, force: true });
}

test("source URL validation requires an exact object key path", () => {
  assert.equal(sourceUrlMatchesObjectKey("https://api.test/local-media/gateway-media/a.png", "gateway-media/a.png"), true);
  assert.equal(sourceUrlMatchesObjectKey("https://api.test/local-media/gateway-media/other.png", "gateway-media/a.png"), false);
  assert.equal(sourceUrlMatchesObjectKey("file:///tmp/a.png", "a.png"), false);
  assert.equal(sourceUrlMatchesObjectKey("https://cdn.test/media-prefix/gateway-media/a.png", "gateway-media/a.png"), true);
});

test("lifecycle error messages redact exact credentials and credential-shaped fields", () => {
  const message = redactLifecycleMessage(
    "request failed accessKeySecret=oss-super-secret Authorization:BearerToken token=home-token",
    ["oss-super-secret", "home-token"],
  );
  assert.equal(message.includes("oss-super-secret"), false);
  assert.equal(message.includes("home-token"), false);
  assert.equal(message.includes("BearerToken"), false);
});

test("an existing same-size Home object is overwritten when its SHA-256 is wrong", async () => {
  const body = Buffer.from("correct-media");
  const context = await setupService({ body });
  try {
    const objectKey = "gateway-media/hash-checked.png";
    context.minio.objects.set(objectKey, Buffer.from("wrong--media!"));
    const job = context.store.createJob({
      id: "hash-repair-run",
      mediaKind: "GENERATED_MEDIA",
      items: [{
        lifecycleObjectId: "media-hash-repair",
        objectKey,
        sourceUrl: `https://source.example.test/local-media/${objectKey}`,
        targetTier: "COLD_HOME_MINIO",
        expectedSizeBytes: body.length,
        expectedSha256: createHash("sha256").update(body).digest("hex"),
        mimeType: "image/png",
      }],
    });

    await context.service.processItem(job.items[0].id);
    assert.equal(context.store.getJob(job.id).status, "SUCCEEDED");
    assert.deepEqual(context.minio.objects.get(objectKey), body);
    assert.equal(context.stats.sourceFetchCount, 1);
  } finally {
    await cleanup(context);
  }
});

test("an unverified existing Home object is replaced when source size and hash are unknown", async () => {
  const body = Buffer.from("authoritative-source");
  const context = await setupService({ body });
  try {
    const objectKey = "gateway-media/unverified-existing.png";
    context.minio.objects.set(objectKey, Buffer.from("unverified-home-object"));
    const job = context.store.createJob({
      id: "unverified-existing-run",
      mediaKind: "GENERATED_MEDIA",
      items: [{
        lifecycleObjectId: "media-unverified-existing",
        objectKey,
        sourceUrl: `https://source.example.test/local-media/${objectKey}`,
        targetTier: "COLD_HOME_MINIO",
        expectedSizeBytes: null,
        expectedSha256: null,
        mimeType: "image/png",
      }],
    });

    await context.service.processItem(job.items[0].id);
    assert.equal(context.store.getJob(job.id).status, "SUCCEEDED");
    assert.deepEqual(context.minio.objects.get(objectKey), body);
    assert.equal(context.stats.sourceFetchCount, 1);
  } finally {
    await cleanup(context);
  }
});

test("restart recovery rehashes a same-size Home object instead of trusting the persisted hash", async () => {
  const body = Buffer.from("verified-home-copy");
  const context = await setupService({ body });
  try {
    const objectKey = "gateway-media/recovered-hash.png";
    const expectedSha256 = createHash("sha256").update(body).digest("hex");
    context.minio.objects.set(objectKey, Buffer.from("tampered-home-copy"));
    const job = context.store.createJob({
      id: "recovered-hash-run",
      mediaKind: "GENERATED_MEDIA",
      items: [{
        lifecycleObjectId: "media-recovered-hash",
        objectKey,
        sourceUrl: `https://source.example.test/local-media/${objectKey}`,
        targetTier: "COLD_HOME_MINIO",
        expectedSizeBytes: body.length,
        mimeType: "image/png",
      }],
    });
    context.store.updateItem(job.items[0].id, {
      homeSizeBytes: body.length,
      homeEtag: `home-${body.length}`,
      homeSha256: expectedSha256,
      homeVerifiedAt: new Date().toISOString(),
    });

    await context.service.processItem(job.items[0].id);
    assert.equal(context.store.getJob(job.id).status, "SUCCEEDED");
    assert.deepEqual(context.minio.objects.get(objectKey), body);
    assert.equal(context.stats.sourceFetchCount, 1);
  } finally {
    await cleanup(context);
  }
});

test("public URL verification rejects size-less or mismatched range responses", async () => {
  const body = Buffer.from("public-url-check");
  const context = await setupService({ body });
  try {
    context.service.fetch = async (_url, options = {}) => {
      if (options.method === "HEAD") return new Response(null, { status: 200 });
      return new Response(Buffer.from([0]), {
        status: 206,
        headers: { "content-range": `bytes 0-0/${body.length + 1}` },
      });
    };
    assert.equal(await context.service.validatePublicUrl("https://oss.example.test/object", body.length), false);

    context.service.fetch = async (_url, options = {}) => {
      if (options.method === "HEAD") return new Response(null, { status: 405 });
      return new Response(Buffer.from([0]), {
        status: 206,
        headers: { "content-range": `bytes 0-0/${body.length}` },
      });
    };
    assert.equal(await context.service.validatePublicUrl("https://oss.example.test/object", body.length), true);
  } finally {
    await cleanup(context);
  }
});

test("the shared HTTP semaphore enforces weighted multipart capacity", async () => {
  const context = await setupService({ body: Buffer.from("http-capacity"), maxHttpConcurrency: 4 });
  try {
    let observedMaximum = 0;
    const useCapacity = () => context.service.httpSemaphore.use(async () => {
      observedMaximum = Math.max(observedMaximum, context.service.httpSemaphore.active);
      await new Promise((resolveDelay) => setTimeout(resolveDelay, 20));
    }, 3);
    await Promise.all([useCapacity(), useCapacity(), useCapacity()]);
    assert.equal(observedMaximum, 3);
    assert.equal(context.service.httpSemaphore.active, 0);
  } finally {
    await cleanup(context);
  }
});

test("streamed Home copy completes when global HTTP concurrency is two", async () => {
  const body = Buffer.from("two-slot-copy");
  const context = await setupService({ body, maxHttpConcurrency: 2 });
  try {
    const objectKey = "gateway-media/two-slot.png";
    const job = context.store.createJob({
      id: "two-slot-run",
      mediaKind: "GENERATED_MEDIA",
      items: [{
        lifecycleObjectId: "media-two-slot",
        objectKey,
        sourceUrl: `https://source.example.test/local-media/${objectKey}`,
        targetTier: "COLD_HOME_MINIO",
        expectedSizeBytes: body.length,
        mimeType: "image/png",
      }],
    });
    await context.service.processItem(job.items[0].id);
    assert.equal(context.store.getJob(job.id).status, "SUCCEEDED");
    assert.deepEqual(context.minio.objects.get(objectKey), body);
  } finally {
    await cleanup(context);
  }
});

test("concurrent jobs for one object share one Home copy", async () => {
  const body = Buffer.from("one-physical-object");
  const context = await setupService({ body, fetchDelayMs: 30 });
  try {
    const objectKey = "gateway-media/shared.png";
    for (const id of ["shared-run-a", "shared-run-b"]) {
      context.store.createJob({
        id,
        mediaKind: "GENERATED_MEDIA",
        items: [{
          lifecycleObjectId: `media-${id}`,
          objectKey,
          sourceUrl: `https://source.example.test/local-media/${objectKey}`,
          targetTier: "COLD_HOME_MINIO",
          expectedSizeBytes: body.length,
          mimeType: "image/png",
        }],
      });
    }
    context.service.start();
    const deadline = Date.now() + 2_000;
    while (Date.now() < deadline) {
      if (["shared-run-a", "shared-run-b"].every((id) => context.store.getJob(id).status === "SUCCEEDED")) break;
      await new Promise((resolveDelay) => setTimeout(resolveDelay, 10));
    }

    assert.equal(context.store.getJob("shared-run-a").status, "SUCCEEDED");
    assert.equal(context.store.getJob("shared-run-b").status, "SUCCEEDED");
    assert.equal(context.stats.sourceFetchCount, 1);
    assert.ok(context.service.telemetry().transferredBytesLastMinute >= body.length);
  } finally {
    await cleanup(context);
  }
});

test("a warm item copies to MinIO, uploads to OSS and selects a verified CDN URL", async () => {
  const body = Buffer.from("verified-media");
  const context = await setupService({ body });
  try {
    const version = context.store.upsertConfigVersion({
      bucket: "oss-media",
      region: "cn-beijing",
      endpoint: "oss-cn-beijing.aliyuncs.com",
      accessKeyId: "id",
      accessKeySecret: "secret",
      publicBaseUrl: "https://cdn.example.test",
    });
    const job = context.store.createJob({
      id: "warm-run",
      mediaKind: "GENERATED_MEDIA",
      configVersionId: version.id,
      items: [{
        lifecycleObjectId: "media-1",
        objectKey: "gateway-media/output.png",
        sourceUrl: "https://source.example.test/local-media/gateway-media/output.png",
        targetTier: "WARM_OSS",
        expectedSizeBytes: body.length,
        mimeType: "image/png",
      }],
    });

    await context.service.processItem(job.items[0].id);
    const completed = context.store.getJob(job.id);
    assert.equal(completed.status, "SUCCEEDED");
    assert.equal(completed.items[0].home.sizeBytes, body.length);
    assert.equal(completed.items[0].oss.selectedUrl, "https://cdn.example.test/gateway-media/output.png");
    assert.deepEqual(context.minio.objects.get("gateway-media/output.png"), body);
    assert.deepEqual(context.ossObjects.get("gateway-media/output.png"), body);
  } finally {
    await cleanup(context);
  }
});

test("an unavailable CDN falls back to the verified direct OSS URL", async () => {
  const body = Buffer.from("cdn-fallback-media");
  const context = await setupService({ body, cdnUnavailable: true });
  try {
    const version = context.store.upsertConfigVersion({
      bucket: "oss-media",
      region: "cn-beijing",
      endpoint: "oss-cn-beijing.aliyuncs.com",
      accessKeyId: "id",
      accessKeySecret: "secret",
      publicBaseUrl: "https://cdn.example.test/prefix",
    });
    const job = context.store.createJob({
      id: "cdn-fallback-run",
      mediaKind: "GENERATED_MEDIA",
      configVersionId: version.id,
      items: [{
        lifecycleObjectId: "media-cdn-fallback",
        objectKey: "gateway-media/fallback.png",
        sourceUrl: "https://source.example.test/local-media/gateway-media/fallback.png",
        targetTier: "WARM_OSS",
        expectedSizeBytes: body.length,
        mimeType: "image/png",
      }],
    });

    await context.service.processItem(job.items[0].id);
    const item = context.store.getJob(job.id).items[0];
    assert.equal(item.status, "SUCCEEDED");
    assert.equal(item.oss.cdnVerified, false);
    assert.equal(item.oss.selectedUrl, "https://oss-media.oss-cn-beijing.aliyuncs.com/gateway-media/fallback.png");
    assert.match(item.warning, /direct OSS URL was selected/);
  } finally {
    await cleanup(context);
  }
});

test("large objects persist multipart checkpoint parts before completing", async () => {
  const body = Buffer.alloc(6 * 1024 * 1024, 11);
  const context = await setupService({ body, threshold: 5 * 1024 * 1024 });
  try {
    const version = context.store.upsertConfigVersion({
      bucket: "oss-media",
      region: "cn-beijing",
      endpoint: "oss-cn-beijing.aliyuncs.com",
      accessKeyId: "id",
      accessKeySecret: "secret",
      publicBaseUrl: "",
    });
    const job = context.store.createJob({
      id: "multipart-run",
      mediaKind: "GENERATED_MEDIA",
      configVersionId: version.id,
      items: [{
        lifecycleObjectId: "media-large",
        objectKey: "gateway-media/output.mp4",
        sourceUrl: "https://source.example.test/local-media/gateway-media/output.mp4",
        targetTier: "WARM_OSS",
        expectedSizeBytes: body.length,
        mimeType: "video/mp4",
      }],
    });

    await context.service.processItem(job.items[0].id);
    assert.equal(context.store.getJob(job.id).status, "SUCCEEDED");
    assert.deepEqual(context.multipartCalls, [{
      key: "gateway-media/output.mp4",
      parallel: 4,
      partSize: 1024 * 1024,
      checkpointUploadId: null,
    }]);
    const parts = context.store.db.prepare("SELECT part_number, etag FROM multipart_parts WHERE item_id = ?").all(job.items[0].id);
    assert.deepEqual(parts, [{ part_number: 1, etag: "part-1" }]);
  } finally {
    await cleanup(context);
  }
});

test("multipart upload resumes from the persisted checkpoint after interruption", async () => {
  const body = Buffer.alloc(6 * 1024 * 1024, 17);
  const context = await setupService({
    body,
    threshold: 5 * 1024 * 1024,
    failFirstMultipart: true,
  });
  try {
    const version = context.store.upsertConfigVersion({
      bucket: "oss-media",
      region: "cn-beijing",
      endpoint: "oss-cn-beijing.aliyuncs.com",
      accessKeyId: "id",
      accessKeySecret: "secret",
      publicBaseUrl: "",
    });
    const job = context.store.createJob({
      id: "multipart-resume-run",
      mediaKind: "GENERATED_MEDIA",
      configVersionId: version.id,
      items: [{
        lifecycleObjectId: "media-resume",
        objectKey: "gateway-media/resume.mp4",
        sourceUrl: "https://source.example.test/local-media/gateway-media/resume.mp4",
        targetTier: "WARM_OSS",
        expectedSizeBytes: body.length,
        mimeType: "video/mp4",
      }],
    });

    await context.service.processItem(job.items[0].id);
    assert.equal(context.store.getJob(job.id).items[0].status, "RETRY_WAIT");
    assert.equal(context.store.getCheckpoint(job.items[0].id).uploadId, "upload-1");

    await context.service.processItem(job.items[0].id);
    assert.equal(context.store.getJob(job.id).status, "SUCCEEDED");
    assert.equal(context.multipartCalls.length, 2);
    assert.equal(context.multipartCalls[0].checkpointUploadId, null);
    assert.equal(context.multipartCalls[1].checkpointUploadId, "upload-1");
  } finally {
    await cleanup(context);
  }
});
