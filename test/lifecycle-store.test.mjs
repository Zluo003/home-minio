import assert from "node:assert/strict";
import { mkdtemp, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";
import {
  LifecycleConflictError,
  LifecycleStore,
  LifecycleValidationError,
  loadLifecycleEncryptionKey,
} from "../web/backend/lifecycle-store.mjs";

async function withStore(callback) {
  const root = await mkdtemp(join(tmpdir(), "home-minio-store-"));
  const dbPath = join(root, "state.sqlite");
  const key = Buffer.alloc(32, 7);
  const store = new LifecycleStore({ dbPath, encryptionKey: key });
  try {
    await callback({ store, dbPath, key });
  } finally {
    store.close();
    await rm(root, { recursive: true, force: true });
  }
}

const ossConfig = {
  bucket: "media-test",
  region: "cn-beijing",
  endpoint: "oss-cn-beijing.aliyuncs.com",
  accessKeyId: "test-access-key",
  accessKeySecret: "test-secret",
  publicBaseUrl: "https://cdn.example.test",
};

test("OSS config versions reuse an HMAC fingerprint and never expose the secret", async () => {
  await withStore(({ store }) => {
    const first = store.upsertConfigVersion(ossConfig);
    const second = store.upsertConfigVersion({ ...ossConfig });

    assert.equal(second.id, first.id);
    assert.equal(first.bucket, ossConfig.bucket);
    assert.equal("accessKeySecret" in first, false);
    assert.deepEqual(store.getDecryptedConfig(first.id), ossConfig);
    const raw = store.db.prepare("SELECT encrypted_payload FROM oss_config_versions WHERE id = ?").get(first.id);
    assert.equal(raw.encrypted_payload.includes(ossConfig.accessKeySecret), false);
  });
});

test("SQLite lifecycle state enables WAL, foreign keys and a busy timeout", async () => {
  await withStore(({ store }) => {
    assert.equal(store.db.pragma("journal_mode", { simple: true }), "wal");
    assert.equal(store.db.pragma("foreign_keys", { simple: true }), 1);
    assert.ok(store.db.pragma("busy_timeout", { simple: true }) >= 5_000);
  });
});

test("Docker secret encryption key takes priority and invalid key material is rejected", async () => {
  const root = await mkdtemp(join(tmpdir(), "home-minio-secret-"));
  const keyPath = join(root, "lifecycle-key");
  try {
    await writeFile(keyPath, "11".repeat(32), "utf8");
    assert.deepEqual(
      loadLifecycleEncryptionKey({
        HOME_MINIO_CONFIG_ENCRYPTION_KEY_FILE: keyPath,
        HOME_MINIO_CONFIG_ENCRYPTION_KEY: "invalid-fallback",
      }),
      Buffer.alloc(32, 0x11),
    );
    assert.throws(
      () => loadLifecycleEncryptionKey({ HOME_MINIO_CONFIG_ENCRYPTION_KEY: "too-short" }),
      LifecycleValidationError,
    );
    await writeFile(keyPath, "too-short", "utf8");
    assert.throws(
      () => loadLifecycleEncryptionKey({ HOME_MINIO_CONFIG_ENCRYPTION_KEY_FILE: keyPath }),
      LifecycleValidationError,
    );
    await writeFile(keyPath, "", "utf8");
    assert.deepEqual(
      loadLifecycleEncryptionKey({
        HOME_MINIO_CONFIG_ENCRYPTION_KEY_FILE: keyPath,
        HOME_MINIO_CONFIG_ENCRYPTION_KEY: "22".repeat(32),
      }),
      Buffer.alloc(32, 0x22),
    );
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

test("a different master key cannot decrypt stored OSS credentials", async () => {
  const root = await mkdtemp(join(tmpdir(), "home-minio-key-"));
  const dbPath = join(root, "state.sqlite");
  const first = new LifecycleStore({ dbPath, encryptionKey: Buffer.alloc(32, 1) });
  const version = first.upsertConfigVersion(ossConfig);
  first.close();

  try {
    assert.throws(
      () => new LifecycleStore({ dbPath, encryptionKey: Buffer.alloc(32, 2) }),
      LifecycleValidationError,
    );
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

test("lifecycle jobs are idempotent and reject job id reuse with different data", async () => {
  await withStore(({ store }) => {
    const version = store.upsertConfigVersion(ossConfig);
    const payload = {
      id: "run-1",
      mediaKind: "GENERATED_MEDIA",
      configVersionId: version.id,
      items: [{
        lifecycleObjectId: "object-1",
        objectKey: "gateway-media/2026/07/output.png",
        sourceUrl: "https://api.example.test/local-media/gateway-media/2026/07/output.png",
        targetTier: "WARM_OSS",
        expectedSizeBytes: 12,
        mimeType: "image/png",
      }],
    };

    const first = store.createJob(payload);
    const second = store.createJob(structuredClone(payload));
    assert.equal(second.id, first.id);
    assert.equal(second.items.length, 1);
    assert.throws(
      () => store.createJob({ ...payload, items: [{ ...payload.items[0], objectKey: "gateway-media/other.png" }] }),
      LifecycleConflictError,
    );
  });
});

test("RUNNING jobs and items recover to retryable state after restart", async () => {
  const root = await mkdtemp(join(tmpdir(), "home-minio-recovery-"));
  const dbPath = join(root, "state.sqlite");
  const key = Buffer.alloc(32, 3);
  const first = new LifecycleStore({ dbPath, encryptionKey: key });
  const job = first.createJob({
    id: "run-recovery",
    mediaKind: "WORKFLOW_UPLOAD",
    items: [{
      lifecycleObjectId: "object-recovery",
      objectKey: "2026/07/file.png",
      sourceUrl: "https://api.example.test/local-media/2026/07/file.png",
      targetTier: "COLD_HOME_MINIO",
      expectedSizeBytes: 4,
    }],
  });
  first.claimItem(job.items[0].id);
  first.close();

  const second = new LifecycleStore({ dbPath, encryptionKey: key });
  try {
    const recovered = second.getJob(job.id);
    assert.equal(recovered.status, "QUEUED");
    assert.equal(recovered.items[0].status, "RETRY_WAIT");
    assert.equal(second.listRunnableItems()[0].id, job.items[0].id);
  } finally {
    second.close();
    await rm(root, { recursive: true, force: true });
  }
});
