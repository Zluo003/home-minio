import assert from "node:assert/strict";
import { createCipheriv, randomBytes } from "node:crypto";
import { mkdtemp, rm, stat } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";
import {
  LifecycleConflictError,
  LifecycleStore,
} from "../web/backend/lifecycle-store.mjs";

async function withStore(callback) {
  const root = await mkdtemp(join(tmpdir(), "home-minio-store-"));
  const dbPath = join(root, "state.sqlite");
  const store = new LifecycleStore({ dbPath, encryptionKey: null });
  try {
    await callback({ store, dbPath });
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

function encryptLegacyConfig(value, key) {
  const iv = randomBytes(12);
  const cipher = createCipheriv("aes-256-gcm", key, iv);
  const ciphertext = Buffer.concat([cipher.update(JSON.stringify(value), "utf8"), cipher.final()]);
  return `v1.${iv.toString("base64url")}.${cipher.getAuthTag().toString("base64url")}.${ciphertext.toString("base64url")}`;
}

test("OSS config versions reuse a content fingerprint and never expose the secret through the API", async () => {
  await withStore(({ store }) => {
    const first = store.upsertConfigVersion(ossConfig);
    const second = store.upsertConfigVersion({ ...ossConfig });

    assert.equal(second.id, first.id);
    assert.equal(first.bucket, ossConfig.bucket);
    assert.equal("accessKeySecret" in first, false);
    assert.deepEqual(store.getDecryptedConfig(first.id), ossConfig);
    const raw = store.db.prepare("SELECT encrypted_payload FROM oss_config_versions WHERE id = ?").get(first.id);
    assert.match(raw.encrypted_payload, /^plain\./);
  });
});

test("SQLite lifecycle state enables WAL, foreign keys and a busy timeout", async () => {
  await withStore(({ store }) => {
    assert.equal(store.db.pragma("journal_mode", { simple: true }), "wal");
    assert.equal(store.db.pragma("foreign_keys", { simple: true }), 1);
    assert.ok(store.db.pragma("busy_timeout", { simple: true }) >= 5_000);
    assert.equal(store.db.prepare("SELECT MAX(version) AS version FROM schema_migrations").get().version, 2);
  });
});

test("Home verification records whether an existing object was reused", async () => {
  await withStore(({ store }) => {
    const job = store.createJob({
      id: "run-home-reused",
      mediaKind: "WORKFLOW_UPLOAD",
      items: [{
        lifecycleObjectId: "object-home-reused",
        objectKey: "New-Waule/uploads/already-cold.png",
        sourceUrl: "https://api.example.test/local-media/New-Waule/uploads/already-cold.png",
        targetTier: "COLD_HOME_MINIO",
        expectedSizeBytes: 12,
      }],
    });
    store.updateItem(job.items[0].id, {
      homeSizeBytes: 12,
      homeReused: true,
      homeVerifiedAt: new Date().toISOString(),
    });
    assert.equal(store.getJob(job.id).items[0].home.reused, true);
  });
});

test("existing v1 lifecycle state upgrades to reuse tracking without losing jobs", async () => {
  const root = await mkdtemp(join(tmpdir(), "home-minio-v1-upgrade-"));
  const dbPath = join(root, "state.sqlite");
  const first = new LifecycleStore({ dbPath, encryptionKey: null });
  first.createJob({
    id: "run-before-v2",
    mediaKind: "WORKFLOW_UPLOAD",
    items: [{
      lifecycleObjectId: "object-before-v2",
      objectKey: "New-Waule/uploads/before-v2.png",
      sourceUrl: "https://api.example.test/local-media/New-Waule/uploads/before-v2.png",
      targetTier: "COLD_HOME_MINIO",
      expectedSizeBytes: 12,
    }],
  });
  first.db.exec(`
    ALTER TABLE transfer_items DROP COLUMN home_reused;
    DELETE FROM schema_migrations WHERE version = 2;
  `);
  first.close();

  const second = new LifecycleStore({ dbPath, encryptionKey: null });
  try {
    assert.equal(second.db.prepare("SELECT MAX(version) AS version FROM schema_migrations").get().version, 2);
    assert.equal(second.db.prepare("SELECT COUNT(*) AS count FROM transfer_items").get().count, 1);
    assert.equal(second.getJob("run-before-v2").items[0].home, null);
    assert.ok(second.db.pragma("table_info(transfer_items)").some((column) => column.name === "home_reused"));
  } finally {
    second.close();
    await rm(root, { recursive: true, force: true });
  }
});

test("lifecycle state reopens without an external encryption key", async () => {
  const root = await mkdtemp(join(tmpdir(), "home-minio-key-"));
  const dbPath = join(root, "state.sqlite");
  const first = new LifecycleStore({ dbPath, encryptionKey: null });
  const version = first.upsertConfigVersion(ossConfig);
  first.close();

  const second = new LifecycleStore({ dbPath, encryptionKey: null });
  try {
    assert.deepEqual(second.getDecryptedConfig(version.id), ossConfig);
    assert.equal(second.health().ready, true);
    assert.equal(second.health().credentialStorage, "LOCAL_SQLITE");
  } finally {
    second.close();
    await rm(root, { recursive: true, force: true });
  }
});

test("legacy encrypted OSS config rows migrate to local SQLite storage when the old key is available", async () => {
  const root = await mkdtemp(join(tmpdir(), "home-minio-legacy-key-"));
  const dbPath = join(root, "state.sqlite");
  const key = Buffer.alloc(32, 5);
  const first = new LifecycleStore({ dbPath, encryptionKey: null });
  const version = first.upsertConfigVersion(ossConfig);
  first.db.prepare("UPDATE oss_config_versions SET encrypted_payload = ? WHERE id = ?")
    .run(encryptLegacyConfig(ossConfig, key), version.id);
  first.close();

  const second = new LifecycleStore({ dbPath, encryptionKey: key });
  try {
    const raw = second.db.prepare("SELECT encrypted_payload FROM oss_config_versions WHERE id = ?").get(version.id);
    assert.match(raw.encrypted_payload, /^plain\./);
    assert.deepEqual(second.getDecryptedConfig(version.id), ossConfig);
    assert.equal(second.health().legacyEncryptedConfigs, 0);
  } finally {
    second.close();
    await rm(root, { recursive: true, force: true });
  }
});

test("SQLite lifecycle state files use owner-only permissions", async () => {
  await withStore(async ({ dbPath }) => {
    const metadata = await stat(dbPath);
    assert.equal(metadata.mode & 0o777, 0o600);
  });
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

test("cancelled lifecycle jobs retain completed items and resume unfinished items", async () => {
  await withStore(({ store }) => {
    const job = store.createJob({
      id: "run-cancel-resume",
      mediaKind: "WORKFLOW_UPLOAD",
      items: [
        {
          lifecycleObjectId: "object-complete",
          objectKey: "2026/07/complete.png",
          sourceUrl: "https://api.example.test/local-media/2026/07/complete.png",
          targetTier: "COLD_HOME_MINIO",
          expectedSizeBytes: 12,
        },
        {
          lifecycleObjectId: "object-pending",
          objectKey: "2026/07/pending.png",
          sourceUrl: "https://api.example.test/local-media/2026/07/pending.png",
          targetTier: "COLD_HOME_MINIO",
          expectedSizeBytes: 18,
        },
      ],
    });
    store.updateItem(job.items[0].id, {
      status: "SUCCEEDED",
      stage: "COMPLETED",
      homeSizeBytes: 12,
      homeVerifiedAt: new Date().toISOString(),
      finishedAt: new Date().toISOString(),
    });

    const cancelled = store.cancelJob(job.id);
    assert.equal(cancelled.status, "CANCELLED");
    assert.deepEqual(cancelled.items.map((item) => item.status), ["SUCCEEDED", "CANCELLED"]);
    assert.equal(store.isJobTerminal(job.id), true);
    assert.deepEqual(store.getJobDiagnostics(job.id).statusCounts, { CANCELLED: 1, SUCCEEDED: 1 });

    const resumed = store.resumeJob(job.id);
    assert.equal(resumed.status, "QUEUED");
    assert.deepEqual(resumed.items.map((item) => item.status), ["SUCCEEDED", "QUEUED"]);
    assert.equal(store.listRunnableItems().length, 1);
  });
});

test("RUNNING jobs and items recover to retryable state after restart", async () => {
  const root = await mkdtemp(join(tmpdir(), "home-minio-recovery-"));
  const dbPath = join(root, "state.sqlite");
  const first = new LifecycleStore({ dbPath, encryptionKey: null });
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

  const second = new LifecycleStore({ dbPath, encryptionKey: null });
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
