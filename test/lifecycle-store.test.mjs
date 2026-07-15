import assert from "node:assert/strict";
import { createCipheriv, randomBytes, randomUUID } from "node:crypto";
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
    assert.equal(store.db.prepare("SELECT MAX(version) AS version FROM schema_migrations").get().version, 5);
    const indexes = new Set(store.db.prepare("SELECT name FROM sqlite_master WHERE type = 'index'").all().map((row) => row.name));
    assert.equal(indexes.has("callback_outbox_run_runnable_idx"), true);
    assert.equal(indexes.has("transfer_items_job_status_idx"), true);
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

test("existing v2 lifecycle state upgrades to streaming runs without losing jobs", async () => {
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
    DROP TABLE callback_outbox;
    DROP INDEX transfer_jobs_manifest_runnable_idx;
    ALTER TABLE transfer_items DROP COLUMN failure_kind;
    ALTER TABLE transfer_items DROP COLUMN failure_status_code;
    ALTER TABLE transfer_items DROP COLUMN retryable;
    ALTER TABLE transfer_jobs DROP COLUMN manifest_url;
    ALTER TABLE transfer_jobs DROP COLUMN manifest_token;
    ALTER TABLE transfer_jobs DROP COLUMN callback_url;
    ALTER TABLE transfer_jobs DROP COLUMN callback_token;
    ALTER TABLE transfer_jobs DROP COLUMN manifest_status;
    ALTER TABLE transfer_jobs DROP COLUMN expected_count;
    ALTER TABLE transfer_jobs DROP COLUMN manifest_accepted_count;
    ALTER TABLE transfer_jobs DROP COLUMN manifest_attempt_count;
    ALTER TABLE transfer_jobs DROP COLUMN manifest_next_retry_at;
    ALTER TABLE transfer_jobs DROP COLUMN last_callback_sequence;
    DELETE FROM schema_migrations WHERE version >= 3;
  `);
  first.close();

  const second = new LifecycleStore({ dbPath, encryptionKey: null });
  try {
    assert.equal(second.db.prepare("SELECT MAX(version) AS version FROM schema_migrations").get().version, 5);
    assert.equal(second.db.prepare("SELECT COUNT(*) AS count FROM transfer_items").get().count, 1);
    assert.equal(second.getJob("run-before-v2").items[0].home, null);
    assert.ok(second.db.pragma("table_info(transfer_items)").some((column) => column.name === "home_reused"));
  } finally {
    second.close();
    await rm(root, { recursive: true, force: true });
  }
});

test("streaming manifests accept a complete run larger than the legacy 100 item request", async () => {
  await withStore(({ store }) => {
    const expectedCount = 250;
    const run = store.createStreamingJob({
      id: "streaming-run-250",
      mediaKind: "WORKFLOW_UPLOAD",
      expectedCount,
      manifestUrl: "https://api.example.test/internal/media-runs/streaming-run-250/manifest",
      manifestToken: "manifest-token",
      callbackUrl: "https://api.example.test/internal/media-runs/streaming-run-250/results",
      callbackToken: "callback-token",
    });
    assert.equal(run.status, "INGESTING");
    assert.ok(store.claimManifestRun(run.id));

    const items = Array.from({ length: expectedCount }, (_, index) => ({
      lifecycleObjectId: `object-${index}`,
      objectKey: `New-Waule/uploads/2026/07/${index}.png`,
      sourceUrl: `https://api.example.test/local-media/New-Waule/uploads/2026/07/${index}.png`,
      targetTier: "COLD_HOME_MINIO",
      expectedSizeBytes: index + 1,
      mimeType: "image/png",
    }));
    store.appendManifestItems(run.id, items.slice(0, 125));
    store.appendManifestItems(run.id, items.slice(125));
    const sealed = store.completeManifest(run.id);

    assert.equal(sealed.manifestStatus, "SEALED");
    assert.equal(sealed.totalCount, expectedCount);
    assert.equal(sealed.manifestAcceptedCount, expectedCount);
    assert.equal(store.db.prepare("SELECT COUNT(*) AS count FROM transfer_items WHERE job_id = ?").get(run.id).count, expectedCount);
  });
});

test("large callback outboxes select one runnable batch without scanning every item per row", async () => {
  await withStore(({ store }) => {
    const expectedCount = 5_000;
    const run = store.createStreamingJob({
      id: "large-callback-outbox",
      mediaKind: "GENERATED_MEDIA",
      expectedCount,
      manifestUrl: "https://api.example.test/internal/large-callback-outbox/manifest",
      manifestToken: "manifest-token",
      callbackUrl: "https://api.example.test/internal/large-callback-outbox/results",
      callbackToken: "callback-token",
    });
    store.claimManifestRun(run.id);
    for (let offset = 0; offset < expectedCount; offset += 500) {
      store.appendManifestItems(run.id, Array.from({ length: 500 }, (_, index) => {
        const sequence = offset + index + 1;
        return {
          lifecycleObjectId: `large-callback-object-${sequence}`,
          objectKey: `gateway-media/large-callback/${sequence}.png`,
          sourceUrl: `https://api.example.test/local-media/gateway-media/large-callback/${sequence}.png`,
          targetTier: "COLD_HOME_MINIO",
          expectedSizeBytes: sequence,
          mimeType: "image/png",
        };
      }));
    }
    store.completeManifest(run.id);
    const completedAt = new Date().toISOString();
    store.db.prepare(`
      UPDATE transfer_items
      SET status = 'SUCCEEDED', stage = 'COMPLETED', finished_at = ?, updated_at = ?
      WHERE job_id = ?
    `).run(completedAt, completedAt, run.id);
    const items = store.db.prepare("SELECT id FROM transfer_items WHERE job_id = ? ORDER BY created_at, id").all(run.id);
    const insert = store.db.prepare(`
      INSERT INTO callback_outbox(
        id, run_id, item_id, sequence, status, payload_json, created_at, updated_at
      ) VALUES (?, ?, ?, ?, 'QUEUED', '{}', ?, ?)
    `);
    store.db.transaction(() => {
      items.forEach((item, index) => insert.run(randomUUID(), run.id, item.id, index + 1, completedAt, completedAt));
      store.db.prepare(`
        UPDATE transfer_jobs
        SET status = 'RESULTS_PENDING', processed_count = ?, succeeded_count = ?,
            last_callback_sequence = ?, updated_at = ?
        WHERE id = ?
      `).run(expectedCount, expectedCount, expectedCount, completedAt, run.id);
    })();

    const startedAt = performance.now();
    const callbacks = store.listRunnableCallbacks(100);
    const elapsedMs = performance.now() - startedAt;

    assert.equal(callbacks.length, 100);
    assert.deepEqual(callbacks.map((callback) => callback.sequence), Array.from({ length: 100 }, (_, index) => index + 1));
    assert.ok(elapsedMs < 1_000, `callback query took ${elapsedMs.toFixed(1)} ms`);
  });
});

test("completed streaming callbacks can be replayed without rerunning transfer items", async () => {
  await withStore(({ store }) => {
    const configVersion = store.upsertConfigVersion(ossConfig);
    const run = store.createStreamingJob({
      id: "callback-replay-run",
      mediaKind: "GENERATED_MEDIA",
      configVersionId: configVersion.id,
      expectedCount: 2,
      manifestUrl: "https://api.example.test/internal/callback-replay-run/manifest",
      manifestToken: "manifest-token",
      callbackUrl: "https://api.example.test/internal/callback-replay-run/results",
      callbackToken: "callback-token",
    });
    store.claimManifestRun(run.id);
    store.appendManifestItems(run.id, [1, 2].map((sequence) => ({
      lifecycleObjectId: `callback-replay-object-${sequence}`,
      objectKey: `gateway-media/callback-replay/${sequence}.png`,
      sourceUrl: `https://api.example.test/local-media/gateway-media/callback-replay/${sequence}.png`,
      targetTier: "WARM_OSS",
      expectedSizeBytes: sequence * 10,
      mimeType: "image/png",
    })));
    store.completeManifest(run.id);

    const completedAt = new Date().toISOString();
    const items = store.getJob(run.id).items;
    for (const item of items) {
      store.updateItem(item.id, {
        status: "SUCCEEDED",
        stage: "COMPLETED",
        homeSizeBytes: item.expectedSizeBytes,
        homeVerifiedAt: completedAt,
        ossSizeBytes: item.expectedSizeBytes,
        ossVerifiedAt: completedAt,
        finishedAt: completedAt,
      });
      store.enqueueItemCallback(item.id);
    }
    const original = store.db.prepare(`
      SELECT id, item_id, sequence, payload_json FROM callback_outbox
      WHERE run_id = ? ORDER BY sequence
    `).all(run.id);
    const claimed = store.claimCallbacks(original.map((callback) => callback.id));
    store.markCallbacksDelivered(claimed.map((callback) => callback.id));
    assert.equal(store.getJobSummary(run.id).status, "SUCCEEDED");

    const replayed = store.replayStreamingJobCallbacks(run.id);
    assert.equal(replayed.status, "RESULTS_PENDING");
    assert.equal(replayed.callbackPendingCount, 2);
    assert.equal(replayed.callbackDeliveredCount, 0);
    assert.deepEqual(
      store.db.prepare(`SELECT id, item_id, sequence, payload_json FROM callback_outbox WHERE run_id = ? ORDER BY sequence`).all(run.id),
      original,
    );
    assert.deepEqual(store.getJob(run.id).items.map((item) => item.status), ["SUCCEEDED", "SUCCEEDED"]);
    assert.equal(store.listRunnableItems().length, 0);

    const idempotent = store.replayStreamingJobCallbacks(run.id);
    assert.equal(idempotent.callbackPendingCount, 2);
    assert.deepEqual(store.listRunnableCallbacks().map((callback) => callback.sequence), [1, 2]);
  });
});

test("a cancelled partial manifest resumes every already-ingested item", async () => {
  await withStore(({ store }) => {
    const run = store.createStreamingJob({
      id: "streaming-partial-resume",
      mediaKind: "WORKFLOW_UPLOAD",
      expectedCount: 2,
      manifestUrl: "https://api.example.test/internal/media-runs/streaming-partial-resume/manifest",
      manifestToken: "manifest-token",
      callbackUrl: "https://api.example.test/internal/media-runs/streaming-partial-resume/results",
      callbackToken: "callback-token",
    });
    store.claimManifestRun(run.id);
    store.appendManifestItems(run.id, [{
      lifecycleObjectId: "partial-object-1",
      objectKey: "New-Waule/uploads/2026/07/partial-1.png",
      sourceUrl: "https://api.example.test/local-media/New-Waule/uploads/2026/07/partial-1.png",
      targetTier: "COLD_HOME_MINIO",
      expectedSizeBytes: 12,
    }]);

    store.cancelJob(run.id, "Cancelled while reading manifest", true);
    assert.equal(store.getJob(run.id).items[0].status, "CANCELLED");

    const resumed = store.resumeStreamingJob(run.id);
    assert.equal(resumed.status, "INGESTING");
    assert.equal(resumed.manifestStatus, "PENDING");
    assert.equal(store.getJob(run.id).items[0].status, "QUEUED");

    store.claimManifestRun(run.id);
    store.appendManifestItems(run.id, [
      {
        lifecycleObjectId: "partial-object-1",
        objectKey: "New-Waule/uploads/2026/07/partial-1.png",
        sourceUrl: "https://api.example.test/local-media/New-Waule/uploads/2026/07/partial-1.png",
        targetTier: "COLD_HOME_MINIO",
        expectedSizeBytes: 12,
      },
      {
        lifecycleObjectId: "partial-object-2",
        objectKey: "New-Waule/uploads/2026/07/partial-2.png",
        sourceUrl: "https://api.example.test/local-media/New-Waule/uploads/2026/07/partial-2.png",
        targetTier: "COLD_HOME_MINIO",
        expectedSizeBytes: 18,
      },
    ]);
    const sealed = store.completeManifest(run.id);
    assert.equal(sealed.totalCount, 2);
    assert.deepEqual(store.getJob(run.id).items.map((item) => item.status), ["QUEUED", "QUEUED"]);
    assert.equal(store.listRunnableItems().length, 2);
  });
});

test("manifest ingestion and callback delivery recover after process restart", async () => {
  const root = await mkdtemp(join(tmpdir(), "home-minio-stream-recovery-"));
  const dbPath = join(root, "state.sqlite");
  const first = new LifecycleStore({ dbPath, encryptionKey: null });
  const run = first.createStreamingJob({
    id: "streaming-recovery",
    mediaKind: "GENERATED_MEDIA",
    expectedCount: 1,
    manifestUrl: "https://api.example.test/internal/media-runs/streaming-recovery/manifest",
    manifestToken: "manifest-token",
    callbackUrl: "https://api.example.test/internal/media-runs/streaming-recovery/results",
    callbackToken: "callback-token",
  });
  first.claimManifestRun(run.id);
  first.appendManifestItems(run.id, [{
    lifecycleObjectId: "object-recovery",
    objectKey: "gateway-media/recovery.png",
    sourceUrl: "https://api.example.test/local-media/gateway-media/recovery.png",
    targetTier: "COLD_HOME_MINIO",
    expectedSizeBytes: 8,
    mimeType: "image/png",
  }]);
  const itemId = first.db.prepare("SELECT id FROM transfer_items WHERE job_id = ?").get(run.id).id;
  first.updateItem(itemId, {
    status: "FAILED",
    stage: "FAILED",
    failureKind: "SOURCE_MISSING",
    failureStatusCode: 404,
    retryable: false,
    error: "Source download failed with HTTP 404.",
    finishedAt: new Date().toISOString(),
  });
  const callback = first.enqueueItemCallback(itemId);
  first.db.prepare("UPDATE callback_outbox SET status = 'SENDING' WHERE id = ?").run(callback.id);
  first.close();

  const second = new LifecycleStore({ dbPath, encryptionKey: null });
  try {
    const recovered = second.getJobSummary(run.id);
    assert.equal(recovered.status, "INGESTING");
    assert.equal(recovered.manifestStatus, "RETRY_WAIT");
    assert.deepEqual(second.listManifestRuns(), [run.id]);
    assert.equal(second.listRunnableCallbacks().length, 0);

    assert.ok(second.claimManifestRun(run.id));
    second.appendManifestItems(run.id, [{
      lifecycleObjectId: "object-recovery",
      objectKey: "gateway-media/recovery.png",
      sourceUrl: "https://api.example.test/local-media/gateway-media/recovery.png",
      targetTier: "COLD_HOME_MINIO",
      expectedSizeBytes: 8,
      mimeType: "image/png",
    }]);
    second.completeManifest(run.id);
    const callbacks = second.listRunnableCallbacks();
    assert.equal(callbacks.length, 1);
    assert.equal(callbacks[0].status, "RETRY_WAIT");
    assert.equal(callbacks[0].payload.failureKind, "SOURCE_MISSING");
  } finally {
    second.close();
    await rm(root, { recursive: true, force: true });
  }
});

test("exhausted callbacks are visible and can be resumed without rerunning completed transfers", async () => {
  await withStore(({ store }) => {
    const run = store.createStreamingJob({
      id: "streaming-callback-resume",
      mediaKind: "GENERATED_MEDIA",
      expectedCount: 1,
      manifestUrl: "https://api.example.test/internal/media-runs/streaming-callback-resume/manifest",
      manifestToken: "manifest-token",
      callbackUrl: "https://api.example.test/internal/media-runs/streaming-callback-resume/results",
      callbackToken: "callback-token",
    });
    store.claimManifestRun(run.id);
    store.appendManifestItems(run.id, [{
      lifecycleObjectId: "callback-object",
      objectKey: "gateway-media/callback-object.png",
      sourceUrl: "https://api.example.test/local-media/gateway-media/callback-object.png",
      targetTier: "COLD_HOME_MINIO",
      expectedSizeBytes: 8,
    }]);
    store.completeManifest(run.id);
    const itemId = store.db.prepare("SELECT id FROM transfer_items WHERE job_id = ?").get(run.id).id;
    store.updateItem(itemId, {
      status: "SUCCEEDED",
      stage: "COMPLETED",
      homeSizeBytes: 8,
      homeSha256: "a".repeat(64),
      homeVerifiedAt: new Date().toISOString(),
      finishedAt: new Date().toISOString(),
    });
    const callback = store.enqueueItemCallback(itemId);
    for (let attempt = 0; attempt < 10; attempt += 1) {
      assert.equal(store.claimCallbacks([callback.id]).length, 1);
      store.markCallbacksRetry([callback.id], "NewWaule callback unavailable");
    }

    const failed = store.getJobSummary(run.id);
    assert.equal(failed.status, "CALLBACK_FAILED");
    assert.equal(failed.callbackTotalCount, 1);
    assert.equal(failed.callbackDeliveredCount, 0);
    assert.equal(failed.callbackPendingCount, 0);
    assert.equal(failed.callbackFailedCount, 1);

    const resumed = store.resumeStreamingJob(run.id);
    assert.equal(resumed.status, "RESULTS_PENDING");
    assert.equal(resumed.callbackFailedCount, 0);
    assert.equal(resumed.callbackPendingCount, 1);
    assert.equal(store.listRunnableCallbacks()[0].id, callback.id);
    assert.equal(store.getJob(run.id).items[0].status, "SUCCEEDED");

    const claimed = store.claimCallbacks([callback.id]);
    store.markCallbacksDelivered(claimed.map((item) => item.id));
    assert.equal(store.getJobSummary(run.id).status, "SUCCEEDED");
  });
});

test("schema v4 restores exhausted callbacks for completed legacy runs", async () => {
  const root = await mkdtemp(join(tmpdir(), "home-minio-v4-callback-upgrade-"));
  const dbPath = join(root, "state.sqlite");
  const first = new LifecycleStore({ dbPath, encryptionKey: null });
  const run = first.createStreamingJob({
    id: "v4-callback-recovery",
    mediaKind: "GENERATED_MEDIA",
    expectedCount: 1,
    manifestUrl: "https://api.example.test/internal/v4-callback-recovery/manifest",
    manifestToken: "manifest-token",
    callbackUrl: "https://api.example.test/internal/v4-callback-recovery/results",
    callbackToken: "callback-token",
  });
  first.claimManifestRun(run.id);
  first.appendManifestItems(run.id, [{
    lifecycleObjectId: "v4-callback-object",
    objectKey: "gateway-media/v4-callback-object.png",
    sourceUrl: "https://api.example.test/local-media/gateway-media/v4-callback-object.png",
    targetTier: "COLD_HOME_MINIO",
    expectedSizeBytes: 8,
  }]);
  first.completeManifest(run.id);
  const itemId = first.db.prepare("SELECT id FROM transfer_items WHERE job_id = ?").get(run.id).id;
  first.updateItem(itemId, {
    status: "SUCCEEDED",
    stage: "COMPLETED",
    homeSizeBytes: 8,
    homeVerifiedAt: new Date().toISOString(),
    finishedAt: new Date().toISOString(),
  });
  const callback = first.enqueueItemCallback(itemId);
  first.db.prepare(`
    UPDATE callback_outbox
    SET status = 'FAILED', attempt_count = 10, error = 'legacy callback failure'
    WHERE id = ?
  `).run(callback.id);
  first.db.prepare("UPDATE transfer_jobs SET status = 'FAILED', error = 'legacy callback failure' WHERE id = ?").run(run.id);
  first.db.prepare("DELETE FROM schema_migrations WHERE version >= 4").run();
  first.close();

  const second = new LifecycleStore({ dbPath, encryptionKey: null });
  try {
    const recovered = second.getJobSummary(run.id);
    assert.equal(recovered.status, "RESULTS_PENDING");
    assert.equal(recovered.callbackFailedCount, 0);
    assert.equal(recovered.callbackPendingCount, 1);
    assert.equal(second.listRunnableCallbacks()[0].id, callback.id);
  } finally {
    second.close();
    await rm(root, { recursive: true, force: true });
  }
});

test("restart recreates an outbox row missing after a terminal item commit", async () => {
  const root = await mkdtemp(join(tmpdir(), "home-minio-callback-gap-"));
  const dbPath = join(root, "state.sqlite");
  const first = new LifecycleStore({ dbPath, encryptionKey: null });
  const run = first.createStreamingJob({
    id: "callback-gap-recovery",
    mediaKind: "GENERATED_MEDIA",
    expectedCount: 1,
    manifestUrl: "https://api.example.test/internal/callback-gap-recovery/manifest",
    manifestToken: "manifest-token",
    callbackUrl: "https://api.example.test/internal/callback-gap-recovery/results",
    callbackToken: "callback-token",
  });
  first.claimManifestRun(run.id);
  first.appendManifestItems(run.id, [{
    lifecycleObjectId: "callback-gap-object",
    objectKey: "gateway-media/callback-gap-object.png",
    sourceUrl: "https://api.example.test/local-media/gateway-media/callback-gap-object.png",
    targetTier: "COLD_HOME_MINIO",
    expectedSizeBytes: 8,
  }]);
  first.completeManifest(run.id);
  const itemId = first.db.prepare("SELECT id FROM transfer_items WHERE job_id = ?").get(run.id).id;
  first.updateItem(itemId, {
    status: "SUCCEEDED",
    stage: "COMPLETED",
    homeSizeBytes: 8,
    homeVerifiedAt: new Date().toISOString(),
    finishedAt: new Date().toISOString(),
  });
  assert.equal(first.getJobSummary(run.id).callbackTotalCount, 0);
  first.close();

  const second = new LifecycleStore({ dbPath, encryptionKey: null });
  try {
    const recovered = second.getJobSummary(run.id);
    assert.equal(recovered.status, "RESULTS_PENDING");
    assert.equal(recovered.callbackTotalCount, 1);
    assert.equal(second.listRunnableCallbacks().length, 1);
  } finally {
    second.close();
    await rm(root, { recursive: true, force: true });
  }
});

test("cancelled streaming runs do not deliver callbacks until the same run is resumed", async () => {
  await withStore(({ store }) => {
    const run = store.createStreamingJob({
      id: "cancelled-callback-barrier",
      mediaKind: "GENERATED_MEDIA",
      expectedCount: 2,
      manifestUrl: "https://api.example.test/internal/cancelled-callback-barrier/manifest",
      manifestToken: "manifest-token",
      callbackUrl: "https://api.example.test/internal/cancelled-callback-barrier/results",
      callbackToken: "callback-token",
    });
    store.claimManifestRun(run.id);
    store.appendManifestItems(run.id, [
      {
        lifecycleObjectId: "cancelled-complete-object",
        objectKey: "gateway-media/cancelled-complete.png",
        sourceUrl: "https://api.example.test/local-media/gateway-media/cancelled-complete.png",
        targetTier: "COLD_HOME_MINIO",
        expectedSizeBytes: 8,
      },
      {
        lifecycleObjectId: "cancelled-pending-object",
        objectKey: "gateway-media/cancelled-pending.png",
        sourceUrl: "https://api.example.test/local-media/gateway-media/cancelled-pending.png",
        targetTier: "COLD_HOME_MINIO",
        expectedSizeBytes: 8,
      },
    ]);
    store.completeManifest(run.id);
    const [completeItem, pendingItem] = store.getJob(run.id).items;
    store.updateItem(completeItem.id, {
      status: "SUCCEEDED",
      stage: "COMPLETED",
      homeSizeBytes: 8,
      homeVerifiedAt: new Date().toISOString(),
      finishedAt: new Date().toISOString(),
    });
    store.enqueueItemCallback(completeItem.id);
    store.cancelJob(run.id);

    assert.equal(store.listRunnableCallbacks().length, 0);
    assert.equal(store.getJob(run.id).items.find((item) => item.id === pendingItem.id).status, "CANCELLED");

    const resumed = store.resumeStreamingJob(run.id);
    assert.equal(resumed.status, "RUNNING");
    assert.equal(store.listRunnableCallbacks().length, 0);
    store.updateItem(pendingItem.id, {
      status: "SUCCEEDED",
      stage: "COMPLETED",
      homeSizeBytes: 8,
      homeVerifiedAt: new Date().toISOString(),
      finishedAt: new Date().toISOString(),
    });
    store.enqueueItemCallback(pendingItem.id);
    assert.equal(store.listRunnableCallbacks().length, 2);
  });
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

test("job summaries expose transfer stages and failed file diagnostics", async () => {
  await withStore(({ store }) => {
    const version = store.upsertConfigVersion(ossConfig);
    const job = store.createJob({
      id: "run-stage-diagnostics",
      mediaKind: "GENERATED_MEDIA",
      configVersionId: version.id,
      items: [
        {
          lifecycleObjectId: "object-complete",
          objectKey: "gateway-media/2026/07/complete.png",
          sourceUrl: "https://api.example.test/local-media/gateway-media/2026/07/complete.png",
          targetTier: "WARM_OSS",
          expectedSizeBytes: 12,
        },
        {
          lifecycleObjectId: "object-failed",
          objectKey: "gateway-media/2026/07/failed.png",
          sourceUrl: "https://api.example.test/local-media/gateway-media/2026/07/failed.png",
          targetTier: "WARM_OSS",
          expectedSizeBytes: 18,
        },
      ],
    });
    const completeItem = job.items.find((item) => item.objectKey.endsWith("/complete.png"));
    const failedItem = job.items.find((item) => item.objectKey.endsWith("/failed.png"));
    store.updateItem(completeItem.id, { status: "SUCCEEDED", stage: "COMPLETED" });
    store.updateItem(failedItem.id, {
      status: "FAILED",
      stage: "FAILED",
      error: "Source download failed with HTTP 404.",
    });

    const summary = store.getJobSummaryWithDiagnostics(job.id);
    assert.deepEqual(summary.diagnostics.stageCounts, { COMPLETED: 1, FAILED: 1 });
    assert.deepEqual(summary.diagnostics.targetStageCounts, {
      WARM_OSS: { COMPLETED: 1, FAILED: 1 },
    });
    assert.equal(summary.diagnostics.failedItemCount, 1);
    assert.deepEqual(summary.diagnostics.failedItems.map((item) => ({
      objectKey: item.objectKey,
      stage: item.stage,
      error: item.error,
    })), [{
      objectKey: "gateway-media/2026/07/failed.png",
      stage: "FAILED",
      error: "Source download failed with HTTP 404.",
    }]);
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
