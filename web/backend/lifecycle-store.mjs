import { createDecipheriv, createHash, randomUUID } from "node:crypto";
import { chmodSync, existsSync, mkdirSync, readFileSync } from "node:fs";
import { dirname, resolve } from "node:path";
import Database from "better-sqlite3";

const SCHEMA_VERSION = 4;
const JOB_TERMINAL_STATUSES = new Set(["SUCCEEDED", "SUCCEEDED_WITH_ERRORS", "FAILED", "CANCELLED"]);
const ITEM_TERMINAL_STATUSES = new Set(["SUCCEEDED", "FAILED", "CANCELLED"]);

export class LifecycleConflictError extends Error {
  constructor(message) {
    super(message);
    this.name = "LifecycleConflictError";
    this.statusCode = 409;
  }
}

export class LifecycleValidationError extends Error {
  constructor(message, statusCode = 400) {
    super(message);
    this.name = "LifecycleValidationError";
    this.statusCode = statusCode;
  }
}

function nowIso() {
  return new Date().toISOString();
}

function decodeEncryptionKey(rawValue) {
  const raw = String(rawValue || "").trim();
  if (!raw) return null;

  const candidates = [];
  if (/^[a-f\d]{64}$/i.test(raw)) candidates.push(Buffer.from(raw, "hex"));
  try {
    candidates.push(Buffer.from(raw, "base64"));
  } catch {
    // Invalid base64 is handled by the final length check.
  }
  candidates.push(Buffer.from(raw, "utf8"));

  return candidates.find((candidate) => candidate.length === 32) ?? null;
}

export function loadLifecycleEncryptionKey(env = process.env) {
  const keyFile = String(env.HOME_MINIO_CONFIG_ENCRYPTION_KEY_FILE || "").trim();
  if (keyFile && existsSync(keyFile)) {
    const fileValue = readFileSync(keyFile, "utf8").trim();
    if (fileValue) {
      const key = decodeEncryptionKey(fileValue);
      if (!key) {
        throw new LifecycleValidationError("HOME_MINIO_CONFIG_ENCRYPTION_KEY_FILE must contain exactly 32 bytes, 64 hex characters, or a 32-byte base64 value.", 500);
      }
      return key;
    }
  }

  const configured = String(env.HOME_MINIO_CONFIG_ENCRYPTION_KEY || "").trim();
  if (!configured) return null;
  const key = decodeEncryptionKey(configured);
  if (!key) {
    throw new LifecycleValidationError("HOME_MINIO_CONFIG_ENCRYPTION_KEY must contain exactly 32 bytes, 64 hex characters, or a 32-byte base64 value.", 500);
  }
  return key;
}

function canonicalOssConfig(input) {
  if (!input || typeof input !== "object" || Array.isArray(input)) {
    throw new LifecycleValidationError("OSS config is required.");
  }
  const bucket = String(input.bucket || "").trim();
  const region = String(input.region || "").trim();
  const endpoint = String(input.endpoint || "").trim().replace(/^https?:\/\//i, "").replace(/\/+$/, "");
  const accessKeyId = String(input.accessKeyId || "").trim();
  const accessKeySecret = String(input.accessKeySecret || "").trim();
  const publicBaseUrl = String(input.publicBaseUrl || "").trim().replace(/\/+$/, "");
  if (!bucket || !region || !endpoint || !accessKeyId || !accessKeySecret) {
    throw new LifecycleValidationError("OSS bucket, region, endpoint, accessKeyId and accessKeySecret are required.");
  }
  if (publicBaseUrl) {
    const parsed = new URL(publicBaseUrl);
    if (parsed.protocol !== "https:" && parsed.protocol !== "http:") {
      throw new LifecycleValidationError("OSS publicBaseUrl must use http or https.");
    }
  }
  return { bucket, region, endpoint, accessKeyId, accessKeySecret, publicBaseUrl };
}

function decryptJson(value, key) {
  if (!key) {
    throw new LifecycleValidationError("旧版加密 OSS 配置需要原加密密钥才能恢复。", 500);
  }
  const [version, ivRaw, tagRaw, ciphertextRaw] = String(value || "").split(".");
  if (version !== "v1" || !ivRaw || !tagRaw || !ciphertextRaw) {
    throw new LifecycleValidationError("Stored OSS credential payload is invalid.", 500);
  }
  try {
    const decipher = createDecipheriv("aes-256-gcm", key, Buffer.from(ivRaw, "base64url"));
    decipher.setAuthTag(Buffer.from(tagRaw, "base64url"));
    const plaintext = Buffer.concat([
      decipher.update(Buffer.from(ciphertextRaw, "base64url")),
      decipher.final(),
    ]);
    return JSON.parse(plaintext.toString("utf8"));
  } catch {
    throw new LifecycleValidationError("Stored OSS credentials cannot be decrypted with the configured master key.", 500);
  }
}

function serializeConfigPayload(value) {
  return `plain.${Buffer.from(JSON.stringify(value), "utf8").toString("base64url")}`;
}

function deserializeConfigPayload(value, legacyEncryptionKey) {
  const payload = String(value || "");
  if (payload.startsWith("plain.")) {
    try {
      return JSON.parse(Buffer.from(payload.slice("plain.".length), "base64url").toString("utf8"));
    } catch {
      throw new LifecycleValidationError("Stored OSS credential payload is invalid.", 500);
    }
  }
  if (payload.startsWith("v1.")) {
    return decryptJson(payload, legacyEncryptionKey);
  }
  try {
    return JSON.parse(payload);
  } catch {
    throw new LifecycleValidationError("Stored OSS credential payload is invalid.", 500);
  }
}

function requestFingerprint(payload) {
  return createHash("sha256").update(JSON.stringify(payload)).digest("hex");
}

function normalizeMediaKind(value) {
  return value === "WORKFLOW_UPLOAD" ? "WORKFLOW_UPLOAD" : value === "GENERATED_MEDIA" ? "GENERATED_MEDIA" : "";
}

function normalizeHttpUrl(value, label) {
  try {
    const parsed = new URL(String(value || "").trim());
    if (parsed.protocol !== "http:" && parsed.protocol !== "https:") throw new Error("invalid protocol");
    return parsed.toString();
  } catch {
    throw new LifecycleValidationError(`${label} must be a valid HTTP URL.`);
  }
}

function normalizeLifecycleItem(item, index = 0) {
  const lifecycleObjectId = String(item?.lifecycleObjectId || "").trim();
  const objectKey = String(item?.objectKey || "").trim().replace(/\\/g, "/").replace(/^\/+/, "");
  const targetTier = item?.targetTier === "WARM_OSS" ? "WARM_OSS" : item?.targetTier === "COLD_HOME_MINIO" ? "COLD_HOME_MINIO" : "";
  if (!lifecycleObjectId || !objectKey || objectKey.split("/").some((segment) => !segment || segment === "." || segment === "..") || !targetTier) {
    throw new LifecycleValidationError(`Lifecycle item ${index + 1} is invalid.`);
  }
  const sourceUrl = normalizeHttpUrl(item?.sourceUrl, `Lifecycle item ${index + 1} sourceUrl`);
  const expectedSizeBytes = item?.expectedSizeBytes == null ? null : Number(item.expectedSizeBytes);
  if (expectedSizeBytes !== null && (!Number.isSafeInteger(expectedSizeBytes) || expectedSizeBytes < 0)) {
    throw new LifecycleValidationError(`Lifecycle item ${index + 1} expectedSizeBytes is invalid.`);
  }
  return {
    lifecycleObjectId,
    objectKey,
    sourceUrl,
    targetTier,
    expectedSizeBytes,
    expectedSha256: typeof item?.expectedSha256 === "string" && /^[a-f\d]{64}$/i.test(item.expectedSha256)
      ? item.expectedSha256.toLowerCase()
      : null,
    mimeType: typeof item?.mimeType === "string" && item.mimeType.trim() ? item.mimeType.trim().slice(0, 191) : null,
  };
}

function parseJson(value, fallback = null) {
  if (!value) return fallback;
  try {
    return JSON.parse(value);
  } catch {
    return fallback;
  }
}

function serializeConfigVersion(row) {
  if (!row) return null;
  return {
    id: row.id,
    fingerprint: row.fingerprint,
    bucket: row.bucket,
    region: row.region,
    endpoint: row.endpoint,
    publicBaseUrl: row.public_base_url || "",
    createdAt: row.created_at,
  };
}

function serializeItem(row) {
  if (!row) return null;
  return {
    id: row.id,
    jobId: row.job_id,
    lifecycleObjectId: row.lifecycle_object_id,
    objectKey: row.object_key,
    sourceUrl: row.source_url,
    targetTier: row.target_tier,
    status: row.status,
    stage: row.stage,
    attemptCount: row.attempt_count,
    nextRetryAt: row.next_retry_at,
    expectedSizeBytes: row.expected_size_bytes,
    expectedSha256: row.expected_sha256,
    mimeType: row.mime_type,
    home: row.home_verified_at
      ? {
          objectKey: row.object_key,
          sizeBytes: row.home_size_bytes,
          etag: row.home_etag,
          sha256: row.home_sha256,
          reused: row.home_reused == null ? null : Boolean(row.home_reused),
          verifiedAt: row.home_verified_at,
        }
      : null,
    oss: row.oss_verified_at
      ? {
          bucket: row.oss_bucket,
          objectKey: row.object_key,
          sizeBytes: row.oss_size_bytes,
          etag: row.oss_etag,
          directUrl: row.oss_direct_url,
          selectedUrl: row.oss_selected_url,
          cdnVerified: Boolean(row.oss_cdn_verified),
          verifiedAt: row.oss_verified_at,
        }
      : null,
    warning: row.warning,
    error: row.error,
    failureKind: row.failure_kind || null,
    failureStatusCode: row.failure_status_code ?? null,
    retryable: row.retryable == null ? null : Boolean(row.retryable),
    startedAt: row.started_at,
    finishedAt: row.finished_at,
    updatedAt: row.updated_at,
  };
}

function serializeJob(row, items = []) {
  if (!row) return null;
  return {
    id: row.id,
    mediaKind: row.media_kind,
    configVersionId: row.config_version_id,
    status: row.status,
    totalCount: row.total_count,
    processedCount: row.processed_count,
    succeededCount: row.succeeded_count,
    failedCount: row.failed_count,
    totalBytes: row.total_bytes,
    processedBytes: row.processed_bytes,
    error: row.error,
    manifestStatus: row.manifest_status || "SEALED",
    expectedCount: row.expected_count ?? row.total_count,
    manifestAcceptedCount: row.manifest_accepted_count ?? row.total_count,
    createdAt: row.created_at,
    startedAt: row.started_at,
    finishedAt: row.finished_at,
    updatedAt: row.updated_at,
    items,
  };
}

function serializeCallback(row) {
  if (!row) return null;
  return {
    id: row.id,
    runId: row.run_id,
    itemId: row.item_id,
    sequence: row.sequence,
    status: row.status,
    payload: parseJson(row.payload_json, {}),
    attemptCount: row.attempt_count,
    nextRetryAt: row.next_retry_at,
    error: row.error,
    createdAt: row.created_at,
    deliveredAt: row.delivered_at,
    updatedAt: row.updated_at,
  };
}

function serializeCachePushJob(row) {
  if (!row) return null;
  const request = parseJson(row.request_json, {});
  return {
    id: row.id,
    objectKey: row.object_key,
    status: row.status,
    newWauleApiUrl: request.newWauleApiUrl || "",
    cacheUploadBaseUrl: request.cacheUploadBaseUrl || "",
    attemptCount: row.attempt_count,
    nextRetryAt: row.next_retry_at,
    error: row.error,
    createdAt: row.created_at,
    startedAt: row.started_at,
    finishedAt: row.finished_at,
    updatedAt: row.updated_at,
  };
}

export class LifecycleStore {
  constructor(options = {}) {
    this.dbPath = resolve(options.dbPath || process.env.HOME_MINIO_STATE_DB || "./state/home-minio.sqlite");
    if (options.encryptionKey === undefined) {
      try {
        this.legacyEncryptionKey = loadLifecycleEncryptionKey();
      } catch {
        this.legacyEncryptionKey = null;
      }
    } else {
      this.legacyEncryptionKey = options.encryptionKey;
    }
    mkdirSync(dirname(this.dbPath), { recursive: true, mode: 0o700 });
    try {
      chmodSync(dirname(this.dbPath), 0o700);
    } catch {
      // Some mounted filesystems do not support POSIX modes.
    }
    this.db = new Database(this.dbPath);
    this.secureStateFiles();
    this.db.pragma("journal_mode = WAL");
    this.db.pragma("foreign_keys = ON");
    this.db.pragma("busy_timeout = 5000");
    this.migrate();
    this.migrateLegacyCredentialPayloads();
    this.recoverInterruptedWork();
    this.secureStateFiles();
  }

  secureStateFiles() {
    for (const path of [this.dbPath, `${this.dbPath}-wal`, `${this.dbPath}-shm`]) {
      if (!existsSync(path)) continue;
      try {
        chmodSync(path, 0o600);
      } catch {
        // Some mounted filesystems do not support POSIX modes.
      }
    }
  }

  migrateLegacyCredentialPayloads() {
    if (!this.legacyEncryptionKey) return;
    const rows = this.db.prepare(`
      SELECT id, encrypted_payload
      FROM oss_config_versions
      WHERE encrypted_payload LIKE 'v1.%'
    `).all();
    if (!rows.length) return;
    const update = this.db.prepare("UPDATE oss_config_versions SET encrypted_payload = ? WHERE id = ?");
    const migrate = this.db.transaction(() => {
      for (const row of rows) {
        update.run(serializeConfigPayload(decryptJson(row.encrypted_payload, this.legacyEncryptionKey)), row.id);
      }
    });
    migrate();
  }

  migrate() {
    this.db.exec(`
      CREATE TABLE IF NOT EXISTS schema_migrations (
        version INTEGER PRIMARY KEY,
        applied_at TEXT NOT NULL
      );
    `);
    let current = this.db.prepare("SELECT COALESCE(MAX(version), 0) AS version FROM schema_migrations").get().version;
    if (current < 1) {
      this.db.exec(`
        CREATE TABLE oss_config_versions (
          id TEXT PRIMARY KEY,
          fingerprint TEXT NOT NULL UNIQUE,
          encrypted_payload TEXT NOT NULL,
          bucket TEXT NOT NULL,
          region TEXT NOT NULL,
          endpoint TEXT NOT NULL,
          public_base_url TEXT,
          created_at TEXT NOT NULL
        );

        CREATE TABLE transfer_jobs (
          id TEXT PRIMARY KEY,
          request_fingerprint TEXT NOT NULL,
          media_kind TEXT NOT NULL CHECK (media_kind IN ('WORKFLOW_UPLOAD', 'GENERATED_MEDIA')),
          config_version_id TEXT REFERENCES oss_config_versions(id),
          status TEXT NOT NULL,
          total_count INTEGER NOT NULL DEFAULT 0,
          processed_count INTEGER NOT NULL DEFAULT 0,
          succeeded_count INTEGER NOT NULL DEFAULT 0,
          failed_count INTEGER NOT NULL DEFAULT 0,
          total_bytes INTEGER NOT NULL DEFAULT 0,
          processed_bytes INTEGER NOT NULL DEFAULT 0,
          error TEXT,
          created_at TEXT NOT NULL,
          started_at TEXT,
          finished_at TEXT,
          updated_at TEXT NOT NULL
        );

        CREATE TABLE transfer_items (
          id TEXT PRIMARY KEY,
          job_id TEXT NOT NULL REFERENCES transfer_jobs(id) ON DELETE CASCADE,
          lifecycle_object_id TEXT NOT NULL,
          object_key TEXT NOT NULL,
          source_url TEXT NOT NULL,
          target_tier TEXT NOT NULL CHECK (target_tier IN ('WARM_OSS', 'COLD_HOME_MINIO')),
          expected_size_bytes INTEGER,
          expected_sha256 TEXT,
          mime_type TEXT,
          status TEXT NOT NULL,
          stage TEXT NOT NULL,
          attempt_count INTEGER NOT NULL DEFAULT 0,
          next_retry_at TEXT,
          home_size_bytes INTEGER,
          home_etag TEXT,
          home_sha256 TEXT,
          home_verified_at TEXT,
          oss_bucket TEXT,
          oss_size_bytes INTEGER,
          oss_etag TEXT,
          oss_direct_url TEXT,
          oss_selected_url TEXT,
          oss_cdn_verified INTEGER NOT NULL DEFAULT 0,
          oss_verified_at TEXT,
          checkpoint_json TEXT,
          warning TEXT,
          error TEXT,
          started_at TEXT,
          finished_at TEXT,
          created_at TEXT NOT NULL,
          updated_at TEXT NOT NULL,
          UNIQUE(job_id, lifecycle_object_id)
        );

        CREATE INDEX transfer_items_runnable_idx ON transfer_items(status, next_retry_at, created_at);
        CREATE INDEX transfer_items_object_key_idx ON transfer_items(object_key);

        CREATE TABLE multipart_parts (
          item_id TEXT NOT NULL REFERENCES transfer_items(id) ON DELETE CASCADE,
          part_number INTEGER NOT NULL,
          etag TEXT NOT NULL,
          size_bytes INTEGER,
          updated_at TEXT NOT NULL,
          PRIMARY KEY(item_id, part_number)
        );

        CREATE TABLE cache_push_jobs (
          id TEXT PRIMARY KEY,
          object_key TEXT NOT NULL,
          status TEXT NOT NULL,
          request_json TEXT NOT NULL,
          attempt_count INTEGER NOT NULL DEFAULT 0,
          next_retry_at TEXT,
          error TEXT,
          created_at TEXT NOT NULL,
          started_at TEXT,
          finished_at TEXT,
          updated_at TEXT NOT NULL
        );

        CREATE INDEX cache_push_jobs_runnable_idx ON cache_push_jobs(status, next_retry_at, created_at);
      `);
      this.db.prepare("INSERT INTO schema_migrations(version, applied_at) VALUES (?, ?)").run(1, nowIso());
      current = 1;
    }
    if (current < 2) {
      this.db.transaction(() => {
        this.db.exec("ALTER TABLE transfer_items ADD COLUMN home_reused INTEGER;");
        this.db.prepare("INSERT INTO schema_migrations(version, applied_at) VALUES (?, ?)").run(2, nowIso());
      })();
      current = 2;
    }
    if (current < 3) {
      this.db.transaction(() => {
        this.db.exec(`
          ALTER TABLE transfer_jobs ADD COLUMN manifest_url TEXT;
          ALTER TABLE transfer_jobs ADD COLUMN manifest_token TEXT;
          ALTER TABLE transfer_jobs ADD COLUMN callback_url TEXT;
          ALTER TABLE transfer_jobs ADD COLUMN callback_token TEXT;
          ALTER TABLE transfer_jobs ADD COLUMN manifest_status TEXT NOT NULL DEFAULT 'SEALED';
          ALTER TABLE transfer_jobs ADD COLUMN expected_count INTEGER;
          ALTER TABLE transfer_jobs ADD COLUMN manifest_accepted_count INTEGER NOT NULL DEFAULT 0;
          ALTER TABLE transfer_jobs ADD COLUMN manifest_attempt_count INTEGER NOT NULL DEFAULT 0;
          ALTER TABLE transfer_jobs ADD COLUMN manifest_next_retry_at TEXT;
          ALTER TABLE transfer_jobs ADD COLUMN last_callback_sequence INTEGER NOT NULL DEFAULT 0;

          ALTER TABLE transfer_items ADD COLUMN failure_kind TEXT;
          ALTER TABLE transfer_items ADD COLUMN failure_status_code INTEGER;
          ALTER TABLE transfer_items ADD COLUMN retryable INTEGER;

          CREATE TABLE callback_outbox (
            id TEXT PRIMARY KEY,
            run_id TEXT NOT NULL REFERENCES transfer_jobs(id) ON DELETE CASCADE,
            item_id TEXT NOT NULL REFERENCES transfer_items(id) ON DELETE CASCADE,
            sequence INTEGER NOT NULL,
            status TEXT NOT NULL,
            payload_json TEXT NOT NULL,
            attempt_count INTEGER NOT NULL DEFAULT 0,
            next_retry_at TEXT,
            error TEXT,
            created_at TEXT NOT NULL,
            delivered_at TEXT,
            updated_at TEXT NOT NULL,
            UNIQUE(run_id, item_id),
            UNIQUE(run_id, sequence)
          );

          CREATE INDEX callback_outbox_runnable_idx
            ON callback_outbox(status, next_retry_at, created_at);
          CREATE INDEX transfer_jobs_manifest_runnable_idx
            ON transfer_jobs(manifest_status, manifest_next_retry_at, created_at);
        `);
        this.db.prepare("INSERT INTO schema_migrations(version, applied_at) VALUES (?, ?)").run(3, nowIso());
      })();
      current = 3;
    }
    if (current < 4) {
      this.db.transaction(() => {
        const migratedAt = nowIso();
        this.db.prepare(`
          UPDATE callback_outbox
          SET status = 'QUEUED', attempt_count = 0, next_retry_at = NULL,
              error = NULL, updated_at = ?
          WHERE status = 'FAILED'
            AND run_id IN (
              SELECT jobs.id
              FROM transfer_jobs jobs
              WHERE jobs.callback_url IS NOT NULL
                AND jobs.manifest_status = 'SEALED'
                AND NOT EXISTS (
                  SELECT 1 FROM transfer_items items
                  WHERE items.job_id = jobs.id
                    AND items.status IN ('QUEUED', 'RUNNING', 'RETRY_WAIT')
                )
            )
        `).run(migratedAt);
        this.db.prepare(`
          UPDATE transfer_jobs
          SET status = 'RESULTS_PENDING', error = NULL, finished_at = NULL, updated_at = ?
          WHERE callback_url IS NOT NULL
            AND manifest_status = 'SEALED'
            AND NOT EXISTS (
              SELECT 1 FROM transfer_items items
              WHERE items.job_id = transfer_jobs.id
                AND items.status IN ('QUEUED', 'RUNNING', 'RETRY_WAIT')
            )
            AND EXISTS (
              SELECT 1 FROM callback_outbox callbacks
              WHERE callbacks.run_id = transfer_jobs.id
                AND callbacks.status != 'DELIVERED'
            )
        `).run(migratedAt);
        this.db.prepare("INSERT INTO schema_migrations(version, applied_at) VALUES (?, ?)").run(4, migratedAt);
      })();
    }
  }

  recoverInterruptedWork() {
    const now = nowIso();
    this.db.prepare(`
      UPDATE transfer_jobs
      SET manifest_status = 'RETRY_WAIT', manifest_next_retry_at = ?, status = 'INGESTING',
          error = COALESCE(error, 'Recovered manifest ingestion after process restart'), updated_at = ?
      WHERE manifest_status = 'FETCHING'
    `).run(now, now);
    this.db.prepare(`
      UPDATE transfer_items
      SET status = 'RETRY_WAIT', next_retry_at = ?, error = COALESCE(error, 'Recovered after process restart'), updated_at = ?
      WHERE status = 'RUNNING'
    `).run(now, now);
    this.db.prepare(`
      UPDATE transfer_jobs
      SET status = 'QUEUED', error = NULL, updated_at = ?
      WHERE status = 'RUNNING'
    `).run(now);
    this.db.prepare(`
      UPDATE cache_push_jobs
      SET status = 'QUEUED', next_retry_at = NULL, error = COALESCE(error, 'Recovered after process restart'), updated_at = ?
      WHERE status = 'RUNNING'
    `).run(now);
    this.db.prepare(`
      UPDATE callback_outbox
      SET status = 'RETRY_WAIT', next_retry_at = ?, error = COALESCE(error, 'Recovered after process restart'), updated_at = ?
      WHERE status = 'SENDING'
    `).run(now, now);

    // A process can stop after persisting an item's terminal state but before
    // creating its outbox row. Rebuild those rows so result delivery cannot
    // remain permanently blocked by the all-items barrier.
    const missingCallbacks = this.db.prepare(`
      SELECT items.id
      FROM transfer_items items
      JOIN transfer_jobs jobs ON jobs.id = items.job_id
      LEFT JOIN callback_outbox callbacks
        ON callbacks.run_id = items.job_id AND callbacks.item_id = items.id
      WHERE jobs.callback_url IS NOT NULL
        AND jobs.callback_token IS NOT NULL
        AND jobs.manifest_status = 'SEALED'
        AND jobs.status NOT IN ('CANCELLED', 'FAILED')
        AND items.status IN ('SUCCEEDED', 'FAILED', 'CANCELLED')
        AND callbacks.id IS NULL
      ORDER BY jobs.created_at, items.created_at, items.id
    `).all();
    for (const item of missingCallbacks) this.enqueueItemCallback(item.id);
  }

  close() {
    this.db.close();
  }

  health() {
    const counts = this.db.prepare(`
      SELECT
        (SELECT COUNT(*) FROM transfer_jobs) AS jobs,
        (SELECT COUNT(*) FROM transfer_items) AS items,
        (SELECT COUNT(*) FROM transfer_items WHERE status IN ('QUEUED', 'RUNNING', 'RETRY_WAIT')) AS active_items
    `).get();
    const legacyEncryptedConfigs = this.db.prepare(`
      SELECT COUNT(*) AS count
      FROM oss_config_versions
      WHERE encrypted_payload LIKE 'v1.%'
    `).get().count;
    return {
      ready: true,
      dbPath: this.dbPath,
      schemaVersion: SCHEMA_VERSION,
      jobs: counts.jobs,
      items: counts.items,
      activeItems: counts.active_items,
      credentialStorage: "LOCAL_SQLITE",
      legacyEncryptedConfigs,
      encryptionError: null,
    };
  }

  upsertConfigVersion(input) {
    const config = canonicalOssConfig(input);
    const fingerprint = createHash("sha256").update(JSON.stringify(config)).digest("hex");
    const existing = this.db.prepare("SELECT * FROM oss_config_versions WHERE fingerprint = ?").get(fingerprint);
    if (existing) return serializeConfigVersion(existing);

    const id = randomUUID();
    const createdAt = nowIso();
    this.db.prepare(`
      INSERT INTO oss_config_versions(
        id, fingerprint, encrypted_payload, bucket, region, endpoint, public_base_url, created_at
      ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    `).run(
      id,
      fingerprint,
      serializeConfigPayload(config),
      config.bucket,
      config.region,
      config.endpoint,
      config.publicBaseUrl || null,
      createdAt,
    );
    return serializeConfigVersion(this.db.prepare("SELECT * FROM oss_config_versions WHERE id = ?").get(id));
  }

  getConfigVersion(id) {
    const row = this.db.prepare("SELECT * FROM oss_config_versions WHERE id = ?").get(id);
    return serializeConfigVersion(row);
  }

  getDecryptedConfig(id) {
    const row = this.db.prepare("SELECT * FROM oss_config_versions WHERE id = ?").get(id);
    if (!row) throw new LifecycleValidationError("OSS config version not found.", 404);
    return deserializeConfigPayload(row.encrypted_payload, this.legacyEncryptionKey);
  }

  createStreamingJob(input) {
    if (!input || typeof input !== "object" || Array.isArray(input)) {
      throw new LifecycleValidationError("Lifecycle run payload is required.");
    }
    const id = String(input.id || "").trim();
    const mediaKind = normalizeMediaKind(input.mediaKind);
    const configVersionId = input.configVersionId ? String(input.configVersionId).trim() : null;
    const expectedCount = Number(input.expectedCount);
    if (!id || id.length > 191 || !mediaKind || !Number.isSafeInteger(expectedCount) || expectedCount < 0) {
      throw new LifecycleValidationError("Lifecycle run requires a valid id, mediaKind and expectedCount.");
    }
    if (configVersionId && !this.getConfigVersion(configVersionId)) {
      throw new LifecycleValidationError("OSS config version not found.", 404);
    }
    const manifestUrl = normalizeHttpUrl(input.manifestUrl, "manifestUrl");
    const callbackUrl = normalizeHttpUrl(input.callbackUrl, "callbackUrl");
    const manifestToken = String(input.manifestToken || "").trim();
    const callbackToken = String(input.callbackToken || "").trim();
    if (!manifestToken || manifestToken.length > 4096 || !callbackToken || callbackToken.length > 4096) {
      throw new LifecycleValidationError("Lifecycle run tokens are required.");
    }
    const normalizedRequest = { id, mediaKind, configVersionId, expectedCount, manifestUrl, callbackUrl };
    const fingerprint = requestFingerprint(normalizedRequest);
    const existing = this.db.prepare("SELECT * FROM transfer_jobs WHERE id = ?").get(id);
    if (existing) {
      if (existing.request_fingerprint !== fingerprint) {
        throw new LifecycleConflictError("Lifecycle run id already exists with different metadata.");
      }
      return this.getJobSummary(id);
    }

    const createdAt = nowIso();
    this.db.prepare(`
      INSERT INTO transfer_jobs(
        id, request_fingerprint, media_kind, config_version_id, status,
        total_count, total_bytes, manifest_url, manifest_token, callback_url, callback_token,
        manifest_status, expected_count, created_at, updated_at
      ) VALUES (?, ?, ?, ?, 'INGESTING', 0, 0, ?, ?, ?, ?, 'PENDING', ?, ?, ?)
    `).run(
      id,
      fingerprint,
      mediaKind,
      configVersionId,
      manifestUrl,
      manifestToken,
      callbackUrl,
      callbackToken,
      expectedCount,
      createdAt,
      createdAt,
    );
    return this.getJobSummary(id);
  }

  getJobSummary(id) {
    const job = serializeJob(this.db.prepare("SELECT * FROM transfer_jobs WHERE id = ?").get(id));
    return job ? { ...job, ...this.getCallbackDeliverySummary(id) } : null;
  }

  getCallbackDeliverySummary(runId) {
    const row = this.db.prepare(`
      SELECT
        COUNT(*) AS callback_total_count,
        SUM(CASE WHEN status = 'DELIVERED' THEN 1 ELSE 0 END) AS callback_delivered_count,
        SUM(CASE WHEN status IN ('QUEUED', 'SENDING', 'RETRY_WAIT') THEN 1 ELSE 0 END) AS callback_pending_count,
        SUM(CASE WHEN status = 'FAILED' THEN 1 ELSE 0 END) AS callback_failed_count
      FROM callback_outbox
      WHERE run_id = ?
    `).get(runId);
    return {
      callbackTotalCount: Number(row?.callback_total_count || 0),
      callbackDeliveredCount: Number(row?.callback_delivered_count || 0),
      callbackPendingCount: Number(row?.callback_pending_count || 0),
      callbackFailedCount: Number(row?.callback_failed_count || 0),
    };
  }

  getJobControl(id) {
    return this.db.prepare("SELECT * FROM transfer_jobs WHERE id = ?").get(id) || null;
  }

  listManifestRuns(limit = 10) {
    return this.db.prepare(`
      SELECT id FROM transfer_jobs
      WHERE manifest_status = 'PENDING'
         OR (manifest_status = 'RETRY_WAIT' AND (manifest_next_retry_at IS NULL OR manifest_next_retry_at <= ?))
      ORDER BY created_at, id
      LIMIT ?
    `).all(nowIso(), Math.max(1, Math.min(100, limit))).map((row) => row.id);
  }

  claimManifestRun(id) {
    const now = nowIso();
    const result = this.db.prepare(`
      UPDATE transfer_jobs
      SET manifest_status = 'FETCHING', status = 'INGESTING', manifest_attempt_count = manifest_attempt_count + 1,
          manifest_next_retry_at = NULL, error = NULL, started_at = COALESCE(started_at, ?), updated_at = ?
      WHERE id = ? AND (manifest_status = 'PENDING' OR manifest_status = 'RETRY_WAIT')
    `).run(now, now, id);
    return result.changes ? this.getJobControl(id) : null;
  }

  appendManifestItems(runId, rawItems) {
    const job = this.getJobControl(runId);
    if (!job || job.manifest_status !== "FETCHING") {
      throw new LifecycleConflictError("Lifecycle manifest is not accepting items.");
    }
    const items = Array.isArray(rawItems) ? rawItems.map((item, index) => normalizeLifecycleItem(item, index)) : [];
    if (!items.length) return this.getJobSummary(runId);
    if (items.some((item) => item.targetTier === "WARM_OSS") && !job.config_version_id) {
      throw new LifecycleValidationError("WARM_OSS items require configVersionId.");
    }
    const insertedAt = nowIso();
    this.db.transaction(() => {
      const find = this.db.prepare("SELECT * FROM transfer_items WHERE job_id = ? AND lifecycle_object_id = ?");
      const insert = this.db.prepare(`
        INSERT INTO transfer_items(
          id, job_id, lifecycle_object_id, object_key, source_url, target_tier,
          expected_size_bytes, expected_sha256, mime_type, status, stage, created_at, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'QUEUED', 'PENDING', ?, ?)
      `);
      for (const item of items) {
        const existing = find.get(runId, item.lifecycleObjectId);
        if (existing) {
          const same = existing.object_key === item.objectKey
            && existing.source_url === item.sourceUrl
            && existing.target_tier === item.targetTier
            && existing.expected_size_bytes === item.expectedSizeBytes
            && existing.expected_sha256 === item.expectedSha256
            && existing.mime_type === item.mimeType;
          if (!same) throw new LifecycleConflictError(`Manifest item ${item.lifecycleObjectId} changed during retry.`);
          continue;
        }
        insert.run(
          randomUUID(), runId, item.lifecycleObjectId, item.objectKey, item.sourceUrl, item.targetTier,
          item.expectedSizeBytes, item.expectedSha256, item.mimeType, insertedAt, insertedAt,
        );
      }
      const totals = this.db.prepare(`
        SELECT COUNT(*) AS count, COALESCE(SUM(COALESCE(expected_size_bytes, 0)), 0) AS bytes
        FROM transfer_items WHERE job_id = ?
      `).get(runId);
      this.db.prepare(`
        UPDATE transfer_jobs
        SET total_count = ?, total_bytes = ?, manifest_accepted_count = ?, updated_at = ?
        WHERE id = ?
      `).run(totals.count, totals.bytes, totals.count, nowIso(), runId);
    })();
    return this.getJobSummary(runId);
  }

  completeManifest(runId) {
    const job = this.getJobControl(runId);
    if (!job || job.manifest_status !== "FETCHING") return this.getJobSummary(runId);
    const actualCount = this.db.prepare("SELECT COUNT(*) AS count FROM transfer_items WHERE job_id = ?").get(runId).count;
    if (job.expected_count !== null && actualCount !== job.expected_count) {
      throw new LifecycleValidationError(`Manifest count mismatch: expected ${job.expected_count}, received ${actualCount}.`);
    }
    const now = nowIso();
    this.db.prepare(`
      UPDATE transfer_jobs
      SET manifest_status = 'SEALED', status = ?, manifest_accepted_count = ?,
          finished_at = CASE WHEN ? = 0 THEN ? ELSE NULL END, updated_at = ?
      WHERE id = ?
    `).run(actualCount === 0 ? "SUCCEEDED" : "QUEUED", actualCount, actualCount, now, now, runId);
    return this.getJobSummary(runId);
  }

  failManifest(runId, error, retryable = true) {
    const job = this.getJobControl(runId);
    if (!job) return null;
    if (job.manifest_status === "CANCELLED") return this.getJobSummary(runId);
    const exhausted = !retryable || job.manifest_attempt_count >= 10;
    const delayMs = Math.min(6 * 60 * 60 * 1000, 60_000 * 2 ** Math.max(0, job.manifest_attempt_count - 1));
    const now = nowIso();
    this.db.prepare(`
      UPDATE transfer_jobs
      SET manifest_status = ?, status = ?, manifest_next_retry_at = ?, error = ?,
          finished_at = CASE WHEN ? THEN ? ELSE NULL END, updated_at = ?
      WHERE id = ?
    `).run(
      exhausted ? "FAILED" : "RETRY_WAIT",
      exhausted ? "FAILED" : "INGESTING",
      exhausted ? null : new Date(Date.now() + delayMs).toISOString(),
      String(error || "Manifest ingestion failed").slice(0, 4000),
      exhausted ? 1 : 0,
      now,
      now,
      runId,
    );
    return this.getJobSummary(runId);
  }

  createJob(input) {
    if (!input || typeof input !== "object" || Array.isArray(input)) {
      throw new LifecycleValidationError("Lifecycle job payload is required.");
    }
    const id = String(input.id || "").trim();
    const mediaKind = normalizeMediaKind(input.mediaKind);
    const configVersionId = input.configVersionId ? String(input.configVersionId).trim() : null;
    const items = Array.isArray(input.items) ? input.items : [];
    if (!id || id.length > 191 || !mediaKind || items.length < 1 || items.length > 100) {
      throw new LifecycleValidationError("Lifecycle job requires a valid id, mediaKind and 1-100 items.");
    }
    if (items.some((item) => item?.targetTier === "WARM_OSS") && !configVersionId) {
      throw new LifecycleValidationError("WARM_OSS items require configVersionId.");
    }
    if (configVersionId && !this.getConfigVersion(configVersionId)) {
      throw new LifecycleValidationError("OSS config version not found.", 404);
    }

    const normalizedItems = items.map((item, index) => normalizeLifecycleItem(item, index));
    const uniqueLifecycleIds = new Set(normalizedItems.map((item) => item.lifecycleObjectId));
    if (uniqueLifecycleIds.size !== normalizedItems.length) {
      throw new LifecycleValidationError("Lifecycle job contains duplicate lifecycleObjectId values.");
    }

    const normalizedRequest = { id, mediaKind, configVersionId, items: normalizedItems };
    const fingerprint = requestFingerprint(normalizedRequest);
    const existing = this.db.prepare("SELECT * FROM transfer_jobs WHERE id = ?").get(id);
    if (existing) {
      if (existing.request_fingerprint !== fingerprint) {
        throw new LifecycleConflictError("Lifecycle job id already exists with a different payload.");
      }
      return this.getJob(id);
    }

    const createdAt = nowIso();
    const insert = this.db.transaction(() => {
      this.db.prepare(`
        INSERT INTO transfer_jobs(
          id, request_fingerprint, media_kind, config_version_id, status,
          total_count, total_bytes, created_at, updated_at
        ) VALUES (?, ?, ?, ?, 'QUEUED', ?, ?, ?, ?)
      `).run(
        id,
        fingerprint,
        mediaKind,
        configVersionId,
        normalizedItems.length,
        normalizedItems.reduce((total, item) => total + (item.expectedSizeBytes || 0), 0),
        createdAt,
        createdAt,
      );
      const statement = this.db.prepare(`
        INSERT INTO transfer_items(
          id, job_id, lifecycle_object_id, object_key, source_url, target_tier,
          expected_size_bytes, expected_sha256, mime_type, status, stage, created_at, updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'QUEUED', 'PENDING', ?, ?)
      `);
      for (const item of normalizedItems) {
        statement.run(
          randomUUID(), id, item.lifecycleObjectId, item.objectKey, item.sourceUrl, item.targetTier,
          item.expectedSizeBytes, item.expectedSha256, item.mimeType, createdAt, createdAt,
        );
      }
    });
    insert();
    return this.getJob(id);
  }

  getJob(id) {
    const row = this.db.prepare("SELECT * FROM transfer_jobs WHERE id = ?").get(id);
    if (!row) return null;
    const items = this.db.prepare("SELECT * FROM transfer_items WHERE job_id = ? ORDER BY created_at, id").all(id).map(serializeItem);
    return { ...serializeJob(row, items), ...this.getCallbackDeliverySummary(id) };
  }

  getJobDiagnostics(id) {
    const statusRows = this.db.prepare(`
      SELECT status, COUNT(*) AS count
      FROM transfer_items
      WHERE job_id = ?
      GROUP BY status
    `).all(id);
    const stageRows = this.db.prepare(`
      SELECT stage, COUNT(*) AS count
      FROM transfer_items
      WHERE job_id = ?
      GROUP BY stage
    `).all(id);
    const retry = this.db.prepare(`
      SELECT MIN(next_retry_at) AS next_retry_at
      FROM transfer_items
      WHERE job_id = ? AND status = 'RETRY_WAIT' AND next_retry_at IS NOT NULL
    `).get(id);
    const errorRows = this.db.prepare(`
      SELECT error, COUNT(*) AS count
      FROM transfer_items
      WHERE job_id = ? AND error IS NOT NULL AND error != ''
      GROUP BY error
      ORDER BY count DESC, error
      LIMIT 5
    `).all(id);
    return {
      statusCounts: Object.fromEntries(statusRows.map((row) => [row.status, row.count])),
      stageCounts: Object.fromEntries(stageRows.map((row) => [row.stage, row.count])),
      nextRetryAt: retry?.next_retry_at || null,
      errorSamples: errorRows.map((row) => ({ message: row.error, count: row.count })),
    };
  }

  getItem(id) {
    return serializeItem(this.db.prepare("SELECT * FROM transfer_items WHERE id = ?").get(id));
  }

  listRunnableItems(limit = 100) {
    return this.db.prepare(`
      SELECT i.* FROM transfer_items i
      JOIN transfer_jobs j ON j.id = i.job_id
      WHERE j.manifest_status = 'SEALED'
        AND (i.status = 'QUEUED' OR (i.status = 'RETRY_WAIT' AND (i.next_retry_at IS NULL OR i.next_retry_at <= ?)))
      ORDER BY i.created_at, i.id
      LIMIT ?
    `).all(nowIso(), Math.max(1, Math.min(100, limit))).map(serializeItem);
  }

  claimItem(id) {
    const now = nowIso();
    const result = this.db.prepare(`
      UPDATE transfer_items
      SET status = 'RUNNING', attempt_count = attempt_count + 1,
          started_at = COALESCE(started_at, ?), next_retry_at = NULL, error = NULL,
          failure_kind = NULL, failure_status_code = NULL, retryable = NULL, updated_at = ?
      WHERE id = ? AND status IN ('QUEUED', 'RETRY_WAIT')
    `).run(now, now, id);
    if (!result.changes) return null;
    const item = this.getItem(id);
    this.db.prepare(`
      UPDATE transfer_jobs SET status = 'RUNNING', started_at = COALESCE(started_at, ?), updated_at = ? WHERE id = ?
    `).run(now, now, item.jobId);
    return item;
  }

  updateItem(id, values) {
    const allowed = new Map([
      ["status", "status"], ["stage", "stage"], ["nextRetryAt", "next_retry_at"],
      ["homeSizeBytes", "home_size_bytes"], ["homeEtag", "home_etag"], ["homeSha256", "home_sha256"], ["homeReused", "home_reused"], ["homeVerifiedAt", "home_verified_at"],
      ["ossBucket", "oss_bucket"], ["ossSizeBytes", "oss_size_bytes"], ["ossEtag", "oss_etag"], ["ossDirectUrl", "oss_direct_url"],
      ["ossSelectedUrl", "oss_selected_url"], ["ossCdnVerified", "oss_cdn_verified"], ["ossVerifiedAt", "oss_verified_at"],
      ["checkpoint", "checkpoint_json"], ["warning", "warning"], ["error", "error"], ["finishedAt", "finished_at"],
      ["failureKind", "failure_kind"], ["failureStatusCode", "failure_status_code"], ["retryable", "retryable"],
    ]);
    const assignments = [];
    const parameters = [];
    for (const [key, value] of Object.entries(values || {})) {
      const column = allowed.get(key);
      if (!column) continue;
      assignments.push(`${column} = ?`);
      parameters.push(
        key === "checkpoint"
          ? (value == null ? null : JSON.stringify(value))
          : key === "ossCdnVerified" || key === "homeReused" || key === "retryable"
            ? (value == null ? null : value ? 1 : 0)
            : value,
      );
    }
    if (!assignments.length) return this.getItem(id);
    assignments.push("updated_at = ?");
    parameters.push(nowIso(), id);
    this.db.prepare(`UPDATE transfer_items SET ${assignments.join(", ")} WHERE id = ?`).run(...parameters);
    const item = this.getItem(id);
    this.refreshJob(item.jobId);
    return item;
  }

  isItemCancelled(id) {
    return this.db.prepare("SELECT status FROM transfer_items WHERE id = ?").get(id)?.status === "CANCELLED";
  }

  enqueueItemCallback(itemId) {
    const itemRow = this.db.prepare("SELECT * FROM transfer_items WHERE id = ?").get(itemId);
    if (!itemRow || !ITEM_TERMINAL_STATUSES.has(itemRow.status)) return null;
    const job = this.getJobControl(itemRow.job_id);
    if (!job?.callback_url || !job.callback_token) return null;
    const existing = this.db.prepare("SELECT * FROM callback_outbox WHERE run_id = ? AND item_id = ?").get(itemRow.job_id, itemId);
    if (existing) return serializeCallback(existing);
    const now = nowIso();
    let callbackId = "";
    this.db.transaction(() => {
      const sequence = Number(job.last_callback_sequence || 0) + 1;
      callbackId = randomUUID();
      this.db.prepare(`
        INSERT INTO callback_outbox(
          id, run_id, item_id, sequence, status, payload_json, created_at, updated_at
        ) VALUES (?, ?, ?, ?, 'QUEUED', ?, ?, ?)
      `).run(callbackId, itemRow.job_id, itemId, sequence, JSON.stringify(serializeItem(itemRow)), now, now);
      this.db.prepare("UPDATE transfer_jobs SET last_callback_sequence = ?, updated_at = ? WHERE id = ?")
        .run(sequence, now, itemRow.job_id);
    })();
    this.refreshJob(itemRow.job_id);
    return serializeCallback(this.db.prepare("SELECT * FROM callback_outbox WHERE id = ?").get(callbackId));
  }

  listRunnableCallbacks(limit = 100) {
    return this.db.prepare(`
      SELECT c.*, j.callback_url, j.callback_token
      FROM callback_outbox c
      JOIN transfer_jobs j ON j.id = c.run_id
      WHERE (
          c.status = 'QUEUED'
          OR (c.status = 'RETRY_WAIT' AND (c.next_retry_at IS NULL OR c.next_retry_at <= ?))
        )
        AND j.manifest_status = 'SEALED'
        AND j.status NOT IN ('CANCELLED', 'FAILED')
        AND NOT EXISTS (
          SELECT 1 FROM transfer_items active_items
          WHERE active_items.job_id = c.run_id
            AND active_items.status IN ('QUEUED', 'RUNNING', 'RETRY_WAIT')
        )
        AND (
          SELECT COUNT(*) FROM callback_outbox all_callbacks
          WHERE all_callbacks.run_id = c.run_id
        ) = (
          SELECT COUNT(*) FROM transfer_items all_items
          WHERE all_items.job_id = c.run_id
        )
      ORDER BY c.run_id, c.sequence
      LIMIT ?
    `).all(nowIso(), Math.max(1, Math.min(1000, limit))).map((row) => ({
      ...serializeCallback(row),
      callbackUrl: row.callback_url,
      callbackToken: row.callback_token,
    }));
  }

  claimCallbacks(ids) {
    const uniqueIds = [...new Set((ids || []).filter(Boolean))];
    if (!uniqueIds.length) return [];
    const now = nowIso();
    const placeholders = uniqueIds.map(() => "?").join(",");
    this.db.prepare(`
      UPDATE callback_outbox
      SET status = 'SENDING', attempt_count = attempt_count + 1, next_retry_at = NULL, error = NULL, updated_at = ?
      WHERE id IN (${placeholders}) AND status IN ('QUEUED', 'RETRY_WAIT')
    `).run(now, ...uniqueIds);
    return this.db.prepare(`
      SELECT c.*, j.callback_url, j.callback_token
      FROM callback_outbox c JOIN transfer_jobs j ON j.id = c.run_id
      WHERE c.id IN (${placeholders}) AND c.status = 'SENDING'
      ORDER BY c.sequence
    `).all(...uniqueIds).map((row) => ({
      ...serializeCallback(row),
      callbackUrl: row.callback_url,
      callbackToken: row.callback_token,
    }));
  }

  markCallbacksDelivered(ids) {
    const uniqueIds = [...new Set((ids || []).filter(Boolean))];
    if (!uniqueIds.length) return;
    const placeholders = uniqueIds.map(() => "?").join(",");
    const runIds = this.db.prepare(`SELECT DISTINCT run_id FROM callback_outbox WHERE id IN (${placeholders})`)
      .all(...uniqueIds).map((row) => row.run_id);
    const now = nowIso();
    this.db.prepare(`
      UPDATE callback_outbox
      SET status = 'DELIVERED', delivered_at = ?, next_retry_at = NULL, error = NULL, updated_at = ?
      WHERE id IN (${placeholders})
    `).run(now, now, ...uniqueIds);
    for (const runId of runIds) this.refreshJob(runId);
  }

  markCallbacksRetry(ids, error) {
    const uniqueIds = [...new Set((ids || []).filter(Boolean))];
    if (!uniqueIds.length) return;
    const placeholders = uniqueIds.map(() => "?").join(",");
    const rows = this.db.prepare(`SELECT id, run_id, attempt_count FROM callback_outbox WHERE id IN (${placeholders})`).all(...uniqueIds);
    const update = this.db.prepare(`
      UPDATE callback_outbox SET status = ?, next_retry_at = ?, error = ?, updated_at = ? WHERE id = ?
    `);
    this.db.transaction(() => {
      for (const row of rows) {
        const exhausted = row.attempt_count >= 10;
        const delayMs = Math.min(6 * 60 * 60 * 1000, 5_000 * 2 ** Math.max(0, row.attempt_count - 1));
        update.run(
          exhausted ? "FAILED" : "RETRY_WAIT",
          exhausted ? null : new Date(Date.now() + delayMs).toISOString(),
          String(error || "Callback failed").slice(0, 4000),
          nowIso(),
          row.id,
        );
      }
    })();
    for (const runId of new Set(rows.map((row) => row.run_id))) this.refreshJob(runId);
  }

  cancelJob(id, reason = "Cancelled by administrator", summaryOnly = false) {
    const readResult = () => summaryOnly ? this.getJobSummary(id) : this.getJob(id);
    const job = this.db.prepare("SELECT status FROM transfer_jobs WHERE id = ?").get(id);
    if (!job) return null;
    if (["SUCCEEDED", "SUCCEEDED_WITH_ERRORS", "FAILED"].includes(job.status)) return readResult();
    const now = nowIso();
    this.db.transaction(() => {
      this.db.prepare(`
        UPDATE transfer_items
        SET status = 'CANCELLED', stage = 'CANCELLED', next_retry_at = NULL,
            error = ?, finished_at = ?, updated_at = ?
        WHERE job_id = ? AND status IN ('QUEUED', 'RUNNING', 'RETRY_WAIT')
      `).run(reason, now, now, id);
      this.db.prepare(`
        UPDATE transfer_jobs
        SET status = 'CANCELLED',
            manifest_status = CASE WHEN manifest_status = 'SEALED' THEN manifest_status ELSE 'CANCELLED' END,
            error = ?, finished_at = ?, updated_at = ?
        WHERE id = ?
      `).run(reason, now, now, id);
    })();
    return readResult();
  }

  resumeStreamingJob(id) {
    const job = this.getJobControl(id);
    if (!job) return null;
    const callbackResetAt = nowIso();
    this.db.prepare(`
      UPDATE callback_outbox
      SET status = 'QUEUED', attempt_count = 0, next_retry_at = NULL, error = NULL, updated_at = ?
      WHERE run_id = ? AND status = 'FAILED'
    `).run(callbackResetAt, id);
    if (job.manifest_status !== "SEALED") {
      if (!["CANCELLED", "FAILED", "RETRY_WAIT"].includes(job.manifest_status)) return this.getJobSummary(id);
      const now = nowIso();
      this.db.transaction(() => {
        this.db.prepare(`
          UPDATE transfer_items
          SET status = 'QUEUED', stage = CASE WHEN home_verified_at IS NULL THEN 'PENDING' ELSE 'HOME_VERIFIED' END,
              next_retry_at = NULL, error = NULL, failure_kind = NULL, failure_status_code = NULL,
              retryable = NULL, finished_at = NULL, updated_at = ?
          WHERE job_id = ? AND status = 'CANCELLED'
        `).run(now, id);
        this.db.prepare(`
          UPDATE transfer_jobs
          SET status = 'INGESTING', manifest_status = 'PENDING', manifest_next_retry_at = NULL,
              error = NULL, finished_at = NULL, updated_at = ?
          WHERE id = ?
        `).run(now, id);
        this.db.prepare(`
          UPDATE callback_outbox
          SET status = 'QUEUED', attempt_count = 0, next_retry_at = NULL, error = NULL, updated_at = ?
          WHERE run_id = ? AND status = 'FAILED'
        `).run(now, id);
      })();
      return this.getJobSummary(id);
    }
    this.resumeJob(id);
    this.refreshJob(id);
    return this.getJobSummary(id);
  }

  resumeJob(id) {
    const job = this.db.prepare("SELECT status FROM transfer_jobs WHERE id = ?").get(id);
    if (!job) return null;
    if (job.status !== "CANCELLED") return this.getJob(id);
    const now = nowIso();
    this.db.transaction(() => {
      this.db.prepare(`
        UPDATE transfer_items
        SET status = 'QUEUED', stage = CASE WHEN home_verified_at IS NULL THEN 'PENDING' ELSE 'HOME_VERIFIED' END,
            next_retry_at = NULL, error = NULL, failure_kind = NULL, failure_status_code = NULL,
            retryable = NULL, finished_at = NULL, updated_at = ?
        WHERE job_id = ? AND status = 'CANCELLED'
      `).run(now, id);
      this.db.prepare(`
        UPDATE transfer_jobs
        SET status = 'QUEUED', error = NULL, finished_at = NULL, updated_at = ?
        WHERE id = ?
      `).run(now, id);
      this.db.prepare(`
        UPDATE callback_outbox
        SET status = 'QUEUED', attempt_count = 0, next_retry_at = NULL, error = NULL, updated_at = ?
        WHERE run_id = ? AND status = 'FAILED'
      `).run(now, id);
    })();
    return this.getJob(id);
  }

  getCheckpoint(id) {
    const row = this.db.prepare("SELECT checkpoint_json FROM transfer_items WHERE id = ?").get(id);
    return parseJson(row?.checkpoint_json, null);
  }

  replaceMultipartParts(itemId, parts, partSize) {
    const updatedAt = nowIso();
    const replace = this.db.transaction(() => {
      this.db.prepare("DELETE FROM multipart_parts WHERE item_id = ?").run(itemId);
      const insert = this.db.prepare(`
        INSERT INTO multipart_parts(item_id, part_number, etag, size_bytes, updated_at) VALUES (?, ?, ?, ?, ?)
      `);
      for (const part of Array.isArray(parts) ? parts : []) {
        if (!Number.isInteger(part.number) || !part.etag) continue;
        insert.run(itemId, part.number, String(part.etag), partSize || null, updatedAt);
      }
    });
    replace();
  }

  refreshJob(jobId) {
    const current = this.db.prepare("SELECT status, callback_url, error FROM transfer_jobs WHERE id = ?").get(jobId);
    if (!current || current.status === "CANCELLED") return;
    const summary = this.db.prepare(`
      SELECT
        COUNT(*) AS total_count,
        SUM(CASE WHEN status IN ('SUCCEEDED', 'FAILED') THEN 1 ELSE 0 END) AS processed_count,
        SUM(CASE WHEN status = 'SUCCEEDED' THEN 1 ELSE 0 END) AS succeeded_count,
        SUM(CASE WHEN status = 'FAILED' THEN 1 ELSE 0 END) AS failed_count,
        COALESCE(SUM(CASE WHEN status = 'SUCCEEDED' THEN COALESCE(home_size_bytes, expected_size_bytes, 0) ELSE 0 END), 0) AS processed_bytes,
        SUM(CASE WHEN status IN ('QUEUED', 'RUNNING', 'RETRY_WAIT') THEN 1 ELSE 0 END) AS active_count
      FROM transfer_items WHERE job_id = ?
    `).get(jobId);
    let status = "RUNNING";
    let finishedAt = null;
    let jobError = current.error;
    if (summary.active_count === 0) {
      if (current.callback_url) {
        let callbacks = this.getCallbackDeliverySummary(jobId);
        if (["RUNNING", "QUEUED"].includes(current.status) && callbacks.callbackFailedCount > 0) {
          const resetAt = nowIso();
          this.db.prepare(`
            UPDATE callback_outbox
            SET status = 'QUEUED', attempt_count = 0, next_retry_at = NULL,
                error = NULL, updated_at = ?
            WHERE run_id = ? AND status = 'FAILED'
          `).run(resetAt, jobId);
          callbacks = this.getCallbackDeliverySummary(jobId);
        }
        if (callbacks.callbackFailedCount > 0) {
          status = "CALLBACK_FAILED";
          jobError = this.db.prepare(`
            SELECT error FROM callback_outbox
            WHERE run_id = ? AND status = 'FAILED' AND error IS NOT NULL
            ORDER BY updated_at DESC LIMIT 1
          `).get(jobId)?.error || "NewWaule result delivery failed.";
        } else if (
          callbacks.callbackTotalCount < summary.processed_count ||
          callbacks.callbackPendingCount > 0 ||
          callbacks.callbackDeliveredCount < summary.processed_count
        ) {
          status = "RESULTS_PENDING";
          jobError = null;
        } else {
          status = summary.failed_count > 0 ? "SUCCEEDED_WITH_ERRORS" : "SUCCEEDED";
          finishedAt = nowIso();
          jobError = null;
        }
      } else {
        status = summary.failed_count > 0 ? "FAILED" : "SUCCEEDED";
        finishedAt = nowIso();
      }
    } else if (current.callback_url) {
      jobError = null;
    }
    this.db.prepare(`
      UPDATE transfer_jobs
      SET status = ?, processed_count = ?, succeeded_count = ?, failed_count = ?, processed_bytes = ?,
          error = ?, finished_at = ?, updated_at = ?
      WHERE id = ?
    `).run(
      status,
      summary.processed_count,
      summary.succeeded_count,
      summary.failed_count,
      summary.processed_bytes,
      jobError,
      finishedAt,
      nowIso(),
      jobId,
    );
  }

  listRecentJobs(limit = 10) {
    return this.db.prepare("SELECT * FROM transfer_jobs ORDER BY created_at DESC LIMIT ?")
      .all(Math.max(1, Math.min(100, limit)))
      .map((row) => ({
        ...serializeJob(row),
        ...this.getCallbackDeliverySummary(row.id),
        diagnostics: this.getJobDiagnostics(row.id),
      }));
  }

  createCachePushJob(input) {
    const objectKey = String(input?.objectKey || "").trim().replace(/\\/g, "/").replace(/^\/+/, "");
    if (!objectKey || objectKey.split("/").some((segment) => !segment || segment === "." || segment === "..")) {
      throw new LifecycleValidationError("Cache push objectKey is invalid.");
    }
    const existing = this.db.prepare(`
      SELECT * FROM cache_push_jobs
      WHERE object_key = ? AND status IN ('QUEUED', 'RUNNING', 'RETRY_WAIT')
      ORDER BY created_at DESC LIMIT 1
    `).get(objectKey);
    if (existing) return { job: serializeCachePushJob(existing), alreadyRunning: true };

    const id = randomUUID();
    const createdAt = nowIso();
    const request = {
      newWauleApiUrl: String(input.newWauleApiUrl || "").replace(/\/+$/, ""),
      cacheUploadBaseUrl: String(input.cacheUploadBaseUrl || "").replace(/\/+$/, ""),
    };
    this.db.prepare(`
      INSERT INTO cache_push_jobs(
        id, object_key, status, request_json, created_at, updated_at
      ) VALUES (?, ?, 'QUEUED', ?, ?, ?)
    `).run(id, objectKey, JSON.stringify(request), createdAt, createdAt);
    return { job: serializeCachePushJob(this.db.prepare("SELECT * FROM cache_push_jobs WHERE id = ?").get(id)), alreadyRunning: false };
  }

  getCachePushJob(id) {
    return serializeCachePushJob(this.db.prepare("SELECT * FROM cache_push_jobs WHERE id = ?").get(id));
  }

  findCachePushJobByObjectKey(objectKey) {
    return serializeCachePushJob(this.db.prepare(`
      SELECT * FROM cache_push_jobs WHERE object_key = ? ORDER BY created_at DESC LIMIT 1
    `).get(objectKey));
  }

  listRunnableCachePushJobs(limit = 100) {
    return this.db.prepare(`
      SELECT * FROM cache_push_jobs
      WHERE status = 'QUEUED' OR (status = 'RETRY_WAIT' AND (next_retry_at IS NULL OR next_retry_at <= ?))
      ORDER BY created_at, id LIMIT ?
    `).all(nowIso(), Math.max(1, Math.min(100, limit))).map(serializeCachePushJob);
  }

  claimCachePushJob(id) {
    const now = nowIso();
    const result = this.db.prepare(`
      UPDATE cache_push_jobs
      SET status = 'RUNNING', attempt_count = attempt_count + 1,
          started_at = COALESCE(started_at, ?), next_retry_at = NULL, error = NULL, updated_at = ?
      WHERE id = ? AND status IN ('QUEUED', 'RETRY_WAIT')
    `).run(now, now, id);
    return result.changes ? this.getCachePushJob(id) : null;
  }

  updateCachePushJob(id, values) {
    const allowed = new Map([
      ["status", "status"], ["nextRetryAt", "next_retry_at"], ["error", "error"], ["finishedAt", "finished_at"],
    ]);
    const assignments = [];
    const parameters = [];
    for (const [key, value] of Object.entries(values || {})) {
      const column = allowed.get(key);
      if (!column) continue;
      assignments.push(`${column} = ?`);
      parameters.push(value);
    }
    if (!assignments.length) return this.getCachePushJob(id);
    assignments.push("updated_at = ?");
    parameters.push(nowIso(), id);
    this.db.prepare(`UPDATE cache_push_jobs SET ${assignments.join(", ")} WHERE id = ?`).run(...parameters);
    return this.getCachePushJob(id);
  }

  isJobTerminal(id) {
    const row = this.db.prepare("SELECT status FROM transfer_jobs WHERE id = ?").get(id);
    return Boolean(row && JOB_TERMINAL_STATUSES.has(row.status));
  }

  isItemTerminal(id) {
    const row = this.db.prepare("SELECT status FROM transfer_items WHERE id = ?").get(id);
    return Boolean(row && ITEM_TERMINAL_STATUSES.has(row.status));
  }
}
