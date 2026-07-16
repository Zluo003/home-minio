import { mkdirSync } from "node:fs";
import { dirname, resolve } from "node:path";
import Database from "better-sqlite3";

export const BAIDUPAN_BACKUP_SCHEMA_VERSION = 7;

function nowIso() {
  return new Date().toISOString();
}

export function migrateBaidupanBackupSchema(db) {
  const migrationTable = db.prepare(`
    SELECT name FROM sqlite_master
    WHERE type = 'table' AND name = 'schema_migrations'
  `).get();
  if (!migrationTable) {
    throw new Error("Home MinIO state database is not initialized; start web-api before running backup.");
  }
  const current = Number(db.prepare("SELECT COALESCE(MAX(version), 0) AS version FROM schema_migrations").get().version);
  if (current < 5) {
    throw new Error(`Home MinIO state database schema ${current} is too old; start the current web-api first.`);
  }
  if (current >= BAIDUPAN_BACKUP_SCHEMA_VERSION) return;

  let migratedVersion = current;
  if (migratedVersion < 6) {
    db.transaction(() => {
      db.exec(`
        CREATE TABLE IF NOT EXISTS baidupan_backup_metadata (
          key TEXT PRIMARY KEY,
          value_json TEXT NOT NULL,
          updated_at TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS baidupan_backup_objects (
          bucket TEXT NOT NULL,
          object_key TEXT NOT NULL,
          source_size INTEGER,
          source_mtime INTEGER,
          status TEXT NOT NULL CHECK (status IN ('PENDING', 'RUNNING', 'COMPLETED', 'FAILED', 'MISSING')),
          attempt_count INTEGER NOT NULL DEFAULT 0,
          remote_path TEXT,
          last_error TEXT,
          first_seen_at TEXT NOT NULL,
          last_seen_at TEXT NOT NULL,
          completed_at TEXT,
          updated_at TEXT NOT NULL,
          PRIMARY KEY(bucket, object_key)
        );

        CREATE INDEX IF NOT EXISTS baidupan_backup_objects_status_idx
          ON baidupan_backup_objects(bucket, status, updated_at);

        CREATE TABLE IF NOT EXISTS baidupan_backup_runs (
          id TEXT PRIMARY KEY,
          bucket TEXT NOT NULL,
          status TEXT NOT NULL,
          dry_run INTEGER NOT NULL DEFAULT 0,
          manifest_path TEXT NOT NULL,
          discovered_count INTEGER NOT NULL DEFAULT 0,
          queued_count INTEGER NOT NULL DEFAULT 0,
          succeeded_count INTEGER NOT NULL DEFAULT 0,
          failed_count INTEGER NOT NULL DEFAULT 0,
          missing_count INTEGER NOT NULL DEFAULT 0,
          error TEXT,
          created_at TEXT NOT NULL,
          started_at TEXT,
          finished_at TEXT,
          updated_at TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS baidupan_backup_run_items (
          run_id TEXT NOT NULL REFERENCES baidupan_backup_runs(id) ON DELETE CASCADE,
          bucket TEXT NOT NULL,
          object_key TEXT NOT NULL,
          source_size INTEGER,
          source_mtime INTEGER,
          status TEXT NOT NULL CHECK (status IN ('PENDING', 'RUNNING', 'SUCCEEDED', 'FAILED', 'MISSING', 'DRY_RUN')),
          attempt_count INTEGER NOT NULL DEFAULT 0,
          remote_path TEXT,
          error TEXT,
          started_at TEXT,
          finished_at TEXT,
          updated_at TEXT NOT NULL,
          PRIMARY KEY(run_id, bucket, object_key)
        );

        CREATE INDEX IF NOT EXISTS baidupan_backup_run_items_status_idx
          ON baidupan_backup_run_items(run_id, status, object_key);
      `);
      db.prepare("INSERT INTO schema_migrations(version, applied_at) VALUES (?, ?)")
        .run(6, nowIso());
    })();
    migratedVersion = 6;
  }
  if (migratedVersion < 7) {
    db.transaction(() => {
      const objectColumns = new Set(
        db.prepare("PRAGMA table_info(baidupan_backup_objects)").all().map((column) => column.name),
      );
      const runItemColumns = new Set(
        db.prepare("PRAGMA table_info(baidupan_backup_run_items)").all().map((column) => column.name),
      );
      if (!objectColumns.has("source_etag")) {
        db.exec("ALTER TABLE baidupan_backup_objects ADD COLUMN source_etag TEXT;");
      }
      if (!runItemColumns.has("source_etag")) {
        db.exec("ALTER TABLE baidupan_backup_run_items ADD COLUMN source_etag TEXT;");
      }
      db.prepare("INSERT INTO schema_migrations(version, applied_at) VALUES (?, ?)")
        .run(7, nowIso());
    })();
  }
}

export class BaidupanBackupStore {
  constructor(options = {}) {
    this.dbPath = resolve(options.dbPath || process.env.HOME_MINIO_STATE_DB || "./state/home-minio.sqlite");
    mkdirSync(dirname(this.dbPath), { recursive: true });
    this.db = new Database(this.dbPath);
    this.db.pragma("journal_mode = WAL");
    this.db.pragma("foreign_keys = ON");
    this.db.pragma("busy_timeout = 5000");
    migrateBaidupanBackupSchema(this.db);
  }

  close() {
    this.db.close();
  }

  getMetadata(key) {
    const row = this.db.prepare("SELECT value_json FROM baidupan_backup_metadata WHERE key = ?").get(key);
    return row ? JSON.parse(row.value_json) : null;
  }

  setMetadata(key, value) {
    const updatedAt = nowIso();
    this.db.prepare(`
      INSERT INTO baidupan_backup_metadata(key, value_json, updated_at)
      VALUES (?, ?, ?)
      ON CONFLICT(key) DO UPDATE SET value_json = excluded.value_json, updated_at = excluded.updated_at
    `).run(key, JSON.stringify(value), updatedAt);
  }

  recoverInterruptedRuns() {
    const updatedAt = nowIso();
    this.db.transaction(() => {
      this.db.prepare(`
        UPDATE baidupan_backup_objects
        SET status = 'PENDING', last_error = COALESCE(last_error, 'Recovered after process interruption'), updated_at = ?
        WHERE status = 'RUNNING'
      `).run(updatedAt);
      this.db.prepare(`
        UPDATE baidupan_backup_run_items
        SET status = 'FAILED', error = COALESCE(error, 'Interrupted before completion'),
            finished_at = ?, updated_at = ?
        WHERE status = 'RUNNING'
      `).run(updatedAt, updatedAt);
      this.db.prepare(`
        UPDATE baidupan_backup_runs
        SET status = 'INTERRUPTED', error = COALESCE(error, 'Backup process was interrupted'),
            finished_at = ?, updated_at = ?
        WHERE status IN ('READY', 'RUNNING')
      `).run(updatedAt, updatedAt);
    })();
  }

  listUnfinishedObjectKeys(bucket) {
    return this.db.prepare(`
      SELECT object_key FROM baidupan_backup_objects
      WHERE bucket = ? AND status != 'COMPLETED'
      ORDER BY object_key
    `).all(bucket).map((row) => row.object_key);
  }

  listCompletedObjects(bucket) {
    return this.db.prepare(`
      SELECT object_key, source_size, source_mtime, source_etag
      FROM baidupan_backup_objects
      WHERE bucket = ? AND status = 'COMPLETED'
      ORDER BY object_key
    `).all(bucket);
  }

  upsertCandidate({
    bucket,
    objectKey,
    size,
    mtime,
    etag = null,
    completed = false,
    forcePending = false,
    remotePath = null,
    seenAt = nowIso(),
  }) {
    const existing = this.db.prepare(`
      SELECT * FROM baidupan_backup_objects WHERE bucket = ? AND object_key = ?
    `).get(bucket, objectKey);
    const normalizedEtag = etag ? String(etag).replace(/^"|"$/g, "") : null;
    const effectiveEtag = normalizedEtag || existing?.source_etag || null;
    const updatedAt = seenAt;
    const sameEtag = !existing?.source_etag
      || !normalizedEtag
      || existing.source_etag === normalizedEtag;
    const sameSource = existing
      && Number(existing.source_size) === Number(size)
      && Number(existing.source_mtime) === Number(mtime)
      && sameEtag;
    const status = !forcePending && (completed || (sameSource && existing.status === "COMPLETED"))
      ? "COMPLETED"
      : "PENDING";
    const completedAt = status === "COMPLETED" ? existing?.completed_at || updatedAt : null;
    if (
      existing
      && sameSource
      && existing.status === "COMPLETED"
      && status === "COMPLETED"
      && !remotePath
      && existing.source_etag === effectiveEtag
    ) {
      return { changed: false, status };
    }
    this.db.prepare(`
      INSERT INTO baidupan_backup_objects(
        bucket, object_key, source_size, source_mtime, source_etag, status, remote_path,
        first_seen_at, last_seen_at, completed_at, updated_at
      ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
      ON CONFLICT(bucket, object_key) DO UPDATE SET
        source_size = excluded.source_size,
        source_mtime = excluded.source_mtime,
        source_etag = excluded.source_etag,
        status = excluded.status,
        remote_path = COALESCE(excluded.remote_path, baidupan_backup_objects.remote_path),
        last_error = NULL,
        last_seen_at = excluded.last_seen_at,
        completed_at = excluded.completed_at,
        updated_at = excluded.updated_at
    `).run(
      bucket,
      objectKey,
      Number(size),
      Number(mtime),
      effectiveEtag,
      status,
      remotePath,
      existing?.first_seen_at || updatedAt,
      updatedAt,
      completedAt,
      updatedAt,
    );
    return {
      changed: !existing || !sameSource,
      status,
    };
  }

  reconcileInventory({ bucket, records, legacyUploadedSignatures = new Set() }) {
    const seenAt = nowIso();
    const inventoryKeys = new Set(records.map((record) => record.objectKey));
    let discoveredCount = 0;
    const reconcile = this.db.transaction(() => {
      for (const record of records) {
        const signature = JSON.stringify([
          bucket,
          record.objectKey,
          String(record.size),
          String(record.mtime),
        ]);
        const result = this.upsertCandidate({
          bucket,
          objectKey: record.objectKey,
          size: record.size,
          mtime: record.mtime,
          etag: record.etag,
          completed: legacyUploadedSignatures.has(signature),
          seenAt,
        });
        if (result.changed) discoveredCount += 1;
      }
      for (const row of this.db.prepare(`
        SELECT object_key
        FROM baidupan_backup_objects
        WHERE bucket = ? AND status != 'COMPLETED'
      `).all(bucket)) {
        if (!inventoryKeys.has(row.object_key)) this.markMissing(bucket, row.object_key);
      }
    });
    reconcile();
    return {
      discoveredCount,
      objectCount: records.length,
    };
  }

  markMissing(bucket, objectKey) {
    const updatedAt = nowIso();
    const existing = this.db.prepare(`
      SELECT first_seen_at FROM baidupan_backup_objects WHERE bucket = ? AND object_key = ?
    `).get(bucket, objectKey);
    this.db.prepare(`
      INSERT INTO baidupan_backup_objects(
        bucket, object_key, status, last_error, first_seen_at, last_seen_at, updated_at
      ) VALUES (?, ?, 'MISSING', 'MinIO source object is missing', ?, ?, ?)
      ON CONFLICT(bucket, object_key) DO UPDATE SET
        status = 'MISSING', last_error = 'MinIO source object is missing',
        last_seen_at = excluded.last_seen_at, updated_at = excluded.updated_at
    `).run(bucket, objectKey, existing?.first_seen_at || updatedAt, updatedAt, updatedAt);
  }

  createRun({ id, bucket, manifestPath, discoveredCount, dryRun }) {
    const createdAt = nowIso();
    this.db.transaction(() => {
      this.db.prepare(`
        INSERT INTO baidupan_backup_runs(
          id, bucket, status, dry_run, manifest_path, discovered_count, created_at, updated_at
        ) VALUES (?, ?, 'READY', ?, ?, ?, ?, ?)
      `).run(id, bucket, dryRun ? 1 : 0, manifestPath, discoveredCount, createdAt, createdAt);
      this.db.prepare(`
        INSERT INTO baidupan_backup_run_items(
          run_id, bucket, object_key, source_size, source_mtime, source_etag, status, error, updated_at
        )
        SELECT ?, bucket, object_key, source_size, source_mtime, source_etag,
               CASE WHEN status = 'MISSING' THEN 'MISSING' ELSE 'PENDING' END,
               CASE WHEN status = 'MISSING' THEN last_error ELSE NULL END,
               ?
        FROM baidupan_backup_objects
        WHERE bucket = ? AND status != 'COMPLETED'
        ORDER BY object_key
      `).run(id, createdAt, bucket);
      const counts = this.db.prepare(`
        SELECT
          SUM(CASE WHEN status = 'PENDING' THEN 1 ELSE 0 END) AS queued,
          SUM(CASE WHEN status = 'MISSING' THEN 1 ELSE 0 END) AS missing
        FROM baidupan_backup_run_items WHERE run_id = ?
      `).get(id);
      this.db.prepare(`
        UPDATE baidupan_backup_runs
        SET queued_count = ?, missing_count = ?, failed_count = ?, updated_at = ?
        WHERE id = ?
      `).run(
        Number(counts.queued || 0),
        Number(counts.missing || 0),
        Number(counts.missing || 0),
        createdAt,
        id,
      );
    })();
    return this.getRun(id);
  }

  getRun(id) {
    return this.db.prepare("SELECT * FROM baidupan_backup_runs WHERE id = ?").get(id) || null;
  }

  listRunItems(id, status = null) {
    return status
      ? this.db.prepare(`
          SELECT * FROM baidupan_backup_run_items
          WHERE run_id = ? AND status = ? ORDER BY object_key
        `).all(id, status)
      : this.db.prepare(`
          SELECT * FROM baidupan_backup_run_items WHERE run_id = ? ORDER BY object_key
        `).all(id);
  }

  startRun(id) {
    const updatedAt = nowIso();
    this.db.prepare(`
      UPDATE baidupan_backup_runs
      SET status = 'RUNNING', started_at = COALESCE(started_at, ?), updated_at = ?
      WHERE id = ? AND status = 'READY'
    `).run(updatedAt, updatedAt, id);
  }

  startItem(runId, bucket, objectKey) {
    const updatedAt = nowIso();
    this.db.transaction(() => {
      this.db.prepare(`
        UPDATE baidupan_backup_run_items
        SET status = 'RUNNING', attempt_count = attempt_count + 1,
            started_at = COALESCE(started_at, ?), error = NULL, updated_at = ?
        WHERE run_id = ? AND bucket = ? AND object_key = ? AND status = 'PENDING'
      `).run(updatedAt, updatedAt, runId, bucket, objectKey);
      this.db.prepare(`
        UPDATE baidupan_backup_objects
        SET status = 'RUNNING', attempt_count = attempt_count + 1, last_error = NULL, updated_at = ?
        WHERE bucket = ? AND object_key = ?
      `).run(updatedAt, bucket, objectKey);
    })();
  }

  completeItem(runId, item, remotePath) {
    const completedAt = nowIso();
    this.db.transaction(() => {
      this.db.prepare(`
        UPDATE baidupan_backup_run_items
        SET status = 'SUCCEEDED', remote_path = ?, error = NULL,
            finished_at = ?, updated_at = ?
        WHERE run_id = ? AND bucket = ? AND object_key = ?
      `).run(remotePath, completedAt, completedAt, runId, item.bucket, item.object_key);
      this.db.prepare(`
        UPDATE baidupan_backup_objects
        SET status = 'COMPLETED', remote_path = ?, last_error = NULL,
            completed_at = ?, last_seen_at = ?, updated_at = ?
        WHERE bucket = ? AND object_key = ?
          AND source_size = ? AND source_mtime = ?
          AND COALESCE(source_etag, '') = COALESCE(?, '')
      `).run(
        remotePath,
        completedAt,
        completedAt,
        completedAt,
        item.bucket,
        item.object_key,
        item.source_size,
        item.source_mtime,
        item.source_etag,
      );
    })();
  }

  failItem(runId, item, error) {
    const failedAt = nowIso();
    const message = String(error || "Backup upload failed").slice(0, 4000);
    this.db.transaction(() => {
      this.db.prepare(`
        UPDATE baidupan_backup_run_items
        SET status = 'FAILED', error = ?, finished_at = ?, updated_at = ?
        WHERE run_id = ? AND bucket = ? AND object_key = ?
      `).run(message, failedAt, failedAt, runId, item.bucket, item.object_key);
      this.db.prepare(`
        UPDATE baidupan_backup_objects
        SET status = 'FAILED', last_error = ?, updated_at = ?
        WHERE bucket = ? AND object_key = ?
          AND source_size = ? AND source_mtime = ?
          AND COALESCE(source_etag, '') = COALESCE(?, '')
      `).run(
        message,
        failedAt,
        item.bucket,
        item.object_key,
        item.source_size,
        item.source_mtime,
        item.source_etag,
      );
    })();
  }

  markDryRunItem(runId, item) {
    const updatedAt = nowIso();
    this.db.prepare(`
      UPDATE baidupan_backup_run_items
      SET status = 'DRY_RUN', finished_at = ?, updated_at = ?
      WHERE run_id = ? AND bucket = ? AND object_key = ?
    `).run(updatedAt, updatedAt, runId, item.bucket, item.object_key);
  }

  finishRun(id) {
    const finishedAt = nowIso();
    const run = this.getRun(id);
    const counts = this.db.prepare(`
      SELECT
        SUM(CASE WHEN status = 'SUCCEEDED' THEN 1 ELSE 0 END) AS succeeded,
        SUM(CASE WHEN status IN ('FAILED', 'MISSING') THEN 1 ELSE 0 END) AS failed,
        SUM(CASE WHEN status = 'MISSING' THEN 1 ELSE 0 END) AS missing
      FROM baidupan_backup_run_items WHERE run_id = ?
    `).get(id);
    const failed = Number(counts.failed || 0);
    const status = run.dry_run ? "DRY_RUN" : failed > 0 ? "SUCCEEDED_WITH_ERRORS" : "SUCCEEDED";
    this.db.prepare(`
      UPDATE baidupan_backup_runs
      SET status = ?, succeeded_count = ?, failed_count = ?, missing_count = ?,
          finished_at = ?, updated_at = ?
      WHERE id = ?
    `).run(
      status,
      Number(counts.succeeded || 0),
      failed,
      Number(counts.missing || 0),
      finishedAt,
      finishedAt,
      id,
    );
    return this.getRun(id);
  }
}
