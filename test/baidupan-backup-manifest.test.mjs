import assert from "node:assert/strict";
import { execFile } from "node:child_process";
import { createServer } from "node:http";
import {
  access,
  chmod,
  mkdir,
  mkdtemp,
  readFile,
  readdir,
  rm,
  utimes,
  writeFile,
} from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { promisify } from "node:util";
import Database from "better-sqlite3";
import test from "node:test";
import {
  prepareBackupRun,
  readMinioInventory,
} from "../scripts/build-baidupan-backup-manifest.mjs";
import { cleanupLegacyMirror } from "../scripts/cleanup-legacy-baidupan-mirror.mjs";
import {
  BaidupanBackupStore,
  migrateBaidupanBackupSchema,
} from "../web/backend/baidupan-backup-store.mjs";
import { LifecycleStore } from "../web/backend/lifecycle-store.mjs";

const execFileAsync = promisify(execFile);
const BUCKET = "waule-media";

test("backup orchestration inventories metadata and never mirrors the full bucket", async () => {
  const script = await readFile(
    join(process.cwd(), "scripts", "backup-to-baidupan.sh"),
    "utf8",
  );
  assert.match(script, /mc ls --recursive --json/);
  assert.doesNotMatch(script, /mc mirror/);
});

function inventoryObject(key, content, options = {}) {
  const mtime = options.mtime ?? 1_700_000_000;
  return {
    status: "success",
    type: "file",
    key,
    size: Buffer.byteLength(content),
    lastModified: new Date(mtime * 1000).toISOString(),
    etag: options.etag || `etag-${key}`,
  };
}

async function writeInventory(path, objects) {
  await writeFile(path, `${objects.map((object) => JSON.stringify(object)).join("\n")}\n`, "utf8");
}

async function readJsonl(path) {
  const content = await readFile(path, "utf8");
  return content.split(/\r?\n/).filter(Boolean).map((line) => JSON.parse(line));
}

async function pathExists(path) {
  try {
    await access(path);
    return true;
  } catch {
    return false;
  }
}

async function withFixture(callback) {
  const root = await mkdtemp(join(tmpdir(), "baidupan-manifest-"));
  const paths = {
    root,
    dbPath: join(root, "state", "home-minio.sqlite"),
    inventoryPath: join(root, "state", "inventory.jsonl"),
    uploadedStatePath: join(root, "state", "uploaded.tsv"),
    manifestPath: join(root, "state", "manifest.jsonl"),
    spoolDir: join(root, "spool"),
    legacyMirrorDir: join(root, "mirror", BUCKET),
  };
  await mkdir(join(root, "state"), { recursive: true });
  await writeFile(paths.uploadedStatePath, "", "utf8");
  const lifecycleStore = new LifecycleStore({ dbPath: paths.dbPath, encryptionKey: null });
  lifecycleStore.close();
  try {
    await callback(paths);
  } finally {
    await rm(root, { recursive: true, force: true });
  }
}

async function startMinioFixture(objects) {
  const byKey = new Map(objects.map((object) => [object.key, object]));
  const server = createServer((request, response) => {
    const path = decodeURIComponent(new URL(request.url, "http://127.0.0.1").pathname);
    const prefix = `/${BUCKET}/`;
    const object = path.startsWith(prefix) ? byKey.get(path.slice(prefix.length)) : null;
    if (!object) {
      response.writeHead(404, { "content-type": "application/xml" });
      response.end("<Error><Code>NoSuchKey</Code></Error>");
      return;
    }
    response.writeHead(200, {
      "content-length": String(Buffer.byteLength(object.content)),
      "content-type": "application/octet-stream",
      etag: `"${object.etag}"`,
      "last-modified": new Date(object.mtime * 1000).toUTCString(),
    });
    response.end(object.content);
  });
  await new Promise((resolvePromise) => server.listen(0, "127.0.0.1", resolvePromise));
  const address = server.address();
  return {
    endpoint: `http://127.0.0.1:${address.port}`,
    close: () => new Promise((resolvePromise, reject) => {
      server.close((error) => (error ? reject(error) : resolvePromise()));
    }),
  };
}

test("first inventory imports legacy success state and manifests only unfinished objects", async () => {
  await withFixture(async (paths) => {
    await writeInventory(paths.inventoryPath, [
      inventoryObject("Result/2026/completed.png", "done", { mtime: 1_700_000_000 }),
      inventoryObject("Result/2026/pending.png", "pending", { mtime: 1_700_000_001 }),
    ]);
    await writeFile(
      paths.uploadedStatePath,
      `${BUCKET}\tResult/2026/completed.png\t4\t1700000000\n`,
      "utf8",
    );

    const summary = await prepareBackupRun({ runId: "bootstrap-run", bucket: BUCKET, ...paths });
    const manifest = await readJsonl(paths.manifestPath);
    const store = new BaidupanBackupStore({ dbPath: paths.dbPath });
    const completed = store.db.prepare(`
      SELECT status, source_etag FROM baidupan_backup_objects
      WHERE bucket = ? AND object_key = ?
    `).get(BUCKET, "Result/2026/completed.png");
    const columns = store.db.prepare("PRAGMA table_info(baidupan_backup_objects)").all();
    store.close();

    assert.equal(summary.bootstrap, true);
    assert.equal(summary.scanned, 2);
    assert.equal(completed.status, "COMPLETED");
    assert.equal(completed.source_etag, "etag-Result/2026/completed.png");
    assert.ok(columns.some((column) => column.name === "source_etag"));
    assert.deepEqual(manifest.map((record) => record.relativePath), ["Result/2026/pending.png"]);
  });
});

test("later inventories combine changed objects with durable database retries", async () => {
  await withFixture(async (paths) => {
    const oldFailure = inventoryObject("Result/2026/old-failure.png", "retry");
    const changed = inventoryObject("Result/2026/changed.png", "changed");
    const unrelated = inventoryObject("Result/2026/unrelated.png", "not-discovered");
    const store = new BaidupanBackupStore({ dbPath: paths.dbPath });
    store.setMetadata(`baidupan.incremental-baseline:${BUCKET}`, { version: 2 });
    store.upsertCandidate({
      bucket: BUCKET,
      objectKey: oldFailure.key,
      size: oldFailure.size,
      mtime: Math.floor(Date.parse(oldFailure.lastModified) / 1000),
      etag: oldFailure.etag,
    });
    store.upsertCandidate({
      bucket: BUCKET,
      objectKey: unrelated.key,
      size: unrelated.size,
      mtime: Math.floor(Date.parse(unrelated.lastModified) / 1000),
      etag: unrelated.etag,
      completed: true,
    });
    store.close();
    await writeInventory(paths.inventoryPath, [oldFailure, changed, unrelated]);

    const summary = await prepareBackupRun({ runId: "incremental-run", bucket: BUCKET, ...paths });
    const manifest = await readJsonl(paths.manifestPath);

    assert.equal(summary.bootstrap, false);
    assert.equal(summary.changed, 1);
    assert.deepEqual(
      manifest.map((record) => record.relativePath),
      ["Result/2026/changed.png", "Result/2026/old-failure.png"],
    );
  });
});

test("an ETag change is queued even when size and modification time are unchanged", async () => {
  await withFixture(async (paths) => {
    const source = inventoryObject("Result/2026/same-key.png", "same-content", {
      mtime: 1_700_000_100,
      etag: "new-etag",
    });
    const store = new BaidupanBackupStore({ dbPath: paths.dbPath });
    store.setMetadata(`baidupan.incremental-baseline:${BUCKET}`, { version: 2 });
    store.upsertCandidate({
      bucket: BUCKET,
      objectKey: source.key,
      size: source.size,
      mtime: 1_700_000_100,
      etag: "old-etag",
      completed: true,
    });
    store.close();
    await writeInventory(paths.inventoryPath, [source]);

    const summary = await prepareBackupRun({ runId: "changed-run", bucket: BUCKET, ...paths });
    const [record] = await readJsonl(paths.manifestPath);

    assert.equal(summary.changed, 1);
    assert.equal(summary.pending, 1);
    assert.equal(record.relativePath, source.key);
    assert.equal(record.etag, "new-etag");
  });
});

test("completed items remain durable without keeping a local mirror", async () => {
  await withFixture(async (paths) => {
    await writeInventory(paths.inventoryPath, [
      inventoryObject("Result/2026/durable.png", "durable"),
    ]);
    await prepareBackupRun({ runId: "first-run", bucket: BUCKET, ...paths });

    const store = new BaidupanBackupStore({ dbPath: paths.dbPath });
    store.startRun("first-run");
    const [item] = store.listRunItems("first-run", "PENDING");
    store.startItem("first-run", item.bucket, item.object_key);
    store.completeItem("first-run", item, "/backup/durable.png");
    store.finishRun("first-run");
    store.close();

    paths.manifestPath = join(paths.root, "state", "second-manifest.jsonl");
    const summary = await prepareBackupRun({ runId: "second-run", bucket: BUCKET, ...paths });

    assert.equal(summary.pending, 0);
    assert.deepEqual(await readJsonl(paths.manifestPath), []);
  });
});

test("one upload failure is persisted, later items succeed, and the spool is emptied", async () => {
  await withFixture(async (paths) => {
    const objects = [
      {
        key: "Result/2026/fail.png",
        content: "fail",
        mtime: 1_700_000_000,
        etag: "etag-fail",
      },
      {
        key: "Result/2026/success.png",
        content: "success",
        mtime: 1_700_000_001,
        etag: "etag-success",
      },
    ];
    await writeInventory(
      paths.inventoryPath,
      objects.map((object) => inventoryObject(object.key, object.content, object)),
    );
    await prepareBackupRun({ runId: "partial-run", bucket: BUCKET, ...paths });

    const fakeBaidupcs = join(paths.root, "fake-baidupcs.sh");
    await writeFile(fakeBaidupcs, `#!/bin/sh
case "$*" in
  *fail.png*) echo "injected upload failure" >&2; exit 7 ;;
  *) exit 0 ;;
esac
`, "utf8");
    await chmod(fakeBaidupcs, 0o755);
    const minio = await startMinioFixture(objects);
    try {
      await assert.rejects(
        execFileAsync(process.execPath, [
          join(process.cwd(), "scripts", "run-baidupan-backup-manifest.mjs"),
          "--run-id", "partial-run",
          "--spool-dir", paths.spoolDir,
          "--db", paths.dbPath,
          "--tool", "baidupcs",
          "--remote-dir", "/NewWaule/home-minio",
          "--baidupcs-bin", fakeBaidupcs,
        ], {
          env: {
            ...process.env,
            MINIO_INTERNAL_ENDPOINT: minio.endpoint,
            MINIO_ROOT_USER: "test-root",
            MINIO_ROOT_PASSWORD: "test-secret",
          },
        }),
      );
    } finally {
      await minio.close();
    }

    const store = new BaidupanBackupStore({ dbPath: paths.dbPath });
    const storedObjects = store.db.prepare(`
      SELECT object_key, status FROM baidupan_backup_objects
      WHERE bucket = ? ORDER BY object_key
    `).all(BUCKET);
    const run = store.getRun("partial-run");
    store.close();

    assert.deepEqual(storedObjects, [
      { object_key: "Result/2026/fail.png", status: "FAILED" },
      { object_key: "Result/2026/success.png", status: "COMPLETED" },
    ]);
    assert.equal(run.status, "SUCCEEDED_WITH_ERRORS");
    assert.equal(run.succeeded_count, 1);
    assert.equal(run.failed_count, 1);
    assert.deepEqual(await readdir(paths.spoolDir), []);
  });
});

test("legacy mirror cleanup removes only files that exactly match current MinIO objects", async () => {
  await withFixture(async (paths) => {
    const duplicate = inventoryObject("Result/2026/duplicate.png", "same", { mtime: 1_700_000_000 });
    const changed = inventoryObject("Result/2026/changed.png", "new-value", { mtime: 1_700_000_001 });
    const pending = inventoryObject("Result/2026/pending.png", "pending", { mtime: 1_700_000_002 });
    await writeInventory(paths.inventoryPath, [duplicate, changed, pending]);

    const duplicatePath = join(paths.legacyMirrorDir, duplicate.key);
    const changedPath = join(paths.legacyMirrorDir, changed.key);
    const pendingPath = join(paths.legacyMirrorDir, pending.key);
    const orphanPath = join(paths.legacyMirrorDir, "Result/2026/orphan.png");
    await mkdir(join(paths.legacyMirrorDir, "Result/2026"), { recursive: true });
    await writeFile(duplicatePath, "same", "utf8");
    await writeFile(changedPath, "old", "utf8");
    await writeFile(pendingPath, "pending", "utf8");
    await writeFile(orphanPath, "orphan", "utf8");
    await utimes(duplicatePath, new Date(duplicate.lastModified), new Date(duplicate.lastModified));
    await utimes(changedPath, new Date(changed.lastModified), new Date(changed.lastModified));
    await utimes(pendingPath, new Date(pending.lastModified), new Date(pending.lastModified));
    const store = new BaidupanBackupStore({ dbPath: paths.dbPath });
    for (const object of [duplicate, changed, pending]) {
      store.upsertCandidate({
        bucket: BUCKET,
        objectKey: object.key,
        size: object.size,
        mtime: Math.floor(Date.parse(object.lastModified) / 1000),
        etag: object.etag,
        completed: object.key === duplicate.key,
      });
    }
    store.close();

    const summary = await cleanupLegacyMirror({
      mirrorDir: paths.legacyMirrorDir,
      inventoryPath: paths.inventoryPath,
      dbPath: paths.dbPath,
      bucket: BUCKET,
    });

    assert.equal(summary.removedCount, 1);
    assert.equal(summary.removedBytes, 4);
    assert.equal(summary.retainedCount, 3);
    assert.equal(await pathExists(duplicatePath), false);
    assert.equal(await pathExists(changedPath), true);
    assert.equal(await pathExists(pendingPath), true);
    assert.equal(await pathExists(orphanPath), true);
  });
});

test("inventory parsing rejects errors instead of treating a partial listing as complete", async () => {
  await withFixture(async (paths) => {
    await writeFile(paths.inventoryPath, `${JSON.stringify({
      status: "error",
      error: "Access denied",
    })}\n`, "utf8");
    await assert.rejects(
      readMinioInventory(paths.inventoryPath),
      /MinIO inventory failed/,
    );
  });
});

test("schema version 6 upgrades in place without touching lifecycle transfer tables", async () => {
  const root = await mkdtemp(join(tmpdir(), "baidupan-schema-v6-"));
  const db = new Database(join(root, "state.sqlite"));
  try {
    db.exec(`
      CREATE TABLE schema_migrations (
        version INTEGER PRIMARY KEY,
        applied_at TEXT NOT NULL
      );
      INSERT INTO schema_migrations(version, applied_at)
      VALUES (6, '2026-07-16T00:00:00.000Z');

      CREATE TABLE baidupan_backup_objects (
        bucket TEXT NOT NULL,
        object_key TEXT NOT NULL,
        source_size INTEGER,
        source_mtime INTEGER,
        PRIMARY KEY(bucket, object_key)
      );
      CREATE TABLE baidupan_backup_run_items (
        run_id TEXT NOT NULL,
        bucket TEXT NOT NULL,
        object_key TEXT NOT NULL,
        source_size INTEGER,
        source_mtime INTEGER,
        PRIMARY KEY(run_id, bucket, object_key)
      );
      CREATE TABLE transfer_items (
        id TEXT PRIMARY KEY,
        status TEXT NOT NULL
      );
      INSERT INTO transfer_items(id, status) VALUES ('existing-transfer', 'SUCCEEDED');
    `);

    migrateBaidupanBackupSchema(db);

    assert.ok(
      db.prepare("PRAGMA table_info(baidupan_backup_objects)").all()
        .some((column) => column.name === "source_etag"),
    );
    assert.ok(
      db.prepare("PRAGMA table_info(baidupan_backup_run_items)").all()
        .some((column) => column.name === "source_etag"),
    );
    assert.deepEqual(
      db.prepare("SELECT * FROM transfer_items").all(),
      [{ id: "existing-transfer", status: "SUCCEEDED" }],
    );
    assert.equal(
      db.prepare("SELECT MAX(version) AS version FROM schema_migrations").get().version,
      7,
    );
  } finally {
    db.close();
    await rm(root, { recursive: true, force: true });
  }
});
