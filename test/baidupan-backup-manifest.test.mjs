import assert from "node:assert/strict";
import { execFile } from "node:child_process";
import { chmod, mkdir, mkdtemp, readFile, rm, stat, utimes, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { promisify } from "node:util";
import test from "node:test";
import { prepareBackupRun } from "../scripts/build-baidupan-backup-manifest.mjs";
import { BaidupanBackupStore } from "../web/backend/baidupan-backup-store.mjs";
import { LifecycleStore } from "../web/backend/lifecycle-store.mjs";

const execFileAsync = promisify(execFile);

async function readJsonl(path) {
  const content = await readFile(path, "utf8");
  return content.split(/\r?\n/).filter(Boolean).map((line) => JSON.parse(line));
}

async function withFixture(callback) {
  const root = await mkdtemp(join(tmpdir(), "baidupan-manifest-"));
  const paths = {
    root,
    dbPath: join(root, "state", "home-minio.sqlite"),
    mirrorDir: join(root, "mirror", "waule-media"),
    mirrorReportPath: join(root, "state", "mirror.jsonl"),
    uploadedStatePath: join(root, "state", "uploaded.tsv"),
    manifestPath: join(root, "state", "manifest.jsonl"),
  };
  await mkdir(join(paths.mirrorDir, "Result", "2026"), { recursive: true });
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

test("first run imports legacy success state and manifests only unfinished files", async () => {
  await withFixture(async (paths) => {
    const completedPath = join(paths.mirrorDir, "Result", "2026", "completed.png");
    const pendingPath = join(paths.mirrorDir, "Result", "2026", "pending.png");
    await writeFile(completedPath, "done", "utf8");
    await writeFile(pendingPath, "pending", "utf8");
    await utimes(completedPath, 1_700_000_000, 1_700_000_000);
    await utimes(pendingPath, 1_700_000_001, 1_700_000_001);
    await writeFile(
      paths.uploadedStatePath,
      "waule-media\tResult/2026/completed.png\t4\t1700000000\n",
      "utf8",
    );

    const summary = await prepareBackupRun({ runId: "bootstrap-run", bucket: "waule-media", ...paths });
    const manifest = await readJsonl(paths.manifestPath);
    const store = new BaidupanBackupStore({ dbPath: paths.dbPath });
    const completed = store.db.prepare(`
      SELECT status FROM baidupan_backup_objects
      WHERE bucket = 'waule-media' AND object_key = 'Result/2026/completed.png'
    `).get();
    store.close();

    assert.equal(summary.bootstrap, true);
    assert.equal(completed.status, "COMPLETED");
    assert.deepEqual(manifest.map((record) => record.relativePath), ["Result/2026/pending.png"]);
  });
});

test("later runs combine mirror changes with database retries without rescanning unrelated files", async () => {
  await withFixture(async (paths) => {
    const oldPendingPath = join(paths.mirrorDir, "Result", "2026", "old-failure.png");
    const changedPath = join(paths.mirrorDir, "Result", "2026", "changed.png");
    const unrelatedPath = join(paths.mirrorDir, "Result", "2026", "unrelated.png");
    await writeFile(oldPendingPath, "retry", "utf8");
    await writeFile(changedPath, "changed", "utf8");
    await writeFile(unrelatedPath, "not-discovered", "utf8");
    const oldPendingStat = await stat(oldPendingPath);

    const store = new BaidupanBackupStore({ dbPath: paths.dbPath });
    store.setMetadata("baidupan.incremental-baseline:waule-media", { version: 1 });
    store.upsertCandidate({
      bucket: "waule-media",
      objectKey: "Result/2026/old-failure.png",
      size: oldPendingStat.size,
      mtime: Math.floor(oldPendingStat.mtimeMs / 1000),
    });
    store.close();
    await writeFile(paths.mirrorReportPath, `${JSON.stringify({
      status: "success",
      target: "/backup/mirror/waule-media/Result/2026/changed.png",
      size: 7,
    })}\n${JSON.stringify({ status: "success", total: 7, transferred: 7 })}\n`, "utf8");

    const summary = await prepareBackupRun({ runId: "incremental-run", bucket: "waule-media", ...paths });
    const manifest = await readJsonl(paths.manifestPath);

    assert.equal(summary.bootstrap, false);
    assert.equal(summary.changed, 1);
    assert.deepEqual(
      manifest.map((record) => record.relativePath),
      ["Result/2026/changed.png", "Result/2026/old-failure.png"],
    );
  });
});

test("an mc-reported change is queued even when size and mtime match the completed record", async () => {
  await withFixture(async (paths) => {
    const changedPath = join(paths.mirrorDir, "Result", "2026", "same-key.png");
    await writeFile(changedPath, "new-version", "utf8");
    await utimes(changedPath, 1_700_000_100, 1_700_000_100);
    const store = new BaidupanBackupStore({ dbPath: paths.dbPath });
    store.setMetadata("baidupan.incremental-baseline:waule-media", { version: 1 });
    store.upsertCandidate({
      bucket: "waule-media",
      objectKey: "Result/2026/same-key.png",
      size: 11,
      mtime: 1_700_000_100,
      completed: true,
    });
    store.close();
    await writeFile(paths.mirrorReportPath, `${JSON.stringify({
      status: "success",
      target: "/backup/mirror/waule-media/Result/2026/same-key.png",
    })}\n`, "utf8");

    const summary = await prepareBackupRun({ runId: "changed-run", bucket: "waule-media", ...paths });
    const [record] = await readJsonl(paths.manifestPath);

    assert.equal(summary.pending, 1);
    assert.equal(record.relativePath, "Result/2026/same-key.png");
    assert.equal(record.size, 11);
    assert.equal(record.mtime, 1_700_000_100);
  });
});

test("completed items are durable and do not enter a later manifest", async () => {
  await withFixture(async (paths) => {
    const objectPath = join(paths.mirrorDir, "Result", "2026", "durable.png");
    await writeFile(objectPath, "durable", "utf8");
    await prepareBackupRun({ runId: "first-run", bucket: "waule-media", ...paths });

    const store = new BaidupanBackupStore({ dbPath: paths.dbPath });
    store.startRun("first-run");
    const [item] = store.listRunItems("first-run", "PENDING");
    store.startItem("first-run", item.bucket, item.object_key);
    store.completeItem("first-run", item, "/backup/durable.png");
    store.finishRun("first-run");
    store.close();

    paths.manifestPath = join(paths.root, "state", "second-manifest.jsonl");
    const summary = await prepareBackupRun({ runId: "second-run", bucket: "waule-media", ...paths });

    assert.equal(summary.pending, 0);
    assert.deepEqual(await readJsonl(paths.manifestPath), []);
  });
});

test("one upload failure is persisted without preventing later manifest items from succeeding", async () => {
  await withFixture(async (paths) => {
    await writeFile(join(paths.mirrorDir, "Result", "2026", "fail.png"), "fail", "utf8");
    await writeFile(join(paths.mirrorDir, "Result", "2026", "success.png"), "success", "utf8");
    await prepareBackupRun({ runId: "partial-run", bucket: "waule-media", ...paths });

    const fakeBaidupcs = join(paths.root, "fake-baidupcs.sh");
    await writeFile(fakeBaidupcs, `#!/bin/sh
case "$*" in
  *fail.png*) echo "injected upload failure" >&2; exit 7 ;;
  *) exit 0 ;;
esac
`, "utf8");
    await chmod(fakeBaidupcs, 0o755);

    await assert.rejects(
      execFileAsync(process.execPath, [
        join(process.cwd(), "scripts", "run-baidupan-backup-manifest.mjs"),
        "--run-id", "partial-run",
        "--mirror-dir", paths.mirrorDir,
        "--db", paths.dbPath,
        "--tool", "baidupcs",
        "--remote-dir", "/NewWaule/home-minio",
        "--baidupcs-bin", fakeBaidupcs,
      ]),
    );

    const store = new BaidupanBackupStore({ dbPath: paths.dbPath });
    const objects = store.db.prepare(`
      SELECT object_key, status FROM baidupan_backup_objects
      WHERE bucket = 'waule-media' ORDER BY object_key
    `).all();
    const run = store.getRun("partial-run");
    store.close();

    assert.deepEqual(objects, [
      { object_key: "Result/2026/fail.png", status: "FAILED" },
      { object_key: "Result/2026/success.png", status: "COMPLETED" },
    ]);
    assert.equal(run.status, "SUCCEEDED_WITH_ERRORS");
    assert.equal(run.succeeded_count, 1);
    assert.equal(run.failed_count, 1);
  });
});
