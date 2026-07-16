#!/usr/bin/env node

import { createReadStream } from "node:fs";
import { access, mkdir, open, readdir, rename, stat } from "node:fs/promises";
import { createInterface } from "node:readline";
import { basename, dirname, isAbsolute, join, relative, resolve, sep } from "node:path";
import { fileURLToPath } from "node:url";
import { BaidupanBackupStore } from "../web/backend/baidupan-backup-store.mjs";

function signatureKey(bucket, relativePath, size, mtime) {
  return JSON.stringify([bucket, relativePath, String(size), String(mtime)]);
}

function normalizeRelativePath(value) {
  const normalized = String(value || "").replace(/^\/+/, "");
  if (!normalized || isAbsolute(normalized)) return null;
  const segments = normalized.split("/");
  if (segments.some((segment) => !segment || segment === "." || segment === "..")) return null;
  return normalized;
}

async function pathExists(path) {
  try {
    await access(path);
    return true;
  } catch {
    return false;
  }
}

async function atomicWrite(path, content) {
  await mkdir(dirname(path), { recursive: true });
  const temporaryPath = join(dirname(path), `.${basename(path)}.${process.pid}.tmp`);
  const handle = await open(temporaryPath, "w");
  try {
    await handle.writeFile(content, "utf8");
    await handle.sync();
  } finally {
    await handle.close();
  }
  await rename(temporaryPath, path);
}

async function readLegacyUploadedSignatures(path) {
  const signatures = new Set();
  if (!(await pathExists(path))) return signatures;
  const lines = createInterface({ input: createReadStream(path), crlfDelay: Infinity });
  for await (const line of lines) {
    if (!line) continue;
    const [bucket, relativePath, size, mtime] = line.split("\t");
    if (bucket && relativePath && size !== undefined && mtime !== undefined) {
      signatures.add(signatureKey(bucket, relativePath, size, mtime));
    }
  }
  return signatures;
}

function relativePathFromMirrorEvent(event, bucket) {
  for (const candidate of [event?.target, event?.source]) {
    if (typeof candidate !== "string") continue;
    const normalized = candidate.replaceAll("\\", "/");
    for (const marker of [`/mirror/${bucket}/`, `/${bucket}/`]) {
      const markerIndex = normalized.lastIndexOf(marker);
      if (markerIndex < 0) continue;
      const relativePath = normalizeRelativePath(normalized.slice(markerIndex + marker.length));
      if (relativePath) return relativePath;
    }
  }
  return null;
}

async function readMirrorChanges(path, bucket) {
  const changes = new Set();
  if (!path || !(await pathExists(path))) return changes;
  const lines = createInterface({ input: createReadStream(path), crlfDelay: Infinity });
  let lineNumber = 0;
  for await (const line of lines) {
    lineNumber += 1;
    if (!line.trim()) continue;
    let event;
    try {
      event = JSON.parse(line);
    } catch (error) {
      throw new Error(`Invalid mc mirror report at ${path}:${lineNumber}: ${error.message}`);
    }
    if (event.status !== "success" || !event.target) continue;
    const relativePath = relativePathFromMirrorEvent(event, bucket);
    if (relativePath) changes.add(relativePath);
  }
  return changes;
}

async function listMirrorFiles(rootDir) {
  const files = [];
  async function walk(currentDir) {
    const entries = await readdir(currentDir, { withFileTypes: true });
    for (const entry of entries) {
      if (entry.name === ".DS_Store") continue;
      const absolutePath = join(currentDir, entry.name);
      if (entry.isDirectory()) {
        await walk(absolutePath);
      } else if (entry.isFile()) {
        const normalized = normalizeRelativePath(relative(rootDir, absolutePath).split(sep).join("/"));
        if (normalized) files.push(normalized);
      }
    }
  }
  await walk(rootDir);
  return files;
}

async function currentRecord({ bucket, mirrorDir, relativePath }) {
  const absolutePath = resolve(mirrorDir, ...relativePath.split("/"));
  if (!absolutePath.startsWith(`${resolve(mirrorDir)}${sep}`)) {
    throw new Error(`Unsafe backup path: ${relativePath}`);
  }
  try {
    const fileStat = await stat(absolutePath);
    if (!fileStat.isFile()) return null;
    return {
      bucket,
      relativePath,
      size: Number(fileStat.size),
      mtime: Math.floor(fileStat.mtimeMs / 1000),
    };
  } catch (error) {
    if (error?.code === "ENOENT") return null;
    throw error;
  }
}

function recordsToJsonl(records) {
  return records.length ? `${records.map((record) => JSON.stringify(record)).join("\n")}\n` : "";
}

export async function prepareBackupRun({
  runId,
  bucket,
  mirrorDir,
  mirrorReportPath,
  uploadedStatePath,
  manifestPath,
  dbPath,
  dryRun = false,
}) {
  const store = new BaidupanBackupStore({ dbPath });
  try {
    store.recoverInterruptedRuns();
    const baselineKey = `baidupan.incremental-baseline:${bucket}`;
    const bootstrap = !store.getMetadata(baselineKey);
    const mirrorChanges = await readMirrorChanges(mirrorReportPath, bucket);
    const candidates = new Set([
      ...store.listUnfinishedObjectKeys(bucket),
      ...mirrorChanges,
    ]);
    if (bootstrap) {
      for (const relativePath of await listMirrorFiles(mirrorDir)) candidates.add(relativePath);
    }
    const legacyUploaded = bootstrap
      ? await readLegacyUploadedSignatures(uploadedStatePath)
      : new Set();

    for (const relativePath of [...candidates].sort()) {
      const record = await currentRecord({ bucket, mirrorDir, relativePath });
      if (!record) {
        store.markMissing(bucket, relativePath);
        continue;
      }
      store.upsertCandidate({
        bucket,
        objectKey: relativePath,
        size: record.size,
        mtime: record.mtime,
        completed: legacyUploaded.has(signatureKey(bucket, relativePath, record.size, record.mtime)),
        forcePending: mirrorChanges.has(relativePath),
      });
    }
    if (bootstrap) {
      store.setMetadata(baselineKey, {
        version: 1,
        bucket,
        importedLegacyState: true,
        createdAt: new Date().toISOString(),
      });
    }

    const run = store.createRun({
      id: runId,
      bucket,
      manifestPath,
      discoveredCount: mirrorChanges.size,
      dryRun,
    });
    const manifestRecords = store.listRunItems(runId, "PENDING").map((item) => ({
      runId,
      bucket: item.bucket,
      relativePath: item.object_key,
      size: Number(item.source_size),
      mtime: Number(item.source_mtime),
    }));
    await atomicWrite(manifestPath, recordsToJsonl(manifestRecords));

    return {
      runId,
      bootstrap,
      changed: mirrorChanges.size,
      pending: Number(run.queued_count),
      missing: Number(run.missing_count),
      manifestPath,
    };
  } finally {
    store.close();
  }
}

function parseArguments(argv) {
  const args = new Map();
  const flags = new Set();
  for (let index = 0; index < argv.length; index += 1) {
    const key = argv[index];
    if (!key.startsWith("--")) throw new Error(`Unexpected argument: ${key}`);
    if (key === "--dry-run") {
      flags.add("dry-run");
      continue;
    }
    const value = argv[index + 1];
    if (value === undefined || value.startsWith("--")) throw new Error(`Missing value for ${key}`);
    args.set(key.slice(2), value);
    index += 1;
  }
  return { args, flags };
}

async function main() {
  const { args, flags } = parseArguments(process.argv.slice(2));
  const required = ["run-id", "bucket", "mirror-dir", "uploaded-state", "manifest", "db"];
  for (const key of required) {
    if (!args.get(key)) throw new Error(`--${key} is required`);
  }
  const summary = await prepareBackupRun({
    runId: args.get("run-id"),
    bucket: args.get("bucket"),
    mirrorDir: resolve(args.get("mirror-dir")),
    mirrorReportPath: args.get("mirror-report") ? resolve(args.get("mirror-report")) : null,
    uploadedStatePath: resolve(args.get("uploaded-state")),
    manifestPath: resolve(args.get("manifest")),
    dbPath: resolve(args.get("db")),
    dryRun: flags.has("dry-run"),
  });
  console.log(`BACKUP_MANIFEST ${JSON.stringify(summary)}`);
}

if (process.argv[1] && resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  main().catch((error) => {
    console.error(error instanceof Error ? error.message : String(error));
    process.exitCode = 1;
  });
}
