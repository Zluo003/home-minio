#!/usr/bin/env node

import { createReadStream } from "node:fs";
import { access, mkdir, open, rename } from "node:fs/promises";
import { createInterface } from "node:readline";
import { basename, dirname, isAbsolute, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { BaidupanBackupStore } from "../web/backend/baidupan-backup-store.mjs";

function signatureKey(bucket, objectKey, size, mtime) {
  return JSON.stringify([bucket, objectKey, String(size), String(mtime)]);
}

function normalizeObjectKey(value) {
  const normalized = String(value || "").replace(/^\/+/, "");
  if (!normalized || isAbsolute(normalized)) return null;
  const segments = normalized.split("/");
  if (segments.some((segment) => !segment || segment === "." || segment === "..")) return null;
  return normalized;
}

function normalizeEtag(value) {
  const normalized = String(value || "").trim().replace(/^"|"$/g, "");
  return normalized || null;
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
    const [bucket, objectKey, size, mtime] = line.split("\t");
    if (bucket && objectKey && size !== undefined && mtime !== undefined) {
      signatures.add(signatureKey(bucket, objectKey, size, mtime));
    }
  }
  return signatures;
}

function inventoryRecord(event, lineNumber) {
  if (event?.status && event.status !== "success") {
    throw new Error(`MinIO inventory failed at line ${lineNumber}: ${event.error || event.message || event.status}`);
  }
  if (event?.type && event.type !== "file") return null;
  const objectKey = normalizeObjectKey(event?.key);
  if (!objectKey) return null;
  const size = Number(event.size);
  const timestamp = Date.parse(event.lastModified);
  if (!Number.isSafeInteger(size) || size < 0 || !Number.isFinite(timestamp)) {
    throw new Error(`Invalid MinIO inventory record at line ${lineNumber}: ${objectKey}`);
  }
  return {
    objectKey,
    size,
    mtime: Math.floor(timestamp / 1000),
    etag: normalizeEtag(event.etag),
  };
}

export async function readMinioInventory(path) {
  const recordsByKey = new Map();
  const lines = createInterface({ input: createReadStream(path), crlfDelay: Infinity });
  let lineNumber = 0;
  for await (const line of lines) {
    lineNumber += 1;
    if (!line.trim()) continue;
    let event;
    try {
      event = JSON.parse(line);
    } catch (error) {
      throw new Error(`Invalid MinIO inventory JSON at ${path}:${lineNumber}: ${error.message}`);
    }
    const record = inventoryRecord(event, lineNumber);
    if (record) recordsByKey.set(record.objectKey, record);
  }
  return [...recordsByKey.values()].sort((left, right) => left.objectKey.localeCompare(right.objectKey));
}

function recordsToJsonl(records) {
  return records.length ? `${records.map((record) => JSON.stringify(record)).join("\n")}\n` : "";
}

export async function prepareBackupRun({
  runId,
  bucket,
  inventoryPath,
  uploadedStatePath,
  manifestPath,
  dbPath,
  dryRun = false,
}) {
  const inventory = await readMinioInventory(inventoryPath);
  const store = new BaidupanBackupStore({ dbPath });
  try {
    store.recoverInterruptedRuns();
    const baselineKey = `baidupan.incremental-baseline:${bucket}`;
    const bootstrap = !store.getMetadata(baselineKey);
    const legacyUploadedSignatures = bootstrap
      ? await readLegacyUploadedSignatures(uploadedStatePath)
      : new Set();
    const reconciliation = store.reconcileInventory({
      bucket,
      records: inventory,
      legacyUploadedSignatures,
    });
    store.setMetadata(baselineKey, {
      version: 2,
      source: "minio-inventory",
      bucket,
      importedLegacyState: bootstrap,
      updatedAt: new Date().toISOString(),
    });

    const run = store.createRun({
      id: runId,
      bucket,
      manifestPath,
      discoveredCount: reconciliation.discoveredCount,
      dryRun,
    });
    const manifestRecords = store.listRunItems(runId, "PENDING").map((item) => ({
      runId,
      bucket: item.bucket,
      relativePath: item.object_key,
      size: Number(item.source_size),
      mtime: Number(item.source_mtime),
      etag: item.source_etag || null,
    }));
    await atomicWrite(manifestPath, recordsToJsonl(manifestRecords));

    return {
      runId,
      bootstrap,
      scanned: reconciliation.objectCount,
      changed: reconciliation.discoveredCount,
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
  const required = ["run-id", "bucket", "inventory", "uploaded-state", "manifest", "db"];
  for (const key of required) {
    if (!args.get(key)) throw new Error(`--${key} is required`);
  }
  const summary = await prepareBackupRun({
    runId: args.get("run-id"),
    bucket: args.get("bucket"),
    inventoryPath: resolve(args.get("inventory")),
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
