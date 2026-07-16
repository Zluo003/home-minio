#!/usr/bin/env node

import { readdir, rm, rmdir, stat } from "node:fs/promises";
import { relative, resolve, sep } from "node:path";
import { fileURLToPath } from "node:url";
import { readMinioInventory } from "./build-baidupan-backup-manifest.mjs";
import { BaidupanBackupStore } from "../web/backend/baidupan-backup-store.mjs";

function normalizeRelativePath(rootDir, absolutePath) {
  const value = relative(rootDir, absolutePath).split(sep).join("/");
  if (!value || value.startsWith("../") || value === "..") return null;
  return value;
}

export async function cleanupLegacyMirror({ mirrorDir, inventoryPath, dbPath, bucket }) {
  const rootDir = resolve(mirrorDir);
  const inventory = new Map(
    (await readMinioInventory(inventoryPath)).map((record) => [record.objectKey, record]),
  );
  const store = new BaidupanBackupStore({ dbPath });
  const completed = new Map(
    store.listCompletedObjects(bucket).map((record) => [record.object_key, record]),
  );
  store.close();
  const summary = {
    removedCount: 0,
    removedBytes: 0,
    retainedCount: 0,
    retainedBytes: 0,
  };

  async function walk(currentDir) {
    let entries;
    try {
      entries = await readdir(currentDir, { withFileTypes: true });
    } catch (error) {
      if (error?.code === "ENOENT") return;
      throw error;
    }
    for (const entry of entries) {
      const absolutePath = resolve(currentDir, entry.name);
      if (!absolutePath.startsWith(`${rootDir}${sep}`)) {
        throw new Error(`Unsafe legacy mirror path: ${absolutePath}`);
      }
      if (entry.isDirectory()) {
        await walk(absolutePath);
        await rmdir(absolutePath).catch((error) => {
          if (error?.code !== "ENOTEMPTY" && error?.code !== "EEXIST" && error?.code !== "ENOENT") throw error;
        });
        continue;
      }
      if (!entry.isFile()) continue;
      const objectKey = normalizeRelativePath(rootDir, absolutePath);
      const source = objectKey ? inventory.get(objectKey) : null;
      const backup = objectKey ? completed.get(objectKey) : null;
      const fileStat = await stat(absolutePath);
      const matchesSource = source
        && Number(fileStat.size) === Number(source.size)
        && Math.floor(fileStat.mtimeMs / 1000) === Number(source.mtime);
      const matchesCompletedBackup = backup
        && Number(backup.source_size) === Number(source?.size)
        && Number(backup.source_mtime) === Number(source?.mtime)
        && (
          !backup.source_etag
          || !source?.etag
          || backup.source_etag === source.etag
        );
      if (matchesSource && matchesCompletedBackup) {
        await rm(absolutePath);
        summary.removedCount += 1;
        summary.removedBytes += Number(fileStat.size);
      } else {
        summary.retainedCount += 1;
        summary.retainedBytes += Number(fileStat.size);
      }
    }
  }

  await walk(rootDir);
  await rmdir(rootDir).catch((error) => {
    if (error?.code !== "ENOTEMPTY" && error?.code !== "EEXIST" && error?.code !== "ENOENT") throw error;
  });
  return summary;
}

function parseArguments(argv) {
  const args = new Map();
  for (let index = 0; index < argv.length; index += 2) {
    const key = argv[index];
    const value = argv[index + 1];
    if (!key?.startsWith("--") || !value) throw new Error(`Invalid argument: ${key || ""}`);
    args.set(key.slice(2), value);
  }
  return args;
}

async function main() {
  const args = parseArguments(process.argv.slice(2));
  for (const key of ["mirror-dir", "inventory", "db", "bucket"]) {
    if (!args.get(key)) throw new Error(`--${key} is required`);
  }
  const summary = await cleanupLegacyMirror({
    mirrorDir: args.get("mirror-dir"),
    inventoryPath: args.get("inventory"),
    dbPath: args.get("db"),
    bucket: args.get("bucket"),
  });
  console.log(`LEGACY_MIRROR_CLEANUP ${JSON.stringify(summary)}`);
  if (summary.retainedCount > 0) {
    console.warn(
      `Retained ${summary.retainedCount} legacy mirror files because backup completion or an exact MinIO match could not be confirmed.`,
    );
  }
}

if (process.argv[1] && resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  main().catch((error) => {
    console.error(error instanceof Error ? error.message : String(error));
    process.exitCode = 1;
  });
}
