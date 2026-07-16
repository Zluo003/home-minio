#!/usr/bin/env node

import { spawn } from "node:child_process";
import { createWriteStream } from "node:fs";
import { mkdir, rename, rm, stat } from "node:fs/promises";
import { dirname, resolve, sep } from "node:path";
import { pipeline } from "node:stream/promises";
import { BaidupanBackupStore } from "../web/backend/baidupan-backup-store.mjs";
import { createMinioSignedHttpClient } from "../web/backend/minio-signed-http.mjs";

function parseArguments(argv) {
  const args = new Map();
  const flags = new Set();
  for (let index = 0; index < argv.length; index += 1) {
    const key = argv[index];
    if (!key.startsWith("--")) throw new Error(`Unexpected argument: ${key}`);
    if (key === "--dry-run" || key === "--no-rapid") {
      flags.add(key.slice(2));
      continue;
    }
    const value = argv[index + 1];
    if (value === undefined || value.startsWith("--")) throw new Error(`Missing value for ${key}`);
    args.set(key.slice(2), value);
    index += 1;
  }
  return { args, flags };
}

function remotePathForFile(tool, remoteDir, bucket, objectKey) {
  return tool === "baidupcs"
    ? `/${remoteDir.replace(/^\/+|\/+$/g, "")}/${bucket}/${objectKey}`
    : `${remoteDir.replace(/\/+$/, "")}/${bucket}/${objectKey}`;
}

function remoteDirname(path) {
  const index = path.lastIndexOf("/");
  return index <= 0 ? "/" : path.slice(0, index);
}

function normalizeEtag(value) {
  const normalized = String(value || "").trim().replace(/^"|"$/g, "");
  return normalized || null;
}

function runCommand(command, args, { allowFailure = false } = {}) {
  return new Promise((resolvePromise, reject) => {
    const child = spawn(command, args, { env: process.env, stdio: ["ignore", "pipe", "pipe"] });
    let errorTail = "";
    child.stdout.on("data", (chunk) => process.stdout.write(chunk));
    child.stderr.on("data", (chunk) => {
      process.stderr.write(chunk);
      errorTail = `${errorTail}${chunk.toString("utf8")}`.slice(-4000);
    });
    child.once("error", reject);
    child.once("exit", (code, signal) => {
      if (code === 0 || allowFailure) {
        resolvePromise({ code: code ?? 1, signal, errorTail });
      } else {
        reject(new Error(errorTail.trim() || `${command} exited with ${code ?? signal}`));
      }
    });
  });
}

function spoolPathForItem(runSpoolDir, item) {
  const filePath = resolve(runSpoolDir, ...item.object_key.split("/"));
  if (!filePath.startsWith(`${resolve(runSpoolDir)}${sep}`)) {
    throw new Error(`Unsafe backup object key: ${item.object_key}`);
  }
  return filePath;
}

async function downloadToSpool(sourceClient, runSpoolDir, item) {
  const filePath = spoolPathForItem(runSpoolDir, item);
  const partialPath = `${filePath}.part`;
  await mkdir(dirname(filePath), { recursive: true });
  await rm(partialPath, { force: true });
  await rm(filePath, { force: true });

  const source = await sourceClient.getObject(item.object_key);
  const expectedSize = Number(item.source_size);
  const responseSize = Number(source.sizeBytes);
  const expectedEtag = normalizeEtag(item.source_etag);
  const responseEtag = normalizeEtag(source.etag);
  const responseMtime = Date.parse(source.lastModified);
  if (Number.isSafeInteger(responseSize) && responseSize >= 0 && responseSize !== expectedSize) {
    throw new Error(`MinIO object size changed before backup: expected ${expectedSize}, received ${responseSize}`);
  }
  if (expectedEtag && responseEtag && expectedEtag !== responseEtag) {
    throw new Error(`MinIO object ETag changed before backup: expected ${expectedEtag}, received ${responseEtag}`);
  }
  if (Number.isFinite(responseMtime) && Math.floor(responseMtime / 1000) !== Number(item.source_mtime)) {
    throw new Error("MinIO object modification time changed before backup");
  }

  try {
    await pipeline(source.body, createWriteStream(partialPath, { flags: "wx", mode: 0o600 }));
    const fileStat = await stat(partialPath);
    if (!fileStat.isFile() || Number(fileStat.size) !== expectedSize) {
      throw new Error(`Downloaded backup file size mismatch: expected ${expectedSize}, received ${fileStat.size}`);
    }
    await rename(partialPath, filePath);
    return filePath;
  } catch (error) {
    await rm(partialPath, { force: true }).catch(() => {});
    await rm(filePath, { force: true }).catch(() => {});
    throw error;
  }
}

function createBackupSourceClient(bucket) {
  const useRootCredentials = process.env.MINIO_ROOT_USER && process.env.MINIO_ROOT_PASSWORD;
  return createMinioSignedHttpClient(process.env, {
    bucket,
    ...(useRootCredentials
      ? {
          accessKeyId: process.env.MINIO_ROOT_USER,
          secretAccessKey: process.env.MINIO_ROOT_PASSWORD,
        }
      : {}),
  });
}

async function main() {
  const { args, flags } = parseArguments(process.argv.slice(2));
  const required = ["run-id", "spool-dir", "db", "tool", "remote-dir"];
  for (const key of required) {
    if (!args.get(key)) throw new Error(`--${key} is required`);
  }
  const runId = args.get("run-id");
  const spoolDir = resolve(args.get("spool-dir"));
  const runSpoolDir = resolve(spoolDir, runId);
  if (!runSpoolDir.startsWith(`${spoolDir}${sep}`)) throw new Error("Unsafe backup run id.");
  const tool = args.get("tool");
  if (!new Set(["baidupcs", "bypy"]).has(tool)) throw new Error("--tool must be baidupcs or bypy");
  const bin = tool === "baidupcs" ? args.get("baidupcs-bin") || "BaiduPCS-Go" : args.get("bypy-bin") || "bypy";
  const configuredMaxParallel = Number(args.get("max-parallel") || 16);
  const maxParallel = Number.isSafeInteger(configuredMaxParallel) && configuredMaxParallel > 0
    ? configuredMaxParallel
    : 16;
  const dryRun = flags.has("dry-run");
  const createdRemoteDirs = new Set();
  const store = new BaidupanBackupStore({ dbPath: resolve(args.get("db")) });
  let sourceClient;

  try {
    const run = store.getRun(runId);
    if (!run) throw new Error(`Backup run not found: ${runId}`);
    if (!dryRun) sourceClient = createBackupSourceClient(run.bucket);
    await rm(runSpoolDir, { recursive: true, force: true });
    await mkdir(runSpoolDir, { recursive: true });
    store.startRun(runId);
    const items = store.listRunItems(runId, "PENDING");
    for (const item of items) {
      const remotePath = remotePathForFile(tool, args.get("remote-dir"), item.bucket, item.object_key);
      if (dryRun) {
        console.log(`DRY ${item.object_key} -> ${remotePath}`);
        store.markDryRunItem(runId, item);
        continue;
      }

      store.startItem(runId, item.bucket, item.object_key);
      let filePath = null;
      try {
        filePath = await downloadToSpool(sourceClient, runSpoolDir, item);
        if (tool === "baidupcs") {
          const remoteDirectory = remoteDirname(remotePath);
          if (!createdRemoteDirs.has(remoteDirectory)) {
            const mkdirResult = await runCommand(bin, ["mkdir", remoteDirectory], { allowFailure: true });
            if (mkdirResult.code === 0) createdRemoteDirs.add(remoteDirectory);
          }
          const uploadArgs = ["upload", filePath, remoteDirectory, "--policy", "rsync"];
          if (flags.has("no-rapid")) uploadArgs.push("--norapid");
          uploadArgs.push("-p", String(maxParallel));
          console.log(`UPLOAD ${item.object_key} -> ${remotePath}`);
          await runCommand(bin, uploadArgs);
        } else {
          console.log(`UPLOAD ${item.object_key} -> ${remotePath}`);
          await runCommand(bin, ["upload", filePath, remotePath]);
        }
        store.completeItem(runId, item, remotePath);
      } catch (error) {
        const message = error instanceof Error ? error.message : String(error);
        store.failItem(runId, item, message);
        console.error(`FAILED ${item.object_key}: ${message}`);
      } finally {
        if (filePath) await rm(filePath, { force: true }).catch((error) => {
          console.error(`SPOOL_CLEANUP_FAILED ${item.object_key}: ${error.message}`);
        });
      }
    }

    const finished = store.finishRun(runId);
    console.log(`BACKUP_DONE run=${runId} status=${finished.status} succeeded=${finished.succeeded_count} failed=${finished.failed_count} missing=${finished.missing_count}`);
    if (!dryRun && Number(finished.failed_count) > 0) process.exitCode = 1;
  } finally {
    await rm(runSpoolDir, { recursive: true, force: true }).catch((error) => {
      console.error(`SPOOL_CLEANUP_FAILED run=${runId}: ${error.message}`);
    });
    sourceClient?.destroy();
    store.close();
  }
}

main().catch((error) => {
  console.error(error instanceof Error ? error.message : String(error));
  process.exitCode = 1;
});
