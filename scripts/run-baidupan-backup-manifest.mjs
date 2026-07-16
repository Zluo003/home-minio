#!/usr/bin/env node

import { spawn } from "node:child_process";
import { stat } from "node:fs/promises";
import { resolve, sep } from "node:path";
import { BaidupanBackupStore } from "../web/backend/baidupan-backup-store.mjs";

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

async function verifyManifestFile(mirrorDir, item) {
  const filePath = resolve(mirrorDir, ...item.object_key.split("/"));
  if (!filePath.startsWith(`${resolve(mirrorDir)}${sep}`)) {
    throw new Error(`Unsafe manifest path: ${item.object_key}`);
  }
  const fileStat = await stat(filePath);
  const size = Number(fileStat.size);
  const mtime = Math.floor(fileStat.mtimeMs / 1000);
  if (!fileStat.isFile() || size !== Number(item.source_size) || mtime !== Number(item.source_mtime)) {
    throw new Error("Local mirror file changed after manifest creation");
  }
  return filePath;
}

async function main() {
  const { args, flags } = parseArguments(process.argv.slice(2));
  const required = ["run-id", "mirror-dir", "db", "tool", "remote-dir"];
  for (const key of required) {
    if (!args.get(key)) throw new Error(`--${key} is required`);
  }
  const runId = args.get("run-id");
  const mirrorDir = resolve(args.get("mirror-dir"));
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

  try {
    const run = store.getRun(runId);
    if (!run) throw new Error(`Backup run not found: ${runId}`);
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
      try {
        const filePath = await verifyManifestFile(mirrorDir, item);
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
      }
    }

    const finished = store.finishRun(runId);
    console.log(`BACKUP_DONE run=${runId} status=${finished.status} succeeded=${finished.succeeded_count} failed=${finished.failed_count} missing=${finished.missing_count}`);
    if (!dryRun && Number(finished.failed_count) > 0) process.exitCode = 1;
  } finally {
    store.close();
  }
}

main().catch((error) => {
  console.error(error instanceof Error ? error.message : String(error));
  process.exitCode = 1;
});
