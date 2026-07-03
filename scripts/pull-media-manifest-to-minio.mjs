import { createWriteStream } from "node:fs";
import { mkdir, readFile, rename, rm, stat, writeFile } from "node:fs/promises";
import { dirname, resolve } from "node:path";
import { spawn } from "node:child_process";
import { pipeline } from "node:stream/promises";

const rootDir = resolve(new URL("..", import.meta.url).pathname);
const envPath = resolve(rootDir, ".env");

function parseEnv(source) {
  const values = {};
  for (const line of source.split(/\r?\n/)) {
    const trimmed = line.trim();
    if (!trimmed || trimmed.startsWith("#") || !trimmed.includes("=")) continue;
    const index = line.indexOf("=");
    const key = line.slice(0, index).trim();
    let value = line.slice(index + 1).trim();
    if ((value.startsWith('"') && value.endsWith('"')) || (value.startsWith("'") && value.endsWith("'"))) {
      value = value.slice(1, -1);
    }
    values[key] = value;
  }
  return values;
}

async function loadEnv() {
  return parseEnv(await readFile(envPath, "utf8"));
}

function assertObjectKey(value) {
  const key = String(value || "").trim().replace(/\\/g, "/").replace(/^\/+/, "");
  if (!key || key.split("/").some((segment) => !segment || segment === "." || segment === "..")) {
    throw new Error(`Invalid objectKey: ${value}`);
  }
  return key;
}

const ARCHIVABLE_OBJECT_KEY_PREFIXES = ["New-Waule/Result/", "uploads/", "gateway-media/"];

function isArchivableObjectKey(key) {
  return ARCHIVABLE_OBJECT_KEY_PREFIXES.some((prefix) => key.startsWith(prefix));
}

function sourceUrlObjectKey(value) {
  try {
    const key = new URL(String(value || "")).pathname.replace(/^\/+/, "");
    return key.startsWith("local-media/") ? key.slice("local-media/".length) : key;
  } catch {
    return "";
  }
}

function run(command, args, options = {}) {
  return new Promise((resolveRun) => {
    const child = spawn(command, args, {
      cwd: rootDir,
      shell: false,
      env: { ...process.env, ...options.env },
      stdio: ["ignore", "pipe", "pipe"],
    });
    child.stdout.on("data", (chunk) => process.stdout.write(chunk));
    child.stderr.on("data", (chunk) => process.stderr.write(chunk));
    child.on("close", (code) => resolveRun(code ?? 1));
  });
}

async function downloadFile(url, targetPath) {
  const response = await fetch(url, { signal: AbortSignal.timeout(30 * 60 * 1000) });
  if (!response.ok || !response.body) {
    throw new Error(`Download failed ${response.status}: ${url}`);
  }
  const tempPath = `${targetPath}.${process.pid}.${Date.now()}.tmp`;
  await mkdir(dirname(targetPath), { recursive: true });
  try {
    await pipeline(response.body, createWriteStream(tempPath, { flags: "wx" }));
    await rename(tempPath, targetPath);
  } catch (error) {
    await rm(tempPath, { force: true }).catch(() => undefined);
    throw error;
  }
}

async function main() {
  const dryRun = process.argv.includes("--dry-run");
  const env = await loadEnv();
  const bucket = env.MINIO_BUCKET || "waule-media";
  const manifestPath = resolve(rootDir, env.MEDIA_PULL_MANIFEST_PATH || "./backup/newwaule-media-manifest.jsonl");
  const workDir = resolve(rootDir, env.MEDIA_PULL_WORK_DIR || "./backup/pull");
  const mirrorDir = resolve(workDir, "mirror", bucket);
  const manifest = await readFile(manifestPath, "utf8");
  const records = manifest
    .split(/\r?\n/)
    .map((line) => line.trim())
    .filter(Boolean)
    .map((line) => JSON.parse(line))
    .filter((record) => record.objectKey && record.sourceUrl);

  console.log(JSON.stringify({ dryRun, manifestPath, bucket, records: records.length }, null, 2));

  let downloaded = 0;
  let skipped = 0;
  let failed = 0;
  let rejected = 0;
  const failedRecords = [];

  for (const record of records) {
    const key = assertObjectKey(record.objectKey);
    const urlKey = sourceUrlObjectKey(record.sourceUrl);
    if (!isArchivableObjectKey(key) || urlKey !== key) {
      rejected += 1;
      console.error(`SKIP unsafe manifest record ${key} <- ${record.sourceUrl}`);
      continue;
    }
    const targetPath = resolve(mirrorDir, key);
    if (!targetPath.startsWith(`${mirrorDir}/`)) {
      throw new Error(`Invalid target path for ${key}`);
    }
    const expectedSize = record.sizeBytes ? Number(record.sizeBytes) : null;
    const existing = await stat(targetPath).catch(() => null);
    if (existing?.isFile() && (!expectedSize || existing.size === expectedSize)) {
      skipped += 1;
      console.log(`SKIP ${key}`);
      continue;
    }

    console.log(`${dryRun ? "DRY" : "GET"} ${record.storageProvider} ${key} <- ${record.sourceUrl}`);
    if (dryRun) {
      skipped += 1;
      continue;
    }

    try {
      await downloadFile(record.sourceUrl, targetPath);
      downloaded += 1;
    } catch (error) {
      failed += 1;
      failedRecords.push({
        objectKey: key,
        storageProvider: record.storageProvider,
        sourceUrl: record.sourceUrl,
        message: error instanceof Error ? error.message : String(error),
      });
      console.error(`FAILED ${key}: ${error instanceof Error ? error.message : String(error)}`);
    }
  }

  if (!dryRun && downloaded + skipped > 0) {
    const code = await run("docker", [
      "run",
      "--rm",
      "--network",
      "container:home-minio",
      "-v",
      `${workDir}:/work`,
      "-e",
      `MINIO_ROOT_USER=${env.MINIO_ROOT_USER}`,
      "-e",
      `MINIO_ROOT_PASSWORD=${env.MINIO_ROOT_PASSWORD}`,
      "-e",
      `MINIO_BUCKET=${bucket}`,
      env.MC_IMAGE || "quay.io/minio/mc:RELEASE.2026-06-13T12-46-12Z",
      "sh",
      "-eu",
      "-c",
      'mc alias set home http://127.0.0.1:9000 "$MINIO_ROOT_USER" "$MINIO_ROOT_PASSWORD" >/dev/null && mc mb --ignore-existing "home/$MINIO_BUCKET" >/dev/null && mc mirror --overwrite --preserve "/work/mirror/$MINIO_BUCKET" "home/$MINIO_BUCKET"',
    ]);
    if (code !== 0) {
      throw new Error(`mc mirror failed with exit code ${code}`);
    }
  }

  if (!dryRun && failedRecords.length > 0) {
    const failedPath = resolve(workDir, `failed-${Date.now()}.jsonl`);
    await mkdir(dirname(failedPath), { recursive: true });
    await writeFile(failedPath, `${failedRecords.map((record) => JSON.stringify(record)).join("\n")}\n`, "utf8");
    console.error(`FAILED_RECORDS ${failedPath}`);
  }

  console.log(JSON.stringify({ downloaded, skipped, failed, rejected }, null, 2));
}

main().catch((error) => {
  console.error(error);
  process.exitCode = 1;
});
