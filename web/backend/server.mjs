import { createServer } from "node:http";
import { spawn } from "node:child_process";
import { randomUUID } from "node:crypto";
import { mkdir, readFile, writeFile } from "node:fs/promises";
import { dirname, resolve } from "node:path";

const rootDir = resolve(new URL("../..", import.meta.url).pathname);
const envPath = resolve(rootDir, ".env");
const port = Number(process.env.HOME_MINIO_WEB_API_PORT || 19090);
const token = process.env.HOME_MINIO_WEB_TOKEN || "";
const outputTailLimit = 12000;
const pullJobs = new Map();
let latestPullJobId = "";
const pushJobs = new Map();
const pushQueue = [];
let activePushJobs = 0;
let latestPushJobId = "";

const editableKeys = [
  "MINIO_ROOT_USER",
  "MINIO_ROOT_PASSWORD",
  "MINIO_API_PORT",
  "MINIO_CONSOLE_PORT",
  "MINIO_DATA_DIR",
  "MINIO_BUCKET",
  "MINIO_INTERNAL_ENDPOINT",
  "MINIO_WAULE_ACCESS_KEY",
  "MINIO_WAULE_SECRET_KEY",
  "HOME_MINIO_WEB_API_PORT",
  "HOME_MINIO_WEB_PORT",
  "HOME_MINIO_WEB_TOKEN",
  "HOME_MINIO_PUBLIC_ENDPOINT",
  "HOME_MINIO_CONSOLE_PUBLIC_URL",
  "NEWWAULE_API_BASE_URL",
  "NEWWAULE_CACHE_UPLOAD_BASE_URL",
  "NEWWAULE_HOME_MINIO_TOKEN",
  "CACHE_PUSH_CONCURRENCY",
  "MEDIA_PULL_MANIFEST_PATH",
  "MEDIA_PULL_WORK_DIR",
  "MEDIA_PULL_CONCURRENCY",
  "BAIDUPAN_BACKUP_ENABLED",
  "BAIDUPAN_TOOL",
  "BAIDUPAN_REMOTE_DIR",
  "BAIDUPAN_WORK_DIR",
  "BAIDUPAN_CRON_SCHEDULE",
  "BYPY_BIN",
  "BAIDUPCS_BIN",
  "BAIDUPCS_CONFIG_DIR",
  "BAIDUPCS_MAX_PARALLEL",
  "BAIDUPCS_UPLOAD_NORAPID",
];

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

function serializeValue(value) {
  const text = String(value ?? "");
  return /[\s#"'$`\\]/.test(text) ? JSON.stringify(text) : text;
}

async function readEnv() {
  const source = await readFile(envPath, "utf8").catch(() => "");
  return { source, values: parseEnv(source) };
}

async function saveEnv(nextValues) {
  const { source, values } = await readEnv();
  const lines = source ? source.split(/\r?\n/) : [];
  const seen = new Set();
  const nextLines = lines.map((line) => {
    const trimmed = line.trim();
    if (!trimmed || trimmed.startsWith("#") || !trimmed.includes("=")) return line;
    const key = line.slice(0, line.indexOf("=")).trim();
    if (!editableKeys.includes(key) || !(key in nextValues)) return line;
    seen.add(key);
    return `${key}=${serializeValue(nextValues[key])}`;
  });

  for (const key of editableKeys) {
    if (key in nextValues && !seen.has(key)) {
      nextLines.push(`${key}=${serializeValue(nextValues[key])}`);
    }
  }

  await writeFile(envPath, `${nextLines.join("\n").replace(/\n+$/, "")}\n`, "utf8");
  return { ...values, ...nextValues };
}

async function readJsonBody(request) {
  const chunks = [];
  for await (const chunk of request) chunks.push(chunk);
  const text = Buffer.concat(chunks).toString("utf8");
  return text ? JSON.parse(text) : {};
}

function send(reply, statusCode, payload) {
  reply.writeHead(statusCode, {
    "content-type": "application/json; charset=utf-8",
    "access-control-allow-origin": "*",
    "access-control-allow-methods": "GET,POST,OPTIONS",
    "access-control-allow-headers": "content-type,x-home-minio-token",
  });
  reply.end(JSON.stringify(payload));
}

function assertAuth(request) {
  if (!token) return;
  if (request.headers["x-home-minio-token"] !== token) {
    const error = new Error("Unauthorized");
    error.statusCode = 401;
    throw error;
  }
}

function appendTail(current, chunk) {
  const next = `${current}${chunk}`;
  return next.length > outputTailLimit ? next.slice(-outputTailLimit) : next;
}

function readInteger(value, fallback, { min, max }) {
  const parsed = Number.parseInt(String(value ?? ""), 10);
  if (!Number.isFinite(parsed)) return fallback;
  return Math.min(max, Math.max(min, parsed));
}

function runCommand(command, args, options = {}) {
  return new Promise((resolveCommand) => {
    console.log(`[home-minio] run ${command} ${args.join(" ")}`);
    const child = spawn(command, args, {
      cwd: rootDir,
      shell: false,
      env: { ...process.env, ...options.env },
    });
    let stdout = "";
    let stderr = "";
    child.stdout.on("data", (chunk) => {
      const text = chunk.toString();
      stdout = appendTail(stdout, text);
      process.stdout.write(text);
    });
    child.stderr.on("data", (chunk) => {
      const text = chunk.toString();
      stderr = appendTail(stderr, text);
      process.stderr.write(text);
    });
    child.on("close", (code) => {
      console.log(`[home-minio] exit ${command} ${args.join(" ")} -> ${code}`);
      resolveCommand({ code, stdout, stderr });
    });
  });
}

function serializePullJob(job) {
  return {
    id: job.id,
    status: job.status,
    code: job.code,
    manifestPath: job.manifestPath,
    manifestCount: job.manifestCount,
    startedAt: job.startedAt,
    finishedAt: job.finishedAt,
    stdout: job.stdout,
    stderr: job.stderr,
    error: job.error,
  };
}

function serializePushJob(job) {
  return {
    id: job.id,
    status: job.status,
    code: job.code,
    objectKey: job.objectKey,
    newWauleApiUrl: job.newWauleApiUrl,
    cacheUploadBaseUrl: job.cacheUploadBaseUrl,
    startedAt: job.startedAt,
    finishedAt: job.finishedAt,
    stdout: job.stdout,
    stderr: job.stderr,
    error: job.error,
  };
}

function getRunningPullJob() {
  for (const job of pullJobs.values()) {
    if (job.status === "RUNNING") return job;
  }
  return null;
}

function startPullJob(meta = {}) {
  const runningJob = getRunningPullJob();
  if (runningJob) {
    return { job: runningJob, alreadyRunning: true };
  }

  const command = "node";
  const args = ["./scripts/pull-media-manifest-to-minio.mjs"];
  const job = {
    id: randomUUID(),
    status: "RUNNING",
    code: null,
    manifestPath: meta.manifestPath || "",
    manifestCount: meta.manifestCount || 0,
    startedAt: new Date().toISOString(),
    finishedAt: null,
    stdout: "",
    stderr: "",
    error: null,
  };
  pullJobs.set(job.id, job);
  latestPullJobId = job.id;

  console.log(`[home-minio] start pull job ${job.id} ${command} ${args.join(" ")}`);
  const child = spawn(command, args, {
    cwd: rootDir,
    shell: false,
    env: process.env,
  });

  child.stdout.on("data", (chunk) => {
    const text = chunk.toString();
    job.stdout = appendTail(job.stdout, text);
    process.stdout.write(text);
  });
  child.stderr.on("data", (chunk) => {
    const text = chunk.toString();
    job.stderr = appendTail(job.stderr, text);
    process.stderr.write(text);
  });
  child.on("error", (error) => {
    job.status = "FAILED";
    job.error = error instanceof Error ? error.message : String(error);
    job.finishedAt = new Date().toISOString();
    console.error(`[home-minio] pull job ${job.id} error: ${job.error}`);
  });
  child.on("close", (code) => {
    job.code = code ?? 1;
    job.status = job.code === 0 ? "SUCCEEDED" : "FAILED";
    job.finishedAt = new Date().toISOString();
    console.log(`[home-minio] pull job ${job.id} exit ${job.code}`);
  });

  return { job, alreadyRunning: false };
}

function normalizeObjectKey(value) {
  const key = String(value || "").trim().replace(/\\/g, "/").replace(/^\/+/, "");
  if (!key || key.split("/").some((segment) => !segment || segment === "." || segment === "..")) {
    return "";
  }
  return key;
}

function getActivePushJobForKey(objectKey) {
  for (const job of pushJobs.values()) {
    if (job.objectKey === objectKey && (job.status === "QUEUED" || job.status === "RUNNING")) {
      return job;
    }
  }
  return null;
}

function pushConcurrency() {
  return readInteger(process.env.CACHE_PUSH_CONCURRENCY, 4, { min: 1, max: 32 });
}

function drainPushQueue() {
  while (activePushJobs < pushConcurrency() && pushQueue.length > 0) {
    const jobId = pushQueue.shift();
    const job = pushJobs.get(jobId);
    if (!job || job.status !== "QUEUED") continue;

    activePushJobs += 1;
    job.status = "RUNNING";
    job.startedAt = new Date().toISOString();
    const command = "node";
    const args = ["./scripts/push-minio-object-to-newwaule.mjs", job.objectKey];
    console.log(`[home-minio] start push job ${job.id} ${command} ${args.join(" ")}`);
    const child = spawn(command, args, {
      cwd: rootDir,
      shell: false,
      env: { ...process.env, ...job.env },
    });

    child.stdout.on("data", (chunk) => {
      const text = chunk.toString();
      job.stdout = appendTail(job.stdout, text);
      process.stdout.write(text);
    });
    child.stderr.on("data", (chunk) => {
      const text = chunk.toString();
      job.stderr = appendTail(job.stderr, text);
      process.stderr.write(text);
    });
    child.on("error", (error) => {
      activePushJobs = Math.max(0, activePushJobs - 1);
      job.status = "FAILED";
      job.error = error instanceof Error ? error.message : String(error);
      job.finishedAt = new Date().toISOString();
      console.error(`[home-minio] push job ${job.id} error: ${job.error}`);
      drainPushQueue();
    });
    child.on("close", (code) => {
      activePushJobs = Math.max(0, activePushJobs - 1);
      job.code = code ?? 1;
      job.status = job.code === 0 ? "SUCCEEDED" : "FAILED";
      job.finishedAt = new Date().toISOString();
      console.log(`[home-minio] push job ${job.id} exit ${job.code}`);
      drainPushQueue();
    });
  }
}

function startPushJob(params) {
  const objectKey = normalizeObjectKey(params.objectKey);
  if (!objectKey) {
    const error = new Error("objectKey is required");
    error.statusCode = 400;
    throw error;
  }
  const existing = getActivePushJobForKey(objectKey);
  if (existing) {
    return { job: existing, alreadyRunning: true };
  }

  const job = {
    id: randomUUID(),
    status: "QUEUED",
    code: null,
    objectKey,
    newWauleApiUrl: params.newWauleApiUrl,
    cacheUploadBaseUrl: params.cacheUploadBaseUrl,
    startedAt: null,
    finishedAt: null,
    stdout: "",
    stderr: "",
    error: null,
    env: params.env,
  };
  pushJobs.set(job.id, job);
  pushQueue.push(job.id);
  latestPushJobId = job.id;
  drainPushQueue();
  return { job, alreadyRunning: false };
}

async function minioReady(values) {
  const apiPort = values.MINIO_API_PORT || "19000";
  try {
    const response = await fetch(`http://minio:9000/minio/health/ready`, { signal: AbortSignal.timeout(2500) });
    return { ok: response.ok, status: response.status, endpoint: `http://127.0.0.1:${apiPort}` };
  } catch (error) {
    return { ok: false, error: error instanceof Error ? error.message : String(error), endpoint: `http://127.0.0.1:${apiPort}` };
  }
}

async function handle(request, reply) {
  if (request.method === "OPTIONS") {
    return send(reply, 200, { ok: true });
  }

  assertAuth(request);
  const url = new URL(request.url || "/", `http://${request.headers.host || "localhost"}`);
  console.log(`[home-minio] ${request.method} ${url.pathname}`);

  if (request.method === "GET" && url.pathname === "/api/config") {
    const { values } = await readEnv();
    return send(reply, 200, { values, editableKeys });
  }

  if (request.method === "POST" && url.pathname === "/api/config") {
    const body = await readJsonBody(request);
    const sanitized = {};
    for (const key of editableKeys) {
      if (Object.prototype.hasOwnProperty.call(body.values || {}, key)) {
        sanitized[key] = String(body.values[key] ?? "");
      }
    }
    const values = await saveEnv(sanitized);
    return send(reply, 200, { values, message: "saved" });
  }

  if (request.method === "GET" && url.pathname === "/api/status") {
    const { values } = await readEnv();
    return send(reply, 200, {
      minio: await minioReady(values),
      ports: {
        minioApi: values.MINIO_API_PORT || "19000",
        minioConsole: values.MINIO_CONSOLE_PORT || "19001",
        webApi: values.HOME_MINIO_WEB_API_PORT || "19090",
        web: values.HOME_MINIO_WEB_PORT || "19091",
      },
      publicUrls: {
        minioEndpoint: values.HOME_MINIO_PUBLIC_ENDPOINT || "",
        minioConsole: values.HOME_MINIO_CONSOLE_PUBLIC_URL || "",
      },
      newWauleConfig: {
        enabled: "true",
        endpoint: values.HOME_MINIO_PUBLIC_ENDPOINT || "",
        region: "us-east-1",
        bucket: values.MINIO_BUCKET || "waule-media",
        accessKeyId: values.MINIO_WAULE_ACCESS_KEY || "",
        secretAccessKey: values.MINIO_WAULE_SECRET_KEY || "",
        forcePathStyle: "true",
        cacheDir: "storage/home-minio-cache",
        publicBaseUrl: values.NEWWAULE_API_BASE_URL || "https://api.example.com",
      },
      cachePush: {
        newWauleApiUrl: values.NEWWAULE_API_BASE_URL || "",
        concurrency: values.CACHE_PUSH_CONCURRENCY || "4",
        latestJob: latestPushJobId && pushJobs.has(latestPushJobId) ? serializePushJob(pushJobs.get(latestPushJobId)) : null,
      },
      baidupan: {
        enabled: values.BAIDUPAN_BACKUP_ENABLED === "true",
        tool: values.BAIDUPAN_TOOL || "baidupcs",
        remoteDir: values.BAIDUPAN_REMOTE_DIR || "/NewWaule/home-minio",
        cronSchedule: values.BAIDUPAN_CRON_SCHEDULE || "35 3 * * *",
      },
      pullJob: latestPullJobId && pullJobs.has(latestPullJobId) ? serializePullJob(pullJobs.get(latestPullJobId)) : null,
    });
  }

  if (request.method === "POST" && url.pathname === "/api/actions/backup-dry-run") {
    const result = await runCommand("bash", ["./scripts/backup-to-baidupan.sh", "--dry-run"]);
    return send(reply, result.code === 0 ? 200 : 500, result);
  }

  if (request.method === "POST" && url.pathname === "/api/actions/backup") {
    const result = await runCommand("bash", ["./scripts/backup-to-baidupan.sh"]);
    return send(reply, result.code === 0 ? 200 : 500, result);
  }

  if (request.method === "POST" && url.pathname === "/api/actions/restore-dry-run") {
    const result = await runCommand("bash", ["./scripts/restore-from-baidupan.sh", "--dry-run"]);
    return send(reply, result.code === 0 ? 200 : 500, result);
  }

  if (request.method === "POST" && url.pathname === "/api/actions/pull-manifest-dry-run") {
    const result = await runCommand("node", ["./scripts/pull-media-manifest-to-minio.mjs", "--dry-run"]);
    return send(reply, result.code === 0 ? 200 : 500, result);
  }

  if (request.method === "POST" && url.pathname === "/api/actions/pull-manifest") {
    const { job, alreadyRunning } = startPullJob();
    return send(reply, alreadyRunning ? 409 : 202, {
      message: alreadyRunning ? "pull job already running" : "pull job started",
      job: serializePullJob(job),
      jobId: job.id,
    });
  }

  if (request.method === "POST" && url.pathname === "/api/actions/pull-manifest-upload") {
    const runningJob = getRunningPullJob();
    if (runningJob) {
      return send(reply, 409, {
        message: "pull job already running",
        job: serializePullJob(runningJob),
        jobId: runningJob.id,
      });
    }

    const body = await readJsonBody(request);
    const manifest = typeof body.manifest === "string" ? body.manifest.trim() : "";
    if (!manifest) {
      return send(reply, 400, { message: "manifest is required" });
    }
    const manifestCount = manifest.split(/\r?\n/).filter((line) => line.trim()).length;

    const { values } = await readEnv();
    const manifestPath = resolve(rootDir, values.MEDIA_PULL_MANIFEST_PATH || "./backup/newwaule-media-manifest.jsonl");
    await mkdir(dirname(manifestPath), { recursive: true });
    await writeFile(manifestPath, `${manifest.replace(/\n+$/, "")}\n`, "utf8");
    console.log(`[home-minio] saved uploaded manifest ${manifestPath} lines=${manifestCount}`);

    const { job } = startPullJob({ manifestPath, manifestCount });
    return send(reply, 202, {
      message: "pull job started",
      job: serializePullJob(job),
      jobId: job.id,
      manifestPath,
    });
  }

  if (request.method === "GET" && url.pathname === "/api/actions/pull-manifest-status") {
    const jobId = url.searchParams.get("jobId") || latestPullJobId;
    const job = jobId ? pullJobs.get(jobId) : null;
    if (!job) {
      return send(reply, 404, { message: "pull job not found" });
    }
    return send(reply, 200, { job: serializePullJob(job), jobId: job.id });
  }

  if (request.method === "POST" && url.pathname === "/api/actions/push-cache-object") {
    const body = await readJsonBody(request);
    const { values } = await readEnv();
    const objectKey = normalizeObjectKey(body.objectKey);
    if (!objectKey) {
      return send(reply, 400, { message: "objectKey is required" });
    }
    const newWauleApiUrl = String(body.newWauleApiUrl || values.NEWWAULE_API_BASE_URL || "").replace(/\/+$/, "");
    const cacheUploadBaseUrl = String(body.cacheUploadBaseUrl || values.NEWWAULE_CACHE_UPLOAD_BASE_URL || "").replace(/\/+$/, "");
    const newWauleToken = String(body.token || values.NEWWAULE_HOME_MINIO_TOKEN || values.HOME_MINIO_WEB_TOKEN || "");
    if (!newWauleApiUrl) {
      return send(reply, 400, { message: "newWauleApiUrl is required" });
    }
    if (!newWauleToken) {
      return send(reply, 400, { message: "token is required" });
    }

    const { job, alreadyRunning } = startPushJob({
      objectKey,
      newWauleApiUrl,
      cacheUploadBaseUrl,
      env: {
        ...values,
        NEWWAULE_API_BASE_URL: newWauleApiUrl,
        ...(cacheUploadBaseUrl ? { NEWWAULE_CACHE_UPLOAD_BASE_URL: cacheUploadBaseUrl } : {}),
        NEWWAULE_HOME_MINIO_TOKEN: newWauleToken,
      },
    });
    return send(reply, 202, {
      message: alreadyRunning ? "push job already running" : "push job queued",
      job: serializePushJob(job),
      jobId: job.id,
    });
  }

  if (request.method === "GET" && url.pathname === "/api/actions/push-cache-status") {
    const objectKey = normalizeObjectKey(url.searchParams.get("objectKey"));
    const jobId = url.searchParams.get("jobId") || latestPushJobId;
    const job = objectKey
      ? [...pushJobs.values()].reverse().find((candidate) => candidate.objectKey === objectKey)
      : jobId
        ? pushJobs.get(jobId)
        : null;
    if (!job) {
      return send(reply, 404, { message: "push job not found" });
    }
    return send(reply, 200, { job: serializePushJob(job), jobId: job.id });
  }

  if (request.method === "POST" && url.pathname === "/api/actions/install-cron") {
    const { values } = await readEnv();
    const schedule = values.BAIDUPAN_CRON_SCHEDULE || "35 3 * * *";
    return send(reply, 200, {
      code: 0,
      stdout: `自动备份计划已保存为：${schedule}\nDocker 调度服务 home-minio-backup-scheduler 会按该时间运行。\n如果刚修改了时间，请执行 docker compose up -d --force-recreate backup-scheduler 让调度容器读取新配置。`,
      stderr: "",
    });
  }

  if (request.method === "GET" && url.pathname === "/api/cron") {
    const { values } = await readEnv();
    return send(reply, 200, {
      code: 0,
      stdout: `BAIDUPAN_BACKUP_ENABLED=${values.BAIDUPAN_BACKUP_ENABLED || "false"}\nBAIDUPAN_CRON_SCHEDULE=${values.BAIDUPAN_CRON_SCHEDULE || "35 3 * * *"}\nservice=home-minio-backup-scheduler`,
      stderr: "",
    });
  }

  return send(reply, 404, { message: "Not found" });
}

createServer((request, reply) => {
  handle(request, reply).catch((error) => {
    send(reply, error.statusCode || 500, { message: error instanceof Error ? error.message : String(error) });
  });
}).listen(port, "0.0.0.0", () => {
  console.log(`home-minio web api listening on ${port}`);
});
