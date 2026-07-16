import { createServer } from "node:http";
import { spawn } from "node:child_process";
import { randomUUID } from "node:crypto";
import { createWriteStream } from "node:fs";
import { mkdir, readFile, writeFile } from "node:fs/promises";
import { dirname, resolve } from "node:path";
import { once } from "node:events";
import { finished } from "node:stream/promises";
import { StringDecoder } from "node:string_decoder";
import {
  collectHomeMinioAuthTokens,
  preferredHomeMinioToken,
  synchronizeHomeMinioTokenValues,
} from "./home-minio-token.mjs";
import { LifecycleStore } from "./lifecycle-store.mjs";
import { LifecycleTransferService } from "./lifecycle-service.mjs";
import {
  describeBackupSchedule,
  normalizeBackupSchedule,
  validateBackupScheduleValues,
} from "./backup-schedule.mjs";
import { CONFIG_FIELDS, SECRET_CONFIG_KEYS } from "../frontend/config-schema.js";

const rootDir = resolve(new URL("../..", import.meta.url).pathname);
const envPath = resolve(rootDir, ".env");
const port = Number(process.env.HOME_MINIO_WEB_API_PORT || 19090);
let acceptedAuthTokens = new Set(collectHomeMinioAuthTokens({
  ...process.env,
  ...(await readEnv()).values,
}));
const outputTailLimit = 12000;
const pullProgressPrefix = "HOME_MINIO_PROGRESS ";
const pullJobs = new Map();
let latestPullJobId = "";
const pushJobs = new Map();
const pushQueue = [];
let activePushJobs = 0;
let latestPushJobId = "";
let lifecycleStartupError = null;
let lifecycleStore;
try {
  lifecycleStore = new LifecycleStore();
} catch (error) {
  lifecycleStartupError = error instanceof Error ? error.message : String(error);
  console.error(`[home-minio] lifecycle state initialization failed: ${lifecycleStartupError}`);
  lifecycleStore = new LifecycleStore({ encryptionKey: null });
}
let lifecycleService = null;
const lifecycleEnvironmentProvider = async () => ({
  ...process.env,
  ...(await readEnv()).values,
});
try {
  const lifecycleHealth = lifecycleStore.health();
  if (!lifecycleHealth.ready) {
    lifecycleStartupError = lifecycleStartupError || lifecycleHealth.encryptionError;
  } else {
    lifecycleService = new LifecycleTransferService({
      store: lifecycleStore,
      env: await lifecycleEnvironmentProvider(),
      environmentProvider: lifecycleEnvironmentProvider,
    });
    lifecycleService.start();
  }
} catch (error) {
  lifecycleStartupError = error instanceof Error ? error.message : String(error);
  console.error(`[home-minio] lifecycle transfer service unavailable: ${lifecycleStartupError}`);
}

const editableKeys = [...CONFIG_FIELDS.keys()];
const secretEditableKeys = new Set(SECRET_CONFIG_KEYS);

function sanitizeEditableValues(values) {
  return Object.fromEntries(
    Object.entries(values).map(([key, value]) => [key, secretEditableKeys.has(key) ? "" : value]),
  );
}

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

async function writeManifestRequestBody(request, manifestPath) {
  const output = createWriteStream(manifestPath);
  const decoder = new StringDecoder("utf8");
  let manifestCount = 0;
  let pending = "";
  let lastEndedWithNewline = true;

  try {
    for await (const chunk of request) {
      const buffer = Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk);
      const text = decoder.write(buffer);
      if (!buffer.byteLength) continue;
      lastEndedWithNewline = /\r?\n$/.test(text);
      pending += text;
      const lines = pending.split(/\r?\n/);
      pending = lines.pop() ?? "";
      for (const line of lines) {
        if (line.trim()) manifestCount += 1;
      }
      if (!output.write(buffer)) {
        await once(output, "drain");
      }
    }

    const tail = decoder.end();
    if (tail) {
      lastEndedWithNewline = /\r?\n$/.test(tail);
      pending += tail;
    }
    if (pending.trim()) {
      manifestCount += 1;
    }
    if (!lastEndedWithNewline) {
      output.write("\n", "utf8");
    }
    output.end();
    await finished(output);
    return manifestCount;
  } catch (error) {
    output.destroy();
    throw error;
  }
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

function assertLifecycleReady() {
  const health = lifecycleStore.health();
  if (!health.ready || !lifecycleService) {
    const error = new Error(lifecycleStartupError || health.encryptionError || "Lifecycle transfer service is unavailable.");
    error.statusCode = 503;
    throw error;
  }
}

function assertAuth(request) {
  if (!acceptedAuthTokens.size) return;
  if (!acceptedAuthTokens.has(String(request.headers["x-home-minio-token"] || "").trim())) {
    const error = new Error("Unauthorized");
    error.statusCode = 401;
    throw error;
  }
}

function appendTail(current, chunk) {
  const next = `${current}${chunk}`;
  return next.length > outputTailLimit ? next.slice(-outputTailLimit) : next;
}

function applyPullProgress(job, chunk) {
  job.progressBuffer += chunk;
  const lines = job.progressBuffer.split(/\r?\n/);
  job.progressBuffer = lines.pop() ?? "";
  for (const line of lines) {
    if (!line.startsWith(pullProgressPrefix)) continue;
    try {
      const progress = JSON.parse(line.slice(pullProgressPrefix.length));
      for (const key of ["processed", "downloaded", "skipped", "failed", "rejected", "records", "uniqueRecords"]) {
        const value = Number(progress[key]);
        if (Number.isFinite(value) && value >= 0) {
          job[key] = Math.floor(value);
        }
      }
    } catch {
      // A malformed progress line stays in stdout for diagnostics.
    }
  }
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
    processed: job.processed,
    downloaded: job.downloaded,
    skipped: job.skipped,
    failed: job.failed,
    rejected: job.rejected,
    records: job.records,
    uniqueRecords: job.uniqueRecords,
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
    processed: 0,
    downloaded: 0,
    skipped: 0,
    failed: 0,
    rejected: 0,
    records: 0,
    uniqueRecords: 0,
    progressBuffer: "",
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
    applyPullProgress(job, text);
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
    applyPullProgress(job, "\n");
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

    const claimed = lifecycleStore.claimCachePushJob(job.id);
    if (!claimed) continue;

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
      lifecycleStore.updateCachePushJob(job.id, {
        status: "FAILED",
        error: job.error,
        finishedAt: job.finishedAt,
      });
      console.error(`[home-minio] push job ${job.id} error: ${job.error}`);
      drainPushQueue();
    });
    child.on("close", (code) => {
      activePushJobs = Math.max(0, activePushJobs - 1);
      job.code = code ?? 1;
      job.status = job.code === 0 ? "SUCCEEDED" : "FAILED";
      job.finishedAt = new Date().toISOString();
      lifecycleStore.updateCachePushJob(job.id, {
        status: job.status,
        error: job.status === "FAILED" ? (job.stderr || `exit ${job.code}`).slice(-4000) : null,
        finishedAt: job.finishedAt,
      });
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
  if (existing) return { job: existing, alreadyRunning: true };

  const persisted = lifecycleStore.createCachePushJob({
    objectKey,
    newWauleApiUrl: params.newWauleApiUrl,
    cacheUploadBaseUrl: params.cacheUploadBaseUrl,
  });
  const existingRuntime = pushJobs.get(persisted.job.id);
  if (existingRuntime) return { job: existingRuntime, alreadyRunning: true };

  const job = {
    id: persisted.job.id,
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
  return { job, alreadyRunning: persisted.alreadyRunning };
}

async function resumeCachePushJobs() {
  const { values } = await readEnv();
  for (const persisted of lifecycleStore.listRunnableCachePushJobs(1000)) {
    if (pushJobs.has(persisted.id)) continue;
    const newWauleApiUrl = persisted.newWauleApiUrl || values.NEWWAULE_API_BASE_URL || "";
    const cacheUploadBaseUrl = persisted.cacheUploadBaseUrl || values.NEWWAULE_CACHE_UPLOAD_BASE_URL || "";
    const newWauleToken = preferredHomeMinioToken(values);
    pushJobs.set(persisted.id, {
      id: persisted.id,
      status: "QUEUED",
      code: null,
      objectKey: persisted.objectKey,
      newWauleApiUrl,
      cacheUploadBaseUrl,
      startedAt: null,
      finishedAt: null,
      stdout: "",
      stderr: "",
      error: null,
      env: {
        ...values,
        NEWWAULE_API_BASE_URL: newWauleApiUrl,
        ...(cacheUploadBaseUrl ? { NEWWAULE_CACHE_UPLOAD_BASE_URL: cacheUploadBaseUrl } : {}),
        NEWWAULE_HOME_MINIO_TOKEN: newWauleToken,
      },
    });
    pushQueue.push(persisted.id);
    latestPushJobId = persisted.id;
  }
  drainPushQueue();
}

async function minioReady(values) {
  const apiPort = values.MINIO_API_PORT || "19000";
  const internalEndpoint = String(
    values.MINIO_INTERNAL_ENDPOINT || process.env.MINIO_INTERNAL_ENDPOINT || "http://minio:9000",
  ).replace(/\/+$/, "");
  const displayEndpoint = values.HOME_MINIO_PUBLIC_ENDPOINT || `http://127.0.0.1:${apiPort}`;
  try {
    const response = await fetch(`${internalEndpoint}/minio/health/ready`, { signal: AbortSignal.timeout(2500) });
    return { ok: response.ok, status: response.status, endpoint: displayEndpoint };
  } catch (error) {
    return { ok: false, error: error instanceof Error ? error.message : String(error), endpoint: displayEndpoint };
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
    const backupSchedule = normalizeBackupSchedule(values);
    return send(reply, 200, {
      values: sanitizeEditableValues({
        ...values,
        BAIDUPAN_BACKUP_FREQUENCY: backupSchedule.frequency,
        BAIDUPAN_BACKUP_TIME: backupSchedule.time,
        BAIDUPAN_TIME_ZONE: backupSchedule.timeZone,
      }),
      configuredKeys: [...secretEditableKeys].filter((key) => Boolean(values[key])),
      editableKeys,
    });
  }

  if (request.method === "POST" && url.pathname === "/api/config") {
    const body = await readJsonBody(request);
    const current = await readEnv();
    const sanitized = {};
    for (const key of editableKeys) {
      if (Object.prototype.hasOwnProperty.call(body.values || {}, key)) {
        const nextValue = String(body.values[key] ?? "");
        sanitized[key] = secretEditableKeys.has(key) && !nextValue.trim()
          ? current.values[key] || ""
          : nextValue;
      }
    }
    try {
      validateBackupScheduleValues(sanitized);
    } catch (error) {
      error.statusCode = 400;
      throw error;
    }
    const values = await saveEnv(synchronizeHomeMinioTokenValues(sanitized));
    acceptedAuthTokens = new Set(collectHomeMinioAuthTokens({ ...process.env, ...values }));
    lifecycleService?.reconfigure({ ...process.env, ...values });
    const backupSchedule = normalizeBackupSchedule(values);
    return send(reply, 200, {
      values: sanitizeEditableValues({
        ...values,
        BAIDUPAN_BACKUP_FREQUENCY: backupSchedule.frequency,
        BAIDUPAN_BACKUP_TIME: backupSchedule.time,
        BAIDUPAN_TIME_ZONE: backupSchedule.timeZone,
      }),
      configuredKeys: [...secretEditableKeys].filter((key) => Boolean(values[key])),
      message: "saved",
    });
  }

  if (request.method === "GET" && url.pathname === "/api/status") {
    const { values } = await readEnv();
    const backupSchedule = normalizeBackupSchedule(values);
    const lifecycleHealth = lifecycleStore.health();
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
        secretAccessKey: "",
        secretAccessKeyConfigured: Boolean(values.MINIO_WAULE_SECRET_KEY),
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
        frequency: backupSchedule.frequency,
        time: backupSchedule.time,
        timeZone: backupSchedule.timeZone,
        scheduleLabel: describeBackupSchedule(backupSchedule),
      },
      pullJob: latestPullJobId && pullJobs.has(latestPullJobId) ? serializePullJob(pullJobs.get(latestPullJobId)) : null,
      lifecycle: {
        ...lifecycleHealth,
        ready: lifecycleHealth.ready && Boolean(lifecycleService),
        ...(lifecycleService ? { settings: lifecycleService.settings() } : {}),
        ...(lifecycleService ? { telemetry: lifecycleService.telemetry() } : {}),
        recentJobs: lifecycleStore.listRecentJobs(10),
        startupError: lifecycleStartupError,
      },
    });
  }

  if (request.method === "POST" && url.pathname === "/api/lifecycle/config-versions") {
    assertLifecycleReady();
    const body = await readJsonBody(request);
    const configVersion = lifecycleStore.upsertConfigVersion(body.oss || body);
    return send(reply, 200, { configVersion });
  }

  if (request.method === "POST" && url.pathname === "/api/v2/lifecycle/runs") {
    assertLifecycleReady();
    const body = await readJsonBody(request);
    const run = await lifecycleService.submitStreamingJob(body);
    return send(reply, 202, { run, runId: run.id });
  }

  const lifecycleV2ActionMatch = /^\/api\/v2\/lifecycle\/runs\/([^/]+)\/(cancel|resume|replay-callbacks)$/.exec(url.pathname);
  if (request.method === "POST" && lifecycleV2ActionMatch) {
    assertLifecycleReady();
    const runId = decodeURIComponent(lifecycleV2ActionMatch[1]);
    const action = lifecycleV2ActionMatch[2];
    const changed = action === "cancel"
      ? lifecycleService.cancelStreamingJob(runId)
      : action === "resume"
        ? lifecycleService.resumeStreamingJob(runId)
        : lifecycleService.replayStreamingJobCallbacks(runId);
    if (!changed) return send(reply, 404, { message: "lifecycle run not found" });
    const run = lifecycleService.getJobSummary(runId);
    return send(reply, 200, { run, runId });
  }

  if (request.method === "GET" && url.pathname.startsWith("/api/v2/lifecycle/runs/")) {
    assertLifecycleReady();
    const runId = decodeURIComponent(url.pathname.slice("/api/v2/lifecycle/runs/".length));
    const run = lifecycleService.getJobSummaryWithDiagnostics(runId);
    if (!run) return send(reply, 404, { message: "lifecycle run not found" });
    return send(reply, 200, { run, runId });
  }

  if (request.method === "POST" && url.pathname === "/api/lifecycle/jobs") {
    assertLifecycleReady();
    const body = await readJsonBody(request);
    const job = await lifecycleService.submitJob(body);
    return send(reply, job.status === "QUEUED" ? 202 : 200, { job, jobId: job.id });
  }

  const lifecycleActionMatch = /^\/api\/lifecycle\/jobs\/([^/]+)\/(cancel|resume)$/.exec(url.pathname);
  if (request.method === "POST" && lifecycleActionMatch) {
    assertLifecycleReady();
    const jobId = decodeURIComponent(lifecycleActionMatch[1]);
    const action = lifecycleActionMatch[2];
    const job = action === "cancel"
      ? lifecycleService.cancelJob(jobId)
      : lifecycleService.resumeJob(jobId);
    if (!job) return send(reply, 404, { message: "lifecycle job not found" });
    return send(reply, 200, { job, jobId: job.id });
  }

  if (request.method === "GET" && url.pathname.startsWith("/api/lifecycle/jobs/")) {
    assertLifecycleReady();
    const jobId = decodeURIComponent(url.pathname.slice("/api/lifecycle/jobs/".length));
    const job = lifecycleService.getJob(jobId);
    if (!job) return send(reply, 404, { message: "lifecycle job not found" });
    return send(reply, 200, { job, jobId: job.id });
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

    const { values } = await readEnv();
    const manifestPath = resolve(rootDir, values.MEDIA_PULL_MANIFEST_PATH || "./backup/newwaule-media-manifest.jsonl");
    await mkdir(dirname(manifestPath), { recursive: true });
    const contentType = String(request.headers["content-type"] || "").toLowerCase();
    let manifestCount = 0;

    if (contentType.includes("application/json")) {
      const body = await readJsonBody(request);
      const manifest = typeof body.manifest === "string" ? body.manifest.trim() : "";
      if (!manifest) {
        return send(reply, 400, { message: "manifest is required" });
      }
      manifestCount = manifest.split(/\r?\n/).filter((line) => line.trim()).length;
      await writeFile(manifestPath, `${manifest.replace(/\n+$/, "")}\n`, "utf8");
    } else {
      manifestCount = await writeManifestRequestBody(request, manifestPath);
      if (manifestCount <= 0) {
        return send(reply, 400, { message: "manifest is required" });
      }
    }
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
    const newWauleToken = String(body.token || preferredHomeMinioToken(values));
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
    const persistedJob = !job
      ? objectKey
        ? lifecycleStore.findCachePushJobByObjectKey(objectKey)
        : jobId
          ? lifecycleStore.getCachePushJob(jobId)
          : null
      : null;
    if (!job && !persistedJob) {
      return send(reply, 404, { message: "push job not found" });
    }
    return send(reply, 200, {
      job: job ? serializePushJob(job) : persistedJob,
      jobId: job?.id || persistedJob.id,
    });
  }

  if (request.method === "POST" && url.pathname === "/api/actions/install-cron") {
    const { values } = await readEnv();
    const schedule = describeBackupSchedule(normalizeBackupSchedule(values));
    return send(reply, 200, {
      code: 0,
      stdout: `当前自动备份计划：${schedule}\nDocker 调度服务 home-minio-backup-scheduler 会按该计划运行。\n如果刚修改了计划，请执行 docker compose up -d --force-recreate backup-scheduler 让调度容器读取新配置。`,
      stderr: "",
    });
  }

  if (request.method === "GET" && url.pathname === "/api/cron") {
    const { values } = await readEnv();
    const schedule = normalizeBackupSchedule(values);
    return send(reply, 200, {
      code: 0,
      stdout: `启用：${values.BAIDUPAN_BACKUP_ENABLED === "true" ? "是" : "否"}\n频率：${describeBackupSchedule(schedule)}\n服务：home-minio-backup-scheduler`,
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

void resumeCachePushJobs().catch((error) => {
  console.error(`[home-minio] cache push recovery failed: ${error instanceof Error ? error.message : String(error)}`);
});

async function shutdown(signal) {
  console.log(`[home-minio] received ${signal}, waiting for lifecycle transfers to stop`);
  await lifecycleService?.stop().catch((error) => {
    console.error(`[home-minio] lifecycle shutdown failed: ${error instanceof Error ? error.message : String(error)}`);
  });
  lifecycleStore.close();
  process.exit(0);
}

process.once("SIGTERM", () => void shutdown("SIGTERM"));
process.once("SIGINT", () => void shutdown("SIGINT"));
