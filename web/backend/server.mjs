import { createServer } from "node:http";
import { spawn } from "node:child_process";
import { readFile, writeFile } from "node:fs/promises";
import { resolve } from "node:path";

const rootDir = resolve(new URL("../..", import.meta.url).pathname);
const envPath = resolve(rootDir, ".env");
const port = Number(process.env.HOME_MINIO_WEB_API_PORT || 19090);
const token = process.env.HOME_MINIO_WEB_TOKEN || "";

const editableKeys = [
  "MINIO_ROOT_USER",
  "MINIO_ROOT_PASSWORD",
  "MINIO_API_PORT",
  "MINIO_CONSOLE_PORT",
  "MINIO_DATA_DIR",
  "MINIO_BUCKET",
  "MINIO_WAULE_ACCESS_KEY",
  "MINIO_WAULE_SECRET_KEY",
  "HOME_MINIO_WEB_API_PORT",
  "HOME_MINIO_WEB_PORT",
  "HOME_MINIO_WEB_TOKEN",
  "BAIDUPAN_BACKUP_ENABLED",
  "BAIDUPAN_TOOL",
  "BAIDUPAN_REMOTE_DIR",
  "BAIDUPAN_WORK_DIR",
  "BYPY_BIN",
  "BAIDUPCS_BIN",
  "BAIDUPCS_MAX_PARALLEL",
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

function runCommand(command, args, options = {}) {
  return new Promise((resolveCommand) => {
    const child = spawn(command, args, {
      cwd: rootDir,
      shell: false,
      env: { ...process.env, ...options.env },
    });
    let stdout = "";
    let stderr = "";
    child.stdout.on("data", (chunk) => {
      stdout += chunk.toString();
    });
    child.stderr.on("data", (chunk) => {
      stderr += chunk.toString();
    });
    child.on("close", (code) => {
      resolveCommand({ code, stdout: stdout.slice(-12000), stderr: stderr.slice(-12000) });
    });
  });
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
      baidupan: {
        enabled: values.BAIDUPAN_BACKUP_ENABLED === "true",
        tool: values.BAIDUPAN_TOOL || "baidupcs",
        remoteDir: values.BAIDUPAN_REMOTE_DIR || "/NewWaule/home-minio",
      },
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

  return send(reply, 404, { message: "Not found" });
}

createServer((request, reply) => {
  handle(request, reply).catch((error) => {
    send(reply, error.statusCode || 500, { message: error instanceof Error ? error.message : String(error) });
  });
}).listen(port, "0.0.0.0", () => {
  console.log(`home-minio web api listening on ${port}`);
});
