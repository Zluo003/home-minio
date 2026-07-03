const API_BASE = `${location.protocol}//${location.hostname}:19090`;
let token = localStorage.getItem("homeMinioToken") || "";

const form = document.getElementById("configForm");
const output = document.getElementById("output");

function headers() {
  return {
    "content-type": "application/json",
    "x-home-minio-token": token,
  };
}

async function api(path, options = {}) {
  const response = await fetch(`${API_BASE}${path}`, {
    ...options,
    headers: {
      ...headers(),
      ...(options.headers || {}),
    },
  });
  if (response.status === 401) {
    token = prompt("请输入 HOME_MINIO_WEB_TOKEN") || "";
    localStorage.setItem("homeMinioToken", token);
    return api(path, options);
  }
  const data = await response.json();
  if (!response.ok) {
    const commandOutput = [data.stdout, data.stderr].filter(Boolean).join("\n");
    throw new Error(commandOutput || data.message || response.statusText);
  }
  return data;
}

const labels = {
  MINIO_ROOT_USER: "MinIO 管理员",
  MINIO_ROOT_PASSWORD: "MinIO 管理员密码",
  MINIO_API_PORT: "MinIO API 端口",
  MINIO_CONSOLE_PORT: "MinIO Console 端口",
  MINIO_DATA_DIR: "MinIO 数据目录",
  MINIO_BUCKET: "媒体 Bucket",
  MINIO_WAULE_ACCESS_KEY: "NewWaule Access Key",
  MINIO_WAULE_SECRET_KEY: "NewWaule Secret Key",
  HOME_MINIO_WEB_API_PORT: "管理后端端口",
  HOME_MINIO_WEB_PORT: "管理前端端口",
  HOME_MINIO_WEB_TOKEN: "管理控制台令牌",
  HOME_MINIO_PUBLIC_ENDPOINT: "MinIO 访问地址",
  HOME_MINIO_CONSOLE_PUBLIC_URL: "MinIO 控制台地址",
  MEDIA_PULL_MANIFEST_PATH: "媒体拉取清单",
  MEDIA_PULL_WORK_DIR: "媒体拉取工作目录",
  BAIDUPAN_BACKUP_ENABLED: "启用百度网盘备份",
  BAIDUPAN_TOOL: "百度网盘工具",
  BAIDUPAN_REMOTE_DIR: "百度网盘目录",
  BAIDUPAN_WORK_DIR: "本地备份工作目录",
  BAIDUPAN_CRON_SCHEDULE: "自动备份时间",
  BYPY_BIN: "bypy 命令",
  BAIDUPCS_BIN: "BaiduPCS-Go 命令",
  BAIDUPCS_MAX_PARALLEL: "BaiduPCS-Go 并发",
};

const secretKeys = new Set(["MINIO_ROOT_PASSWORD", "MINIO_WAULE_SECRET_KEY", "HOME_MINIO_WEB_TOKEN"]);

function renderForm(values, keys) {
  form.innerHTML = keys.map((key) => {
    const value = values[key] ?? "";
    if (key === "BAIDUPAN_BACKUP_ENABLED") {
      return `<label><span>${labels[key] || key}</span><select name="${key}"><option value="true" ${value === "true" ? "selected" : ""}>true</option><option value="false" ${value !== "true" ? "selected" : ""}>false</option></select></label>`;
    }
    if (key === "BAIDUPAN_TOOL") {
      return `<label><span>${labels[key] || key}</span><select name="${key}"><option value="baidupcs" ${value !== "bypy" ? "selected" : ""}>baidupcs</option><option value="bypy" ${value === "bypy" ? "selected" : ""}>bypy</option></select></label>`;
    }
    const type = secretKeys.has(key) ? "password" : "text";
    return `<label><span>${labels[key] || key}</span><input type="${type}" name="${key}" value="${String(value).replaceAll('"', "&quot;")}" /></label>`;
  }).join("");
}

async function loadConfig() {
  const data = await api("/api/config");
  renderForm(data.values, data.editableKeys);
}

async function loadStatus() {
  const status = await api("/api/status");
  document.getElementById("statusText").textContent = status.minio.ok ? "MinIO online" : "MinIO offline";
  document.getElementById("portText").textContent = `API ${status.ports.minioApi} · Console ${status.ports.minioConsole} · Web ${status.ports.web}`;
  document.getElementById("minioState").textContent = status.publicUrls.minioEndpoint || (status.minio.ok ? status.minio.endpoint : status.minio.error || "offline");
  document.getElementById("baidupanState").textContent = status.baidupan.enabled ? status.baidupan.remoteDir : "未启用";
  document.getElementById("toolState").textContent = `${status.baidupan.tool} · ${status.baidupan.cronSchedule}`;
  const configText = Object.entries(status.newWauleConfig)
    .map(([key, value]) => `${key}: ${value}`)
    .join("\n");
  document.getElementById("newWauleConfig").textContent = [
    "在 NewWaule 管理后台 -> 系统配置 -> Home MinIO 冷存储 填写：",
    "",
    configText,
    "",
    "publicBaseUrl 请改成 NewWaule API 公网根地址，例如 https://api.example.com",
  ].join("\n");
}

async function saveConfig() {
  const values = {};
  new FormData(form).forEach((value, key) => {
    values[key] = value;
  });
  await api("/api/config", { method: "POST", body: JSON.stringify({ values }) });
  output.textContent = "配置已保存。端口、密钥或 MinIO 账号变更后，请重启 docker compose。";
  await loadStatus();
}

async function runAction(path) {
  output.textContent = "执行中...";
  try {
    const result = await api(path, { method: "POST", body: "{}" });
    output.textContent = [result.stdout, result.stderr].filter(Boolean).join("\n") || `exit ${result.code}`;
  } catch (error) {
    output.textContent = error instanceof Error ? error.message : String(error);
  }
}

async function runGetAction(path) {
  output.textContent = "执行中...";
  try {
    const result = await api(path);
    output.textContent = [result.stdout, result.stderr].filter(Boolean).join("\n") || `exit ${result.code}`;
  } catch (error) {
    output.textContent = error instanceof Error ? error.message : String(error);
  }
}

document.getElementById("refreshButton").addEventListener("click", () => {
  loadStatus().catch((error) => {
    output.textContent = error.message;
  });
});
document.getElementById("saveButton").addEventListener("click", saveConfig);
document.getElementById("dryRunButton").addEventListener("click", () => runAction("/api/actions/backup-dry-run"));
document.getElementById("backupButton").addEventListener("click", () => runAction("/api/actions/backup"));
document.getElementById("pullManifestDryRunButton").addEventListener("click", () => runAction("/api/actions/pull-manifest-dry-run"));
document.getElementById("pullManifestButton").addEventListener("click", () => runAction("/api/actions/pull-manifest"));
document.getElementById("installCronButton").addEventListener("click", () => runAction("/api/actions/install-cron"));
document.getElementById("showCronButton").addEventListener("click", () => runGetAction("/api/cron"));
document.getElementById("restoreDryRunButton").addEventListener("click", () => runAction("/api/actions/restore-dry-run"));
document.getElementById("copyConfigButton").addEventListener("click", async () => {
  await navigator.clipboard.writeText(document.getElementById("newWauleConfig").textContent || "");
  output.textContent = "NewWaule 后台配置参考已复制。";
});

await loadConfig();
await loadStatus();
