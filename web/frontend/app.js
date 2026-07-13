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
  HOME_MINIO_BIND_ADDRESS: "宿主机绑定地址",
  MINIO_API_PORT: "MinIO API 端口",
  MINIO_CONSOLE_PORT: "MinIO Console 端口",
  MINIO_DATA_DIR: "MinIO 数据目录",
  MINIO_BUCKET: "媒体 Bucket",
  MINIO_INTERNAL_ENDPOINT: "MinIO 内部写入地址",
  MINIO_WAULE_ACCESS_KEY: "NewWaule Access Key",
  MINIO_WAULE_SECRET_KEY: "NewWaule Secret Key",
  HOME_MINIO_WEB_API_PORT: "管理后端端口",
  HOME_MINIO_WEB_PORT: "管理前端端口",
  HOME_MINIO_WEB_TOKEN: "管理控制台令牌",
  HOME_MINIO_STATE_DB: "SQLite 状态数据库",
  HOME_MINIO_CONFIG_ENCRYPTION_KEY: "OSS 凭据加密主密钥",
  HOME_MINIO_CONFIG_ENCRYPTION_KEY_FILE: "主密钥 Docker Secret",
  HOME_MINIO_TRANSFER_WORK_DIR: "Multipart 临时目录",
  HOME_MINIO_PUBLIC_ENDPOINT: "MinIO 访问地址",
  HOME_MINIO_CONSOLE_PUBLIC_URL: "MinIO 控制台地址",
  NEWWAULE_API_BASE_URL: "NewWaule API 公网地址",
  NEWWAULE_CACHE_UPLOAD_BASE_URL: "缓存上传基地址",
  NEWWAULE_HOME_MINIO_TOKEN: "NewWaule 推送令牌",
  CACHE_PUSH_CONCURRENCY: "缓存推送并发",
  MEDIA_PULL_MANIFEST_PATH: "媒体拉取清单",
  MEDIA_PULL_WORK_DIR: "媒体拉取工作目录",
  MEDIA_PULL_CONCURRENCY: "媒体拉取并发",
  OSS_FILE_CONCURRENCY: "OSS 文件并发",
  OSS_MULTIPART_CONCURRENCY: "大文件分片并发",
  OSS_MAX_HTTP_CONCURRENCY: "全局 HTTP 并发",
  OSS_MULTIPART_THRESHOLD_BYTES: "Multipart 阈值（字节）",
  OSS_PART_SIZE_BYTES: "Multipart 分片大小（字节）",
  BAIDUPAN_BACKUP_ENABLED: "启用百度网盘备份",
  BAIDUPAN_TOOL: "百度网盘工具",
  BAIDUPAN_REMOTE_DIR: "百度网盘目录",
  BAIDUPAN_WORK_DIR: "本地备份工作目录",
  BAIDUPAN_CRON_SCHEDULE: "自动备份时间",
  BYPY_BIN: "bypy 命令",
  BAIDUPCS_BIN: "BaiduPCS-Go 命令",
  BAIDUPCS_CONFIG_DIR: "BaiduPCS-Go 配置目录",
  BAIDUPCS_MAX_PARALLEL: "BaiduPCS-Go 并发",
  BAIDUPCS_UPLOAD_NORAPID: "跳过秒传",
};

const secretKeys = new Set(["MINIO_ROOT_PASSWORD", "MINIO_WAULE_SECRET_KEY", "HOME_MINIO_WEB_TOKEN", "HOME_MINIO_CONFIG_ENCRYPTION_KEY", "NEWWAULE_HOME_MINIO_TOKEN"]);

function renderForm(values, keys, configuredKeys = []) {
  const configured = new Set(configuredKeys);
  form.innerHTML = keys.map((key) => {
    const value = values[key] ?? "";
    if (key === "BAIDUPAN_BACKUP_ENABLED") {
      return `<label><span>${labels[key] || key}</span><select name="${key}"><option value="true" ${value === "true" ? "selected" : ""}>true</option><option value="false" ${value !== "true" ? "selected" : ""}>false</option></select></label>`;
    }
    if (key === "BAIDUPAN_TOOL") {
      return `<label><span>${labels[key] || key}</span><select name="${key}"><option value="baidupcs" ${value !== "bypy" ? "selected" : ""}>baidupcs</option><option value="bypy" ${value === "bypy" ? "selected" : ""}>bypy</option></select></label>`;
    }
    const type = secretKeys.has(key) ? "password" : "text";
    const placeholder = secretKeys.has(key) && configured.has(key) ? "已配置，留空不修改" : "";
    return `<label><span>${labels[key] || key}</span><input type="${type}" name="${key}" value="${String(value).replaceAll('"', "&quot;")}" placeholder="${placeholder}" /></label>`;
  }).join("");
}

async function loadConfig() {
  const data = await api("/api/config");
  renderForm(data.values, data.editableKeys, data.configuredKeys);
}

async function loadStatus() {
  const status = await api("/api/status");
  document.getElementById("statusText").textContent = status.minio.ok ? "MinIO online" : "MinIO offline";
  document.getElementById("portText").textContent = `API ${status.ports.minioApi} · Console ${status.ports.minioConsole} · Web ${status.ports.web}`;
  document.getElementById("minioState").textContent = status.publicUrls.minioEndpoint || (status.minio.ok ? status.minio.endpoint : status.minio.error || "offline");
  document.getElementById("baidupanState").textContent = status.baidupan.enabled ? status.baidupan.remoteDir : "未启用";
  document.getElementById("toolState").textContent = status.cachePush?.latestJob
    ? `push ${status.cachePush.latestJob.status} · ${status.cachePush.concurrency}`
    : `${status.baidupan.tool} · ${status.baidupan.cronSchedule}`;
  document.getElementById("lifecycleState").textContent = status.lifecycle?.ready
    ? `${status.lifecycle.activeItems} 处理中 · ${status.lifecycle.jobs} 批次`
    : status.lifecycle?.startupError || status.lifecycle?.encryptionError || "未就绪";
  const lifecycleSettings = status.lifecycle?.settings || {};
  const lifecycleTelemetry = status.lifecycle?.telemetry || {};
  const recentJobs = Array.isArray(status.lifecycle?.recentJobs) ? status.lifecycle.recentJobs : [];
  document.getElementById("transferStatus").textContent = [
    `状态：${status.lifecycle?.ready ? "ready" : "unavailable"}`,
    `拉取并发：${lifecycleSettings.pullConcurrency ?? "-"}`,
    `OSS 文件并发：${lifecycleSettings.ossFileConcurrency ?? "-"}`,
    `Multipart 并发：${lifecycleSettings.multipartConcurrency ?? "-"}`,
    `全局 HTTP 并发：${lifecycleSettings.maxHttpConcurrency ?? "-"}`,
    `Multipart 阈值：${lifecycleSettings.multipartThresholdBytes ?? "-"}`,
    `分片大小：${lifecycleSettings.partSizeBytes ?? "-"}`,
    `当前活跃对象：${lifecycleTelemetry.activeObjects ?? 0}`,
    `当前 HTTP 占用：${lifecycleTelemetry.activeHttpRequests ?? 0}/${lifecycleSettings.maxHttpConcurrency ?? "-"}`,
    `60 秒吞吐：${Math.round(lifecycleTelemetry.throughputBytesPerSecond ?? 0)} bytes/s`,
    "",
    "最近任务：",
    ...recentJobs.map((job) => `${job.id} · ${job.mediaKind} · ${job.status} · ${job.succeededCount}/${job.totalCount} · ${job.processedBytes} bytes`),
  ].join("\n");
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
    if (result.job) {
      output.textContent = `任务已启动：${result.jobId || result.job.id}\n状态：${result.job.status}`;
      return;
    }
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
