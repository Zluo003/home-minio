import { CONFIG_PAGES } from "./config-schema.js";

const API_BASE = `${location.protocol}//${location.hostname}:19090`;
let token = localStorage.getItem("homeMinioToken") || "";
let configState = { values: {}, configuredKeys: new Set(), editableKeys: new Set() };
let messageTimer = null;

const output = document.getElementById("output");

function headers() {
  return {
    "content-type": "application/json",
    "x-home-minio-token": token,
  };
}

async function api(path, options = {}, retried = false) {
  const response = await fetch(`${API_BASE}${path}`, {
    ...options,
    headers: {
      ...headers(),
      ...(options.headers || {}),
    },
  });
  if (response.status === 401 && !retried) {
    const nextToken = prompt("请输入 Home MinIO 管理 API 令牌") || "";
    if (!nextToken) throw new Error("需要管理 API 令牌才能访问控制台。");
    token = nextToken;
    localStorage.setItem("homeMinioToken", token);
    return api(path, options, true);
  }
  const data = await response.json();
  if (!response.ok) {
    const commandOutput = [data.stdout, data.stderr].filter(Boolean).join("\n");
    throw new Error(commandOutput || data.message || response.statusText);
  }
  return data;
}

function escapeHtml(value) {
  return String(value ?? "")
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#039;");
}

function showMessage(message, tone = "ok") {
  const element = document.getElementById("globalMessage");
  element.textContent = message;
  element.dataset.tone = tone;
  element.hidden = false;
  clearTimeout(messageTimer);
  messageTimer = setTimeout(() => {
    element.hidden = true;
  }, 5000);
}

function renderField(field) {
  const configured = configState.configuredKeys.has(field.key);
  const savedValue = configState.values[field.key];
  const value = savedValue === undefined || savedValue === null || savedValue === ""
    ? field.defaultValue ?? ""
    : savedValue;
  const classes = ["config-field", field.wide ? "config-field--wide" : ""].filter(Boolean).join(" ");
  const restartBadge = field.restart ? '<span class="field-badge">需重启</span>' : "";
  const fieldId = `config-${field.key.toLowerCase().replaceAll("_", "-")}`;
  let control = "";

  if (field.type === "select") {
    control = `<select id="${fieldId}" name="${field.key}">${(field.options || []).map(([optionValue, label]) => (
      `<option value="${escapeHtml(optionValue)}" ${String(value) === String(optionValue) ? "selected" : ""}>${escapeHtml(label)}</option>`
    )).join("")}</select>`;
  } else {
    const inputType = field.type === "password" ? "password" : field.type === "number" ? "number" : "text";
    const placeholder = field.type === "password" && configured
      ? "已配置，留空不修改"
      : field.placeholder || "";
    const inputValue = field.type === "password" ? "" : value;
    const numericAttributes = inputType === "number"
      ? `${field.min !== undefined ? ` min="${field.min}"` : ""}${field.max !== undefined ? ` max="${field.max}"` : ""}${field.step !== undefined ? ` step="${field.step}"` : ""}`
      : "";
    control = `<input id="${fieldId}" type="${inputType}" name="${field.key}" value="${escapeHtml(inputValue)}" placeholder="${escapeHtml(placeholder)}"${numericAttributes}${inputType === "password" ? ' autocomplete="new-password"' : ""} />`;
  }

  return `
    <label class="${classes}" for="${fieldId}">
      <span class="field-title"><strong>${escapeHtml(field.label)}</strong>${restartBadge}</span>
      ${control}
      <small>${escapeHtml(field.help)}</small>
    </label>
  `;
}

function renderConfigPages() {
  for (const page of CONFIG_PAGES) {
    const slot = document.querySelector(`[data-config-page="${page.id}"]`);
    if (!slot) continue;
    const fields = page.fields.filter((field) => !field.hidden && configState.editableKeys.has(field.key));
    slot.innerHTML = `
      <div class="page-heading">
        <div>
          <p class="page-kicker">页面配置</p>
          <h2>${escapeHtml(page.title)}</h2>
          <p>${escapeHtml(page.description)}</p>
        </div>
      </div>
      <form class="panel config-form" data-config-form="${page.id}">
        <div class="config-grid">${fields.map(renderField).join("")}</div>
        <footer class="form-footer">
          <span>密码和令牌留空不会覆盖已保存值。</span>
          <button class="button-primary" type="submit">保存本页</button>
        </footer>
      </form>
    `;
    slot.querySelector("form")?.addEventListener("submit", (event) => {
      event.preventDefault();
      saveConfigPage(page.id, event.currentTarget).catch((error) => showMessage(error.message, "error"));
    });
  }
}

async function loadConfig() {
  const data = await api("/api/config");
  configState = {
    values: data.values || {},
    configuredKeys: new Set(data.configuredKeys || []),
    editableKeys: new Set(data.editableKeys || []),
  };
  renderConfigPages();
}

async function saveConfigPage(pageId, form) {
  const page = CONFIG_PAGES.find((item) => item.id === pageId);
  if (!page) return;
  const values = {};
  const formData = new FormData(form);
  for (const field of page.fields) {
    if (!configState.editableKeys.has(field.key) || !formData.has(field.key)) continue;
    values[field.key] = String(formData.get(field.key) ?? "");
  }
  const button = form.querySelector('button[type="submit"]');
  button.disabled = true;
  try {
    const data = await api("/api/config", { method: "POST", body: JSON.stringify({ values }) });
    configState.values = data.values || configState.values;
    configState.configuredKeys = new Set(data.configuredKeys || []);
    renderConfigPages();
    const needsRestart = page.fields.some((field) => field.restart && Object.prototype.hasOwnProperty.call(values, field.key));
    showMessage(needsRestart ? "配置已保存。标记为“需重启”的项目会在重新创建容器后生效。" : "配置已保存并用于后续任务。", "ok");
    await loadStatus();
  } finally {
    if (button.isConnected) button.disabled = false;
  }
}

function formatBytes(value) {
  const bytes = Number(value || 0);
  if (!Number.isFinite(bytes) || bytes <= 0) return "0 B";
  const units = ["B", "KiB", "MiB", "GiB", "TiB"];
  let amount = bytes;
  let unit = 0;
  while (amount >= 1024 && unit < units.length - 1) {
    amount /= 1024;
    unit += 1;
  }
  return `${amount.toFixed(unit === 0 ? 0 : 2)} ${units[unit]}`;
}

function formatTime(value) {
  if (!value) return "-";
  const date = new Date(value);
  return Number.isNaN(date.getTime()) ? String(value) : date.toLocaleString("zh-CN", { hour12: false });
}

function transferStatusMarkup(status) {
  const lifecycleSettings = status.lifecycle?.settings || {};
  const lifecycleTelemetry = status.lifecycle?.telemetry || {};
  const recentJobs = Array.isArray(status.lifecycle?.recentJobs) ? status.lifecycle.recentJobs : [];
  const summary = `
    <div class="transfer-summary">
      <span>状态 <strong>${status.lifecycle?.ready ? "ready" : "unavailable"}</strong></span>
      <span>活跃对象 <strong>${Number(lifecycleTelemetry.activeObjects || 0)}</strong></span>
      <span>HTTP <strong>${Number(lifecycleTelemetry.activeHttpRequests || 0)}/${escapeHtml(lifecycleSettings.maxHttpConcurrency ?? "-")}</strong></span>
      <span>实时吞吐 <strong>${escapeHtml(formatBytes(lifecycleTelemetry.throughputBytesPerSecond))}/s</strong></span>
      <span>拉取并发 <strong>${escapeHtml(lifecycleSettings.pullConcurrency ?? "-")}</strong></span>
      <span>OSS 并发 <strong>${escapeHtml(lifecycleSettings.ossFileConcurrency ?? "-")}</strong></span>
    </div>
  `;
  if (!recentJobs.length) return `${summary}<p class="transfer-empty">暂无生命周期任务。</p>`;
  const jobs = recentJobs.map((job) => {
    const diagnostics = job.diagnostics || {};
    const counts = diagnostics.statusCounts || {};
    const processed = Number(job.processedCount || 0);
    const total = Math.max(0, Number(job.totalCount || 0));
    const percent = total > 0 ? Math.min(100, Math.max(0, Math.round(processed / total * 100))) : 0;
    const canCancel = job.status === "QUEUED" || job.status === "RUNNING";
    const canResume = job.status === "CANCELLED";
    const errors = Array.isArray(diagnostics.errorSamples) ? diagnostics.errorSamples : [];
    const errorMarkup = errors.length
      ? `<ul class="transfer-errors">${errors.map((entry) => `<li><strong>${Number(entry.count || 0)} 个</strong>${escapeHtml(entry.message || "未知错误")}</li>`).join("")}</ul>`
      : "";
    return `
      <article class="transfer-job transfer-job--${escapeHtml(String(job.status || "unknown").toLowerCase())}">
        <header>
          <div>
            <strong>${escapeHtml(job.mediaKind)} · ${escapeHtml(job.status)}</strong>
            <small>${escapeHtml(job.id)}</small>
          </div>
          ${canCancel ? `<button class="button-danger" data-job-action="cancel" data-job-id="${escapeHtml(job.id)}" type="button">中止</button>` : ""}
          ${canResume ? `<button class="button-primary" data-job-action="resume" data-job-id="${escapeHtml(job.id)}" type="button">继续</button>` : ""}
        </header>
        <div class="transfer-progress-head"><span>${processed}/${total} 个文件</span><strong>${percent}%</strong></div>
        <div class="transfer-progress" role="progressbar" aria-valuemin="0" aria-valuemax="100" aria-valuenow="${percent}"><i style="width:${percent}%"></i></div>
        <div class="transfer-job-meta">
          <span>成功 ${Number(job.succeededCount || 0)}</span>
          <span>运行 ${Number(counts.RUNNING || 0)}</span>
          <span>排队 ${Number(counts.QUEUED || 0)}</span>
          <span>重试等待 ${Number(counts.RETRY_WAIT || 0)}</span>
          <span>失败 ${Number(job.failedCount || 0)}</span>
          <span>已处理 ${escapeHtml(formatBytes(job.processedBytes))}</span>
          <span>下次重试 ${escapeHtml(formatTime(diagnostics.nextRetryAt))}</span>
        </div>
        ${errorMarkup}
      </article>
    `;
  }).join("");
  return `${summary}<div class="transfer-jobs">${jobs}</div>`;
}

function renderTransferStatus(status) {
  const markup = transferStatusMarkup(status);
  document.getElementById("transferStatus").innerHTML = markup;
  document.getElementById("overviewTransferStatus").innerHTML = markup;
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
    : status.lifecycle?.startupError || "未就绪";

  renderTransferStatus(status);

  const configText = Object.entries(status.newWauleConfig)
    .map(([key, value]) => `${key}: ${value}`)
    .join("\n");
  document.getElementById("newWauleConfig").textContent = [
    "NewWaule 管理后台 -> 系统设置 -> Home MinIO 冷存储",
    "",
    configText,
  ].join("\n");
}

async function runLifecycleJobAction(jobId, action) {
  await api(`/api/lifecycle/jobs/${encodeURIComponent(jobId)}/${action}`, { method: "POST", body: "{}" });
  showMessage(action === "cancel" ? "生命周期任务已中止。" : "生命周期任务已继续。", "ok");
  await loadStatus();
}

for (const targetId of ["transferStatus", "overviewTransferStatus"]) {
  document.getElementById(targetId).addEventListener("click", (event) => {
    const button = event.target.closest("button[data-job-action][data-job-id]");
    if (!button) return;
    button.disabled = true;
    runLifecycleJobAction(button.dataset.jobId, button.dataset.jobAction)
      .catch((error) => showMessage(error instanceof Error ? error.message : String(error), "error"))
      .finally(() => {
        if (button.isConnected) button.disabled = false;
      });
  });
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

function setPage(pageId) {
  const validPages = new Set(["overview", ...CONFIG_PAGES.map((page) => page.id)]);
  const selected = validPages.has(pageId) ? pageId : "overview";
  document.querySelectorAll("[data-page]").forEach((page) => {
    page.hidden = page.dataset.page !== selected;
  });
  document.querySelectorAll("[data-page-target]").forEach((button) => {
    const active = button.dataset.pageTarget === selected;
    button.classList.toggle("is-active", active);
    button.setAttribute("aria-current", active ? "page" : "false");
  });
  if (location.hash !== `#${selected}`) history.replaceState(null, "", `#${selected}`);
}

document.querySelectorAll("[data-page-target]").forEach((button) => {
  button.addEventListener("click", () => setPage(button.dataset.pageTarget));
});
window.addEventListener("hashchange", () => setPage(location.hash.slice(1)));

document.getElementById("refreshButton").addEventListener("click", () => {
  Promise.all([loadConfig(), loadStatus()])
    .then(() => showMessage("状态和配置已刷新。"))
    .catch((error) => showMessage(error.message, "error"));
});
document.getElementById("dryRunButton").addEventListener("click", () => runAction("/api/actions/backup-dry-run"));
document.getElementById("backupButton").addEventListener("click", () => runAction("/api/actions/backup"));
document.getElementById("pullManifestDryRunButton").addEventListener("click", () => runAction("/api/actions/pull-manifest-dry-run"));
document.getElementById("pullManifestButton").addEventListener("click", () => runAction("/api/actions/pull-manifest"));
document.getElementById("installCronButton").addEventListener("click", () => runAction("/api/actions/install-cron"));
document.getElementById("showCronButton").addEventListener("click", () => runGetAction("/api/cron"));
document.getElementById("restoreDryRunButton").addEventListener("click", () => runAction("/api/actions/restore-dry-run"));
document.getElementById("copyConfigButton").addEventListener("click", async () => {
  await navigator.clipboard.writeText(document.getElementById("newWauleConfig").textContent || "");
  showMessage("NewWaule 后台配置已复制。", "ok");
});

setPage(location.hash.slice(1));
await Promise.all([loadConfig(), loadStatus()]);
let statusRefreshRunning = false;
setInterval(() => {
  if (document.hidden || statusRefreshRunning) return;
  statusRefreshRunning = true;
  loadStatus()
    .catch((error) => showMessage(error instanceof Error ? error.message : String(error), "error"))
    .finally(() => {
      statusRefreshRunning = false;
    });
}, 3_000);
