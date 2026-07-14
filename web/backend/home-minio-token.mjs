function normalizeToken(value) {
  return typeof value === "string" ? value.trim() : "";
}

export function collectHomeMinioAuthTokens(values) {
  return [...new Set([
    normalizeToken(values?.HOME_MINIO_WEB_TOKEN),
    normalizeToken(values?.NEWWAULE_HOME_MINIO_TOKEN),
  ].filter(Boolean))];
}

export function preferredHomeMinioToken(values) {
  return normalizeToken(values?.HOME_MINIO_WEB_TOKEN)
    || normalizeToken(values?.NEWWAULE_HOME_MINIO_TOKEN);
}

export function synchronizeHomeMinioTokenValues(values) {
  const next = { ...(values || {}) };
  if (Object.prototype.hasOwnProperty.call(next, "HOME_MINIO_WEB_TOKEN")) {
    next.NEWWAULE_HOME_MINIO_TOKEN = next.HOME_MINIO_WEB_TOKEN;
  } else if (Object.prototype.hasOwnProperty.call(next, "NEWWAULE_HOME_MINIO_TOKEN")) {
    next.HOME_MINIO_WEB_TOKEN = next.NEWWAULE_HOME_MINIO_TOKEN;
  }
  return next;
}
