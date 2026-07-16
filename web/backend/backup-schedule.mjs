export const BACKUP_FREQUENCIES = new Set([
  "daily",
  "weekly:1",
  "weekly:2",
  "weekly:3",
  "weekly:4",
  "weekly:5",
  "weekly:6",
  "weekly:0",
]);

const frequencyLabels = new Map([
  ["daily", "每天"],
  ["weekly:1", "每周一"],
  ["weekly:2", "每周二"],
  ["weekly:3", "每周三"],
  ["weekly:4", "每周四"],
  ["weekly:5", "每周五"],
  ["weekly:6", "每周六"],
  ["weekly:0", "每周日"],
]);

function normalizeTime(value) {
  const match = /^(\d{1,2}):(\d{2})$/.exec(String(value || "").trim());
  if (!match) return null;
  const hour = Number(match[1]);
  const minute = Number(match[2]);
  return hour <= 23 && minute <= 59
    ? `${String(hour).padStart(2, "0")}:${String(minute).padStart(2, "0")}`
    : null;
}

function legacySchedule(value) {
  const [minute, hour, dayOfMonth, month, dayOfWeek] = String(value || "").trim().split(/\s+/);
  const time = /^\d{1,2}$/.test(hour || "") && /^\d{1,2}$/.test(minute || "")
    ? normalizeTime(`${hour}:${String(minute).padStart(2, "0")}`)
    : null;
  let frequency = "daily";
  if (dayOfMonth === "*" && month === "*" && /^(?:[0-6]|7)$/.test(dayOfWeek || "")) {
    frequency = `weekly:${dayOfWeek === "7" ? "0" : dayOfWeek}`;
  }
  return { time, frequency };
}

export function isValidTimeZone(value) {
  const timeZone = String(value || "").trim();
  if (!timeZone) return false;
  try {
    new Intl.DateTimeFormat("en-US", { timeZone }).format();
    return true;
  } catch {
    return false;
  }
}

export function normalizeBackupSchedule(values = {}, options = {}) {
  const legacy = legacySchedule(values.BAIDUPAN_CRON_SCHEDULE);
  const fallbackTimeZone = String(
    options.defaultTimeZone
      || values.TZ
      || (Intl.DateTimeFormat().resolvedOptions().timeZone || "UTC"),
  );
  const configuredFrequency = String(values.BAIDUPAN_BACKUP_FREQUENCY || "").trim();
  const configuredTimeZone = String(values.BAIDUPAN_TIME_ZONE || "").trim();
  return {
    frequency: BACKUP_FREQUENCIES.has(configuredFrequency) ? configuredFrequency : legacy.frequency,
    time: normalizeTime(values.BAIDUPAN_BACKUP_TIME) || legacy.time || "03:35",
    timeZone: isValidTimeZone(configuredTimeZone)
      ? configuredTimeZone
      : isValidTimeZone(fallbackTimeZone)
        ? fallbackTimeZone
        : "UTC",
  };
}

export function validateBackupScheduleValues(values = {}) {
  if (Object.hasOwn(values, "BAIDUPAN_BACKUP_FREQUENCY")
    && !BACKUP_FREQUENCIES.has(String(values.BAIDUPAN_BACKUP_FREQUENCY))) {
    throw new Error("备份频率无效。");
  }
  if (Object.hasOwn(values, "BAIDUPAN_BACKUP_TIME") && !normalizeTime(values.BAIDUPAN_BACKUP_TIME)) {
    throw new Error("备份时间无效。");
  }
  if (Object.hasOwn(values, "BAIDUPAN_TIME_ZONE") && !isValidTimeZone(values.BAIDUPAN_TIME_ZONE)) {
    throw new Error("备份时区无效。");
  }
}

export function describeBackupSchedule(schedule) {
  return `${frequencyLabels.get(schedule.frequency) || "每天"} ${schedule.time} (${schedule.timeZone})`;
}
