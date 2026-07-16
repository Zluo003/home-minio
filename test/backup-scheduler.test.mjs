import assert from "node:assert/strict";
import test from "node:test";
import { nextBackupRunDelaySeconds } from "../scripts/next-backup-run-delay.mjs";
import {
  describeBackupSchedule,
  normalizeBackupSchedule,
  validateBackupScheduleValues,
} from "../web/backend/backup-schedule.mjs";

test("scheduler calculates today's backup in the selected time zone", () => {
  const now = new Date("2026-07-15T19:34:30.000Z");
  assert.equal(nextBackupRunDelaySeconds({
    time: "03:35",
    timeZone: "Asia/Shanghai",
    frequency: "daily",
    now,
  }), 30);
});

test("scheduler rolls an elapsed daily time to tomorrow", () => {
  const now = new Date("2026-07-15T19:35:30.000Z");
  assert.equal(nextBackupRunDelaySeconds({
    time: "03:35",
    timeZone: "Asia/Shanghai",
    frequency: "daily",
    now,
  }), 86_370);
});

test("weekly schedules wait for the selected weekday", () => {
  const now = new Date("2026-07-15T19:34:30.000Z");
  assert.equal(nextBackupRunDelaySeconds({
    time: "03:35",
    timeZone: "Asia/Shanghai",
    frequency: "weekly:5",
    now,
  }), 86_430);
});

test("legacy Cron values migrate to direct schedule fields", () => {
  assert.deepEqual(normalizeBackupSchedule({
    BAIDUPAN_CRON_SCHEDULE: "20 4 * * 1",
  }, { defaultTimeZone: "UTC" }), {
    frequency: "weekly:1",
    time: "04:20",
    timeZone: "UTC",
  });
});

test("schedule descriptions and validation use administrator-facing values", () => {
  assert.equal(describeBackupSchedule({
    frequency: "weekly:1",
    time: "04:20",
    timeZone: "Asia/Shanghai",
  }), "每周一 04:20 (Asia/Shanghai)");
  assert.throws(
    () => validateBackupScheduleValues({ BAIDUPAN_BACKUP_TIME: "25:00" }),
    /备份时间无效/,
  );
});
