#!/usr/bin/env node

import { resolve } from "node:path";
import { fileURLToPath } from "node:url";
import {
  BACKUP_FREQUENCIES,
  isValidTimeZone,
} from "../web/backend/backup-schedule.mjs";

const weekdayNumbers = new Map([
  ["Sun", 0],
  ["Mon", 1],
  ["Tue", 2],
  ["Wed", 3],
  ["Thu", 4],
  ["Fri", 5],
  ["Sat", 6],
]);

function parseTime(value) {
  const match = /^(\d{2}):(\d{2})$/.exec(String(value || ""));
  if (!match) throw new Error(`Invalid backup time: ${value}`);
  const hour = Number(match[1]);
  const minute = Number(match[2]);
  if (hour > 23 || minute > 59) throw new Error(`Invalid backup time: ${value}`);
  return { hour, minute };
}

function zonedParts(date, timeZone) {
  const parts = new Intl.DateTimeFormat("en-US", {
    timeZone,
    calendar: "gregory",
    numberingSystem: "latn",
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
    weekday: "short",
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
    hourCycle: "h23",
  }).formatToParts(date);
  const values = Object.fromEntries(parts.map((part) => [part.type, part.value]));
  return {
    dateKey: `${values.year}-${values.month}-${values.day}`,
    weekday: weekdayNumbers.get(values.weekday),
    hour: Number(values.hour),
    minute: Number(values.minute),
    second: Number(values.second),
  };
}

export function nextBackupRunDelaySeconds({
  time,
  timeZone,
  frequency = "daily",
  now = new Date(),
}) {
  const targetTime = parseTime(time);
  if (!isValidTimeZone(timeZone)) throw new Error(`Invalid backup time zone: ${timeZone}`);
  if (!BACKUP_FREQUENCIES.has(frequency)) throw new Error(`Invalid backup frequency: ${frequency}`);
  const targetWeekday = frequency.startsWith("weekly:") ? Number(frequency.slice(7)) : null;
  const current = zonedParts(now, timeZone);
  const currentMinute = current.hour * 60 + current.minute + (current.second + now.getMilliseconds() / 1000) / 60;
  const targetMinute = targetTime.hour * 60 + targetTime.minute;
  const todayEligible = targetWeekday === null || current.weekday === targetWeekday;
  const allowCurrentDate = todayEligible && targetMinute > currentMinute;
  const firstCandidateMs = Math.floor(now.getTime() / 60_000) * 60_000 + 60_000;

  for (let offset = 0; offset <= 8 * 24 * 60; offset += 1) {
    const candidate = new Date(firstCandidateMs + offset * 60_000);
    const candidateParts = zonedParts(candidate, timeZone);
    if (candidateParts.dateKey === current.dateKey && !allowCurrentDate) continue;
    if (targetWeekday !== null && candidateParts.weekday !== targetWeekday) continue;
    if (candidateParts.hour === targetTime.hour && candidateParts.minute === targetTime.minute) {
      return Math.max(1, Math.ceil((candidate.getTime() - now.getTime()) / 1000));
    }
  }
  throw new Error("Unable to calculate the next backup time within eight days.");
}

function main() {
  console.log(nextBackupRunDelaySeconds({
    time: process.argv[2],
    timeZone: process.argv[3],
    frequency: process.argv[4] || "daily",
  }));
}

if (process.argv[1] && resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  try {
    main();
  } catch (error) {
    console.error(error instanceof Error ? error.message : String(error));
    process.exitCode = 1;
  }
}
