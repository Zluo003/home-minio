#!/usr/bin/env node

import { resolve } from "node:path";
import { fileURLToPath } from "node:url";

export function nextDailyRunDelaySeconds(hhmm, now = new Date()) {
  const match = /^(\d{2}):(\d{2})$/.exec(String(hhmm || ""));
  if (!match) throw new Error(`Invalid daily backup time: ${hhmm}`);
  const hour = Number(match[1]);
  const minute = Number(match[2]);
  if (hour > 23 || minute > 59) throw new Error(`Invalid daily backup time: ${hhmm}`);

  const target = new Date(now);
  target.setHours(hour, minute, 0, 0);
  if (target.getTime() <= now.getTime()) target.setDate(target.getDate() + 1);
  return Math.max(1, Math.ceil((target.getTime() - now.getTime()) / 1000));
}

function main() {
  console.log(nextDailyRunDelaySeconds(process.argv[2]));
}

if (process.argv[1] && resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  try {
    main();
  } catch (error) {
    console.error(error instanceof Error ? error.message : String(error));
    process.exitCode = 1;
  }
}
