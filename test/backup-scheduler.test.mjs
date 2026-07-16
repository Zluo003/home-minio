import assert from "node:assert/strict";
import test from "node:test";
import { nextDailyRunDelaySeconds } from "../scripts/next-daily-run-delay.mjs";

test("daily scheduler waits until today's configured time", () => {
  const now = new Date(2026, 6, 16, 3, 34, 30, 0);
  assert.equal(nextDailyRunDelaySeconds("03:35", now), 30);
});

test("daily scheduler rolls an elapsed configured time to tomorrow", () => {
  const now = new Date(2026, 6, 16, 3, 35, 30, 0);
  assert.equal(nextDailyRunDelaySeconds("03:35", now), 86_370);
});

test("daily scheduler rejects invalid times", () => {
  assert.throws(() => nextDailyRunDelaySeconds("25:00"), /Invalid daily backup time/);
});
