import assert from "node:assert/strict";
import test from "node:test";
import { CONFIG_FIELDS, CONFIG_PAGES, SECRET_CONFIG_KEYS } from "../web/frontend/config-schema.js";

test("configuration is split into focused pages and every field has help text", () => {
  assert.deepEqual(CONFIG_PAGES.map((page) => page.id), ["storage", "newwaule", "transfer", "backup", "advanced"]);
  assert.ok(CONFIG_FIELDS.size >= 30);
  for (const page of CONFIG_PAGES) {
    assert.ok(page.title);
    assert.ok(page.description);
    assert.ok(page.fields.length > 0);
    for (const field of page.fields) {
      assert.ok(field.key);
      assert.ok(field.label);
      assert.ok(field.help);
      assert.equal(CONFIG_FIELDS.get(field.key)?.pageId, page.id);
    }
  }
});

test("configuration no longer exposes a lifecycle encryption key", () => {
  assert.equal(CONFIG_FIELDS.has("HOME_MINIO_CONFIG_ENCRYPTION_KEY"), false);
  assert.equal(CONFIG_FIELDS.has("HOME_MINIO_CONFIG_ENCRYPTION_KEY_FILE"), false);
  assert.equal(SECRET_CONFIG_KEYS.has("MINIO_ROOT_PASSWORD"), true);
  assert.equal(SECRET_CONFIG_KEYS.has("HOME_MINIO_WEB_TOKEN"), true);
});

test("configuration exposes one unified Home MinIO management token", () => {
  const visibleTokenFields = CONFIG_PAGES
    .flatMap((page) => page.fields)
    .filter((field) => field.type === "password" && !field.hidden && /TOKEN/.test(field.key));
  assert.deepEqual(visibleTokenFields.map((field) => field.key), ["HOME_MINIO_WEB_TOKEN"]);
  assert.equal(CONFIG_FIELDS.get("NEWWAULE_HOME_MINIO_TOKEN")?.hidden, true);
});

test("backup scheduling uses direct frequency, time and time-zone controls", () => {
  assert.equal(CONFIG_FIELDS.has("BAIDUPAN_CRON_SCHEDULE"), false);
  assert.equal(CONFIG_FIELDS.get("BAIDUPAN_BACKUP_FREQUENCY")?.type, "select");
  assert.equal(CONFIG_FIELDS.get("BAIDUPAN_BACKUP_TIME")?.type, "time");
  assert.equal(CONFIG_FIELDS.get("BAIDUPAN_TIME_ZONE")?.type, "select");
  assert.equal(CONFIG_FIELDS.get("BAIDUPAN_WORK_DIR")?.defaultValue, "/data/backup");
  assert.match(CONFIG_FIELDS.get("BAIDUPAN_WORK_DIR")?.help || "", /不长期保存完整媒体副本/);
});
