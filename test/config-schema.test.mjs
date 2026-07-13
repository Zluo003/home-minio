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
