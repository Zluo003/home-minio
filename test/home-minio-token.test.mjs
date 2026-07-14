import assert from "node:assert/strict";
import test from "node:test";
import {
  collectHomeMinioAuthTokens,
  preferredHomeMinioToken,
  synchronizeHomeMinioTokenValues,
} from "../web/backend/home-minio-token.mjs";

test("management API accepts the unified token and a distinct legacy cache token", () => {
  assert.deepEqual(collectHomeMinioAuthTokens({
    HOME_MINIO_WEB_TOKEN: "management-token",
    NEWWAULE_HOME_MINIO_TOKEN: "legacy-token",
  }), ["management-token", "legacy-token"]);
  assert.equal(preferredHomeMinioToken({
    HOME_MINIO_WEB_TOKEN: "management-token",
    NEWWAULE_HOME_MINIO_TOKEN: "legacy-token",
  }), "management-token");
});

test("saving either token field synchronizes the compatibility alias", () => {
  assert.deepEqual(synchronizeHomeMinioTokenValues({
    HOME_MINIO_WEB_TOKEN: "one-token",
    CACHE_PUSH_CONCURRENCY: "4",
  }), {
    HOME_MINIO_WEB_TOKEN: "one-token",
    NEWWAULE_HOME_MINIO_TOKEN: "one-token",
    CACHE_PUSH_CONCURRENCY: "4",
  });
  assert.deepEqual(synchronizeHomeMinioTokenValues({
    NEWWAULE_HOME_MINIO_TOKEN: "legacy-save",
  }), {
    HOME_MINIO_WEB_TOKEN: "legacy-save",
    NEWWAULE_HOME_MINIO_TOKEN: "legacy-save",
  });
});
