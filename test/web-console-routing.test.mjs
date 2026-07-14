import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import test from "node:test";

const appSource = await readFile(new URL("../web/frontend/app.js", import.meta.url), "utf8");
const nginxTemplate = await readFile(new URL("../web/nginx/default.conf.template", import.meta.url), "utf8");
const composeSource = await readFile(new URL("../docker-compose.yml", import.meta.url), "utf8");

test("the web console reaches its API through the same-origin nginx proxy", () => {
  assert.match(appSource, /const API_BASE = location\.origin;/);
  assert.doesNotMatch(appSource, /location\.hostname}:19090/);
  assert.match(nginxTemplate, /location \/api\//);
  assert.match(nginxTemplate, /proxy_pass http:\/\/web-api:\$\{HOME_MINIO_WEB_API_PORT\};/);
  assert.match(composeSource, /default\.conf\.template:\/etc\/nginx\/templates\/default\.conf\.template:ro/);
});
