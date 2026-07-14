import assert from "node:assert/strict";
import { Readable } from "node:stream";
import test from "node:test";
import { createMinioSignedHttpClient } from "../web/backend/minio-signed-http.mjs";

const env = {
  MINIO_INTERNAL_ENDPOINT: "http://minio.test:9000",
  MINIO_BUCKET: "media",
  MINIO_WAULE_ACCESS_KEY: "waule-user",
  MINIO_WAULE_SECRET_KEY: "waule-secret",
};

test("signed MinIO transport streams objects through the proven path-style HTTP contract", async () => {
  let uploaded = Buffer.alloc(0);
  const requests = [];
  const client = createMinioSignedHttpClient(env, {
    fetchImpl: async (url, init) => {
      requests.push({ url: String(url), method: init.method, authorization: init.headers.Authorization });
      if (init.method === "HEAD" && new URL(url).pathname === "/media") return new Response(null, { status: 200 });
      if (init.method === "PUT") {
        const chunks = [];
        for await (const chunk of init.body) chunks.push(Buffer.from(chunk));
        uploaded = Buffer.concat(chunks);
        return new Response(null, { status: 200, headers: { etag: '"uploaded"' } });
      }
      if (init.method === "HEAD") {
        return new Response(null, {
          status: 200,
          headers: { "content-length": String(uploaded.length), etag: '"uploaded"' },
        });
      }
      throw new Error(`Unexpected request ${init.method} ${url}`);
    },
  });

  await client.ensureBucket();
  await client.putObject("gateway-media/a b.png", Readable.from(Buffer.from("media-body")), {
    contentLength: 10,
    contentType: "image/png",
  });
  const head = await client.headObject("gateway-media/a b.png");

  assert.deepEqual(uploaded, Buffer.from("media-body"));
  assert.equal(head.sizeBytes, uploaded.length);
  assert.equal(requests[1].url, "http://minio.test:9000/media/gateway-media/a%20b.png");
  assert.match(requests[1].authorization, /^AWS4-HMAC-SHA256 /);
});

test("signed MinIO transport exposes authorization failures instead of UnknownError", async () => {
  const client = createMinioSignedHttpClient(env, {
    fetchImpl: async () => new Response(
      "<Error><Code>AccessDenied</Code><Message>Invalid credentials</Message></Error>",
      { status: 403 },
    ),
  });

  await assert.rejects(
    client.ensureBucket(),
    /MinIO bucket HEAD failed with HTTP 403\. AccessDenied: Invalid credentials/,
  );
});
