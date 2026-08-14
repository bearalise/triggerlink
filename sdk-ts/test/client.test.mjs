// client.send：事件发送到平台 Event API（协议第 9 节）。
import { test } from "node:test";
import assert from "node:assert/strict";
import { createServer } from "node:http";
import { createClient } from "../dist/index.js";

// 起一个假平台，记录收到的请求并按给定状态码/响应体回复。
async function fakePlatform(handler) {
  const received = [];
  const server = createServer((req, res) => {
    const chunks = [];
    req.on("data", (c) => chunks.push(c));
    req.on("end", () => {
      received.push({
        method: req.method,
        url: req.url,
        authorization: req.headers.authorization,
        body: Buffer.concat(chunks).toString(),
      });
      const { status, body } = handler(received[received.length - 1]);
      res.writeHead(status, { "content-type": "application/json" });
      res.end(body);
    });
  });
  await new Promise((resolve) => server.listen(0, "127.0.0.1", resolve));
  return { received, url: `http://127.0.0.1:${server.address().port}`, close: () => server.close() };
}

test("send 单个事件：POST /v1/events，Bearer 鉴权，返回 ids", async () => {
  const platform = await fakePlatform(() => ({ status: 200, body: '{"ids":["evt_1"],"status":200}' }));
  const client = createClient({ id: "web", signingKey: "sk", eventKey: "ek", baseUrl: platform.url });

  const result = await client.send({ id: "order-1-paid", name: "order/paid", data: { order_id: "1" } });

  assert.deepEqual(result, { ids: ["evt_1"], status: 200 });
  assert.equal(platform.received.length, 1);
  const req = platform.received[0];
  assert.equal(req.method, "POST");
  assert.equal(req.url, "/v1/events");
  assert.equal(req.authorization, "Bearer ek");
  assert.deepEqual(JSON.parse(req.body), { id: "order-1-paid", name: "order/paid", data: { order_id: "1" } });
  platform.close();
});

test("send 事件数组：body 原样为数组", async () => {
  const platform = await fakePlatform(() => ({ status: 200, body: '{"ids":["a","b"],"status":200}' }));
  const client = createClient({ id: "web", signingKey: "sk", eventKey: "ek", baseUrl: platform.url });

  const result = await client.send([{ name: "x/y" }, { name: "x/z", data: { n: 1 } }]);

  assert.deepEqual(result, { ids: ["a", "b"], status: 200 });
  assert.deepEqual(JSON.parse(platform.received[0].body), [{ name: "x/y" }, { name: "x/z", data: { n: 1 } }]);
  platform.close();
});

test("send 缺 eventKey / 空数组 / 缺 name：直接抛错，不发请求", async () => {
  const noKey = createClient({ id: "web", signingKey: "sk" });
  await assert.rejects(() => noKey.send({ name: "x/y" }), /eventKey is required/);

  const client = createClient({ id: "web", signingKey: "sk", eventKey: "ek" });
  await assert.rejects(() => client.send([]), /must not be empty/);
  await assert.rejects(() => client.send({ data: {} }), /event\.name is required/);
});

test("send 平台返回非 2xx：抛错并携带状态码", async () => {
  const platform = await fakePlatform(() => ({ status: 401, body: '{"error":"bad key"}' }));
  const client = createClient({ id: "web", signingKey: "sk", eventKey: "wrong", baseUrl: platform.url });

  await assert.rejects(() => client.send({ name: "x/y" }), /401.*bad key/);
  platform.close();
});

// 轮询等待条件成立（register 是后台异步重试，测试里需要等它完成）。
async function waitFor(cond, timeoutMs = 3000) {
  const deadline = Date.now() + timeoutMs;
  while (!cond()) {
    if (Date.now() > deadline) throw new Error("waitFor: timeout");
    await new Promise((r) => setTimeout(r, 20));
  }
}

test("register：POST /api/v1/apps，Bearer 鉴权，成功即停", async () => {
  const platform = await fakePlatform(() => ({ status: 201, body: "{}" }));
  const client = createClient({ id: "web", signingKey: "sk", eventKey: "ek", baseUrl: platform.url });

  client.register("http://localhost:3000/api/triggerlink", { intervalMs: 10 });

  await waitFor(() => platform.received.length >= 1);
  const req = platform.received[0];
  assert.equal(req.method, "POST");
  assert.equal(req.url, "/api/v1/apps");
  assert.equal(req.authorization, "Bearer ek");
  assert.deepEqual(JSON.parse(req.body), { url: "http://localhost:3000/api/triggerlink" });

  await new Promise((r) => setTimeout(r, 50)); // 成功后不应再有重试
  assert.equal(platform.received.length, 1);
  platform.close();
});

test("register：失败后按 intervalMs 重试直至成功", async () => {
  let calls = 0;
  const platform = await fakePlatform(() => {
    calls++;
    return calls < 3
      ? { status: 502, body: '{"error":"introspect failed"}' }
      : { status: 200, body: "{}" };
  });
  const client = createClient({ id: "web", signingKey: "sk", eventKey: "ek", baseUrl: platform.url });

  client.register("http://localhost:3000/api/triggerlink", { intervalMs: 10, attempts: 5 });

  await waitFor(() => platform.received.length >= 3);
  assert.equal(platform.received.length, 3); // 两次失败 + 第三次成功，随后停止
  platform.close();
});

test("register：缺 eventKey / 缺 serveUrl 直接抛错", () => {
  const noKey = createClient({ id: "web", signingKey: "sk" });
  assert.throws(() => noKey.register("http://x/api/triggerlink"), /eventKey is required/);

  const client = createClient({ id: "web", signingKey: "sk", eventKey: "ek" });
  assert.throws(() => client.register(""), /serveUrl is required/);
});
