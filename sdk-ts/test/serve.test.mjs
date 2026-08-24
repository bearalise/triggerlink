// 端到端模拟平台行为：内省 + 多次回调推进 + memo 注入（协议第 3/4/5/6 节）。
import { test } from "node:test";
import assert from "node:assert/strict";
import { createClient, createFunction, serve } from "../dist/index.js";
import { sign } from "../dist/sign.js";

const KEY = "test-signing-key";
const client = createClient({ id: "web", signingKey: KEY });

function signedHeaders(body = new Uint8Array(0), now = new Date()) {
  return { "content-type": "application/json", "x-triggerlink-signature": sign(KEY, body, now) };
}

function execRequest(fnId, steps = {}, attempt = 1) {
  const payload = JSON.stringify({
    ctx: {
      run_id: "run_test1",
      function_id: fnId,
      attempt,
      event: { id: "evt_1", name: "doc/uploaded", data: { doc_id: "d1" }, ts: new Date().toISOString() },
      steps,
    },
  });
  const body = new TextEncoder().encode(payload);
  return new Request("http://localhost/api/triggerlink", {
    method: "POST",
    headers: signedHeaders(body),
    body,
  });
}

test("GET manifest: 未签名 401，签名后返回函数清单", async () => {
  const fn = createFunction({ id: "process-doc", event: "doc/uploaded", retries: 4 }, async () => ({}));
  const { GET } = serve({ client, functions: [fn] });

  const bad = await GET(new Request("http://localhost/api/triggerlink"));
  assert.equal(bad.status, 401);

  const ok = await GET(new Request("http://localhost/api/triggerlink", { headers: signedHeaders() }));
  assert.equal(ok.status, 200);
  const manifest = await ok.json();
  assert.equal(manifest.sdk, "triggerlink-ts/0.4.2");
  assert.equal(manifest.app_id, "web");
  assert.deepEqual(manifest.functions, [{ id: "process-doc", event: "doc/uploaded", retries: 4 }]);
});

test("POST 回调：三次回调推进两步函数，memo 命中不重跑", async () => {
  const executed = [];
  const fn = createFunction({ id: "process-doc", event: "doc/uploaded" }, async ({ event, step }) => {
    const parsed = await step.run("parse", async () => {
      executed.push("parse");
      return "parsed:" + event.data.doc_id;
    });
    const chunks = await step.run("chunk", async () => {
      executed.push("chunk");
      return 12;
    });
    return { parsed, chunks };
  });
  const { POST } = serve({ client, functions: [fn] });

  // 第 1 跳：无 memo → 执行 parse → StepComplete
  let resp = await POST(execRequest("process-doc", {}, 1));
  let op = await resp.json();
  assert.equal(resp.status, 200);
  assert.equal(op.op, "StepComplete");
  assert.equal(op.step_id, "parse");
  assert.equal(op.output, "parsed:d1");
  const parseHash = op.id;
  assert.deepEqual(executed, ["parse"]);

  // 第 2 跳：注入 parse memo → parse 不重跑，执行 chunk → StepComplete
  const steps = { [parseHash]: { id: "parse", status: "completed", output: "parsed:d1" } };
  resp = await POST(execRequest("process-doc", steps, 2));
  op = await resp.json();
  assert.equal(op.op, "StepComplete");
  assert.equal(op.step_id, "chunk");
  assert.equal(op.output, 12);
  assert.deepEqual(executed, ["parse", "chunk"]); // parse 未重跑

  // 第 3 跳：两个 memo 都注入 → 函数走到底 → RunComplete
  steps[op.id] = { id: "chunk", status: "completed", output: 12 };
  resp = await POST(execRequest("process-doc", steps, 3));
  op = await resp.json();
  assert.equal(op.op, "RunComplete");
  assert.deepEqual(op.output, { parsed: "parsed:d1", chunks: 12 });
  assert.deepEqual(executed, ["parse", "chunk"]); // 全程每个 step 只执行一次
});

test("POST 回调：step 抛错 → StepError 且携带 memo 键", async () => {
  const fn = createFunction({ id: "failing", event: "x/y" }, async ({ step }) => {
    await step.run("boom", async () => {
      throw new Error("third-party timeout");
    });
  });
  const { POST } = serve({ client, functions: [fn] });
  const op = await (await POST(execRequest("failing"))).json();
  assert.equal(op.op, "StepError");
  assert.equal(op.step_id, "boom");
  assert.equal(op.error.message, "third-party timeout");
  assert.ok(op.id); // 平台据此落失败 memo
});

test("POST 回调：sleep 挂起 → 唤醒 memo 命中后函数续跑", async () => {
  const executed = [];
  const fn = createFunction({ id: "sleeper", event: "x/y" }, async ({ step }) => {
    await step.run("before", async () => {
      executed.push("before");
      return "b";
    });
    await step.sleep("nap", 60_000);
    await step.run("after", async () => {
      executed.push("after");
      return "a";
    });
    return "done";
  });
  const { POST } = serve({ client, functions: [fn] });

  // 第 1 跳：执行 before → StepComplete
  let resp = await POST(execRequest("sleeper", {}, 1));
  let op = await resp.json();
  assert.equal(op.op, "StepComplete");
  const beforeHash = op.id;

  // 第 2 跳：before memo 注入 → 走到 sleep → Sleep opcode（携带 until）
  const steps = { [beforeHash]: { id: "before", status: "completed", output: "b" } };
  resp = await POST(execRequest("sleeper", steps, 2));
  op = await resp.json();
  assert.equal(op.op, "Sleep");
  assert.equal(op.step_id, "nap");
  assert.ok(op.id && op.until);
  assert.ok(new Date(op.until).getTime() > Date.now() + 50_000);
  const sleepHash = op.id;
  assert.deepEqual(executed, ["before"]); // sleep 之后的 step 未执行

  // 第 3 跳（唤醒）：平台已把 sleep memo 置 completed 注入 → 直接续跑 after → StepComplete
  steps[sleepHash] = { id: "nap", status: "completed" };
  resp = await POST(execRequest("sleeper", steps, 3));
  op = await resp.json();
  assert.equal(op.op, "StepComplete");
  assert.equal(op.step_id, "after");
  assert.deepEqual(executed, ["before", "after"]);
});

test("POST 回调：sendEvent 扇出 → memo 命中直接返回 IDs", async () => {
  const fn = createFunction({ id: "fanout", event: "x/y" }, async ({ step }) => {
    await step.run("before", () => "b");
    const ids = await step.sendEvent("notify", [
      { name: "notify/email", data: { to: "a@b.c" } },
      { id: "custom-1", name: "notify/sms" },
    ]);
    return { sent: ids };
  });
  const { POST } = serve({ client, functions: [fn] });

  // 第 1 跳：before → StepComplete
  let resp = await POST(execRequest("fanout", {}, 1));
  let op = await resp.json();
  const beforeHash = op.id;

  // 第 2 跳：走到 sendEvent → SendEvent opcode（携带事件列表）
  const steps = { [beforeHash]: { id: "before", status: "completed", output: "b" } };
  resp = await POST(execRequest("fanout", steps, 2));
  op = await resp.json();
  assert.equal(op.op, "SendEvent");
  assert.equal(op.step_id, "notify");
  assert.equal(op.events.length, 2);
  assert.equal(op.events[0].name, "notify/email");
  assert.equal(op.events[1].id, "custom-1");
  const sendHash = op.id;

  // 第 3 跳：平台已发事件并写入 memo（output = IDs）→ 函数走到底
  steps[sendHash] = { id: "notify", status: "completed", output: ["evt_a", "custom-1"] };
  resp = await POST(execRequest("fanout", steps, 3));
  op = await resp.json();
  assert.equal(op.op, "RunComplete");
  assert.deepEqual(op.output, { sent: ["evt_a", "custom-1"] });
});

test("POST 回调：handler 未捕获异常 → RunError", async () => {
  const fn = createFunction({ id: "panics", event: "x/y" }, async () => {
    throw new Error("bug outside step");
  });
  const { POST } = serve({ client, functions: [fn] });
  const op = await (await POST(execRequest("panics"))).json();
  assert.equal(op.op, "RunError");
  assert.equal(op.error.message, "bug outside step");
});

test("POST 回调：未知函数 404，过期签名 401", async () => {
  const fn = createFunction({ id: "a", event: "x/y" }, async () => ({}));
  const { POST } = serve({ client, functions: [fn] });

  assert.equal((await POST(execRequest("ghost"))).status, 404);

  const stale = new Date(Date.now() - 10 * 60 * 1000); // 10 分钟前，超出 ±5 分钟容差
  const body = new TextEncoder().encode(JSON.stringify({ ctx: {} }));
  const req = new Request("http://localhost/api/triggerlink", {
    method: "POST",
    headers: signedHeaders(body, stale),
    body,
  });
  assert.equal((await POST(req)).status, 401);
});
