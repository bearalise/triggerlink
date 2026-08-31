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
  assert.equal(manifest.sdk, "triggerlink-ts/0.4.6");
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

test("POST 回调：waitForEvent 挂起 → opcode 字段逐一断言（timeout 毫秒转 Go duration）", async () => {
  const fn = createFunction({ id: "waiter", event: "x/y" }, async ({ step }) => {
    const ev = await step.waitForEvent("wait-payment", {
      event: "order/payed",
      match: "data.order_id == event.data.order_id",
      timeout: 1500,
    });
    return { got: ev };
  });
  const { POST } = serve({ client, functions: [fn] });

  const op = await (await POST(execRequest("waiter"))).json();
  assert.equal(op.op, "WaitForEvent");
  assert.equal(op.step_id, "wait-payment");
  assert.ok(op.id); // step_hash
  assert.equal(op.event, "order/payed");
  assert.equal(op.match, "data.order_id == event.data.order_id");
  assert.equal(op.timeout, "1.5s"); // 1500ms → "1.5s"
});

test("POST 回调：waitForEvent timeout 字符串原样透传，可选字段缺省不出现", async () => {
  const fn = createFunction({ id: "waiter2", event: "x/y" }, async ({ step }) => {
    await step.waitForEvent("wait", { event: "order/payed", timeout: "168h" });
  });
  const { POST } = serve({ client, functions: [fn] });

  const op = await (await POST(execRequest("waiter2"))).json();
  assert.equal(op.op, "WaitForEvent");
  assert.equal(op.timeout, "168h");
  assert.ok(!("match" in op)); // undefined 被 JSON 序列化省略

  const fn2 = createFunction({ id: "waiter3", event: "x/y" }, async ({ step }) => {
    await step.waitForEvent("wait", { event: "order/payed" });
  });
  const { POST: POST2 } = serve({ client, functions: [fn2] });
  const op2 = await (await POST2(execRequest("waiter3"))).json();
  assert.equal(op2.op, "WaitForEvent");
  assert.ok(!("timeout" in op2) && !("match" in op2));
});

test("POST 回调：waitForEvent memo 命中返回事件；output=null 返回 null（超时分支）", async () => {
  const fn = createFunction({ id: "resumer", event: "x/y" }, async ({ step }) => {
    const ev = await step.waitForEvent("wait-payment", { event: "order/payed" });
    return { got: ev };
  });
  const { POST } = serve({ client, functions: [fn] });

  // 第 1 跳：拿 step_hash
  let op = await (await POST(execRequest("resumer"))).json();
  assert.equal(op.op, "WaitForEvent");
  const h = op.id;

  // 事件命中：memo output = 事件 JSON → 返回事件对象
  const arrived = { id: "evt_pay", name: "order/payed", data: { order_id: "o1" }, ts: new Date().toISOString() };
  let steps = { [h]: { id: "wait-payment", status: "completed", output: arrived } };
  op = await (await POST(execRequest("resumer", steps, 2))).json();
  assert.equal(op.op, "RunComplete");
  assert.deepEqual(op.output, { got: arrived });

  // 超时：memo output = JSON null → 返回 null
  steps = { [h]: { id: "wait-payment", status: "completed", output: null } };
  op = await (await POST(execRequest("resumer", steps, 3))).json();
  assert.equal(op.op, "RunComplete");
  assert.deepEqual(op.output, { got: null });
});

test("GET manifest：cancelOn 序列化为 cancel_on，无配置则不出现", async () => {
  const fn = createFunction(
    {
      id: "onboarding",
      event: "user/signup",
      cancelOn: [
        { event: "user/deleted", match: "data.user_id == event.data.user_id" },
        { event: "user/opted-out" },
      ],
    },
    async () => ({}),
  );
  const plain = createFunction({ id: "plain", event: "x/y" }, async () => ({}));
  const { GET } = serve({ client, functions: [fn, plain] });

  const manifest = await (await GET(new Request("http://localhost/api/triggerlink", { headers: signedHeaders() }))).json();
  const onboarding = manifest.functions.find((f) => f.id === "onboarding");
  assert.deepEqual(onboarding.cancel_on, [
    { event: "user/deleted", match: "data.user_id == event.data.user_id" },
    { event: "user/opted-out" }, // match 缺省被省略
  ]);
  const plainFn = manifest.functions.find((f) => f.id === "plain");
  assert.ok(!("cancel_on" in plainFn));
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
