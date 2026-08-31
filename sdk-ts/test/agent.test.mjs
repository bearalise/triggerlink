// Agent 原语测试（docs/agent-design.md §7）：
// 用 ai/test 的 MockLanguageModelV4 驱动循环，并用一个模拟平台重入语义的 driver
// （捕获 StepInterrupt → 记录 memo → 带 memo 重新构造 ExecCtx 重入）验证 durability。
import { test } from "node:test";
import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import { MockLanguageModelV4 } from "ai/test";
import { z } from "zod";
import { createAgent, createTool, lastAssistantTextMessageContent } from "../dist/agent.js";
import { StepInterrupt } from "../dist/execx.js";
import { ExecCtx } from "../dist/execx.js";
import { createStepTool } from "../dist/step.js";

const FN_ID = "fn-test";

function usage(inp, out) {
  return {
    inputTokens: { total: inp, noCache: inp, cacheRead: 0, cacheWrite: 0 },
    outputTokens: { total: out, text: out, reasoning: 0 },
  };
}

function textResult(text, inp = 10, out = 5) {
  return {
    content: [{ type: "text", text }],
    finishReason: { unified: "stop", raw: undefined },
    usage: usage(inp, out),
    warnings: [],
  };
}

function toolCallsResult(calls, inp = 10, out = 5) {
  return {
    content: calls.map((c) => ({
      type: "tool-call",
      toolCallId: c.id,
      toolName: c.name,
      input: JSON.stringify(c.input),
    })),
    finishReason: { unified: "tool-calls", raw: undefined },
    usage: usage(inp, out),
    warnings: [],
  };
}

/**
 * 模拟平台重入：反复执行 agent.run，StepComplete → 记 memo 重入；
 * 函数正常返回 → 完成；StepError / 其他错误 → 抛出。
 * 返回最终结果与全部 memo（steps 以 step_hash 为键，与平台一致）。
 */
async function drive(agent, input, initialSteps = {}) {
  const steps = { ...initialSteps };
  for (;;) {
    const ec = new ExecCtx(FN_ID, steps);
    try {
      const result = await agent.run(createStepTool(ec), input);
      return { result, steps };
    } catch (err) {
      if (err instanceof StepInterrupt && err.opcode.op === "StepComplete") {
        steps[err.opcode.id] = {
          id: err.opcode.step_id,
          status: "completed",
          output: err.opcode.output,
        };
        continue;
      }
      if (err instanceof StepInterrupt && err.opcode.op === "StepError") {
        throw new Error("StepError: " + err.opcode.error.message);
      }
      throw err;
    }
  }
}

const searchTool = (handler) => ({
  description: "Search the knowledge base",
  parameters: z.object({ query: z.string() }),
  handler,
});

test("text-only 完成：一次 LLM 调用直接返回文本", async () => {
  const model = new MockLanguageModelV4({ doGenerate: textResult("hello") });
  const agent = createAgent({ name: "a1", model });

  const { result } = await drive(agent, "hi");
  assert.equal(result.text, "hello");
  assert.equal(result.iterations, 1);
  assert.deepEqual(result.usage, { inputTokens: 10, outputTokens: 5 });
  assert.equal(model.doGenerateCalls.length, 1);
});

test("工具调用流程：llm → tool → llm，各自 memo 化", async () => {
  const model = new MockLanguageModelV4({
    doGenerate: [
      toolCallsResult([{ id: "call_1", name: "search", input: { query: "triggerlink" } }]),
      textResult("answer", 20, 8),
    ],
  });
  const seen = [];
  const agent = createAgent({
    name: "a2",
    model,
    tools: {
      search: searchTool(({ query }) => {
        seen.push(query);
        return { hits: 3 };
      }),
    },
  });

  const { result, steps } = await drive(agent, "question");
  assert.deepEqual(seen, ["triggerlink"]);
  assert.equal(result.text, "answer");
  assert.equal(result.iterations, 2);
  assert.deepEqual(result.usage, { inputTokens: 30, outputTokens: 13 });
  // 工具执行记录:函数代码可直接拿到结构化输出(done 工具模式)
  assert.deepEqual(result.toolCalls, [
    { toolCallId: "call_1", toolName: "search", input: { query: "triggerlink" }, output: { hits: 3 } },
  ]);
  // 完整消息历史(result.output,与 AgentKit 对齐):user → assistant(tool-call) → tool(result) → assistant(text)
  assert.deepEqual(result.output.map((m) => m.role), ["user", "assistant", "tool", "assistant"]);
  assert.equal(result.output.findLastIndex((m) => m.role === "assistant"), 3);
  assert.equal(lastAssistantTextMessageContent(result), "answer");
  assert.equal(model.doGenerateCalls.length, 2);

  // 三个 memo：两次 LLM（agent/a2）+ 一次工具（agent/a2/search）
  const memos = Object.values(steps);
  assert.deepEqual(
    memos.map((m) => m.id).sort(),
    [`agent/a2`, `agent/a2`, `agent/a2/search`].sort(),
  );
  // 第二次 LLM 调用收到了工具结果
  const secondCall = model.doGenerateCalls[1];
  const toolMsg = secondCall.prompt.findLast((m) => m.role === "tool");
  assert.equal(toolMsg.content[0].toolName, "search");
  assert.deepEqual(toolMsg.content[0].output, { type: "json", value: { hits: 3 } });
});

test("一次响应多个 tool call：按数组序逐个执行", async () => {
  const model = new MockLanguageModelV4({
    doGenerate: [
      toolCallsResult([
        { id: "c1", name: "search", input: { query: "first" } },
        { id: "c2", name: "search", input: { query: "second" } },
      ]),
      textResult("done"),
    ],
  });
  const seen = [];
  const agent = createAgent({
    name: "a3",
    model,
    tools: { search: searchTool(({ query }) => (seen.push(query), query)) },
  });

  const { result } = await drive(agent, "q");
  assert.deepEqual(seen, ["first", "second"]);
  assert.equal(result.text, "done");
});

test("maxIterations 超限：抛错使 run 失败", async () => {
  const model = new MockLanguageModelV4({
    doGenerate: toolCallsResult([{ id: "c1", name: "search", input: { query: "x" } }]),
  });
  const agent = createAgent({
    name: "a4",
    model,
    maxIterations: 1,
    tools: { search: searchTool(() => "r") },
  });

  await assert.rejects(drive(agent, "q"), /maxIterations \(1\) exceeded/);
});

test("redact 钩子：持久化前改写输出，恢复与后续调用所见即所存", async () => {
  const model = new MockLanguageModelV4({
    doGenerate: [
      toolCallsResult([{ id: "c1", name: "search", input: { query: "x" } }]),
      textResult("final"),
    ],
  });
  const agent = createAgent({
    name: "a5",
    model,
    tools: { search: searchTool(() => ({ secret: "s3cr3t", hits: 3 })) },
    redact: (output, ctx) => {
      if (ctx.kind === "tool") {
        const { secret: _dropped, ...rest } = output;
        return rest;
      }
      return output;
    },
  });

  const { result, steps } = await drive(agent, "q");
  assert.equal(result.text, "final");
  const toolMemo = Object.values(steps).find((m) => m.id === "agent/a5/search");
  assert.deepEqual(toolMemo.output, { hits: 3 }); // secret 未落库
  // 后续 LLM 调用看到的也是脱敏后的结果
  const toolMsg = model.doGenerateCalls[1].prompt.findLast((m) => m.role === "tool");
  assert.deepEqual(toolMsg.content[0].output, { type: "json", value: { hits: 3 } });
});

test("redact 钩子破坏 llm memo 结构：fail loud，坏 memo 不落库", async () => {
  const model = new MockLanguageModelV4({ doGenerate: textResult("x") });
  const agent = createAgent({
    name: "a6",
    model,
    redact: (output, ctx) => (ctx.kind === "llm" ? { text: "only-text" } : output),
  });

  await assert.rejects(drive(agent, "q"), /damaged structure/);
});

test("replay：预置 llm:0/tool:0 memo，恢复只重跑第二次 LLM 调用", async () => {
  const hash = (stepId, seq) =>
    createHash("sha256").update(`${FN_ID}:${stepId}:${seq}`).digest("hex");

  const preseeded = {
    [hash("agent/a7", 0)]: {
      id: "agent/a7",
      status: "completed",
      output: {
        text: "",
        toolCalls: [{ toolCallId: "c1", toolName: "search", input: { query: "x" } }],
        responseMessages: [
          {
            role: "assistant",
            content: [
              { type: "tool-call", toolCallId: "c1", toolName: "search", input: { query: "x" } },
            ],
          },
        ],
        usage: { inputTokens: 10, outputTokens: 5 },
      },
    },
    [hash("agent/a7/search", 0)]: {
      id: "agent/a7/search",
      status: "completed",
      output: { hits: 3 },
    },
  };

  const model = new MockLanguageModelV4({ doGenerate: textResult("resumed answer", 20, 8) });
  const agent = createAgent({
    name: "a7",
    model,
    tools: {
      search: searchTool(() => {
        throw new Error("tool handler must not re-run on replay");
      }),
    },
  });

  const { result } = await drive(agent, "q", preseeded);
  assert.equal(result.text, "resumed answer");
  assert.equal(result.iterations, 2);
  // 重放路径:memo 命中的工具执行也补全进 toolCalls 记录
  assert.deepEqual(result.toolCalls, [
    { toolCallId: "c1", toolName: "search", input: { query: "x" }, output: { hits: 3 } },
  ]);
  // 模型只被调一次（第二次 llm step）；第一次命中 memo
  assert.equal(model.doGenerateCalls.length, 1);
  // 恢复重建的历史里包含 memo 中的 assistant tool-call 与 tool 结果
  const prompt = model.doGenerateCalls[0].prompt;
  assert.equal(prompt.at(-2).role, "assistant");
  assert.equal(prompt.at(-1).role, "tool");
  // 用量含 memo 命中部分
  assert.deepEqual(result.usage, { inputTokens: 30, outputTokens: 13 });
});

test("同一函数内两个不同 Agent 同名：拒绝（memo 键前缀冲突）", async () => {
  const model = new MockLanguageModelV4({ doGenerate: textResult("x") });
  const first = createAgent({ name: "dup", model });
  const second = createAgent({ name: "dup", model });

  const ec = new ExecCtx(FN_ID, {});
  const step = createStepTool(ec);
  // 第一个 agent 在首个 llm step 中断；用 memo 让它走完，再跑第二个同名 agent
  await assert.rejects(first.run(step, "q"), (err) => err instanceof StepInterrupt);
  // 模拟平台推进后重入：同一 StepTool 上跑另一个同名 agent
  await assert.rejects(second.run(step, "q"), /same name already ran/);
});

test("createAgent 参数校验", () => {
  const model = new MockLanguageModelV4();
  assert.throws(() => createAgent({ name: "", model }), /name is required/);
  assert.throws(() => createAgent({ name: "a/b", model }), /must not contain/);
  assert.throws(() => createAgent({ name: "ok", model, maxIterations: 0 }), /positive integer/);
});

test("createTool：工厂定义的工具与字面量等价，且可跨 Agent 复用", async () => {
  const shared = createTool({
    description: "Search the knowledge base",
    parameters: z.object({ query: z.string() }),
    handler: ({ query }) => `result:${query}`,
  });
  assert.throws(() => createTool({ description: "", parameters: z.object({}), handler: () => 1 }), /description/);

  const model = new MockLanguageModelV4({
    doGenerate: [
      toolCallsResult([{ id: "c1", name: "search", input: { query: "x" } }]),
      textResult("done"),
    ],
  });
  const agent = createAgent({ name: "ct", model, tools: { search: shared } });
  const { result } = await drive(agent, "q");
  assert.equal(result.text, "done");
  const toolMsg = model.doGenerateCalls[1].prompt.findLast((m) => m.role === "tool");
  assert.deepEqual(toolMsg.content[0].output, { type: "json", value: "result:x" });
});

test("内置 provider：anthropic/openai/deepseek/zai 开箱构造 LanguageModel", async () => {
  const { anthropic, openai, deepseek, createDeepSeek, zai, createZhipu } = await import("../dist/agent.js");
  const a = anthropic("claude-sonnet-4-5");
  assert.equal(a.provider, "anthropic.messages");
  assert.equal(a.modelId, "claude-sonnet-4-5");
  assert.equal(openai("gpt-4o").provider, "openai.responses");
  assert.equal(deepseek("deepseek-v4-flash").provider, "deepseek.chat");
  assert.equal(zai("glm-5").provider, "zhipu.chat");
  // createXxx 支持自定义配置（如 baseURL / apiKey）
  const custom = createDeepSeek({ apiKey: "k", baseURL: "https://example.com/v1" });
  assert.equal(custom("deepseek-v4-flash").modelId, "deepseek-v4-flash");
  const customZai = createZhipu({ apiKey: "k", baseURL: "https://api.z.ai/api/paas/v4" });
  assert.equal(customZai("glm-5").modelId, "glm-5");
});

test("lifecycle.onResponse:每次真实 LLM 调用触发一次,result 形状正确", async () => {
  const model = new MockLanguageModelV4({
    doGenerate: [
      toolCallsResult([{ id: "c1", name: "search", input: { query: "x" } }]),
      textResult("final answer", 20, 8),
    ],
  });
  const seen = [];
  const agent = createAgent({
    name: "lc1",
    model,
    tools: { search: searchTool(() => "r") },
    lifecycle: {
      onResponse: ({ result, iteration }) => {
        seen.push({ iteration, text: result.text, viaHelper: lastAssistantTextMessageContent(result) });
      },
    },
  });

  const { result } = await drive(agent, "q");
  assert.equal(result.text, "final answer");
  assert.equal(seen.length, 2);
  // 第一轮:text 为空、有 tool call;helper 能消费 result.output
  assert.equal(seen[0].iteration, 0);
  assert.equal(seen[0].viaHelper, ""); // tool-call-only 轮次 text 为空串
  // 第二轮:最终文本
  assert.equal(seen[1].iteration, 1);
  assert.equal(seen[1].text, "final answer");
  assert.equal(seen[1].viaHelper, "final answer");
});

test("lifecycle.onResponse:memo 命中的恢复重放不触发", async () => {
  const hash = (stepId, seq) =>
    createHash("sha256").update(`${FN_ID}:${stepId}:${seq}`).digest("hex");
  const preseeded = {
    [hash("agent/lc2", 0)]: {
      id: "agent/lc2",
      status: "completed",
      output: {
        text: "",
        toolCalls: [{ toolCallId: "c1", toolName: "search", input: { query: "x" } }],
        responseMessages: [
          { role: "assistant", content: [{ type: "tool-call", toolCallId: "c1", toolName: "search", input: { query: "x" } }] },
        ],
        usage: { inputTokens: 10, outputTokens: 5 },
      },
    },
    [hash("agent/lc2/search", 0)]: { id: "agent/lc2/search", status: "completed", output: "r" },
  };
  let fired = 0;
  const model = new MockLanguageModelV4({ doGenerate: textResult("done") });
  const agent = createAgent({
    name: "lc2",
    model,
    tools: { search: searchTool(() => "r") },
    lifecycle: { onResponse: () => fired++ },
  });

  await drive(agent, "q", preseeded);
  assert.equal(fired, 1); // 只有第二次真实 LLM 调用触发;第一轮 memo 命中不触发
});

test("lifecycle.onResponse 抛错:视同 step 失败", async () => {
  const model = new MockLanguageModelV4({ doGenerate: textResult("x") });
  const agent = createAgent({
    name: "lc3",
    model,
    lifecycle: {
      onResponse: () => {
        throw new Error("hook boom");
      },
    },
  });
  await assert.rejects(drive(agent, "q"), /StepError: hook boom/);
});
