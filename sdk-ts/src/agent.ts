// Agent 原语：把单 Agent 的 LLM/tool 循环分解为 durable step（设计文档 docs/agent-design.md）。
// 每次 LLM 调用与每次工具执行各自 memo 化；崩溃恢复时从 memo 重建对话历史，只重跑失败的调用。
// 本模块只通过子路径 @triggerlink/sdk/agent 导出——ai/zod 是 optional peer 依赖，
// 主入口 index.ts 不得 import 本模块（§8.1），否则未装 ai 的普通用户会在 import 时崩溃。
import {
  generateText,
  tool,
  type JSONValue,
  type LanguageModel,
  type ModelMessage,
  type ToolSet,
} from "ai";
import type { ZodType } from "zod";
import type { StepTool } from "./step.js";

// 内置 provider，开箱即用：默认实例从环境变量读 API key
// （ANTHROPIC_API_KEY / OPENAI_API_KEY / DEEPSEEK_API_KEY）；
// 需要自定义 baseURL/apiKey/代理时用 createXxx 构造专属实例。
// 其余 provider 不受影响：createAgent 的 model 接受任意 AI SDK LanguageModel。
export { anthropic, createAnthropic } from "@ai-sdk/anthropic";
export { openai, createOpenAI } from "@ai-sdk/openai";
export { deepseek, createDeepSeek } from "@ai-sdk/deepseek";

/** Agent 工具定义。parameters 为 zod schema；handler 入参是 schema parse 后的值。 */
export interface AgentTool<P = unknown, R = unknown> {
  description: string;
  parameters: ZodType<P>;
  handler: (params: P) => Promise<R> | R;
}

/**
 * 定义一个 Agent 工具（泛型工厂）：让 zod schema 的类型流到 handler 入参。
 * 与直接写字面量等价，但获得完整的类型推断；跨 Agent 复用工具时也应使用它。
 */
export function createTool<P, R>(def: AgentTool<P, R>): AgentTool<P, R> {
  if (!def.description) throw new Error("createTool: description is required");
  if (!def.parameters) throw new Error("createTool: parameters is required");
  if (typeof def.handler !== "function") throw new Error("createTool: handler is required");
  return def;
}

/** redact 钩子的上下文（§5.7）。 */
export interface RedactCtx {
  /** 产生输出的 step 类型 */
  kind: "llm" | "tool";
  /** Agent 循环迭代号（0 起） */
  iteration: number;
  /** kind === "tool" 时的工具名 */
  toolName?: string;
}

/**
 * 输出脱敏钩子：在 step.run 内部、输出被持久化之前调用。
 * 必须 replay-safe 且确定（同输入同输出）——memo 是恢复时重建对话历史的唯一来源，
 * 删改的字段会原样出现在恢复后的后续 LLM 调用里。llm 输出的结构（text/toolCalls/
 * responseMessages）不得破坏，否则抛错（见 assertLlmMemoShape）。
 */
export type RedactHook = (output: unknown, ctx: RedactCtx) => unknown;

export interface AgentOpts {
  /** 稳定标识，用作 memo 键前缀（agent/<name>/...）；同一函数内不同 Agent 必须不同名（§5.2） */
  name: string;
  /** AI SDK 的 LanguageModel（用户自带 provider 包，如 @ai-sdk/anthropic） */
  model: LanguageModel;
  system?: string;
  // eslint 友好起见用 any：具体 P/R 由 createTool 在定义处完成推断，此处只承载运行时分发
  tools?: Record<string, AgentTool<any, any>>;
  /** 迭代上限（一次迭代 = 一次 LLM 调用 + 其全部工具执行），默认 10；超限抛错使 run 失败 */
  maxIterations?: number;
  redact?: RedactHook;
}

export interface AgentResult {
  /** 最终一条 assistant 文本 */
  text: string;
  /** 实际执行的迭代次数 */
  iterations: number;
  /** 各迭代 token 用量合计（memo 命中时也照常累计） */
  usage: { inputTokens: number; outputTokens: number };
  /**
   * 本次运行的全部工具执行记录（按执行顺序，含结构化输出）。
   * 覆盖 AgentKit 的 "done 工具写 state.kv" 模式：函数代码从这里的 output 读取
   * 工具产出，无需解析最终文本。恢复重放时 memo 命中路径同样重建该列表。
   */
  toolCalls: AgentToolCallRecord[];
  /**
   * 完整对话历史（user 输入 + 各轮 assistant 消息 + 工具结果），字段名与 AgentKit
   * 的 result.output 对齐——可自行 findLastIndex 等遍历（元素含 role/content）。
   * 恢复重放时由 memo 原样重建；启用 redact 时历史内容即脱敏后内容。
   */
  output: ModelMessage[];
}

/** 取最后一条 assistant 消息的文本内容（拼接所有 text part）；没有则返回 undefined。 */
export function lastAssistantTextMessageContent(result: AgentResult): string | undefined {
  // 不用 findLast:tsconfig lib 为 ES2022
  for (let i = result.output.length - 1; i >= 0; i--) {
    const msg = result.output[i];
    if (msg.role !== "assistant") continue;
    if (typeof msg.content === "string") return msg.content;
    return msg.content
      .filter((p) => p.type === "text")
      .map((p) => p.text)
      .join("");
  }
  return undefined;
}

/** 一次工具执行的记录。 */
export interface AgentToolCallRecord {
  toolCallId: string;
  toolName: string;
  input: unknown;
  /** 工具的返回值（redact 钩子启用时为脱敏后的值——与模型所见一致） */
  output: unknown;
}

export interface Agent {
  readonly name: string;
  run(step: StepTool, input: string): Promise<AgentResult>;
}

/** llm step 的 memo 输出：恢复重建历史所需的全部信息。 */
interface LlmMemo {
  text: string;
  toolCalls: Array<{ toolCallId: string; toolName: string; input: unknown }>;
  /** 该迭代的 assistant 消息（含 tool-call parts），恢复时原样喂回模型 */
  responseMessages: ModelMessage[];
  usage: { inputTokens?: number; outputTokens?: number };
}

/** redact 钩子可能破坏 llm memo 结构；此处 fail loud，不让坏 memo 落库或参与历史重建（§5.7）。 */
function assertLlmMemoShape(m: unknown, name: string): asserts m is LlmMemo {
  const o = m as LlmMemo | null;
  if (
    !o ||
    typeof o.text !== "string" ||
    !Array.isArray(o.toolCalls) ||
    !Array.isArray(o.responseMessages) ||
    !o.usage ||
    typeof o.usage !== "object"
  ) {
    throw new Error(
      `agent "${name}": llm step memo has a damaged structure (must keep text/toolCalls/responseMessages/usage) — check the redact hook`,
    );
  }
}

// 同一函数调用（每次回调都是一个新 StepTool 实例）内 agent 名 → 实例的登记簿，
// 用于拒绝两个不同 Agent 共用 name 造成的 memo 键前缀冲突（§5.2）。
// 同一个 Agent 实例多次 run（如循环里）是合法的：ExecCtx 序号机制保证 memo 键确定。
const claimedNames = new WeakMap<StepTool, Map<string, Agent>>();

/** 定义一个 Agent：system prompt + tools + 模型，循环直到模型不再调用工具。 */
export function createAgent(opts: AgentOpts): Agent {
  if (!opts.name || opts.name.includes("/")) {
    throw new Error('createAgent: name is required and must not contain "/"');
  }
  if (!opts.model) throw new Error("createAgent: model is required");
  const maxIterations = opts.maxIterations ?? 10;
  if (!Number.isInteger(maxIterations) || maxIterations < 1) {
    throw new Error("createAgent: maxIterations must be a positive integer");
  }

  const toolDefs = opts.tools ?? {};
  // 以 schema-only 方式把工具交给 AI SDK（不传 execute）：
  // 模型的 tool call 原样返回不执行，"决策"与"执行"之间就是我们的 step 边界（§4.1）。
  const aiTools: ToolSet = {};
  for (const [toolName, def] of Object.entries(toolDefs)) {
    aiTools[toolName] = tool({ description: def.description, inputSchema: def.parameters });
  }

  const agent: Agent = {
    name: opts.name,

    async run(step: StepTool, input: string): Promise<AgentResult> {
      let registry = claimedNames.get(step);
      if (!registry) {
        registry = new Map();
        claimedNames.set(step, registry);
      }
      const claimed = registry.get(agent.name);
      if (claimed && claimed !== agent) {
        throw new Error(
          `agent "${agent.name}": another agent with the same name already ran in this function; names must be unique per function (memo key prefix collision)`,
        );
      }
      registry.set(agent.name, agent);

      const redact = opts.redact;
      const llmStepId = `agent/${agent.name}/llm`;
      const toolStepId = `agent/${agent.name}/tool`;
      const messages: ModelMessage[] = [{ role: "user", content: input }];
      const usage = { inputTokens: 0, outputTokens: 0 };
      const toolCalls: AgentToolCallRecord[] = [];

      for (let i = 0; ; i++) {
        if (i >= maxIterations) {
          throw new Error(`agent "${agent.name}": maxIterations (${maxIterations}) exceeded`);
        }

        const llmMemo = await step.run(llmStepId, async () => {
          const res = await generateText({
            model: opts.model,
            system: opts.system,
            messages,
            tools: aiTools,
          });
          const memo: LlmMemo = {
            text: res.text,
            toolCalls: res.toolCalls.map((c) => ({
              toolCallId: c.toolCallId,
              toolName: c.toolName,
              input: c.input,
            })),
            responseMessages: res.responseMessages,
            usage: { inputTokens: res.usage.inputTokens, outputTokens: res.usage.outputTokens },
          };
          const out = redact ? redact(memo, { kind: "llm", iteration: i }) : memo;
          assertLlmMemoShape(out, agent.name); // 落库前拦截坏结构
          return out;
        });
        assertLlmMemoShape(llmMemo, agent.name); // memo 命中路径同样校验（防御脏数据）

        usage.inputTokens += llmMemo.usage.inputTokens ?? 0;
        usage.outputTokens += llmMemo.usage.outputTokens ?? 0;
        messages.push(...llmMemo.responseMessages);

        if (llmMemo.toolCalls.length === 0) {
          return { text: llmMemo.text, iterations: i + 1, usage, toolCalls, output: messages };
        }

        // 并行 tool call 顺序执行（数组序），每个一个 durable step（§5.3）
        for (const call of llmMemo.toolCalls) {
          const def = toolDefs[call.toolName];
          if (!def) {
            throw new Error(`agent "${agent.name}": model called unknown tool "${call.toolName}"`);
          }
          const output = await step.run(toolStepId, async () => {
            const out: unknown = await def.handler(def.parameters.parse(call.input));
            return redact
              ? redact(out, { kind: "tool", iteration: i, toolName: call.toolName })
              : out;
          });
          // memo 命中时 step.run 直接返回缓存值,重放路径同样补全记录
          toolCalls.push({ toolCallId: call.toolCallId, toolName: call.toolName, input: call.input, output });
          const toolMsg: ModelMessage = {
            role: "tool",
            content: [
              {
                type: "tool-result",
                toolCallId: call.toolCallId,
                toolName: call.toolName,
                output: { type: "json", value: (output ?? null) as JSONValue },
              },
            ],
          };
          messages.push(toolMsg);
        }
      }
    },
  };

  return agent;
}
