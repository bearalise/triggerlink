// 函数定义与 handler 上下文类型（协议第 4 节）。
import type { StepTool } from "./step.js";

/** 触发函数的事件（与平台 eventPayload 同构）。 */
export interface EventPayload<T = unknown> {
  id: string;
  name: string;
  data: T;
  ts: string; // RFC3339
}

/** 传给用户 handler 的上下文。 */
export interface HandlerContext<T = unknown> {
  event: EventPayload<T>;
  step: StepTool;
  runId: string;
  attempt: number;
}

export interface FunctionOpts {
  /** 稳定标识，改名会丢历史 memo 关联 */
  id: string;
  /** 订阅的事件名 */
  event: string;
  /** 重试上限；0/缺省 = 平台默认（4） */
  retries?: number;
}

export interface TriggerFunction<T = unknown> {
  readonly opts: Required<FunctionOpts>;
  readonly handler: (ctx: HandlerContext<T>) => Promise<unknown>;
}

/** 定义一个 durable 函数。副作用必须放进 step.run（协议第 6 节约束）。 */
export function createFunction<T = unknown>(
  opts: FunctionOpts,
  handler: (ctx: HandlerContext<T>) => Promise<unknown>,
): TriggerFunction<T> {
  if (!opts.id) throw new Error("createFunction: id is required");
  if (!opts.event) throw new Error("createFunction: event is required");
  return { opts: { retries: 0, ...opts }, handler };
}
