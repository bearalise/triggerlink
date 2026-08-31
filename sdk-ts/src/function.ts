// 函数定义与 handler 上下文类型（协议第 4 节）。
import type { StepTool } from "./step.js";

/** 触发函数的事件（与平台 eventPayload 同构）。 */
export interface EventPayload<T = unknown> {
  id: string;
  name: string;
  data: T;
  ts: string; // RFC3339
}

/** step.waitForEvent 命中后返回的事件（与 EventPayload 同构）。 */
export type TriggerLinkEvent<T = unknown> = EventPayload<T>;

/** cancelOn 取消规则（FR-4.9）：event 到达且 match 命中时，平台取消该函数的在途 run。 */
export interface CancelOnRule {
  /** 触发取消的事件名 */
  event: string;
  /** 可选 expr 表达式，留空 = 该事件到达即取消；环境：data=到达事件 data，event=本 run 触发事件 {name, data} */
  match?: string;
}

/** 传给用户 handler 的上下文。 */
export interface HandlerContext<T = unknown> {
  event: EventPayload<T>;
  step: StepTool;
  runId: string;
  attempt: number;
}

/** triggerlink/run.failed 内部事件的 data（FR-2.11），即 onFailure handler 的 event.data。 */
export interface RunFailedEventData {
  run_id: string;
  function_id: string;
  error: string;
  /** 触发该 run 的原始事件 */
  event: EventPayload;
}

/** 防抖配置（FR-4.5）。时长可写 Go duration 字符串（"5m"）或毫秒数（300000）。 */
export interface DebounceOpts {
  /** 合并窗口：窗口内每来一个新事件就把触发时间推后一个 period */
  period: string | number;
  /** 可选 expr 表达式（环境只含 data），按其求值结果分组；留空 = 整个函数共用一个窗口 */
  key?: string;
  /** 距首个事件的最长延迟，给持续刷新的窗口封顶；缺省 = 不封顶 */
  timeout?: string | number;
}

/** 限流配置（FR-4.4）。超限的 run 不丢弃，延迟到下一窗口。 */
export interface ThrottleOpts {
  /** 每个窗口内允许启动的 run 数 */
  limit: number;
  /** 窗口长度 */
  period: string | number;
  /** 可选 expr 表达式（环境只含 data），按其求值结果分组 */
  key?: string;
}

/** 批处理配置（FR-4.7）。攒够 maxSize 或距首条超过 timeout 即以事件数组触发一个 run。 */
export interface BatchOpts {
  /** 单批最大事件数，随 manifest 上报为 max_size */
  maxSize: number;
  /** 距首条事件的最长等待；攒不满时由它兜底 flush */
  timeout: string | number;
  /** 可选 expr 表达式（环境只含 data），按其求值结果分组 */
  key?: string;
}

export interface FunctionOpts {
  /** 稳定标识，改名会丢历史 memo 关联 */
  id: string;
  /** 订阅的事件名；纯 cron 函数可省略（event/cron 至少其一） */
  event?: string;
  /** 事件触发 match 表达式（FR-3.1），expr 语法，环境只含 data（= 事件 data） */
  match?: string;
  /** 标准 5 字段 cron（分 时 日 月 周），UTC */
  cron?: string;
  /** 重试上限；0/缺省 = 平台默认（4） */
  retries?: number;
  /** 取消规则，随 manifest 上报为 cancel_on */
  cancelOn?: CancelOnRule[];
  /**
   * run 级超时（FR-4.3），随 manifest 上报为 timeout（Go duration 字符串或毫秒数，
   * 如 "5m" 或 300000）。run 从创建起超过该时长仍在 Queued/Running，平台置 Failed
   * 并发 triggerlink/run.failed 事件——配置了 onFailure 的函数会因此触发。缺省 = 不限时。
   */
  timeout?: string | number;
  /** 防抖（FR-4.5），随 manifest 上报为 debounce。与 batch 同为路由层流控，同时配置时以 debounce 为准 */
  debounce?: DebounceOpts;
  /** 限流（FR-4.4），随 manifest 上报为 throttle */
  throttle?: ThrottleOpts;
  /** 批处理（FR-4.7），随 manifest 上报为 batch */
  batch?: BatchOpts;
  /**
   * run 进入 Failed 终态时的处理器（FR-2.11）。serve 时注册隐式函数 <id>/on-failure，
   * 订阅内部事件 triggerlink/run.failed（match 按 function_id 过滤），handler 内可用全部 step 原语。
   * onFailure 自身失败不会递归触发。
   */
  onFailure?: (ctx: HandlerContext<RunFailedEventData>) => Promise<unknown>;
}

export interface TriggerFunction<T = unknown> {
  readonly opts: FunctionOpts & { retries: number };
  readonly handler: (ctx: HandlerContext<T>) => Promise<unknown>;
}

/** 定义一个 durable 函数。副作用必须放进 step.run（协议第 6 节约束）。 */
export function createFunction<T = unknown>(
  opts: FunctionOpts,
  handler: (ctx: HandlerContext<T>) => Promise<unknown>,
): TriggerFunction<T> {
  if (!opts.id) throw new Error("createFunction: id is required");
  if (!opts.event && !opts.cron) {
    throw new Error("createFunction: event or cron is required"); // 纯 cron 函数无事件订阅
  }
  return { opts: { retries: 0, ...opts }, handler };
}
