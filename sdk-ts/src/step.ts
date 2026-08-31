// step 原语（协议第 5/6 节；对应 Go 的 sdk/step）。
import { ExecCtx, StepInterrupt, type OutgoingEvent } from "./execx.js";
import type { TriggerLinkEvent } from "./function.js";

/** step.waitForEvent 的配置。 */
export interface WaitForEventOpts {
  /** 等待的事件名（必填） */
  event: string;
  /** 可选 expr 表达式；求值环境：data=到达事件 data，event=本 run 触发事件 {name, data} */
  match?: string;
  /** 可选超时：Go duration 字符串（如 "168h"）或毫秒数（1500 → "1.5s"）；超时后返回 null */
  timeout?: string | number;
}

/** handler 内可用的 step 工具。 */
export interface StepTool {
  /**
   * 执行一个 durable step：fn 的返回值被平台持久化，崩溃恢复后直接注入不重跑。
   * memo 命中 → 返回缓存值；未命中 → 执行 fn，然后抛 StepInterrupt 中断函数，
   * 由 serve 序列化为 opcode 交平台持久化并发起下一次回调。
   */
  run<T>(id: string, fn: () => Promise<T> | T): Promise<T>;
  /**
   * 挂起当前 run 直至 durMs 之后：函数中断，平台到点重新回调恢复。
   * 挂起期间不占连接与计算；恢复重入时 memo 命中直接返回，不会重复睡眠。
   */
  sleep(id: string, durMs: number): Promise<void>;
  /** 挂起当前 run 直至绝对时刻 at（sleep 的绝对时间版本）。 */
  sleepUntil(id: string, at: Date | string): Promise<void>;
  /**
   * 在函数内可靠扇出事件：平台先落库后路由，崩溃不丢。
   * memo 语义同 run——恢复重入时直接返回已发事件 ID 列表，不会重复发送。
   */
  sendEvent(id: string, events: OutgoingEvent | OutgoingEvent[]): Promise<string[]>;
  /**
   * 挂起当前 run 直至匹配事件到达或超时（协议 5.7 / FR-2.6）：
   * 事件命中 → 返回该事件；超时 → 返回 null（超时分支）。
   * 与 sleep 同为挂起原语（恢复重入时 memo 命中直接返回），但唤醒由平台在
   * 事件路由命中/超时扫描时驱动，挂起期间零占用。
   */
  waitForEvent(id: string, opts: WaitForEventOpts): Promise<TriggerLinkEvent | null>;
}

/** 为一次函数调用构造 step 工具。 */
export function createStepTool(ec: ExecCtx): StepTool {
  return {
    async run<T>(id: string, fn: () => Promise<T> | T): Promise<T> {
      const h = ec.nextHash(id);
      const memo = ec.steps[h];
      if (memo && memo.status === "completed") {
        return memo.output as T;
      }
      try {
        const output = await fn();
        throw new StepInterrupt({ op: "StepComplete", id: h, step_id: id, output });
      } catch (err) {
        if (err instanceof StepInterrupt) throw err;
        throw new StepInterrupt({
          op: "StepError",
          id: h,
          step_id: id,
          error: { message: errMessage(err), stack: errStack(err), retryable: true },
        });
      }
    },
    async sleep(id: string, durMs: number): Promise<void> {
      return this.sleepUntil(id, new Date(Date.now() + durMs));
    },
    async sleepUntil(id: string, at: Date | string): Promise<void> {
      const h = ec.nextHash(id);
      const memo = ec.steps[h];
      if (memo && memo.status === "completed") return; // 已睡过（平台唤醒时已置 completed）
      const until = (typeof at === "string" ? new Date(at) : at).toISOString();
      throw new StepInterrupt({ op: "Sleep", id: h, step_id: id, until });
    },
    async sendEvent(id: string, events: OutgoingEvent | OutgoingEvent[]): Promise<string[]> {
      const h = ec.nextHash(id);
      const memo = ec.steps[h];
      if (memo && memo.status === "completed") return memo.output as string[];
      const list = Array.isArray(events) ? events : [events];
      if (list.length === 0) throw new Error(`step.sendEvent "${id}": no events`);
      throw new StepInterrupt({ op: "SendEvent", id: h, step_id: id, events: list });
    },
    async waitForEvent(id: string, opts: WaitForEventOpts): Promise<TriggerLinkEvent | null> {
      const h = ec.nextHash(id);
      const memo = ec.steps[h];
      if (memo && memo.status === "completed") {
        if (memo.output == null) return null; // 超时分支：平台以 output=JSON null 完成
        return memo.output as TriggerLinkEvent;
      }
      if (!opts.event) throw new Error(`step.waitForEvent "${id}": event is required`);
      throw new StepInterrupt({
        op: "WaitForEvent",
        id: h,
        step_id: id,
        event: opts.event,
        match: opts.match,
        timeout: typeof opts.timeout === "number" ? msToGoDuration(opts.timeout) : opts.timeout,
      });
    },
  };
}

/** 毫秒 → Go duration 字符串（协议要求 timeout 为 Go duration，如 1500 → "1.5s"）。 */
export function msToGoDuration(ms: number, what = "step.waitForEvent"): string {
  if (!Number.isFinite(ms) || ms <= 0) {
    throw new Error(`${what}: invalid timeout ${ms}`);
  }
  return `${ms / 1000}s`;
}

export function errMessage(err: unknown): string {
  return err instanceof Error ? err.message : String(err);
}

function errStack(err: unknown): string | undefined {
  return err instanceof Error ? err.stack : undefined;
}
