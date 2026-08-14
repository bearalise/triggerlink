// step 原语（协议第 5/6 节；对应 Go 的 sdk/step）。
import { ExecCtx, StepInterrupt, type OutgoingEvent } from "./execx.js";

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
  };
}

export function errMessage(err: unknown): string {
  return err instanceof Error ? err.message : String(err);
}

function errStack(err: unknown): string | undefined {
  return err instanceof Error ? err.stack : undefined;
}
