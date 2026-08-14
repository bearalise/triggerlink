// 执行上下文与 opcode 类型（协议第 4/5/6 节；对应 Go 的 sdk/internal/execx）。
import { createHash } from "node:crypto";

/** 平台注入的单个 step memo。 */
export interface StepState {
  id: string; // step_id
  status: string; // "completed"
  output?: unknown;
}

export interface OpError {
  message: string;
  stack?: string;
  retryable: boolean;
}

/** 应用 → 平台的执行指令（serve 序列化为响应）。 */
export interface Opcode {
  op: "StepComplete" | "StepError" | "RunComplete" | "RunError" | "Sleep" | "SendEvent";
  id?: string; // step_hash
  step_id?: string;
  output?: unknown;
  error?: OpError;
  until?: string; // Sleep：唤醒时刻（RFC3339/ISO）
  events?: OutgoingEvent[]; // SendEvent：待扇出事件
}

/** step.sendEvent 待扇出的事件（id/ts 缺省由平台补全，id 缺省为确定性派生）。 */
export interface OutgoingEvent {
  id?: string;
  name: string;
  data?: unknown;
  ts?: string;
}

/** step 中断：正常控制流，由 serve 捕获并序列化为 opcode。 */
export class StepInterrupt extends Error {
  readonly opcode: Opcode;
  constructor(opcode: Opcode) {
    super(`step interrupt: ${opcode.op}`);
    this.name = "StepInterrupt";
    this.opcode = opcode;
  }
}

/** 单次函数调用的执行上下文。 */
export class ExecCtx {
  readonly functionId: string;
  readonly steps: Record<string, StepState>;
  private readonly counters = new Map<string, number>();

  constructor(functionId: string, steps?: Record<string, StepState>) {
    this.functionId = functionId;
    this.steps = steps ?? {};
  }

  /**
   * memo 键：hex(sha256(functionID + ":" + stepID + ":" + 序号))，序号从 0 起。
   * 平台视其为不透明字符串，只要求同一 run 多次重入间确定（协议第 6 节）。
   */
  nextHash(stepId: string): string {
    const n = this.counters.get(stepId) ?? 0;
    this.counters.set(stepId, n + 1);
    return createHash("sha256").update(`${this.functionId}:${stepId}:${n}`).digest("hex");
  }
}
