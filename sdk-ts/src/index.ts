// @triggerlink/sdk — TriggerLink TypeScript SDK（M0 原型）。
// 用法见 sdk-ts/README.md 与 docs/protocol.md。
export {
  createClient,
  type Client,
  type ClientOptions,
  type SendEventInput,
  type SendResult,
  type RegisterOptions,
} from "./client.js";
export {
  createFunction,
  type TriggerFunction,
  type FunctionOpts,
  type HandlerContext,
  type EventPayload,
  type TriggerLinkEvent,
  type CancelOnRule,
} from "./function.js";
export { serve, sdkVersion, type ServeOptions } from "./serve.js";
export { StepInterrupt, type Opcode, type StepState, type OutgoingEvent } from "./execx.js";
export type { StepTool, WaitForEventOpts } from "./step.js";
