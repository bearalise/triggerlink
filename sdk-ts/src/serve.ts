// serve：承载内省与执行回调的 Web-standard handler（协议第 3/4/5 节）。
// 返回的 { GET, POST } 可直接导出为 Next.js App Router route handler，
// 也可适配任意支持 Fetch API Request/Response 的框架。
import type { Client } from "./client.js";
import type { TriggerFunction, EventPayload } from "./function.js";
import { ExecCtx, StepInterrupt, type Opcode, type StepState } from "./execx.js";
import { createStepTool, errMessage } from "./step.js";
import { verifySignature } from "./sign.js";

export const sdkVersion = "triggerlink-ts/0.3.0";
export const SIGNATURE_HEADER = "x-triggerlink-signature";

const MAX_BODY = 10 << 20; // 10 MB，与平台一致

export interface ServeOptions {
  client: Client;
  functions: TriggerFunction[];
}

interface CallbackRequest {
  ctx: {
    run_id: string;
    function_id: string;
    attempt: number;
    event: EventPayload;
    steps?: Record<string, StepState>;
  };
}

function json(data: unknown, status = 200): Response {
  return Response.json(data, { status });
}

export function serve(opts: ServeOptions) {
  const byID = new Map<string, TriggerFunction>();
  for (const fn of opts.functions) byID.set(fn.opts.id, fn);

  async function GET(req: Request): Promise<Response> {
    if (!verifySignature(opts.client.signingKey, req.headers.get(SIGNATURE_HEADER), new Uint8Array(0))) {
      return json({ error: "unauthorized" }, 401);
    }
    return json({
      sdk: sdkVersion,
      app_id: opts.client.id,
      functions: [...byID.values()].map((f) => ({
        id: f.opts.id,
        event: f.opts.event,
        retries: f.opts.retries,
      })),
    });
  }

  async function POST(req: Request): Promise<Response> {
    const body = new Uint8Array(await req.arrayBuffer());
    if (body.byteLength > MAX_BODY) return json({ error: "body too large" }, 400);
    if (!verifySignature(opts.client.signingKey, req.headers.get(SIGNATURE_HEADER), body)) {
      return json({ error: "unauthorized" }, 401);
    }
    let cbReq: CallbackRequest;
    try {
      cbReq = JSON.parse(new TextDecoder().decode(body));
    } catch {
      return json({ error: "invalid request" }, 400);
    }
    const fn = byID.get(cbReq?.ctx?.function_id);
    if (!fn) return json({ error: "function not found" }, 404);

    const ec = new ExecCtx(cbReq.ctx.function_id, cbReq.ctx.steps);
    const step = createStepTool(ec);

    let opcode: Opcode;
    try {
      const output = await fn.handler({
        event: cbReq.ctx.event,
        step,
        runId: cbReq.ctx.run_id,
        attempt: cbReq.ctx.attempt,
      });
      opcode = { op: "RunComplete", output };
    } catch (err) {
      if (err instanceof StepInterrupt) {
        opcode = err.opcode;
      } else {
        opcode = { op: "RunError", error: { message: errMessage(err), retryable: true } };
      }
    }
    return json(opcode);
  }

  return { GET, POST };
}
