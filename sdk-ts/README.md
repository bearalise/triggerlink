# @triggerlink/sdk — TypeScript SDK (M0 prototype)

Lets Next.js / Node.js applications integrate with the TriggerLink platform using an
Inngest-style DX. See the protocol spec at
[`docs/protocol.md`](../docs/protocol.md). Currently supports the `step.run`,
`step.sleep` / `step.sleepUntil`, and `step.sendEvent` primitives, plus a native
AI agent primitive via the `@triggerlink/sdk/agent` subpath.

## Integrating with Next.js (App Router)

```bash
npm install @triggerlink/sdk
```

```ts
// app/api/triggerlink/route.ts
import { createClient, createFunction, serve } from "@triggerlink/sdk";

export const runtime = "nodejs";     // requires node:crypto and a longer execution limit
export const maxDuration = 300;      // a single step must fit within the function limit (platform callback timeout is 5 minutes)

const client = createClient({
  id: "web",
  signingKey: process.env.TRIGGERLINK_SIGNING_KEY!, // must match the platform's -signing-key
});

const fulfillOrder = createFunction(
  { id: "fulfill-order", event: "order/paid" },
  async ({ event, step }) => {
    const { order_id } = event.data as { order_id: string };

    // One side effect per step: on retry/crash recovery, completed steps are injected from memo and not re-run
    const tracking = await step.run("create-shipment", () =>
      logistics.createShipment(order_id),
    );
    await step.run("send-sms", () => sms.send(order_id, tracking));

    return { tracking };
  },
);

export const { GET, POST } = serve({ client, functions: [fulfillOrder] });
```

Platform-side registration (pick one):

```bash
# Platform-side static introspection: point -app at the app's serve URL at startup
triggerlink -event-key ... -signing-key ... \
  -app https://your-app.vercel.app/api/triggerlink
```

```ts
// Or app-side self-registration (reversed direction): call once after startup, retries in the background, does not block startup
client.register("http://localhost:3000/api/triggerlink"); // requires eventKey
```

Note: for local development, point the serve URL at `http://localhost:3000/api/triggerlink`; after changing functions in the app, call `POST /api/v1/apps/sync {"url":"..."}` to sync — no platform restart needed.

## Constraints (same as the Go SDK; see protocol section 6)

- Side effects must go inside `step.run`; the function is re-invoked from the start on every callback, so code outside steps runs repeatedly;
- The step call sequence must be deterministic: branches/loops may only depend on event data and the outputs of completed steps;
- A single step's duration must be shorter than both the deployment platform's function limit and the platform callback timeout (5 minutes by default).

## AI Agents (`@triggerlink/sdk/agent`)

A native agent primitive: a single agent (system prompt + tools + model) whose
LLM/tool loop is decomposed into ordinary durable steps. **Each LLM call and each
tool execution is individually memoized** — on crash recovery, completed calls are
injected from memo and only the failed call re-runs (no re-billed LLM tokens).
Built on the [Vercel AI SDK](https://github.com/vercel/ai) for multi-provider
support; design details in [`docs/agent-design.md`](../docs/agent-design.md).

```bash
npm install @triggerlink/sdk zod   # zod is for tool schemas; ai + providers are bundled
```

```ts
import { createFunction } from "@triggerlink/sdk";
import { createAgent, createTool, anthropic } from "@triggerlink/sdk/agent";   // subpath import, not the main entry
import { z } from "zod";

// Built-in providers, zero extra installs: anthropic / openai / deepseek
// (plus createAnthropic / createOpenAI / createDeepSeek for custom baseURL/apiKey).
// Default instances read ANTHROPIC_API_KEY / OPENAI_API_KEY / DEEPSEEK_API_KEY from the env.
// Any other AI SDK LanguageModel can still be passed as `model` directly.

// createTool is a generic factory: the zod schema's type flows into the handler's
// params — annotate nothing. Plain object literals also work; use createTool when
// sharing a tool across agents.
const searchKb = createTool({
  description: "Search the knowledge base",
  parameters: z.object({ query: z.string() }),
  handler: async ({ query }) => kb.search(query),   // query: string, inferred
});

const researcher = createAgent({
  name: "researcher",                    // stable ID, used in memo keys — do not rename casually
  model: anthropic("claude-sonnet-4-5"), // any AI SDK LanguageModel
  system: "You are a research assistant. Answer concisely.",
  tools: { search: searchKb },
  maxIterations: 10,                     // safety cap; the run fails when exceeded
});

const answerQuestion = createFunction(
  { id: "answer-question", event: "question/asked" },
  async ({ event, step }) => {
    const { question } = event.data as { question: string };
    const result = await researcher.run(step, question); // each LLM/tool call is a durable step

    // Function code is the router: chain agents, branch, or fan out — no extra abstraction
    await step.sendEvent("notify", { name: "question/answered", data: { answer: result.text } });
    return result;
  },
);
```

Notes:

- **Durability granularity**: an agent run with *L* LLM calls and *T* tool executions costs
  *L + T* platform callbacks (one per step). Each step appears in the dashboard run detail
  with its output — per-call tracing and token usage for free.
- **Multi-tool responses** are executed sequentially in array order, one step each.
- **Structured tool outputs**: `AgentResult.toolCalls` lists every tool execution of the run
  (`{ toolCallId, toolName, input, output }`, in order; rebuilt from memos on recovery).
  This covers the "done tool writes to shared state" pattern from agent frameworks like
  AgentKit — read the tool's output here instead of parsing the final text.
- **Message history**: `AgentResult.output` is the full conversation (user input, assistant
  turns, tool results), each element carrying `role`/`content` — same field name and shape as
  AgentKit's `result.output`, so helpers like `findLastIndex((m) => m.role === "assistant")`
  port directly. `lastAssistantTextMessageContent(result)` is the built-in shortcut.
- **`redact` hook** (optional): transforms each step's output inside `step.run` before
  persistence, e.g. to strip secrets or PII from memos. It must be deterministic and
  replay-safe — the memo is what the model sees of its own prior turns after a crash-resume:
  `redact: (output, ctx) => ...` with `ctx = { kind: "llm" | "tool", iteration, toolName? }`.
- **Constraints**: same as any function — a single LLM call must finish within the platform
  callback timeout (5 minutes by default); two different agents in one function must have
  different `name`s; changing the tool set or loop structure between retries of the same run
  can misalign memo keys (changing prompt text is safe).
- `ai` and the three built-in providers are regular dependencies of the SDK (bundled, no extra install); `zod` is an optional peer dependency — install it if you define tool schemas.
- **HTTP proxies**: if your environment routes external traffic through `http_proxy`/`https_proxy`, note that Node's global `fetch` ignores them by default — LLM calls will fail with `AI_APICallError: Cannot connect to API`. On Node 24+, start your app with `node --use-env-proxy`; on older Node, install `undici` and set `setGlobalDispatcher(new EnvHttpProxyAgent())` before serving.

## Sending events (any TS code, modeled after Inngest's `inngest.send`)

```ts
const client = createClient({
  id: "web",
  signingKey: process.env.TRIGGERLINK_SIGNING_KEY!,
  eventKey: process.env.TRIGGERLINK_EVENT_KEY!,   // required for send / register
  baseUrl: process.env.TRIGGERLINK_BASE_URL,      // defaults to http://localhost:8288
});

// Single event; id is an idempotency key, safe to retry (generated by the platform if omitted)
await client.send({
  id: `order-${orderId}-paid`,
  name: "order/paid",
  data: { order_id: orderId },
});

// Or a batch
await client.send([{ name: "x/y" }, { name: "x/z", data: { n: 1 } }]);
```

## Development

```bash
npm install
npm test        # tsc build + node:test (simulates the platform's three-callback progression / memo injection / signature verification / error paths)
npm run build   # outputs dist/ (ESM + .d.ts)
```
