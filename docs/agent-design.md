# TriggerLink Agent Primitive — Design Document

> **Status**: Draft, pending review
> **Scope**: sdk-ts first; Go SDK parity is a later milestone
> **Related**: [architecture.md](architecture.md), [protocol.md](protocol.md), [triggerlink-prd.md](triggerlink-prd.md)

## 1. Background and Motivation

TriggerLink today provides durable execution primitives (`step.run` / `sleep` / `sendEvent`) but nothing LLM-aware. Users who want an AI agent inside a TriggerLink function must hand-wrap an external framework (e.g. Inngest's AgentKit) in a single `step.run`, which has two problems:

- **Coarse recovery granularity**: the whole agent loop is one memo. A failure on iteration 9 of 10 re-runs the entire loop — every LLM call is re-billed.
- **Conceptual mismatch**: external agent frameworks bring their own orchestrator (AgentKit's Network/Router, optionally backed by Inngest), which competes with TriggerLink's own function/step model.

This document proposes a **native agent primitive in sdk-ts**: a minimal `createAgent` whose internal LLM/tool loop is decomposed into ordinary durable steps. Each LLM call and each tool execution is individually memoized, so crash recovery resumes from the exact failed call.

Key property: **zero platform and protocol changes**. The agent loop is pure SDK-side composition over the existing step primitives and the existing re-invocation execution model.

## 2. Goals and Non-Goals

### Goals

- Single-agent capability: system prompt + tools + model, with a loop that ends when the model stops calling tools.
- Per-LLM-call and per-tool-call durability via existing `step.run` semantics.
- Multi-provider model support without maintaining provider adapters ourselves — build on the [Vercel AI SDK](https://github.com/vercel/ai) (`ai` package) as the model abstraction layer.
- Deterministic replay under TriggerLink's existing memo-hash model (protocol §6) with no new constraints on users.
- Free observability: every LLM/tool call shows up as a step in the dashboard run details.

### Non-Goals (explicitly out of scope)

- **Network / Router / multi-agent abstractions.** A TriggerLink function *is* the router — the user writes ordinary code to sequence agents, branch on outputs, and fan out via `step.sendEvent`. This matches what AgentKit itself recommends as "code-based routing".
- **Streaming responses.** The platform callback model is request/response; streaming is a separate future topic.
- **MCP tool sources.** The AI SDK ecosystem has MCP clients; can be added later as a tool adapter without changing this design.
- **Human-in-the-loop (`waitForEvent`).** Blocked on the platform milestone, not on this design.
- **Go SDK parity.** The AI ecosystem is TS-centric; Go parity is evaluated after the TS API stabilizes.

## 3. API Design

New module `sdk-ts/src/agent.ts`, exposed as the subpath export `@triggerlink/sdk/agent` (see §8.1 for why it is not re-exported from the main entry).

```ts
import { createFunction } from "@triggerlink/sdk";
import { createAgent } from "@triggerlink/sdk/agent";
import { anthropic } from "@ai-sdk/anthropic";   // user brings their provider package
import { z } from "zod";

const researcher = createAgent({
  name: "researcher",                    // stable ID; used in memo keys — do not rename casually
  model: anthropic("claude-sonnet-4-5"), // AI SDK LanguageModel
  system: "You are a research assistant. Answer concisely.",
  tools: {
    search: {
      description: "Search the knowledge base",
      parameters: z.object({ query: z.string() }),
      handler: async ({ query }) => kb.search(query),  // plain async fn; no step access needed
    },
  },
  maxIterations: 20,                     // safety cap; default 10
});

export const answerQuestion = createFunction(
  { id: "answer-question", event: "question/asked" },
  async ({ event, step }) => {
    // The agent loop runs here; each LLM/tool call is an individual durable step.
    const answer = await researcher.run(step, event.data.question);

    // Function code is the router: branch, chain agents, or fan out.
    await step.sendEvent("notify", { name: "question/answered", data: { answer } });
    return answer;
  },
);
```

### Type sketches

```ts
interface AgentTool<P = unknown, R = unknown> {
  description: string;
  parameters: ZodType<P>;              // zod schema, converted to JSON Schema for the model
  handler: (params: P) => Promise<R> | R;
}

// Generic factory: makes the zod schema's type flow into the handler params.
// Plain literals also work; createTool exists for inference and cross-agent sharing.
declare function createTool<P, R>(def: AgentTool<P, R>): AgentTool<P, R>;

interface AgentOpts {
  name: string;                        // unique per function (see §5.2)
  model: LanguageModel;                // from the `ai` package
  system?: string;
  tools?: Record<string, AgentTool>;
  maxIterations?: number;              // default 10; throws (RunError) when exceeded
  redact?: RedactHook;                 // opt-in; transforms step output before persistence (see §5.7)
}

interface RedactCtx {
  kind: "llm" | "tool";                // which step produced the output
  iteration: number;                   // agent loop iteration (0-based)
  toolName?: string;                   // set when kind === "tool"
}
type RedactHook = (output: unknown, ctx: RedactCtx) => unknown;

interface Agent {
  readonly name: string;
  run(step: StepTool, input: string): Promise<AgentResult>;
}

interface AgentResult {
  text: string;                        // final assistant message
  iterations: number;
  usage: { inputTokens: number; outputTokens: number };  // summed across iterations
  // every tool execution of the run, in order; rebuilt from memos on recovery.
  // Function code reads structured tool outputs here (no shared-state abstraction needed).
  toolCalls: Array<{ toolCallId: string; toolName: string; input: unknown; output: unknown }>;
  // full conversation history (user + assistant + tool messages, each with role/content);
  // rebuilt from memos. Same field name as AgentKit's result.output, so traversal helpers
  // like findLastIndex(m => m.role === "assistant") port directly; the built-in shortcut
  // is lastAssistantTextMessageContent(result).
  output: ModelMessage[];
}
```

Dependencies: `ai` and the three built-in provider packages (`@ai-sdk/anthropic`, `@ai-sdk/openai`, `@ai-sdk/deepseek`) are **regular dependencies** of `@triggerlink/sdk` — bundled for zero-extra-install DX (revised in v0.4.1; the original optional-peer design made every agent user hand-install four packages). `zod` stays an **optional peer dependency**: it appears only in user code (tool schemas) and in types, never at SDK runtime. Other providers remain usable by passing any AI SDK `LanguageModel` as `model`.

## 4. Execution Model

### 4.1 The loop, mapped onto steps

The agent loop never lets the AI SDK auto-execute tools or run its own multi-step loop. Each iteration is a **single-shot** `generateText` call; tool execution is done by us, one durable step at a time:

```
agent.run(step, input):
  messages = [user(input)]
  for i in 0..maxIterations-1:
    resp = await step.run(`agent/${name}/llm`, () =>
      generateText({ model, system, messages, tools: schemaOnly(tools) }))
    // resp memo output: { text, toolCalls: [{name, args}], usage }

    if resp.toolCalls is empty:
      return { text: resp.text, ... }
    messages.push(assistantMsg(resp))

    for call of resp.toolCalls:        // sequential, array order (see §5.3)
      out = await step.run(`agent/${name}/tool`, () =>
        tools[call.name].handler(call.args))
      // out memo output: the tool's return value (JSON-serializable)
      messages.push(toolMsg(call, out))
```

Tools are passed to `generateText` **without** their `execute` functions (schema only: description + JSON Schema parameters). The model's tool calls therefore come back unexecuted in the response, giving us the step boundary between "the model decided" and "the tool ran".

### 4.2 Why this replay is deterministic

TriggerLink's recovery model re-executes the function from the beginning on every callback, skipping memoized steps (protocol §1). For the agent loop:

- On re-entry, iteration 0's `llm` step memo-hits and returns the **recorded** response — the same tool calls in the same order, so the same `tool` steps follow, which also memo-hit.
- The loop reconstructs the full message history from memos and advances exactly to the first unfinished `llm`/`tool` step.
- Memo keys come from the existing `ExecCtx.nextHash` counter (`sha256(functionID:stepID:seq)`, sdk-ts/src/execx.ts), so repeating the same step ID across loop iterations is already supported — no new keying scheme is needed.

The LLM is non-deterministic, but that doesn't matter: each LLM response is persisted before anything downstream of it runs, so from the platform's point of view the loop is a deterministic function of the memos.

### 4.3 Callback count and cost

An agent run with *L* LLM calls and *T* tool executions requires *L + T + 1* platform callbacks (one per step + final `RunComplete` of the enclosing function). This is inherent to the one-step-per-callback model (protocol §1) and is the price of per-step durability. Mitigations for chatty agents: keep `maxIterations` low, and prefer fewer, larger tools.

## 5. Constraints and Edge Cases

### 5.1 Executor callback timeout

The platform's HTTP callback timeout is 5 minutes (`internal/executor/executor.go`). A single `llm` step must complete within it — fine for normal completions, but a very long generation (huge `max_tokens`, slow provider) can hit it. **Decided: no `timeoutMs` option on `agent.run`; we rely on the provider/AI-SDK defaults** (users who need a cap can pass `abortSignal` through model call options themselves). Document the 5-minute platform limit so users understand a callback timeout surfaces as a step retry rather than a clean LLM error.

### 5.2 Memo key collisions

Step IDs are prefixed `agent/${name}/`, so agent steps cannot collide with a user's own `step.run` IDs unless the user adopts the same prefix (documented as reserved). Two agents in one function must have different `name`s; `createAgent` cannot enforce this at definition time, so `agent.run` validates uniqueness against the `ExecCtx` at first use (cheap runtime check: track claimed prefixes per context).

### 5.3 Parallel tool calls

A model may emit multiple tool calls in one response. They are executed **sequentially in array order**, each as its own step. Rationale: the M0 execution model advances one step per callback; true parallel execution would require a new opcode (e.g. `StepBatch`) and platform changes — explicitly out of scope. Sequential order is deterministic and replay-safe.

### 5.4 Payload growth

Every callback carries all completed step memos (protocol §4). Total memo size grows with conversation history (each memo holds one message's worth of output), so the *n*-th callback payload is O(history) — acceptable for M0-scale agents, but document the 10 MB body cap and recommend bounded iterations and compact tool outputs. A future optimization (platform-side, out of scope here) is delta memo transport.

### 5.5 Error semantics

- LLM call failure (network, 5xx, rate limit) → `StepError` with `retryable: true` (current step.ts behavior) → platform backoff/retry, attempt cap applies (default 4).
- Tool handler throws → same path.
- `maxIterations` exceeded → throw out of the handler → `RunError`, so the run fails loudly rather than looping forever.
- Non-retryable classification (e.g. invalid API key, schema rejection) is a follow-up: step.ts currently hardcodes `retryable: true`; adding a `NonRetryableError` type benefits all steps, not just agents.

### 5.6 Code-change constraint (existing, restated)

As with any TriggerLink function, the step sequence must stay deterministic across retries: changing the agent's tool set or loop structure between attempts of the same run can misalign memo keys (protocol §6). Changing prompt *text* is safe (it doesn't alter the step sequence).

### 5.7 Output redaction hook

**Decided: needed, opt-in.** LLM responses and tool outputs are persisted verbatim in step memos (and shown in the dashboard), which can leak secrets, PII, or provider reasoning traces into the database. `createAgent` accepts a `redact` hook that transforms each step's output **inside** `step.run`, before the value is returned and persisted:

```
resp = await step.run(`agent/${name}/llm`, async () => {
  const raw = await generateText(...);
  return redact ? redact(rawMemo, { kind: "llm", iteration: i }) : rawMemo;
});
```

Same for `tool` steps (with `toolName` in the context). One hook covers both kinds; it is pure SDK-side, no platform or protocol changes.

**Critical tradeoff — redaction affects replay.** The memo is not just an audit record: on recovery it is the *only* source from which the message history is reconstructed (§4.2). A redacted memo means the model sees the redacted version of its own prior turns after a crash-resume. Therefore:

- The hook must be **replay-safe**: redact content the model does not need later (reasoning traces, echoed credentials, PII), never structurally alter fields the loop depends on (`toolCalls`, message roles). The SDK documents this and type-guards the `llm` memo shape after the hook runs (throws `RunError` on structural damage — fail loud, not corrupt).
- The hook must be **deterministic**: same input → same output. A non-deterministic hook (e.g. one embedding timestamps) would make the memo differ from what a hypothetical re-execution would produce, complicating debugging.
- Observability-only redaction (persist full, mask in the dashboard) would require platform support for a separate display field — noted as a possible future enhancement, out of scope here.

Default: no hook, output persisted as-is (documented plainly so the leak surface is a conscious choice).

## 6. Observability

No new machinery: each `llm`/`tool` step appears in the dashboard's run detail with its memo output — the LLM response (including token usage) and each tool result. This gives per-call tracing, latency (step timestamps), and cost attribution for free, comparable to what AgentKit advertises as built-in tracing.

The `llm` step memo output includes `usage` per call; `AgentResult.usage` aggregates them for the caller.

## 7. Testing Strategy

- **Unit** (`sdk-ts/test/agent.test.mjs`): drive the loop with the AI SDK's `MockLanguageModelV2` (`ai/test`) — scripted responses: (a) text-only finish, (b) tool call → tool result → finish, (c) multi-tool-call ordering, (d) `maxIterations` guard, (e) `redact` hook transforms persisted output and rejects structural damage to `llm` memos.
- **Replay test**: simulate memo injection by pre-seeding an `ExecCtx` with completed `llm`/`tool` memos and assert the loop resumes at the right call without re-invoking the mock model — this is the core durability property.
- **E2E** (optional, follows existing `e2e/` patterns): a 2-iteration agent against a mock model inside a real platform+app round trip, with a kill-and-resume between iterations.

## 8. File-Level Changes

| File | Change |
|---|---|
| `sdk-ts/src/agent.ts` | New: `createAgent`, loop implementation, types (~250 lines) |
| `sdk-ts/package.json` | Add `./agent` subpath export; `ai` + built-in providers as regular deps, `zod` as optional peer (revised in v0.4.1, see §3); version bump |
| `sdk-ts/test/agent.test.mjs` | New tests |
| `sdk-ts/README.md` | Agent section with quickstart |
| `examples/nextjs/` (or new `examples/agent/`) | Minimal agent example app |
| `docs/user-guide.md` | "Agents" chapter |
| `README.md` | One-line mention + link |

No changes under `internal/`, `cmd/`, or `sdk/` (Go).

### 8.1 Publishing

**Decided: ship in the existing `@triggerlink/sdk` package as a subpath export `@triggerlink/sdk/agent` — not a separate package.**

- The agent module must **not** be re-exported from `index.ts`: a subpath export isolates the `ai`/provider imports behind `import { createAgent } from "@triggerlink/sdk/agent"`, so non-agent users never load that code at runtime (install size grows, runtime does not).
- Dependency layout (revised in v0.4.1): `ai` and the three built-in providers are regular dependencies — bundled so agent users need no extra installs beyond `zod` (optional peer, used for tool schemas). The v0.4.0 layout (everything an optional peer) forced four manual installs per agent user and was reversed after first use.
- Same-package release keeps the agent loop atomically in sync with the `StepTool`/`ExecCtx` memo semantics it depends on, and fits the current manual publish flow (`prepublishOnly` build → `npm publish` from `sdk-ts/`; no CI release pipeline exists yet). Splitting into `@triggerlink/agent` later remains possible — the subpath import keeps that migration to one line for users.

## 9. Milestones

1. **A1 — core loop**: `createAgent` + `run` with schema-only tools, sequential tool execution, `maxIterations`, `redact` hook (§5.7), unit + replay tests with the mock model.
2. **A2 — polish**: usage aggregation, README/user-guide docs, example app.
3. **A3 — later (separate designs)**: MCP tool adapter, `NonRetryableError`, parallel tool execution (needs platform opcode), Go SDK parity, streaming.

## 10. Alternatives Considered

- **Thin wrapper over AgentKit** (`step.run` around `network.run`): near-zero effort, but inherits coarse recovery granularity and an upstream project whose release cadence has slowed. Rejected as the long-term answer; acceptable as a stopgap.
- **Vendoring AgentKit source** (Apache 2.0): full control, but we would own an unmaintained framework including abstractions (Network/Router/State) that duplicate TriggerLink's own function model. Rejected.
- **Hand-written provider adapters** instead of the Vercel AI SDK: avoids a dependency but we would re-implement and maintain per-provider request/response/tool-call normalization — exactly the layer the AI SDK already solves well. Rejected.

## 11. Open Questions

All resolved:

- ~~Should `agent.run` accept a per-call `timeoutMs`?~~ **Resolved: rely on provider/AI-SDK defaults; no SDK-level timeout option** (see §5.1).
- ~~Memo output redaction hook?~~ **Resolved: yes, opt-in `redact` hook covering both `llm` and `tool` step outputs, applied inside `step.run` before persistence** (see §5.7, including the replay-safety tradeoff).
- ~~Naming: `createAgent` vs. a future multi-agent story?~~ **Resolved: keep `createAgent`; committed to "function code is the router"** — a future multi-agent capability would be a higher-level composition over this primitive, not a rename of it.
