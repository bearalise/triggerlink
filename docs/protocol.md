# TriggerLink Platform↔App Protocol Specification (M0)

> This document is the **authoritative definition** of the HTTP protocol between the platform and the SDK (app side), intended for implementers of SDKs in any language.
> Reference implementations of this protocol: `internal/executor` (platform side) and `sdk/serve.go` + `sdk/internal/execx` (Go SDK side).
> If this document conflicts with the implementation, the implementation wins and this document should be corrected.

## 1. Overview

There are only two interactions between the platform and the app, both hitting the **app's serve endpoint** (a single URL, e.g. `https://app.example.com/api/triggerlink`):

| Direction | Method | Purpose | When |
|---|---|---|---|
| Platform → App | GET | Introspection: fetch the function manifest | Once per `-app` at platform startup |
| Platform → App | POST | Execution callback: advance one execution of a run | On every dispatch / every step advance / every retry |

All requests carry an HMAC-SHA256 signature header (see Section 2). All bodies are JSON, capped at 10 MB (both sides truncate reads at `10 << 20`).

**Execution model**: the platform never runs user code; it only schedules and persists. On each POST callback, the app re-executes the function **from the beginning**, skipping already-completed steps via the injected step memos, until:
- it reaches the next unfinished step → executes it and returns `StepComplete`/`StepError`, interrupting the function;
- the function returns normally → returns `RunComplete`;
- the function throws an uncaught error → returns `RunError`.

The platform persists each opcode and decides whether to advance, back off and retry, or finalize. A successful N-step function requires N+1 callbacks (one per step + one final `RunComplete`).

## 2. Request Signing

Header name: `X-TriggerLink-Signature`

Format:

```
t=<unix seconds>,v1=<hex(hmac_sha256(signing_key, "<t>" + "." + "<body>"))>
```

- `signing_key`: the same secret shared between the platform's `-signing-key` and the app's `Options.SigningKey`.
- `<body>`: the raw request bytes. GET introspection requests have no body and are signed over empty bytes.
- Verification rules:
  1. Parse `t` and `v1`; reject (401) if either is missing;
  2. Reject if the timestamp deviates from local time by **more than ±5 minutes** (replay protection);
  3. Recompute the signature with the same algorithm and compare in **constant time**; reject on mismatch.

Implementation references: `internal/sign/sign.go` (sign + verify), `sdk/sign.go` (SDK verification).

## 3. GET Introspection (manifest)

At startup, the platform sends one signed GET (no body) to each configured `-app` URL, expecting a 200 within 10 seconds.

### Response

```json
{
  "sdk": "triggerlink-go/0.0.0-m0",
  "app_id": "docsvc",
  "functions": [
    { "id": "process-doc", "event": "doc/uploaded", "retries": 4 }
  ]
}
```

| Field | Type | Description |
|---|---|---|
| `sdk` | string | SDK identifier and version; recorded by the platform, not validated |
| `app_id` | string | App identifier; recorded by the platform, not validated |
| `functions[].id` | string | Stable function identifier (renaming it loses the association with historical memos) |
| `functions[].event` | string | Subscribed event name (exact match against the event's `name`) |
| `functions[].match` | string | Optional event-trigger match expression (FR-3.1), omitted when empty. expr-lang syntax; the evaluation environment contains only `data` (= the arriving event's `data`). The function is triggered only when the event name matches and the expression evaluates true |
| `functions[].cron` | string | Optional schedule trigger, omitted when empty. Standard 5-field cron (minute hour day-of-month month day-of-week), **UTC** |
| `functions[].retries` | int | Retry limit; 0 = platform default (4). The M0 platform does not yet differentiate this per function |
| `functions[].cancel_on` | array | Optional cancel rules: `[{"event": "user/deleted", "match": "data.user_id == event.data.user_id"}]`. When the named event arrives and `match` evaluates true, the platform cancels that function's in-flight (Queued/Running) runs; an omitted `match` means "cancel on arrival". `match` is an expr-lang expression evaluated with `data` = the arriving event's `data` and `event` = the run's triggering event `{"name", "data"}` |
| `functions[].timeout` | string | Optional run-level timeout (FR-4.3) as a Go duration string (e.g. `"5m"`, `"168h"`), omitted when unset. If a run is still Queued/Running this long after creation, the platform marks it `Failed` and emits `triggerlink/run.failed` (so the function's `onFailure` handler fires, Section 3.2). Absent = no time limit |

Non-200 response or timeout: the platform logs a warning and skips that app (non-fatal; other apps register as usual).

### 3.1 Cron Trigger Behavior

Functions declaring `cron` are triggered by the platform's scheduler in addition to (or instead of) their event subscription:

- The expression is standard 5-field cron interpreted in **UTC**; the finest granularity is **one minute**.
- Each tick creates a new run whose triggering event is a platform-generated schedule event.
- Missed ticks are **not** backfilled: while the platform is down, ticks are simply skipped; after a restart, scheduling resumes from the next tick.

### 3.2 onFailure: Run-Failure Handler (FR-2.11)

When a run reaches the `Failed` terminal state, the platform emits the internal event **`triggerlink/run.failed`** with data:

```json
{
  "run_id": "run_...",
  "function_id": "process-doc",
  "error": "third-party timeout",
  "event": { "id": "evt_...", "name": "doc/uploaded", "data": { "doc_id": "d1" }, "ts": "2026-01-01T00:00:00Z" }
}
```

(`event` is the run's original triggering event.)

SDKs surface this via an `onFailure` handler in the function options. At serve time the SDK registers an **implicit function** for every function with an `onFailure` handler:

- `id` = `<function id>/on-failure`
- `event` = `triggerlink/run.failed`
- `match` = `data.function_id == '<function id>'`

The implicit function is an ordinary function in every respect: it appears in the manifest (visible to the platform and the Dashboard), and its handler may use all step primitives. **No re-entry**: a failure of the onFailure handler's own run does not recursively trigger another `triggerlink/run.failed` handler.

## 4. POST Execution Callback

### 4.1 Request

The platform POSTs (signed) to the serve URL of the app owning the function, with body:

```json
{
  "ctx": {
    "run_id": "run_...",
    "function_id": "process-doc",
    "attempt": 1,
    "event": {
      "id": "evt_...",
      "name": "doc/uploaded",
      "data": { "doc_id": "d1" },
      "ts": "2026-01-01T00:00:00Z"
    },
    "steps": {
      "<step_hash>": {
        "id": "parse",
        "status": "completed",
        "output": "parsed:d1"
      }
    }
  }
}
```

| Field | Description |
|---|---|
| `run_id` | ID of this execution instance, generated by the platform |
| `function_id` | ID of the routed function; the app should return 404 if no such function exists |
| `attempt` | Attempt number: 1 on the first try, incremented by +1 before each callback (step and run retries share this counter) |
| `event` | The triggering event, echoed verbatim (`data` is arbitrary JSON, `ts` is RFC3339) |
| `steps` | **Memo injection**: contains only step records with `status == "completed"`, keyed by `step_hash` (an opaque string to the app; see Section 6); `{}` on the first execution |

The platform-side HTTP client defaults to a 5-minute timeout — a single step's execution must return within that limit.

### 4.2 Response

The app must return 200 plus an opcode JSON (Section 5). The platform treats the following as "this attempt failed" and retries with backoff:

- Non-200 HTTP status (the response body is truncated to 200 characters and logged);
- Connection failure / timeout;
- 200 but the body is not a valid opcode JSON, or `op` is unrecognized.

## 5. Opcodes (execution instructions from app → platform)

Common structure:

```json
{
  "op": "StepComplete",
  "id": "<step_hash>",
  "step_id": "parse",
  "output": {},
  "error": { "message": "...", "stack": "...", "retryable": true }
}
```

`op` has seven possible values; platform behavior for each:

### 5.1 `StepComplete`

A step finished executing successfully.

| Field | Required | Description |
|---|---|---|
| `id` | Yes | The memo key for this step (step_hash) |
| `step_id` | Yes | Human-readable step name |
| `output` | No | Step return value, arbitrary JSON, persisted verbatim |

Platform behavior: write the memo (`(run_id, step_hash)` upsert) → complete the current queue item → **immediately enqueue the next-hop** callback (advancing to the next step).

### 5.2 `StepError`

A step failed to execute.

| Field | Required | Description |
|---|---|---|
| `id` | Yes | Memo key |
| `step_id` | Yes | Step name |
| `error.message` | Yes | Error message, persisted for troubleshooting |
| `error.stack` | No | Stack trace |
| `error.retryable` | No | **Ignored** by the M0 platform; always retries |

Platform behavior: write the failure memo → run the retry/finalize logic (Section 7).

### 5.3 `RunComplete`

The function returned normally; the entire run succeeded.

| Field | Required | Description |
|---|---|---|
| `output` | No | Function return value, arbitrary JSON, persisted to the run |

Platform behavior: mark the run `Completed`, done.

### 5.4 `RunError`

The function threw an uncaught error (including panics and exceptions the SDK cannot classify).

| Field | Required | Description |
|---|---|---|
| `error.message` | Yes | Error message |
| `error.retryable` | No | Ignored in M0; always retries |

Platform behavior: run the retry/finalize logic (Section 7).

### 5.5 `Sleep`

Request to suspend the run until a specified time (`step.Sleep` / `step.SleepUntil`).

| Field | Required | Description |
|---|---|---|
| `id` | Yes | Memo key |
| `step_id` | Yes | Step name |
| `until` | Yes | Wake-up time (RFC3339); if missing, treated as "this attempt failed" (backoff retry) |

Platform behavior (two-phase suspend + wake-up conversion):

1. **Suspend**: write the sleeping memo (`(run_id, step_hash)`, `status="sleeping"`, `op="Sleep"`) → complete the current queue item → enqueue the next hop with `at = until` (the queue's `at` is "not before" semantics). While suspended, the run stays `Running` with **no connection or compute held**.
2. **Wake up**: when the time arrives and the queue item is leased out, dispatch first flips the run's sleeping memo to `completed` (idempotent UPDATE), then issues the callback per Section 4 — the sleep step is injected as completed.

SDK semantics: if the sleep primitive's memo hits (`steps[hash]` is completed) → return immediately (already slept); on miss → throw a `Sleep` opcode to interrupt the function. The SDK contains no clock logic at all — "whether it has already slept" is determined entirely by the platform's wake-up conversion.

Crash semantics: the sleep queue item is pending with `at` in the future, so after a platform restart it is naturally picked up by executor polling (no reconciliation sweep changes needed); if the wake-up callback fails, the Section 7 backoff retry applies, and since the memo is already completed, execution resumes after the sleep.

### 5.6 `SendEvent`

Reliable fan-out of events from within a function (`step.SendEvent` / `step.sendEvent`); after execution the run **advances immediately** (unlike sleep, it does not suspend).

| Field | Required | Description |
|---|---|---|
| `id` | Yes | Memo key |
| `step_id` | Yes | Step name |
| `events` | Yes | Array of events, at least 1; element shape `{id?, name, data?, ts?}` (same as Section 9 events; only `name` is required) |

Platform behavior:

1. Validate and fill in each event: `data` defaults to `{}`, `ts` defaults to the receipt time; when `id` is absent, **derive a deterministic ID via `sha256(run_id + ":" + step_hash + ":" + index)`** — retrying the same step within a crash window re-sends events with the same IDs, which the `events` table deduplicates idempotently, so no duplicate fan-out occurs ("persist before send; no loss on crash");
2. Same ordering guarantee as the Event API: `InsertEvent` persists successfully → push into the runner stream (routing is skipped if the same ID already exists);
3. Write the step memo (`op="SendEvent"`, `status="completed"`, `output` = JSON array of event IDs) → complete the current queue item → immediately enqueue the next hop.

SDK semantics: memo hit → deserialize `output` and return the ID list directly (**no re-send**); miss → throw a `SendEvent` interrupt. The SDK return value is `string[]` (the event ID list).

### 5.7 `WaitForEvent`

Request to suspend the run until a matching event arrives or a timeout elapses (`step.WaitForEvent` / `step.waitForEvent`).

| Field | Required | Description |
|---|---|---|
| `id` | Yes | Memo key |
| `step_id` | Yes | Step name |
| `event` | Yes | Name of the event to wait for; if missing, treated as "this attempt failed" (backoff retry) |
| `match` | No | expr-lang expression for fine-grained matching; same evaluation environment as `cancel_on` (Section 3): `data` = the arriving event's `data`, `event` = the run's triggering event `{"name", "data"}`. Empty = any event with this name matches |
| `timeout` | No | Maximum wait as a Go duration string (e.g. `"168h"`, `"30m"`); absent = wait forever |

Platform behavior (event-driven suspend + resume):

1. **Suspend**: write the waiting memo (`(run_id, step_hash)`, `status="waiting"`, `op="WaitForEvent"`) + persist a wait record (event name, match expression, expiry) → complete the current queue item → **do not enqueue**. While suspended, the run stays `Running` with no connection or compute held. The wait record is idempotent on `(run_id, step_hash)`: replaying the same opcode within a crash window creates no duplicate wait.
2. **Resume on match**: when an event is routed, each waiting record with the same event name whose `match` evaluates true gets its memo flipped to `completed` with `output` = the matched event `{"id", "name", "data", "ts"}`, and the run is re-enqueued with `at = now`.
3. **Resume on timeout**: a periodic scan expires waits whose `timeout` has elapsed; the step memo is completed with `output` = JSON `null`, and the run is re-enqueued the same way.

SDK semantics: memo hit → if `output` is JSON `null`, return `nil` (Go) / `null` (TS) — this is the timeout branch; otherwise deserialize `output` into the event object and return it. Miss → throw a `WaitForEvent` interrupt (Go serializes `timeout` via `time.Duration.String()`; TS accepts a Go duration string or a millisecond number, e.g. `1500` → `"1.5s"`).

## 6. Step Memo Key (step_hash) Semantics

- **Platform's view: an opaque string**. The platform only stores/loads memos by `(run_id, step_hash)` and injects them verbatim on the next callback; it never computes or interprets the key.
- **SDK's view: must be deterministic across multiple re-invocations of the same run**. That is: as long as the event data and completed step outputs are unchanged, the key sequence computed on the Nth callback must be identical to the one from the 1st. Otherwise memos won't match on recovery and completed steps will be re-run.

Reference algorithm from the Go SDK (`sdk/internal/execx`):

```
step_hash = hex(sha256(functionID + ":" + stepID + ":" + occurrenceIndex))
```

where `occurrenceIndex` is how many times that `stepID` has appeared in the current function execution (starting from 0, incremented on each `step.Run` call). This gives stable keys even when the same stepID is reused in a loop with a deterministic iteration count.

This implies constraints on function code (every SDK must convey these to users):

1. Side effects may only happen inside steps;
2. The step call sequence may only depend on event data and the outputs of completed steps — never on time, randomness, or external mutable state;
3. Orchestration code between steps must be lightweight — it is re-executed from the top on every callback.

## 7. Retry and Finalization Semantics (platform behavior)

- `attempt + 1` before each callback; when `attempt >= MaxRetries` (default 4, a platform-level configuration in M0, not per-function), no more retries: the run is marked `Failed` and the error is recorded.
- Retries are implemented via queue leases: release the lease and set `at = now + backoff`, with backoff `min(1s << attempt, 30s)`.
- **App crash / callback timeout** is handled the same as an explicit `StepError`: backoff retry. After the app recovers, the callbacks it receives still carry memos for completed steps — this is what enables resuming from the last completed step.
- **Platform crash**: after the lease expires (default 30s), the restart reconciliation sweep re-enqueues the item and the callback is re-issued. The SDK must therefore tolerate duplicate callbacks for the same attempt (memo upserts guarantee idempotency).
- **Sleep suspension** (Section 5.5) does not consume backoff retries: suspension and wake-up are each a normal callback, and the wake-up hop's `attempt` still increments by +1 (M0 uses a shared counter, so long functions should watch the limit).

## 8. SDK Implementation Checklist (language-agnostic)

A minimal viable SDK needs:

1. **Serve endpoint**: dispatch GET/POST by method; verify signatures (Section 2); return the manifest on GET (Section 3).
2. **POST execution flow**:
   - Verify signature → parse `ctx` → look up the function by `function_id` (return 404 if not found);
   - Build the execution context (inject the `steps` memos + a per-stepID occurrence counter);
   - Invoke the user handler and capture three result classes: step interrupt (a language-level exception mechanism, e.g. Go panic, TS/Python throw) → `StepComplete`/`StepError`; handler returns normally → `RunComplete`; any other exception → `RunError`;
   - Serialize the opcode and return 200.
3. **Step primitives**: `run` — memo hit (`steps[hash]` exists) → deserialize `output` and return it directly; miss → run the user fn, throwing a `StepComplete` interrupt on success or a `StepError` interrupt on failure. `sleep`/`sleepUntil` — memo hit → return directly; miss → throw a `Sleep` interrupt (carrying `until`). `sendEvent` — memo hit → return the ID list directly; miss → throw a `SendEvent` interrupt (carrying the event array). `waitForEvent` — memo hit → return the event object (`output` = JSON `null` means the timeout branch → return nil/null); miss → throw a `WaitForEvent` interrupt (carrying `event`, optional `match` and `timeout`).
4. **Event type**: `{id, name, data, ts}`, same shape as in Section 4.

The platform does not validate SDK internals — it only cares about the on-the-wire behavior in Sections 2–5.

## 9. Appendix: Platform Public API

Unrelated to the SDK, but part of the platform's external protocol, recorded here for completeness:

```
POST /v1/events
Authorization: Bearer <event-key>
```

The body is a single event or an array of events: `{"id"?, "name", "data"?, "ts"?}`. If `id` is absent the platform generates one; if provided, deduplication is idempotent by ID (re-submitting a duplicate returns 200 but does not route it again). Response: `{"ids": [...], "status": 200}`.

See `internal/eventapi/handler.go` for details.

## Admin API (ops/CLI → platform)

Authentication is the same as the Event API (`Authorization: Bearer <event-key>`; a separate admin key will be split out later).

```
GET  /api/v1/apps        → registered apps and their function manifests: {"apps":[{"url","functions":[{"id","event","retries"}]}]}
POST /api/v1/apps        body {"url": "<serve_url>"} → introspect and register; 201 for a new app, 200 if it already exists (synced)
POST /api/v1/apps/sync   body {"url": "<serve_url>"} → sync an already-registered app only; 404 if not registered; 502 if introspection fails
```

Sync semantics replace the entire function set for a serve URL: renamed/deleted functions leave no residue. In-flight runs of removed/renamed functions are marked Failed outright by the platform at the next dispatch (no retries); before rolling out an app upgrade, let in-flight runs converge, or publish under a new function_id.
Static introspection of `-app` URLs at startup is retained and can be mixed with dynamic registration (for the same URL, the later registration overrides the earlier one).

See `internal/appapi/handler.go` for details.

### Admin API: Run/Function Operations (Dashboard basic auth)

The following endpoints sit under the same `/api/v1/*` namespace as the Dashboard's read APIs and are protected by **Dashboard basic auth** (not the event-key Bearer above):

```
POST /api/v1/runs/{id}/replay       → 200 {"run_id": "<new run id>"}; 404 if the run does not exist
POST /api/v1/functions/{id}/pause   → 200 {}
POST /api/v1/functions/{id}/resume  → 200 {}
GET  /api/v1/functions              → each entry additionally carries "paused": bool
```

- **Replay** creates a brand-new run from the original run's triggering-event snapshot and executes it from scratch — no step memos are inherited from the original run.
- **Pause** stops the function from accepting new triggers; in-flight runs continue to completion. **Resume** re-enables triggering.
