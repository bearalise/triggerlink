# TriggerLink User Guide

> An integration guide for **users** of the platform: whether your system should use TriggerLink, and how. Based on the M0 release.
> For the architecture, see [architecture.md](architecture.md).

## 1. Why TriggerLink

Start with a requirement almost every backend system runs into:

> After a user pays successfully, you need to: create a shipment → send an SMS notification → sync to the CRM → update reports.
> The logistics API times out occasionally, the SMS platform occasionally returns 5xx, and a failure in any step must not cause the earlier steps to re-run (the shipment must not be created twice).

The traditional approaches all hurt:

| Approach | Problem |
|---|---|
| Synchronous calls inside the payment API | The API gets slowed down and dragged down; when a third party goes down, payment fails too |
| Message queue + consumers | Every flow needs its own consumer, state tracking, retry backoff, and dead-letter handling; flow state is scattered across code and the database, and when something crashes halfway nobody knows which steps already ran |
| Polling a task table in the database | Essentially building your own task queue — you have to get leases, concurrency, and backoff right yourself |
| Cron scripts as a fallback re-run | High latency, and you still have to maintain "which order reached which step" yourself |

What these problems have in common: **driving a multi-step business process forward, retrying failures, recovering from crashes, and recording whether each step "already ran" are all boilerplate engineering unrelated to your business**.

TriggerLink takes that part over. You only need to:

```go
fn := triggerlink.CreateFunction(
    triggerlink.FunctionOpts{ID: "fulfill-order", Event: "order/paid"},
    func(ctx context.Context, in triggerlink.Input) (any, error) {
        // Write the flow like ordinary sequential code; wrap each side effect in a step.Run
        tracking, err := step.Run(ctx, "create-shipment", func(ctx context.Context) (string, error) {
            return logisticsAPI.CreateShipment(orderID)
        })
        if err != nil {
            return nil, err // just return the error; retries are the platform's job
        }
        _, err = step.Run(ctx, "send-sms", func(ctx context.Context) (any, error) {
            return nil, sms.Send(orderID, tracking)
        })
        return map[string]any{"tracking": tracking}, err
    },
)
```

The guarantees you get:

- **Step-level memoization**: once `create-shipment` succeeds, even if the process crashes a second later, the recovered run will not call the logistics API again — completed step results are persisted by the platform and injected back.
- **Automatic retry on failure**: if any step returns an error or the process crashes, the platform retries automatically with exponential backoff (up to 4 attempts by default) — no retry code to write.
- **Events are never lost**: events are written to SQLite before being routed; after a platform or application crash and restart, reconciliation picks them up and resumes automatically.
- **Minimal producer footprint**: a system in any language can trigger a flow with a single HTTP POST, with idempotent deduplication supported.

In one sentence: **you write sequential code; the platform turns it into crash-recoverable, retryable durable execution**.

## 2. Three Typical Scenarios (Decide Whether It Fits You)

### Scenario A: Event-driven multi-step business processes

**Pain point**: after a core event such as an order, payment, or approval occurs, you need to chain multiple unreliable third parties (logistics, SMS, CRM, invoicing). A hiccup in any link must not affect the main flow, and steps that already succeeded must not run again.

**Usage**: the main system (payment service) only emits one event, `order/paid`; the fulfillment service defines a function subscribing to that event and wraps each third-party call in a step. If a step fails, only that step is retried — the main system is unaware.

**Result**: the main flow is decoupled from fulfillment; third-party outages are downgraded from "production incident" to "a retry record in the platform logs". For the full code, see [Section 7](#7-end-to-end-example-order-fulfillment-pipeline).

### Scenario B: Long-running AI / data processing pipelines

**Pain point**: after a document is uploaded, you need to parse → chunk → call the embedding API (slow and expensive) → store. The embedding step takes minutes; if the process restarts or times out midway, re-running from scratch is both slow and costly.

**Usage**: one step per stage. Once the 10-minute embedding completes, its result is written to the memo; on any subsequent retry for any reason, parse, chunk, and embedding are injected directly without re-running — only the failed store step resumes.

**Result**: the failure cost of a long pipeline drops from "redo everything" to "resume from the breakpoint". `examples/pipeline/main.go` in the repo is a runnable example of exactly this scenario (use `TRIGGERLINK_CRASH_AT=embed` to inject a crash and experience it firsthand).

### Scenario C: Event decoupling across heterogeneous systems

**Pain point**: the company has a legacy PHP system, Python crawlers, and third-party webhooks; the events they produce need to trigger follow-up flows written in Go. Making the legacy system adopt an MQ SDK is unrealistic, and having the Go service poll everywhere is inelegant.

**Usage**: any system that can send an HTTP POST is a TriggerLink producer. PHP uses `curl`, Python uses `requests`, and a webhook receiver just forwards. Event IDs provide idempotent deduplication, so producers can retry safely.

**Result**: between old and new systems there is only a single HTTP-protocol seam; the reliability of everything downstream is covered by the platform. See [Section 6](#6-sending-events-from-other-systems-producer-side) for examples in each language.

**When you don't need TriggerLink**: for a simple async task that completes in a single call (sending one email), a goroutine or a single MQ message is enough; scenarios requiring complex DAG orchestration or human-in-the-loop approval flows are beyond M0's capabilities (see [Section 9](#9-m0-boundaries-planned-capabilities)).

## 3. The Three Roles You Need to Understand

| Role | What it is | Who deploys it |
|---|---|---|
| **Platform** | `cmd/triggerlink`, a single-process server (event API + queue + execution engine + SQLite) | The platform operator, one deployment |
| **Application** | Your Go service, embedding the `sdk` package, defining durable functions and exposing a serve endpoint | Each business team, deployed independently |
| **Producer** | Any system that can send HTTP, pushing events to the platform's `POST /v1/events` | Any language, any system |

The typical relationship: **the producer sends an event → the platform persists and routes it → the platform calls the application back over HTTP to execute the function → inside the application, `step.Run` writes a memo for each step, and crashes resume automatically**.

## 4. Deploying the Platform

```bash
go run ./cmd/triggerlink start \
  -addr :8288 \
  -db triggerlink.db \
  -event-key <producer auth key> \
  -signing-key <platform↔app shared signing key> \
  -app http://app1-host:8080/api/triggerlink \
  -app http://app2-host:9090/api/triggerlink   # repeatable, to connect multiple apps
```

Subcommands: `start` runs the production server (both keys required); `dev` runs the same kernel for local development and auto-generates any missing `-event-key` / `-signing-key` (printed to the startup logs). Invoking the binary with flags only (no subcommand) behaves like `start`.

Flag reference:

| Flag | Default | Description |
|---|---|---|
| `-addr` | `:8288` | Event API listen address |
| `-db` | `triggerlink.db` | SQLite file path (WAL mode; a single file is enough) |
| `-event-key` | (required for `start`) | Bearer key for producers calling `POST /v1/events` |
| `-signing-key` | (required for `start`) | HMAC signing key used when the platform calls back applications; **must match the application's `SigningKey`** |
| `-app` | none | Application serve URL; repeat to connect multiple applications |

Notes:

- **Applications can register dynamically while the platform is running**: `POST /api/v1/apps {"url":"..."}` (same Bearer as the event key). Static introspection via `-app` at platform startup is still supported; after changing function definitions, call `POST /api/v1/apps/sync` — no platform restart needed. Registration info is persisted to the platform database and restored automatically after a platform restart, so no re-registration is required. Events are routed as of the moment they are received: events sent before registration completes are not buffered and will not trigger functions registered later.
- If the platform crashes, just restart it: the startup reconciliation sweep picks up unrouted events and runs whose leases have expired (the default lease is 30s, so a run waits at most 30s before being rescheduled).
- An application crash needs no intervention either: after the callback fails the platform retries with backoff, and once the app recovers the run resumes from the step breakpoint.

### 4.1 Dashboard

The platform ships with a built-in Dashboard at `http://localhost:8288/dashboard` (the browser will prompt with a basic auth login).

- Credential configuration: the `-dashboard-auth user:password` startup flag, or the `TRIGGERLINK_DASHBOARD_AUTH` environment variable; if neither is set, the startup logs print a one-time random credential (suitable for intranet/local use only).
- Four pages: **Run list** (filter by function/status), **Run detail** (step timeline with each step's output/error/retry count), **Event stream** (payloads with backlinks to the runs they triggered), **Function list** (24h success rate).
- Data refreshes automatically (2–5s polling); the detail page stops polling once a run reaches a terminal state.
- **Cancelling a run**: the run detail page shows a **Stop run** button while the run is Queued/Running (e.g. stuck retrying against a failing app). Cancelling marks the run `Cancelled` (a terminal state), drops its pending queue items, and discards any in-flight callback result — no further retries or steps will execute.
- **Replaying a run**: the run detail page shows a **Replay** button once the run reaches a terminal state. Replaying creates a brand-new run from the original run's triggering-event snapshot and executes it from scratch — no step memos are inherited (completed steps of the original run will run again). The page jumps to the new run's detail automatically.
- **Pausing a function**: each row on the function list page has a **Pause/Resume** button and a `paused` badge. Pausing stops the function from accepting new triggers; in-flight runs continue to completion. Resume re-enables triggering.

The corresponding admin API (also protected by basic auth, usable from scripts):

```bash
curl -u user:password localhost:8288/api/v1/runs?status=running
curl -u user:password localhost:8288/api/v1/runs/<run_id>
curl -u user:password localhost:8288/api/v1/events
curl -u user:password localhost:8288/api/v1/functions
# Cancel a non-terminal run (409 if already terminal)
curl -u user:password -X POST localhost:8288/api/v1/runs/<run_id>/cancel
# Replay a run from scratch: 200 {"run_id": "<new run id>"}; 404 if no such run
curl -u user:password -X POST localhost:8288/api/v1/runs/<run_id>/replay
# Pause / resume a function's triggering (in-flight runs are unaffected)
curl -u user:password -X POST localhost:8288/api/v1/functions/<function_id>/pause
curl -u user:password -X POST localhost:8288/api/v1/functions/<function_id>/resume
```

## 5. Integrating into Your Application (Application Side)

### 5.1 Minimal Integration (Standard Library net/http)

```go
package main

import (
    "context"
    "log"
    "net/http"

    triggerlink "github.com/bearalise/triggerlink/sdk"
    "github.com/bearalise/triggerlink/sdk/step"
)

func main() {
    client := triggerlink.NewClient(triggerlink.Options{
        AppID:      "myservice",
        SigningKey: "same key as the platform's -signing-key",
    })

    fn := triggerlink.CreateFunction(
        triggerlink.FunctionOpts{
            ID:      "handle-signup",   // stable identifier; renaming loses historical memos
            Event:   "user/signup",     // the event name to subscribe to
            Retries: 4,                 // 0 = platform default of 4
        },
        func(ctx context.Context, in triggerlink.Input) (any, error) {
            email := in.Event.Data // json.RawMessage; decode it yourself

            // One step per side effect: if SendEmail crashes, retries won't re-run completed steps
            if _, err := step.Run(ctx, "send-welcome-email", func(ctx context.Context) (any, error) {
                return nil, sendWelcomeEmail(email)
            }); err != nil {
                return nil, err
            }

            if _, err := step.Run(ctx, "create-crm-contact", func(ctx context.Context) (any, error) {
                return nil, createCRMContact(email)
            }); err != nil {
                return nil, err
            }

            return map[string]any{"ok": true}, nil
        },
    )

    http.Handle("/api/triggerlink", triggerlink.Serve(client, fn))
    log.Fatal(http.ListenAndServe(":8080", nil))
}
```

Key points:

- `Serve` returns a standard `http.Handler`; one endpoint hosts all functions — `GET` answers the introspection manifest, `POST` answers execution callbacks.
- The platform re-invokes the handler once per step it advances (re-invocation): the handler executes from the top, but completed steps return directly from the memo and **do not re-run**.
- `step.Run` is generic: the return value of `step.Run(ctx, "load", func(ctx) (User, error) {...})` is persisted and injected on recovery.

### 5.2 Integrating Frameworks like Gin / Echo

`Serve` is a standard handler — mount it with the framework's adapter:

```go
// Gin
r := gin.Default()
r.Any("/api/triggerlink", gin.WrapH(triggerlink.Serve(client, fn1, fn2)))
```

### 5.3 Usage Constraints of step.Run (Important)

The function is re-entered repeatedly (once per step plus once per retry), so:

- **Side effects must go inside `step.Run`**: sending email, writing to the database, calling third-party APIs. Code placed outside runs on every re-invocation.
- **The step call sequence must be deterministic**: branches and loops may only depend on event data and the return values of completed steps — not on time, randomness, or external state. Otherwise the memo keys won't line up on recovery and steps will be treated as new and re-run.
- Keep the glue code around steps (the orchestration logic) lightweight; the heavy work belongs inside steps.
- Reusing the same step ID inside a loop is allowed (the SDK distinguishes occurrences with `sha256(functionID:stepID:ordinal)`), but the loop count must be deterministic:

```go
for _, item := range order.Items { // order.Items comes from event data — deterministic
    _, err := step.Run(ctx, "charge-item", func(ctx context.Context) (any, error) {
        return nil, charge(item)
    })
    if err != nil {
        return nil, err
    }
}
```

### 5.3.1 step.Sleep / step.SleepUntil (Durable Delays)

`step.Sleep(ctx, "wait-one-day", 24*time.Hour)` suspends the run until the specified time: the function exits, and the platform calls back to resume it when the time comes; **while suspended it holds no connection or compute**, and a platform restart loses nothing (queue items are persisted). The memo semantics match `step.Run`: when the run is re-entered after resuming, the sleep step returns immediately instead of sleeping again. For an absolute time, use `step.SleepUntil(ctx, id, t)`; the TS SDK equivalents are `step.sleep(id, ms)` / `step.sleepUntil(id, at)`.

```go
_, err := step.Run(ctx, "send-welcome", func(ctx context.Context) (any, error) {
    return sendWelcomeEmail(email)
})
step.Sleep(ctx, "wait-one-day", 24*time.Hour) // suspend for a day, zero resource usage
_, err = step.Run(ctx, "send-reminder", func(ctx context.Context) (any, error) {
    return sendReminder(email)
})
```

### 5.3.2 step.SendEvent (Fan-out from Within a Function)

`step.SendEvent(ctx, "notify", step.Event{Name: "user/greeted", Data: ...})` reliably fans out events from within a function: the platform persists them before routing (crash-safe), and functions subscribed to the event are triggered right away. The memo semantics match `step.Run`: when the run is re-entered after recovery, it returns the IDs of the already-sent events (`[]string`) without sending again. If the event `ID` is omitted, the platform derives it deterministically from `(run_id, step_hash, ordinal)` — crash retries are naturally idempotent; supplying your own business-unique key is even stronger. The TS SDK equivalent is `step.sendEvent(id, events)` (single or array), returning `Promise<string[]>`.

### 5.3.3 step.WaitForEvent (Wait for Another Event)

`step.WaitForEvent` suspends the run until a matching event arrives or a timeout elapses — the classic "wait for the payment callback after creating the order" pattern. Like `step.Sleep`, suspension holds no connection or compute; unlike `Sleep`, resumption is driven by event routing (or the timeout scan), not the queue. The memo semantics match the other primitives: on re-entry the matched event is injected directly instead of waiting again.

```go
ev, err := step.WaitForEvent(ctx, "wait-payment", step.WaitOpts{
    Event:   "order/payed",                              // required: the event name to wait for
    Match:   "data.order_id == event.data.order_id",     // optional: fine-grained match (expr-lang)
    Timeout: 7 * 24 * time.Hour,                         // optional: nil return on timeout
})
if err != nil {
    return nil, err
}
if ev == nil {
    return map[string]any{"status": "payment timeout"}, nil // timeout branch
}
// ev is *triggerlink.Event {ID, Name, Data, TS} — the matched event
```

In the `Match` expression, `data` is the arriving event's `data` and `event` is this run's triggering event `{name, data}` — the example above means "only the `order/payed` event whose `order_id` equals this order's". The TS SDK equivalent is `step.waitForEvent(id, { event, match?, timeout? })`, returning `Promise<TriggerLinkEvent | null>`; `timeout` accepts a Go duration string (`"168h"`) or a millisecond number (`1500` → `"1.5s"`).

### 5.3.4 CancelOn (Cancelling In-Flight Runs by Event)

`CancelOn` in `FunctionOpts` declares cancellation rules that ship with the manifest: when the named event arrives (and the optional `match` expression hits), the platform cancels that function's in-flight (Queued/Running) runs — e.g. cancel the onboarding flow when the account is deleted:

```go
fn := triggerlink.CreateFunction(
    triggerlink.FunctionOpts{
        ID: "onboarding", Event: "user/signup",
        CancelOn: []triggerlink.CancelOn{
            {Event: "user/deleted", Match: "data.user_id == event.data.user_id"},
        },
    },
    handler,
)
```

The `Match` environment is the same as `step.WaitForEvent`: `data` = the arriving event's `data`, `event` = the run's triggering event `{name, data}`; an empty `Match` cancels on arrival. The TS SDK equivalent is `cancelOn: [{ event: "user/deleted", match: "..." }]` in `createFunction` options. Note the rules are evaluated against runs of the function that carries them — a run already in a terminal state is unaffected.

### 5.3.5 Match / Cron (Trigger Conditions)

`Match` narrows event triggering, and `Cron` adds a schedule trigger — both ship with the manifest:

```go
fn := triggerlink.CreateFunction(
    triggerlink.FunctionOpts{
        ID: "nightly-report", Event: "report/generate",
        Match: "data.kind == 'full'", // only events whose data.kind is "full" trigger
        Cron:  "0 9 * * *",           // also run every day at 09:00 UTC
    },
    handler,
)
```

```ts
const fn = createFunction(
  {
    id: "nightly-report",
    event: "report/generate",
    match: "data.kind == 'full'",
    cron: "0 9 * * *",
  },
  handler,
);
```

Notes:

- `Match` is an expr-lang expression whose environment contains only `data` (= the event's `data`) — unlike `CancelOn`/`WaitForEvent`, there is no `event` variable.
- `Cron` is a standard 5-field cron (minute hour day-of-month month day-of-week) interpreted in **UTC** with one-minute granularity; ticks missed while the platform is down are **not** backfilled — scheduling resumes from the next tick after a restart.

### 5.3.6 OnFailure (Run-Failure Handler)

`OnFailure` declares a handler that runs when the function's run reaches the Failed terminal state (all retries exhausted) — for alerts, compensating actions, and the like. The SDK registers an implicit function `<id>/on-failure` (subscribing to the internal event `triggerlink/run.failed`, filtered by `function_id`); it shows up in the manifest and Dashboard, and inside the handler you can use all step primitives:

```go
fn := triggerlink.CreateFunction(
    triggerlink.FunctionOpts{
        ID: "process-doc", Event: "doc/uploaded",
        OnFailure: func(ctx context.Context, in triggerlink.Input) (any, error) {
            // in.Event.Data = {"run_id","function_id","error","event":{...triggering event...}}
            _, err := step.Run(ctx, "alert", func(ctx context.Context) (any, error) {
                return nil, alertOps("process-doc failed: " + gjson.GetBytes(in.Event.Data, "error").String())
            })
            return nil, err
        },
    },
    handler,
)
```

```ts
const fn = createFunction(
  {
    id: "process-doc",
    event: "doc/uploaded",
    onFailure: async ({ event, step }) => {
      // event.data = { run_id, function_id, error, event: {...triggering event...} }
      await step.run("alert", () => alertOps(`process-doc failed: ${event.data.error}`));
    },
  },
  handler,
);
```

The implicit function's own failure does **not** recursively trigger another failure handler (no re-entry).

### 5.3.7 Timeout (Run-Level Timeout)

`Timeout` in `FunctionOpts` caps how long a single run may stay alive, counted from run creation: if the run is still Queued/Running when the timeout elapses, the platform marks it `Failed` and emits the internal `triggerlink/run.failed` event — so a function with an `OnFailure` handler will have it fire on timeout (FR-4.3). The default (0 / unset) is no time limit. This is a run-level cap, distinct from the per-callback HTTP timeout (a single step's execution must return within 5 minutes):

```go
fn := triggerlink.CreateFunction(
    triggerlink.FunctionOpts{
        ID: "slow-etl", Event: "etl/start",
        Timeout: 30 * time.Minute, // any run alive longer than 30m is failed
    },
    handler,
)
```

```ts
// timeout accepts a Go duration string ("30m") or a millisecond number
const fn = createFunction({ id: "slow-etl", event: "etl/start", timeout: "30m" }, handler);
```

### 5.4 Error Handling Semantics

- A step returning an error → the platform records a failure memo and retries with backoff (starting at 1s, exponential, capped at 30s); after **4 total attempts by default** the run is marked Failed.
- The handler returning an error or panicking → treated as a run-level error and retried the same way.
- In M0, step and run retries share the `run.attempt` counter, and there is no "non-retryable error" marker yet — swallow and log unrecoverable errors inside your function yourself.

## 6. Sending Events from Other Systems (Producer Side)

Any system only needs a single HTTP call:

```
POST http://<platform host>:8288/v1/events
Authorization: Bearer <event-key>
Content-Type: application/json
```

Event fields:

| Field | Required | Description |
|---|---|---|
| `name` | yes | Event name, e.g. `user/signup`; matched against functions' `Event` (fan-out: multiple functions may subscribe to the same event name) |
| `data` | no | Arbitrary JSON, defaults to `{}`; passed through to the function as-is |
| `id` | no | Event ID; **when provided, the platform deduplicates idempotently by ID** — producer retries are safe, and bringing your own business-unique key is strongly recommended |
| `ts` | no | Event time (RFC3339); defaults to the platform's receive time |

Response: `{"ids": ["..."], "status": 200}`. A duplicate ID also returns 200 but does not trigger again.

Batch is supported: pass an array directly as the request body, `[{...}, {...}]`.

### 6.1 curl

```bash
# If your shell has http_proxy configured, internal calls need --noproxy '*'
curl --noproxy '*' -X POST http://localhost:8288/v1/events \
  -H "Authorization: Bearer dev" \
  -H "Content-Type: application/json" \
  -d '{
    "id":   "order-12345-paid",
    "name": "order/paid",
    "data": {"order_id": "12345", "amount": 9900, "currency": "CNY"}
  }'
```

### 6.2 Go

```go
func emitEvent(ctx context.Context, name, id string, data any) error {
    payload, _ := json.Marshal(map[string]any{"id": id, "name": name, "data": data})
    req, _ := http.NewRequestWithContext(ctx, http.MethodPost,
        "http://triggerlink:8288/v1/events", bytes.NewReader(payload))
    req.Header.Set("Authorization", "Bearer "+eventKey)
    req.Header.Set("Content-Type", "application/json")
    resp, err := http.DefaultClient.Do(req)
    if err != nil {
        return err
    }
    defer resp.Body.Close()
    if resp.StatusCode != http.StatusOK {
        return fmt.Errorf("emit event: status %d", resp.StatusCode)
    }
    return nil
}
```

### 6.3 Python

```python
import requests

def emit_event(name: str, event_id: str, data: dict):
    resp = requests.post(
        "http://triggerlink:8288/v1/events",
        headers={"Authorization": f"Bearer {EVENT_KEY}"},
        json={"id": event_id, "name": name, "data": data},
        timeout=5,
    )
    resp.raise_for_status()

# Example: the order system triggers the fulfillment flow after a successful payment
emit_event("order/paid", f"order-{order_id}-paid", {"order_id": order_id})
```

### 6.4 Node.js

```javascript
async function emitEvent(name, id, data) {
  const resp = await fetch("http://triggerlink:8288/v1/events", {
    method: "POST",
    headers: {
      Authorization: `Bearer ${process.env.TRIGGERLINK_EVENT_KEY}`,
      "Content-Type": "application/json",
    },
    body: JSON.stringify({ id, name, data }),
  });
  if (!resp.ok) throw new Error(`emit event: ${resp.status}`);
}
```

### 6.5 Integration Ideas for Other Systems

- **Database / CDC**: in Debezium, triggers, or an outbox polling program, turn changed rows into events POSTed to the platform; use `table-primaryKey-operation` as the `id` for idempotency.
- **Message queue bridging**: turn existing Kafka/RabbitMQ consumers into forwarders that convert messages into events as-is; with event ID deduplication, at-least-once delivery by the bridge is safe.
- **Webhook ingress**: the platform has built-in signed webhook endpoints (see Section 6.6); if you run your own webhook-receiving service, authenticate there and forward as TriggerLink events to gain retries and crash recovery without implementing your own task queue.
- **Scheduled jobs**: M0 has no Cron; use system cron + curl to emit events on a schedule instead (see Section 9).

### 6.6 Webhook Trigger Endpoints (FR-3.4)

For external systems that can only call a URL (Stripe, GitHub, ...), create a dedicated webhook endpoint instead of handing out the event-key. Security relies entirely on an HMAC signature — no basic auth.

Create one via the management API (Dashboard basic auth); the secret is returned in full **only** in the creation response:

```bash
# Dashboard basic auth 凭据见 4.1 节
curl --noproxy '*' -u admin:<password> -X POST http://localhost:8288/api/v1/webhooks \
  -H "Content-Type: application/json" \
  -d '{"event_name": "stripe/payment"}'           # secret 缺省自动生成，也可显式指定
# → {"id":"whk_...","url":"/hooks/whk_...","event_name":"stripe/payment","secret":"whsec_..."}

curl --noproxy '*' -u admin:<password> http://localhost:8288/api/v1/webhooks
# → {"webhooks":[{"id","url","event_name","created_at"}]}   （列表不回显 secret）

curl --noproxy '*' -u admin:<password> -X DELETE http://localhost:8288/api/v1/webhooks/whk_...
# → 200 {}（不存在则 404）
```

The external system then POSTs to the dedicated URL, signed with the webhook's secret (same format as the platform↔app signature):

```
POST http://localhost:8288/hooks/whk_...
X-TriggerLink-Signature: t=<unix seconds>,v1=<hex(hmac_sha256(secret, "<t>" + "." + "<body>"))>
```

```bash
BODY='{"amount":9900,"currency":"CNY"}'
T=$(date +%s)
SIG=$(printf '%s.%s' "$T" "$BODY" | openssl dgst -sha256 -hmac "$SECRET" | awk '{print $NF}')
curl --noproxy '*' -X POST http://localhost:8288/hooks/whk_... \
  -H "X-TriggerLink-Signature: t=$T,v1=$SIG" \
  -d "$BODY"
# → 200 {"id":"evt_..."}
```

Behavior notes:

- The timestamp must be within **±5 minutes** (replay protection); bad/missing signature → 401, unknown webhook ID → 404.
- The body must be JSON: a JSON object becomes the event's `data` verbatim; any other JSON value is wrapped as `{"payload": <value>}`; non-JSON → 400.
- After verification the platform **persists the event first, then routes it** (same ordering guarantee as `/v1/events`) with a freshly generated `evt_*` ID; name is the webhook's configured `event_name`. Functions subscribing to that event name are triggered normally.

## 7. End-to-End Example: Order Fulfillment Pipeline

Recalling Scenario A: after a successful payment, create a shipment → notify by SMS; the logistics API is unreliable, and the shipment must never be created twice.

Application side (the `fulfillment` service):

```go
client := triggerlink.NewClient(triggerlink.Options{AppID: "fulfillment", SigningKey: key})

fn := triggerlink.CreateFunction(
    triggerlink.FunctionOpts{ID: "fulfill-order", Event: "order/paid"},
    func(ctx context.Context, in triggerlink.Input) (any, error) {
        var order struct {
            OrderID string `json:"order_id"`
            Amount  int    `json:"amount"`
        }
        if err := json.Unmarshal(in.Event.Data, &order); err != nil {
            return nil, err
        }

        tracking, err := step.Run(ctx, "create-shipment", func(ctx context.Context) (string, error) {
            return logisticsAPI.CreateShipment(order.OrderID) // crashed here? on retry, tracking is injected directly — no duplicate shipment
        })
        if err != nil {
            return nil, err
        }

        if _, err = step.Run(ctx, "notify-user", func(ctx context.Context) (any, error) {
            return nil, sms.Send(order.OrderID, tracking)
        }); err != nil {
            return nil, err
        }

        return map[string]any{"tracking": tracking}, nil
    },
)

http.Handle("/api/triggerlink", triggerlink.Serve(client, fn))
```

The order system (any language), after a successful payment:

```bash
curl -X POST http://triggerlink:8288/v1/events \
  -H "Authorization: Bearer $EVENT_KEY" \
  -d '{"id":"order-12345-paid","name":"order/paid","data":{"order_id":"12345","amount":9900}}'
```

The result: the event is persisted before the response returns, and fulfillment executes asynchronously; logistics API timeouts are retried automatically with backoff; after an application restart the run resumes from the `create-shipment` breakpoint without creating a duplicate shipment.

For a complete runnable example in the repo, see `examples/pipeline/main.go` (a 4-step document processing pipeline with a `TRIGGERLINK_CRASH_AT` crash-injection demo).

### 7.1 A Complete Web-Triggered Loop (Next.js Example)

`examples/nextjs` demonstrates a more complete application path that you can drive directly from a browser:

1. The application defines a long-running function `build-report` and **self-registers with the platform at startup via `POST /api/v1/apps`** (`instrumentation.ts` — a local-development convenience; for production, explicit sync or `-app` static introspection is recommended — see the notes below);
2. A button on the web page → the application sends a `report/requested` event to the platform and **returns immediately** with a `task_id` (the function executes asynchronously without blocking the web request);
3. While it runs, open the platform Dashboard to see the run in Running state with the step timeline advancing;
4. When the function completes, the Dashboard run detail shows the return value; **the application learns of completion via "the last step writes the result into the application's own storage + page polling"**, then performs follow-up actions (simulated archiving in the example).

On "the application gets notified" in step 4: the M0 platform has no completion-callback webhook. Since the function already runs inside your application process (the platform's callbacks arrive at the serve endpoint), writing the result into the application's own database/cache inside the last step is the recommended way to learn about completion and results in M0 (keep it idempotent and retry-safe). Platform-level completion/failure notification — via an onFailure callback and in-function `sendEvent` — will be provided in a later milestone.

On "self-registration" in step 1: it is purely a local-development convenience (works at startup, zero platform configuration) and has an inherent timing problem (it must retry while waiting for the platform and the port to be ready). **For production, explicit sync is recommended**: after deployment, have the deployment pipeline call `POST /api/v1/apps` (the same pattern as Inngest's "Sync app"), or use the platform's `-app` static introspection; serverless scenarios in particular should avoid startup-hook self-registration (cold starts trigger it repeatedly per instance). Registration info — whether it comes from the admin API, self-registration, or `-app` introspection — is persisted to the platform database, and after a platform restart the registry is restored automatically from the database (look for `restored N app registration(s) from db` in the logs), so no re-registration is needed. Automatic dev-server port discovery will be provided in a later milestone.

## 8. Operations and Troubleshooting

- **Platform logs**: registration (`registered function ...`), reconciliation (`reconciled N ...`), and every retry and termination are logged — when troubleshooting, look at the platform output first.
- **Debugging "the event didn't trigger"**: check in order: ① the producer received a 200; ② the function appears as registered in the platform startup logs (did the application start before the platform?); ③ the event name matches the function's `Event` exactly; ④ whether that event ID was already sent before (idempotent deduplication).
- **Inspecting state**: use the built-in Dashboard (`http://localhost:8288/dashboard`, see the Dashboard section above) or the read-only admin API; you can also query SQLite directly:
  ```bash
  sqlite3 triggerlink.db \
    "SELECT id, function_id, status, attempt, error FROM runs ORDER BY created_at DESC LIMIT 10;"
  sqlite3 triggerlink.db "SELECT * FROM step_runs WHERE run_id='<run_id>';"
  ```
- **Re-running a failed run**: use the Dashboard's **Replay** button on the run detail page, or `POST /api/v1/runs/<run_id>/replay`. This starts a fresh run from the original triggering event with no step memos carried over — every step re-executes, so only replay functions whose steps are safe to re-run (or whose side effects are idempotent).
- **Stopping new work during an incident**: pause the affected function (Dashboard function list, or `POST /api/v1/functions/<function_id>/pause`) to stop accepting new triggers while in-flight runs finish; resume with `POST /api/v1/functions/<function_id>/resume` once the downstream issue is resolved.
- **Security**: `-event-key` protects the event ingress; `-signing-key` protects both directions of the platform↔application link (callback signing + introspection signing). Both keys should come from environment variables / a secrets manager — never hardcode them.

## 9. M0 Boundaries (Planned Capabilities)

The following capabilities are **not available** in the current version:

- Cron-based scheduled triggers (use an external cron emitting events instead)
- Postgres backend, multi-replica platform (currently single-process + SQLite)
