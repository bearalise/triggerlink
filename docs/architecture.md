# TriggerLink Architecture and Module Relationships

> Based on the M0 prototype code. TriggerLink is an open-source, event-driven durable execution platform: write asynchronous workflows as ordinary Go functions that survive crashes and retry at the step level.

## 1. Overall Architecture

The system is split into a **platform side** (a single-process server, `cmd/triggerlink`) and an **application side** (user applications embedding the `sdk`), communicating over HTTP:

```
                        ┌──────────────────────────────────────────────┐
                        │        cmd/triggerlink (platform process)    │
                        │                                              │
  external producers    │  ┌───────────┐   chan   ┌─────────┐          │
  POST /v1/events ──────►│  eventapi   │────────►│ runner  │          │
  (Bearer event-key)    │   Handler   │  stream  │         │          │
                        │  └─────┬─────┘          └────┬────┘          │
                        │        │  InsertEvent        │ CreateRun     │
                        │        │                     │ Enqueue       │
                        │        ▼                     ▼               │
                        │  ┌──────────────────────────────────┐        │
                        │  │     store (Store iface / SQLite) │        │
                        │  │  events / runs / step_runs /     │        │
                        │  │  queue_items                     │        │
                        │  └──────────────▲───────────────────┘        │
                        │                 │ LeaseBatch / SaveStep      │
                        │          ┌──────┴───────┐                    │
                        │          │   executor   │◄── registry        │
                        │          └──────┬───────┘   (function        │
                        └─────────────────┼─────────── routing table)──┘
                                          │ POST callback (HMAC-signed)
                                          ▼
                        ┌──────────────────────────────────────────────┐
                        │         user application process (embeds sdk)│
                        │  Serve handler                               │
                        │    ├─ GET  → function manifest (introspection│
                        │    │        for registration)                │
                        │    └─ POST → execute function                │
                        │          user handler → step.Run ──┐         │
                        │          ↑ recover                 │ panic   │
                        │          │                         │ carries │
                        │          └──── execx.Ctx ◄─────────┘ Opcode  │
                        │                (memo injection / hash count) │
                        └──────────────────────────────────────────────┘
                                          │ returns Opcode JSON
                                          ▼ (back to executor)
```

## 2. Directory and Module Overview

```
cmd/triggerlink/   Platform server entrypoint: assembles store / registry / runner / executor / eventapi
internal/          Platform internals (not exposed to the SDK)
  eventapi/        Event API: POST /v1/events receives events
  runner/          Event routing: match subscribed functions → create run → enqueue
  executor/        Execution engine: lease queue items → HTTP-callback the app → handle opcodes
  registry/        In-memory registry: event name → functions, function ID → serve URL routing
  store/           SQLite storage layer (Store interface + SQLiteStore implementation)
  sign/            Platform→app HMAC-SHA256 request signing / verification
  ids/             Globally unique ID generation (time-ordered prefix + random suffix)
sdk/               Go SDK embedded in user applications (must be independently distributable)
  client.go        Client / Options: app-level configuration (AppID, SigningKey)
  event.go         Event / Input / Function / CreateFunction types
  serve.go         Serve handler: GET manifest introspection + POST execution callback
  sign.go          SDK-side signature verification (intentionally duplicated from internal/sign)
  step/            step.Run: the only durable step primitive
  internal/execx/  Execution context: step hash counting, memo table, opcode types
examples/pipeline/ Example app: 4-step document processing pipeline (with crash demo)
e2e/               End-to-end tests
docs/              Documentation (this document and others)
```

## 3. Platform-Side Module Responsibilities

### 3.1 `internal/eventapi` — Event Ingestion
- Handles `POST /v1/events`, authenticated with a Bearer token (event key); accepts a single event or a batch array.
- Ordering guarantee: **persist with `InsertEvent` first → push into the in-memory stream channel → then return 200** (write-ahead before processing).
- Idempotent dedup on duplicate event IDs (`INSERT OR IGNORE`): re-submissions still return 200 but are not routed again.

### 3.2 `internal/runner` — Event Routing
- Consumes events from the stream and fans out with `registry.Match` to find subscribed functions.
- Creates one `Run` (state Queued) per matched function and enqueues one `QueueItem`, then calls `MarkEventRouted`.
- **Startup reconciliation sweep**: scans `UnroutedEvents` (persisted but not yet routed) and routes them again, covering the crash window.

### 3.3 `internal/executor` — Execution Engine (Core)
- Polls `LeaseBatch` to atomically lease queue items (default lease 30s, batch 16, poll interval 100ms), with per-function-ID concurrency limits (default 100).
- **Startup reconciliation sweep**: `RequeueExpiredLeases` resets items with expired leases back to pending (recovery from server crashes).
- Each dispatch is one HTTP callback:
  1. Load the run and completed step memos, assemble the `callbackRequest` (run_id / attempt / event / steps).
  2. HMAC-sign and POST to the application's serve URL.
  3. Parse the opcode returned by the application and drive the state machine:
     - `StepComplete` → write step memo → enqueue the next hop (advance via re-invocation)
     - `StepError` → write failure memo → back off and retry, or terminate
     - `Sleep` → write sleeping memo → enqueue a wake-up hop with `at=until` (zero resource usage while suspended; on the wake-up dispatch, the sleeping memo is first marked completed before the callback, and since queue items are persisted, the wait naturally survives platform restarts)
     - `SendEvent` → persist the event first, then push to the stream (a missing ID is derived from run+step, so crash retries are idempotent) → write memo (output = list of event IDs) → enqueue the next hop immediately
     - `RunComplete` / `RunError` → run reaches a terminal state
- Retries: backoff `BaseBackoff << attempt` (default starting at 1s, capped at 30s), with a total attempt limit of 4 (in M0, step and run retries share the run.attempt counter).

### 3.4 `internal/registry` — Function Registry
- Thread-safe in-memory tables: `byEvent` (event name → function list) and `byID` (function ID → routing entry).
- At server **startup**, performs one signed GET introspection (`Fetch`) against each `-app` URL to pull the function manifest and build routing; at runtime, apps can be dynamically registered/synced via the management API (`POST /api/v1/apps`, `POST /api/v1/apps/sync`, see `internal/appapi`), which replaces the function set wholesale per serve URL.

### 3.5 `internal/store` — Storage Layer
- The `Store` interface is the sole storage contract between components, implemented by `SQLiteStore` (modernc.org/sqlite, WAL mode, single connection to avoid SQLITE_BUSY).
- Four tables:
  - `events`: event facts, including `routed_at` for reconciliation.
  - `runs`: execution instances, state machine Queued → Running → Completed/Failed; event fields are denormalized so in-flight runs can still execute after events are cleaned up.
  - `step_runs`: the memoization core, primary key `(run_id, step_hash)`, upsert on conflict with attempt+1.
  - `queue_items`: the queue; `score` defines FIFO order, `at` is the "not before" time (backoff), `lease_expires` implements leasing; items are DELETEd on completion.

### 3.6 `internal/sign` and `internal/ids`
- `sign`: HMAC-SHA256 signing; header format `t=<unix seconds>,v1=<hex(hmac(key, "<t>.<body>"))>`; verification applies timestamp tolerance to prevent replay.
- `ids`: IDs of the form `<prefix>_<millisecond timestamp><16 random hex>`, with no third-party dependencies.

## 4. SDK-Side Module Responsibilities

### 4.1 `sdk` (package name `triggerlink`)
- `NewClient` / `CreateFunction`: define durable functions (ID + subscribed events + handler).
- `Serve(client, fns...)`: a handler mounted on the application's HTTP server (e.g. `/api/triggerlink`):
  - `GET` → verify signature, then return the function manifest (for platform introspection/registration).
  - `POST` → verify signature, parse the callback request, build an `execx.Ctx` to run the user handler, recover the Opcode panicked by a step and serialize it back; an uncaught panic becomes `RunError`.
- `sdk/sign.go`: a verification implementation algorithmically identical to `internal/sign`. **Intentionally duplicated** — the SDK must be independently distributable and cannot import the platform's `internal/`.

### 4.2 `sdk/step` — The Durable Step Primitive
`step.Run[T](ctx, id, fn)` plus `step.Sleep/SleepUntil`. Mechanism:
1. Compute the memo key (see execx).
2. **Memo hit** → deserialize and return the cached output directly, without re-running fn.
3. **Memo miss** → run fn, then interrupt the entire function with `panic(Opcode)`; Serve recovers it and serializes it for the platform to persist; the platform then issues the next re-invocation to advance to the next step.

### 4.3 `sdk/internal/execx` — Execution Context (SDK Internal)
- `Ctx`: carries the completed step memos injected by the platform (`Steps`) and a per-stepID invocation counter.
- `NextHash`: memo key = `sha256(functionID + ":" + stepID + ":" + sequence number)`, so the same stepID appearing multiple times in a loop still gets stable keys.
- Where the constraint comes from: the step invocation sequence must be deterministic (branches/loops may only depend on event data and the outputs of completed steps), otherwise memo keys will not line up during recovery.

## 5. Key Data Flows

### 5.1 Normal Execution (Full Lifecycle of One Run)
```
producer → eventapi: persist + stream
         → runner: Match → CreateRun(Queued) → Enqueue
         → executor: Lease → POST callback (carrying completed step memos)
         → sdk Serve: handler runs to the first un-memoized step → panic StepComplete
         → executor: store step memo → Enqueue next hop
         → … (one re-invocation per step) …
         → sdk: handler returns normally → RunComplete → executor: CompleteRun
```

### 5.2 Crash Recovery
- **Application crash** (mid-step): the HTTP callback fails/times out → executor backs off and retries → on the re-callback, completed steps are skipped via memo injection, and the run resumes from the interruption point.
- **Platform crash**: on restart, the runner's reconciliation sweep re-routes unrouted events, and the executor's reconciliation sweep re-enqueues items with expired leases (default 30s).

## 6. Module Dependency Graph

```
cmd/triggerlink
  └─► internal/{eventapi, runner, executor, registry, store}

internal/eventapi  ─► internal/store, internal/ids
internal/runner    ─► internal/store, internal/registry, internal/ids
internal/executor  ─► internal/store, internal/registry, internal/sign, internal/ids
internal/registry  ─► internal/sign
internal/store     ─► (database/sql + modernc.org/sqlite)
internal/sign      ─► (standard library)
internal/ids       ─► (standard library)

sdk (triggerlink)  ─► sdk/internal/execx
sdk/step           ─► sdk/internal/execx
sdk/internal/execx ─► (standard library)
examples/pipeline  ─► sdk, sdk/step
```

Design notes:
- `internal/*` is assembled only by `cmd/triggerlink`; components are decoupled through the `store.Store` interface and `registry.Registry`; **all cross-component state is persisted through the store**, and the only volatile in-memory structures are the stream channel and the registry (both backed by startup reconciliation sweeps).
- The `sdk` imports none of `internal/*` (Go internal package rules + the independent-distribution requirement); the platform and SDK are coupled only through the JSON protocol (callbackRequest / Opcode) and two isomorphic implementations of the signing algorithm.
- The protocol types (stepState / Opcode / callbackRequest) in `internal/executor`, `sdk/internal/execx`, and `sdk/serve.go` must remain strictly isomorphic — they are the de facto interface between the two sides.
