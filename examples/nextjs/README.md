# Next.js Example

Demonstrates a Next.js (App Router) app integrating with the TriggerLink platform via `@triggerlink/sdk`, with two functions:

- `process-doc`: a 4-step document processing pipeline (parse → chunk → embed → store), isomorphic to the Go example `examples/pipeline`, triggered from the command line;
- `build-report`: a long-running task triggered by a web button (collect → analyze → render), demonstrating a complete end-to-end application scenario (see below).

## End-to-end web scenario (build-report)

Covers a complete user journey:

```
Click the web button
  → POST /api/report: the app sends a report/requested event to the platform and immediately returns a task_id (without waiting for execution)
  → The platform routes and advances build-report in the background (~6 seconds, 3 steps)
  → The platform Dashboard (http://localhost:8288/dashboard) shows the run: Running → step timeline → Completed + return value
  → The function's last step writes the result back to the app-side task status table (app/lib/tasks.ts)
  → The page polls /api/report?task_id= to learn of completion, displays the result, and automatically performs a follow-up action (POST /api/report/followup, simulating archival)
```

**Steps:**

```bash
# 1. Start the platform (with Dashboard credentials; no -app needed for a local demo — the app self-registers)
go run ./cmd/triggerlink -event-key dev -signing-key dev -dashboard-auth admin:dev

# 2. Start the app (see the "Running" section below); registration succeeds when the terminal shows [triggerlink] registered ...

# 3. Open http://localhost:3000 in a browser and click "Generate Report"
#    - While running: the page shows the current stage; the Dashboard Run list shows a Running run, and the detail page timeline advances step by step
#    - After completion: the Dashboard Run detail shows the return value (report JSON); the page displays the same result and automatically archives it
```

> **Self-registration is a convenience for local development, not the recommended production path.** This example's `instrumentation.ts` makes "start the app → platform immediately usable" zero-touch, but it has inherent timing issues (it must retry while waiting for the platform and the port to be ready), and after a platform restart the in-memory registry is lost while the app is unaware and won't re-register. The recommended production approach is **explicit sync**: after deployment, the deployment pipeline/operator calls `POST /api/v1/apps {"url":"..."}` (the same pattern as Inngest's "Sync app"), or use the platform's `-app` static introspection. Serverless (Vercel etc.) scenarios in particular should avoid self-registration in a startup hook (cold starts trigger it repeatedly per instance).

**How the app "gets notified"**: the M0 platform has no completion-callback webhook. Since durable functions already run inside the app process (the platform's callbacks hit the serve endpoint), the fn of the function's last step writes the result into the app's own storage (here, an in-memory task status table), and the page learns of completion by polling — this is the recommended way for an app to learn of completion/results in M0. Platform-level completion notifications (onFailure callbacks, in-function sendEvent) will come in a later milestone.

Note: `app/lib/tasks.ts` is a demo-grade in-memory implementation (shared via `globalThis`); in production it should be replaced with shared storage such as Redis/DB.

## Running

```bash
# 0. Build the local SDK first (the file: dependency points to ../../sdk-ts)
cd ../../sdk-ts && npm install && npm run build && cd ../examples/nextjs

# 1. Install dependencies and start Next.js (:3000)
npm install
npm run dev

# 2. Start the platform with -app pointing to this app's serve endpoint (the app must be started first)
#    (the app also self-registers on startup, so the -app static introspection here can be omitted; the two are equivalent)
cd ../..
go run ./cmd/triggerlink -event-key dev -signing-key dev \
  -app http://localhost:3000/api/triggerlink

# 3. Trigger (either one)
curl --noproxy '*' -X POST localhost:3000/api/send -d '{"doc_id":"d1"}'   # forwarded via this app
curl --noproxy '*' -X POST localhost:8288/v1/events -H "Authorization: Bearer dev" \
  -d '{"id":"doc-uploaded-d1","name":"doc/uploaded","data":{"doc_id":"d1"}}'  # direct to the platform
```

Watch the Next.js terminal: the platform calls back multiple times, and `EXECUTE parse/chunk/embed/store` each appear once in turn.

## Crash recovery demo

While a callback is executing `embed` (the 2-second slow step), kill `npm run dev` with `Ctrl-C`, then restart it.
After the failed callback the platform automatically retries with backoff; once the app is back, parse/chunk are injected from memoization and not re-run,
and execution resumes from the `embed` breakpoint until completion.

## File overview

- `app/api/triggerlink/route.ts` — serve endpoint: GET introspection + POST execution callbacks; the function definitions also live here
- `app/api/send/route.ts` — demo producer: sends a `doc/uploaded` event to the platform (with an idempotency key)
- `app/api/report/route.ts` — entry point for the web scenario: POST triggers `build-report`, GET polls task status
- `app/api/report/followup/route.ts` — demonstrates the "post-completion follow-up action" (simulated archival)
- `app/ReportPanel.tsx` — web button and polling UI (client component)
- `app/lib/tasks.ts` — app-side task status table (demo-grade in-memory implementation)
- `app/lib/platform.ts` — shared SDK client (used by the serve endpoint, `client.send` for events, and `client.register` for self-registration)
- `instrumentation.ts` — Next.js startup hook; calls `client.register` for self-registration when the app starts (a local-development convenience; explicit sync is recommended in production — see above)

## Deployment notes

- `export const runtime = "nodejs"`: the SDK depends on `node:crypto`, so the Edge runtime cannot be used;
- `export const maxDuration = 300`: each step's duration must be less than the function time limit and the platform callback timeout (default 5 minutes);
- `TRIGGERLINK_SIGNING_KEY` must match the platform's `-signing-key`; on the event-sending side, `TRIGGERLINK_EVENT_KEY` must match the platform's `-event-key`;
- The platform supports dynamic registration: if the app starts later or the function definitions change, call `POST /api/v1/apps {"url":"..."}` (Bearer same as the event key) to register/sync without restarting the platform; the `-app` static introspection at startup is still retained.
