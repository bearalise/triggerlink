# TriggerLink Deployment Guide (Fresh Environment)

> Goal: get TriggerLink running on one (or a few) fresh machines and connect your applications. Based on M0.
> Related docs: [User Guide](user-guide.md) (integration and concepts), [Protocol Specification](protocol.md), [Architecture](architecture.md).

## 1. Deployment Topology

M0 has only three kinds of deployment units (processes):

```
┌─────────────────┐        ┌──────────────────┐        ┌─────────────────┐
│  App A (Go/TS)  │◄───────│  triggerlink     │◄───────│  Producer       │
│  :8080          │ callback│ platform :8288  │ events │  (any system)   │
└─────────────────┘        │  + SQLite file    │        └─────────────────┘
┌─────────────────┐        └──────────────────┘
│  App B          │◄─────── Platform and apps only need HTTP
└─────────────────┘        connectivity; they may run on the
                           same machine or on separate machines
```

- **Platform**: a single process plus one SQLite file, with no other external dependencies. It can run on the same machine as the apps or be deployed independently.
- **Apps**: your business services (Go embedding `sdk`, or Next.js embedding `sdk-ts`), any number of them.
- Network requirements: ① producers can reach the platform on `:8288`; ② **the platform can reach each app's serve endpoint** (callbacks + introspection).

## 2. Prerequisites

| Component | Requirement |
|---|---|
| Platform | Go 1.23+ (build from source) or a prebuilt binary; Linux/macOS |
| Go apps | Go 1.23+ |
| Next.js/TS apps | Node.js ≥ 18 (22 recommended), and must use the Node.js runtime (not Edge) |
| Operating system | Accurate clock (NTP sync) — signature verification allows a ±5 minute timestamp tolerance; excessive clock drift causes callback 401s |

## 3. Get the Binary

**Option A: prebuilt binary (recommended).** Download the archive for your platform from [GitHub Releases](https://github.com/bearalise/triggerlink/releases) (linux/darwin/windows × amd64/arm64, with `checksums.txt`), or run the Docker image:

```bash
docker run -d --name triggerlink \
  -p 8288:8288 -v triggerlink-data:/data \
  ghcr.io/bearalise/triggerlink:latest \
  -event-key <EVENT_KEY> -signing-key <SIGNING_KEY> \
  -dashboard-auth admin:<password>
# A docker-compose.yml example is included in the repository root.
```

**Option B: build from source.**

```bash
git clone https://github.com/bearalise/triggerlink.git
cd triggerlink

# Platform binary (output at ./triggerlink)
go build ./cmd/triggerlink

# For TypeScript apps: build sdk-ts (examples/nextjs depends on it via file:)
cd sdk-ts && npm install && npm run build && cd ..
```

## 4. Generate Keys

Two keys with different purposes — **do not reuse them**:

```bash
openssl rand -hex 32   # use as EVENT_KEY: Bearer key for producers → platform
openssl rand -hex 32   # use as SIGNING_KEY: HMAC signing key for platform ↔ apps
```

- Distribute `EVENT_KEY` to all event producers; if it leaks, anyone can deliver events.
- Configure `SIGNING_KEY` on the platform and on **every app**; if it leaks, platform callbacks can be forged.
- In production, put them in environment variables or a secrets manager — never commit them to a repository.

## 5. Startup (Order Matters)

**Start apps before the platform** (so startup introspection succeeds in one pass). It is not fatal if an app is not ready: the platform logs `warn: introspect` and skips that app; once the app is up, register it via the admin API — `POST /api/v1/apps {"url":"..."}` (same Bearer token as the event key), no platform restart needed. Registration is persisted to the database: after a platform restart, all registered apps are automatically restored from the database (log line `restored N app registration(s) from db`), so dynamically registered apps do not need to be synced again.

```bash
# 1. Start the app (Go app shown as an example; for Next.js apps see Section 8)
TRIGGERLINK_SIGNING_KEY=<SIGNING_KEY> ./your-app &      # listens on :8080

# 2. Start the platform
./triggerlink start \
  -addr :8288 \
  -db /var/lib/triggerlink/triggerlink.db \
  -event-key <EVENT_KEY> \
  -signing-key <SIGNING_KEY> \
  -app http://<app-A-address>:8080/api/triggerlink \
  -app http://<app-B-address>:3000/api/triggerlink
```

Seeing `level=INFO msg="registered function" function_id=<id> event=<name>` in the startup logs means registration succeeded; `level=WARN msg="introspect app"` means the corresponding app is not up or its signing key does not match.

See the "Deploying the Platform" section of the [User Guide](user-guide.md) for a full description of all flags.

### Logging

- Logs are structured key/value lines (Go `log/slog` text handler) on stderr; under systemd they land in journald as-is (`journalctl -u triggerlink`).
- `-log-level debug|info|warn|error` (default `info`, or env `TRIGGERLINK_LOG_LEVEL`) controls verbosity. `debug` adds per-event routing, callback durations, and step suspend/resume lines — useful when tracing a single run, noisy under load.

### Data Retention

- `-retention-days` (default `30`, or env `TRIGGERLINK_RETENTION_DAYS`) bounds how much history the database keeps. A janitor goroutine cleans once at startup and hourly thereafter:
  - terminal runs with `ended_at` older than the cutoff, cascading to their `step_runs`, `waits`, and leftover `queue_items`;
  - events received before the cutoff, **except** those still referenced by a `Queued`/`Running` run;
  - resolved/timed-out/cancelled waits created before the cutoff.
- Nothing in flight is ever deleted, and a run keeps its own event snapshot, so run details survive the deletion of the triggering event.
- The floor is 1 day: a shorter window would delete events still inside the 24h dedup window and break duplicate detection, so the platform refuses to start. Pass a negative value to disable cleanup entirely (unbounded growth — size the disk accordingly).
- SQLite does not return freed pages to the filesystem on its own; after a large one-off cleanup, run `sqlite3 triggerlink.db 'VACUUM;'` with the platform stopped if you need the file to shrink.

### Metrics (`/metrics`)

`GET /metrics` exposes Prometheus text format. The endpoint is **public** (no basic auth), following scrape convention — on a public deployment, restrict it at the reverse proxy.

| Series | Type | Meaning |
|---|---|---|
| `triggerlink_events_received_total` | counter | Events accepted at an ingress (Event API + webhook endpoints), duplicates included |
| `triggerlink_events_routed_total{result}` | counter | Routing decisions per (event, subscribing function); `result` = `routed` / `paused` / `filtered` (trigger `match` missed) / `debounced` (merged into an open debounce window) / `batched` (added to a batch window) / `no_subscriber` (counted once per unsubscribed event) |
| `triggerlink_runs_total{status}` | counter | Runs that reached a terminal state; `status` = `completed` / `failed` / `cancelled` / `timeout` |
| `triggerlink_run_duration_seconds` | histogram | Run creation → terminal state, durable sleeps and waits included |
| `triggerlink_step_duration_seconds{op}` | histogram | `Run` / `SendEvent` = in-callback execution time; `Sleep` = requested sleep length; `WaitForEvent` = wall-clock until resolved |
| `triggerlink_callback_duration_seconds` | histogram | Platform→app callback round trip, one per run hop |
| `triggerlink_runs_throttled_total` | counter | Dispatch attempts deferred by throttle (FR-4.4), counted once per deferral |
| `triggerlink_queue_depth` | gauge | Pending queue items already due (`at <= now`) |
| `triggerlink_waits_active` | gauge | `step.waitForEvent` suspensions still waiting |

The two gauges are refreshed by a 10s sampler rather than on scrape, so scrape frequency never drives database load. Go runtime and process collectors are registered alongside (`go_*`, `process_*`).

Minimal scrape config:

```yaml
scrape_configs:
  - job_name: triggerlink
    static_configs:
      - targets: ["triggerlink-host:8288"]
```

### Dashboard Authentication

- `-dashboard-auth user:password` (or env `TRIGGERLINK_DASHBOARD_AUTH`): protects `/dashboard` and the read-only `/api/v1/{events,functions,runs}` endpoints.
- If not configured, a random password is generated at startup and printed to the logs; for public deployments you must configure it explicitly and put the service behind a TLS reverse proxy (basic auth is transmitted in plaintext).

## 6. Managing with systemd (Recommended for Production)

`/etc/triggerlink/env` (permissions 0600):

```ini
TRIGGERLINK_EVENT_KEY=replace-with-your-EVENT_KEY
TRIGGERLINK_SIGNING_KEY=replace-with-your-SIGNING_KEY
```

`/etc/systemd/system/triggerlink.service`:

```ini
[Unit]
Description=TriggerLink platform
After=network.target

[Service]
Type=simple
EnvironmentFile=/etc/triggerlink/env
WorkingDirectory=/var/lib/triggerlink
ExecStart=/usr/local/bin/triggerlink start \
  -addr :8288 \
  -db /var/lib/triggerlink/triggerlink.db \
  -event-key ${TRIGGERLINK_EVENT_KEY} \
  -signing-key ${TRIGGERLINK_SIGNING_KEY} \
  -app http://app-a.internal:8080/api/triggerlink
Restart=always
RestartSec=3

[Install]
WantedBy=multi-user.target
```

```bash
sudo systemctl daemon-reload
sudo systemctl enable --now triggerlink
journalctl -u triggerlink -f    # watch registration / reconciliation / retry logs
```

Create a unit for each app process as well, and add `Before=triggerlink.service` (or add `After=` the app in the platform unit) to guarantee startup order.

## 7. Network and Exposure

- **Platform `:8288`**: expose only to trusted producers (internal network / VPN / behind a gateway). If exposed to the public internet, always put a TLS reverse proxy in front (Caddy/Nginx) — `EVENT_KEY` is a bare Bearer token, and plaintext HTTP is equivalent to making it public.
- **App serve endpoints**: only need to be reachable by the platform. If both run on the same machine, `http://localhost` is fine; on separate machines, make sure the firewall allows traffic from the platform to the app ports.
- **Proxy pitfall**: when a machine has `http_proxy` configured, Go's HTTP client uses the proxy by default, so platform callbacks to internal apps may be intercepted by the proxy. Set `no_proxy=localhost,127.0.0.1,*.internal` in the platform and producer shells (for curl, add `--noproxy '*'`).

## 8. Deploying Next.js Apps

Self-hosted (co-located with the platform):

```bash
cd examples/nextjs        # or your own app
npm install
npm run build
TRIGGERLINK_SIGNING_KEY=<SIGNING_KEY> npm run start -- -p 3000
```

Point the platform's `-app` at `http://<host>:3000/api/triggerlink`.

Notes for serverless (e.g. Vercel):

- `export const runtime = "nodejs"` (the SDK depends on `node:crypto`);
- `export const maxDuration = 300` and **each step's duration < the function time limit** (the platform's callback timeout defaults to 5 minutes);
- The platform must be able to reach the URL over the public internet; after the app is deployed, call `POST /api/v1/apps` to register it dynamically (static introspection via `-app` at startup is still supported).

## 9. Data, Backup, and Upgrades

- **Data**: all state lives in the SQLite file specified by `-db` (plus its `-wal`/`-shm` companion files).
- **Backup**: use SQLite's online backup; do not directly `cp` a database file that is being written to:
  ```bash
  sqlite3 /var/lib/triggerlink/triggerlink.db ".backup /backup/triggerlink-$(date +%F).db"
  ```
  The restart reconciliation mechanism guarantees that unfinished runs automatically resume after restoring from a backup.
- **Upgrade**: stop the platform → back up the database → replace the binary → start (the schema migrates automatically via `CREATE TABLE IF NOT EXISTS`).
  After a rolling app upgrade that changes function definitions, call `POST /api/v1/apps/sync` to sync — no platform restart needed. However, if an upgrade renamed or deleted function IDs, in-flight runs of those functions will be marked Failed by the platform after the sync (with no retry); let in-flight runs converge first, or publish under new function IDs.

## 10. Post-Deployment Verification Checklist

1. The platform logs show `registered function ...` for every app, with no `warn: introspect`;
2. Send a test event:
   ```bash
   curl --noproxy '*' -X POST http://localhost:8288/v1/events \
     -H "Authorization: Bearer <EVENT_KEY>" \
     -d '{"id":"smoke-test-1","name":"doc/uploaded","data":{"doc_id":"smoke"}}'
   ```
   Expect `{"ids":[...],"status":200}` in return;
3. The platform logs show `executor: run ... completed`;
4. Confirm in the database: `sqlite3 triggerlink.db "SELECT status FROM runs;"` → `Completed`.

If any step fails, follow the troubleshooting order in the "Running and Troubleshooting" section of the [User Guide](user-guide.md).

## 11. M0 Deployment Limitations

- The platform is **single-replica** (SQLite single-writer), with no horizontal scaling; availability is "systemd restarts it after a crash + restart reconciliation resumes runs" (the lease defaults to 30s, i.e. a scheduling gap of at most 30s).
- No multi-tenant isolation: all apps share one platform and one set of keys.
- A Postgres backend and multi-replica platform are planned for later milestones.
