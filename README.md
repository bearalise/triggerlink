# TriggerLink

[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

An open-source, event-driven durable execution platform (M0 prototype). Write crash-recoverable, step-level-retryable async workflows as plain Go functions.

## Install

Prebuilt binaries (linux/darwin/windows) are on [GitHub Releases](https://github.com/bearalise/triggerlink/releases); a multi-arch image is at `ghcr.io/bearalise/triggerlink` (see `docker-compose.yml` for a minimal setup). Or build from source: `go build ./cmd/triggerlink`.

## Quickstart

```bash
# 1. Start the example app (a 4-step document processing pipeline). The platform introspects once at startup; the app can also be started later and registered dynamically via POST /api/v1/apps
go run ./examples/pipeline &

# 2. Start the platform (single-file SQLite)
go run ./cmd/triggerlink -event-key dev -signing-key dev \
  -app http://localhost:8080/api/triggerlink &

# 3. Send an event
# If your shell has http_proxy configured, curl needs --noproxy '*' for a direct connection
curl --noproxy '*' -X POST localhost:8288/v1/events \
  -H "Authorization: Bearer dev" \
  -d '{"name":"doc/uploaded","data":{"doc_id":"d1"}}'
```

## Crash recovery demo

```bash
pkill -f 'examples/pipeline' 2>/dev/null   # stop the example started in the quickstart (frees :8080)
TRIGGERLINK_CRASH_AT=embed go run ./examples/pipeline &
curl --noproxy '*' -X POST localhost:8288/v1/events -H "Authorization: Bearer dev" \
  -d '{"name":"doc/uploaded","data":{"doc_id":"crash-1"}}'
# The app crashes inside the embed step → restart the app (without the env var):
go run ./examples/pipeline &
# Observe: parse/chunk are not re-run (memoized results injected); the run resumes from the embed breakpoint until completion
```

You can also demo a server-side crash: `kill` the `cmd/triggerlink` process and restart it — the reconciliation sweep at startup will find runs with expired leases and re-enqueue them. Note that the default lease is 30s, so after killing the server you must wait a full 30s for the lease to expire before the restart's reconciliation picks up the run.

## Dashboard

Start with `-dashboard-auth admin:<password>` and visit `http://localhost:8288/dashboard` (run list/details, event stream, function stats). See `docs/user-guide.md` for details.

## Use in your app

```go
client := triggerlink.NewClient(triggerlink.Options{AppID: "myapp", SigningKey: "..."})
fn := triggerlink.CreateFunction(
    triggerlink.FunctionOpts{ID: "handle-signup", Event: "user/signup"},
    func(ctx context.Context, in triggerlink.Input) (any, error) {
        _, err := step.Run(ctx, "send-email", func(ctx context.Context) (any, error) {
            return sendEmail(in.Event.Data)
        })
        return map[string]any{"ok": true}, err
    })
http.Handle("/api/triggerlink", triggerlink.Serve(client, fn))
```

Constraints: put side effects inside `step.Run`; the step call sequence must be deterministic (branches/loops may only depend on event data and the outputs of completed steps).

## Out of scope for M0

Cron triggers and a Postgres backend are planned for later milestones.

## Development

```bash
go test ./...
```

## License

MIT — see [LICENSE](LICENSE).
