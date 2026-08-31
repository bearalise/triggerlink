package executor

import (
	"context"
	"encoding/json"
	"net/http"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bearalise/triggerlink/internal/registry"
	"github.com/bearalise/triggerlink/internal/store"
)

// seedRunN 造第 n 个 Queued run（同函数、不同 key），并入队。
func (e *env) seedRunN(t *testing.T, n int, tenant string) string {
	t.Helper()
	ctx := context.Background()
	runID := "run_" + tenant + "_" + string(rune('a'+n))
	data, _ := json.Marshal(map[string]string{"tenant": tenant})
	if _, err := e.store.CreateRun(ctx, store.Run{ID: runID, FunctionID: "fn", Status: store.RunQueued,
		EventID: "evt_" + runID, EventName: "e/x", EventData: data, EventTS: time.Now()}); err != nil {
		t.Fatal(err)
	}
	if err := e.store.Enqueue(ctx, store.QueueItem{ID: "qi_" + runID, FunctionID: "fn", RunID: runID,
		Score: time.Now().UnixNano(), At: time.Now(), Status: store.QueuePending}); err != nil {
		t.Fatal(err)
	}
	return runID
}

func countStatus(t *testing.T, e *env, status string) int {
	t.Helper()
	n, err := e.store.CountRunsWithStatus(context.Background(), status)
	if err != nil {
		t.Fatal(err)
	}
	return n
}

// 窗口内只放行 limit 个 run 的首次执行；超限的 run 不丢弃，留在队列里等下一窗口。
func TestThrottleDefersOverLimitRuns(t *testing.T) {
	var calls atomic.Int32
	env := newEnv(t, func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		w.Write([]byte(`{"op":"RunComplete","output":"ok"}`))
	})
	env.reg.Register(registry.Function{ID: "fn", Event: "e/x", AppURL: env.reg.All()[0].AppURL,
		Throttle: &registry.Throttle{Limit: 2, Period: time.Hour}})
	for i := 0; i < 4; i++ {
		env.seedRunN(t, i, "t1")
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go env.exec.Run(ctx)

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) && countStatus(t, env, store.RunCompleted) < 2 {
		time.Sleep(10 * time.Millisecond)
	}
	// 稳定一小会儿，确认第 3、4 个确实没被放行
	time.Sleep(300 * time.Millisecond)

	if got := countStatus(t, env, store.RunCompleted); got != 2 {
		t.Fatalf("completed=%d, want exactly 2 (limit=2 per window)", got)
	}
	if got := countStatus(t, env, store.RunQueued); got != 2 {
		t.Fatalf("queued=%d, want 2 (over-limit runs are deferred, not dropped)", got)
	}
	if calls.Load() != 2 {
		t.Fatalf("app callbacks=%d, want 2", calls.Load())
	}
}

// key 分组：不同 key 各有各的配额。
func TestThrottleKeyIsolation(t *testing.T) {
	env := newEnv(t, func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(`{"op":"RunComplete","output":"ok"}`))
	})
	env.reg.Register(registry.Function{ID: "fn", Event: "e/x", AppURL: env.reg.All()[0].AppURL,
		Throttle: &registry.Throttle{Limit: 1, Period: time.Hour, Key: "data.tenant"}})
	env.seedRunN(t, 0, "t1")
	env.seedRunN(t, 1, "t1")
	env.seedRunN(t, 0, "t2")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go env.exec.Run(ctx)

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) && countStatus(t, env, store.RunCompleted) < 2 {
		time.Sleep(10 * time.Millisecond)
	}
	time.Sleep(200 * time.Millisecond)

	if got := countStatus(t, env, store.RunCompleted); got != 2 {
		t.Fatalf("completed=%d, want 2 (one per tenant)", got)
	}
	if got := countStatus(t, env, store.RunQueued); got != 1 {
		t.Fatalf("queued=%d, want 1 (second t1 run deferred)", got)
	}
}

// 重试与挂起恢复跳不再占额：否则一个多跳流程会被自己的每一跳反复限流。
func TestThrottleOnlyCountsFirstExecution(t *testing.T) {
	var calls atomic.Int32
	env := newEnv(t, func(w http.ResponseWriter, r *http.Request) {
		if calls.Add(1) == 1 {
			w.Write([]byte(`{"op":"StepComplete","id":"h1","step_id":"s1","output":"a"}`))
			return
		}
		w.Write([]byte(`{"op":"RunComplete","output":"done"}`))
	})
	env.reg.Register(registry.Function{ID: "fn", Event: "e/x", AppURL: env.reg.All()[0].AppURL,
		// limit=1：第二跳若也占额就会被永远推迟，run 完不成
		Throttle: &registry.Throttle{Limit: 1, Period: time.Hour}})
	env.seedRun(t)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go env.exec.Run(ctx)

	env.waitStatus(t, store.RunCompleted)
	if calls.Load() < 2 {
		t.Fatalf("callbacks=%d, want >= 2 (the second hop must not be throttled)", calls.Load())
	}
}
