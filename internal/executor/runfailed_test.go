package executor

import (
	"context"
	"encoding/json"
	"net/http"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bearalise/triggerlink/internal/store"
)

// runFailedEvents 返回已落库的 triggerlink/run.failed 事件（未路由扫描即可，事件合成不经 runner 也会落库）。
func runFailedEvents(t *testing.T, st store.Store) []store.Event {
	t.Helper()
	evts, err := st.UnroutedEvents(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	var out []store.Event
	for _, e := range evts {
		if e.Name == "triggerlink/run.failed" {
			out = append(out, e)
		}
	}
	return out
}

// TestRunFailedEmitsEvent：run 进入 Failed 终态（重试耗尽）→ 平台合成 triggerlink/run.failed
// 事件（FR-2.11）：落库 + 入 stream，id 确定性（runfailed_<run_id>），data 字段齐全。
func TestRunFailedEmitsEvent(t *testing.T) {
	app := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(map[string]any{
			"op": "StepError", "id": "h1", "step_id": "embed",
			"error": map[string]any{"message": "boom", "retryable": true}})
	})
	e := newEnv(t, app)
	e.exec.MaxRetries = 1 // 1 次尝试即终态，缩短测试
	stream := make(chan store.Event, 4)
	e.exec.Stream = stream
	e.seedRun(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go e.exec.Run(ctx)

	e.waitStatus(t, store.RunFailed)

	// FailRun 落终态与 emit 事件合成是两次独立存储调用（见 failRun 注释），
	// waitStatus 观察到 Failed 时 emit 可能尚未完成——轮询等待而非立即断言。
	var evts []store.Event
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if evts = runFailedEvents(t, e.store); len(evts) == 1 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if len(evts) != 1 {
		t.Fatalf("run.failed events=%d, want 1", len(evts))
	}
	evt := evts[0]
	if evt.ID != "runfailed_run_1" {
		t.Fatalf("event id=%s, want runfailed_run_1", evt.ID)
	}
	var data struct {
		RunID      string `json:"run_id"`
		FunctionID string `json:"function_id"`
		Error      string `json:"error"`
		Event      struct {
			ID   string          `json:"id"`
			Name string          `json:"name"`
			Data json.RawMessage `json:"data"`
			TS   time.Time       `json:"ts"`
		} `json:"event"`
	}
	if err := json.Unmarshal(evt.Data, &data); err != nil {
		t.Fatalf("data=%s: %v", evt.Data, err)
	}
	if data.RunID != "run_1" || data.FunctionID != "fn" || data.Error != "boom" {
		t.Fatalf("data fields: %s", evt.Data)
	}
	// 触发事件快照冗余自 run（seedRun 的 evt_1 / e/x / {"n":1}）
	if data.Event.ID != "evt_1" || data.Event.Name != "e/x" ||
		string(data.Event.Data) != `{"n":1}` || data.Event.TS.IsZero() {
		t.Fatalf("event snapshot: %s", evt.Data)
	}

	// 入 stream 断言（驱动订阅 triggerlink/run.failed 的函数路由）
	select {
	case got := <-stream:
		if got.ID != evt.ID {
			t.Fatalf("streamed event id=%s, want %s", got.ID, evt.ID)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("run.failed event not streamed")
	}

	// 幂等：同 ID 再插不重复（崩溃重发去重）
	inserted, err := e.store.InsertEvent(ctx, evt)
	if err != nil || inserted {
		t.Fatalf("dedup: inserted=%v err=%v", inserted, err)
	}
}

// TestCancelledRunNoRunFailedEvent：Cancelled 终态不触发 triggerlink/run.failed（FR-2.11）。
func TestCancelledRunNoRunFailedEvent(t *testing.T) {
	var calls atomic.Int32
	app := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		json.NewEncoder(w).Encode(map[string]any{"op": "RunComplete", "output": map[string]any{}})
	})
	e := newEnv(t, app)
	stream := make(chan store.Event, 4)
	e.exec.Stream = stream
	e.seedRun(t)
	if ok, err := e.store.CancelRun(context.Background(), "run_1"); err != nil || !ok {
		t.Fatalf("cancel: ok=%v err=%v", ok, err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go e.exec.Run(ctx)

	time.Sleep(300 * time.Millisecond)
	if n := len(runFailedEvents(t, e.store)); n != 0 {
		t.Fatalf("cancelled run emitted %d run.failed events", n)
	}
	select {
	case evt := <-stream:
		t.Fatalf("unexpected streamed event: %+v", evt)
	default:
	}
}
