package executor

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bearalise/triggerlink/internal/registry"
	"github.com/bearalise/triggerlink/internal/store"
)

type env struct {
	store *SQLiteStoreAlias
	reg   *registry.Registry
	exec  *Executor
}

// SQLiteStoreAlias 避免测试文件重复写 store.SQLiteStore 长名。
type SQLiteStoreAlias = store.SQLiteStore

func newEnv(t *testing.T, appHandler http.HandlerFunc) *env {
	t.Helper()
	st, err := store.Open(filepath.Join(t.TempDir(), "test.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() })
	app := httptest.NewServer(appHandler)
	t.Cleanup(app.Close)
	reg := registry.New()
	reg.Register(registry.Function{ID: "fn", Event: "e/x", AppURL: app.URL})
	exec := &Executor{
		Store: st, Reg: reg, Client: app.Client(), SigningKey: "k",
		MaxRetries: 3, Concurrency: 10, BatchSize: 4,
		PollInterval: 10 * time.Millisecond, LeaseDuration: 30 * time.Second,
		BaseBackoff: 10 * time.Millisecond,
	}
	return &env{store: st, reg: reg, exec: exec}
}

func (e *env) seedRun(t *testing.T) {
	t.Helper()
	ctx := context.Background()
	if _, err := e.store.CreateRun(ctx, store.Run{ID: "run_1", FunctionID: "fn", Status: store.RunQueued,
		EventID: "evt_1", EventName: "e/x", EventData: json.RawMessage(`{"n":1}`), EventTS: time.Now()}); err != nil {
		t.Fatal(err)
	}
	if err := e.store.Enqueue(ctx, store.QueueItem{ID: "qi_1", FunctionID: "fn", RunID: "run_1",
		Score: time.Now().UnixNano(), At: time.Now(), Status: store.QueuePending}); err != nil {
		t.Fatal(err)
	}
}

func (e *env) waitStatus(t *testing.T, want string) store.Run {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		r, err := e.store.GetRun(context.Background(), "run_1")
		if err == nil && r.Status == want {
			return r
		}
		time.Sleep(10 * time.Millisecond)
	}
	r, _ := e.store.GetRun(context.Background(), "run_1")
	t.Fatalf("status=%s, want %s", r.Status, want)
	return r
}

func TestStepMemoThenRunComplete(t *testing.T) {
	var calls atomic.Int32
	var sawMemo atomic.Bool
	app := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		var req callbackRequest
		json.Unmarshal(body, &req)
		n := calls.Add(1)
		if len(req.Ctx.Steps) > 0 {
			sawMemo.Store(true) // 第二次回调必须带上第一次的 step memo
		}
		if n == 1 {
			json.NewEncoder(w).Encode(map[string]any{
				"op": "StepComplete", "id": "h1", "step_id": "parse", "output": "done"})
			return
		}
		json.NewEncoder(w).Encode(map[string]any{"op": "RunComplete", "output": map[string]any{"ok": true}})
	})
	e := newEnv(t, app)
	e.seedRun(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go e.exec.Run(ctx)

	r := e.waitStatus(t, store.RunCompleted)
	if string(r.Output) != `{"ok":true}` {
		t.Fatalf("output=%s", r.Output)
	}
	if !sawMemo.Load() {
		t.Fatal("second callback missing steps memo")
	}
	steps, _ := e.store.StepResults(context.Background(), "run_1")
	if steps["h1"].Status != store.StepCompleted || string(steps["h1"].Output) != `"done"` {
		t.Fatalf("step memo: %+v", steps)
	}
}

func TestStepErrorRetriesThenFails(t *testing.T) {
	var calls atomic.Int32
	app := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		json.NewEncoder(w).Encode(map[string]any{
			"op": "StepError", "id": "h1", "step_id": "embed",
			"error": map[string]any{"message": "boom", "retryable": true}})
	})
	e := newEnv(t, app)
	e.seedRun(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go e.exec.Run(ctx)

	r := e.waitStatus(t, store.RunFailed)
	if r.Error != "boom" {
		t.Fatalf("error=%q", r.Error)
	}
	if int(calls.Load()) != 3 { // MaxRetries=3：恰好 3 次回调
		t.Fatalf("calls=%d", calls.Load())
	}
}

func TestHTTPErrorRetriesThenFails(t *testing.T) {
	var calls atomic.Int32
	app := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		w.WriteHeader(http.StatusInternalServerError)
	})
	e := newEnv(t, app)
	e.seedRun(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go e.exec.Run(ctx)

	e.waitStatus(t, store.RunFailed)
	if int(calls.Load()) != 3 {
		t.Fatalf("calls=%d", calls.Load())
	}
}

func TestSleepSuspendsAndResumes(t *testing.T) {
	var calls atomic.Int32
	var sleepUntil atomic.Value // time.Time，记录 app 请求的唤醒时刻
	app := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		var req callbackRequest
		json.Unmarshal(body, &req)
		n := calls.Add(1)
		if n == 1 {
			until := time.Now().Add(300 * time.Millisecond)
			sleepUntil.Store(until)
			json.NewEncoder(w).Encode(map[string]any{
				"op": "Sleep", "id": "h_sleep", "step_id": "wait",
				"until": until.UTC().Format(time.RFC3339Nano)})
			return
		}
		// 第二次回调（唤醒跳）：sleep memo 必须已按 completed 注入
		if st, ok := req.Ctx.Steps["h_sleep"]; !ok || st.Status != "completed" {
			t.Errorf("wake callback steps=%v, want h_sleep completed", req.Ctx.Steps)
		}
		json.NewEncoder(w).Encode(map[string]any{"op": "RunComplete", "output": "ok"})
	})
	e := newEnv(t, app)
	e.seedRun(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	start := time.Now()
	go e.exec.Run(ctx)

	r := e.waitStatus(t, store.RunCompleted)
	_ = r
	if elapsed := time.Since(start); elapsed < 250*time.Millisecond {
		t.Fatalf("resumed too early: %v", elapsed) // 挂起语义：必须睡到 until 才唤醒
	}
	if int(calls.Load()) != 2 {
		t.Fatalf("calls=%d, want 2", calls.Load())
	}
	steps, _ := e.store.StepResults(context.Background(), "run_1")
	if steps["h_sleep"].Status != store.StepCompleted || steps["h_sleep"].Op != "Sleep" {
		t.Fatalf("sleep memo: %+v", steps["h_sleep"])
	}
}

// TestSleepMissingUntilRetried：until 缺失按失败尝试处理（退避重试直至终结）。
func TestSleepMissingUntilRetried(t *testing.T) {
	var calls atomic.Int32
	app := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		json.NewEncoder(w).Encode(map[string]any{"op": "Sleep", "id": "h1", "step_id": "wait"})
	})
	e := newEnv(t, app)
	e.seedRun(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go e.exec.Run(ctx)

	e.waitStatus(t, store.RunFailed)
	if int(calls.Load()) != 3 { // MaxRetries=3
		t.Fatalf("calls=%d", calls.Load())
	}
}

func TestSendEventFansOut(t *testing.T) {
	var calls atomic.Int32
	app := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if calls.Add(1) == 1 {
			json.NewEncoder(w).Encode(map[string]any{
				"op": "SendEvent", "id": "h_send", "step_id": "notify",
				"events": []map[string]any{
					{"name": "notify/email", "data": map[string]any{"to": "a@b.c"}},
					{"id": "custom-1", "name": "notify/sms"},
				}})
			return
		}
		json.NewEncoder(w).Encode(map[string]any{"op": "RunComplete", "output": "ok"})
	})
	e := newEnv(t, app)
	stream := make(chan store.Event, 8)
	e.exec.Stream = stream
	e.seedRun(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go e.exec.Run(ctx)

	e.waitStatus(t, store.RunCompleted)

	// 两个事件落库（缺省 ID 为确定性派生 evt_*），且推入 stream 触发路由
	var streamed []store.Event
	for i := 0; i < 2; i++ {
		select {
		case evt := <-stream:
			streamed = append(streamed, evt)
		case <-time.After(2 * time.Second):
			t.Fatalf("streamed %d events, want 2", len(streamed))
		}
	}
	if streamed[0].Name != "notify/email" || streamed[1].Name != "notify/sms" {
		t.Fatalf("streamed: %+v", streamed)
	}

	// memo：SendEvent 完成，output 为事件 ID 列表
	steps, _ := e.store.StepResults(context.Background(), "run_1")
	memo := steps["h_send"]
	if memo.Status != store.StepCompleted || memo.Op != "SendEvent" {
		t.Fatalf("memo: %+v", memo)
	}
	var ids []string
	json.Unmarshal(memo.Output, &ids)
	if len(ids) != 2 || ids[1] != "custom-1" || !strings.HasPrefix(ids[0], "evt_") {
		t.Fatalf("ids: %v", ids)
	}

	// 崩溃窗口重试幂等：同事件再 InsertEvent 不重复（derived ID 去重）
	inserted, _ := e.store.InsertEvent(context.Background(), store.Event{ID: ids[0], Name: "notify/email", Data: json.RawMessage(`{}`), TS: time.Now()})
	if inserted {
		t.Fatal("derived event id must dedup on retry")
	}
}

// TestFunctionRetriesOverride：函数注册的 Retries 覆盖全局 MaxRetries（FR-4.2）。
func TestFunctionRetriesOverride(t *testing.T) {
	var calls atomic.Int32
	app := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		json.NewEncoder(w).Encode(map[string]any{
			"op": "StepError", "id": "h1", "step_id": "embed",
			"error": map[string]any{"message": "boom", "retryable": true}})
	})
	e := newEnv(t, app)
	f, _ := e.reg.Lookup("fn")
	f.Retries = 1
	e.reg.Register(f)
	e.seedRun(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go e.exec.Run(ctx)

	e.waitStatus(t, store.RunFailed)
	if int(calls.Load()) != 1 { // fn.Retries=1：1 次尝试即终结，不走全局 MaxRetries=3
		t.Fatalf("calls=%d, want 1", calls.Load())
	}
}

func TestStepErrorThenRecover(t *testing.T) {
	var calls atomic.Int32
	app := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if calls.Add(1) == 1 {
			json.NewEncoder(w).Encode(map[string]any{
				"op": "StepError", "id": "h1", "step_id": "embed",
				"error": map[string]any{"message": "flaky", "retryable": true}})
			return
		}
		json.NewEncoder(w).Encode(map[string]any{"op": "RunComplete", "output": "ok"})
	})
	e := newEnv(t, app)
	e.seedRun(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go e.exec.Run(ctx)

	e.waitStatus(t, store.RunCompleted)
	steps, _ := e.store.StepResults(context.Background(), "run_1")
	if steps["h1"].Status != store.StepFailed { // 失败记录留痕（Dashboard 时间线的基础）
		t.Fatalf("step: %+v", steps["h1"])
	}
}

// 取消语义：被取消 run 的队列项(含取消前已 leased、被对账重置回 pending 的)不得再触发回调。
func TestCancelledRunNotDispatched(t *testing.T) {
	var calls atomic.Int32
	app := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		json.NewEncoder(w).Encode(map[string]any{"op": "RunComplete", "output": map[string]any{}})
	})
	e := newEnv(t, app)
	e.seedRun(t)
	if ok, err := e.store.CancelRun(context.Background(), "run_1"); err != nil || !ok {
		t.Fatalf("cancel: ok=%v err=%v", ok, err)
	}
	// 模拟竞态：取消前已 leased 的项被重启对账重置回 pending(等价于再入队一个)
	if err := e.store.Enqueue(context.Background(), store.QueueItem{ID: "qi_2", FunctionID: "fn",
		RunID: "run_1", Score: time.Now().UnixNano(), At: time.Now(), Status: store.QueuePending}); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go e.exec.Run(ctx)

	time.Sleep(300 * time.Millisecond)
	if n := calls.Load(); n != 0 {
		t.Fatalf("cancelled run dispatched %d times", n)
	}
	r, _ := e.store.GetRun(context.Background(), "run_1")
	if r.Status != store.RunCancelled {
		t.Fatalf("status=%s, want Cancelled", r.Status)
	}
}

// TestWaitForEventSuspends：WaitForEvent opcode → memo waiting + wait 落库 + 不入队（挂起零占用）。
func TestWaitForEventSuspends(t *testing.T) {
	var calls atomic.Int32
	app := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		json.NewEncoder(w).Encode(map[string]any{
			"op": "WaitForEvent", "id": "h_wait", "step_id": "wait-payed",
			"event": "order/payed", "match": `data.order_id == event.data.order_id`, "timeout": "1h"})
	})
	e := newEnv(t, app)
	e.seedRun(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go e.exec.Run(ctx)

	// run 保持 Running（Waiting 是 Running 子状态），且不会产生后续回调
	time.Sleep(300 * time.Millisecond)
	r, _ := e.store.GetRun(context.Background(), "run_1")
	if r.Status != store.RunRunning {
		t.Fatalf("status=%s, want Running", r.Status)
	}
	if int(calls.Load()) != 1 {
		t.Fatalf("calls=%d, want 1（挂起期间不得再回调）", calls.Load())
	}
	steps, _ := e.store.StepResults(context.Background(), "run_1")
	memo := steps["h_wait"]
	if memo.Status != store.StepWaiting || memo.Op != "WaitForEvent" {
		t.Fatalf("memo: %+v", memo)
	}
	waits, err := e.store.WaitingWaitsForEvent(context.Background(), "order/payed")
	if err != nil || len(waits) != 1 {
		t.Fatalf("waits=%+v err=%v", waits, err)
	}
	w0 := waits[0]
	if w0.RunID != "run_1" || w0.StepHash != "h_wait" || w0.MatchExpr == "" || w0.ExpiresAt == nil {
		t.Fatalf("wait: %+v", w0)
	}
	// 挂起后队列里不得有该 run 的残留项
	items, _ := e.store.LeaseBatch(context.Background(), time.Now().Add(2*time.Hour), time.Minute, 8)
	for _, it := range items {
		if it.RunID == "run_1" {
			t.Fatalf("suspended run still enqueued: %+v", it)
		}
	}
}

// TestWaitForEventIdempotent：同一 opcode 重放（崩溃重试）不产生第二条 wait。
func TestWaitForEventIdempotent(t *testing.T) {
	var calls atomic.Int32
	app := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		json.NewEncoder(w).Encode(map[string]any{
			"op": "WaitForEvent", "id": "h_wait", "step_id": "wait-payed", "event": "order/payed"})
	})
	e := newEnv(t, app)
	e.seedRun(t)
	// 模拟崩溃窗口：第一次 dispatch 后队列项意外重置回 pending（再造一个等价项）
	if err := e.store.Enqueue(context.Background(), store.QueueItem{ID: "qi_2", FunctionID: "fn",
		RunID: "run_1", Score: time.Now().UnixNano(), At: time.Now(), Status: store.QueuePending}); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go e.exec.Run(ctx)

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		waits, _ := e.store.WaitingWaitsForEvent(context.Background(), "order/payed")
		if calls.Load() >= 2 && len(waits) >= 1 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if int(calls.Load()) != 2 {
		t.Fatalf("calls=%d, want 2", calls.Load())
	}
	waits, err := e.store.WaitingWaitsForEvent(context.Background(), "order/payed")
	if err != nil || len(waits) != 1 {
		t.Fatalf("waits=%+v err=%v, want exactly 1", waits, err)
	}
	if waits[0].ExpiresAt != nil { // 无 timeout：expires_at 为 NULL
		t.Fatalf("expires_at=%v, want nil", waits[0].ExpiresAt)
	}
}

// TestWaitForEventMissingEventRetried：event 缺失按失败尝试处理（退避重试直至终结）。
func TestWaitForEventMissingEventRetried(t *testing.T) {
	var calls atomic.Int32
	app := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		json.NewEncoder(w).Encode(map[string]any{"op": "WaitForEvent", "id": "h1", "step_id": "wait"})
	})
	e := newEnv(t, app)
	e.seedRun(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go e.exec.Run(ctx)

	e.waitStatus(t, store.RunFailed)
	if int(calls.Load()) != 3 { // MaxRetries=3
		t.Fatalf("calls=%d", calls.Load())
	}
}
