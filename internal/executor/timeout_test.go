package executor

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bearalise/triggerlink/internal/registry"
	"github.com/bearalise/triggerlink/internal/store"
)

// TestRunTimeout：run 级超时（FR-4.3）——注册了 Timeout 的函数，其在途 run 超过
// Timeout 仍在 Queued/Running → 扫描置 Failed（error = "run timeout after <d>"）、
// 合成 triggerlink/run.failed 事件、pending 队列项被清理；未配置 Timeout 的函数不受影响。
func TestRunTimeout(t *testing.T) {
	var calls atomic.Int32
	app := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		json.NewEncoder(w).Encode(map[string]any{"op": "RunComplete"})
	}))
	t.Cleanup(app.Close)

	st, err := store.Open(filepath.Join(t.TempDir(), "test.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() })
	reg := registry.New()
	reg.Register(registry.Function{ID: "fn-timed", Event: "e/x", AppURL: app.URL, Timeout: 30 * time.Millisecond})
	reg.Register(registry.Function{ID: "fn-calm", Event: "e/x", AppURL: app.URL}) // 无 Timeout

	ctx := context.Background()
	// 超时 run：Queued + 一个 pending 队列项（断言清理用）
	if _, err := st.CreateRun(ctx, store.Run{ID: "run_t", FunctionID: "fn-timed", Status: store.RunQueued,
		EventID: "evt_t", EventName: "e/x", EventData: json.RawMessage(`{"n":1}`), EventTS: time.Now()}); err != nil {
		t.Fatal(err)
	}
	if err := st.Enqueue(ctx, store.QueueItem{ID: "qi_t", FunctionID: "fn-timed", RunID: "run_t",
		Score: time.Now().UnixNano(), At: time.Now(), Status: store.QueuePending}); err != nil {
		t.Fatal(err)
	}
	// 对照 run：同事件但函数无 Timeout（不入队，避免 dispatch 干扰）
	if _, err := st.CreateRun(ctx, store.Run{ID: "run_c", FunctionID: "fn-calm", Status: store.RunQueued,
		EventID: "evt_t", EventName: "e/x", EventData: json.RawMessage(`{"n":1}`), EventTS: time.Now()}); err != nil {
		t.Fatal(err)
	}

	// PollInterval 拉大到 500ms、超时扫描 5ms：保证扫描先于租赁，应用不被回调
	exec := &Executor{
		Store: st, Reg: reg, Client: app.Client(), SigningKey: "k",
		MaxRetries: 3, Concurrency: 10, BatchSize: 4,
		PollInterval: 500 * time.Millisecond, LeaseDuration: 30 * time.Second,
		BaseBackoff: 10 * time.Millisecond, TimeoutScanInterval: 5 * time.Millisecond,
	}
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	go exec.Run(runCtx)

	// 等 run_t 超时终态化
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		r, err := st.GetRun(ctx, "run_t")
		if err == nil && r.Status == store.RunFailed {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	r, err := st.GetRun(ctx, "run_t")
	if err != nil {
		t.Fatal(err)
	}
	if r.Status != store.RunFailed || r.Error != "run timeout after 30ms" || r.EndedAt == nil {
		t.Fatalf("timed out run: %+v", r)
	}

	// pending 队列项已清理
	items, err := st.LeaseBatch(ctx, time.Now(), time.Minute, 10)
	if err != nil {
		t.Fatal(err)
	}
	for _, it := range items {
		if it.RunID == "run_t" {
			t.Fatalf("timed-out run still has pending queue item: %+v", it)
		}
	}

	// 走 failRun 出口：合成 triggerlink/run.failed 事件（确定性 id + error 字段）
	evts := runFailedEvents(t, st)
	if len(evts) != 1 || evts[0].ID != "runfailed_run_t" {
		t.Fatalf("run.failed events: %+v", evts)
	}
	var data struct {
		RunID string `json:"run_id"`
		Error string `json:"error"`
	}
	if err := json.Unmarshal(evts[0].Data, &data); err != nil {
		t.Fatal(err)
	}
	if data.RunID != "run_t" || data.Error != "run timeout after 30ms" {
		t.Fatalf("run.failed data: %s", evts[0].Data)
	}

	// 未配置 Timeout 的函数：run 保持 Queued，不被扫描动
	rc, err := st.GetRun(ctx, "run_c")
	if err != nil {
		t.Fatal(err)
	}
	if rc.Status != store.RunQueued {
		t.Fatalf("untimed function run touched: %+v", rc)
	}
	// 扫描路径不触发回调
	if n := calls.Load(); n != 0 {
		t.Fatalf("app called %d times, want 0", n)
	}
}
