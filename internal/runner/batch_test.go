package runner

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/bearalise/triggerlink/internal/registry"
	"github.com/bearalise/triggerlink/internal/store"
)

// 攒够 max_size 立即 flush 成一个批 run，事件按到达顺序进快照。
func TestBatchFlushesOnSize(t *testing.T) {
	r, st := setupDebounce(t, registry.Function{ID: "bulk-index", Event: "doc/changed",
		AppURL: "http://app/serve",
		Batch:  &registry.Batch{MaxSize: 3, Timeout: time.Hour}})
	ctx := context.Background()

	routeAll(t, r, st, event("evt_1", "d1"), event("evt_2", "d2"))
	if n, _ := st.CountRuns(ctx); n != 0 {
		t.Fatalf("runs=%d, want 0 before the batch is full", n)
	}

	routeAll(t, r, st, event("evt_3", "d3"))

	runs, err := st.ListRuns(ctx, store.ListRunsOptions{Limit: 10})
	if err != nil || len(runs) != 1 {
		t.Fatalf("runs=%d err=%v, want 1", len(runs), err)
	}
	run, err := st.GetRun(ctx, runs[0].ID)
	if err != nil {
		t.Fatal(err)
	}
	if !run.Batch {
		t.Fatal("flushed run must be marked as batch")
	}
	var events []struct {
		ID string `json:"id"`
	}
	if err := json.Unmarshal(run.EventData, &events); err != nil {
		t.Fatalf("event_data must be an event array: %v (%s)", err, run.EventData)
	}
	if len(events) != 3 || events[0].ID != "evt_1" || events[2].ID != "evt_3" {
		t.Fatalf("events=%+v, want evt_1..evt_3 in arrival order", events)
	}
	if run.EventID != "evt_1" {
		t.Fatalf("run.EventID=%s, want the first event of the batch", run.EventID)
	}
	// 窗口已清空
	windows, _ := st.ListBatchWindows(ctx)
	if len(windows) != 0 {
		t.Fatalf("windows=%+v, want none after flush", windows)
	}
}

// 攒不满时由 timeout 兜底 flush（scanBatchTimeouts）。
func TestBatchFlushesOnTimeout(t *testing.T) {
	r, st := setupDebounce(t, registry.Function{ID: "bulk-index", Event: "doc/changed",
		AppURL: "http://app/serve",
		Batch:  &registry.Batch{MaxSize: 100, Timeout: 20 * time.Millisecond}})
	ctx := context.Background()

	routeAll(t, r, st, event("evt_1", "d1"), event("evt_2", "d2"))
	r.scanBatchTimeouts(ctx) // 还没到点
	if n, _ := st.CountRuns(ctx); n != 0 {
		t.Fatalf("runs=%d, want 0 before the timeout elapses", n)
	}

	time.Sleep(30 * time.Millisecond)
	r.scanBatchTimeouts(ctx)

	if n, _ := st.CountRuns(ctx); n != 1 {
		t.Fatalf("runs=%d, want 1 after the timeout flush", n)
	}
	windows, _ := st.ListBatchWindows(ctx)
	if len(windows) != 0 {
		t.Fatalf("windows=%+v, want none after flush", windows)
	}
}

// key 分组：不同 key 各攒各的，各自独立成批。
func TestBatchKeyIsolation(t *testing.T) {
	r, st := setupDebounce(t, registry.Function{ID: "bulk-index", Event: "doc/changed",
		AppURL: "http://app/serve",
		Batch:  &registry.Batch{MaxSize: 2, Timeout: time.Hour, Key: "data.doc_id"}})
	ctx := context.Background()

	routeAll(t, r, st, event("evt_1", "d1"), event("evt_2", "d2"))
	if n, _ := st.CountRuns(ctx); n != 0 {
		t.Fatalf("runs=%d, want 0 (each key has one event)", n)
	}
	routeAll(t, r, st, event("evt_3", "d1"))

	if n, _ := st.CountRuns(ctx); n != 1 {
		t.Fatalf("runs=%d, want 1 (only key d1 is full)", n)
	}
	left, _ := st.BatchEntries(ctx, "bulk-index", "d2")
	if len(left) != 1 {
		t.Fatalf("key d2 entries=%d, want 1 still waiting", len(left))
	}
}

// 同时配了 debounce 与 batch 时以 debounce 为准（清单契约）。
func TestDebounceWinsOverBatch(t *testing.T) {
	r, st := setupDebounce(t, registry.Function{ID: "both", Event: "doc/changed",
		AppURL:   "http://app/serve",
		Debounce: &registry.Debounce{Period: time.Hour},
		Batch:    &registry.Batch{MaxSize: 1, Timeout: time.Hour}})
	ctx := context.Background()

	routeAll(t, r, st, event("evt_1", "d1"))

	if _, ok, _ := st.GetDebouncePending(ctx, "both", ""); !ok {
		t.Fatal("debounce window must be opened when both are configured")
	}
	windows, _ := st.ListBatchWindows(ctx)
	if len(windows) != 0 {
		t.Fatalf("batch windows=%+v, want none", windows)
	}
}

// 函数去掉了 batch 配置（或已下线）时，超时扫描不擅自 flush 遗留窗口。
func TestBatchTimeoutSkipsUnconfiguredFunction(t *testing.T) {
	r, st := setupDebounce(t, registry.Function{ID: "bulk-index", Event: "doc/changed",
		AppURL: "http://app/serve",
		Batch:  &registry.Batch{MaxSize: 100, Timeout: time.Millisecond}})
	ctx := context.Background()
	routeAll(t, r, st, event("evt_1", "d1"))
	time.Sleep(5 * time.Millisecond)

	// 应用重新注册，去掉了 batch 配置
	r.Reg.Sync("http://app/serve", []registry.Function{{ID: "bulk-index", Event: "doc/changed"}})
	r.scanBatchTimeouts(ctx)

	if n, _ := st.CountRuns(ctx); n != 0 {
		t.Fatalf("runs=%d, want 0 (no batch config, no flush)", n)
	}
	if windows, _ := st.ListBatchWindows(ctx); len(windows) != 1 {
		t.Fatalf("windows=%+v, want the orphan window kept", windows)
	}
}
