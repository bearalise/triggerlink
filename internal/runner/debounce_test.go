package runner

import (
	"context"
	"encoding/json"
	"path/filepath"
	"testing"
	"time"

	"github.com/bearalise/triggerlink/internal/registry"
	"github.com/bearalise/triggerlink/internal/store"
)

// setupDebounce 起一个只注册了 fn 的 runner（直接驱动 route，不跑 goroutine：
// 防抖断言关心的是每个事件处理完后的库内状态，串行驱动最直观）。
func setupDebounce(t *testing.T, fn registry.Function) (*Runner, store.Store) {
	t.Helper()
	st, err := store.Open(filepath.Join(t.TempDir(), "test.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() })
	reg := registry.New()
	reg.Register(fn)
	return &Runner{Store: st, Reg: reg}, st
}

func event(id, docID string) store.Event {
	return store.Event{ID: id, Name: "doc/changed",
		Data: json.RawMessage(`{"doc_id":"` + docID + `"}`), TS: time.Now()}
}

func routeAll(t *testing.T, r *Runner, st store.Store, events ...store.Event) {
	t.Helper()
	ctx := context.Background()
	for _, e := range events {
		if _, err := st.InsertEvent(ctx, e); err != nil {
			t.Fatal(err)
		}
		if err := r.route(ctx, e); err != nil {
			t.Fatal(err)
		}
	}
}

// 窗口内的多个事件合并为一个 run，快照取尾事件；触发时间随每个事件推后。
func TestDebounceMergesWindow(t *testing.T) {
	r, st := setupDebounce(t, registry.Function{ID: "index-doc", Event: "doc/changed",
		AppURL: "http://app/serve", Debounce: &registry.Debounce{Period: time.Hour}})
	ctx := context.Background()

	routeAll(t, r, st, event("evt_1", "d1"), event("evt_2", "d1"), event("evt_3", "d1"))

	runs, err := st.ListRuns(ctx, store.ListRunsOptions{Limit: 10})
	if err != nil {
		t.Fatal(err)
	}
	if len(runs) != 1 {
		t.Fatalf("runs=%d, want 1 (three events merged)", len(runs))
	}
	if runs[0].EventID != "evt_3" {
		t.Fatalf("snapshot event=%s, want evt_3 (latest wins)", runs[0].EventID)
	}
	// 队列里仍只有一项，且被推到了未来（period=1h）
	if n, err := st.QueueDepth(ctx); err != nil || n != 0 {
		t.Fatalf("queue depth due now=%d err=%v, want 0 (trigger is in the future)", n, err)
	}
	p, ok, err := st.GetDebouncePending(ctx, "index-doc", "")
	if err != nil || !ok {
		t.Fatalf("pending window missing: ok=%v err=%v", ok, err)
	}
	if p.RunID != runs[0].ID {
		t.Fatalf("pending run_id=%s, want %s", p.RunID, runs[0].ID)
	}
	if !p.TriggerAt.After(p.FirstAt) {
		t.Fatalf("trigger_at=%v must be pushed past first_at=%v", p.TriggerAt, p.FirstAt)
	}
}

// key 表达式分组：不同 key 各开各的窗口，互不合并。
func TestDebounceKeyIsolatesWindows(t *testing.T) {
	r, st := setupDebounce(t, registry.Function{ID: "index-doc", Event: "doc/changed",
		AppURL:   "http://app/serve",
		Debounce: &registry.Debounce{Period: time.Hour, Key: "data.doc_id"}})
	ctx := context.Background()

	routeAll(t, r, st, event("evt_1", "d1"), event("evt_2", "d2"), event("evt_3", "d1"))

	n, err := st.CountRuns(ctx)
	if err != nil || n != 2 {
		t.Fatalf("runs=%d err=%v, want 2 (one per key)", n, err)
	}
	for _, key := range []string{"d1", "d2"} {
		if _, ok, err := st.GetDebouncePending(ctx, "index-doc", key); err != nil || !ok {
			t.Fatalf("window for key %q missing: ok=%v err=%v", key, ok, err)
		}
	}
}

// timeout 封顶：持续到来的事件不能把触发时间推过 first_at + timeout。
func TestDebounceTimeoutCapsDelay(t *testing.T) {
	r, st := setupDebounce(t, registry.Function{ID: "index-doc", Event: "doc/changed",
		AppURL:   "http://app/serve",
		Debounce: &registry.Debounce{Period: time.Hour, Timeout: 50 * time.Millisecond}})
	ctx := context.Background()

	routeAll(t, r, st, event("evt_1", "d1"))
	first, _, err := st.GetDebouncePending(ctx, "index-doc", "")
	if err != nil {
		t.Fatal(err)
	}
	routeAll(t, r, st, event("evt_2", "d1"))

	p, _, err := st.GetDebouncePending(ctx, "index-doc", "")
	if err != nil {
		t.Fatal(err)
	}
	latest := first.FirstAt.Add(50 * time.Millisecond)
	if p.TriggerAt.After(latest) {
		t.Fatalf("trigger_at=%v exceeds cap %v (period must not push past timeout)", p.TriggerAt, latest)
	}
	if !p.FirstAt.Equal(first.FirstAt) {
		t.Fatalf("first_at moved: %v → %v", first.FirstAt, p.FirstAt)
	}
}

// 暂停的函数不开窗（FR-7.3 与防抖叠加时，暂停优先）。
func TestDebouncePausedFunctionOpensNoWindow(t *testing.T) {
	r, st := setupDebounce(t, registry.Function{ID: "index-doc", Event: "doc/changed",
		AppURL: "http://app/serve", Debounce: &registry.Debounce{Period: time.Hour}})
	ctx := context.Background()
	if err := st.SetFunctionPaused(ctx, "index-doc", true); err != nil {
		t.Fatal(err)
	}

	routeAll(t, r, st, event("evt_1", "d1"))

	if n, _ := st.CountRuns(ctx); n != 0 {
		t.Fatalf("runs=%d, want 0 while paused", n)
	}
	if _, ok, _ := st.GetDebouncePending(ctx, "index-doc", ""); ok {
		t.Fatal("paused function must not open a debounce window")
	}
}

// 关窗后（run 被派发）的下一个事件重新开窗，产生新的 run。
func TestDebounceReopensAfterDispatch(t *testing.T) {
	r, st := setupDebounce(t, registry.Function{ID: "index-doc", Event: "doc/changed",
		AppURL: "http://app/serve", Debounce: &registry.Debounce{Period: time.Hour}})
	ctx := context.Background()

	routeAll(t, r, st, event("evt_1", "d1"))
	p, _, err := st.GetDebouncePending(ctx, "index-doc", "")
	if err != nil {
		t.Fatal(err)
	}
	// 模拟 executor 派发该 run 时关窗
	if err := st.DeleteDebouncePendingByRun(ctx, p.RunID); err != nil {
		t.Fatal(err)
	}

	routeAll(t, r, st, event("evt_2", "d1"))

	n, err := st.CountRuns(ctx)
	if err != nil || n != 2 {
		t.Fatalf("runs=%d err=%v, want 2 (window reopened)", n, err)
	}
}
