package scheduler

import (
	"context"
	"encoding/json"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/bearalise/triggerlink/internal/registry"
	"github.com/bearalise/triggerlink/internal/store"
)

func setupSched(t *testing.T, fns ...registry.Function) (*Scheduler, store.Store) {
	t.Helper()
	st, err := store.Open(filepath.Join(t.TempDir(), "test.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() })
	reg := registry.New()
	for _, f := range fns {
		reg.Register(f)
	}
	return &Scheduler{Store: st, Reg: reg}, st
}

// TestCronFiresAndIdempotent：每分钟表达式到点触发（CreateRun + Enqueue）；
// 同一到点重复触发幂等（fired 记录 + 确定性 EventID）；窗口推进后触发下一分钟。
func TestCronFiresAndIdempotent(t *testing.T) {
	s, st := setupSched(t, registry.Function{ID: "fn-cron", Event: "", Cron: "* * * * *", AppURL: "http://app/serve"})
	ctx := context.Background()

	t0 := time.Date(2026, 8, 31, 10, 0, 30, 0, time.UTC)
	s.check(ctx, t0) // 首次调用只建立基线，不触发
	if n, _ := st.CountRuns(ctx); n != 0 {
		t.Fatalf("baseline check fired %d runs", n)
	}

	// 窗口 (10:00:30, 10:01:15] 含到点 10:01:00 → 触发
	s.check(ctx, t0.Add(45*time.Second))
	if n, _ := st.CountRuns(ctx); n != 1 {
		t.Fatalf("runs=%d, want 1", n)
	}

	// run 字段断言：EventID 确定性、EventName、EventData 携带到点 ts
	at := time.Date(2026, 8, 31, 10, 1, 0, 0, time.UTC)
	wantEventID := fmt.Sprintf("cron_fn-cron_%d", at.Unix())
	runs := listRuns(t, st)
	if runs[0].EventID != wantEventID || runs[0].EventName != EventName || runs[0].FunctionID != "fn-cron" {
		t.Fatalf("run: %+v", runs[0])
	}
	full, err := st.GetRun(ctx, runs[0].ID) // ListRuns 不填充 EventData/EventTS，按 ID 取全量
	if err != nil {
		t.Fatal(err)
	}
	var data map[string]string
	if err := json.Unmarshal(full.EventData, &data); err != nil || data["ts"] != at.Format(time.RFC3339) {
		t.Fatalf("event data=%s err=%v, want ts=%s", full.EventData, err, at.Format(time.RFC3339))
	}
	if !full.EventTS.Equal(at) {
		t.Fatalf("event ts=%v, want %v", full.EventTS, at)
	}

	// 队列项已入队可租赁
	items, err := st.LeaseBatch(ctx, time.Now(), time.Minute, 4)
	if err != nil || len(items) != 1 || items[0].RunID != runs[0].ID {
		t.Fatalf("queue items=%+v err=%v", items, err)
	}

	// 幂等：基线回拨重扫同一窗口（崩溃重放），同一到点不重复建 run
	s.mu.Lock()
	s.last = t0
	s.mu.Unlock()
	s.check(ctx, t0.Add(45*time.Second))
	if n, _ := st.CountRuns(ctx); n != 1 {
		t.Fatalf("idempotent replay fired, runs=%d, want 1", n)
	}

	// 窗口推进到下一分钟 → 触发新到点 10:02:00
	s.check(ctx, t0.Add(90*time.Second))
	if n, _ := st.CountRuns(ctx); n != 2 {
		t.Fatalf("runs=%d, want 2", n)
	}
}

// TestCronInvalidExprSkipped：非法 cron 表达式记日志跳过，不影响其他函数。
func TestCronInvalidExprSkipped(t *testing.T) {
	s, st := setupSched(t,
		registry.Function{ID: "fn-bad", Cron: "not a cron", AppURL: "http://app/serve"},
		registry.Function{ID: "fn-good", Cron: "* * * * *", AppURL: "http://app/serve"})
	ctx := context.Background()

	t0 := time.Date(2026, 8, 31, 10, 0, 30, 0, time.UTC)
	s.check(ctx, t0)
	s.check(ctx, t0.Add(45*time.Second))

	runs := listRuns(t, st)
	if len(runs) != 1 || runs[0].FunctionID != "fn-good" {
		t.Fatalf("runs=%+v, want only fn-good", runs)
	}
}

// TestCronNoBackfillAfterStart：Run 以启动时刻为基线，错过的到点不补跑（M2 语义）。
func TestCronNoBackfillAfterStart(t *testing.T) {
	s, st := setupSched(t, registry.Function{ID: "fn-cron", Cron: "* * * * *", AppURL: "http://app/serve"})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Run 内部以 time.Now() 为基线：即便表达式每分钟到点，启动前的到点不补
	go s.Run(ctx)
	time.Sleep(50 * time.Millisecond) // 等基线建立
	s.check(ctx, time.Now().Add(500*time.Millisecond)) // 窗口不足 1 分钟，不含新到点
	if n, _ := st.CountRuns(ctx); n != 0 {
		t.Fatalf("backfilled %d runs", n)
	}
}

// listRuns 读取全部 run 摘要（scheduler 测试断言用；EventData/EventTS 需再按 ID GetRun 取全量）。
func listRuns(t *testing.T, st store.Store) []store.Run {
	t.Helper()
	runs, err := st.ListRuns(context.Background(), store.ListRunsOptions{})
	if err != nil {
		t.Fatal(err)
	}
	return runs
}

// TestCronPausedFunctionNotFired：暂停的函数 cron 到点不触发（FR-7.3）；恢复后正常触发。
func TestCronPausedFunctionNotFired(t *testing.T) {
	s, st := setupSched(t, registry.Function{ID: "fn-cron", Cron: "* * * * *", AppURL: "http://app/serve"})
	ctx := context.Background()
	if err := st.SetFunctionPaused(ctx, "fn-cron", true); err != nil {
		t.Fatal(err)
	}

	t0 := time.Date(2026, 8, 31, 10, 0, 30, 0, time.UTC)
	s.check(ctx, t0) // 建立基线
	s.check(ctx, t0.Add(45*time.Second))
	if n, _ := st.CountRuns(ctx); n != 0 {
		t.Fatalf("paused function fired %d runs", n)
	}

	// 恢复后窗口推进到下一分钟 → 正常触发
	if err := st.SetFunctionPaused(ctx, "fn-cron", false); err != nil {
		t.Fatal(err)
	}
	s.check(ctx, t0.Add(90*time.Second))
	if n, _ := st.CountRuns(ctx); n != 1 {
		t.Fatalf("resumed function not fired, runs=%d, want 1", n)
	}
}
