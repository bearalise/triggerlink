package store

import (
	"context"
	"path/filepath"
	"testing"
	"time"
)

// TestPausedFunctionsPersistence：暂停标记的持久化往返（FR-7.3）——
// 设置/清除语义正确；ReplaceAppFunctions 整体覆盖 functions 表不影响暂停状态；
// 关闭重开（模拟平台重启）后标记仍在。
func TestPausedFunctionsPersistence(t *testing.T) {
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "test.db")

	st, err := Open(dbPath)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}

	if paused, err := st.IsFunctionPaused(ctx, "f1"); err != nil || paused {
		t.Fatalf("initial: paused=%v err=%v", paused, err)
	}
	if err := st.SetFunctionPaused(ctx, "f1", true); err != nil {
		t.Fatal(err)
	}
	if err := st.SetFunctionPaused(ctx, "f2", true); err != nil {
		t.Fatal(err)
	}
	if paused, err := st.IsFunctionPaused(ctx, "f1"); err != nil || !paused {
		t.Fatalf("after set: paused=%v err=%v", paused, err)
	}
	ids, err := st.PausedFunctionIDs(ctx)
	if err != nil || len(ids) != 2 || !ids["f1"] || !ids["f2"] {
		t.Fatalf("paused ids: %+v err=%v", ids, err)
	}

	// functions 表整体覆盖（应用重新注册的 ReplaceAppFunctions 语义）不丢暂停状态
	if err := st.ReplaceAppFunctions(ctx, "http://a/serve", []RegisteredFunction{
		{ID: "f1", Event: "x/y"},
		{ID: "f2", Event: "x/y"},
	}); err != nil {
		t.Fatal(err)
	}
	if paused, _ := st.IsFunctionPaused(ctx, "f1"); !paused {
		t.Fatal("paused flag lost after ReplaceAppFunctions")
	}

	// 恢复 f2：集合只剩 f1
	if err := st.SetFunctionPaused(ctx, "f2", false); err != nil {
		t.Fatal(err)
	}
	ids, err = st.PausedFunctionIDs(ctx)
	if err != nil || len(ids) != 1 || !ids["f1"] {
		t.Fatalf("after resume f2: %+v err=%v", ids, err)
	}

	// 关闭重开（模拟平台重启）→ 标记仍在
	st.Close()
	st2, err := Open(dbPath)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	t.Cleanup(func() { st2.Close() })
	if paused, err := st2.IsFunctionPaused(ctx, "f1"); err != nil || !paused {
		t.Fatalf("after reopen: paused=%v err=%v", paused, err)
	}
	if paused, _ := st2.IsFunctionPaused(ctx, "f2"); paused {
		t.Fatal("f2 must stay resumed after reopen")
	}
}

// TestTimeoutRun：run 超时终态化（FR-4.3 的存储原语）——Queued/Running → Failed +
// error + ended_at，同事务删除 pending 队列项；终态幂等（transitioned=false）。
func TestTimeoutRun(t *testing.T) {
	st := openTemp(t)
	ctx := context.Background()
	seedRun(t, st)
	if err := st.Enqueue(ctx, QueueItem{ID: "qi_1", FunctionID: "process-doc", RunID: "run_1",
		Score: time.Now().UnixNano(), At: time.Now(), Status: QueuePending}); err != nil {
		t.Fatal(err)
	}

	ok, err := st.TimeoutRun(ctx, "run_1", "run timeout after 5m0s")
	if err != nil || !ok {
		t.Fatalf("timeout: ok=%v err=%v", ok, err)
	}
	r, _ := st.GetRun(ctx, "run_1")
	if r.Status != RunFailed || r.Error != "run timeout after 5m0s" || r.EndedAt == nil {
		t.Fatalf("timed out: %+v", r)
	}

	// 幂等：终态不再迁移
	if ok, _ := st.TimeoutRun(ctx, "run_1", "again"); ok {
		t.Fatal("second timeout should report transitioned=false")
	}

	// pending 队列项已随超时删除：LeaseBatch 租不到
	items, err := st.LeaseBatch(ctx, time.Now(), time.Minute, 10)
	if err != nil {
		t.Fatal(err)
	}
	for _, it := range items {
		if it.RunID == "run_1" {
			t.Fatalf("timed-out run still has pending queue item: %+v", it)
		}
	}
}

// TestAppFunctionsTimeoutRoundtrip：timeout 列（FR-4.3）持久化往返，
// 关闭重开后仍在；未设置的函数读出为 0（不限制）。
func TestAppFunctionsTimeoutRoundtrip(t *testing.T) {
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "test.db")

	st, err := Open(dbPath)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if err := st.ReplaceAppFunctions(ctx, "http://a/serve", []RegisteredFunction{
		{ID: "f1", Event: "x/y", Timeout: 5 * time.Minute},
		{ID: "f2", Event: "x/y"},
	}); err != nil {
		t.Fatal(err)
	}

	got, err := st.LoadRegisteredFunctions(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 || got[0].Timeout != 5*time.Minute || got[1].Timeout != 0 {
		t.Fatalf("roundtrip: %+v", got)
	}

	st.Close()
	st2, err := Open(dbPath)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	t.Cleanup(func() { st2.Close() })
	got, err = st2.LoadRegisteredFunctions(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 || got[0].Timeout != 5*time.Minute || got[1].Timeout != 0 {
		t.Fatalf("after reopen: %+v", got)
	}
}
