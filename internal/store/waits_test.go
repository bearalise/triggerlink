package store

import (
	"context"
	"encoding/json"
	"testing"
	"time"
)

// seedWaitRun 造一个 Running run 供 wait 挂靠。
func seedWaitRun(t *testing.T, st *SQLiteStore, id, fnID string) {
	t.Helper()
	if _, err := st.CreateRun(context.Background(), Run{ID: id, FunctionID: fnID, Status: RunQueued,
		EventID: "evt_t", EventName: "order/created", EventData: json.RawMessage(`{"order_id":"o1"}`), EventTS: time.Now()}); err != nil {
		t.Fatal(err)
	}
}

func TestWaitCreateAndQueryByEvent(t *testing.T) {
	st := openTemp(t)
	ctx := context.Background()
	seedWaitRun(t, st, "run_w1", "fn")

	expires := time.Now().Add(time.Hour)
	w := Wait{ID: "wait_1", RunID: "run_w1", StepHash: "h1", StepID: "wait-payed",
		EventName: "order/payed", MatchExpr: `data.order_id == event.data.order_id`,
		ExpiresAt: &expires, Status: WaitWaiting}
	if err := st.CreateWait(ctx, w); err != nil {
		t.Fatal(err)
	}
	// 无超时的 wait
	if err := st.CreateWait(ctx, Wait{ID: "wait_2", RunID: "run_w1", StepHash: "h2", StepID: "wait-ship",
		EventName: "order/shipped", Status: WaitWaiting}); err != nil {
		t.Fatal(err)
	}

	got, err := st.WaitingWaitsForEvent(ctx, "order/payed")
	if err != nil || len(got) != 1 {
		t.Fatalf("waits: %+v err=%v", got, err)
	}
	if got[0].ID != "wait_1" || got[0].StepID != "wait-payed" || got[0].ExpiresAt == nil {
		t.Fatalf("wait: %+v", got[0])
	}
	// 别的状态/事件名不应命中
	if err := st.SetWaitStatus(ctx, "wait_1", WaitResolved); err != nil {
		t.Fatal(err)
	}
	got, _ = st.WaitingWaitsForEvent(ctx, "order/payed")
	if len(got) != 0 {
		t.Fatalf("resolved wait still returned: %+v", got)
	}
	got, _ = st.WaitingWaitsForEvent(ctx, "order/shipped")
	if len(got) != 1 || got[0].ExpiresAt != nil {
		t.Fatalf("wait_2: %+v", got)
	}
}

// TestWaitCreateIdempotent：同 (run_id, step_hash) 重复 CreateWait 不产生第二条（崩溃重试幂等）。
func TestWaitCreateIdempotent(t *testing.T) {
	st := openTemp(t)
	ctx := context.Background()
	seedWaitRun(t, st, "run_w2", "fn")

	w := Wait{RunID: "run_w2", StepHash: "h1", StepID: "wait-payed", EventName: "order/payed", Status: WaitWaiting}
	if err := st.CreateWait(ctx, w); err != nil {
		t.Fatal(err)
	}
	w.ID = "wait_dup" // 不同 id、同 (run_id, step_hash)
	if err := st.CreateWait(ctx, w); err != nil {
		t.Fatal(err)
	}
	got, err := st.WaitingWaitsForEvent(ctx, "order/payed")
	if err != nil || len(got) != 1 {
		t.Fatalf("waits=%+v err=%v, want exactly 1", got, err)
	}
}

func TestExpiredWaits(t *testing.T) {
	st := openTemp(t)
	ctx := context.Background()
	seedWaitRun(t, st, "run_w3", "fn")

	past := time.Now().Add(-time.Minute)
	future := time.Now().Add(time.Hour)
	mk := func(id, hash string, exp *time.Time) {
		if err := st.CreateWait(ctx, Wait{ID: id, RunID: "run_w3", StepHash: hash, StepID: "s",
			EventName: "e/x", ExpiresAt: exp, Status: WaitWaiting}); err != nil {
			t.Fatal(err)
		}
	}
	mk("wait_exp", "h_exp", &past)
	mk("wait_future", "h_future", &future)
	mk("wait_noexp", "h_noexp", nil)

	got, err := st.ExpiredWaits(ctx, time.Now())
	if err != nil || len(got) != 1 || got[0].ID != "wait_exp" {
		t.Fatalf("expired: %+v err=%v", got, err)
	}
	// 已终结的 wait 不再被扫描到
	if err := st.SetWaitStatus(ctx, "wait_exp", WaitTimeout); err != nil {
		t.Fatal(err)
	}
	got, _ = st.ExpiredWaits(ctx, time.Now())
	if len(got) != 0 {
		t.Fatalf("expired after timeout: %+v", got)
	}
}

func TestCancelWaitsForRun(t *testing.T) {
	st := openTemp(t)
	ctx := context.Background()
	seedWaitRun(t, st, "run_w4", "fn")
	seedWaitRun(t, st, "run_w5", "fn")
	for _, w := range []Wait{
		{ID: "wait_a", RunID: "run_w4", StepHash: "h1", StepID: "s", EventName: "e/x", Status: WaitWaiting},
		{ID: "wait_b", RunID: "run_w4", StepHash: "h2", StepID: "s", EventName: "e/y", Status: WaitWaiting},
		{ID: "wait_c", RunID: "run_w5", StepHash: "h1", StepID: "s", EventName: "e/x", Status: WaitWaiting},
	} {
		if err := st.CreateWait(ctx, w); err != nil {
			t.Fatal(err)
		}
	}
	n, err := st.CancelWaitsForRun(ctx, "run_w4")
	if err != nil || n != 2 {
		t.Fatalf("cancelled=%d err=%v, want 2", n, err)
	}
	got, _ := st.WaitingWaitsForEvent(ctx, "e/x")
	if len(got) != 1 || got[0].RunID != "run_w5" { // 其他 run 不受影响
		t.Fatalf("remaining: %+v", got)
	}
}

// TestActiveRunsByFunction：cancelOn 候选集只含 Queued/Running。
func TestActiveRunsByFunction(t *testing.T) {
	st := openTemp(t)
	ctx := context.Background()
	seedWaitRun(t, st, "run_a1", "fn_a") // Queued
	seedWaitRun(t, st, "run_a2", "fn_a")
	if _, err := st.MarkRunRunning(ctx, "run_a2"); err != nil { // Running
		t.Fatal(err)
	}
	seedWaitRun(t, st, "run_a3", "fn_a")
	if err := st.CompleteRun(ctx, "run_a3", json.RawMessage(`{}`)); err != nil { // 终态
		t.Fatal(err)
	}
	seedWaitRun(t, st, "run_b1", "fn_b")

	runs, err := st.ActiveRunsByFunction(ctx, "fn_a")
	if err != nil || len(runs) != 2 {
		t.Fatalf("runs=%+v err=%v, want 2", runs, err)
	}
	for _, r := range runs {
		if r.EventName != "order/created" || string(r.EventData) != `{"order_id":"o1"}` {
			t.Fatalf("run missing trigger event fields: %+v", r)
		}
	}
}

// TestCancelOnPersistence：cancel_on JSON 列随注册记录持久化往返。
func TestCancelOnPersistence(t *testing.T) {
	st := openTemp(t)
	ctx := context.Background()
	if err := st.ReplaceAppFunctions(ctx, "http://a/serve", []RegisteredFunction{
		{ID: "fn1", Event: "e/x", Retries: 3, CancelOn: `[{"event":"e/cancel","match":"data.id == event.data.id"}]`},
		{ID: "fn2", Event: "e/y"},
	}); err != nil {
		t.Fatal(err)
	}
	got, err := st.LoadRegisteredFunctions(ctx)
	if err != nil || len(got) != 2 {
		t.Fatalf("loaded: %+v err=%v", got, err)
	}
	if got[0].CancelOn != `[{"event":"e/cancel","match":"data.id == event.data.id"}]` {
		t.Fatalf("cancel_on=%q", got[0].CancelOn)
	}
	if got[1].CancelOn != "" {
		t.Fatalf("cancel_on=%q, want empty", got[1].CancelOn)
	}
	// upsert 覆盖
	if err := st.ReplaceAppFunctions(ctx, "http://a/serve", []RegisteredFunction{
		{ID: "fn1", Event: "e/x", Retries: 3},
	}); err != nil {
		t.Fatal(err)
	}
	got, _ = st.LoadRegisteredFunctions(ctx)
	if len(got) != 1 || got[0].CancelOn != "" {
		t.Fatalf("after replace: %+v", got)
	}
}
