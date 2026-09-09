package store

import (
	"context"
	"encoding/json"
	"testing"
	"time"
)

func TestSaveAndGetStepResults(t *testing.T) {
	st := openTemp(t)
	ctx := context.Background()
	seedRun(t, st)

	sr := StepResult{RunID: "run_1", StepHash: "h1", StepID: "parse", Op: "Run",
		Status: StepCompleted, Output: json.RawMessage(`"ok"`), Attempt: 1}
	if err := st.SaveStepResult(ctx, sr); err != nil {
		t.Fatal(err)
	}
	// 同键再存 = 重试后更新，attempt 自增
	sr.Status = StepFailed
	sr.Error = "boom"
	if err := st.SaveStepResult(ctx, sr); err != nil {
		t.Fatal(err)
	}

	got, err := st.StepResults(ctx, "run_1")
	if err != nil || len(got) != 1 {
		t.Fatalf("len=%d err=%v", len(got), err)
	}
	g := got["h1"]
	if g.StepID != "parse" || g.Status != StepFailed || g.Error != "boom" || g.Attempt != 2 {
		t.Fatalf("unexpected: %+v", g)
	}
}

func TestCompleteSleepingSteps(t *testing.T) {
	st := openTemp(t)
	ctx := context.Background()
	seedRun(t, st)

	save := func(hash, status string) {
		t.Helper()
		if err := st.SaveStepResult(ctx, StepResult{RunID: "run_1", StepHash: hash, StepID: hash, Op: "Sleep",
			Status: status, Attempt: 1}); err != nil {
			t.Fatal(err)
		}
	}
	save("h_sleep", StepSleeping)
	save("h_done", StepCompleted)
	save("h_fail", StepFailed)

	if err := st.CompleteSleepingSteps(ctx, "run_1"); err != nil {
		t.Fatal(err)
	}
	got, _ := st.StepResults(ctx, "run_1")
	if got["h_sleep"].Status != StepCompleted {
		t.Fatalf("sleeping not completed: %+v", got["h_sleep"])
	}
	if got["h_done"].Status != StepCompleted || got["h_fail"].Status != StepFailed {
		t.Fatalf("non-sleeping steps touched: %+v", got)
	}
	// 幂等：再调一次无变化、无报错
	if err := st.CompleteSleepingSteps(ctx, "run_1"); err != nil {
		t.Fatal(err)
	}
}

func seedQueueItem(t *testing.T, st *SQLStore, id string, at time.Time) {
	t.Helper()
	item := QueueItem{ID: id, FunctionID: "process-doc", RunID: "run_1",
		Score: time.Now().UnixNano(), At: at, Status: QueuePending}
	if err := st.Enqueue(context.Background(), item); err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
}

func TestQueueLeaseLifecycle(t *testing.T) {
	st := openTemp(t)
	ctx := context.Background()
	seedRun(t, st)
	now := time.Now()
	seedQueueItem(t, st, "qi_future", now.Add(time.Hour)) // 未到点，不可租
	seedQueueItem(t, st, "qi_1", now)

	items, err := st.LeaseBatch(ctx, now, 30*time.Second, 10)
	if err != nil || len(items) != 1 || items[0].ID != "qi_1" {
		t.Fatalf("lease: %+v err=%v", items, err)
	}
	// 已租出的不会再被租到
	items, err = st.LeaseBatch(ctx, now, 30*time.Second, 10)
	if err != nil || len(items) != 0 {
		t.Fatalf("re-lease: %+v err=%v", items, err)
	}
	// 租约过期 → 对账重入队
	n, err := st.RequeueExpiredLeases(ctx, now.Add(time.Minute))
	if err != nil || n != 1 {
		t.Fatalf("requeue n=%d err=%v", n, err)
	}
	items, err = st.LeaseBatch(ctx, now, 30*time.Second, 10)
	if err != nil || len(items) != 1 {
		t.Fatalf("lease after requeue: %+v err=%v", items, err)
	}
	// 退避释放：未来 at 不可见
	if err := st.ReleaseLease(ctx, "qi_1", now.Add(time.Hour)); err != nil {
		t.Fatal(err)
	}
	items, _ = st.LeaseBatch(ctx, now, 30*time.Second, 10)
	if len(items) != 0 {
		t.Fatalf("after backoff release: %+v", items)
	}
	// 完成即删除
	if err := st.CompleteQueueItem(ctx, "qi_1"); err != nil {
		t.Fatal(err)
	}
	items, _ = st.LeaseBatch(ctx, now.Add(2*time.Hour), 30*time.Second, 10)
	// qi_1 已删除；qi_future 此刻已到期，应被租出
	if len(items) != 1 || items[0].ID != "qi_future" {
		t.Fatalf("after complete: %+v", items)
	}
}
