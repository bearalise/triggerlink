package store

import (
	"context"
	"encoding/json"
	"testing"
	"time"
)

func seedRun(t *testing.T, st *SQLiteStore) Run {
	t.Helper()
	r := Run{
		ID:         "run_1",
		FunctionID: "process-doc",
		Status:     RunQueued,
		EventID:    "evt_1",
		EventName:  "doc/uploaded",
		EventData:  json.RawMessage(`{"doc_id":"d1"}`),
		EventTS:    time.Now(),
	}
	if err := st.CreateRun(context.Background(), r); err != nil {
		t.Fatalf("CreateRun: %v", err)
	}
	return r
}

func TestRunLifecycle(t *testing.T) {
	st := openTemp(t)
	ctx := context.Background()
	seedRun(t, st)

	if _, err := st.MarkRunRunning(ctx, "run_1"); err != nil {
		t.Fatal(err)
	}
	r, err := st.GetRun(ctx, "run_1")
	if err != nil || r.Status != RunRunning {
		t.Fatalf("running: %+v err=%v", r, err)
	}
	if r.EventName != "doc/uploaded" || string(r.EventData) != `{"doc_id":"d1"}` {
		t.Fatalf("event snapshot: %+v", r)
	}

	att, err := st.IncrementRunAttempt(ctx, "run_1")
	if err != nil || att != 1 {
		t.Fatalf("attempt=%d err=%v", att, err)
	}
	att, _ = st.IncrementRunAttempt(ctx, "run_1")
	if att != 2 {
		t.Fatalf("attempt=%d", att)
	}

	if err := st.CompleteRun(ctx, "run_1", json.RawMessage(`{"ok":true}`)); err != nil {
		t.Fatal(err)
	}
	r, _ = st.GetRun(ctx, "run_1")
	if r.Status != RunCompleted || string(r.Output) != `{"ok":true}` {
		t.Fatalf("completed: %+v", r)
	}
}

func TestFailRunAndCount(t *testing.T) {
	st := openTemp(t)
	ctx := context.Background()
	seedRun(t, st)
	if err := st.FailRun(ctx, "run_1", "boom"); err != nil {
		t.Fatal(err)
	}
	r, _ := st.GetRun(ctx, "run_1")
	if r.Status != RunFailed || r.Error != "boom" {
		t.Fatalf("failed: %+v", r)
	}
	n, err := st.CountRuns(ctx)
	if err != nil || n != 1 {
		t.Fatalf("count=%d err=%v", n, err)
	}
}

func TestCancelRun(t *testing.T) {
	st := openTemp(t)
	ctx := context.Background()
	seedRun(t, st)
	if err := st.Enqueue(ctx, QueueItem{ID: "qi_1", FunctionID: "process-doc", RunID: "run_1",
		Score: time.Now().UnixNano(), At: time.Now(), Status: QueuePending}); err != nil {
		t.Fatal(err)
	}

	ok, err := st.CancelRun(ctx, "run_1")
	if err != nil || !ok {
		t.Fatalf("cancel: ok=%v err=%v", ok, err)
	}
	r, _ := st.GetRun(ctx, "run_1")
	if r.Status != RunCancelled || r.EndedAt == nil {
		t.Fatalf("cancelled: %+v", r)
	}

	// 幂等：终态不再迁移
	if ok, _ := st.CancelRun(ctx, "run_1"); ok {
		t.Fatal("second cancel should report transitioned=false")
	}
	// 终态 run 不允许回到 Running
	if ok, _ := st.MarkRunRunning(ctx, "run_1"); ok {
		t.Fatal("cancelled run must not transition to Running")
	}

	// pending 队列项已随取消删除：LeaseBatch 租不到
	items, err := st.LeaseBatch(ctx, time.Now(), time.Minute, 10)
	if err != nil {
		t.Fatal(err)
	}
	for _, it := range items {
		if it.RunID == "run_1" {
			t.Fatalf("cancelled run still has pending queue item: %+v", it)
		}
	}
}
