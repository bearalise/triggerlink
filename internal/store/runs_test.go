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

	if err := st.MarkRunRunning(ctx, "run_1"); err != nil {
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
