package store

import (
	"context"
	"encoding/json"
	"testing"
	"time"
)

func seedRunRec(t *testing.T, st *SQLiteStore, id, fnID, status, eventID string) {
	t.Helper()
	ctx := context.Background()
	r := Run{ID: id, FunctionID: fnID, EventID: eventID, EventName: "doc/uploaded",
		EventData: json.RawMessage(`{"doc_id":"d1"}`), EventTS: time.Now()}
	if err := st.CreateRun(ctx, r); err != nil {
		t.Fatal(err)
	}
	switch status {
	case RunRunning:
		if _, err := st.MarkRunRunning(ctx, id); err != nil {
			t.Fatal(err)
		}
	case RunCompleted:
		if err := st.CompleteRun(ctx, id, json.RawMessage(`{"ok":true}`)); err != nil {
			t.Fatal(err)
		}
	}
}

func TestListRunsFiltersAndCursor(t *testing.T) {
	st := openTemp(t)
	ctx := context.Background()
	// 用 NewTestID 的 seq 保证 id 字符串序严格递增
	seedRunRec(t, st, NewTestID("run", 1), "fn-a", RunCompleted, "evt_1")
	seedRunRec(t, st, NewTestID("run", 2), "fn-a", RunRunning, "evt_1")
	seedRunRec(t, st, NewTestID("run", 3), "fn-b", RunCompleted, "evt_2")

	all, err := st.ListRuns(ctx, ListRunsOptions{})
	if err != nil || len(all) != 3 {
		t.Fatalf("all: len=%d err=%v", len(all), err)
	}
	if all[0].ID != NewTestID("run", 3) {
		t.Fatalf("order: first=%s", all[0].ID)
	}
	if all[0].CreatedAt.IsZero() || all[0].EndedAt == nil {
		t.Fatalf("timestamps not filled: %+v", all[0])
	}
	if all[0].EventData != nil || all[0].Output != nil {
		t.Fatalf("list must not fill payload fields")
	}

	byFn, _ := st.ListRuns(ctx, ListRunsOptions{FunctionID: "fn-a"})
	if len(byFn) != 2 {
		t.Fatalf("byFn: %d", len(byFn))
	}
	byStatus, _ := st.ListRuns(ctx, ListRunsOptions{Status: RunRunning})
	if len(byStatus) != 1 || byStatus[0].ID != NewTestID("run", 2) {
		t.Fatalf("byStatus: %+v", byStatus)
	}
	page, _ := st.ListRuns(ctx, ListRunsOptions{Before: NewTestID("run", 3)})
	if len(page) != 2 || page[0].ID != NewTestID("run", 2) {
		t.Fatalf("cursor: %+v", page)
	}
	limited, _ := st.ListRuns(ctx, ListRunsOptions{Limit: 1})
	if len(limited) != 1 {
		t.Fatalf("limit: %d", len(limited))
	}
	byEvent, _ := st.ListRuns(ctx, ListRunsOptions{EventID: "evt_1"})
	if len(byEvent) != 2 {
		t.Fatalf("byEvent: %d", len(byEvent))
	}
}

func TestListEventsCursorAndName(t *testing.T) {
	st := openTemp(t)
	ctx := context.Background()
	for i, name := range []string{"a", "b", "a"} {
		e := Event{ID: NewTestID("evt", i+1), Name: name, Data: json.RawMessage(`{}`), TS: time.Now()}
		if _, err := st.InsertEvent(ctx, e); err != nil {
			t.Fatal(err)
		}
	}
	all, err := st.ListEvents(ctx, ListEventsOptions{})
	if err != nil || len(all) != 3 || all[0].ID != NewTestID("evt", 3) {
		t.Fatalf("all: %+v err=%v", all, err)
	}
	named, _ := st.ListEvents(ctx, ListEventsOptions{Name: "a"})
	if len(named) != 2 {
		t.Fatalf("named: %d", len(named))
	}
	page, _ := st.ListEvents(ctx, ListEventsOptions{Before: NewTestID("evt", 3), Limit: 1})
	if len(page) != 1 || page[0].ID != NewTestID("evt", 2) {
		t.Fatalf("page: %+v", page)
	}
}

func TestRunIDsByEventIDs(t *testing.T) {
	st := openTemp(t)
	ctx := context.Background()
	seedRunRec(t, st, NewTestID("run", 1), "fn-a", RunQueued, "evt_1")
	seedRunRec(t, st, NewTestID("run", 2), "fn-b", RunQueued, "evt_1")
	seedRunRec(t, st, NewTestID("run", 3), "fn-c", RunQueued, "evt_2")
	m, err := st.RunIDsByEventIDs(ctx, []string{"evt_1", "evt_2"})
	if err != nil || len(m["evt_1"]) != 2 || len(m["evt_2"]) != 1 {
		t.Fatalf("%+v err=%v", m, err)
	}
	empty, err := st.RunIDsByEventIDs(ctx, nil)
	if err != nil || len(empty) != 0 {
		t.Fatalf("empty: %+v err=%v", empty, err)
	}
}

func TestListStepRecordsOrderAndUpdatedAt(t *testing.T) {
	st := openTemp(t)
	ctx := context.Background()
	seedRunRec(t, st, NewTestID("run", 1), "fn-a", RunQueued, "evt_1")
	for i, sid := range []string{"parse", "chunk"} {
		err := st.SaveStepResult(ctx, StepResult{
			RunID: NewTestID("run", 1), StepHash: NewTestID("sh", i+1), StepID: sid, Op: "run",
			Status: StepCompleted, Output: json.RawMessage(`{"n":1}`), Attempt: 1,
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	recs, err := st.ListStepRecords(ctx, NewTestID("run", 1))
	if err != nil || len(recs) != 2 {
		t.Fatalf("%+v err=%v", recs, err)
	}
	if recs[0].StepID != "parse" || recs[1].StepID != "chunk" {
		t.Fatalf("order: %+v", recs)
	}
	if recs[0].UpdatedAt == nil {
		t.Fatalf("updated_at should be set on new rows")
	}
}

func TestFunctionStats24h(t *testing.T) {
	st := openTemp(t)
	ctx := context.Background()
	seedRunRec(t, st, NewTestID("run", 1), "fn-a", RunCompleted, "evt_1")
	seedRunRec(t, st, NewTestID("run", 2), "fn-a", RunRunning, "evt_1")
	seedRunRec(t, st, NewTestID("run", 3), "fn-b", RunCompleted, "evt_2")
	stats, err := st.FunctionStats24h(ctx, time.Now().Add(-24*time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	if s := stats["fn-a"]; s.Total != 2 || s.Completed != 1 || s.Failed != 0 {
		t.Fatalf("fn-a: %+v", s)
	}
	if s := stats["fn-b"]; s.Total != 1 || s.Completed != 1 {
		t.Fatalf("fn-b: %+v", s)
	}
	future, _ := st.FunctionStats24h(ctx, time.Now().Add(time.Hour))
	if len(future) != 0 {
		t.Fatalf("future window: %+v", future)
	}
}
