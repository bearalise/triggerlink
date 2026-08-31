package store

import (
	"context"
	"encoding/json"
	"testing"
	"time"
)

// insertRunAt 造一条指定状态的 run；terminal 为真时 ended_at 设为 endedAt。
func insertRunAt(t *testing.T, st *SQLiteStore, id, eventID, status string, endedAt *time.Time) {
	t.Helper()
	ctx := context.Background()
	if _, err := st.CreateRun(ctx, Run{ID: id, FunctionID: "f1", Status: RunQueued,
		EventID: eventID, EventName: "doc/uploaded", EventData: json.RawMessage(`{}`), EventTS: time.Now()}); err != nil {
		t.Fatal(err)
	}
	if endedAt == nil {
		if _, err := st.db.ExecContext(ctx, `UPDATE runs SET status=? WHERE id=?`, status, id); err != nil {
			t.Fatal(err)
		}
		return
	}
	if _, err := st.db.ExecContext(ctx,
		`UPDATE runs SET status=?, ended_at=? WHERE id=?`, status, fmtTime(*endedAt), id); err != nil {
		t.Fatal(err)
	}
}

func countRows(t *testing.T, st *SQLiteStore, table string) int {
	t.Helper()
	var n int
	if err := st.db.QueryRow(`SELECT COUNT(*) FROM ` + table).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

// 过期终态 run 连同 step_runs / waits / 残留 queue_items 一并删除；未到期与在途 run 保留。
func TestDeleteRunsBefore(t *testing.T) {
	st := openTemp(t)
	ctx := context.Background()
	old := time.Now().Add(-48 * time.Hour)
	recent := time.Now().Add(-time.Hour)

	insertRunAt(t, st, "run_old", "evt_old", RunCompleted, &old)
	insertRunAt(t, st, "run_recent", "evt_recent", RunCompleted, &recent)
	insertRunAt(t, st, "run_live", "evt_live", RunRunning, nil)

	for _, rid := range []string{"run_old", "run_recent", "run_live"} {
		if err := st.SaveStepResult(ctx, StepResult{RunID: rid, StepHash: "h", StepID: "s",
			Op: "Run", Status: StepCompleted, Output: json.RawMessage(`"x"`)}); err != nil {
			t.Fatal(err)
		}
		if err := st.CreateWait(ctx, Wait{ID: "wait_" + rid, RunID: rid, StepHash: "hw", StepID: "sw",
			EventName: "doc/ready", Status: WaitResolved}); err != nil {
			t.Fatal(err)
		}
		if err := st.Enqueue(ctx, QueueItem{ID: "qi_" + rid, FunctionID: "f1", RunID: rid,
			Score: 1, At: time.Now(), Status: QueuePending}); err != nil {
			t.Fatal(err)
		}
	}

	n, err := st.DeleteRunsBefore(ctx, time.Now().Add(-24*time.Hour))
	if err != nil || n != 1 {
		t.Fatalf("deleted=%d err=%v, want 1", n, err)
	}
	if got := countRows(t, st, "runs"); got != 2 {
		t.Fatalf("runs=%d, want 2", got)
	}
	// 从属记录级联删除
	for _, table := range []string{"step_runs", "waits", "queue_items"} {
		if got := countRows(t, st, table); got != 2 {
			t.Fatalf("%s=%d, want 2 (cascade)", table, got)
		}
	}
	if _, err := st.GetRun(ctx, "run_recent"); err != nil {
		t.Fatalf("recent run should survive: %v", err)
	}
	if _, err := st.GetRun(ctx, "run_live"); err != nil {
		t.Fatalf("in-flight run should survive: %v", err)
	}
}

// 过期事件删除；被在途 run 引用的事件跳过；未到期事件保留。
func TestDeleteEventsBefore(t *testing.T) {
	st := openTemp(t)
	ctx := context.Background()
	old := time.Now().Add(-48 * time.Hour)

	for _, id := range []string{"evt_old", "evt_referenced", "evt_done_ref"} {
		if _, err := st.InsertEvent(ctx, Event{ID: id, Name: "doc/uploaded",
			Data: json.RawMessage(`{}`), TS: old}); err != nil {
			t.Fatal(err)
		}
		if _, err := st.db.ExecContext(ctx,
			`UPDATE events SET received_at=? WHERE id=?`, fmtTime(old), id); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := st.InsertEvent(ctx, Event{ID: "evt_fresh", Name: "doc/uploaded",
		Data: json.RawMessage(`{}`), TS: time.Now()}); err != nil {
		t.Fatal(err)
	}
	// 在途 run 引用 evt_referenced；终态 run 引用 evt_done_ref（不阻止删除）
	insertRunAt(t, st, "run_live", "evt_referenced", RunRunning, nil)
	ended := time.Now()
	insertRunAt(t, st, "run_done", "evt_done_ref", RunCompleted, &ended)

	n, err := st.DeleteEventsBefore(ctx, time.Now().Add(-24*time.Hour))
	if err != nil || n != 2 {
		t.Fatalf("deleted=%d err=%v, want 2 (evt_old + evt_done_ref)", n, err)
	}
	var exists int
	if err := st.db.QueryRow(`SELECT COUNT(*) FROM events WHERE id=?`, "evt_referenced").Scan(&exists); err != nil {
		t.Fatal(err)
	}
	if exists != 1 {
		t.Fatal("event referenced by an in-flight run must be kept")
	}
	if got := countRows(t, st, "events"); got != 2 {
		t.Fatalf("events=%d, want 2 (evt_referenced + evt_fresh)", got)
	}
}

// 已解除的老 wait 删除；waiting 中的挂起与未到期的记录保留。
func TestDeleteFinishedWaitsBefore(t *testing.T) {
	st := openTemp(t)
	ctx := context.Background()
	seedRun(t, st)
	old := time.Now().Add(-48 * time.Hour)

	for _, w := range []Wait{
		{ID: "wait_old_done", RunID: "run_1", StepHash: "h1", StepID: "s1", EventName: "e", Status: WaitResolved},
		{ID: "wait_old_waiting", RunID: "run_1", StepHash: "h2", StepID: "s2", EventName: "e", Status: WaitWaiting},
		{ID: "wait_new_done", RunID: "run_1", StepHash: "h3", StepID: "s3", EventName: "e", Status: WaitTimeout},
	} {
		if err := st.CreateWait(ctx, w); err != nil {
			t.Fatal(err)
		}
	}
	for _, id := range []string{"wait_old_done", "wait_old_waiting"} {
		if _, err := st.db.ExecContext(ctx, `UPDATE waits SET created_at=? WHERE id=?`, fmtTime(old), id); err != nil {
			t.Fatal(err)
		}
	}

	n, err := st.DeleteFinishedWaitsBefore(ctx, time.Now().Add(-24*time.Hour))
	if err != nil || n != 1 {
		t.Fatalf("deleted=%d err=%v, want 1", n, err)
	}
	if got := countRows(t, st, "waits"); got != 2 {
		t.Fatalf("waits=%d, want 2", got)
	}
	if n, err := st.ActiveWaitCount(ctx); err != nil || n != 1 {
		t.Fatalf("waiting wait must survive: count=%d err=%v", n, err)
	}
}
