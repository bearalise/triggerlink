package store

import (
	"context"
	"testing"
	"time"
)

// QueueDepth 只数"已到点的 pending"：未到点的延迟项与已租赁项都不算积压。
func TestQueueDepth(t *testing.T) {
	st := openTemp(t)
	ctx := context.Background()
	seedRun(t, st)
	now := time.Now()

	items := []QueueItem{
		{ID: "qi_due1", FunctionID: "f1", RunID: "run_1", Score: 1, At: now.Add(-time.Minute), Status: QueuePending},
		{ID: "qi_due2", FunctionID: "f1", RunID: "run_1", Score: 2, At: now.Add(-time.Second), Status: QueuePending},
		{ID: "qi_future", FunctionID: "f1", RunID: "run_1", Score: 3, At: now.Add(time.Hour), Status: QueuePending},
	}
	for _, it := range items {
		if err := st.Enqueue(ctx, it); err != nil {
			t.Fatal(err)
		}
	}
	if n, err := st.QueueDepth(ctx); err != nil || n != 2 {
		t.Fatalf("depth=%d err=%v, want 2", n, err)
	}

	// 租赁走人后不再是积压
	if _, err := st.LeaseBatch(ctx, now, 30*time.Second, 1); err != nil {
		t.Fatal(err)
	}
	if n, err := st.QueueDepth(ctx); err != nil || n != 1 {
		t.Fatalf("depth after lease=%d err=%v, want 1", n, err)
	}

	// 空库为 0
	if err := st.CompleteQueueItem(ctx, "qi_due2"); err != nil {
		t.Fatal(err)
	}
	if n, err := st.QueueDepth(ctx); err != nil || n != 0 {
		t.Fatalf("depth after complete=%d err=%v, want 0", n, err)
	}
}

// ActiveWaitCount 只数 waiting 状态：解除（resolved/timeout/cancelled）后不再计入。
func TestActiveWaitCount(t *testing.T) {
	st := openTemp(t)
	ctx := context.Background()
	seedRun(t, st)

	if n, err := st.ActiveWaitCount(ctx); err != nil || n != 0 {
		t.Fatalf("empty count=%d err=%v, want 0", n, err)
	}
	for _, w := range []Wait{
		{ID: "wait_1", RunID: "run_1", StepHash: "h1", StepID: "s1", EventName: "doc/ready", Status: WaitWaiting},
		{ID: "wait_2", RunID: "run_1", StepHash: "h2", StepID: "s2", EventName: "doc/ready", Status: WaitWaiting},
	} {
		if err := st.CreateWait(ctx, w); err != nil {
			t.Fatal(err)
		}
	}
	if n, err := st.ActiveWaitCount(ctx); err != nil || n != 2 {
		t.Fatalf("count=%d err=%v, want 2", n, err)
	}

	if err := st.SetWaitStatus(ctx, "wait_1", WaitResolved); err != nil {
		t.Fatal(err)
	}
	if n, err := st.ActiveWaitCount(ctx); err != nil || n != 1 {
		t.Fatalf("count after resolve=%d err=%v, want 1", n, err)
	}
}
