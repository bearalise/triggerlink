package store

import (
	"context"
	"encoding/json"
	"testing"
	"time"
)

func batchEvent(id string) Event {
	return Event{ID: id, Name: "doc/changed", Data: json.RawMessage(`{"id":"` + id + `"}`), TS: time.Now()}
}

// 追加返回窗口内条目总数；条目按 seq 升序取回（= 到达顺序）；窗口 first_at 保持首条时刻。
func TestAppendAndReadBatchEntries(t *testing.T) {
	st := openTemp(t)
	ctx := context.Background()

	for i, id := range []string{"evt_1", "evt_2", "evt_3"} {
		n, err := st.AppendBatchEntry(ctx, "fn", "k1", batchEvent(id))
		if err != nil || n != i+1 {
			t.Fatalf("append %s: count=%d err=%v, want %d", id, n, err, i+1)
		}
	}
	// 不同 key 各自计数
	if n, err := st.AppendBatchEntry(ctx, "fn", "k2", batchEvent("evt_9")); err != nil || n != 1 {
		t.Fatalf("other key: count=%d err=%v, want 1", n, err)
	}

	entries, err := st.BatchEntries(ctx, "fn", "k1")
	if err != nil || len(entries) != 3 {
		t.Fatalf("entries=%d err=%v, want 3", len(entries), err)
	}
	for i, want := range []string{"evt_1", "evt_2", "evt_3"} {
		if entries[i].Event.ID != want {
			t.Fatalf("entry[%d]=%s, want %s (seq order)", i, entries[i].Event.ID, want)
		}
	}

	windows, err := st.ListBatchWindows(ctx)
	if err != nil || len(windows) != 2 {
		t.Fatalf("windows=%d err=%v, want 2", len(windows), err)
	}
}

// flush 建 run + 入队 + 删条目；只删读过的 seq，之后到达的留在窗口里继续攒。
func TestFlushBatch(t *testing.T) {
	st := openTemp(t)
	ctx := context.Background()

	for _, id := range []string{"evt_1", "evt_2"} {
		if _, err := st.AppendBatchEntry(ctx, "fn", "k1", batchEvent(id)); err != nil {
			t.Fatal(err)
		}
	}
	entries, err := st.BatchEntries(ctx, "fn", "k1")
	if err != nil {
		t.Fatal(err)
	}
	// flush 决策做出后、事务开始前又来一条
	if _, err := st.AppendBatchEntry(ctx, "fn", "k1", batchEvent("evt_late")); err != nil {
		t.Fatal(err)
	}

	payload, _ := json.Marshal([]map[string]any{{"id": "evt_1"}, {"id": "evt_2"}})
	run := Run{ID: "run_b1", FunctionID: "fn", Status: RunQueued, EventID: "evt_1",
		EventName: "doc/changed", EventData: payload, EventTS: time.Now(), Batch: true}
	item := QueueItem{ID: "qi_b1", FunctionID: "fn", RunID: "run_b1",
		Score: time.Now().UnixNano(), At: time.Now(), Status: QueuePending}
	if err := st.FlushBatch(ctx, "fn", "k1", entries[len(entries)-1].Seq, run, item); err != nil {
		t.Fatal(err)
	}

	got, err := st.GetRun(ctx, "run_b1")
	if err != nil {
		t.Fatal(err)
	}
	if !got.Batch {
		t.Fatal("flushed run must be marked as a batch run")
	}
	if string(got.EventData) != string(payload) {
		t.Fatalf("event_data=%s, want the event array", got.EventData)
	}
	if n, err := st.QueueDepth(ctx); err != nil || n != 1 {
		t.Fatalf("queue depth=%d err=%v, want 1", n, err)
	}

	// 迟到的那条仍在窗口里
	left, err := st.BatchEntries(ctx, "fn", "k1")
	if err != nil || len(left) != 1 || left[0].Event.ID != "evt_late" {
		t.Fatalf("remaining entries=%+v, want only evt_late", left)
	}
	windows, err := st.ListBatchWindows(ctx)
	if err != nil || len(windows) != 1 {
		t.Fatalf("window must stay open for the late event: %+v err=%v", windows, err)
	}
}

// 全部条目被 flush 时窗口关闭。
func TestFlushBatchClosesEmptyWindow(t *testing.T) {
	st := openTemp(t)
	ctx := context.Background()
	if _, err := st.AppendBatchEntry(ctx, "fn", "k1", batchEvent("evt_1")); err != nil {
		t.Fatal(err)
	}
	entries, _ := st.BatchEntries(ctx, "fn", "k1")

	run := Run{ID: "run_b1", FunctionID: "fn", Status: RunQueued, EventID: "evt_1",
		EventName: "doc/changed", EventData: json.RawMessage(`[]`), EventTS: time.Now(), Batch: true}
	item := QueueItem{ID: "qi_b1", FunctionID: "fn", RunID: "run_b1",
		Score: time.Now().UnixNano(), At: time.Now(), Status: QueuePending}
	if err := st.FlushBatch(ctx, "fn", "k1", entries[0].Seq, run, item); err != nil {
		t.Fatal(err)
	}

	windows, err := st.ListBatchWindows(ctx)
	if err != nil || len(windows) != 0 {
		t.Fatalf("windows=%+v err=%v, want none", windows, err)
	}
}

// 非批 run 的 Batch 为 false（老行默认 0，读回也是 false）。
func TestNonBatchRunFlag(t *testing.T) {
	st := openTemp(t)
	ctx := context.Background()
	seedRun(t, st)
	r, err := st.GetRun(ctx, "run_1")
	if err != nil {
		t.Fatal(err)
	}
	if r.Batch {
		t.Fatal("ordinary run must not be marked as batch")
	}
}
