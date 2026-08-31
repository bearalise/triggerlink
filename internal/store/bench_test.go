package store

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"
)

// 基准跑在 openTemp 选出的后端上，因此同一组基准可直接对比两个后端：
//
//	go test ./internal/store/ -bench . -benchtime 500x -run '^$'
//	TEST_POSTGRES_URI=postgres://... go test ./internal/store/ -bench . -benchtime 500x -run '^$'

// 事件接收：Event API 热路径上唯一的写。
func BenchmarkInsertEvent(b *testing.B) {
	st := openTemp(b)
	ctx := context.Background()
	data := json.RawMessage(`{"doc_id":"d1","payload":"0123456789012345678901234567890123456789"}`)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := st.InsertEvent(ctx, Event{
			ID: fmt.Sprintf("evt_%d", i), Name: "doc/uploaded", Data: data, TS: time.Now()}); err != nil {
			b.Fatal(err)
		}
	}
}

// 路由一次事件的写入放大：建 run + 入队（Event API 之后 runner 干的活）。
func BenchmarkRouteEvent(b *testing.B) {
	st := openTemp(b)
	ctx := context.Background()
	data := json.RawMessage(`{"doc_id":"d1"}`)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		runID := fmt.Sprintf("run_%d", i)
		if _, err := st.CreateRun(ctx, Run{ID: runID, FunctionID: "fn", Status: RunQueued,
			EventID: fmt.Sprintf("evt_%d", i), EventName: "doc/uploaded", EventData: data, EventTS: time.Now()}); err != nil {
			b.Fatal(err)
		}
		if err := st.Enqueue(ctx, QueueItem{ID: fmt.Sprintf("qi_%d", i), FunctionID: "fn", RunID: runID,
			Score: time.Now().UnixNano(), At: time.Now(), Status: QueuePending}); err != nil {
			b.Fatal(err)
		}
	}
}

// 队列租赁：executor 每轮轮询都要跑的原子出队。
func BenchmarkLeaseBatch(b *testing.B) {
	st := openTemp(b)
	ctx := context.Background()
	for i := 0; i < b.N*4; i++ {
		if err := st.Enqueue(ctx, QueueItem{ID: fmt.Sprintf("qi_%d", i), FunctionID: "fn",
			RunID: fmt.Sprintf("run_%d", i), Score: int64(i), At: time.Now(), Status: QueuePending}); err != nil {
			b.Fatal(err)
		}
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := st.LeaseBatch(ctx, time.Now(), 30*time.Second, 4); err != nil {
			b.Fatal(err)
		}
	}
}

// step memo 写入：每个 step 完成都会落一次。
func BenchmarkSaveStepResult(b *testing.B) {
	st := openTemp(b)
	ctx := context.Background()
	out := json.RawMessage(`{"result":"ok"}`)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := st.SaveStepResult(ctx, StepResult{
			RunID: "run_1", StepHash: fmt.Sprintf("h%d", i), StepID: "parse", Op: "Run",
			Status: StepCompleted, Output: out, Attempt: 1}); err != nil {
			b.Fatal(err)
		}
	}
}
