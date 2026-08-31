package step

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	triggerlink "github.com/bearalise/triggerlink/sdk"
	"github.com/bearalise/triggerlink/sdk/internal/execx"
)

// capture 在 execx 上下文中调用 fn 并 recover 出 opcode（模拟 serve 的中断机制）。
func capture(ctx context.Context, fn func()) (op *execx.Opcode) {
	defer func() {
		if rv := recover(); rv != nil {
			op, _ = rv.(*execx.Opcode)
		}
	}()
	fn()
	return nil
}

func TestMemoHitSkipsExecution(t *testing.T) {
	// 预计算 "parse" 第 0 次出现的 hash
	pre := execx.New("fn", nil)
	h := pre.NextHash("parse")

	ec := execx.New("fn", map[string]execx.StepState{
		h: {ID: "parse", Status: "completed", Output: json.RawMessage(`"cached"`)},
	})
	ctx := execx.With(context.Background(), ec)

	executed := false
	got, err := Run(ctx, "parse", func(ctx context.Context) (string, error) {
		executed = true
		return "fresh", nil
	})
	if err != nil || got != "cached" {
		t.Fatalf("got=%q err=%v", got, err)
	}
	if executed {
		t.Fatal("memo hit must not execute fn")
	}
}

func TestMissExecutesAndPanicsStepComplete(t *testing.T) {
	ec := execx.New("fn", nil)
	ctx := execx.With(context.Background(), ec)

	executed := false
	op := capture(ctx, func() {
		Run(ctx, "parse", func(ctx context.Context) (int, error) {
			executed = true
			return 42, nil
		})
	})
	if !executed {
		t.Fatal("miss must execute fn")
	}
	if op == nil || op.Op != "StepComplete" || op.StepID != "parse" || op.ID == "" {
		t.Fatalf("op=%+v", op)
	}
	if v, ok := op.Output.(int); !ok || v != 42 {
		t.Fatalf("output=%v", op.Output)
	}
}

func TestErrorPanicsStepError(t *testing.T) {
	ec := execx.New("fn", nil)
	ctx := execx.With(context.Background(), ec)

	op := capture(ctx, func() {
		Run(ctx, "embed", func(ctx context.Context) (string, error) {
			return "", errors.New("rate limited")
		})
	})
	if op == nil || op.Op != "StepError" || op.StepID != "embed" {
		t.Fatalf("op=%+v", op)
	}
	if op.Err == nil || op.Err.Message != "rate limited" || !op.Err.Retryable {
		t.Fatalf("err=%+v", op.Err)
	}
}

func TestSleepMissPanicsSleepOpcode(t *testing.T) {
	ec := execx.New("fn", nil)
	ctx := execx.With(context.Background(), ec)

	before := time.Now()
	op := capture(ctx, func() {
		Sleep(ctx, "wait-a-bit", time.Minute)
	})
	if op == nil || op.Op != "Sleep" || op.StepID != "wait-a-bit" || op.ID == "" {
		t.Fatalf("op=%+v", op)
	}
	if op.Until == nil || op.Until.Before(before.Add(59*time.Second)) {
		t.Fatalf("until=%v", op.Until)
	}
}

func TestSleepMemoHitReturns(t *testing.T) {
	pre := execx.New("fn", nil)
	h := pre.NextHash("nap")

	ec := execx.New("fn", map[string]execx.StepState{
		h: {ID: "nap", Status: "completed"}, // 平台唤醒后注入：已睡过
	})
	ctx := execx.With(context.Background(), ec)

	op := capture(ctx, func() {
		Sleep(ctx, "nap", time.Hour) // memo 命中：直接返回，不抛 opcode
	})
	if op != nil {
		t.Fatalf("memo hit must not panic, got %+v", op)
	}
}

func TestSendEventMissPanicsOpcode(t *testing.T) {
	ec := execx.New("fn", nil)
	ctx := execx.With(context.Background(), ec)

	op := capture(ctx, func() {
		_, _ = SendEvent(ctx, "notify",
			Event{Name: "notify/email", Data: map[string]any{"to": "a@b.c"}},
			Event{ID: "custom-1", Name: "notify/sms"})
	})
	if op == nil || op.Op != "SendEvent" || op.StepID != "notify" || op.ID == "" {
		t.Fatalf("op=%+v", op)
	}
	evts, ok := op.Events.([]Event)
	if !ok || len(evts) != 2 || evts[0].Name != "notify/email" {
		t.Fatalf("events=%v", op.Events)
	}
}

func TestSendEventMemoHitReturnsIDs(t *testing.T) {
	pre := execx.New("fn", nil)
	h := pre.NextHash("notify")

	ec := execx.New("fn", map[string]execx.StepState{
		h: {ID: "notify", Status: "completed", Output: json.RawMessage(`["evt_a","evt_b"]`)},
	})
	ctx := execx.With(context.Background(), ec)

	ids, err := SendEvent(ctx, "notify", Event{Name: "notify/email"})
	if err != nil || len(ids) != 2 || ids[0] != "evt_a" {
		t.Fatalf("ids=%v err=%v", ids, err)
	}
}

func TestSendEventEmptyReturnsError(t *testing.T) {
	ec := execx.New("fn", nil)
	ctx := execx.With(context.Background(), ec)
	if _, err := SendEvent(ctx, "notify"); err == nil {
		t.Fatal("expected error for empty events")
	}
}

func TestWaitForEventMissPanicsOpcode(t *testing.T) {
	ec := execx.New("fn", nil)
	ctx := execx.With(context.Background(), ec)

	op := capture(ctx, func() {
		_, _ = WaitForEvent(ctx, "wait-payment", WaitOpts{
			Event:   "order/payed",
			Match:   "data.order_id == event.data.order_id",
			Timeout: 7 * 24 * time.Hour,
		})
	})
	if op == nil || op.Op != "WaitForEvent" || op.StepID != "wait-payment" || op.ID == "" {
		t.Fatalf("op=%+v", op)
	}
	if op.Event != "order/payed" {
		t.Fatalf("event=%q", op.Event)
	}
	if op.Match != "data.order_id == event.data.order_id" {
		t.Fatalf("match=%q", op.Match)
	}
	if op.Timeout != "168h0m0s" { // time.Duration.String() 序列化
		t.Fatalf("timeout=%q", op.Timeout)
	}
}

func TestWaitForEventNoOptionalFields(t *testing.T) {
	ec := execx.New("fn", nil)
	ctx := execx.With(context.Background(), ec)

	op := capture(ctx, func() {
		_, _ = WaitForEvent(ctx, "wait", WaitOpts{Event: "order/payed"})
	})
	if op == nil || op.Op != "WaitForEvent" || op.Match != "" || op.Timeout != "" {
		t.Fatalf("op=%+v", op)
	}
}

func TestWaitForEventMemoHitReturnsEvent(t *testing.T) {
	pre := execx.New("fn", nil)
	h := pre.NextHash("wait-payment")

	ts := time.Date(2026, 8, 31, 12, 0, 0, 0, time.UTC)
	out, _ := json.Marshal(map[string]any{
		"id": "evt_pay", "name": "order/payed",
		"data": map[string]any{"order_id": "o1"}, "ts": ts,
	})
	ec := execx.New("fn", map[string]execx.StepState{
		h: {ID: "wait-payment", Status: "completed", Output: out},
	})
	ctx := execx.With(context.Background(), ec)

	var ev *triggerlink.Event
	var err error
	op := capture(ctx, func() {
		ev, err = WaitForEvent(ctx, "wait-payment", WaitOpts{Event: "order/payed"})
	})
	if op != nil {
		t.Fatalf("memo hit must not panic, got %+v", op)
	}
	if err != nil || ev == nil {
		t.Fatalf("ev=%v err=%v", ev, err)
	}
	if ev.ID != "evt_pay" || ev.Name != "order/payed" || !ev.TS.Equal(ts) {
		t.Fatalf("ev=%+v", ev)
	}
	if string(ev.Data) != `{"order_id":"o1"}` {
		t.Fatalf("data=%s", ev.Data)
	}
}

func TestWaitForEventMemoNullReturnsNil(t *testing.T) {
	pre := execx.New("fn", nil)
	h := pre.NextHash("wait-payment")

	ec := execx.New("fn", map[string]execx.StepState{
		h: {ID: "wait-payment", Status: "completed", Output: json.RawMessage("null")}, // 超时分支
	})
	ctx := execx.With(context.Background(), ec)

	ev, err := WaitForEvent(ctx, "wait-payment", WaitOpts{Event: "order/payed"})
	if err != nil || ev != nil {
		t.Fatalf("ev=%v err=%v, want nil,nil", ev, err)
	}
}

func TestWaitForEventRequiresEvent(t *testing.T) {
	ec := execx.New("fn", nil)
	ctx := execx.With(context.Background(), ec)
	if _, err := WaitForEvent(ctx, "wait", WaitOpts{}); err == nil {
		t.Fatal("expected error for empty event")
	}
}

func TestPanicsOutsideFunction(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Fatal("expected panic without exec context")
		}
	}()
	Run(context.Background(), "x", func(ctx context.Context) (int, error) { return 0, nil })
}

func TestLoopSameStepDistinctHashes(t *testing.T) {
	ec := execx.New("fn", nil)
	ctx := execx.With(context.Background(), ec)
	hashes := map[string]bool{}
	for i := 0; i < 3; i++ {
		op := capture(ctx, func() {
			Run(ctx, "tool", func(ctx context.Context) (int, error) { return i, nil })
		})
		if op == nil {
			t.Fatal("expected opcode")
		}
		if hashes[op.ID] {
			t.Fatalf("duplicate hash in loop: %s", op.ID)
		}
		hashes[op.ID] = true
	}
}
