package step

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

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
