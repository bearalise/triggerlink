// Package step 提供 durable step 原语：Run（执行并 memo）与 Sleep/SleepUntil（挂起）。
//
// 机制（PRD 8.2/10.1）：memo 命中 → 注入缓存值直接返回；未命中 → 执行 fn，
// 然后以 panic 携带 opcode 中断函数，由 Serve recover 并序列化给平台。
package step

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/triggerlink/triggerlink/sdk/internal/execx"
)

// Run 执行一个 durable step：fn 的返回值被平台持久化，崩溃恢复后直接注入不重跑。
// fn 内做副作用；编排逻辑（step 之间的胶水代码）保持轻量且确定（PRD 8.1）。
func Run[T any](ctx context.Context, id string, fn func(context.Context) (T, error)) (T, error) {
	ec := execx.From(ctx)
	if ec == nil {
		panic("step.Run: no triggerlink execution context (called outside a TriggerLink function?)")
	}
	h := ec.NextHash(id)
	if st, ok := ec.Steps[h]; ok && st.Status == "completed" {
		var out T
		if err := json.Unmarshal(st.Output, &out); err != nil {
			var zero T
			return zero, fmt.Errorf("step.Run %q: unmarshal memo: %w", id, err)
		}
		return out, nil
	}
	v, err := fn(ctx)
	if err != nil {
		panic(&execx.Opcode{
			Op: "StepError", ID: h, StepID: id,
			Err: &execx.OpError{Message: err.Error(), Retryable: true},
		})
	}
	panic(&execx.Opcode{Op: "StepComplete", ID: h, StepID: id, Output: v})
}

// Sleep 挂起当前 run 直至 d 之后：函数中断，平台到点重新回调恢复。
// 挂起期间不占连接与计算；memo 语义同 Run——恢复重入时直接返回，不会重复睡眠。
func Sleep(ctx context.Context, id string, d time.Duration) {
	SleepUntil(ctx, id, time.Now().Add(d))
}

// SleepUntil 挂起当前 run 直至绝对时刻 at（Sleep 的绝对时间版本）。
func SleepUntil(ctx context.Context, id string, at time.Time) {
	ec := execx.From(ctx)
	if ec == nil {
		panic("step.Sleep: no triggerlink execution context (called outside a TriggerLink function?)")
	}
	h := ec.NextHash(id)
	if st, ok := ec.Steps[h]; ok && st.Status == "completed" {
		return // 已睡过（平台唤醒时已把 memo 置为 completed）
	}
	panic(&execx.Opcode{Op: "Sleep", ID: h, StepID: id, Until: &at})
}

// Event 是 SendEvent 待扇出的事件（与平台 eventPayload 同构）。
// ID 缺省时平台按 (run_id, step_hash, 序号) 确定性派生——崩溃重试不会重复扇出；
// 自带业务唯一键则更强（与 Event API 的幂等去重同语义）。
type Event struct {
	ID   string    `json:"id,omitempty"`
	Name string    `json:"name"`
	Data any       `json:"data,omitempty"`
	TS   time.Time `json:"ts,omitempty"`
}

// SendEvent 在函数内可靠扇出事件（PRD FR-2.7）：平台先落库后路由，崩溃不丢；
// memo 语义同 Run——恢复重入时直接返回已发事件 ID 列表，不会重复发送。
func SendEvent(ctx context.Context, id string, events ...Event) ([]string, error) {
	ec := execx.From(ctx)
	if ec == nil {
		panic("step.SendEvent: no triggerlink execution context (called outside a TriggerLink function?)")
	}
	h := ec.NextHash(id)
	if st, ok := ec.Steps[h]; ok && st.Status == "completed" {
		var ids []string
		if err := json.Unmarshal(st.Output, &ids); err != nil {
			return nil, fmt.Errorf("step.SendEvent %q: unmarshal memo: %w", id, err)
		}
		return ids, nil
	}
	if len(events) == 0 {
		return nil, fmt.Errorf("step.SendEvent %q: no events", id)
	}
	panic(&execx.Opcode{Op: "SendEvent", ID: h, StepID: id, Events: events})
}
