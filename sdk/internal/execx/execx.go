// Package execx 是 SDK 内部的执行上下文：step hash 计数器、memo 表、opcode 类型。
// step 包与 serve 包通过它协作；对用户完全不可见（internal）。
package execx

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"strconv"
	"sync"
	"time"
)

// StepState 是平台注入的单个 step memo（与服务端 executor.stepState 同构）。
type StepState struct {
	ID     string          `json:"id"`
	Status string          `json:"status"`
	Output json.RawMessage `json:"output,omitempty"`
}

// OpError 是 step/函数错误的协议形状。
type OpError struct {
	Message   string `json:"message"`
	Stack     string `json:"stack,omitempty"`
	Retryable bool   `json:"retryable"`
}

// Opcode 是 SDK 以 panic 携带、由 serve recover 后序列化的执行指令。
type Opcode struct {
	Op     string     `json:"op"`
	ID     string     `json:"id,omitempty"`
	StepID string     `json:"step_id,omitempty"`
	Output any        `json:"output,omitempty"`
	Err    *OpError   `json:"error,omitempty"`
	Until  *time.Time `json:"until,omitempty"`  // Sleep：唤醒时刻
	Events any        `json:"events,omitempty"` // SendEvent：待扇出事件列表
}

// Ctx 是单次函数调用的执行上下文。
type Ctx struct {
	FunctionID string
	Steps      map[string]StepState

	mu       sync.Mutex
	counters map[string]int
}

// New 构造执行上下文；steps 为平台注入的已完成 step memo（可为 nil）。
func New(functionID string, steps map[string]StepState) *Ctx {
	if steps == nil {
		steps = map[string]StepState{}
	}
	return &Ctx{FunctionID: functionID, Steps: steps, counters: map[string]int{}}
}

// NextHash 返回 stepID 本次调用第 N 次出现的 memo 键：
// sha256(functionID + ":" + stepID + ":" + 序号)（PRD 8.2）。
func (c *Ctx) NextHash(stepID string) string {
	c.mu.Lock()
	n := c.counters[stepID]
	c.counters[stepID] = n + 1
	c.mu.Unlock()
	sum := sha256.Sum256([]byte(c.FunctionID + ":" + stepID + ":" + strconv.Itoa(n)))
	return hex.EncodeToString(sum[:])
}

type ctxKey struct{}

// With 把执行上下文放进 context。
func With(parent context.Context, c *Ctx) context.Context {
	return context.WithValue(parent, ctxKey{}, c)
}

// From 取出执行上下文；不在 TriggerLink 函数内时返回 nil。
func From(ctx context.Context) *Ctx {
	c, _ := ctx.Value(ctxKey{}).(*Ctx)
	return c
}
