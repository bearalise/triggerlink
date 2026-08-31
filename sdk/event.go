package triggerlink

import (
	"context"
	"encoding/json"
	"time"
)

// Event 是触发函数的事件（与平台 eventPayload 同构）。
type Event struct {
	ID   string          `json:"id"`
	Name string          `json:"name"`
	Data json.RawMessage `json:"data"`
	TS   time.Time       `json:"ts"`
}

// Input 是函数处理器的入参。
type Input struct {
	Event Event
}

// CancelOn 是一条取消规则（FR-4.9）：Event 到达且 Match 命中时，平台取消该函数的在途 run。
// Match 为 expr 表达式，留空表示该事件到达即取消；求值环境：data=到达事件 data，
// event=本 run 触发事件 {name, data}。
type CancelOn struct {
	Event string `json:"event"`
	Match string `json:"match,omitempty"`
}

// FunctionOpts 是函数定义配置。M0 仅支持事件触发。
type FunctionOpts struct {
	ID       string     // 稳定标识，改名语义见 PRD 8.6
	Event    string     // 订阅的事件名，如 "user/signup"
	Retries  int        // 重试上限；0 = 平台默认（4）
	CancelOn []CancelOn // 取消规则，随 manifest 上报（cancel_on）
}

// Function 是一个 durable 函数定义。
type Function struct {
	opts    FunctionOpts
	handler func(ctx context.Context, in Input) (any, error)
}

// CreateFunction 注册一个函数。handler 中的副作用必须放进 step.Run（PRD 8.1）。
func CreateFunction(opts FunctionOpts, handler func(ctx context.Context, in Input) (any, error)) *Function {
	return &Function{opts: opts, handler: handler}
}
