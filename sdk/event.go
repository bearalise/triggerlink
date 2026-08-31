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

// Debounce 是防抖配置（FR-4.5）：同 (函数, Key) 的事件在 Period 窗口内合并为一个 run，
// 取尾事件——窗口内每来一个新事件就把触发时间推后 Period。Key 是可选 expr 表达式，
// 环境只含 data，留空表示整个函数共用一个窗口。Timeout 是距首个事件的最长延迟，
// 用于给持续刷新的窗口封顶；0 = 不封顶。
type Debounce struct {
	Period  time.Duration
	Key     string
	Timeout time.Duration
}

// Throttle 是限流配置（FR-4.4）：同 (函数, Key) 在 Period 窗口内最多启动 Limit 个 run
// 的首次执行。超限的 run 不丢弃，延迟到下一窗口。
type Throttle struct {
	Limit  int
	Period time.Duration
	Key    string
}

// Batch 是批处理配置（FR-4.7）：同 (函数, Key) 的事件攒够 MaxSize 条，或距首条超过
// Timeout，即以事件数组触发一个 run（Input.Events 为全部事件，Input.Event 为首条）。
type Batch struct {
	MaxSize int
	Timeout time.Duration
	Key     string
}

// FunctionHandler 是函数处理器类型，普通 handler 与 OnFailure handler 同型。
type FunctionHandler func(ctx context.Context, in Input) (any, error)

// FunctionOpts 是函数定义配置。
type FunctionOpts struct {
	ID       string     // 稳定标识，改名语义见 PRD 8.6
	Event    string     // 订阅的事件名，如 "user/signup"
	Match    string     // 事件触发 match 表达式（FR-3.1），expr 语法，环境只含 data（= 事件 data）
	Cron     string     // 标准 5 字段 cron（分 时 日 月 周），UTC
	Retries  int        // 重试上限；0 = 平台默认（4）
	CancelOn []CancelOn // 取消规则，随 manifest 上报（cancel_on）
	// Timeout：run 级超时（FR-4.3），随 manifest 上报（timeout，Go duration 字符串）。
	// run 从创建起超过该时长仍在 Queued/Running，平台置 Failed 并发 triggerlink/run.failed
	// 事件——配置了 OnFailure 的函数会因此触发。0 = 不限时。
	Timeout time.Duration
	// Debounce / Throttle / Batch：流控配置（FR-4.5 / FR-4.4 / FR-4.7），随 manifest 上报为
	// debounce / throttle / batch，时长为 Go duration 字符串。nil = 不启用。
	// Debounce 与 Batch 都作用于路由层，同时配置时以 Debounce 为准。
	Debounce *Debounce
	Throttle *Throttle
	Batch    *Batch
	// OnFailure：run 进入 Failed 终态时的处理器（FR-2.11）。Serve 时注册隐式函数
	// <id>/on-failure，订阅内部事件 triggerlink/run.failed（match 按 function_id 过滤），
	// Input.Event.Data = {"run_id","function_id","error","event":{...触发事件...}}。
	// OnFailure 自身失败不会递归触发。
	OnFailure FunctionHandler
}

// Function 是一个 durable 函数定义。
type Function struct {
	opts    FunctionOpts
	handler FunctionHandler
}

// CreateFunction 注册一个函数。handler 中的副作用必须放进 step.Run（PRD 8.1）。
func CreateFunction(opts FunctionOpts, handler FunctionHandler) *Function {
	return &Function{opts: opts, handler: handler}
}
