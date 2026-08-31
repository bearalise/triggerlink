// match 表达式求值（事件触发 match、step.waitForEvent 与 cancelOn 共用，FR-3.1 / FR-2.6 / FR-4.9）。
// 用 expr-lang/expr；编译结果按表达式字符串缓存（sync.Map），编译/求值失败只记日志、
// 视为不命中，绝不 panic 中断路由。
package runner

import (
	"encoding/json"
	"log/slog"
	"sync"

	"github.com/expr-lang/expr"
	"github.com/expr-lang/expr/vm"

	"github.com/bearalise/triggerlink/internal/store"
)

// matchCache 缓存预编译的表达式程序，键为表达式字符串。
var matchCache sync.Map // string → *vm.Program

// evalMatch 判定 match 表达式是否命中。空表达式恒命中。
// 求值环境：data = 到达事件的 data（JSON 反序列化）；event = 该 run 触发事件的 {"name","data"}。
func evalMatch(exprStr string, arriving store.Event, run store.Run) bool {
	if exprStr == "" {
		return true
	}
	prog, err := compileMatch(exprStr)
	if err != nil {
		slog.Warn("match compile failed", "component", "runner", "expr", exprStr, "err", err)
		return false
	}
	env := map[string]any{
		"data": jsonToAny(arriving.Data),
		"event": map[string]any{
			"name": run.EventName,
			"data": jsonToAny(run.EventData),
		},
	}
	out, err := expr.Run(prog, env)
	if err != nil {
		slog.Warn("match eval failed", "component", "runner", "expr", exprStr, "run_id", run.ID, "err", err)
		return false
	}
	hit, ok := out.(bool)
	if !ok {
		slog.Warn("match returned non-bool", "component", "runner", "expr", exprStr, "run_id", run.ID, "result", out)
		return false
	}
	return hit
}

// evalTriggerMatch 判定事件触发 match（FR-3.1）：环境只含 data = 到达事件 data。
// 空表达式恒命中；编译/求值失败视为不命中（不为该函数建 run）。
func evalTriggerMatch(exprStr string, arriving store.Event) bool {
	if exprStr == "" {
		return true
	}
	prog, err := compileMatch(exprStr)
	if err != nil {
		slog.Warn("trigger match compile failed", "component", "runner", "expr", exprStr, "err", err)
		return false
	}
	out, err := expr.Run(prog, map[string]any{"data": jsonToAny(arriving.Data)})
	if err != nil {
		slog.Warn("trigger match eval failed", "component", "runner", "expr", exprStr, "event_id", arriving.ID, "err", err)
		return false
	}
	hit, ok := out.(bool)
	if !ok {
		slog.Warn("trigger match returned non-bool", "component", "runner", "expr", exprStr, "event_id", arriving.ID, "result", out)
		return false
	}
	return hit
}

func compileMatch(exprStr string) (*vm.Program, error) {
	if p, ok := matchCache.Load(exprStr); ok {
		return p.(*vm.Program), nil
	}
	// 不带 Env 选项编译：允许运行时环境自由组合（各 run 触发事件结构不同）
	prog, err := expr.Compile(exprStr)
	if err != nil {
		return nil, err
	}
	matchCache.Store(exprStr, prog)
	return prog, nil
}

// jsonToAny 把事件 data 反序列化为任意 JSON 值；空/非法 JSON 归一为空对象。
func jsonToAny(raw json.RawMessage) any {
	if len(raw) == 0 {
		return map[string]any{}
	}
	var v any
	if err := json.Unmarshal(raw, &v); err != nil || v == nil {
		return map[string]any{}
	}
	return v
}
