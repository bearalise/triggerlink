// Package flowkey 求值流控的分组键（debounce/throttle/batch 的 key，FR-4.4/4.5/4.7）。
//
// 独立成包是因为求值发生在两处：路由层（runner，debounce/batch 开窗）与执行层
// （executor，throttle 占额），两边必须对同一事件算出同一个键。
//
// 求值环境只含 data（= 事件 data），与触发 match（FR-3.1）一致但要求结果是标量：
// 表达式为空、编译失败、求值失败一律归到固定键 ""——即整个函数共用一个窗口/配额。
// 宁可合并过头，也不要因为表达式笔误把流控整个绕过。
package flowkey

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"sync"

	"github.com/expr-lang/expr"
	"github.com/expr-lang/expr/vm"
)

// cache 缓存预编译的表达式程序，键为表达式字符串。
var cache sync.Map // string → *vm.Program

// Eval 求值分组键。eventID 只用于日志定位。
func Eval(exprStr string, data json.RawMessage, eventID string) string {
	if exprStr == "" {
		return ""
	}
	prog, err := compile(exprStr)
	if err != nil {
		slog.Warn("flow-control key compile failed", "component", "flowkey", "expr", exprStr, "err", err)
		return ""
	}
	out, err := expr.Run(prog, map[string]any{"data": jsonToAny(data)})
	if err != nil {
		slog.Warn("flow-control key eval failed", "component", "flowkey",
			"expr", exprStr, "event_id", eventID, "err", err)
		return ""
	}
	if out == nil {
		return ""
	}
	return fmt.Sprintf("%v", out)
}

func compile(exprStr string) (*vm.Program, error) {
	if p, ok := cache.Load(exprStr); ok {
		return p.(*vm.Program), nil
	}
	prog, err := expr.Compile(exprStr)
	if err != nil {
		return nil, err
	}
	cache.Store(exprStr, prog)
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
