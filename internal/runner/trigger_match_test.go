package runner

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/bearalise/triggerlink/internal/registry"
	"github.com/bearalise/triggerlink/internal/store"
)

// TestTriggerMatch：事件触发 match（FR-3.1）——命中/空 match 建 run；不命中/非法表达式不建。
func TestTriggerMatch(t *testing.T) {
	r, st, stream := setup(t)
	reg := r.Reg
	reg.Register(registry.Function{ID: "fn-hit", Event: "order/created", AppURL: "http://app/serve",
		Match: `data.amount > 100`})
	reg.Register(registry.Function{ID: "fn-miss", Event: "order/created", AppURL: "http://app/serve",
		Match: `data.amount > 1000`})
	reg.Register(registry.Function{ID: "fn-nomatch", Event: "order/created", AppURL: "http://app/serve"}) // 空 match：恒命中
	reg.Register(registry.Function{ID: "fn-badexpr", Event: "order/created", AppURL: "http://app/serve",
		Match: `data.amount >>`}) // 非法表达式：编译失败视为不命中
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go r.Run(ctx)

	stream <- store.Event{ID: "evt_ord", Name: "order/created",
		Data: json.RawMessage(`{"amount":500}`), TS: time.Now()}
	waitRuns(t, st, 2) // fn-hit 与 fn-nomatch 各建一个 run

	// 建出的 run 只属于命中的两个函数
	seen := map[string]bool{}
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) && len(seen) < 2 {
		for _, fid := range []string{"fn-hit", "fn-miss", "fn-nomatch", "fn-badexpr"} {
			runs, _ := st.ActiveRunsByFunction(ctx, fid)
			if len(runs) > 0 {
				seen[fid] = true
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	if !seen["fn-hit"] || !seen["fn-nomatch"] {
		t.Fatalf("hit/empty-match functions must have runs: %v", seen)
	}
	if seen["fn-miss"] || seen["fn-badexpr"] {
		t.Fatalf("miss/bad-expr functions must not have runs: %v", seen)
	}
}

// TestTriggerMatchEnvOnlyData：触发 match 环境只含 data（不含 event），引用 event 求值失败视为不命中。
func TestTriggerMatchEnvOnlyData(t *testing.T) {
	r, st, stream := setup(t)
	r.Reg.Register(registry.Function{ID: "fn-env", Event: "order/created", AppURL: "http://app/serve",
		Match: `data.order_id == event.data.order_id`})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go r.Run(ctx)

	stream <- store.Event{ID: "evt_env", Name: "order/created",
		Data: json.RawMessage(`{"order_id":"o1"}`), TS: time.Now()}
	waitRouted(t, st)
	time.Sleep(200 * time.Millisecond)

	runs, _ := st.ActiveRunsByFunction(ctx, "fn-env")
	if len(runs) != 0 {
		t.Fatalf("env must only contain data, runs=%+v", runs)
	}
}
