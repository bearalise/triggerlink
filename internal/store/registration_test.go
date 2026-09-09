package store

import (
	"context"
	"testing"
	"time"
)

// 流控配置以原样 JSON 落库并原样取回；改注册（同 app 重新 Replace）时覆盖，
// 清空配置的函数读回空串（而非留着上一次的值）。
func TestRegisteredFunctionFlowControlRoundTrip(t *testing.T) {
	st := openTemp(t)
	ctx := context.Background()

	debounce := `{"period":"5m","key":"data.doc_id","timeout":"1h"}`
	throttle := `{"limit":10,"period":"1m"}`
	batch := `{"max_size":100,"timeout":"30s"}`
	in := RegisteredFunction{
		ID: "index-doc", AppURL: "http://a/serve", Event: "doc/changed",
		Match: `data.kind == "full"`, Retries: 3, Timeout: 5 * time.Minute,
		Debounce: debounce, Throttle: throttle, Batch: batch,
	}
	if err := st.ReplaceAppFunctions(ctx, "http://a/serve", []RegisteredFunction{in}); err != nil {
		t.Fatal(err)
	}

	got, err := st.LoadRegisteredFunctions(ctx)
	if err != nil || len(got) != 1 {
		t.Fatalf("len=%d err=%v", len(got), err)
	}
	if got[0] != in {
		t.Fatalf("round trip mismatch:\n in=%+v\nout=%+v", in, got[0])
	}

	// 去掉流控配置后重新注册：三列清空
	in.Debounce, in.Throttle, in.Batch = "", "", ""
	if err := st.ReplaceAppFunctions(ctx, "http://a/serve", []RegisteredFunction{in}); err != nil {
		t.Fatal(err)
	}
	got, err = st.LoadRegisteredFunctions(ctx)
	if err != nil || len(got) != 1 {
		t.Fatalf("len=%d err=%v", len(got), err)
	}
	if got[0].Debounce != "" || got[0].Throttle != "" || got[0].Batch != "" {
		t.Fatalf("flow control must be cleared, got %+v", got[0])
	}
}

// 老库（functions 表无这三列）迁移后读回空串，不报错——ensureColumn 幂等补列。
func TestRegisteredFunctionFlowControlLegacyRow(t *testing.T) {
	st := openTemp(t)
	ctx := context.Background()
	// 模拟老行：直接写入不含新列的记录
	if _, err := st.exec(ctx,
		`INSERT INTO functions (id, app_url, event, retries, updated_at) VALUES (?,?,?,?,?)`,
		"legacy", "http://a/serve", "x/y", 0, fmtTime(time.Now())); err != nil {
		t.Fatal(err)
	}
	got, err := st.LoadRegisteredFunctions(ctx)
	if err != nil || len(got) != 1 {
		t.Fatalf("len=%d err=%v", len(got), err)
	}
	if got[0].Debounce != "" || got[0].Throttle != "" || got[0].Batch != "" {
		t.Fatalf("legacy row must read back empty, got %+v", got[0])
	}
}
