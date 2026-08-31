package registry

import (
	"encoding/json"
	"strings"
	"testing"
	"time"
)

func TestManifestFlowControlParsing(t *testing.T) {
	raw := `{"id":"f1","event":"doc/changed",
	  "debounce":{"period":"5m","key":"data.doc_id","timeout":"1h"},
	  "throttle":{"limit":10,"period":"1m","key":"data.tenant"},
	  "batch":{"max_size":100,"timeout":"30s"}}`
	var f Function
	if err := json.Unmarshal([]byte(raw), &f); err != nil {
		t.Fatal(err)
	}
	if f.Debounce == nil || f.Debounce.Period != 5*time.Minute ||
		f.Debounce.Key != "data.doc_id" || f.Debounce.Timeout != time.Hour {
		t.Fatalf("debounce=%+v", f.Debounce)
	}
	if f.Throttle == nil || f.Throttle.Limit != 10 ||
		f.Throttle.Period != time.Minute || f.Throttle.Key != "data.tenant" {
		t.Fatalf("throttle=%+v", f.Throttle)
	}
	if f.Batch == nil || f.Batch.MaxSize != 100 || f.Batch.Timeout != 30*time.Second || f.Batch.Key != "" {
		t.Fatalf("batch=%+v", f.Batch)
	}
}

// 未配置 = nil；三个字段互相独立。
func TestManifestFlowControlAbsent(t *testing.T) {
	var f Function
	if err := json.Unmarshal([]byte(`{"id":"f1","event":"x/y","debounce":null}`), &f); err != nil {
		t.Fatal(err)
	}
	if f.Debounce != nil || f.Throttle != nil || f.Batch != nil {
		t.Fatalf("want all nil, got %+v %+v %+v", f.Debounce, f.Throttle, f.Batch)
	}
}

// 单个配置非法只让该配置失效，不影响同一函数的其余字段与其他配置（内省不整体失败）。
func TestManifestFlowControlInvalidIgnored(t *testing.T) {
	cases := []struct {
		name string
		body string
	}{
		{"debounce 缺 period", `{"debounce":{"key":"data.x"}}`},
		{"debounce period 非法", `{"debounce":{"period":"5 minutes"}}`},
		{"debounce period 为 0", `{"debounce":{"period":"0s"}}`},
		{"debounce timeout 非法", `{"debounce":{"period":"1m","timeout":"nope"}}`},
		{"throttle limit 为 0", `{"throttle":{"limit":0,"period":"1m"}}`},
		{"throttle 缺 period", `{"throttle":{"limit":5}}`},
		{"batch max_size 为 0", `{"batch":{"max_size":0,"timeout":"30s"}}`},
		{"batch 缺 timeout", `{"batch":{"max_size":10}}`},
		{"类型不匹配", `{"throttle":{"limit":"ten","period":"1m"}}`},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			body := `{"id":"f1","event":"x/y","retries":3,` + c.body[1:]
			var f Function
			if err := json.Unmarshal([]byte(body), &f); err != nil {
				t.Fatalf("manifest decode must not fail: %v", err)
			}
			if f.ID != "f1" || f.Retries != 3 {
				t.Fatalf("other fields must survive: %+v", f)
			}
			if f.Debounce != nil || f.Throttle != nil || f.Batch != nil {
				t.Fatalf("invalid config must be dropped: %+v %+v %+v", f.Debounce, f.Throttle, f.Batch)
			}
		})
	}
}

// Marshal → Unmarshal 往返恒等：落库形态与清单形态是同一段 JSON。
func TestFlowControlJSONRoundTrip(t *testing.T) {
	in := Function{
		ID:       "f1",
		Event:    "x/y",
		Debounce: &Debounce{Period: 90 * time.Second, Key: "data.k", Timeout: 2 * time.Hour},
		Throttle: &Throttle{Limit: 3, Period: time.Minute},
		Batch:    &Batch{MaxSize: 50, Timeout: 10 * time.Second, Key: "data.t"},
	}
	b, err := json.Marshal(in)
	if err != nil {
		t.Fatal(err)
	}
	var out Function
	if err := json.Unmarshal(b, &out); err != nil {
		t.Fatalf("round trip decode: %v (json=%s)", err, b)
	}
	if *out.Debounce != *in.Debounce || *out.Throttle != *in.Throttle || *out.Batch != *in.Batch {
		t.Fatalf("round trip mismatch:\n in=%+v %+v %+v\nout=%+v %+v %+v",
			in.Debounce, in.Throttle, in.Batch, out.Debounce, out.Throttle, out.Batch)
	}
	// 时长以 Go duration 字符串编码（与 manifest 契约一致，不是纳秒整数）
	if got := string(b); !strings.Contains(got, `"period":"1m30s"`) || !strings.Contains(got, `"max_size":50`) {
		t.Fatalf("unexpected wire form: %s", got)
	}
}
