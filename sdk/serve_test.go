// 外部测试包 + dot import：step 包依赖本包（WaitForEvent 返回 triggerlink.Event），
// 内部测试包再 import step 会构成 Go 不允许的测试 import cycle；dot import 保持下文无需限定名。
package triggerlink_test

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	. "github.com/bearalise/triggerlink/sdk"
	"github.com/bearalise/triggerlink/sdk/internal/execx"
	"github.com/bearalise/triggerlink/sdk/step"
)

func signHeader(key string, body []byte) string {
	t := time.Now().Unix()
	mac := hmac.New(sha256.New, []byte(key))
	fmt.Fprintf(mac, "%d.", t)
	mac.Write(body)
	return fmt.Sprintf("t=%d,v1=%s", t, hex.EncodeToString(mac.Sum(nil)))
}

func testClient() *Client { return NewClient(Options{AppID: "docsvc", SigningKey: "k"}) }

// twoStepFn：第一步 parse，第二步 embed，然后完成。
func twoStepFn() *Function {
	return CreateFunction(FunctionOpts{ID: "process-doc", Event: "doc/uploaded", Retries: 4},
		func(ctx context.Context, in Input) (any, error) {
			parsed, err := step.Run(ctx, "parse", func(ctx context.Context) (string, error) {
				return "parsed", nil
			})
			if err != nil {
				return nil, err
			}
			embedded, err := step.Run(ctx, "embed", func(ctx context.Context) (string, error) {
				return parsed + "+embedded", nil
			})
			if err != nil {
				return nil, err
			}
			return map[string]any{"result": embedded}, nil
		})
}

func postExec(t *testing.T, h http.Handler, key string, body []byte) (*httptest.ResponseRecorder, map[string]any) {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, "/api/triggerlink", bytes.NewReader(body))
	req.Header.Set("X-TriggerLink-Signature", signHeader(key, body))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	var out map[string]any
	json.NewDecoder(rec.Body).Decode(&out)
	return rec, out
}

func execBody(steps map[string]execx.StepState) []byte {
	req := map[string]any{
		"ctx": map[string]any{
			"run_id":      "run_1",
			"function_id": "process-doc",
			"attempt":     1,
			"event":       map[string]any{"id": "evt_1", "name": "doc/uploaded", "data": map[string]any{"doc_id": "d1"}, "ts": time.Now()},
			"steps":       steps,
		},
	}
	b, _ := json.Marshal(req)
	return b
}

func TestGetManifest(t *testing.T) {
	h := Serve(testClient(), twoStepFn())
	req := httptest.NewRequest(http.MethodGet, "/api/triggerlink", nil)
	req.Header.Set("X-TriggerLink-Signature", signHeader("k", nil))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("code=%d", rec.Code)
	}
	var m struct {
		SDK       string `json:"sdk"`
		AppID     string `json:"app_id"`
		Functions []struct {
			ID      string `json:"id"`
			Event   string `json:"event"`
			Retries int    `json:"retries"`
		} `json:"functions"`
	}
	json.NewDecoder(rec.Body).Decode(&m)
	if m.SDK != "triggerlink-go/0.0.0-m0" || m.AppID != "docsvc" ||
		len(m.Functions) != 1 || m.Functions[0].ID != "process-doc" || m.Functions[0].Event != "doc/uploaded" {
		t.Fatalf("manifest=%+v", m)
	}
}

func TestManifestCancelOn(t *testing.T) {
	fn := CreateFunction(FunctionOpts{
		ID: "onboarding", Event: "user/signup",
		CancelOn: []CancelOn{
			{Event: "user/deleted", Match: "data.user_id == event.data.user_id"},
			{Event: "user/opted-out"},
		},
	}, func(ctx context.Context, in Input) (any, error) { return nil, nil })
	h := Serve(testClient(), fn)
	req := httptest.NewRequest(http.MethodGet, "/api/triggerlink", nil)
	req.Header.Set("X-TriggerLink-Signature", signHeader("k", nil))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	body := rec.Body.Bytes()

	var m struct {
		Functions []struct {
			ID       string `json:"id"`
			CancelOn []struct {
				Event string `json:"event"`
				Match string `json:"match"`
			} `json:"cancel_on"`
		} `json:"functions"`
	}
	json.Unmarshal(body, &m)
	if len(m.Functions) != 1 || len(m.Functions[0].CancelOn) != 2 {
		t.Fatalf("manifest=%s", body)
	}
	c0, c1 := m.Functions[0].CancelOn[0], m.Functions[0].CancelOn[1]
	if c0.Event != "user/deleted" || c0.Match != "data.user_id == event.data.user_id" {
		t.Fatalf("cancel_on[0]=%+v", c0)
	}
	if c1.Event != "user/opted-out" || c1.Match != "" {
		t.Fatalf("cancel_on[1]=%+v", c1)
	}
	// match 为空时按 omitempty 省略（与服务端 CancelRule 反序列化对齐）
	if !bytes.Contains(body, []byte(`"cancel_on"`)) ||
		bytes.Count(body, []byte(`"match"`)) != 1 {
		t.Fatalf("body=%s", body)
	}
}

func TestManifestMatchCron(t *testing.T) {
	fn := CreateFunction(FunctionOpts{
		ID: "nightly-report", Event: "report/generate",
		Match: "data.kind == 'full'", Cron: "0 9 * * *",
	}, func(ctx context.Context, in Input) (any, error) { return nil, nil })
	plain := CreateFunction(FunctionOpts{ID: "plain", Event: "x/y"},
		func(ctx context.Context, in Input) (any, error) { return nil, nil })
	h := Serve(testClient(), fn, plain)
	req := httptest.NewRequest(http.MethodGet, "/api/triggerlink", nil)
	req.Header.Set("X-TriggerLink-Signature", signHeader("k", nil))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	body := rec.Body.Bytes()

	var m struct {
		Functions []struct {
			ID    string `json:"id"`
			Match string `json:"match"`
			Cron  string `json:"cron"`
		} `json:"functions"`
	}
	json.Unmarshal(body, &m)
	byID := map[string]struct {
		Match string
		Cron  string
	}{}
	for _, f := range m.Functions {
		byID[f.ID] = struct {
			Match string
			Cron  string
		}{f.Match, f.Cron}
	}
	if byID["nightly-report"].Match != "data.kind == 'full'" || byID["nightly-report"].Cron != "0 9 * * *" {
		t.Fatalf("manifest=%s", body)
	}
	// 未配置时按 omitempty 省略
	if strings.Contains(string(body), `"id":"plain","event":"x/y","match"`) ||
		strings.Contains(string(body), `"id":"plain","event":"x/y","cron"`) {
		t.Fatalf("plain fn should omit match/cron: %s", body)
	}
}

func TestManifestTimeout(t *testing.T) {
	fn := CreateFunction(FunctionOpts{
		ID: "slow-etl", Event: "etl/start", Timeout: 5 * time.Minute,
	}, func(ctx context.Context, in Input) (any, error) { return nil, nil })
	plain := CreateFunction(FunctionOpts{ID: "plain", Event: "x/y"},
		func(ctx context.Context, in Input) (any, error) { return nil, nil })
	h := Serve(testClient(), fn, plain)
	req := httptest.NewRequest(http.MethodGet, "/api/triggerlink", nil)
	req.Header.Set("X-TriggerLink-Signature", signHeader("k", nil))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	body := rec.Body.Bytes()

	var m struct {
		Functions []struct {
			ID      string `json:"id"`
			Timeout string `json:"timeout"`
		} `json:"functions"`
	}
	json.Unmarshal(body, &m)
	byID := map[string]string{}
	for _, f := range m.Functions {
		byID[f.ID] = f.Timeout
	}
	if byID["slow-etl"] != "5m0s" {
		t.Fatalf("manifest=%s", body)
	}
	// 未配置时按 omitempty 省略
	if byID["plain"] != "" || strings.Contains(string(body), `"id":"plain","event":"x/y","timeout"`) {
		t.Fatalf("plain fn should omit timeout: %s", body)
	}
}

func TestOnFailureImplicitFunction(t *testing.T) {
	fn := CreateFunction(FunctionOpts{
		ID: "process-doc", Event: "doc/uploaded",
		OnFailure: func(ctx context.Context, in Input) (any, error) {
			return step.Run(ctx, "notify", func(ctx context.Context) (string, error) {
				data, _ := json.Marshal(in.Event.Data)
				return "notified:" + string(data), nil
			})
		},
	}, func(ctx context.Context, in Input) (any, error) { return nil, nil })
	h := Serve(testClient(), fn)

	// manifest：隐式函数出现，id/订阅事件/match 断言
	req := httptest.NewRequest(http.MethodGet, "/api/triggerlink", nil)
	req.Header.Set("X-TriggerLink-Signature", signHeader("k", nil))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	var m struct {
		Functions []struct {
			ID    string `json:"id"`
			Event string `json:"event"`
			Match string `json:"match"`
		} `json:"functions"`
	}
	json.Unmarshal(rec.Body.Bytes(), &m)
	var implicit *struct {
		ID    string `json:"id"`
		Event string `json:"event"`
		Match string `json:"match"`
	}
	for i, f := range m.Functions {
		if f.ID == "process-doc/on-failure" {
			implicit = &m.Functions[i]
		}
	}
	if implicit == nil {
		t.Fatalf("implicit on-failure fn missing: %s", rec.Body.Bytes())
	}
	if implicit.Event != "triggerlink/run.failed" ||
		implicit.Match != "data.function_id == 'process-doc'" {
		t.Fatalf("implicit=%+v", *implicit)
	}

	// 隐式函数可被正常回调执行（内部事件 data 透传给 handler）
	failedEvent := map[string]any{
		"run_id": "run_9", "function_id": "process-doc", "error": "boom",
		"event": map[string]any{"id": "evt_1", "name": "doc/uploaded", "data": map[string]any{"doc_id": "d1"}},
	}
	body, _ := json.Marshal(map[string]any{"ctx": map[string]any{
		"run_id": "run_of_1", "function_id": "process-doc/on-failure", "attempt": 1,
		"event": map[string]any{"id": "evt_of", "name": "triggerlink/run.failed", "data": failedEvent, "ts": time.Now()},
		"steps": map[string]any{},
	}})
	rec2, out := postExec(t, h, "k", body)
	if rec2.Code != http.StatusOK || out["op"] != "StepComplete" || out["step_id"] != "notify" {
		t.Fatalf("out=%v code=%d", out, rec2.Code)
	}
}

func TestRejectsBadSignature(t *testing.T) {
	h := Serve(testClient(), twoStepFn())
	body := execBody(nil)
	_, out := postExec(t, h, "wrong-key", body)
	if len(out) != 0 {
		t.Fatalf("expected empty error body, got %v", out)
	}
	req := httptest.NewRequest(http.MethodPost, "/api/triggerlink", bytes.NewReader(body))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("code=%d", rec.Code)
	}
}

func TestProgressesOneStepPerCall(t *testing.T) {
	h := Serve(testClient(), twoStepFn())

	// 第 1 次调用：无 memo → 执行 parse 并中断
	rec, out := postExec(t, h, "k", execBody(nil))
	if rec.Code != http.StatusOK || out["op"] != "StepComplete" || out["step_id"] != "parse" {
		t.Fatalf("call1: %v", out)
	}
	hash1, _ := out["id"].(string)
	if hash1 == "" {
		t.Fatal("missing step hash")
	}

	// 第 2 次调用：注入 parse memo → 跳过 parse，执行 embed
	steps := map[string]execx.StepState{
		hash1: {ID: "parse", Status: "completed", Output: json.RawMessage(`"parsed"`)},
	}
	_, out = postExec(t, h, "k", execBody(steps))
	if out["op"] != "StepComplete" || out["step_id"] != "embed" {
		t.Fatalf("call2: %v", out)
	}
	hash2, _ := out["id"].(string)

	// 第 3 次调用：两个 memo 都注入 → RunComplete，embed 用了 parse 的注入值
	steps[hash2] = execx.StepState{ID: "embed", Status: "completed", Output: json.RawMessage(`"parsed+embedded"`)}
	_, out = postExec(t, h, "k", execBody(steps))
	if out["op"] != "RunComplete" {
		t.Fatalf("call3: %v", out)
	}
	output, _ := out["output"].(map[string]any)
	if output["result"] != "parsed+embedded" {
		t.Fatalf("output=%v", out["output"])
	}
}

func TestUnknownFunction404(t *testing.T) {
	h := Serve(testClient(), twoStepFn())
	body := execBody(nil)
	var req map[string]any
	json.Unmarshal(body, &req)
	req["ctx"].(map[string]any)["function_id"] = "nope"
	body, _ = json.Marshal(req)
	rec, _ := postExec(t, h, "k", body)
	if rec.Code != http.StatusNotFound {
		t.Fatalf("code=%d", rec.Code)
	}
}

func TestUserPanicBecomesRunError(t *testing.T) {
	fn := CreateFunction(FunctionOpts{ID: "panicky", Event: "e/x"},
		func(ctx context.Context, in Input) (any, error) {
			panic("user bug")
		})
	h := Serve(testClient(), fn)
	req := map[string]any{"ctx": map[string]any{
		"run_id": "r", "function_id": "panicky", "attempt": 1,
		"event": map[string]any{"id": "e", "name": "e/x", "data": map[string]any{}, "ts": time.Now()},
		"steps": map[string]any{},
	}}
	body, _ := json.Marshal(req)
	rec, out := postExec(t, h, "k", body)
	if rec.Code != http.StatusOK || out["op"] != "RunError" {
		t.Fatalf("out=%v code=%d", out, rec.Code)
	}
	errMap, _ := out["error"].(map[string]any)
	msg, _ := errMap["message"].(string)
	if msg == "" || !bytes.Contains([]byte(msg), []byte("user bug")) {
		t.Fatalf("error=%v", out["error"])
	}
}
