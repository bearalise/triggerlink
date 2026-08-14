package triggerlink

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
	"testing"
	"time"

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
