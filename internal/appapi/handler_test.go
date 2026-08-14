package appapi

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/triggerlink/triggerlink/internal/registry"
	"github.com/triggerlink/triggerlink/internal/sign"
)

// fakeApp 模拟一个 SDK 应用 serve 端点：验签后返回给定的函数清单。
func fakeApp(t *testing.T, fns []map[string]any) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := sign.Verify("sk", r.Header.Get("X-TriggerLink-Signature"), nil, time.Now(), 5*time.Minute); err != nil {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		json.NewEncoder(w).Encode(map[string]any{"sdk": "test", "app_id": "a", "functions": fns})
	}))
	t.Cleanup(srv.Close)
	return srv
}

func doReq(t *testing.T, h http.Handler, method, path, body, key string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(method, path, bytes.NewBufferString(body))
	if key != "" {
		req.Header.Set("Authorization", "Bearer "+key)
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

func TestRegisterListSync(t *testing.T) {
	reg := registry.New()
	h := NewHandler(reg, "ek", "sk", nil)

	// 未鉴权 → 401
	if rec := doReq(t, h, "GET", "/api/v1/apps", "", "wrong"); rec.Code != http.StatusUnauthorized {
		t.Fatalf("auth: %d", rec.Code)
	}

	app := fakeApp(t, []map[string]any{{"id": "f1", "event": "x/y", "retries": 4}})

	// 注册新 app → 201，函数进注册表
	rec := doReq(t, h, "POST", "/api/v1/apps", `{"url":"`+app.URL+`"}`, "ek")
	if rec.Code != http.StatusCreated {
		t.Fatalf("register: %d body=%s", rec.Code, rec.Body)
	}
	if _, ok := reg.Lookup("f1"); !ok {
		t.Fatal("f1 not registered")
	}

	// 列表 → 含该 app
	rec = doReq(t, h, "GET", "/api/v1/apps", "", "ek")
	if rec.Code != http.StatusOK {
		t.Fatalf("list: %d", rec.Code)
	}
	var list struct {
		Apps []struct {
			URL       string `json:"url"`
			Functions []struct {
				ID string `json:"id"`
			} `json:"functions"`
		} `json:"apps"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &list); err != nil || len(list.Apps) != 1 ||
		list.Apps[0].URL != app.URL || len(list.Apps[0].Functions) != 1 {
		t.Fatalf("list=%s err=%v", rec.Body, err)
	}

	// 重复 POST 同 URL → 200（视为同步）
	rec = doReq(t, h, "POST", "/api/v1/apps", `{"url":"`+app.URL+`"}`, "ek")
	if rec.Code != http.StatusOK {
		t.Fatalf("re-register: %d", rec.Code)
	}

	// sync 已注册 app → 200；sync 未知 app → 404
	rec = doReq(t, h, "POST", "/api/v1/apps/sync", `{"url":"`+app.URL+`"}`, "ek")
	if rec.Code != http.StatusOK {
		t.Fatalf("sync: %d", rec.Code)
	}
	rec = doReq(t, h, "POST", "/api/v1/apps/sync", `{"url":"http://ghost"}`, "ek")
	if rec.Code != http.StatusNotFound {
		t.Fatalf("sync ghost: %d", rec.Code)
	}

	// 内省失败（URL 不可达）→ 502
	rec = doReq(t, h, "POST", "/api/v1/apps", `{"url":"http://127.0.0.1:1"}`, "ek")
	if rec.Code != http.StatusBadGateway {
		t.Fatalf("bad app: %d", rec.Code)
	}
}

func TestPersistHook(t *testing.T) {
	app := fakeApp(t, []map[string]any{{"id": "f1", "event": "x/y", "retries": 4}})

	// 注册成功 → Persist 被调用且参数正确
	reg := registry.New()
	h := NewHandler(reg, "ek", "sk", nil)
	var gotURL string
	var gotFns []registry.Function
	h.Persist = func(_ context.Context, appURL string, fns []registry.Function) error {
		gotURL, gotFns = appURL, fns
		return nil
	}
	rec := doReq(t, h, "POST", "/api/v1/apps", `{"url":"`+app.URL+`"}`, "ek")
	if rec.Code != http.StatusCreated {
		t.Fatalf("register: %d body=%s", rec.Code, rec.Body)
	}
	if gotURL != app.URL || len(gotFns) != 1 || gotFns[0].ID != "f1" || gotFns[0].Retries != 4 {
		t.Fatalf("persist args: url=%s fns=%+v", gotURL, gotFns)
	}
	if _, ok := reg.Lookup("f1"); !ok {
		t.Fatal("f1 not registered")
	}

	// Persist 失败 → 500，内存注册表不变
	reg2 := registry.New()
	h2 := NewHandler(reg2, "ek", "sk", nil)
	h2.Persist = func(_ context.Context, _ string, _ []registry.Function) error {
		return errors.New("db down")
	}
	rec = doReq(t, h2, "POST", "/api/v1/apps", `{"url":"`+app.URL+`"}`, "ek")
	if rec.Code != http.StatusInternalServerError {
		t.Fatalf("persist fail: %d body=%s", rec.Code, rec.Body)
	}
	if _, ok := reg2.Lookup("f1"); ok {
		t.Fatal("registry mutated despite persist failure")
	}

	// 内省失败（502 路径）→ Persist 不被调用
	reg3 := registry.New()
	h3 := NewHandler(reg3, "ek", "sk", nil)
	called := false
	h3.Persist = func(_ context.Context, _ string, _ []registry.Function) error {
		called = true
		return nil
	}
	rec = doReq(t, h3, "POST", "/api/v1/apps", `{"url":"http://127.0.0.1:1"}`, "ek")
	if rec.Code != http.StatusBadGateway {
		t.Fatalf("bad app: %d", rec.Code)
	}
	if called {
		t.Fatal("persist called on introspect failure")
	}
}
