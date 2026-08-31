package dashapi

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// TestWebhookEndpoints：webhook 触发端点管理 API（FR-3.4）——
// 创建（自动生成/指定 secret）、列表不回显 secret、删除与 404。
func TestWebhookEndpoints(t *testing.T) {
	h, _, _ := newTestHandler(t)

	do := func(method, path, body string) *httptest.ResponseRecorder {
		t.Helper()
		req := httptest.NewRequest(method, path, strings.NewReader(body))
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		return rec
	}

	// 创建：缺省 secret 自动生成；secret 仅在创建响应中完整返回
	rec := do(http.MethodPost, "/api/v1/webhooks", `{"event_name":"stripe/payment"}`)
	if rec.Code != http.StatusOK {
		t.Fatalf("create: %d %s", rec.Code, rec.Body)
	}
	var created struct {
		ID        string `json:"id"`
		URL       string `json:"url"`
		EventName string `json:"event_name"`
		Secret    string `json:"secret"`
	}
	decode(t, rec, &created)
	if !strings.HasPrefix(created.ID, "whk_") || created.URL != "/hooks/"+created.ID ||
		created.EventName != "stripe/payment" || created.Secret == "" {
		t.Fatalf("created: %+v", created)
	}

	// 创建：指定 secret
	rec = do(http.MethodPost, "/api/v1/webhooks", `{"event_name":"github/push","secret":"my-secret"}`)
	if rec.Code != http.StatusOK {
		t.Fatalf("create with secret: %d %s", rec.Code, rec.Body)
	}
	var created2 struct {
		ID     string `json:"id"`
		Secret string `json:"secret"`
	}
	decode(t, rec, &created2)
	if created2.Secret != "my-secret" {
		t.Fatalf("custom secret: %+v", created2)
	}

	// 缺 event_name → 400
	if rec := do(http.MethodPost, "/api/v1/webhooks", `{}`); rec.Code != http.StatusBadRequest {
		t.Fatalf("create without event_name: %d", rec.Code)
	}

	// 列表：两条，不回显 secret
	rec = do(http.MethodGet, "/api/v1/webhooks", "")
	if rec.Code != http.StatusOK {
		t.Fatalf("list: %d", rec.Code)
	}
	var list struct {
		Webhooks []map[string]any `json:"webhooks"`
	}
	decode(t, rec, &list)
	if len(list.Webhooks) != 2 {
		t.Fatalf("list: %+v", list.Webhooks)
	}
	for _, wh := range list.Webhooks {
		if _, ok := wh["secret"]; ok {
			t.Fatalf("list must not echo secret: %+v", wh)
		}
		if wh["id"] == nil || wh["url"] == nil || wh["event_name"] == nil || wh["created_at"] == nil {
			t.Fatalf("list entry missing fields: %+v", wh)
		}
	}

	// 删除：存在 → 200 {}；不存在 → 404
	if rec := do(http.MethodDelete, "/api/v1/webhooks/"+created.ID, ""); rec.Code != http.StatusOK {
		t.Fatalf("delete: %d %s", rec.Code, rec.Body)
	}
	if rec := do(http.MethodDelete, "/api/v1/webhooks/"+created.ID, ""); rec.Code != http.StatusNotFound {
		t.Fatalf("re-delete: %d", rec.Code)
	}
	if rec := do(http.MethodDelete, "/api/v1/webhooks/whk_none", ""); rec.Code != http.StatusNotFound {
		t.Fatalf("delete missing: %d", rec.Code)
	}

	// 方法不允许：PUT → 405
	if rec := do(http.MethodPut, "/api/v1/webhooks", `{}`); rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("put: %d", rec.Code)
	}
}
