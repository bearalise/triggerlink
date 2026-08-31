package webhook

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/bearalise/triggerlink/internal/sign"
	"github.com/bearalise/triggerlink/internal/store"
)

// newTestStack 组装真实 SQLite + handler + 有缓冲 stream。
func newTestStack(t *testing.T) (*Handler, *store.SQLiteStore, chan store.Event) {
	t.Helper()
	st, err := store.Open(filepath.Join(t.TempDir(), "test.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() })
	stream := make(chan store.Event, 8)
	return NewHandler(st, stream), st, stream
}

// seedWebhook 落库一个 webhook 并返回其 secret。
func seedWebhook(t *testing.T, st *store.SQLiteStore, id, eventName, secret string) {
	t.Helper()
	if err := st.CreateWebhook(context.Background(), store.Webhook{
		ID: id, EventName: eventName, Secret: secret, CreatedAt: time.Now(),
	}); err != nil {
		t.Fatal(err)
	}
}

// post 以给定签名头 POST body；signHeader 为 nil 时按 secret 正确签名。
func post(t *testing.T, h *Handler, path, body, secret string, at time.Time) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, path, strings.NewReader(body))
	req.Header.Set("X-TriggerLink-Signature", sign.Sign(secret, []byte(body), at))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

// TestValidSignature：验签通过 → 事件落库 + 入 stream + 200 {"id": "evt_..."}。
func TestValidSignature(t *testing.T) {
	h, st, stream := newTestStack(t)
	seedWebhook(t, st, "whk_a", "stripe/payment", "s3cret")

	body := `{"amount":100,"currency":"usd"}`
	rec := post(t, h, "/hooks/whk_a", body, "s3cret", time.Now())
	if rec.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", rec.Code, rec.Body)
	}
	var out struct {
		ID string `json:"id"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &out); err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(out.ID, "evt_") {
		t.Fatalf("event id: %q", out.ID)
	}

	// stream 收到同一条事件
	select {
	case e := <-stream:
		if e.ID != out.ID || e.Name != "stripe/payment" || string(e.Data) != body {
			t.Fatalf("stream event: %+v", e)
		}
	default:
		t.Fatal("event not pushed to stream")
	}
	// 已落库（先 WAL 后处理：200 返回时 InsertEvent 已成功）
	events, err := st.ListEvents(context.Background(), store.ListEventsOptions{})
	if err != nil || len(events) != 1 || events[0].ID != out.ID {
		t.Fatalf("events: %+v err=%v", events, err)
	}
}

// TestBadSignature：签名错误 / 时间戳过期 → 401，不落库不入 stream。
func TestBadSignature(t *testing.T) {
	h, st, stream := newTestStack(t)
	seedWebhook(t, st, "whk_a", "stripe/payment", "s3cret")
	body := `{"amount":100}`

	// 错 secret
	if rec := post(t, h, "/hooks/whk_a", body, "wrong", time.Now()); rec.Code != http.StatusUnauthorized {
		t.Fatalf("wrong secret: %d", rec.Code)
	}
	// 无签名头
	req := httptest.NewRequest(http.MethodPost, "/hooks/whk_a", strings.NewReader(body))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("no signature: %d", rec.Code)
	}
	// 时间戳超出 ±5min 窗口（防重放）
	if rec := post(t, h, "/hooks/whk_a", body, "s3cret", time.Now().Add(-10*time.Minute)); rec.Code != http.StatusUnauthorized {
		t.Fatalf("stale timestamp: %d", rec.Code)
	}

	if n, _ := st.CountRuns(context.Background()); n != 0 {
		t.Fatal("unexpected side effect")
	}
	events, _ := st.ListEvents(context.Background(), store.ListEventsOptions{})
	if len(events) != 0 {
		t.Fatalf("events after 401: %+v", events)
	}
	select {
	case e := <-stream:
		t.Fatalf("event pushed on bad signature: %+v", e)
	default:
	}
}

// TestUnknownWebhook：未知 id → 404（先查库再验签，不泄露签名错误信息）。
func TestUnknownWebhook(t *testing.T) {
	h, _, _ := newTestStack(t)
	rec := post(t, h, "/hooks/whk_none", `{}`, "whatever", time.Now())
	if rec.Code != http.StatusNotFound {
		t.Fatalf("status=%d", rec.Code)
	}
}

// TestNonJSONBody：完全非 JSON → 400。
func TestNonJSONBody(t *testing.T) {
	h, st, _ := newTestStack(t)
	seedWebhook(t, st, "whk_a", "stripe/payment", "s3cret")
	rec := post(t, h, "/hooks/whk_a", `not json at all`, "s3cret", time.Now())
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status=%d body=%s", rec.Code, rec.Body)
	}
	events, _ := st.ListEvents(context.Background(), store.ListEventsOptions{})
	if len(events) != 0 {
		t.Fatalf("events after 400: %+v", events)
	}
}

// TestNonObjectWrapped：合法 JSON 但非对象（数组/标量）→ 包成 {"payload": ...} 作为事件 data。
func TestNonObjectWrapped(t *testing.T) {
	h, st, stream := newTestStack(t)
	seedWebhook(t, st, "whk_a", "stripe/payment", "s3cret")

	for _, body := range []string{`[1,2,3]`, `"hello"`, `42`} {
		rec := post(t, h, "/hooks/whk_a", body, "s3cret", time.Now())
		if rec.Code != http.StatusOK {
			t.Fatalf("body %s: status=%d", body, rec.Code)
		}
		e := <-stream
		var data struct {
			Payload json.RawMessage `json:"payload"`
		}
		if err := json.Unmarshal(e.Data, &data); err != nil {
			t.Fatalf("body %s: data=%s err=%v", body, e.Data, err)
		}
		if string(data.Payload) != body {
			t.Fatalf("body %s: payload=%s", body, data.Payload)
		}
	}
}

// TestMethodNotAllowed：非 POST → 405。
func TestMethodNotAllowed(t *testing.T) {
	h, st, _ := newTestStack(t)
	seedWebhook(t, st, "whk_a", "stripe/payment", "s3cret")
	req := httptest.NewRequest(http.MethodGet, "/hooks/whk_a", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("status=%d", rec.Code)
	}
}
