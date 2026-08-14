package eventapi

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"

	"github.com/triggerlink/triggerlink/internal/store"
)

func setup(t *testing.T) (*Handler, store.Store, chan store.Event) {
	t.Helper()
	st, err := store.Open(filepath.Join(t.TempDir(), "test.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() })
	stream := make(chan store.Event, 16)
	return NewHandler(st, "dev-key", stream), st, stream
}

func post(t *testing.T, h *Handler, key, body string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, "/v1/events", bytes.NewBufferString(body))
	if key != "" {
		req.Header.Set("Authorization", "Bearer "+key)
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

func TestRejectsBadKey(t *testing.T) {
	h, _, _ := setup(t)
	if rec := post(t, h, "wrong", `{"name":"a","data":{}}`); rec.Code != http.StatusUnauthorized {
		t.Fatalf("code=%d", rec.Code)
	}
	if rec := post(t, h, "", `{"name":"a","data":{}}`); rec.Code != http.StatusUnauthorized {
		t.Fatalf("code=%d", rec.Code)
	}
}

func TestAcceptsSingleAndBatch(t *testing.T) {
	h, _, stream := setup(t)

	rec := post(t, h, "dev-key", `{"name":"doc/uploaded","data":{"doc_id":"d1"}}`)
	if rec.Code != http.StatusOK {
		t.Fatalf("code=%d body=%s", rec.Code, rec.Body)
	}
	var resp struct {
		IDs []string `json:"ids"`
	}
	json.NewDecoder(rec.Body).Decode(&resp)
	if len(resp.IDs) != 1 || resp.IDs[0] == "" {
		t.Fatalf("ids=%v", resp.IDs)
	}
	e := <-stream // 落库且入 stream
	if e.Name != "doc/uploaded" || e.ID != resp.IDs[0] {
		t.Fatalf("event=%+v", e)
	}

	rec = post(t, h, "dev-key", `[{"name":"a","data":{}},{"name":"b","data":{}}]`)
	json.NewDecoder(rec.Body).Decode(&resp)
	if len(resp.IDs) != 2 {
		t.Fatalf("batch ids=%v", resp.IDs)
	}
	<-stream
	<-stream
}

func TestDedupDoesNotRestream(t *testing.T) {
	h, _, stream := setup(t)
	body := `{"id":"fixed-id","name":"a","data":{}}`
	post(t, h, "dev-key", body)
	<-stream
	rec := post(t, h, "dev-key", body) // 重复 event id
	if rec.Code != http.StatusOK {
		t.Fatalf("dedup should still 200, got %d", rec.Code)
	}
	select {
	case e := <-stream:
		t.Fatalf("duplicate event re-streamed: %+v", e)
	default:
	}
}

func TestRejectsInvalid(t *testing.T) {
	h, _, _ := setup(t)
	if rec := post(t, h, "dev-key", `{"data":{}}`); rec.Code != http.StatusBadRequest {
		t.Fatalf("missing name: code=%d", rec.Code)
	}
	if rec := post(t, h, "dev-key", `not-json`); rec.Code != http.StatusBadRequest {
		t.Fatalf("bad json: code=%d", rec.Code)
	}
	if rec := post(t, h, "dev-key", `[]`); rec.Code != http.StatusBadRequest {
		t.Fatalf("empty batch: code=%d", rec.Code)
	}
	req := httptest.NewRequest(http.MethodGet, "/v1/events", nil)
	req.Header.Set("Authorization", "Bearer dev-key")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("GET: code=%d", rec.Code)
	}
}
