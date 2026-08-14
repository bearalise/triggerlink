package dashboard

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestSPAFallback(t *testing.T) {
	h := Handler()
	for _, path := range []string{"/dashboard", "/dashboard/", "/dashboard/runs/run_123"} {
		req := httptest.NewRequest(http.MethodGet, path, nil)
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		if rec.Code != http.StatusOK {
			t.Fatalf("%s: %d", path, rec.Code)
		}
		if ct := rec.Header().Get("Content-Type"); ct != "text/html; charset=utf-8" {
			t.Fatalf("%s: content-type %s", path, ct)
		}
	}
}
