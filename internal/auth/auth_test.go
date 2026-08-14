package auth

import (
	"net/http"
	"net/http/httptest"
	"testing"
)

func okHandler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusOK) })
}

func TestParseCreds(t *testing.T) {
	u, p, err := ParseCreds("admin:s3cret")
	if err != nil || u != "admin" || p != "s3cret" {
		t.Fatalf("%q %q %v", u, p, err)
	}
	for _, bad := range []string{"", "nocolon", ":pass", "user:"} {
		if _, _, err := ParseCreds(bad); err == nil {
			t.Fatalf("should reject %q", bad)
		}
	}
}

func TestBasic(t *testing.T) {
	h := Basic("admin", "pw", okHandler())

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("no creds: %d", rec.Code)
	}
	if rec.Header().Get("WWW-Authenticate") != `Basic realm="triggerlink"` {
		t.Fatalf("missing challenge: %v", rec.Header())
	}

	req = httptest.NewRequest(http.MethodGet, "/", nil)
	req.SetBasicAuth("admin", "wrong")
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusUnauthorized {
		t.Fatalf("wrong pass: %d", rec.Code)
	}

	req = httptest.NewRequest(http.MethodGet, "/", nil)
	req.SetBasicAuth("admin", "pw")
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("ok: %d", rec.Code)
	}
}
