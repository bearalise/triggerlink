package registry

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/triggerlink/triggerlink/internal/sign"
)

func TestMatchAndLookup(t *testing.T) {
	r := New()
	r.Register(Function{ID: "process-doc", Event: "doc/uploaded", AppURL: "http://app/serve"})
	r.Register(Function{ID: "audit", Event: "doc/uploaded", AppURL: "http://app2/serve"})
	r.Register(Function{ID: "other", Event: "user/signup", AppURL: "http://app3/serve"})

	fns := r.Match("doc/uploaded")
	if len(fns) != 2 { // fan-out：一个事件命中两个函数
		t.Fatalf("match=%v", fns)
	}
	if f, ok := r.Lookup("audit"); !ok || f.AppURL != "http://app2/serve" {
		t.Fatalf("lookup=%+v ok=%v", f, ok)
	}
	if _, ok := r.Lookup("nope"); ok {
		t.Fatal("unexpected hit")
	}
	if len(r.Match("nope")) != 0 {
		t.Fatal("unexpected match")
	}
}

func TestFetch(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := sign.Verify("k", r.Header.Get("X-TriggerLink-Signature"), nil, time.Now(), 5*time.Minute); err != nil {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		json.NewEncoder(w).Encode(map[string]any{
			"sdk":    "triggerlink-go/0.0.0-m0",
			"app_id": "docsvc",
			"functions": []map[string]any{
				{"id": "process-doc", "event": "doc/uploaded", "retries": 4},
			},
		})
	}))
	defer srv.Close()

	fns, err := Fetch(context.Background(), srv.Client(), srv.URL, "k")
	if err != nil || len(fns) != 1 {
		t.Fatalf("fetch=%v err=%v", fns, err)
	}
	if fns[0].ID != "process-doc" || fns[0].AppURL != srv.URL {
		t.Fatalf("function=%+v", fns[0])
	}

	if _, err := Fetch(context.Background(), srv.Client(), srv.URL, "wrong-key"); err == nil {
		t.Fatal("expected error on 401")
	}
}

func TestSyncReplacesAppFunctions(t *testing.T) {
	r := New()
	r.Sync("http://a/serve", []Function{{ID: "f1", Event: "x/y"}, {ID: "f2", Event: "x/y"}})
	r.Sync("http://b/serve", []Function{{ID: "g1", Event: "x/y"}})

	// 再同步 a：f1 改名 f1b、f2 删除 → 旧函数不得残留
	r.Sync("http://a/serve", []Function{{ID: "f1b", Event: "x/y"}})

	if _, ok := r.Lookup("f1"); ok {
		t.Fatal("stale f1 still registered")
	}
	if _, ok := r.Lookup("f2"); ok {
		t.Fatal("stale f2 still registered")
	}
	if f, ok := r.Lookup("f1b"); !ok || f.AppURL != "http://a/serve" {
		t.Fatalf("f1b=%+v ok=%v", f, ok)
	}
	if got := len(r.Match("x/y")); got != 2 { // f1b + g1
		t.Fatalf("match=%d, want 2", got)
	}

	apps := r.Apps()
	if len(apps) != 2 || apps[0] != "http://a/serve" || apps[1] != "http://b/serve" {
		t.Fatalf("apps=%v", apps)
	}
	if got := len(r.FunctionsOf("http://a/serve")); got != 1 {
		t.Fatalf("FunctionsOf=%d, want 1", got)
	}
	if got := len(r.FunctionsOf("http://ghost")); got != 0 {
		t.Fatalf("FunctionsOf ghost=%d, want 0", got)
	}
}

// 应用换地址（端口/域名变化）重新注册同一函数 ID：旧 URL 的订阅条目必须清除，
// 否则 byEvent 残留会让一个事件 fan-out 出重复 run。
func TestSyncMigratesFunctionAcrossURLs(t *testing.T) {
	r := New()
	r.Sync("http://a/serve", []Function{{ID: "f1", Event: "x/y"}})
	r.Sync("http://b/serve", []Function{{ID: "f1", Event: "x/y"}})

	if got := len(r.Match("x/y")); got != 1 {
		t.Fatalf("match=%d, want 1（旧 URL 条目残留会导致重复路由）", got)
	}
	if f, ok := r.Lookup("f1"); !ok || f.AppURL != "http://b/serve" {
		t.Fatalf("f1=%+v ok=%v", f, ok)
	}
	if apps := r.Apps(); len(apps) != 1 || apps[0] != "http://b/serve" {
		t.Fatalf("apps=%v", apps)
	}
}
