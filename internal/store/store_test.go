package store

import (
	"context"
	"encoding/json"
	"fmt"
	"path/filepath"
	"testing"
	"time"
)

func openTemp(t *testing.T) *SQLiteStore {
	t.Helper()
	st, err := Open(filepath.Join(t.TempDir(), "test.db"))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { st.Close() })
	return st
}

func TestInsertEventDedup(t *testing.T) {
	st := openTemp(t)
	ctx := context.Background()
	e := Event{ID: "evt_1", Name: "doc/uploaded", Data: json.RawMessage(`{"doc_id":"d1"}`), TS: time.Now()}
	inserted, err := st.InsertEvent(ctx, e)
	if err != nil || !inserted {
		t.Fatalf("first insert: inserted=%v err=%v", inserted, err)
	}
	inserted, err = st.InsertEvent(ctx, e)
	if err != nil || inserted {
		t.Fatalf("dup insert: inserted=%v err=%v", inserted, err)
	}
}

func TestUnroutedAndMarkRouted(t *testing.T) {
	st := openTemp(t)
	ctx := context.Background()
	e := Event{ID: "evt_2", Name: "doc/uploaded", Data: json.RawMessage(`{}`), TS: time.Now()}
	if _, err := st.InsertEvent(ctx, e); err != nil {
		t.Fatal(err)
	}
	pending, err := st.UnroutedEvents(ctx)
	if err != nil || len(pending) != 1 || pending[0].ID != "evt_2" {
		t.Fatalf("unrouted: %+v err=%v", pending, err)
	}
	if string(pending[0].Data) != `{}` {
		t.Fatalf("data roundtrip: %s", pending[0].Data)
	}
	if err := st.MarkEventRouted(ctx, "evt_2"); err != nil {
		t.Fatal(err)
	}
	pending, err = st.UnroutedEvents(ctx)
	if err != nil || len(pending) != 0 {
		t.Fatalf("after routed: %+v err=%v", pending, err)
	}
}

// NewTestID 生成时间有序的确定性测试 ID（模拟 ids.NewID 的字符串序）。
func NewTestID(prefix string, seq int) string {
	return fmt.Sprintf("%s_%013d%016x", prefix, seq, seq)
}

func TestAppFunctionsPersistence(t *testing.T) {
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "test.db")

	st, err := Open(dbPath)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}

	// 两个 app 各自注册
	if err := st.ReplaceAppFunctions(ctx, "http://a/serve", []RegisteredFunction{
		{ID: "f1", Event: "x/y", Retries: 4},
		{ID: "f2", Event: "x/y"},
	}); err != nil {
		t.Fatal(err)
	}
	if err := st.ReplaceAppFunctions(ctx, "http://b/serve", []RegisteredFunction{
		{ID: "g1", Event: "x/y"},
	}); err != nil {
		t.Fatal(err)
	}

	// 再同步 a：f1 改名 f1b、f2 删除 → 旧记录不得残留
	if err := st.ReplaceAppFunctions(ctx, "http://a/serve", []RegisteredFunction{
		{ID: "f1b", Event: "x/y"},
	}); err != nil {
		t.Fatal(err)
	}

	// 函数 g1 迁到 app a（应用换地址重新注册）→ b 名下不再出现
	if err := st.ReplaceAppFunctions(ctx, "http://a/serve", []RegisteredFunction{
		{ID: "f1b", Event: "x/y"},
		{ID: "g1", Event: "x/y"},
	}); err != nil {
		t.Fatal(err)
	}
	if err := st.ReplaceAppFunctions(ctx, "http://b/serve", nil); err != nil {
		t.Fatal(err)
	}

	got, err := st.LoadRegisteredFunctions(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 || got[0].ID != "f1b" || got[1].ID != "g1" {
		t.Fatalf("load: %+v", got)
	}
	for _, f := range got {
		if f.AppURL != "http://a/serve" {
			t.Fatalf("app_url: %+v", f)
		}
	}

	// 关闭重开（模拟平台重启）→ 记录仍在
	st.Close()
	st2, err := Open(dbPath)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	t.Cleanup(func() { st2.Close() })
	got, err = st2.LoadRegisteredFunctions(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 {
		t.Fatalf("after reopen: %+v", got)
	}
}
