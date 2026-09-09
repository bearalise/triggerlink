package e2e

import (
	"database/sql"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"

	"github.com/bearalise/triggerlink/internal/store"
)

// openStore 给一个 e2e 用例一套干净的存储。后端由环境变量选择：
//
//	go test ./e2e/                                     # SQLite（默认）
//	TEST_POSTGRES_URI=postgres://... go test ./e2e/    # 整套 e2e 跑 Postgres
//
// 平台代码只认 store.Store 接口，两个后端共用同一套查询实现，因此 e2e 不必分叉。
func openStore(t *testing.T) *store.SQLStore {
	t.Helper()
	if uri := os.Getenv("TEST_POSTGRES_URI"); uri != "" {
		return openPostgresSchema(t, uri)
	}
	st, err := store.Open(filepath.Join(t.TempDir(), "test.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() })
	return st
}

// openPostgresSchema 为每个用例建独立 schema，结束整体 DROP（用例之间互不干扰）。
func openPostgresSchema(t *testing.T, uri string) *store.SQLStore {
	t.Helper()
	admin, err := sql.Open("pgx", uri)
	if err != nil {
		t.Fatalf("open admin conn: %v", err)
	}
	schema := fmt.Sprintf("tl_e2e_%d_%d", time.Now().UnixNano(), rand.Int31())
	if _, err := admin.Exec(`CREATE SCHEMA ` + schema); err != nil {
		admin.Close()
		t.Fatalf("create schema: %v", err)
	}
	sep := "?"
	if strings.Contains(uri, "?") {
		sep = "&"
	}
	st, err := store.Open(uri + sep + "search_path=" + schema)
	if err != nil {
		admin.Exec(`DROP SCHEMA ` + schema + ` CASCADE`)
		admin.Close()
		t.Fatalf("open postgres: %v", err)
	}
	t.Cleanup(func() {
		st.Close()
		admin.Exec(`DROP SCHEMA ` + schema + ` CASCADE`)
		admin.Close()
	})
	return st
}

// 后端可切换的证明：同一条 happy path 在当前后端上跑通，且 store 报告的后端名符合预期。
func TestBackendSmoke(t *testing.T) {
	st := openStore(t)
	want := "sqlite"
	if os.Getenv("TEST_POSTGRES_URI") != "" {
		want = "postgres"
	}
	if got := st.Backend(); got != want {
		t.Fatalf("backend=%s, want %s", got, want)
	}
}
