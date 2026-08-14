package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"path/filepath"
	"testing"

	_ "modernc.org/sqlite"
)

// 模拟 M0 旧库：step_runs 无 updated_at 列；Open 迁移后应可正常写入。
func TestMigrateLegacyStepRuns(t *testing.T) {
	path := filepath.Join(t.TempDir(), "legacy.db")
	raw, err := sql.Open("sqlite", "file:"+path)
	if err != nil {
		t.Fatal(err)
	}
	_, err = raw.Exec(`CREATE TABLE step_runs (
	  run_id TEXT NOT NULL, step_hash TEXT NOT NULL, step_id TEXT NOT NULL,
	  op TEXT NOT NULL, status TEXT NOT NULL, output TEXT, error TEXT,
	  attempt INTEGER NOT NULL DEFAULT 0, PRIMARY KEY (run_id, step_hash))`)
	if err != nil {
		t.Fatal(err)
	}
	raw.Close()

	st, err := Open(path)
	if err != nil {
		t.Fatalf("Open legacy: %v", err)
	}
	defer st.Close()
	err = st.SaveStepResult(context.Background(), StepResult{
		RunID: "run_x", StepHash: "h1", StepID: "parse", Op: "run",
		Status: StepCompleted, Output: json.RawMessage(`{}`), Attempt: 1,
	})
	if err != nil {
		t.Fatalf("SaveStepResult after migrate: %v", err)
	}
	recs, err := st.ListStepRecords(context.Background(), "run_x")
	if err != nil || len(recs) != 1 || recs[0].UpdatedAt == nil {
		t.Fatalf("%+v err=%v", recs, err)
	}
}
