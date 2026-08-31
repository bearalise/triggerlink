// 建表 DDL 与迁移。两个后端共用同一套逻辑 schema：列名、约束、索引完全一致，
// 只有整数宽度与布尔类型按方言取值。
//
// 时间与 JSON 一律存 TEXT（UTC RFC3339Nano 字符串 / 原样 JSON 文本），Postgres 侧
// 不用 TIMESTAMPTZ/JSONB——代价是运维直接写 SQL 查时间不够地道，换来的是查询主体
// 两边共用一份、语义天然一致。若将来需要原生类型，改动集中在这里与读写两侧的
// scan 目标，不影响其余逻辑。
package store

import (
	"context"
	"database/sql"
)

const sqliteSchema = `
CREATE TABLE IF NOT EXISTS events (
  id          TEXT PRIMARY KEY,
  name        TEXT NOT NULL,
  data        TEXT NOT NULL,
  ts          TEXT NOT NULL,
  received_at TEXT NOT NULL,
  routed_at   TEXT
);
CREATE TABLE IF NOT EXISTS runs (
  id          TEXT PRIMARY KEY,
  function_id TEXT NOT NULL,
  status      TEXT NOT NULL,
  event_id    TEXT NOT NULL,
  event_name  TEXT NOT NULL,
  event_data  TEXT NOT NULL,
  event_ts    TEXT NOT NULL,
  output      TEXT,
  error       TEXT,
  attempt     INTEGER NOT NULL DEFAULT 0,
  created_at  TEXT NOT NULL,
  ended_at    TEXT
);
CREATE TABLE IF NOT EXISTS step_runs (
  run_id    TEXT NOT NULL,
  step_hash TEXT NOT NULL,
  step_id   TEXT NOT NULL,
  op        TEXT NOT NULL,
  status    TEXT NOT NULL,
  output    TEXT,
  error     TEXT,
  attempt   INTEGER NOT NULL DEFAULT 0,
  seq       INTEGER NOT NULL DEFAULT 0,
  PRIMARY KEY (run_id, step_hash)
);
CREATE TABLE IF NOT EXISTS queue_items (
  id            TEXT PRIMARY KEY,
  function_id   TEXT NOT NULL,
  run_id        TEXT NOT NULL,
  score         INTEGER NOT NULL,
  at            TEXT NOT NULL,
  status        TEXT NOT NULL,
  lease_expires TEXT,
  created_at    TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_queue_poll ON queue_items(status, at, score);
CREATE TABLE IF NOT EXISTS functions (
  id         TEXT PRIMARY KEY,
  app_url    TEXT NOT NULL,
  event      TEXT NOT NULL,
  retries    INTEGER NOT NULL DEFAULT 0,
  updated_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS waits (
  id         TEXT PRIMARY KEY,
  run_id     TEXT NOT NULL,
  step_hash  TEXT NOT NULL,
  step_id    TEXT NOT NULL,
  event_name TEXT NOT NULL,
  match_expr TEXT NOT NULL DEFAULT '',
  expires_at TEXT,
  status     TEXT NOT NULL,
  created_at TEXT NOT NULL
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_waits_run_step ON waits(run_id, step_hash);
CREATE INDEX IF NOT EXISTS idx_waits_event ON waits(status, event_name);
CREATE TABLE IF NOT EXISTS debounce_pending (
  function_id   TEXT NOT NULL,
  debounce_key  TEXT NOT NULL,
  run_id        TEXT NOT NULL,
  first_at      TEXT NOT NULL,
  trigger_at    TEXT NOT NULL,
  PRIMARY KEY (function_id, debounce_key)
);
CREATE INDEX IF NOT EXISTS idx_debounce_run ON debounce_pending(run_id);
CREATE TABLE IF NOT EXISTS batch_windows (
  function_id TEXT NOT NULL,
  batch_key   TEXT NOT NULL,
  first_at    TEXT NOT NULL,
  PRIMARY KEY (function_id, batch_key)
);
CREATE TABLE IF NOT EXISTS batch_entries (
  function_id TEXT NOT NULL,
  batch_key   TEXT NOT NULL,
  seq         INTEGER NOT NULL,
  event_id    TEXT NOT NULL,
  event_name  TEXT NOT NULL,
  event_data  TEXT NOT NULL,
  event_ts    TEXT NOT NULL,
  added_at    TEXT NOT NULL,
  PRIMARY KEY (function_id, batch_key, seq)
);
CREATE TABLE IF NOT EXISTS throttle_state (
  constraint_key TEXT PRIMARY KEY,
  window_start   TEXT NOT NULL,
  count          INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS paused_functions (
  function_id TEXT PRIMARY KEY,
  paused_at   TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS webhooks (
  id         TEXT PRIMARY KEY,
  event_name TEXT NOT NULL,
  secret     TEXT NOT NULL,
  created_at TEXT NOT NULL
);
`

const postgresSchema = `
CREATE TABLE IF NOT EXISTS events (
  id          TEXT PRIMARY KEY,
  name        TEXT NOT NULL,
  data        TEXT NOT NULL,
  ts          TEXT NOT NULL,
  received_at TEXT NOT NULL,
  routed_at   TEXT
);
CREATE TABLE IF NOT EXISTS runs (
  id          TEXT PRIMARY KEY,
  function_id TEXT NOT NULL,
  status      TEXT NOT NULL,
  event_id    TEXT NOT NULL,
  event_name  TEXT NOT NULL,
  event_data  TEXT NOT NULL,
  event_ts    TEXT NOT NULL,
  output      TEXT,
  error       TEXT,
  attempt     INTEGER NOT NULL DEFAULT 0,
  batch       BOOLEAN NOT NULL DEFAULT FALSE,
  created_at  TEXT NOT NULL,
  ended_at    TEXT
);
CREATE TABLE IF NOT EXISTS step_runs (
  run_id    TEXT NOT NULL,
  step_hash TEXT NOT NULL,
  step_id   TEXT NOT NULL,
  op        TEXT NOT NULL,
  status    TEXT NOT NULL,
  output    TEXT,
  error     TEXT,
  attempt   INTEGER NOT NULL DEFAULT 0,
  seq       INTEGER NOT NULL DEFAULT 0,
  PRIMARY KEY (run_id, step_hash)
);
CREATE TABLE IF NOT EXISTS queue_items (
  id            TEXT PRIMARY KEY,
  function_id   TEXT NOT NULL,
  run_id        TEXT NOT NULL,
  score         BIGINT NOT NULL,
  at            TEXT NOT NULL,
  status        TEXT NOT NULL,
  lease_expires TEXT,
  created_at    TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_queue_poll ON queue_items(status, at, score);
CREATE TABLE IF NOT EXISTS functions (
  id         TEXT PRIMARY KEY,
  app_url    TEXT NOT NULL,
  event      TEXT NOT NULL,
  retries    INTEGER NOT NULL DEFAULT 0,
  updated_at TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS waits (
  id         TEXT PRIMARY KEY,
  run_id     TEXT NOT NULL,
  step_hash  TEXT NOT NULL,
  step_id    TEXT NOT NULL,
  event_name TEXT NOT NULL,
  match_expr TEXT NOT NULL DEFAULT '',
  expires_at TEXT,
  status     TEXT NOT NULL,
  created_at TEXT NOT NULL
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_waits_run_step ON waits(run_id, step_hash);
CREATE INDEX IF NOT EXISTS idx_waits_event ON waits(status, event_name);
CREATE TABLE IF NOT EXISTS debounce_pending (
  function_id   TEXT NOT NULL,
  debounce_key  TEXT NOT NULL,
  run_id        TEXT NOT NULL,
  first_at      TEXT NOT NULL,
  trigger_at    TEXT NOT NULL,
  PRIMARY KEY (function_id, debounce_key)
);
CREATE INDEX IF NOT EXISTS idx_debounce_run ON debounce_pending(run_id);
CREATE TABLE IF NOT EXISTS batch_windows (
  function_id TEXT NOT NULL,
  batch_key   TEXT NOT NULL,
  first_at    TEXT NOT NULL,
  PRIMARY KEY (function_id, batch_key)
);
CREATE TABLE IF NOT EXISTS batch_entries (
  function_id TEXT NOT NULL,
  batch_key   TEXT NOT NULL,
  seq         BIGINT NOT NULL,
  event_id    TEXT NOT NULL,
  event_name  TEXT NOT NULL,
  event_data  TEXT NOT NULL,
  event_ts    TEXT NOT NULL,
  added_at    TEXT NOT NULL,
  PRIMARY KEY (function_id, batch_key, seq)
);
CREATE TABLE IF NOT EXISTS throttle_state (
  constraint_key TEXT PRIMARY KEY,
  window_start   TEXT NOT NULL,
  count          INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS paused_functions (
  function_id TEXT PRIMARY KEY,
  paused_at   TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS webhooks (
  id         TEXT PRIMARY KEY,
  event_name TEXT NOT NULL,
  secret     TEXT NOT NULL,
  created_at TEXT NOT NULL
);
`

// migrate 建表并补齐老库缺的列（可反复执行）。
func migrate(ctx context.Context, db *sql.DB, d dialect) error {
	if _, err := db.ExecContext(ctx, d.schema()); err != nil {
		return err
	}
	// 老库补列（早期版本的 triggerlink.db 无这些列）。Postgres 侧是新库，
	// 这些列已在 DDL 里，ADD COLUMN IF NOT EXISTS 直接落空。
	for _, c := range []struct{ table, column, typ string }{
		{"step_runs", "updated_at", "TEXT"},
		// seq 是 run 内的写入序（step 时间线的排序依据）。老行为 0，回落到按 updated_at 排。
		{"step_runs", "seq", "INTEGER NOT NULL DEFAULT 0"},
		{"functions", "cancel_on", "TEXT"},
		{"functions", "match", "TEXT"},
		{"functions", "cron", "TEXT"},
		{"functions", "timeout", "TEXT"},
		{"functions", "debounce", "TEXT"},
		{"functions", "throttle", "TEXT"},
		{"functions", "batch", "TEXT"},
	} {
		if err := d.ensureColumn(ctx, db, c.table, c.column, c.typ); err != nil {
			return err
		}
	}
	// runs.batch 标记批触发的 run（FR-4.7）：其 event_data 是事件数组而非单个事件的 data。
	return d.ensureColumn(ctx, db, "runs", "batch", d.boolType())
}
