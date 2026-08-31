// Package store 是 TriggerLink 的 SQLite 存储层。所有时间以 UTC RFC3339Nano 字符串落库。
package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	_ "modernc.org/sqlite"
)

// Run 状态机（PRD 第 5 章）。
const (
	RunQueued    = "Queued"
	RunRunning   = "Running"
	RunCompleted = "Completed"
	RunFailed    = "Failed"
	RunCancelled = "Cancelled" // 终态：dashboard/API 主动取消（FR：run 取消）
)

// Step Run 状态。
const (
	StepCompleted = "completed"
	StepFailed    = "failed"
	StepSleeping  = "sleeping" // sleep 挂起中；唤醒 dispatch 时由平台转为 completed
	StepWaiting   = "waiting"  // waitForEvent 挂起中；事件命中/超时后由平台转为 completed
)

// wait 记录状态（step.waitForEvent 挂起的等待项，PRD 8.2 情况4）。
const (
	WaitWaiting   = "waiting"
	WaitResolved  = "resolved"  // 事件命中，step 已完成
	WaitTimeout   = "timeout"   // 超时，step 以 null output 完成
	WaitCancelled = "cancelled" // run 终态化时清扫
)

// 队列项状态（done 直接 DELETE，不落终态行）。
const (
	QueuePending = "pending"
	QueueLeased  = "leased"
)

// Event 是进入系统的不可变事实。
type Event struct {
	ID         string
	Name       string
	Data       json.RawMessage
	TS         time.Time
	ReceivedAt time.Time
}

// Run 是 Function 的一次执行实例。EventData/EventName/EventTS 冗余自触发事件，
// 使保留策略清理 events 表后在途 run 仍可执行（PRD FR-6.6）。
type Run struct {
	ID         string
	FunctionID string
	Status     string
	EventID    string
	EventName  string
	EventData  json.RawMessage
	EventTS    time.Time
	Output     json.RawMessage
	Error      string
	Attempt    int
	CreatedAt  time.Time
	EndedAt    *time.Time
}

// StepResult 是 memo 核心：以 (run_id, step_hash) 为键的 step 执行记录。
type StepResult struct {
	RunID    string
	StepHash string
	StepID   string
	Op       string
	Status   string
	Output   json.RawMessage
	Error    string
	Attempt  int
}

// Wait 是一条 step.waitForEvent 挂起记录。按 (run_id, step_hash) 幂等（唯一索引）。
// ExpiresAt 为 nil 表示无超时；status 流转见 Wait* 常量。
type Wait struct {
	ID        string
	RunID     string
	StepHash  string
	StepID    string
	EventName string
	MatchExpr string
	ExpiresAt *time.Time
	Status    string
	CreatedAt time.Time
}

// RegisteredFunction 是一条持久化的函数注册记录（PRD FR-5.3 的存储形态）。
// 以函数 ID 为主键：函数迁移到新 appURL 时 upsert 覆盖旧行，与 Registry.Sync 语义对齐。
// CancelOn 是 cancelOn 规则（FR-4.9）的 JSON 序列化形态，空串表示无。
// Match / Cron 分别是事件触发过滤（FR-3.1）与 cron 触发（FR-3.2）表达式，空串表示无。
type RegisteredFunction struct {
	ID       string
	AppURL   string
	Event    string
	Match    string
	Cron     string
	Retries  int
	CancelOn string
}

// QueueItem 是队列中的待执行单元；score 决定 FIFO 顺序，at 是"不早于"时间（退避用）。
type QueueItem struct {
	ID         string
	FunctionID string
	RunID      string
	Score      int64
	At         time.Time
	Status     string
}

// Store 是组件间唯一的存储契约。
type Store interface {
	InsertEvent(ctx context.Context, e Event) (bool, error)
	MarkEventRouted(ctx context.Context, id string) error
	UnroutedEvents(ctx context.Context) ([]Event, error)

	CreateRun(ctx context.Context, r Run) (bool, error)
	GetRun(ctx context.Context, id string) (Run, error)
	MarkRunRunning(ctx context.Context, id string) (bool, error)
	CompleteRun(ctx context.Context, id string, output json.RawMessage) error
	FailRun(ctx context.Context, id string, errMsg string) error
	CancelRun(ctx context.Context, id string) (bool, error)
	IncrementRunAttempt(ctx context.Context, id string) (int, error)
	CountRuns(ctx context.Context) (int, error)
	CountRunsWithStatus(ctx context.Context, status string) (int, error)

	StepResults(ctx context.Context, runID string) (map[string]StepResult, error)
	SaveStepResult(ctx context.Context, sr StepResult) error
	CompleteSleepingSteps(ctx context.Context, runID string) error

	CreateWait(ctx context.Context, w Wait) error
	WaitingWaitsForEvent(ctx context.Context, eventName string) ([]Wait, error)
	ExpiredWaits(ctx context.Context, now time.Time) ([]Wait, error)
	SetWaitStatus(ctx context.Context, id, status string) error
	CancelWaitsForRun(ctx context.Context, runID string) (int, error)
	ActiveRunsByFunction(ctx context.Context, functionID string) ([]Run, error)

	ListRuns(ctx context.Context, o ListRunsOptions) ([]Run, error)
	ListEvents(ctx context.Context, o ListEventsOptions) ([]Event, error)
	RunIDsByEventIDs(ctx context.Context, eventIDs []string) (map[string][]string, error)
	ListStepRecords(ctx context.Context, runID string) ([]StepRecord, error)
	FunctionStats24h(ctx context.Context, since time.Time) (map[string]FunctionStat, error)

	Enqueue(ctx context.Context, item QueueItem) error
	LeaseBatch(ctx context.Context, now time.Time, leaseDur time.Duration, limit int) ([]QueueItem, error)
	ReleaseLease(ctx context.Context, id string, retryAt time.Time) error
	CompleteQueueItem(ctx context.Context, id string) error
	RequeueExpiredLeases(ctx context.Context, now time.Time) (int, error)

	ReplaceAppFunctions(ctx context.Context, appURL string, fns []RegisteredFunction) error
	LoadRegisteredFunctions(ctx context.Context) ([]RegisteredFunction, error)
}

// SQLiteStore 是 Store 的 SQLite 实现。
type SQLiteStore struct {
	db *sql.DB
}

// Open 打开（必要时创建）数据库并执行迁移。单连接：SQLite 单写者，避免 SQLITE_BUSY 抖动。
func Open(path string) (*SQLiteStore, error) {
	dsn := fmt.Sprintf("file:%s?_pragma=busy_timeout(5000)&_pragma=journal_mode(WAL)", path)
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, fmt.Errorf("open sqlite: %w", err)
	}
	db.SetMaxOpenConns(1)
	if err := migrate(db); err != nil {
		db.Close()
		return nil, fmt.Errorf("migrate: %w", err)
	}
	return &SQLiteStore{db: db}, nil
}

// Close 关闭底层数据库。
func (s *SQLiteStore) Close() error { return s.db.Close() }

func migrate(db *sql.DB) error {
	_, err := db.Exec(`
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
`)
	if err != nil {
		return err
	}
	// 老库补列（已有 triggerlink.db 无这些列）
	if err := ensureColumn(db, "step_runs", "updated_at", "TEXT"); err != nil {
		return err
	}
	if err := ensureColumn(db, "functions", "cancel_on", "TEXT"); err != nil {
		return err
	}
	if err := ensureColumn(db, "functions", "match", "TEXT"); err != nil {
		return err
	}
	return ensureColumn(db, "functions", "cron", "TEXT")
}

// ensureColumn 给已存在的表补列（SQLite 无 ADD COLUMN IF NOT EXISTS，先查 PRAGMA）。
func ensureColumn(db *sql.DB, table, column, typ string) error {
	rows, err := db.Query(`PRAGMA table_info(` + table + `)`)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var cid, notnull, pk int
		var name, ctype string
		var dflt sql.NullString
		if err := rows.Scan(&cid, &name, &ctype, &notnull, &dflt, &pk); err != nil {
			return err
		}
		if name == column {
			return nil
		}
	}
	if err := rows.Err(); err != nil {
		return err
	}
	_, err = db.Exec(`ALTER TABLE ` + table + ` ADD COLUMN ` + column + ` ` + typ)
	return err
}

func fmtTime(t time.Time) string { return t.UTC().Format(time.RFC3339Nano) }

func parseTime(s string) (time.Time, error) { return time.Parse(time.RFC3339Nano, s) }

// InsertEvent 落库事件；相同 id 已存在时返回 inserted=false（FR-1.4 去重）。
func (s *SQLiteStore) InsertEvent(ctx context.Context, e Event) (bool, error) {
	res, err := s.db.ExecContext(ctx,
		`INSERT OR IGNORE INTO events (id, name, data, ts, received_at) VALUES (?,?,?,?,?)`,
		e.ID, e.Name, string(e.Data), fmtTime(e.TS), fmtTime(time.Now()))
	if err != nil {
		return false, err
	}
	n, err := res.RowsAffected()
	return n == 1, err
}

// MarkEventRouted 标记事件已被 Runner 路由（对账扫描的判据，PRD 7.2）。
func (s *SQLiteStore) MarkEventRouted(ctx context.Context, id string) error {
	_, err := s.db.ExecContext(ctx, `UPDATE events SET routed_at=? WHERE id=?`, fmtTime(time.Now()), id)
	return err
}

// UnroutedEvents 返回已落库但未路由的事件（启动对账用），按接收时间升序。
func (s *SQLiteStore) UnroutedEvents(ctx context.Context) ([]Event, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT id, name, data, ts, received_at FROM events WHERE routed_at IS NULL ORDER BY received_at`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Event
	for rows.Next() {
		var e Event
		var data, ts, ra string
		if err := rows.Scan(&e.ID, &e.Name, &data, &ts, &ra); err != nil {
			return nil, err
		}
		e.Data = json.RawMessage(data)
		if e.TS, err = parseTime(ts); err != nil {
			return nil, err
		}
		if e.ReceivedAt, err = parseTime(ra); err != nil {
			return nil, err
		}
		out = append(out, e)
	}
	return out, rows.Err()
}

// CreateRun 写入 run 初始状态（Queued）。幂等：同 ID 已存在时不覆盖，
// 返回 created=false——路由的"对账扫描与 stream 消费重叠"双路由竞态靠它兜底（调用方不得再入队）。
func (s *SQLiteStore) CreateRun(ctx context.Context, r Run) (bool, error) {
	res, err := s.db.ExecContext(ctx,
		`INSERT OR IGNORE INTO runs (id, function_id, status, event_id, event_name, event_data, event_ts, created_at)
		 VALUES (?,?,?,?,?,?,?,?)`,
		r.ID, r.FunctionID, RunQueued, r.EventID, r.EventName, string(r.EventData), fmtTime(r.EventTS), fmtTime(time.Now()))
	if err != nil {
		return false, err
	}
	n, err := res.RowsAffected()
	return n > 0, err
}

// GetRun 读取单个 run。
func (s *SQLiteStore) GetRun(ctx context.Context, id string) (Run, error) {
	var r Run
	var edata, ets, cat string
	var output, errStr, eat sql.NullString
	err := s.db.QueryRowContext(ctx,
		`SELECT id, function_id, status, event_id, event_name, event_data, event_ts, output, error, attempt, created_at, ended_at
		 FROM runs WHERE id=?`, id).
		Scan(&r.ID, &r.FunctionID, &r.Status, &r.EventID, &r.EventName, &edata, &ets, &output, &errStr, &r.Attempt, &cat, &eat)
	if err != nil {
		return Run{}, err
	}
	r.EventData = json.RawMessage(edata)
	if r.EventTS, err = parseTime(ets); err != nil {
		return Run{}, err
	}
	if r.CreatedAt, err = parseTime(cat); err != nil {
		return Run{}, err
	}
	if output.Valid {
		r.Output = json.RawMessage(output.String)
	}
	r.Error = errStr.String
	if eat.Valid {
		t, err := parseTime(eat.String)
		if err != nil {
			return Run{}, err
		}
		r.EndedAt = &t
	}
	return r, nil
}

// MarkRunRunning 将非终态 run 置为 Running；返回 false 表示 run 已处终态
// （如回调启动前被取消），调用方应放弃本次 dispatch。
func (s *SQLiteStore) MarkRunRunning(ctx context.Context, id string) (bool, error) {
	res, err := s.db.ExecContext(ctx,
		`UPDATE runs SET status=? WHERE id=? AND status NOT IN (?,?,?)`,
		RunRunning, id, RunCompleted, RunFailed, RunCancelled)
	if err != nil {
		return false, err
	}
	n, err := res.RowsAffected()
	return n == 1, err
}

// CompleteRun 落终态 Completed + 输出。
func (s *SQLiteStore) CompleteRun(ctx context.Context, id string, output json.RawMessage) error {
	_, err := s.db.ExecContext(ctx,
		`UPDATE runs SET status=?, output=?, ended_at=? WHERE id=?`,
		RunCompleted, string(output), fmtTime(time.Now()), id)
	return err
}

// FailRun 落终态 Failed + 错误信息。
func (s *SQLiteStore) FailRun(ctx context.Context, id string, errMsg string) error {
	_, err := s.db.ExecContext(ctx,
		`UPDATE runs SET status=?, error=?, ended_at=? WHERE id=?`,
		RunFailed, errMsg, fmtTime(time.Now()), id)
	return err
}

// CancelRun 把 Queued/Running 的 run 置为终态 Cancelled，并同事务删除其 pending 队列项
// （防止 executor 再次拉起）。返回 transitioned=false 表示 run 已处终态（幂等）。
// 注意：可能有一个 leased 项正在回调途中，由 executor 在回调返回后重检 run 状态兜底。
func (s *SQLiteStore) CancelRun(ctx context.Context, id string) (bool, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return false, err
	}
	defer tx.Rollback()
	res, err := tx.ExecContext(ctx,
		`UPDATE runs SET status=?, ended_at=? WHERE id=? AND status IN (?,?)`,
		RunCancelled, fmtTime(time.Now()), id, RunQueued, RunRunning)
	if err != nil {
		return false, err
	}
	n, err := res.RowsAffected()
	if err != nil {
		return false, err
	}
	if n == 1 {
		if _, err := tx.ExecContext(ctx,
			`DELETE FROM queue_items WHERE run_id=? AND status=?`, id, QueuePending); err != nil {
			return false, err
		}
	}
	return n == 1, tx.Commit()
}

// IncrementRunAttempt 尝试计数 +1 并返回新值（M0：step 与 run 重试共用该计数）。
func (s *SQLiteStore) IncrementRunAttempt(ctx context.Context, id string) (int, error) {
	if _, err := s.db.ExecContext(ctx, `UPDATE runs SET attempt=attempt+1 WHERE id=?`, id); err != nil {
		return 0, err
	}
	var n int
	err := s.db.QueryRowContext(ctx, `SELECT attempt FROM runs WHERE id=?`, id).Scan(&n)
	return n, err
}

// CountRuns 返回 run 总数（e2e 断言用）。
func (s *SQLiteStore) CountRuns(ctx context.Context) (int, error) {
	var n int
	err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM runs`).Scan(&n)
	return n, err
}

// CountRunsWithStatus 统计指定状态的 run 数（e2e 断言用）。
func (s *SQLiteStore) CountRunsWithStatus(ctx context.Context, status string) (int, error) {
	var n int
	err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM runs WHERE status=?`, status).Scan(&n)
	return n, err
}

// StepResults 返回一个 run 的全部 step memo，键为 step_hash。
func (s *SQLiteStore) StepResults(ctx context.Context, runID string) (map[string]StepResult, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT step_hash, step_id, op, status, output, error, attempt FROM step_runs WHERE run_id=?`, runID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[string]StepResult{}
	for rows.Next() {
		var sr StepResult
		var output, errStr sql.NullString
		if err := rows.Scan(&sr.StepHash, &sr.StepID, &sr.Op, &sr.Status, &output, &errStr, &sr.Attempt); err != nil {
			return nil, err
		}
		sr.RunID = runID
		if output.Valid {
			sr.Output = json.RawMessage(output.String)
		}
		sr.Error = errStr.String
		out[sr.StepHash] = sr
	}
	return out, rows.Err()
}

// SaveStepResult 写入或更新一条 step memo；冲突时 attempt+1（重试语义）。updated_at 恒为写入时刻。
func (s *SQLiteStore) SaveStepResult(ctx context.Context, sr StepResult) error {
	_, err := s.db.ExecContext(ctx,
		`INSERT INTO step_runs (run_id, step_hash, step_id, op, status, output, error, attempt, updated_at)
		 VALUES (?,?,?,?,?,?,?,?,?)
		 ON CONFLICT(run_id, step_hash) DO UPDATE SET
		   status=excluded.status, output=excluded.output, error=excluded.error,
		   attempt=step_runs.attempt+1, updated_at=excluded.updated_at`,
		sr.RunID, sr.StepHash, sr.StepID, sr.Op, sr.Status,
		sql.NullString{String: string(sr.Output), Valid: sr.Output != nil},
		sql.NullString{String: sr.Error, Valid: sr.Error != ""}, sr.Attempt,
		fmtTime(time.Now()))
	return err
}

// CompleteSleepingSteps 把一个 run 的 sleeping step memo 置为 completed（sleep 唤醒转换）。
// 唤醒 dispatch 前由 executor 调用，使 SDK 重入时将 sleep step 视为已睡过；幂等。
func (s *SQLiteStore) CompleteSleepingSteps(ctx context.Context, runID string) error {
	_, err := s.db.ExecContext(ctx,
		`UPDATE step_runs SET status=?, updated_at=? WHERE run_id=? AND status=?`,
		StepCompleted, fmtTime(time.Now()), runID, StepSleeping)
	return err
}

// CreateWait 写入一条 waitForEvent 挂起记录；INSERT OR IGNORE 使 (run_id, step_hash)
// 重复的 opcode（崩溃重试）不产生第二条 wait。
func (s *SQLiteStore) CreateWait(ctx context.Context, w Wait) error {
	var expires sql.NullString
	if w.ExpiresAt != nil {
		expires = sql.NullString{String: fmtTime(*w.ExpiresAt), Valid: true}
	}
	_, err := s.db.ExecContext(ctx,
		`INSERT OR IGNORE INTO waits (id, run_id, step_hash, step_id, event_name, match_expr, expires_at, status, created_at)
		 VALUES (?,?,?,?,?,?,?,?,?)`,
		w.ID, w.RunID, w.StepHash, w.StepID, w.EventName, w.MatchExpr, expires, w.Status, fmtTime(time.Now()))
	return err
}

// WaitingWaitsForEvent 返回订阅某事件名且仍在等待的 wait（事件到达时的候选集）。
func (s *SQLiteStore) WaitingWaitsForEvent(ctx context.Context, eventName string) ([]Wait, error) {
	return s.queryWaits(ctx,
		`SELECT id, run_id, step_hash, step_id, event_name, match_expr, expires_at, status, created_at
		 FROM waits WHERE status=? AND event_name=?`, WaitWaiting, eventName)
}

// ExpiredWaits 返回已过 expires_at 仍在等待的 wait（超时扫描用）。
func (s *SQLiteStore) ExpiredWaits(ctx context.Context, now time.Time) ([]Wait, error) {
	return s.queryWaits(ctx,
		`SELECT id, run_id, step_hash, step_id, event_name, match_expr, expires_at, status, created_at
		 FROM waits WHERE status=? AND expires_at IS NOT NULL AND expires_at<?`, WaitWaiting, fmtTime(now))
}

func (s *SQLiteStore) queryWaits(ctx context.Context, q string, args ...any) ([]Wait, error) {
	rows, err := s.db.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Wait
	for rows.Next() {
		var w Wait
		var exp sql.NullString
		var cat string
		if err := rows.Scan(&w.ID, &w.RunID, &w.StepHash, &w.StepID, &w.EventName, &w.MatchExpr, &exp, &w.Status, &cat); err != nil {
			return nil, err
		}
		if exp.Valid {
			t, err := parseTime(exp.String)
			if err != nil {
				return nil, err
			}
			w.ExpiresAt = &t
		}
		if w.CreatedAt, err = parseTime(cat); err != nil {
			return nil, err
		}
		out = append(out, w)
	}
	return out, rows.Err()
}

// SetWaitStatus 更新 wait 状态（resolved/timeout/cancelled）。
func (s *SQLiteStore) SetWaitStatus(ctx context.Context, id, status string) error {
	_, err := s.db.ExecContext(ctx, `UPDATE waits SET status=? WHERE id=?`, status, id)
	return err
}

// CancelWaitsForRun 把一个 run 的全部 waiting wait 置为 cancelled（run 取消/终态清扫）。返回影响行数。
func (s *SQLiteStore) CancelWaitsForRun(ctx context.Context, runID string) (int, error) {
	res, err := s.db.ExecContext(ctx,
		`UPDATE waits SET status=? WHERE run_id=? AND status=?`, WaitCancelled, runID, WaitWaiting)
	if err != nil {
		return 0, err
	}
	n, err := res.RowsAffected()
	return int(n), err
}

// ActiveRunsByFunction 返回某函数的全部非终态 run（cancelOn 匹配的候选集，FR-4.9）。
// 带触发事件名/数据：cancelOn 的 match 表达式需要以 run 触发事件作为 event 环境。
func (s *SQLiteStore) ActiveRunsByFunction(ctx context.Context, functionID string) ([]Run, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT id, function_id, status, event_id, event_name, event_data, event_ts
		 FROM runs WHERE function_id=? AND status IN (?,?)`, functionID, RunQueued, RunRunning)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Run
	for rows.Next() {
		var r Run
		var edata, ets string
		if err := rows.Scan(&r.ID, &r.FunctionID, &r.Status, &r.EventID, &r.EventName, &edata, &ets); err != nil {
			return nil, err
		}
		r.EventData = json.RawMessage(edata)
		if r.EventTS, err = parseTime(ets); err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

// Enqueue 写入一个 pending 队列项。
func (s *SQLiteStore) Enqueue(ctx context.Context, item QueueItem) error {
	_, err := s.db.ExecContext(ctx,
		`INSERT INTO queue_items (id, function_id, run_id, score, at, status, created_at)
		 VALUES (?,?,?,?,?,?,?)`,
		item.ID, item.FunctionID, item.RunID, item.Score, fmtTime(item.At), QueuePending, fmtTime(time.Now()))
	return err
}

// LeaseBatch 原子租赁至多 limit 个到期的 pending 项（UPDATE...RETURNING）。
func (s *SQLiteStore) LeaseBatch(ctx context.Context, now time.Time, leaseDur time.Duration, limit int) ([]QueueItem, error) {
	expires := fmtTime(now.Add(leaseDur))
	rows, err := s.db.QueryContext(ctx,
		`UPDATE queue_items SET status=?, lease_expires=?
		 WHERE id IN (SELECT id FROM queue_items WHERE status=? AND at<=? ORDER BY score LIMIT ?)
		 RETURNING id, function_id, run_id, score, at`,
		QueueLeased, expires, QueuePending, fmtTime(now), limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []QueueItem
	for rows.Next() {
		var it QueueItem
		var at string
		if err := rows.Scan(&it.ID, &it.FunctionID, &it.RunID, &it.Score, &at); err != nil {
			return nil, err
		}
		if it.At, err = parseTime(at); err != nil {
			return nil, err
		}
		it.Status = QueueLeased
		out = append(out, it)
	}
	return out, rows.Err()
}

// ReleaseLease 释放租约回到 pending，并推迟到 retryAt 可见（退避重试）。
func (s *SQLiteStore) ReleaseLease(ctx context.Context, id string, retryAt time.Time) error {
	_, err := s.db.ExecContext(ctx,
		`UPDATE queue_items SET status=?, at=?, lease_expires=NULL WHERE id=?`,
		QueuePending, fmtTime(retryAt), id)
	return err
}

// CompleteQueueItem 删除已完成的队列项。
func (s *SQLiteStore) CompleteQueueItem(ctx context.Context, id string) error {
	_, err := s.db.ExecContext(ctx, `DELETE FROM queue_items WHERE id=?`, id)
	return err
}

// RequeueExpiredLeases 把租约过期的项重置为 pending（进程崩溃后对账，PRD 7.2）。
func (s *SQLiteStore) RequeueExpiredLeases(ctx context.Context, now time.Time) (int, error) {
	res, err := s.db.ExecContext(ctx,
		`UPDATE queue_items SET status=?, lease_expires=NULL WHERE status=? AND lease_expires<=?`,
		QueuePending, QueueLeased, fmtTime(now))
	if err != nil {
		return 0, err
	}
	n, err := res.RowsAffected()
	return int(n), err
}

// ReplaceAppFunctions 用一次内省结果整体替换 appURL 名下的持久化函数集合（对应 Registry.Sync）。
// 单事务：先删除该 app 旧记录，再逐条 upsert；ON CONFLICT 覆盖处理函数跨 URL 迁移。
func (s *SQLiteStore) ReplaceAppFunctions(ctx context.Context, appURL string, fns []RegisteredFunction) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, `DELETE FROM functions WHERE app_url=?`, appURL); err != nil {
		return err
	}
	now := fmtTime(time.Now())
	for _, f := range fns {
		if _, err := tx.ExecContext(ctx,
			`INSERT INTO functions (id, app_url, event, match, cron, retries, cancel_on, updated_at) VALUES (?,?,?,?,?,?,?,?)
			 ON CONFLICT(id) DO UPDATE SET
			   app_url=excluded.app_url, event=excluded.event, match=excluded.match, cron=excluded.cron,
			   retries=excluded.retries, cancel_on=excluded.cancel_on, updated_at=excluded.updated_at`,
			f.ID, appURL, f.Event, f.Match, f.Cron, f.Retries, f.CancelOn, now); err != nil {
			return err
		}
	}
	return tx.Commit()
}

// LoadRegisteredFunctions 返回全部持久化的函数注册记录（启动恢复用），按 app_url, id 排序。
func (s *SQLiteStore) LoadRegisteredFunctions(ctx context.Context) ([]RegisteredFunction, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT id, app_url, event, COALESCE(match, ''), COALESCE(cron, ''), retries, COALESCE(cancel_on, '')
		 FROM functions ORDER BY app_url, id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []RegisteredFunction
	for rows.Next() {
		var f RegisteredFunction
		if err := rows.Scan(&f.ID, &f.AppURL, &f.Event, &f.Match, &f.Cron, &f.Retries, &f.CancelOn); err != nil {
			return nil, err
		}
		out = append(out, f)
	}
	return out, rows.Err()
}
