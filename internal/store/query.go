package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"strings"
	"time"
)

// ListRunsOptions 是 ListRuns 的过滤条件；空值表示不过滤。
// Before 为复合游标 "<created_at>|<id>"（取该位置之前的记录），因事件路由的 run
// ID 是 (event_id, function_id) 的哈希（derivedRunID），不含时间序，无法用 id 翻页。
type ListRunsOptions struct {
	FunctionID string
	Status     string
	EventID    string
	Before     string
	Limit      int
}

func clampLimit(n int) int {
	if n <= 0 || n > 200 {
		return 50
	}
	return n
}

// splitRunCursor 解析 Before 复合游标 "<created_at>|<id>"；时间部分归一化为存储格式
// （调用方传入的可能是 JSON 序列化的变宽 RFC3339Nano），使字符串比较与时间序一致。
// 非法游标返回 ok=false，调用方按无游标处理。
func splitRunCursor(before string) (ts time.Time, id string, ok bool) {
	i := strings.LastIndex(before, "|")
	if i <= 0 || i == len(before)-1 {
		return time.Time{}, "", false
	}
	t, err := parseTime(before[:i])
	if err != nil {
		return time.Time{}, "", false
	}
	return t, before[i+1:], true
}

// ListRuns 按 created_at（创建时间）倒序返回 run 摘要，id 作同刻 tiebreaker；
// 不填充 EventData/Output（列表页不需要大 payload）。
func (s *SQLStore) ListRuns(ctx context.Context, o ListRunsOptions) ([]Run, error) {
	var cursor, cursorTS, cursorID string
	if ts, id, ok := splitRunCursor(o.Before); ok {
		cursor, cursorTS, cursorID = o.Before, fmtTime(ts), id
	}
	rows, err := s.query(ctx,
		`SELECT id, function_id, status, event_id, event_name, attempt, created_at, ended_at
		 FROM runs
		 WHERE (?='' OR function_id=?) AND (?='' OR status=?) AND (?='' OR event_id=?)
		   AND (?='' OR created_at<? OR (created_at=? AND id<?))
		 ORDER BY created_at DESC, id DESC LIMIT ?`,
		o.FunctionID, o.FunctionID, o.Status, o.Status, o.EventID, o.EventID,
		cursor, cursorTS, cursorTS, cursorID, clampLimit(o.Limit))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Run
	for rows.Next() {
		var r Run
		var cat string
		var eat sql.NullString
		if err := rows.Scan(&r.ID, &r.FunctionID, &r.Status, &r.EventID, &r.EventName, &r.Attempt, &cat, &eat); err != nil {
			return nil, err
		}
		if r.CreatedAt, err = parseTime(cat); err != nil {
			return nil, err
		}
		if eat.Valid {
			t, err := parseTime(eat.String)
			if err != nil {
				return nil, err
			}
			r.EndedAt = &t
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

// ListEventsOptions 是 ListEvents 的过滤条件。
type ListEventsOptions struct {
	Name   string
	Before string
	Limit  int
}

// ListEvents 按 id（时间序）倒序返回事件。
func (s *SQLStore) ListEvents(ctx context.Context, o ListEventsOptions) ([]Event, error) {
	rows, err := s.query(ctx,
		`SELECT id, name, data, ts, received_at FROM events
		 WHERE (?='' OR name=?) AND (?='' OR id<?)
		 ORDER BY id DESC LIMIT ?`,
		o.Name, o.Name, o.Before, o.Before, clampLimit(o.Limit))
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

// RunIDsByEventIDs 反查事件触发的 run ID 列表（事件流页"触发了哪些 run"）。
func (s *SQLStore) RunIDsByEventIDs(ctx context.Context, eventIDs []string) (map[string][]string, error) {
	out := map[string][]string{}
	if len(eventIDs) == 0 {
		return out, nil
	}
	ph := strings.TrimSuffix(strings.Repeat("?,", len(eventIDs)), ",")
	args := make([]any, len(eventIDs))
	for i, id := range eventIDs {
		args[i] = id
	}
	rows, err := s.query(ctx,
		`SELECT event_id, id FROM runs WHERE event_id IN (`+ph+`) ORDER BY id`, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var eid, rid string
		if err := rows.Scan(&eid, &rid); err != nil {
			return nil, err
		}
		out[eid] = append(out[eid], rid)
	}
	return out, rows.Err()
}

// StepRecord 是 Run 详情时间线的一行（比 StepResult 少 step_hash，多 updated_at）。
type StepRecord struct {
	StepID    string
	Op        string
	Status    string
	Output    json.RawMessage
	Error     string
	Attempt   int
	UpdatedAt *time.Time
}

// ListStepRecords 按写入序（=执行序）返回一个 run 的 step 时间线。M0 遗留行 UpdatedAt 为 nil、
// seq 为 0，回落到按 updated_at 排（尽力而为，与加 seq 列之前的行为一致）。
func (s *SQLStore) ListStepRecords(ctx context.Context, runID string) ([]StepRecord, error) {
	rows, err := s.query(ctx,
		`SELECT step_id, op, status, output, error, attempt, updated_at
		 FROM step_runs WHERE run_id=? ORDER BY seq, updated_at`, runID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []StepRecord
	for rows.Next() {
		var sr StepRecord
		var output, errStr, ua sql.NullString
		if err := rows.Scan(&sr.StepID, &sr.Op, &sr.Status, &output, &errStr, &sr.Attempt, &ua); err != nil {
			return nil, err
		}
		if output.Valid {
			sr.Output = json.RawMessage(output.String)
		}
		sr.Error = errStr.String
		if ua.Valid {
			t, err := parseTime(ua.String)
			if err != nil {
				return nil, err
			}
			sr.UpdatedAt = &t
		}
		out = append(out, sr)
	}
	return out, rows.Err()
}

// FunctionStat 是一个函数的 24h 聚合统计。
type FunctionStat struct {
	Total     int
	Completed int
	Failed    int
}

// FunctionStats24h 聚合 since 之后创建的 run，按键 function_id 返回。
func (s *SQLStore) FunctionStats24h(ctx context.Context, since time.Time) (map[string]FunctionStat, error) {
	rows, err := s.query(ctx,
		`SELECT function_id, status, COUNT(*) FROM runs WHERE created_at>=? GROUP BY function_id, status`,
		fmtTime(since))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[string]FunctionStat{}
	for rows.Next() {
		var fid, status string
		var n int
		if err := rows.Scan(&fid, &status, &n); err != nil {
			return nil, err
		}
		st := out[fid]
		st.Total += n
		switch status {
		case RunCompleted:
			st.Completed += n
		case RunFailed:
			st.Failed += n
		}
		out[fid] = st
	}
	return out, rows.Err()
}
