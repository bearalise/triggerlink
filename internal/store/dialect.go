// 方言层：抹平 SQLite 与 Postgres 的 SQL 差异，使查询主体只写一份。
//
// 差异面刻意压到最小——查询一律写标准 SQL + `?` 占位符 + `ON CONFLICT` 语法
// （SQLite 3.24+ 与 Postgres 都支持），方言只负责三件事：占位符改写、建表 DDL、
// 补列策略。避免"一个后端一份 1500 行实现"的长期双写成本。
package store

import (
	"context"
	"database/sql"
	"strconv"
	"strings"
)

type dialect interface {
	// name 用于日志与错误信息。
	name() string
	// rebind 把 `?` 占位符改写成该后端的形式。
	rebind(query string) string
	// schema 返回全量建表 DDL（CREATE TABLE/INDEX IF NOT EXISTS，可反复执行）。
	schema() string
	// ensureColumn 幂等补列（老库升级路径）。
	ensureColumn(ctx context.Context, db *sql.DB, table, column, typ string) error
	// boolType 是布尔列在该后端的类型名（补列时用）。
	boolType() string
}

// --- SQLite ---

type sqliteDialect struct{}

func (sqliteDialect) name() string { return "sqlite" }

// SQLite 原生就用 `?`，无需改写。
func (sqliteDialect) rebind(query string) string { return query }

func (sqliteDialect) schema() string { return sqliteSchema }

func (sqliteDialect) boolType() string { return "INTEGER NOT NULL DEFAULT 0" }

// ensureColumn 给已存在的表补列（SQLite 无 ADD COLUMN IF NOT EXISTS，先查 PRAGMA）。
func (sqliteDialect) ensureColumn(ctx context.Context, db *sql.DB, table, column, typ string) error {
	rows, err := db.QueryContext(ctx, `PRAGMA table_info(`+table+`)`)
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
	_, err = db.ExecContext(ctx, `ALTER TABLE `+table+` ADD COLUMN `+column+` `+typ)
	return err
}

// --- Postgres ---

type postgresDialect struct{}

func (postgresDialect) name() string { return "postgres" }

// rebind 把第 n 个 `?` 换成 `$n`（pgx 的 stdlib 模式只认 $n）。
// 字符串字面量里出现 `?` 的查询本代码库没有；若将来出现，需在此处加转义处理。
func (postgresDialect) rebind(query string) string {
	if !strings.Contains(query, "?") {
		return query
	}
	var b strings.Builder
	b.Grow(len(query) + 8)
	n := 0
	for _, r := range query {
		if r == '?' {
			n++
			b.WriteByte('$')
			b.WriteString(strconv.Itoa(n))
			continue
		}
		b.WriteRune(r)
	}
	return b.String()
}

func (postgresDialect) schema() string { return postgresSchema }

func (postgresDialect) boolType() string { return "BOOLEAN NOT NULL DEFAULT FALSE" }

func (postgresDialect) ensureColumn(ctx context.Context, db *sql.DB, table, column, typ string) error {
	_, err := db.ExecContext(ctx, `ALTER TABLE `+table+` ADD COLUMN IF NOT EXISTS `+column+` `+typ)
	return err
}

// --- 绑定了方言的执行封装 ---
//
// 所有查询都经过这四个方法，占位符改写只在这里发生；调用点保持写 `?`。

func (s *SQLStore) exec(ctx context.Context, query string, args ...any) (sql.Result, error) {
	return s.db.ExecContext(ctx, s.d.rebind(query), args...)
}

func (s *SQLStore) query(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
	return s.db.QueryContext(ctx, s.d.rebind(query), args...)
}

func (s *SQLStore) queryRow(ctx context.Context, query string, args ...any) *sql.Row {
	return s.db.QueryRowContext(ctx, s.d.rebind(query), args...)
}

func (s *SQLStore) begin(ctx context.Context) (*boundTx, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	return &boundTx{Tx: tx, d: s.d}, nil
}

// boundTx 是绑定了方言的事务；嵌入 *sql.Tx，Commit/Rollback 照常可用。
type boundTx struct {
	*sql.Tx
	d dialect
}

func (t *boundTx) exec(ctx context.Context, query string, args ...any) (sql.Result, error) {
	return t.Tx.ExecContext(ctx, t.d.rebind(query), args...)
}

func (t *boundTx) query(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
	return t.Tx.QueryContext(ctx, t.d.rebind(query), args...)
}

func (t *boundTx) queryRow(ctx context.Context, query string, args ...any) *sql.Row {
	return t.Tx.QueryRowContext(ctx, t.d.rebind(query), args...)
}
