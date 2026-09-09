// 打开存储：按 URI 分流到 SQLite 或 Postgres。
package store

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib" // Postgres 驱动（database/sql 兼容模式）
	_ "modernc.org/sqlite"             // SQLite 驱动（纯 Go，无 cgo）
)

// Open 打开（必要时创建）存储并执行迁移。
//
// uri 以 postgres:// 或 postgresql:// 开头时用 Postgres，否则当作 SQLite 文件路径。
// 一个 flag 覆盖两种后端，部署时不必区分参数名。
func Open(uri string) (*SQLStore, error) {
	if isPostgresURI(uri) {
		return OpenPostgres(uri)
	}
	return OpenSQLite(uri)
}

func isPostgresURI(uri string) bool {
	return strings.HasPrefix(uri, "postgres://") || strings.HasPrefix(uri, "postgresql://")
}

// OpenSQLite 打开 SQLite 库。单连接：SQLite 单写者，避免 SQLITE_BUSY 抖动。
func OpenSQLite(path string) (*SQLStore, error) {
	dsn := fmt.Sprintf("file:%s?_pragma=busy_timeout(5000)&_pragma=journal_mode(WAL)", path)
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, fmt.Errorf("open sqlite: %w", err)
	}
	db.SetMaxOpenConns(1)
	return finishOpen(db, sqliteDialect{})
}

// OpenPostgres 打开 Postgres 库。连接池上限保守取 10：M2 是单消费者进程，
// 队列租赁不并发争抢，池子过大只会白占服务端连接。
func OpenPostgres(uri string) (*SQLStore, error) {
	db, err := sql.Open("pgx", uri)
	if err != nil {
		return nil, fmt.Errorf("open postgres: %w", err)
	}
	db.SetMaxOpenConns(10)
	db.SetMaxIdleConns(5)
	db.SetConnMaxLifetime(time.Hour)
	return finishOpen(db, postgresDialect{})
}

func finishOpen(db *sql.DB, d dialect) (*SQLStore, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := db.PingContext(ctx); err != nil {
		db.Close()
		return nil, fmt.Errorf("connect %s: %w", d.name(), err)
	}
	if err := migrate(ctx, db, d); err != nil {
		db.Close()
		return nil, fmt.Errorf("migrate %s: %w", d.name(), err)
	}
	return &SQLStore{db: db, d: d}, nil
}

// Close 关闭底层数据库。
func (s *SQLStore) Close() error { return s.db.Close() }
