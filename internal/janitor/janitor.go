// Package janitor 是保留策略清理（FR-6.6）：周期删除超过保留期的历史数据，
// 使长期运行的实例不会无界增长。在途数据永不删除——清理只看终态记录。
//
// 独立 goroutine 而非挂在 runner tick 上：清理是重事务（级联删除），
// 不该与 1s 粒度的事件路由/超时扫描共用节奏，也便于单独测试。
package janitor

import (
	"context"
	"log/slog"
	"time"
)

// Store 是清理需要的存储操作（store.Store 天然满足）。
type Store interface {
	DeleteRunsBefore(ctx context.Context, cutoff time.Time) (int, error)
	DeleteEventsBefore(ctx context.Context, cutoff time.Time) (int, error)
	DeleteFinishedWaitsBefore(ctx context.Context, cutoff time.Time) (int, error)
}

// Janitor 周期执行保留策略清理。
type Janitor struct {
	Store     Store
	Retention time.Duration // 保留期；<=0 表示不清理（Run 直接返回）
	Interval  time.Duration // 清理周期，默认 1h
}

// Run 阻塞运行直到 ctx 取消。启动即清理一次（重启后不必等满一个周期），此后按 Interval 循环。
func (j *Janitor) Run(ctx context.Context) error {
	if j.Retention <= 0 {
		slog.Info("retention cleanup disabled", "component", "janitor")
		return nil
	}
	interval := j.Interval
	if interval <= 0 {
		interval = time.Hour
	}
	tick := time.NewTicker(interval)
	defer tick.Stop()
	for {
		j.Clean(ctx, time.Now())
		select {
		case <-ctx.Done():
			return nil
		case <-tick.C:
		}
	}
}

// Clean 执行一次清理（与 ticker 解耦，便于测试直接驱动）。
// 三步各自独立：任一步失败只记日志，不阻断其余两步——清理是尽力而为的后台工作。
func (j *Janitor) Clean(ctx context.Context, now time.Time) {
	cutoff := now.Add(-j.Retention)
	started := time.Now()

	runs, err := j.Store.DeleteRunsBefore(ctx, cutoff)
	if err != nil {
		slog.Error("delete expired runs", "component", "janitor", "cutoff", cutoff, "err", err)
	}
	waits, err := j.Store.DeleteFinishedWaitsBefore(ctx, cutoff)
	if err != nil {
		slog.Error("delete expired waits", "component", "janitor", "cutoff", cutoff, "err", err)
	}
	// 事件放最后：run 删除后，原本被终态 run 引用的事件也一并可删。
	events, err := j.Store.DeleteEventsBefore(ctx, cutoff)
	if err != nil {
		slog.Error("delete expired events", "component", "janitor", "cutoff", cutoff, "err", err)
	}

	attrs := []any{"component", "janitor", "cutoff", cutoff,
		"runs", runs, "waits", waits, "events", events, "duration", time.Since(started)}
	if runs+waits+events > 0 {
		slog.Info("retention cleanup", attrs...)
	} else {
		slog.Debug("retention cleanup", attrs...)
	}
}
