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
	DeleteThrottleStateBefore(ctx context.Context, cutoff time.Time) (int, error)
}

// staleThrottleAge 是限流配额记录的清理阈值：窗口起点早于此的记录必然已过期
// （最长的合理窗口也远短于一天），删掉只是回收行，不影响限流语义。
// 它与 -retention-days 无关——配额记录不是历史数据，不该跟着保留期一起留 30 天。
const staleThrottleAge = 24 * time.Hour

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
	throttles, err := j.Store.DeleteThrottleStateBefore(ctx, now.Add(-staleThrottleAge))
	if err != nil {
		slog.Error("delete stale throttle state", "component", "janitor", "err", err)
	}

	attrs := []any{"component", "janitor", "cutoff", cutoff, "runs", runs, "waits", waits,
		"events", events, "throttle_state", throttles, "duration", time.Since(started)}
	if runs+waits+events+throttles > 0 {
		slog.Info("retention cleanup", attrs...)
	} else {
		slog.Debug("retention cleanup", attrs...)
	}
}
