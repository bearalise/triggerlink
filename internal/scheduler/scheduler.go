// Package scheduler 是 cron 触发器（FR-3.2）：轮询注册了 Cron 表达式的函数，
// 到点直接 CreateRun + Enqueue（不经事件路由）。
// 语义：标准 5 字段 cron（分 时 日 月 周），UTC，分钟粒度。
// 重启不补跑（M2 可接受）：启动时以当前时刻为基线，只触发之后的到点。
package scheduler

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/robfig/cron/v3"

	"github.com/bearalise/triggerlink/internal/ids"
	"github.com/bearalise/triggerlink/internal/registry"
	"github.com/bearalise/triggerlink/internal/store"
)

// EventName 是 cron 触发 run 的事件名（run 的 EventName / EventData 快照用）。
const EventName = "triggerlink/cron"

// Scheduler 定时触发注册了 Cron 的函数。
type Scheduler struct {
	Store        store.Store
	Reg          *registry.Registry
	PollInterval time.Duration // 默认 1s

	mu     sync.Mutex
	last   time.Time            // 上次检查时刻：next=sched.Next(last)，只触发 (last, now] 内的到点
	fired  map[string]int64     // fnID → 最近触发的到点 unix 秒（同一分钟幂等）
	scheds map[string]schedEnt  // fnID → 解析缓存（表达式变化时重解析）
}

type schedEnt struct {
	expr  string
	sched cron.Schedule
}

// Run 阻塞运行直到 ctx 取消。以启动时刻为基线：崩溃/重启窗口内错过的到点不补跑。
func (s *Scheduler) Run(ctx context.Context) error {
	s.mu.Lock()
	if s.fired == nil {
		s.fired = map[string]int64{}
	}
	if s.scheds == nil {
		s.scheds = map[string]schedEnt{}
	}
	s.last = time.Now().UTC()
	s.mu.Unlock()

	interval := s.PollInterval
	if interval <= 0 {
		interval = time.Second
	}
	tick := time.NewTicker(interval)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case now := <-tick.C:
			s.check(ctx, now)
		}
	}
}

// check 检查 (last, now] 窗口内到点的 cron 函数并触发。与 ticker 解耦以便测试直接驱动。
func (s *Scheduler) check(ctx context.Context, now time.Time) {
	now = now.UTC()
	s.mu.Lock()
	if s.fired == nil {
		s.fired = map[string]int64{}
	}
	if s.scheds == nil {
		s.scheds = map[string]schedEnt{}
	}
	last := s.last
	if last.IsZero() {
		last = now // 直接驱动（测试）且未 Run 时：基线即 now，只触发恰好到点的表达式
	}
	s.last = now
	s.mu.Unlock()

	for _, f := range s.Reg.CronFunctions() {
		sched, err := s.scheduleFor(f)
		if err != nil {
			log.Printf("scheduler: cron parse %q (function %s): %v", f.Cron, f.ID, err)
			continue
		}
		if next := sched.Next(last); !next.After(now) {
			s.fire(ctx, f, next)
		}
	}
}

// scheduleFor 返回函数的已解析 cron schedule（按表达式缓存，变化时重解析）。
func (s *Scheduler) scheduleFor(f registry.Function) (cron.Schedule, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if ent, ok := s.scheds[f.ID]; ok && ent.expr == f.Cron {
		return ent.sched, nil
	}
	// 契约约定 cron 按 UTC 解释：显式 CRON_TZ=UTC 前缀，与宿主机时区无关。
	sched, err := cron.ParseStandard("CRON_TZ=UTC " + f.Cron)
	if err != nil {
		return nil, err
	}
	s.scheds[f.ID] = schedEnt{expr: f.Cron, sched: sched}
	return sched, nil
}

// fire 触发一次 cron run：EventID = cron_<function_id>_<到点unix秒>（确定性，
// 配合 fired 记录保证同一到点不重复建 run）；EventData = {"ts":<到点 RFC3339>}。
func (s *Scheduler) fire(ctx context.Context, f registry.Function, at time.Time) {
	s.mu.Lock()
	if s.fired[f.ID] == at.Unix() {
		s.mu.Unlock()
		return
	}
	s.fired[f.ID] = at.Unix()
	s.mu.Unlock()

	data, _ := json.Marshal(map[string]any{"ts": at.Format(time.RFC3339)})
	runID := ids.NewID("run")
	created, err := s.Store.CreateRun(ctx, store.Run{
		ID:         runID,
		FunctionID: f.ID,
		Status:     store.RunQueued,
		EventID:    fmt.Sprintf("cron_%s_%d", f.ID, at.Unix()),
		EventName:  EventName,
		EventData:  data,
		EventTS:    at,
	})
	if err != nil {
		log.Printf("scheduler: create run for %s: %v", f.ID, err)
		return
	}
	if !created {
		return // runID 是新生成的，撞库只可能是极端巧合；直接放弃本次触发
	}
	if err := s.Store.Enqueue(ctx, store.QueueItem{
		ID:         ids.NewID("qi"),
		FunctionID: f.ID,
		RunID:      runID,
		Score:      time.Now().UnixNano(),
		At:         time.Now(),
		Status:     store.QueuePending,
	}); err != nil {
		log.Printf("scheduler: enqueue run %s: %v", runID, err)
		return
	}
	log.Printf("scheduler: run %s fired for function %s (cron %q, at %s)",
		runID, f.ID, f.Cron, at.Format(time.RFC3339))
}
