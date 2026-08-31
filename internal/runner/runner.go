// Package runner 消费事件：匹配订阅函数、创建 run、入队（PRD 7.2）。
// 启动时先做对账扫描：把"已落库未路由"的事件重新喂入路由（崩溃恢复承诺的实现前提）。
package runner

import (
	"context"
	"log"
	"time"

	"github.com/bearalise/triggerlink/internal/ids"
	"github.com/bearalise/triggerlink/internal/registry"
	"github.com/bearalise/triggerlink/internal/store"
)

// Runner 消费 stream 中的事件并路由。
type Runner struct {
	Store  store.Store
	Reg    *registry.Registry
	Stream <-chan store.Event
}

// Run 阻塞运行直到 ctx 取消。
func (r *Runner) Run(ctx context.Context) error {
	// 启动对账：崩溃窗口内"已落库未路由"的事件
	orphans, err := r.Store.UnroutedEvents(ctx)
	if err != nil {
		return err
	}
	for _, e := range orphans {
		if err := r.route(ctx, e); err != nil {
			log.Printf("runner: reconcile route %s: %v", e.ID, err)
		}
	}
	if len(orphans) > 0 {
		log.Printf("runner: reconciled %d unrouted events", len(orphans))
	}

	for {
		select {
		case <-ctx.Done():
			return nil
		case e := <-r.Stream:
			if err := r.route(ctx, e); err != nil {
				log.Printf("runner: route %s: %v", e.ID, err)
			}
		}
	}
}

func (r *Runner) route(ctx context.Context, e store.Event) error {
	for _, f := range r.Reg.Match(e.Name) {
		runID := ids.NewID("run")
		if err := r.Store.CreateRun(ctx, store.Run{
			ID:         runID,
			FunctionID: f.ID,
			Status:     store.RunQueued,
			EventID:    e.ID,
			EventName:  e.Name,
			EventData:  e.Data,
			EventTS:    e.TS,
		}); err != nil {
			return err
		}
		// 延迟事件（FR-1.6）：ts 为未来时间时，到点前不出队触发。
		at := time.Now()
		if e.TS.After(at) {
			at = e.TS
		}
		if err := r.Store.Enqueue(ctx, store.QueueItem{
			ID:         ids.NewID("qi"),
			FunctionID: f.ID,
			RunID:      runID,
			Score:      time.Now().UnixNano(),
			At:         at,
			Status:     store.QueuePending,
		}); err != nil {
			return err
		}
	}
	return r.Store.MarkEventRouted(ctx, e.ID)
}
