// Package runner 消费事件：匹配订阅函数、创建 run、入队（PRD 7.2）。
// 启动时先做对账扫描：把"已落库未路由"的事件重新喂入路由（崩溃恢复承诺的实现前提）。
// 另负责 step.waitForEvent 的恢复（事件命中/超时扫描）与 cancelOn 取消（FR-2.6 / FR-4.9）。
package runner

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
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

	PollInterval time.Duration // waitForEvent 超时扫描间隔，默认 1s
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

	interval := r.PollInterval
	if interval <= 0 {
		interval = time.Second
	}
	tick := time.NewTicker(interval)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-tick.C:
			r.scanWaitTimeouts(ctx)
		case e := <-r.Stream:
			if err := r.route(ctx, e); err != nil {
				log.Printf("runner: route %s: %v", e.ID, err)
			}
		}
	}
}

func (r *Runner) route(ctx context.Context, e store.Event) error {
	for _, f := range r.Reg.Match(e.Name) {
		// 暂停的函数不接新触发（FR-7.3）：跳过建 run；事件仍照常标记 routed。
		// 查询失败时 fail-open（不跳过），避免存储抖动导致触发丢失。
		if paused, err := r.Store.IsFunctionPaused(ctx, f.ID); err != nil {
			log.Printf("runner: check paused %s: %v", f.ID, err)
		} else if paused {
			continue
		}
		// 事件触发 match（FR-3.1）：非空且不命中（含编译/求值失败）则不为该函数建 run。
		if !evalTriggerMatch(f.Match, e) {
			continue
		}
		// run ID 由 (event_id, function_id) 确定性派生：对账扫描与 stream 消费重叠导致
		// 同一事件被路由两次时，第二次 CreateRun 幂等落空（created=false），不重复入队执行。
		runID := derivedRunID(e.ID, f.ID)
		created, err := r.Store.CreateRun(ctx, store.Run{
			ID:         runID,
			FunctionID: f.ID,
			Status:     store.RunQueued,
			EventID:    e.ID,
			EventName:  e.Name,
			EventData:  e.Data,
			EventTS:    e.TS,
		})
		if err != nil {
			return err
		}
		if !created {
			continue
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
	// waitForEvent 恢复与 cancelOn 取消：失败只记日志，不影响事件路由落账。
	if err := r.resumeWaits(ctx, e); err != nil {
		log.Printf("runner: resume waits on %s: %v", e.ID, err)
	}
	if err := r.applyCancelOn(ctx, e); err != nil {
		log.Printf("runner: cancelOn on %s: %v", e.ID, err)
	}
	return r.Store.MarkEventRouted(ctx, e.ID)
}

// resumeWaits 处理到达事件对 waitForEvent 挂起的恢复：命中的 wait 补写 completed memo
// （output = 匹配事件 {"id","name","data","ts"}）、置 resolved、以 at=now 入队恢复 run。
// 恢复前重查 run 状态：终态 run 不恢复，wait 置 cancelled 清扫。
func (r *Runner) resumeWaits(ctx context.Context, e store.Event) error {
	waits, err := r.Store.WaitingWaitsForEvent(ctx, e.Name)
	if err != nil {
		return err
	}
	for _, w := range waits {
		run, err := r.Store.GetRun(ctx, w.RunID)
		if err != nil {
			log.Printf("runner: wait %s get run %s: %v", w.ID, w.RunID, err)
			continue
		}
		if isTerminal(run.Status) {
			r.Store.SetWaitStatus(ctx, w.ID, store.WaitCancelled)
			continue
		}
		if !evalMatch(w.MatchExpr, e, run) {
			continue
		}
		out, _ := json.Marshal(map[string]any{"id": e.ID, "name": e.Name, "data": jsonToAny(e.Data), "ts": e.TS})
		if err := r.Store.SaveStepResult(ctx, store.StepResult{
			RunID: w.RunID, StepHash: w.StepHash, StepID: w.StepID, Op: "WaitForEvent",
			Status: store.StepCompleted, Output: out}); err != nil {
			log.Printf("runner: resume wait %s save step: %v", w.ID, err)
			continue
		}
		if err := r.Store.SetWaitStatus(ctx, w.ID, store.WaitResolved); err != nil {
			log.Printf("runner: resume wait %s: %v", w.ID, err)
			continue
		}
		r.enqueueResume(ctx, run)
		log.Printf("runner: run %s resumed by event %s (wait %s)", run.ID, e.ID, w.ID)
	}
	return nil
}

// scanWaitTimeouts 定时扫描超时的 wait：step 以 output=null 完成，置 timeout，入队恢复。
func (r *Runner) scanWaitTimeouts(ctx context.Context) {
	waits, err := r.Store.ExpiredWaits(ctx, time.Now())
	if err != nil {
		log.Printf("runner: scan wait timeouts: %v", err)
		return
	}
	for _, w := range waits {
		run, err := r.Store.GetRun(ctx, w.RunID)
		if err != nil {
			log.Printf("runner: wait %s get run %s: %v", w.ID, w.RunID, err)
			continue
		}
		if isTerminal(run.Status) {
			r.Store.SetWaitStatus(ctx, w.ID, store.WaitCancelled)
			continue
		}
		if err := r.Store.SaveStepResult(ctx, store.StepResult{
			RunID: w.RunID, StepHash: w.StepHash, StepID: w.StepID, Op: "WaitForEvent",
			Status: store.StepCompleted, Output: json.RawMessage("null")}); err != nil {
			log.Printf("runner: timeout wait %s save step: %v", w.ID, err)
			continue
		}
		if err := r.Store.SetWaitStatus(ctx, w.ID, store.WaitTimeout); err != nil {
			log.Printf("runner: timeout wait %s: %v", w.ID, err)
			continue
		}
		r.enqueueResume(ctx, run)
		log.Printf("runner: run %s wait %s timed out", run.ID, w.ID)
	}
}

// applyCancelOn 处理 cancelOn（FR-4.9）：注册了该事件名规则的函数，其在途 run
// 求值 match 命中即取消（复用 CancelRun），并清扫其 waiting wait。
func (r *Runner) applyCancelOn(ctx context.Context, e store.Event) error {
	for _, f := range r.Reg.CancelMatchers(e.Name) {
		runs, err := r.Store.ActiveRunsByFunction(ctx, f.ID)
		if err != nil {
			return err
		}
		for _, run := range runs {
			if cancelMatchHit(f, e, run) {
				cancelled, err := r.Store.CancelRun(ctx, run.ID)
				if err != nil {
					log.Printf("runner: cancelOn cancel run %s: %v", run.ID, err)
					continue
				}
				if cancelled {
					r.Store.CancelWaitsForRun(ctx, run.ID)
					log.Printf("runner: run %s cancelled by cancelOn event %s", run.ID, e.ID)
				}
			}
		}
	}
	return nil
}

// cancelMatchHit 判定函数 f 的任一匹配该事件的 cancelOn 规则是否命中 run。
func cancelMatchHit(f registry.Function, e store.Event, run store.Run) bool {
	for _, rule := range f.CancelOn {
		if rule.Event == e.Name && evalMatch(rule.Match, e, run) {
			return true
		}
	}
	return false
}

// enqueueResume 以 at=now 入队恢复跳（事件命中/超时共用）。
func (r *Runner) enqueueResume(ctx context.Context, run store.Run) {
	if err := r.Store.Enqueue(ctx, store.QueueItem{
		ID:         ids.NewID("qi"),
		FunctionID: run.FunctionID,
		RunID:      run.ID,
		Score:      time.Now().UnixNano(),
		At:         time.Now(),
		Status:     store.QueuePending,
	}); err != nil {
		log.Printf("runner: enqueue resume for run %s: %v", run.ID, err)
	}
}

func isTerminal(status string) bool {
	return status == store.RunCompleted || status == store.RunFailed || status == store.RunCancelled
}

// derivedRunID 由 (event_id, function_id) 派生确定性 run ID（双路由幂等的前提）。
func derivedRunID(eventID, functionID string) string {
	sum := sha256.Sum256([]byte(eventID + ":" + functionID))
	return "run_" + hex.EncodeToString(sum[:12])
}
