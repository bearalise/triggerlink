// Package runner 消费事件：匹配订阅函数、创建 run、入队（PRD 7.2）。
// 启动时先做对账扫描：把"已落库未路由"的事件重新喂入路由（崩溃恢复承诺的实现前提）。
// 另负责 step.waitForEvent 的恢复（事件命中/超时扫描）与 cancelOn 取消（FR-2.6 / FR-4.9）。
package runner

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"log/slog"
	"time"

	"github.com/bearalise/triggerlink/internal/ids"
	"github.com/bearalise/triggerlink/internal/metrics"
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
			slog.Error("reconcile route failed", "component", "runner", "event_id", e.ID, "err", err)
		}
	}
	if len(orphans) > 0 {
		slog.Info("reconciled unrouted events", "component", "runner", "count", len(orphans))
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
				slog.Error("route failed", "component", "runner", "event_id", e.ID, "err", err)
			}
		}
	}
}

func (r *Runner) route(ctx context.Context, e store.Event) error {
	routed := 0
	subscribers := r.Reg.Match(e.Name)
	if len(subscribers) == 0 {
		metrics.EventRouted(metrics.ResultNoSubscriber)
	}
	for _, f := range subscribers {
		// 暂停的函数不接新触发（FR-7.3）：跳过建 run；事件仍照常标记 routed。
		// 查询失败时 fail-open（不跳过），避免存储抖动导致触发丢失。
		if paused, err := r.Store.IsFunctionPaused(ctx, f.ID); err != nil {
			slog.Warn("check paused failed", "component", "runner", "function_id", f.ID, "err", err)
		} else if paused {
			metrics.EventRouted(metrics.ResultPaused)
			continue
		}
		// 事件触发 match（FR-3.1）：非空且不命中（含编译/求值失败）则不为该函数建 run。
		if !evalTriggerMatch(f.Match, e) {
			metrics.EventRouted(metrics.ResultFiltered)
			continue
		}
		// 防抖（FR-4.5）：不直接建 run，交给窗口逻辑合并。
		if f.Debounce != nil {
			merged, err := r.routeDebounced(ctx, f, e)
			if err != nil {
				return err
			}
			if merged {
				routed++
			}
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
		routed++
		metrics.EventRouted(metrics.ResultRouted)
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
	slog.Debug("event routed", "component", "runner", "event_id", e.ID, "event", e.Name, "runs", routed)
	// waitForEvent 恢复与 cancelOn 取消：失败只记日志，不影响事件路由落账。
	if err := r.resumeWaits(ctx, e); err != nil {
		slog.Error("resume waits failed", "component", "runner", "event_id", e.ID, "err", err)
	}
	if err := r.applyCancelOn(ctx, e); err != nil {
		slog.Error("cancel_on failed", "component", "runner", "event_id", e.ID, "err", err)
	}
	return r.Store.MarkEventRouted(ctx, e.ID)
}

// routeDebounced 处理配置了 debounce 的函数（FR-4.5）：同 (function_id, key) 的事件
// 在 period 窗口内合并为一个 run，取尾事件。
//
//   - 无窗口：建 run（快照 = 本事件）+ 入队 at=now+period + 开窗，返回 created=true。
//   - 有窗口：把 run 的事件快照换成本事件、把队列项与窗口的触发时间一起推后，run 复用。
//
// debounce.timeout 以窗口内首个事件为基准封顶：持续到来的事件不会把触发无限推后。
// 窗口在 dispatch 时由 executor 删除。竞态边界：事件恰好在 dispatch 租约之后到达时，
// 队列项已不是 pending，续窗落不到队列上——该事件仍写进了快照，但可能与下一个窗口的
// run 紧邻产生两个 run。M2 接受这一语义（PRD 记录）。
func (r *Runner) routeDebounced(ctx context.Context, f registry.Function, e store.Event) (bool, error) {
	key := evalFlowControlKey(f.Debounce.Key, e)
	now := time.Now()

	pending, ok, err := r.Store.GetDebouncePending(ctx, f.ID, key)
	if err != nil {
		return false, err
	}
	if ok {
		if err := r.Store.UpdateRunEventSnapshot(ctx, pending.RunID, e); err != nil {
			return false, err
		}
		at := debounceTriggerAt(now, pending.FirstAt, f.Debounce)
		if _, err := r.Store.UpdateQueueItemAt(ctx, pending.RunID, at); err != nil {
			return false, err
		}
		pending.TriggerAt = at
		if err := r.Store.UpsertDebouncePending(ctx, pending); err != nil {
			return false, err
		}
		metrics.EventRouted(metrics.ResultDebounced)
		slog.Debug("event merged into debounce window", "component", "runner",
			"event_id", e.ID, "function_id", f.ID, "run_id", pending.RunID,
			"debounce_key", key, "trigger_at", at)
		return false, nil
	}

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
		return false, err
	}
	if !created {
		// 该事件此前已为本函数建过 run（对账重放），窗口早已走完，不重复开窗。
		return false, nil
	}
	at := debounceTriggerAt(now, now, f.Debounce)
	if err := r.Store.Enqueue(ctx, store.QueueItem{
		ID:         ids.NewID("qi"),
		FunctionID: f.ID,
		RunID:      runID,
		Score:      now.UnixNano(),
		At:         at,
		Status:     store.QueuePending,
	}); err != nil {
		return false, err
	}
	if err := r.Store.UpsertDebouncePending(ctx, store.DebouncePending{
		FunctionID: f.ID, Key: key, RunID: runID, FirstAt: now, TriggerAt: at,
	}); err != nil {
		return false, err
	}
	metrics.EventRouted(metrics.ResultRouted)
	slog.Debug("debounce window opened", "component", "runner",
		"event_id", e.ID, "function_id", f.ID, "run_id", runID, "debounce_key", key, "trigger_at", at)
	return true, nil
}

// debounceTriggerAt 计算触发时刻：now+period，若配了 timeout 则不超过 firstAt+timeout。
func debounceTriggerAt(now, firstAt time.Time, d *registry.Debounce) time.Time {
	at := now.Add(d.Period)
	if d.Timeout > 0 {
		if max := firstAt.Add(d.Timeout); at.After(max) {
			return max
		}
	}
	return at
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
			slog.Error("get run for wait failed", "component", "runner", "wait_id", w.ID, "run_id", w.RunID, "err", err)
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
			slog.Error("resume wait save step failed", "component", "runner", "wait_id", w.ID, "run_id", w.RunID, "err", err)
			continue
		}
		if err := r.Store.SetWaitStatus(ctx, w.ID, store.WaitResolved); err != nil {
			slog.Error("resume wait failed", "component", "runner", "wait_id", w.ID, "run_id", w.RunID, "err", err)
			continue
		}
		metrics.StepObserved("WaitForEvent", time.Since(w.CreatedAt))
		r.enqueueResume(ctx, run)
		slog.Info("run resumed by event", "component", "runner", "run_id", run.ID, "function_id", run.FunctionID, "event_id", e.ID, "wait_id", w.ID)
	}
	return nil
}

// scanWaitTimeouts 定时扫描超时的 wait：step 以 output=null 完成，置 timeout，入队恢复。
func (r *Runner) scanWaitTimeouts(ctx context.Context) {
	waits, err := r.Store.ExpiredWaits(ctx, time.Now())
	if err != nil {
		slog.Error("scan wait timeouts failed", "component", "runner", "err", err)
		return
	}
	for _, w := range waits {
		run, err := r.Store.GetRun(ctx, w.RunID)
		if err != nil {
			slog.Error("get run for wait failed", "component", "runner", "wait_id", w.ID, "run_id", w.RunID, "err", err)
			continue
		}
		if isTerminal(run.Status) {
			r.Store.SetWaitStatus(ctx, w.ID, store.WaitCancelled)
			continue
		}
		if err := r.Store.SaveStepResult(ctx, store.StepResult{
			RunID: w.RunID, StepHash: w.StepHash, StepID: w.StepID, Op: "WaitForEvent",
			Status: store.StepCompleted, Output: json.RawMessage("null")}); err != nil {
			slog.Error("timeout wait save step failed", "component", "runner", "wait_id", w.ID, "run_id", w.RunID, "err", err)
			continue
		}
		if err := r.Store.SetWaitStatus(ctx, w.ID, store.WaitTimeout); err != nil {
			slog.Error("timeout wait failed", "component", "runner", "wait_id", w.ID, "run_id", w.RunID, "err", err)
			continue
		}
		metrics.StepObserved("WaitForEvent", time.Since(w.CreatedAt))
		r.enqueueResume(ctx, run)
		slog.Info("wait timed out", "component", "runner", "run_id", run.ID, "function_id", run.FunctionID, "wait_id", w.ID)
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
					slog.Error("cancel_on cancel run failed", "component", "runner", "run_id", run.ID, "function_id", run.FunctionID, "err", err)
					continue
				}
				if cancelled {
					metrics.RunFinished(metrics.StatusCancelled, time.Since(run.CreatedAt))
					r.Store.CancelWaitsForRun(ctx, run.ID)
					slog.Info("run cancelled by cancel_on", "component", "runner", "run_id", run.ID, "function_id", run.FunctionID, "event_id", e.ID)
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
		slog.Error("enqueue resume failed", "component", "runner", "run_id", run.ID, "function_id", run.FunctionID, "err", err)
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
