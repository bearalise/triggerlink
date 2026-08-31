// Package executor 是执行引擎：租赁队列项 → HTTP 回调应用 serve 端点 →
// 解析 SDK 返回的 opcode → 写 step memo → 决定推进/重试/终结（PRD 8.2/8.3）。
package executor

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/bearalise/triggerlink/internal/ids"
	"github.com/bearalise/triggerlink/internal/registry"
	"github.com/bearalise/triggerlink/internal/sign"
	"github.com/bearalise/triggerlink/internal/store"
)

// Executor 执行队列中的 run。
type Executor struct {
	Store               store.Store
	Reg                 *registry.Registry
	Client              *http.Client
	SigningKey          string
	Stream              chan<- store.Event // step.sendEvent 扇出用；nil 时仅落库不入流（测试用）
	MaxRetries          int                // 默认 4；M0：回调尝试总数上限；函数注册了 Retries 时被其覆盖
	Concurrency         int                // 每函数并发 run 上限，默认 100
	BatchSize           int                // 每轮租赁上限，默认 16
	PollInterval        time.Duration      // 默认 100ms
	LeaseDuration       time.Duration      // 默认 30s
	BaseBackoff         time.Duration      // 默认 1s；退避 = BaseBackoff << attempt，封顶 30s
	TimeoutScanInterval time.Duration      // run 超时扫描（FR-4.3）间隔，默认 1s
}

// Run 阻塞运行直到 ctx 取消。启动先做租约对账（崩溃恢复）。
func (e *Executor) Run(ctx context.Context) error {
	e.defaults()
	if n, err := e.Store.RequeueExpiredLeases(ctx, time.Now()); err != nil {
		return err
	} else if n > 0 {
		slog.Info("reconciled expired leases", "component", "executor", "count", n)
	}

	var mu sync.Mutex
	inFlight := map[string]int{}
	tick := time.NewTicker(e.PollInterval)
	defer tick.Stop()
	timeoutTick := time.NewTicker(e.TimeoutScanInterval)
	defer timeoutTick.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-timeoutTick.C:
			e.scanRunTimeouts(ctx)
			continue
		case <-tick.C:
		}
		items, err := e.Store.LeaseBatch(ctx, time.Now(), e.LeaseDuration, e.BatchSize)
		if err != nil {
			slog.Error("lease batch failed", "component", "executor", "err", err)
			continue
		}
		for _, it := range items {
			mu.Lock()
			if inFlight[it.FunctionID] >= e.Concurrency {
				mu.Unlock()
				e.Store.ReleaseLease(ctx, it.ID, time.Now().Add(e.PollInterval))
				continue
			}
			inFlight[it.FunctionID]++
			mu.Unlock()
			go func() {
				defer func() { mu.Lock(); inFlight[it.FunctionID]--; mu.Unlock() }()
				e.dispatch(ctx, it)
			}()
		}
	}
}

func (e *Executor) defaults() {
	if e.MaxRetries <= 0 {
		e.MaxRetries = 4
	}
	if e.Concurrency <= 0 {
		e.Concurrency = 100
	}
	if e.BatchSize <= 0 {
		e.BatchSize = 16
	}
	if e.PollInterval <= 0 {
		e.PollInterval = 100 * time.Millisecond
	}
	if e.LeaseDuration <= 0 {
		e.LeaseDuration = 30 * time.Second
	}
	if e.BaseBackoff <= 0 {
		e.BaseBackoff = time.Second
	}
	if e.TimeoutScanInterval <= 0 {
		e.TimeoutScanInterval = time.Second
	}
	if e.Client == nil {
		e.Client = &http.Client{Timeout: 5 * time.Minute} // PRD 8.3：step 内同步执行，默认 5 分钟
	}
}

// ---- 协议类型（与 sdk/serve.go 的响应、sdk 的 callbackRequest 严格同构）----

type stepState struct {
	ID     string          `json:"id"`
	Status string          `json:"status"`
	Output json.RawMessage `json:"output,omitempty"`
}

type eventPayload struct {
	ID   string          `json:"id"`
	Name string          `json:"name"`
	Data json.RawMessage `json:"data"`
	TS   time.Time       `json:"ts"`
}

type callbackRequest struct {
	Ctx struct {
		RunID      string               `json:"run_id"`
		FunctionID string               `json:"function_id"`
		Attempt    int                  `json:"attempt"`
		Event      eventPayload         `json:"event"`
		Steps      map[string]stepState `json:"steps"`
	} `json:"ctx"`
}

type opError struct {
	Message   string `json:"message"`
	Stack     string `json:"stack,omitempty"`
	Retryable bool   `json:"retryable"`
}

type opcode struct {
	Op      string          `json:"op"`
	ID      string          `json:"id,omitempty"`
	StepID  string          `json:"step_id,omitempty"`
	Output  json.RawMessage `json:"output,omitempty"`
	Error   *opError        `json:"error,omitempty"`
	Until   time.Time       `json:"until,omitempty"`   // Sleep opcode：唤醒时间（RFC3339）
	Events  []sendEventIn   `json:"events,omitempty"`  // SendEvent opcode：待扇出事件
	Event   string          `json:"event,omitempty"`   // WaitForEvent opcode：等待的事件名（必填）
	Match   string          `json:"match,omitempty"`   // WaitForEvent opcode：可选 match 表达式（expr-lang）
	Timeout string          `json:"timeout,omitempty"` // WaitForEvent opcode：可选超时（Go duration，如 168h）
}

// sendEventIn 是 SendEvent opcode 携带的事件（id/ts 可缺省，平台补全）。
type sendEventIn struct {
	ID   string          `json:"id,omitempty"`
	Name string          `json:"name"`
	Data json.RawMessage `json:"data,omitempty"`
	TS   time.Time       `json:"ts,omitempty"`
}

// ---- 单次执行 ----

func (e *Executor) dispatch(ctx context.Context, item store.QueueItem) {
	run, err := e.Store.GetRun(ctx, item.RunID)
	if err != nil {
		slog.Error("get run failed", "component", "executor", "run_id", item.RunID, "err", err)
		e.Store.CompleteQueueItem(ctx, item.ID)
		return
	}
	if run.Status == store.RunCompleted || run.Status == store.RunFailed || run.Status == store.RunCancelled {
		e.Store.CompleteQueueItem(ctx, item.ID) // 终态 run 的残留队列项，清扫
		return
	}
	fn, ok := e.Reg.Lookup(run.FunctionID)
	if !ok {
		e.failRun(ctx, run, "function not registered: "+run.FunctionID)
		e.Store.CompleteQueueItem(ctx, item.ID)
		return
	}
	// 函数级重试覆盖（FR-4.2）：未配置用全局 MaxRetries。M0 语义：尝试总数上限。
	maxAttempts := fn.Retries
	if maxAttempts <= 0 {
		maxAttempts = e.MaxRetries
	}
	attempt, err := e.Store.IncrementRunAttempt(ctx, run.ID)
	if err != nil {
		slog.Error("increment attempt failed", "component", "executor", "run_id", run.ID, "function_id", run.FunctionID, "err", err)
		e.Store.ReleaseLease(ctx, item.ID, time.Now().Add(e.BaseBackoff))
		return
	}
	running, err := e.Store.MarkRunRunning(ctx, run.ID)
	if err != nil {
		slog.Error("mark run running failed", "component", "executor", "run_id", run.ID, "function_id", run.FunctionID, "err", err)
		e.Store.ReleaseLease(ctx, item.ID, time.Now().Add(e.BaseBackoff))
		return
	}
	if !running {
		// dispatch 启动期间 run 已到终态（如被取消）：放弃回调，收尾队列项。
		e.Store.CompleteQueueItem(ctx, item.ID)
		return
	}

	// sleep 唤醒转换：到达唤醒时刻的 dispatch 把 sleeping memo 置为 completed，
	// 使应用重入时 sleep step 命中 memo 直接返回（幂等；顺序执行下至多一个 sleeping step）
	if err := e.Store.CompleteSleepingSteps(ctx, run.ID); err != nil {
		e.retryOrFail(ctx, item, run, attempt, maxAttempts, "complete sleeping steps: "+err.Error())
		return
	}

	steps, err := e.Store.StepResults(ctx, run.ID)
	if err != nil {
		e.retryOrFail(ctx, item, run, attempt, maxAttempts, "load steps: "+err.Error())
		return
	}
	var req callbackRequest
	req.Ctx.RunID = run.ID
	req.Ctx.FunctionID = run.FunctionID
	req.Ctx.Attempt = attempt
	req.Ctx.Event = eventPayload{ID: run.EventID, Name: run.EventName, Data: run.EventData, TS: run.EventTS}
	req.Ctx.Steps = map[string]stepState{}
	for h, sr := range steps {
		if sr.Status == store.StepCompleted {
			req.Ctx.Steps[h] = stepState{ID: sr.StepID, Status: "completed", Output: sr.Output}
		}
	}

	body, _ := json.Marshal(req)
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, fn.AppURL, bytes.NewReader(body))
	if err != nil {
		e.retryOrFail(ctx, item, run, attempt, maxAttempts, err.Error())
		return
	}
	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set("X-TriggerLink-Signature", sign.Sign(e.SigningKey, body, time.Now()))

	started := time.Now()
	resp, err := e.Client.Do(httpReq)
	if err != nil {
		e.retryOrFail(ctx, item, run, attempt, maxAttempts, "callback: "+err.Error())
		return
	}
	defer resp.Body.Close()
	slog.Debug("callback returned", "component", "executor", "run_id", run.ID, "function_id", run.FunctionID,
		"attempt", attempt, "status", resp.StatusCode, "duration", time.Since(started))
	respBody, _ := io.ReadAll(io.LimitReader(resp.Body, 10<<20))
	if resp.StatusCode != http.StatusOK {
		e.retryOrFail(ctx, item, run, attempt, maxAttempts, fmt.Sprintf("app status %d: %s", resp.StatusCode, truncate(string(respBody), 200)))
		return
	}
	var op opcode
	if err := json.Unmarshal(respBody, &op); err != nil {
		e.retryOrFail(ctx, item, run, attempt, maxAttempts, "decode opcode: "+err.Error())
		return
	}

	// 回调在途期间 run 可能已到终态（取消、超时扫描置 Failed）：推进/重试前重检，
	// 终态则丢弃 opcode 收尾，避免把已终结的 run 继续往下推。
	if latest, err := e.Store.GetRun(ctx, run.ID); err == nil &&
		(latest.Status == store.RunCancelled || latest.Status == store.RunCompleted || latest.Status == store.RunFailed) {
		e.Store.CompleteQueueItem(ctx, item.ID)
		slog.Info("run reached terminal state in flight, discarding opcode", "component", "executor",
			"run_id", run.ID, "function_id", run.FunctionID, "status", latest.Status, "op", op.Op)
		return
	}

	switch op.Op {
	case "StepComplete":
		e.Store.SaveStepResult(ctx, store.StepResult{
			RunID: run.ID, StepHash: op.ID, StepID: op.StepID, Op: "Run",
			Status: store.StepCompleted, Output: op.Output, Attempt: attempt})
		e.Store.CompleteQueueItem(ctx, item.ID)
		e.enqueueNext(ctx, run, time.Now()) // 推进下一跳 re-invocation
	case "StepError":
		msg := opcodeErrMsg(op, "step error")
		e.Store.SaveStepResult(ctx, store.StepResult{
			RunID: run.ID, StepHash: op.ID, StepID: op.StepID, Op: "Run",
			Status: store.StepFailed, Error: msg, Attempt: attempt})
		e.retryOrFail(ctx, item, run, attempt, maxAttempts, msg)
	case "Sleep":
		if op.Until.IsZero() {
			e.retryOrFail(ctx, item, run, attempt, maxAttempts, "sleep opcode missing until")
			return
		}
		// 挂起：写 sleeping memo → 完成当前队列项 → 以 at=until 入队唤醒跳。
		// 挂起期间无连接/计算占用；平台重启后队列项落库自然续等。
		e.Store.SaveStepResult(ctx, store.StepResult{
			RunID: run.ID, StepHash: op.ID, StepID: op.StepID, Op: "Sleep",
			Status: store.StepSleeping, Attempt: attempt})
		e.Store.CompleteQueueItem(ctx, item.ID)
		e.enqueueNext(ctx, run, op.Until)
		slog.Debug("run sleeping", "component", "executor", "run_id", run.ID, "function_id", run.FunctionID, "step_id", op.StepID, "until", op.Until.Format(time.RFC3339))
	case "SendEvent":
		e.handleSendEvent(ctx, item, run, attempt, maxAttempts, op)
	case "WaitForEvent":
		e.handleWaitForEvent(ctx, item, run, attempt, maxAttempts, op)
	case "RunComplete":
		e.Store.CompleteRun(ctx, run.ID, op.Output)
		e.Store.CompleteQueueItem(ctx, item.ID)
		slog.Info("run completed", "component", "executor", "run_id", run.ID, "function_id", run.FunctionID,
			"attempt", attempt, "duration", time.Since(run.CreatedAt))
	case "RunError":
		e.retryOrFail(ctx, item, run, attempt, maxAttempts, opcodeErrMsg(op, "run error"))
	default:
		e.retryOrFail(ctx, item, run, attempt, maxAttempts, "unknown opcode: "+op.Op)
	}
}

// handleSendEvent 处理 SendEvent opcode（PRD FR-2.7：函数内可靠扇出）。
// 顺序承诺同 eventapi：先 InsertEvent 落库成功 → 再推入 stream → 写 memo → 推进下一跳。
// 缺省事件 ID 按 (run_id, step_hash, 序号) 确定性派生：崩溃窗口内重试同一 step 时
// 重发同 ID 事件，由 InsertEvent 幂等去重，不会重复扇出。
func (e *Executor) handleSendEvent(ctx context.Context, item store.QueueItem, run store.Run, attempt, maxAttempts int, op opcode) {
	if len(op.Events) == 0 {
		e.retryOrFail(ctx, item, run, attempt, maxAttempts, "send_event: no events")
		return
	}
	idsOut := make([]string, 0, len(op.Events))
	for i, in := range op.Events {
		if in.Name == "" {
			e.retryOrFail(ctx, item, run, attempt, maxAttempts, "send_event: event name required")
			return
		}
		if in.Data == nil {
			in.Data = json.RawMessage(`{}`)
		}
		if in.ID == "" {
			in.ID = derivedEventID(run.ID, op.ID, i)
		}
		if in.TS.IsZero() {
			in.TS = time.Now()
		}
		evt := store.Event{ID: in.ID, Name: in.Name, Data: in.Data, TS: in.TS}
		inserted, err := e.Store.InsertEvent(ctx, evt)
		if err != nil {
			e.retryOrFail(ctx, item, run, attempt, maxAttempts, "send_event: "+err.Error())
			return
		}
		if inserted && e.Stream != nil {
			e.Stream <- evt
		}
		idsOut = append(idsOut, in.ID)
	}
	out, _ := json.Marshal(idsOut)
	e.Store.SaveStepResult(ctx, store.StepResult{
		RunID: run.ID, StepHash: op.ID, StepID: op.StepID, Op: "SendEvent",
		Status: store.StepCompleted, Output: out, Attempt: attempt})
	e.Store.CompleteQueueItem(ctx, item.ID)
	e.enqueueNext(ctx, run, time.Now()) // 扇出后立即推进（不中断函数）
	slog.Debug("run sent events", "component", "executor", "run_id", run.ID, "function_id", run.FunctionID, "step_id", op.StepID, "count", len(idsOut))
}

// derivedEventID 为缺省 ID 的扇出事件派生确定性 ID（崩溃重试幂等的前提）。
func derivedEventID(runID, stepHash string, i int) string {
	sum := sha256.Sum256([]byte(runID + ":" + stepHash + ":" + strconv.Itoa(i)))
	return "evt_" + hex.EncodeToString(sum[:8])
}

// handleWaitForEvent 处理 WaitForEvent opcode（PRD 8.2 情况4 / FR-2.6）。
// 与 Sleep 的挂起形态相同，但唤醒不由队列驱动：写 waiting memo + wait 记录后不入队，
// 挂起期间零占用；事件命中（Runner 路由时）或超时（Runner 定时扫描）时由 Runner
// 补写 completed memo 并以 at=now 入队恢复。run 保持 Running（Waiting 是其子状态）。
// (run_id, step_hash) 幂等：崩溃窗口内重试同一 opcode 由 CreateWait 的 INSERT OR IGNORE 去重。
func (e *Executor) handleWaitForEvent(ctx context.Context, item store.QueueItem, run store.Run, attempt, maxAttempts int, op opcode) {
	if op.Event == "" {
		e.retryOrFail(ctx, item, run, attempt, maxAttempts, "wait_for_event: event name required")
		return
	}
	var expires *time.Time
	if op.Timeout != "" {
		d, err := time.ParseDuration(op.Timeout)
		if err != nil {
			e.retryOrFail(ctx, item, run, attempt, maxAttempts, "wait_for_event: invalid timeout: "+op.Timeout)
			return
		}
		t := time.Now().Add(d)
		expires = &t
	}
	e.Store.SaveStepResult(ctx, store.StepResult{
		RunID: run.ID, StepHash: op.ID, StepID: op.StepID, Op: "WaitForEvent",
		Status: store.StepWaiting, Attempt: attempt})
	if err := e.Store.CreateWait(ctx, store.Wait{
		ID: ids.NewID("wait"), RunID: run.ID, StepHash: op.ID, StepID: op.StepID,
		EventName: op.Event, MatchExpr: op.Match, ExpiresAt: expires, Status: store.WaitWaiting,
	}); err != nil {
		e.retryOrFail(ctx, item, run, attempt, maxAttempts, "wait_for_event: "+err.Error())
		return
	}
	e.Store.CompleteQueueItem(ctx, item.ID)
	slog.Debug("run waiting for event", "component", "executor", "run_id", run.ID, "function_id", run.FunctionID, "step_id", op.StepID, "event", op.Event)
}

// scanRunTimeouts 扫描 run 级超时（FR-4.3）：对注册了 Timeout 的函数，其在途 run
// 从 created_at 起超过 Timeout 仍在 Queued/Running → 走 failRun 语义置 Failed
// （TimeoutRun 事务内清理 pending 队列项；transitioned 后合成 triggerlink/run.failed 事件）。
func (e *Executor) scanRunTimeouts(ctx context.Context) {
	for _, fn := range e.Reg.All() {
		if fn.Timeout <= 0 {
			continue
		}
		runs, err := e.Store.ActiveRunsByFunction(ctx, fn.ID)
		if err != nil {
			slog.Error("timeout scan list runs failed", "component", "executor", "function_id", fn.ID, "err", err)
			continue
		}
		for _, run := range runs {
			if time.Since(run.CreatedAt) <= fn.Timeout {
				continue
			}
			msg := fmt.Sprintf("run timeout after %s", fn.Timeout)
			transitioned, err := e.Store.TimeoutRun(ctx, run.ID, msg)
			if err != nil {
				slog.Error("timeout run failed", "component", "executor", "run_id", run.ID, "function_id", run.FunctionID, "err", err)
				continue
			}
			if transitioned {
				e.emitRunFailed(ctx, run, msg)
				slog.Warn("run timed out", "component", "executor", "run_id", run.ID, "function_id", run.FunctionID, "timeout", fn.Timeout, "duration", time.Since(run.CreatedAt))
			}
		}
	}
}

func (e *Executor) retryOrFail(ctx context.Context, item store.QueueItem, run store.Run, attempt, maxAttempts int, msg string) {
	if attempt >= maxAttempts {
		e.failRun(ctx, run, msg)
		e.Store.CompleteQueueItem(ctx, item.ID)
		slog.Error("run failed", "component", "executor", "run_id", run.ID, "function_id", run.FunctionID,
			"attempt", attempt, "duration", time.Since(run.CreatedAt), "error", msg)
		return
	}
	slog.Warn("run attempt failed, retrying", "component", "executor", "run_id", run.ID, "function_id", run.FunctionID,
		"attempt", attempt, "max_attempts", maxAttempts, "backoff", e.backoff(attempt), "error", msg)
	e.Store.ReleaseLease(ctx, item.ID, time.Now().Add(e.backoff(attempt)))
}

// failRun 是 run 进入 Failed 终态的唯一出口（FR-2.11）：落终态后合成内部事件
// triggerlink/run.failed，供用户以普通函数（订阅该事件 + match function_id）做 onFailure 补偿。
func (e *Executor) failRun(ctx context.Context, run store.Run, errMsg string) {
	if err := e.Store.FailRun(ctx, run.ID, errMsg); err != nil {
		slog.Error("fail run failed", "component", "executor", "run_id", run.ID, "function_id", run.FunctionID, "err", err)
		return
	}
	e.emitRunFailed(ctx, run, errMsg)
}

// emitRunFailed 合成 run 失败的内部事件：id = runfailed_<run_id>（确定性，
// 崩溃重发由 InsertEvent 幂等去重），data 携带 run_id/function_id/error 与触发事件快照。
// Stream 为 nil 时仅落库（测试用）；落库失败只记日志（M2：不重发，run 终态本身不丢）。
func (e *Executor) emitRunFailed(ctx context.Context, run store.Run, errMsg string) {
	data, err := json.Marshal(map[string]any{
		"run_id":      run.ID,
		"function_id": run.FunctionID,
		"error":       errMsg,
		"event":       eventPayload{ID: run.EventID, Name: run.EventName, Data: run.EventData, TS: run.EventTS},
	})
	if err != nil {
		slog.Error("marshal run.failed event failed", "component", "executor", "run_id", run.ID, "function_id", run.FunctionID, "err", err)
		return
	}
	evt := store.Event{ID: "runfailed_" + run.ID, Name: "triggerlink/run.failed", Data: data, TS: time.Now()}
	inserted, err := e.Store.InsertEvent(ctx, evt)
	if err != nil {
		slog.Error("insert run.failed event failed", "component", "executor", "run_id", run.ID, "function_id", run.FunctionID, "err", err)
		return
	}
	if inserted && e.Stream != nil {
		e.Stream <- evt
	}
}

func (e *Executor) backoff(attempt int) time.Duration {
	d := e.BaseBackoff << attempt
	if d > 30*time.Second {
		d = 30 * time.Second
	}
	return d
}

func (e *Executor) enqueueNext(ctx context.Context, run store.Run, at time.Time) {
	if err := e.Store.Enqueue(ctx, store.QueueItem{
		ID:         ids.NewID("qi"),
		FunctionID: run.FunctionID,
		RunID:      run.ID,
		Score:      time.Now().UnixNano(),
		At:         at,
		Status:     store.QueuePending,
	}); err != nil {
		slog.Error("enqueue next hop failed", "component", "executor", "run_id", run.ID, "function_id", run.FunctionID, "err", err)
	}
}

func opcodeErrMsg(op opcode, fallback string) string {
	if op.Error != nil && op.Error.Message != "" {
		return op.Error.Message
	}
	return fallback
}

func truncate(s string, n int) string {
	if len(s) > n {
		return s[:n]
	}
	return s
}
