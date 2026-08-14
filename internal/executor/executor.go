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
	"log"
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
	Store          store.Store
	Reg            *registry.Registry
	Client         *http.Client
	SigningKey     string
	Stream         chan<- store.Event // step.sendEvent 扇出用；nil 时仅落库不入流（测试用）
	MaxRetries     int                // 默认 4；M0：回调尝试总数上限
	Concurrency    int                // 每函数并发 run 上限，默认 100
	BatchSize      int                // 每轮租赁上限，默认 16
	PollInterval   time.Duration      // 默认 100ms
	LeaseDuration  time.Duration      // 默认 30s
	BaseBackoff    time.Duration      // 默认 1s；退避 = BaseBackoff << attempt，封顶 30s
}

// Run 阻塞运行直到 ctx 取消。启动先做租约对账（崩溃恢复）。
func (e *Executor) Run(ctx context.Context) error {
	e.defaults()
	if n, err := e.Store.RequeueExpiredLeases(ctx, time.Now()); err != nil {
		return err
	} else if n > 0 {
		log.Printf("executor: reconciled %d expired leases", n)
	}

	var mu sync.Mutex
	inFlight := map[string]int{}
	tick := time.NewTicker(e.PollInterval)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-tick.C:
		}
		items, err := e.Store.LeaseBatch(ctx, time.Now(), e.LeaseDuration, e.BatchSize)
		if err != nil {
			log.Printf("executor: lease: %v", err)
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
	Op     string          `json:"op"`
	ID     string          `json:"id,omitempty"`
	StepID string          `json:"step_id,omitempty"`
	Output json.RawMessage `json:"output,omitempty"`
	Error  *opError        `json:"error,omitempty"`
	Until  time.Time       `json:"until,omitempty"`  // Sleep opcode：唤醒时间（RFC3339）
	Events []sendEventIn   `json:"events,omitempty"` // SendEvent opcode：待扇出事件
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
		log.Printf("executor: get run %s: %v", item.RunID, err)
		e.Store.CompleteQueueItem(ctx, item.ID)
		return
	}
	if run.Status == store.RunCompleted || run.Status == store.RunFailed {
		e.Store.CompleteQueueItem(ctx, item.ID) // 终态 run 的残留队列项，清扫
		return
	}
	fn, ok := e.Reg.Lookup(run.FunctionID)
	if !ok {
		e.Store.FailRun(ctx, run.ID, "function not registered: "+run.FunctionID)
		e.Store.CompleteQueueItem(ctx, item.ID)
		return
	}
	attempt, err := e.Store.IncrementRunAttempt(ctx, run.ID)
	if err != nil {
		log.Printf("executor: attempt %s: %v", run.ID, err)
		e.Store.ReleaseLease(ctx, item.ID, time.Now().Add(e.BaseBackoff))
		return
	}
	e.Store.MarkRunRunning(ctx, run.ID)

	// sleep 唤醒转换：到达唤醒时刻的 dispatch 把 sleeping memo 置为 completed，
	// 使应用重入时 sleep step 命中 memo 直接返回（幂等；顺序执行下至多一个 sleeping step）
	if err := e.Store.CompleteSleepingSteps(ctx, run.ID); err != nil {
		e.retryOrFail(ctx, item, run, attempt, "complete sleeping steps: "+err.Error())
		return
	}

	steps, err := e.Store.StepResults(ctx, run.ID)
	if err != nil {
		e.retryOrFail(ctx, item, run, attempt, "load steps: "+err.Error())
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
		e.retryOrFail(ctx, item, run, attempt, err.Error())
		return
	}
	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set("X-TriggerLink-Signature", sign.Sign(e.SigningKey, body, time.Now()))

	resp, err := e.Client.Do(httpReq)
	if err != nil {
		e.retryOrFail(ctx, item, run, attempt, "callback: "+err.Error())
		return
	}
	defer resp.Body.Close()
	respBody, _ := io.ReadAll(io.LimitReader(resp.Body, 10<<20))
	if resp.StatusCode != http.StatusOK {
		e.retryOrFail(ctx, item, run, attempt, fmt.Sprintf("app status %d: %s", resp.StatusCode, truncate(string(respBody), 200)))
		return
	}
	var op opcode
	if err := json.Unmarshal(respBody, &op); err != nil {
		e.retryOrFail(ctx, item, run, attempt, "decode opcode: "+err.Error())
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
		e.retryOrFail(ctx, item, run, attempt, msg)
	case "Sleep":
		if op.Until.IsZero() {
			e.retryOrFail(ctx, item, run, attempt, "sleep opcode missing until")
			return
		}
		// 挂起：写 sleeping memo → 完成当前队列项 → 以 at=until 入队唤醒跳。
		// 挂起期间无连接/计算占用；平台重启后队列项落库自然续等。
		e.Store.SaveStepResult(ctx, store.StepResult{
			RunID: run.ID, StepHash: op.ID, StepID: op.StepID, Op: "Sleep",
			Status: store.StepSleeping, Attempt: attempt})
		e.Store.CompleteQueueItem(ctx, item.ID)
		e.enqueueNext(ctx, run, op.Until)
		log.Printf("executor: run %s sleeping until %s", run.ID, op.Until.Format(time.RFC3339))
	case "SendEvent":
		e.handleSendEvent(ctx, item, run, attempt, op)
	case "RunComplete":
		e.Store.CompleteRun(ctx, run.ID, op.Output)
		e.Store.CompleteQueueItem(ctx, item.ID)
		log.Printf("executor: run %s completed", run.ID)
	case "RunError":
		e.retryOrFail(ctx, item, run, attempt, opcodeErrMsg(op, "run error"))
	default:
		e.retryOrFail(ctx, item, run, attempt, "unknown opcode: "+op.Op)
	}
}

// handleSendEvent 处理 SendEvent opcode（PRD FR-2.7：函数内可靠扇出）。
// 顺序承诺同 eventapi：先 InsertEvent 落库成功 → 再推入 stream → 写 memo → 推进下一跳。
// 缺省事件 ID 按 (run_id, step_hash, 序号) 确定性派生：崩溃窗口内重试同一 step 时
// 重发同 ID 事件，由 InsertEvent 幂等去重，不会重复扇出。
func (e *Executor) handleSendEvent(ctx context.Context, item store.QueueItem, run store.Run, attempt int, op opcode) {
	if len(op.Events) == 0 {
		e.retryOrFail(ctx, item, run, attempt, "send_event: no events")
		return
	}
	idsOut := make([]string, 0, len(op.Events))
	for i, in := range op.Events {
		if in.Name == "" {
			e.retryOrFail(ctx, item, run, attempt, "send_event: event name required")
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
			e.retryOrFail(ctx, item, run, attempt, "send_event: "+err.Error())
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
	log.Printf("executor: run %s sent %d event(s)", run.ID, len(idsOut))
}

// derivedEventID 为缺省 ID 的扇出事件派生确定性 ID（崩溃重试幂等的前提）。
func derivedEventID(runID, stepHash string, i int) string {
	sum := sha256.Sum256([]byte(runID + ":" + stepHash + ":" + strconv.Itoa(i)))
	return "evt_" + hex.EncodeToString(sum[:8])
}

func (e *Executor) retryOrFail(ctx context.Context, item store.QueueItem, run store.Run, attempt int, msg string) {	if attempt >= e.MaxRetries {
		e.Store.FailRun(ctx, run.ID, msg)
		e.Store.CompleteQueueItem(ctx, item.ID)
		log.Printf("executor: run %s failed after %d attempts: %s", run.ID, attempt, msg)
		return
	}
	log.Printf("executor: run %s attempt %d failed, retrying: %s", run.ID, attempt, msg)
	e.Store.ReleaseLease(ctx, item.ID, time.Now().Add(e.backoff(attempt)))
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
		log.Printf("executor: enqueue next for run %s: %v", run.ID, err)
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
