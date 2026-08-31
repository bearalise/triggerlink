package runner

import (
	"context"
	"encoding/json"
	"path/filepath"
	"testing"
	"time"

	"github.com/bearalise/triggerlink/internal/registry"
	"github.com/bearalise/triggerlink/internal/store"
)

func setup(t *testing.T) (*Runner, store.Store, chan store.Event) {
	t.Helper()
	st, err := store.Open(filepath.Join(t.TempDir(), "test.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() })
	reg := registry.New()
	reg.Register(registry.Function{ID: "process-doc", Event: "doc/uploaded", AppURL: "http://app/serve"})
	stream := make(chan store.Event, 16)
	return &Runner{Store: st, Reg: reg, Stream: stream}, st, stream
}

func waitRuns(t *testing.T, st store.Store, want int) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if n, _ := st.CountRuns(context.Background()); n == want {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	n, _ := st.CountRuns(context.Background())
	t.Fatalf("runs=%d, want %d", n, want)
}

// waitRouted 等待所有已落库事件完成路由标记（routed_at 已提交）。
func waitRouted(t *testing.T, st store.Store) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if evts, _ := st.UnroutedEvents(context.Background()); len(evts) == 0 {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("events still unrouted")
}

func TestRoutesStreamEvent(t *testing.T) {
	r, st, stream := setup(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go r.Run(ctx)

	stream <- store.Event{ID: "evt_1", Name: "doc/uploaded",
		Data: json.RawMessage(`{"doc_id":"d1"}`), TS: time.Now()}
	waitRuns(t, st, 1)

	// 事件无订阅者：也标记 routed，不产生 run
	stream <- store.Event{ID: "evt_2", Name: "no/subscribers", Data: json.RawMessage(`{}`), TS: time.Now()}
	waitRuns(t, st, 1)
}

// TestDoubleRouteIdempotent：同一事件被路由两次（启动对账与 stream 消费重叠的竞态）
// 只产生一个 run 且只入队一次——run ID 由 (event_id, function_id) 确定性派生，
// 第二次 CreateRun 幂等落空。
func TestDoubleRouteIdempotent(t *testing.T) {
	r, st, _ := setup(t)
	ctx := context.Background()
	e := store.Event{ID: "evt_dup", Name: "doc/uploaded",
		Data: json.RawMessage(`{"doc_id":"d1"}`), TS: time.Now()}
	if err := r.route(ctx, e); err != nil {
		t.Fatal(err)
	}
	if err := r.route(ctx, e); err != nil { // 模拟双路由
		t.Fatal(err)
	}
	if n, _ := st.CountRuns(ctx); n != 1 {
		t.Fatalf("runs=%d, want 1", n)
	}
	items, err := st.LeaseBatch(ctx, time.Now(), time.Minute, 16)
	if err != nil {
		t.Fatal(err)
	}
	if len(items) != 1 {
		t.Fatalf("queue items=%d, want 1", len(items))
	}
}

// TestDelayedEventNotLeasedBeforeTS：ts 为未来时间的延迟事件，到点前队列项不可租赁（FR-1.6）。
func TestDelayedEventNotLeasedBeforeTS(t *testing.T) {
	r, st, stream := setup(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go r.Run(ctx)

	future := time.Now().Add(time.Hour)
	stream <- store.Event{ID: "evt_delay", Name: "doc/uploaded",
		Data: json.RawMessage(`{}`), TS: future}
	waitRuns(t, st, 1)

	// 到点前：LeaseBatch 取不到
	items, err := st.LeaseBatch(ctx, time.Now(), time.Minute, 4)
	if err != nil {
		t.Fatal(err)
	}
	if len(items) != 0 {
		t.Fatalf("leased %d items before ts, want 0", len(items))
	}
	// 到点后：可租赁
	items, err = st.LeaseBatch(ctx, future, time.Minute, 4)
	if err != nil {
		t.Fatal(err)
	}
	if len(items) != 1 {
		t.Fatalf("leased %d items at ts, want 1", len(items))
	}
}

func TestStartupReconciliation(t *testing.T) {
	r, st, _ := setup(t)
	// 直接落库、不走 stream —— 模拟"已接收但崩溃前未路由"
	if _, err := st.InsertEvent(context.Background(), store.Event{
		ID: "evt_orphan", Name: "doc/uploaded", Data: json.RawMessage(`{}`), TS: time.Now()}); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go r.Run(ctx)
	waitRuns(t, st, 1) // 对账扫描捡起孤儿事件并创建 run

	// run 落库先于 MarkEventRouted（route 内顺序提交），须等路由标记提交后
	// 再启动第二个 Runner，否则其启动对账会把"标记在途"的事件重复路由
	waitRouted(t, st)

	// 再启动一个 Runner：事件已 routed，不得重复建 run
	r2 := &Runner{Store: st, Reg: r.Reg, Stream: make(chan store.Event, 1)}
	go r2.Run(ctx)
	time.Sleep(200 * time.Millisecond)
	if n, _ := st.CountRuns(context.Background()); n != 1 {
		t.Fatalf("duplicate routing, runs=%d", n)
	}
}

// ---- step.waitForEvent 与 cancelOn（FR-2.6 / FR-4.9）----

// seedWaitingRun 造一个 Running run + waiting 状态的 wait（模拟 executor 已处理 WaitForEvent opcode）。
func seedWaitingRun(t *testing.T, st store.Store, runID, fnID, eventName, match string, expires *time.Time) store.Wait {
	t.Helper()
	ctx := context.Background()
	if _, err := st.CreateRun(ctx, store.Run{ID: runID, FunctionID: fnID, Status: store.RunQueued,
		EventID: "evt_trig", EventName: "order/created",
		EventData: json.RawMessage(`{"order_id":"o1"}`), EventTS: time.Now()}); err != nil {
		t.Fatal(err)
	}
	if _, err := st.MarkRunRunning(ctx, runID); err != nil {
		t.Fatal(err)
	}
	w := store.Wait{ID: "wait_" + runID, RunID: runID, StepHash: "h_wait", StepID: "wait-payed",
		EventName: eventName, MatchExpr: match, ExpiresAt: expires, Status: store.WaitWaiting}
	if err := st.CreateWait(ctx, w); err != nil {
		t.Fatal(err)
	}
	return w
}

// waitStepStatus 轮询直到指定 step memo 到达目标状态。
func waitStepStatus(t *testing.T, st store.Store, runID, hash, want string) store.StepResult {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		steps, _ := st.StepResults(context.Background(), runID)
		if sr, ok := steps[hash]; ok && sr.Status == want {
			return sr
		}
		time.Sleep(10 * time.Millisecond)
	}
	steps, _ := st.StepResults(context.Background(), runID)
	t.Fatalf("step %s/%s never reached %s: %+v", runID, hash, want, steps)
	return store.StepResult{}
}

// TestWaitResumedByEvent：事件到达且 match 命中 → memo completed（output=事件 JSON）、wait resolved、run 入队恢复。
func TestWaitResumedByEvent(t *testing.T) {
	r, st, stream := setup(t)
	seedWaitingRun(t, st, "run_w", "process-doc", "order/payed", `data.order_id == event.data.order_id`, nil)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go r.Run(ctx)

	ts := time.Now()
	stream <- store.Event{ID: "evt_pay", Name: "order/payed",
		Data: json.RawMessage(`{"order_id":"o1"}`), TS: ts}

	sr := waitStepStatus(t, st, "run_w", "h_wait", store.StepCompleted)
	var out map[string]json.RawMessage
	if err := json.Unmarshal(sr.Output, &out); err != nil {
		t.Fatalf("output=%s not an object: %v", sr.Output, err)
	}
	var id, name string
	json.Unmarshal(out["id"], &id)
	json.Unmarshal(out["name"], &name)
	if id != "evt_pay" || name != "order/payed" || string(out["data"]) != `{"order_id":"o1"}` || out["ts"] == nil {
		t.Fatalf("output fields: %s", sr.Output)
	}
	// wait 已 resolved（不再出现在 waiting 集合），恢复跳已入队可租赁
	waits, _ := st.WaitingWaitsForEvent(ctx, "order/payed")
	if len(waits) != 0 {
		t.Fatalf("wait still waiting: %+v", waits)
	}
	items, err := st.LeaseBatch(ctx, time.Now(), time.Minute, 4)
	if err != nil || len(items) != 1 || items[0].RunID != "run_w" {
		t.Fatalf("resume queue item: %+v err=%v", items, err)
	}
}

// TestWaitMatchMiss：match 不命中 → 不恢复；空 match → 全部命中。
func TestWaitMatchMiss(t *testing.T) {
	r, st, stream := setup(t)
	seedWaitingRun(t, st, "run_miss", "process-doc", "order/payed", `data.order_id == "other"`, nil)
	seedWaitingRun(t, st, "run_nomatch", "process-doc", "order/payed", "", nil)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go r.Run(ctx)

	stream <- store.Event{ID: "evt_pay2", Name: "order/payed",
		Data: json.RawMessage(`{"order_id":"o1"}`), TS: time.Now()}

	// 空 match 的 run_nomatch 必须恢复
	waitStepStatus(t, st, "run_nomatch", "h_wait", store.StepCompleted)
	// match 不命中的 run_miss 保持等待
	waits, _ := st.WaitingWaitsForEvent(ctx, "order/payed")
	if len(waits) != 1 || waits[0].RunID != "run_miss" {
		t.Fatalf("waits: %+v", waits)
	}
	steps, _ := st.StepResults(ctx, "run_miss")
	if len(steps) != 0 {
		t.Fatalf("missed run must not have memo: %+v", steps)
	}
}

// TestWaitTerminalRunNotResumed：终态 run 不恢复，wait 置 cancelled 清扫。
func TestWaitTerminalRunNotResumed(t *testing.T) {
	r, st, stream := setup(t)
	seedWaitingRun(t, st, "run_term", "process-doc", "order/payed", "", nil)
	if ok, err := st.CancelRun(context.Background(), "run_term"); err != nil || !ok {
		t.Fatalf("cancel: ok=%v err=%v", ok, err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go r.Run(ctx)

	stream <- store.Event{ID: "evt_pay3", Name: "order/payed", Data: json.RawMessage(`{}`), TS: time.Now()}
	waitRouted(t, st)
	time.Sleep(200 * time.Millisecond)

	steps, _ := st.StepResults(ctx, "run_term")
	if len(steps) != 0 {
		t.Fatalf("terminal run resumed: %+v", steps)
	}
	waits, _ := st.WaitingWaitsForEvent(ctx, "order/payed")
	if len(waits) != 0 { // wait 已离开 waiting（置 cancelled）
		t.Fatalf("wait still waiting: %+v", waits)
	}
	items, _ := st.LeaseBatch(ctx, time.Now(), time.Minute, 4)
	if len(items) != 0 {
		t.Fatalf("terminal run enqueued: %+v", items)
	}
}

// TestWaitTimeoutScan：过期的 waiting wait 由定时扫描恢复，output = JSON null。
func TestWaitTimeoutScan(t *testing.T) {
	r, st, _ := setup(t)
	r.PollInterval = 20 * time.Millisecond
	past := time.Now().Add(-time.Minute)
	seedWaitingRun(t, st, "run_to", "process-doc", "order/payed", "", &past)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go r.Run(ctx)

	sr := waitStepStatus(t, st, "run_to", "h_wait", store.StepCompleted)
	if string(sr.Output) != "null" {
		t.Fatalf("timeout output=%s, want null", sr.Output)
	}
	waits, _ := st.WaitingWaitsForEvent(ctx, "order/payed")
	if len(waits) != 0 {
		t.Fatalf("wait still waiting after timeout: %+v", waits)
	}
	items, _ := st.LeaseBatch(ctx, time.Now(), time.Minute, 4)
	if len(items) != 1 || items[0].RunID != "run_to" {
		t.Fatalf("resume queue item: %+v", items)
	}
}

// TestCancelOnCancelsActiveRun：cancelOn match 命中 → 在途 run 取消、waiting wait 清扫；
// 不命中 → run 保持。
func TestCancelOnCancelsActiveRun(t *testing.T) {
	st, err := store.Open(filepath.Join(t.TempDir(), "test.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() })
	reg := registry.New()
	reg.Register(registry.Function{ID: "fn-cancel", Event: "order/created", AppURL: "http://app/serve",
		CancelOn: []registry.CancelRule{{Event: "order/cancelled", Match: `data.order_id == event.data.order_id`}}})
	stream := make(chan store.Event, 16)
	r := &Runner{Store: st, Reg: reg, Stream: stream}

	// 两个在途 run：o1（将被命中）、o2（match 不命中）；o1 还有一个挂起的 wait
	seedWaitingRun(t, st, "run_o1", "fn-cancel", "order/payed", "", nil)
	if _, err := st.CreateRun(context.Background(), store.Run{ID: "run_o2", FunctionID: "fn-cancel", Status: store.RunQueued,
		EventID: "evt_t2", EventName: "order/created",
		EventData: json.RawMessage(`{"order_id":"o2"}`), EventTS: time.Now()}); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go r.Run(ctx)

	stream <- store.Event{ID: "evt_cancel", Name: "order/cancelled",
		Data: json.RawMessage(`{"order_id":"o1"}`), TS: time.Now()}
	waitRouted(t, st)

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		run, _ := st.GetRun(ctx, "run_o1")
		if run.Status == store.RunCancelled {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	run1, _ := st.GetRun(ctx, "run_o1")
	if run1.Status != store.RunCancelled {
		t.Fatalf("run_o1 status=%s, want Cancelled", run1.Status)
	}
	run2, _ := st.GetRun(ctx, "run_o2")
	if run2.Status == store.RunCancelled {
		t.Fatalf("run_o2 must not be cancelled (match miss)")
	}
	// 被取消 run 的 waiting wait 已清扫
	waits, _ := st.WaitingWaitsForEvent(ctx, "order/payed")
	if len(waits) != 0 {
		t.Fatalf("waits of cancelled run: %+v", waits)
	}
}

// TestPausedFunctionNotRouted：暂停的函数不接新触发（FR-7.3）——路由时跳过建 run，
// 但事件仍照常标记 routed；恢复后新事件正常建 run。
func TestPausedFunctionNotRouted(t *testing.T) {
	r, st, _ := setup(t)
	ctx := context.Background()
	if err := st.SetFunctionPaused(ctx, "process-doc", true); err != nil {
		t.Fatal(err)
	}

	e := store.Event{ID: "evt_p1", Name: "doc/uploaded", Data: json.RawMessage(`{}`), TS: time.Now()}
	if _, err := st.InsertEvent(ctx, e); err != nil {
		t.Fatal(err)
	}
	if err := r.route(ctx, e); err != nil {
		t.Fatal(err)
	}
	if n, _ := st.CountRuns(ctx); n != 0 {
		t.Fatalf("paused function routed, runs=%d, want 0", n)
	}
	// 事件仍标记 routed（不对账重扫）
	if evts, _ := st.UnroutedEvents(ctx); len(evts) != 0 {
		t.Fatalf("event not marked routed: %+v", evts)
	}

	// 恢复后：新事件正常建 run
	if err := st.SetFunctionPaused(ctx, "process-doc", false); err != nil {
		t.Fatal(err)
	}
	e2 := store.Event{ID: "evt_p2", Name: "doc/uploaded", Data: json.RawMessage(`{}`), TS: time.Now()}
	if _, err := st.InsertEvent(ctx, e2); err != nil {
		t.Fatal(err)
	}
	if err := r.route(ctx, e2); err != nil {
		t.Fatal(err)
	}
	if n, _ := st.CountRuns(ctx); n != 1 {
		t.Fatalf("resumed function not routed, runs=%d, want 1", n)
	}
}
