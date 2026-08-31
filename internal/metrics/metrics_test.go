package metrics

import (
	"context"
	"errors"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	dto "github.com/prometheus/client_model/go"
)

// 指标是包级全局：断言一律用"操作前后的增量"，不依赖测试执行顺序。
func delta(t *testing.T, name string, labels map[string]string, fn func()) float64 {
	t.Helper()
	before := gaugeOrCounter(t, name, labels)
	fn()
	return gaugeOrCounter(t, name, labels) - before
}

func gaugeOrCounter(t *testing.T, name string, labels map[string]string) float64 {
	t.Helper()
	mfs, err := reg.Gather()
	if err != nil {
		t.Fatalf("gather: %v", err)
	}
	for _, mf := range mfs {
		if mf.GetName() != name {
			continue
		}
		for _, m := range mf.GetMetric() {
			if !labelsMatch(m.GetLabel(), labels) {
				continue
			}
			switch {
			case m.Counter != nil:
				return m.Counter.GetValue()
			case m.Gauge != nil:
				return m.Gauge.GetValue()
			case m.Histogram != nil:
				return float64(m.Histogram.GetSampleCount())
			}
		}
	}
	return 0
}

func labelsMatch(pairs []*dtoLabel, want map[string]string) bool {
	for k, v := range want {
		found := false
		for _, p := range pairs {
			if p.GetName() == k && p.GetValue() == v {
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}
	return true
}

func TestCountersAndHistograms(t *testing.T) {
	if d := delta(t, "triggerlink_events_received_total", nil, func() { EventReceived(3) }); d != 3 {
		t.Fatalf("events_received delta=%v, want 3", d)
	}
	if d := delta(t, "triggerlink_events_routed_total", map[string]string{"result": ResultRouted},
		func() { EventRouted(ResultRouted) }); d != 1 {
		t.Fatalf("events_routed{routed} delta=%v, want 1", d)
	}
	// 其他 result 值不受影响
	if d := delta(t, "triggerlink_events_routed_total", map[string]string{"result": ResultPaused},
		func() { EventRouted(ResultRouted) }); d != 0 {
		t.Fatalf("events_routed{paused} delta=%v, want 0", d)
	}

	if d := delta(t, "triggerlink_runs_total", map[string]string{"status": StatusCompleted},
		func() { RunFinished(StatusCompleted, 2*time.Second) }); d != 1 {
		t.Fatalf("runs_total{completed} delta=%v, want 1", d)
	}
	// RunFinished 同时观测 run_duration（样本数 +1）
	if d := delta(t, "triggerlink_run_duration_seconds", nil,
		func() { RunFinished(StatusFailed, time.Second) }); d != 1 {
		t.Fatalf("run_duration samples delta=%v, want 1", d)
	}

	if d := delta(t, "triggerlink_step_duration_seconds", map[string]string{"op": "Run"},
		func() { StepObserved("Run", 50*time.Millisecond) }); d != 1 {
		t.Fatalf("step_duration{Run} delta=%v, want 1", d)
	}
	// 负时长（时钟回拨/已过期的 sleep）归零而非丢弃
	if d := delta(t, "triggerlink_step_duration_seconds", map[string]string{"op": "Sleep"},
		func() { StepObserved("Sleep", -time.Second) }); d != 1 {
		t.Fatalf("step_duration{Sleep} delta=%v, want 1", d)
	}
	if d := delta(t, "triggerlink_callback_duration_seconds", nil,
		func() { CallbackObserved(120 * time.Millisecond) }); d != 1 {
		t.Fatalf("callback_duration delta=%v, want 1", d)
	}
}

type fakeSource struct {
	depth, waits int
	err          error
}

func (f fakeSource) QueueDepth(context.Context) (int, error)      { return f.depth, f.err }
func (f fakeSource) ActiveWaitCount(context.Context) (int, error) { return f.waits, f.err }

func TestSampleSetsGauges(t *testing.T) {
	Sample(context.Background(), fakeSource{depth: 7, waits: 3})
	if got := testutil.ToFloat64(queueDepth); got != 7 {
		t.Fatalf("queue_depth=%v, want 7", got)
	}
	if got := testutil.ToFloat64(waitsActive); got != 3 {
		t.Fatalf("waits_active=%v, want 3", got)
	}
	// 查询失败：保留上一次采样值，不写 0（避免告警误报"积压清空"）
	Sample(context.Background(), fakeSource{depth: 99, waits: 99, err: errors.New("db down")})
	if got := testutil.ToFloat64(queueDepth); got != 7 {
		t.Fatalf("queue_depth after error=%v, want 7 (unchanged)", got)
	}
}

func TestStartSamplerStopsWithContext(t *testing.T) {
	Sample(context.Background(), fakeSource{depth: 0, waits: 0})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { StartSampler(ctx, fakeSource{depth: 5, waits: 2}, time.Hour); close(done) }()
	// 首次采样在进入 select 之前发生：无需等一个 tick
	deadline := time.After(2 * time.Second)
	for testutil.ToFloat64(queueDepth) != 5 {
		select {
		case <-deadline:
			t.Fatal("sampler did not sample before first tick")
		default:
			time.Sleep(5 * time.Millisecond)
		}
	}
	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("sampler did not stop on ctx cancel")
	}
}

func TestHandlerExposesSeries(t *testing.T) {
	EventReceived(1)
	RunFinished(StatusCompleted, time.Second)

	rec := httptest.NewRecorder()
	Handler().ServeHTTP(rec, httptest.NewRequest("GET", "/metrics", nil))
	if rec.Code != 200 {
		t.Fatalf("status=%d", rec.Code)
	}
	body := rec.Body.String()
	for _, want := range []string{
		"triggerlink_events_received_total",
		`triggerlink_events_routed_total{result="no_subscriber"}`,
		`triggerlink_runs_total{status="completed"}`,
		"triggerlink_run_duration_seconds_bucket",
		"triggerlink_step_duration_seconds_bucket",
		"triggerlink_callback_duration_seconds_bucket",
		"triggerlink_queue_depth",
		"triggerlink_waits_active",
		"go_goroutines", // Go runtime collector 已挂上自定义注册表
	} {
		if !strings.Contains(body, want) {
			t.Fatalf("missing series %q in /metrics output", want)
		}
	}
}

// dtoLabel 是 Gather 返回的标签对类型别名（仅测试用）。
type dtoLabel = dto.LabelPair
