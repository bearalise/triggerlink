// Package metrics 是全平台指标的唯一注册点（FR-6.4）：集中定义 triggerlink_* 序列，
// 各组件只调用这里导出的记录函数，不自行注册 collector（避免重复注册 panic）。
//
// 注册表是显式的 prometheus.NewRegistry（非 DefaultRegisterer）：暴露面可控，
// 测试可直接 Gather 断言；Go runtime 与进程指标显式挂上，与默认注册表行为一致。
package metrics

import (
	"context"
	"log/slog"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// 事件路由结果（EventRouted 的 result 标签）。
// 一个事件对每个订阅函数产生一条结果；无任何函数订阅时记一条 no_subscriber。
const (
	ResultRouted       = "routed"        // 已建 run 并入队
	ResultNoSubscriber = "no_subscriber" // 没有函数订阅该事件名
	ResultPaused       = "paused"        // 函数被暂停（FR-7.3），跳过建 run
	ResultFiltered     = "filtered"      // 触发 match 表达式（FR-3.1）不命中
	ResultDebounced    = "debounced"     // 并入已有的防抖窗口（FR-4.5），未新建 run
)

// run 终态（RunFinished 的 status 标签）。
const (
	StatusCompleted = "completed"
	StatusFailed    = "failed"
	StatusCancelled = "cancelled"
	StatusTimeout   = "timeout"
)

var (
	reg = prometheus.NewRegistry()

	eventsReceived = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "triggerlink_events_received_total",
		Help: "Events accepted at an ingress (Event API + webhook endpoints), including duplicates.",
	})

	eventsRouted = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "triggerlink_events_routed_total",
		Help: "Routing decisions per (event, subscribing function); no_subscriber is counted once per unsubscribed event.",
	}, []string{"result"})

	runsTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "triggerlink_runs_total",
		Help: "Runs that reached a terminal state, by status.",
	}, []string{"status"})

	runDuration = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "triggerlink_run_duration_seconds",
		Help:    "Wall-clock time from run creation to terminal state (includes durable sleeps and waits).",
		Buckets: []float64{.1, .5, 1, 5, 10, 30, 60, 300, 900, 3600, 21600},
	})

	stepDuration = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name: "triggerlink_step_duration_seconds",
		// 各 op 的口径不同，见 StepObserved 的注释。
		Help:    "Step durations by op: Run/SendEvent = in-callback execution, Sleep = requested sleep length, WaitForEvent = wall-clock until resolved.",
		Buckets: []float64{.005, .025, .1, .5, 1, 5, 15, 60, 300, 1800, 21600},
	}, []string{"op"})

	callbackDuration = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "triggerlink_callback_duration_seconds",
		Help:    "Round-trip time of platform→app callbacks (one per run hop, successful or not).",
		Buckets: []float64{.005, .025, .1, .25, .5, 1, 2.5, 5, 10, 30, 60, 300},
	})

	runsThrottled = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "triggerlink_runs_throttled_total",
		Help: "Dispatch attempts deferred by throttle (FR-4.4); a run over the limit is counted once per deferral.",
	})

	queueDepth = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "triggerlink_queue_depth",
		Help: "Pending queue items already due (at <= now); sampled periodically, not on scrape.",
	})

	waitsActive = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "triggerlink_waits_active",
		Help: "step.waitForEvent suspensions in the waiting state; sampled periodically, not on scrape.",
	})
)

func init() {
	reg.MustRegister(
		eventsReceived, eventsRouted, runsTotal, runDuration,
		stepDuration, callbackDuration, runsThrottled, queueDepth, waitsActive,
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
	)
	// 标签值预置为 0：仪表盘/告警在首次事件之前就能拿到序列（rate() 不至于无数据）。
	for _, v := range []string{ResultRouted, ResultNoSubscriber, ResultPaused, ResultFiltered, ResultDebounced} {
		eventsRouted.WithLabelValues(v)
	}
	for _, v := range []string{StatusCompleted, StatusFailed, StatusCancelled, StatusTimeout} {
		runsTotal.WithLabelValues(v)
	}
}

// Handler 返回 /metrics 的 HTTP handler（Prometheus 文本格式）。
func Handler() http.Handler {
	return promhttp.HandlerFor(reg, promhttp.HandlerOpts{})
}

// Registry 返回内部注册表，供测试 Gather 断言。
func Registry() *prometheus.Registry { return reg }

// EventReceived 记录入口接收到的 n 个事件（含重复 ID：去重发生在存储层之后）。
func EventReceived(n int) { eventsReceived.Add(float64(n)) }

// EventRouted 记录一次路由结果（result 取 Result* 常量）。
func EventRouted(result string) { eventsRouted.WithLabelValues(result).Inc() }

// RunFinished 记录一个 run 进入终态（status 取 Status* 常量）及其总耗时。
func RunFinished(status string, d time.Duration) {
	runsTotal.WithLabelValues(status).Inc()
	runDuration.Observe(d.Seconds())
}

// StepObserved 记录一个 step 的耗时。口径按 op 区分：
//   - Run / SendEvent：应用回调内同步执行的时长（即该跳回调往返）。
//   - Sleep：挂起时的目标时长（until - now），不等回调。
//   - WaitForEvent：挂起到解除（事件命中或超时）的墙钟时长。
func StepObserved(op string, d time.Duration) {
	if d < 0 {
		d = 0
	}
	stepDuration.WithLabelValues(op).Observe(d.Seconds())
}

// RunThrottled 记录一次因限流被推迟的派发（FR-4.4）。
func RunThrottled() { runsThrottled.Inc() }

// CallbackObserved 记录一次平台→应用回调的往返耗时。
func CallbackObserved(d time.Duration) { callbackDuration.Observe(d.Seconds()) }

// SetQueueDepth / SetWaitsActive 设置周期采样的 gauge。
func SetQueueDepth(n int)  { queueDepth.Set(float64(n)) }
func SetWaitsActive(n int) { waitsActive.Set(float64(n)) }

// DepthSource 是采样器需要的存储查询（store.Store 天然满足）。
type DepthSource interface {
	QueueDepth(ctx context.Context) (int, error)
	ActiveWaitCount(ctx context.Context) (int, error)
}

// StartSampler 周期刷新 queue_depth / waits_active 两个 gauge，直到 ctx 取消。
// 刻意不挂在 promhttp 的查询路径上：抓取频率不该决定数据库负载。
// interval <= 0 时用默认 10s。阻塞运行，调用方自行 go。
func StartSampler(ctx context.Context, src DepthSource, interval time.Duration) {
	if interval <= 0 {
		interval = 10 * time.Second
	}
	tick := time.NewTicker(interval)
	defer tick.Stop()
	for {
		Sample(ctx, src)
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
		}
	}
}

// Sample 采样一次（与 ticker 解耦，便于测试直接驱动）。
func Sample(ctx context.Context, src DepthSource) {
	if n, err := src.QueueDepth(ctx); err != nil {
		slog.Warn("sample queue depth", "component", "metrics", "err", err)
	} else {
		SetQueueDepth(n)
	}
	if n, err := src.ActiveWaitCount(ctx); err != nil {
		slog.Warn("sample active waits", "component", "metrics", "err", err)
	} else {
		SetWaitsActive(n)
	}
}
