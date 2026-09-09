// triggerlink M0：单进程服务器（Event API + Runner + Queue + Executor + SQLite）。
// 子命令：start（生产，密钥必填）、dev（本地开发，密钥缺省自动生成）；无子命令按 start 处理。
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/bearalise/triggerlink/internal/appapi"
	"github.com/bearalise/triggerlink/internal/auth"
	"github.com/bearalise/triggerlink/internal/dashapi"
	"github.com/bearalise/triggerlink/internal/dashboard"
	"github.com/bearalise/triggerlink/internal/eventapi"
	"github.com/bearalise/triggerlink/internal/executor"
	"github.com/bearalise/triggerlink/internal/ids"
	"github.com/bearalise/triggerlink/internal/janitor"
	"github.com/bearalise/triggerlink/internal/metrics"
	"github.com/bearalise/triggerlink/internal/registry"
	"github.com/bearalise/triggerlink/internal/runner"
	"github.com/bearalise/triggerlink/internal/scheduler"
	"github.com/bearalise/triggerlink/internal/store"
	"github.com/bearalise/triggerlink/internal/webhook"
)

// version 由 goreleaser 通过 -ldflags "-X main.version=..." 注入；本地构建为 dev。
var version = "dev"

// metricsSampleInterval 是 queue_depth / waits_active 两个 gauge 的采样周期。
const metricsSampleInterval = 10 * time.Second

// 保留策略（FR-6.6）：清理周期，以及保留期下限——事件去重窗口是 24h，
// 保留期短于它会把仍在去重窗口内的事件删掉，使重复投递不再被识别。
const (
	retentionInterval = time.Hour
	minRetentionDays  = 1
)

type stringSlice []string

func (s *stringSlice) String() string { return "" }
func (s *stringSlice) Set(v string) error {
	*s = append(*s, v)
	return nil
}

// fatal 记录 error 级日志后退出（替代 log.Fatal：保持结构化输出格式一致）。
func fatal(msg string, args ...any) {
	slog.Error(msg, args...)
	os.Exit(1)
}

func main() {
	// 子命令：triggerlink start（生产，密钥必填）/ triggerlink dev（本地开发，密钥缺省自动生成）。
	// 无子命令时按 start 处理（兼容旧的 flag 直传用法）。
	cmd := "start"
	args := os.Args[1:]
	if len(args) > 0 && args[0] == "version" {
		fmt.Printf("triggerlink %s\n", version)
		return
	}
	if len(args) > 0 && (args[0] == "start" || args[0] == "dev") {
		cmd, args = args[0], args[1:]
	}

	var addr, dbPath, eventKey, signingKey string
	var apps stringSlice
	var dashAuth, logLevel string
	var retentionDays int
	fs := flag.NewFlagSet("triggerlink "+cmd, flag.ExitOnError)
	fs.StringVar(&addr, "addr", ":8288", "Event API 监听地址")
	fs.StringVar(&dbPath, "db", "triggerlink.db", "存储位置：SQLite 文件路径，或 postgres://... 连接串（一个 flag 两种后端）")
	fs.StringVar(&eventKey, "event-key", "", "Event API 鉴权 key（start 必填，dev 缺省自动生成）")
	fs.StringVar(&signingKey, "signing-key", "", "平台→应用回调签名密钥（start 必填，dev 缺省自动生成）")
	fs.StringVar(&dashAuth, "dashboard-auth", "", "Dashboard basic auth 凭据 user:password（默认读 TRIGGERLINK_DASHBOARD_AUTH；都为空则生成随机密码打日志）")
	fs.StringVar(&logLevel, "log-level", "", "日志级别 debug|info|warn|error（默认 info；env TRIGGERLINK_LOG_LEVEL 兜底）")
	fs.IntVar(&retentionDays, "retention-days", 0, "历史数据保留天数（终态 run / 事件 / 已解除 wait），默认 30；0 表示读 env TRIGGERLINK_RETENTION_DAYS，负数表示关闭清理")
	fs.Var(&apps, "app", "应用 serve URL（可重复，如 http://localhost:8080/api/triggerlink）")
	fs.Parse(args)

	// 结构化日志（FR-6.3）：全局 slog 默认 handler，尽早设置——之后所有包的 slog 调用共用。
	level, badLevel := parseLogLevel(logLevel)
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: level})))
	if badLevel != "" {
		slog.Warn("unknown log level, falling back to info", "value", badLevel)
	}

	if cmd == "dev" {
		if eventKey == "" {
			eventKey = ids.NewID("evtkey")
			slog.Info("dev: generated event key", "event_key", eventKey)
		}
		if signingKey == "" {
			signingKey = ids.NewID("signkey")
			slog.Info("dev: generated signing key", "signing_key", signingKey)
		}
	}
	if eventKey == "" || signingKey == "" {
		fatal("必须提供 -event-key 与 -signing-key（或使用 triggerlink dev 自动生成）")
	}

	retention, err := resolveRetention(retentionDays)
	if err != nil {
		fatal("-retention-days", "err", err)
	}

	st, err := store.Open(dbPath)
	if err != nil {
		fatal("open store", "db", redactDBURI(dbPath), "err", err)
	}
	defer st.Close()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// 启动时先从库中恢复持久化的注册信息（动态注册/自注册的 app 重启不丢），
	// 再走 -app 静态内省；静态内省结果最新，覆盖同名 app 并重新落库。
	reg := registry.New()
	persist := func(ctx context.Context, appURL string, fns []registry.Function) error {
		rfns := make([]store.RegisteredFunction, 0, len(fns))
		for _, f := range fns {
			var cancelOn string
			if len(f.CancelOn) > 0 {
				b, _ := json.Marshal(f.CancelOn)
				cancelOn = string(b)
			}
			rfns = append(rfns, store.RegisteredFunction{
				ID: f.ID, AppURL: appURL, Event: f.Event, Match: f.Match, Cron: f.Cron,
				Retries: f.Retries, CancelOn: cancelOn, Timeout: f.Timeout,
				Debounce: marshalFlowControl(f.ID, "debounce", f.Debounce),
				Throttle: marshalFlowControl(f.ID, "throttle", f.Throttle),
				Batch:    marshalFlowControl(f.ID, "batch", f.Batch),
			})
		}
		return st.ReplaceAppFunctions(ctx, appURL, rfns)
	}
	if rfns, err := st.LoadRegisteredFunctions(ctx); err != nil {
		slog.Warn("load persisted registrations", "err", err)
	} else {
		byApp := map[string][]registry.Function{}
		for _, rf := range rfns {
			var cancelOn []registry.CancelRule
			if rf.CancelOn != "" {
				if err := json.Unmarshal([]byte(rf.CancelOn), &cancelOn); err != nil {
					slog.Warn("decode cancel_on", "function_id", rf.ID, "err", err)
				}
			}
			byApp[rf.AppURL] = append(byApp[rf.AppURL], registry.Function{
				ID: rf.ID, Event: rf.Event, Match: rf.Match, Cron: rf.Cron, Retries: rf.Retries,
				CancelOn: cancelOn, Timeout: rf.Timeout, AppURL: rf.AppURL,
				Debounce: unmarshalFlowControl[registry.Debounce](rf.ID, "debounce", rf.Debounce),
				Throttle: unmarshalFlowControl[registry.Throttle](rf.ID, "throttle", rf.Throttle),
				Batch:    unmarshalFlowControl[registry.Batch](rf.ID, "batch", rf.Batch),
			})
		}
		for appURL, fns := range byApp {
			reg.Sync(appURL, fns)
		}
		if len(byApp) > 0 {
			slog.Info("restored app registrations from db", "apps", len(byApp))
		}
	}

	// 内省所有 -app 配置的应用，构建函数注册表
	introspectClient := &http.Client{Timeout: 10 * time.Second}
	for _, app := range apps {
		fns, err := registry.Fetch(ctx, introspectClient, app, signingKey)
		if err != nil {
			slog.Warn("introspect app", "app_url", app, "err", err)
			continue
		}
		reg.Sync(app, fns)
		if err := persist(ctx, app, fns); err != nil {
			slog.Warn("persist registration", "app_url", app, "err", err)
		}
		for _, f := range fns {
			slog.Info("registered function", "function_id", f.ID, "event", f.Event, "app_url", app)
		}
	}

	stream := make(chan store.Event, 1024)
	rnr := &runner.Runner{Store: st, Reg: reg, Stream: stream}
	exec := &executor.Executor{Store: st, Reg: reg, SigningKey: signingKey, Stream: stream}
	sched := &scheduler.Scheduler{Store: st, Reg: reg}
	go func() {
		if err := rnr.Run(ctx); err != nil {
			slog.Error("runner exited", "err", err)
		}
	}()
	go func() {
		if err := exec.Run(ctx); err != nil {
			slog.Error("executor exited", "err", err)
		}
	}()
	go func() {
		if err := sched.Run(ctx); err != nil {
			slog.Error("scheduler exited", "err", err)
		}
	}()
	// 队列深度 / 活跃 wait 数按固定周期采样（不挂在 /metrics 抓取路径上，FR-6.4）。
	go metrics.StartSampler(ctx, st, metricsSampleInterval)
	jan := &janitor.Janitor{Store: st, Retention: retention, Interval: retentionInterval}
	go func() {
		if err := jan.Run(ctx); err != nil {
			slog.Error("janitor exited", "err", err)
		}
	}()

	mux := http.NewServeMux()
	mux.Handle("/v1/events", eventapi.NewHandler(st, eventKey, stream))
	admin := appapi.NewHandler(reg, eventKey, signingKey, introspectClient)
	admin.Persist = persist
	mux.Handle("/api/v1/apps", admin)
	mux.Handle("/api/v1/apps/sync", admin)
	dashUser, dashPass := resolveDashAuth(dashAuth)
	dash := dashapi.NewHandler(st, reg)
	secure := func(h http.Handler) http.Handler { return auth.Basic(dashUser, dashPass, h) }
	mux.Handle("/api/v1/events", secure(dash))
	mux.Handle("/api/v1/functions", secure(dash))
	mux.Handle("/api/v1/runs", secure(dash))
	mux.Handle("/api/v1/runs/", secure(dash))
	mux.Handle("/api/v1/webhooks", secure(dash))
	mux.Handle("/api/v1/webhooks/", secure(dash))
	// webhook 接收端点：公开路径（不套 basic auth），安全完全依赖 HMAC 验签（FR-3.4）
	mux.Handle("/hooks/", webhook.NewHandler(st, stream))
	// Prometheus 抓取端点：按惯例公开（不套 basic auth）；公网部署应在反代层限制来源。
	mux.Handle("/metrics", metrics.Handler())
	mux.Handle("/dashboard/", secure(dashboard.Handler()))
	srv := &http.Server{Addr: addr, Handler: mux, ReadHeaderTimeout: 10 * time.Second}
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		srv.Shutdown(shutdownCtx)
	}()

	slog.Info("triggerlink listening", "version", version, "mode", cmd, "addr", addr,
		"backend", st.Backend(), "apps", len(apps), "retention", retention)
	if err := srv.ListenAndServe(); err != http.ErrServerClosed {
		fatal("http server", "err", err)
	}
}

// parseLogLevel 解析日志级别：flag 优先，env TRIGGERLINK_LOG_LEVEL 兜底，缺省 info。
// 非法值退回 info，并把原值作为第二返回值交给调用方告警（此时 handler 尚未装好，不能直接打日志）。
func parseLogLevel(flagVal string) (slog.Level, string) {
	v := flagVal
	if v == "" {
		v = os.Getenv("TRIGGERLINK_LOG_LEVEL")
	}
	switch strings.ToLower(strings.TrimSpace(v)) {
	case "":
		return slog.LevelInfo, ""
	case "debug":
		return slog.LevelDebug, ""
	case "info":
		return slog.LevelInfo, ""
	case "warn", "warning":
		return slog.LevelWarn, ""
	case "error":
		return slog.LevelError, ""
	default:
		return slog.LevelInfo, v
	}
}

// marshalFlowControl / unmarshalFlowControl 在注册表的流控配置与其落库 JSON 之间
// 转换（FR-4.4/4.5/4.7）。两侧用同一套 Marshal/UnmarshalJSON，落库形态与清单形态一致。
// 转换失败只记日志并视为未配置：一条坏记录不该让平台起不来或整个 app 恢复不了。
func marshalFlowControl[T any](functionID, field string, v *T) string {
	if v == nil {
		return ""
	}
	b, err := json.Marshal(v)
	if err != nil {
		slog.Warn("encode flow-control config", "function_id", functionID, "field", field, "err", err)
		return ""
	}
	return string(b)
}

func unmarshalFlowControl[T any](functionID, field, raw string) *T {
	if raw == "" {
		return nil
	}
	var v T
	if err := json.Unmarshal([]byte(raw), &v); err != nil {
		slog.Warn("decode flow-control config", "function_id", functionID, "field", field, "err", err)
		return nil
	}
	return &v
}

// redactDBURI 抹掉连接串里的口令再进日志——Postgres 的 -db 值形如
// postgres://user:password@host/db，原样打印等于把口令写进日志文件。
func redactDBURI(uri string) string {
	i := strings.Index(uri, "://")
	if i < 0 {
		return uri // SQLite 文件路径，无凭据
	}
	rest := uri[i+3:]
	at := strings.Index(rest, "@")
	if at < 0 {
		return uri
	}
	cred := rest[:at]
	if c := strings.Index(cred, ":"); c >= 0 {
		cred = cred[:c] + ":***"
	}
	return uri[:i+3] + cred + rest[at:]
}

// resolveRetention 解析保留期（FR-6.6）：flag 优先，env TRIGGERLINK_RETENTION_DAYS 兜底，
// 都未设时默认 30 天；负数表示显式关闭清理（返回 0）。
// 正数但小于事件去重窗口（24h）时直接报错而非静默放宽：那会让去重窗口内的事件被删，
// 重复投递不再被识别——属于配置错误，不该在运行时才暴露。
func resolveRetention(flagDays int) (time.Duration, error) {
	days := flagDays
	if days == 0 {
		v := strings.TrimSpace(os.Getenv("TRIGGERLINK_RETENTION_DAYS"))
		if v == "" {
			days = 30
		} else {
			n, err := strconv.Atoi(v)
			if err != nil {
				return 0, fmt.Errorf("TRIGGERLINK_RETENTION_DAYS=%q: 需要整数天数", v)
			}
			days = n
		}
	}
	if days < 0 {
		return 0, nil // 显式关闭
	}
	if days < minRetentionDays {
		return 0, fmt.Errorf("保留期 %d 天小于事件去重窗口（%d 天）；用负数显式关闭清理", days, minRetentionDays)
	}
	return time.Duration(days) * 24 * time.Hour, nil
}

// resolveDashAuth 解析 Dashboard 凭据：flag 优先，env 兜底，都为空则生成随机密码。
func resolveDashAuth(flagVal string) (string, string) {
	v := flagVal
	if v == "" {
		v = os.Getenv("TRIGGERLINK_DASHBOARD_AUTH")
	}
	if v == "" {
		pass := ids.NewID("dash")
		slog.Warn("dashboard auth 未配置，生成随机凭据（仅适合内网/本地使用）", "user", "admin", "password", pass)
		return "admin", pass
	}
	user, pass, err := auth.ParseCreds(v)
	if err != nil {
		fatal("-dashboard-auth", "err", err)
	}
	return user, pass
}
