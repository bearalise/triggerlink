// triggerlink M0：单进程服务器（Event API + Runner + Queue + Executor + SQLite）。
// 子命令：start（生产，密钥必填）、dev（本地开发，密钥缺省自动生成）；无子命令按 start 处理。
package main

import (
	"context"
	"encoding/json"
	"flag"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/bearalise/triggerlink/internal/appapi"
	"github.com/bearalise/triggerlink/internal/auth"
	"github.com/bearalise/triggerlink/internal/dashapi"
	"github.com/bearalise/triggerlink/internal/dashboard"
	"github.com/bearalise/triggerlink/internal/eventapi"
	"github.com/bearalise/triggerlink/internal/executor"
	"github.com/bearalise/triggerlink/internal/ids"
	"github.com/bearalise/triggerlink/internal/registry"
	"github.com/bearalise/triggerlink/internal/runner"
	"github.com/bearalise/triggerlink/internal/store"
)

// version 由 goreleaser 通过 -ldflags "-X main.version=..." 注入；本地构建为 dev。
var version = "dev"

type stringSlice []string

func (s *stringSlice) String() string { return "" }
func (s *stringSlice) Set(v string) error {
	*s = append(*s, v)
	return nil
}

func main() {
	// 子命令：triggerlink start（生产，密钥必填）/ triggerlink dev（本地开发，密钥缺省自动生成）。
	// 无子命令时按 start 处理（兼容旧的 flag 直传用法）。
	cmd := "start"
	args := os.Args[1:]
	if len(args) > 0 && args[0] == "version" {
		log.Printf("triggerlink %s", version)
		return
	}
	if len(args) > 0 && (args[0] == "start" || args[0] == "dev") {
		cmd, args = args[0], args[1:]
	}

	var addr, dbPath, eventKey, signingKey string
	var apps stringSlice
	var dashAuth string
	fs := flag.NewFlagSet("triggerlink "+cmd, flag.ExitOnError)
	fs.StringVar(&addr, "addr", ":8288", "Event API 监听地址")
	fs.StringVar(&dbPath, "db", "triggerlink.db", "SQLite 数据库路径")
	fs.StringVar(&eventKey, "event-key", "", "Event API 鉴权 key（start 必填，dev 缺省自动生成）")
	fs.StringVar(&signingKey, "signing-key", "", "平台→应用回调签名密钥（start 必填，dev 缺省自动生成）")
	fs.StringVar(&dashAuth, "dashboard-auth", "", "Dashboard basic auth 凭据 user:password（默认读 TRIGGERLINK_DASHBOARD_AUTH；都为空则生成随机密码打日志）")
	fs.Var(&apps, "app", "应用 serve URL（可重复，如 http://localhost:8080/api/triggerlink）")
	fs.Parse(args)

	if cmd == "dev" {
		if eventKey == "" {
			eventKey = ids.NewID("evtkey")
			log.Printf("dev: 自动生成 event-key=%s", eventKey)
		}
		if signingKey == "" {
			signingKey = ids.NewID("signkey")
			log.Printf("dev: 自动生成 signing-key=%s", signingKey)
		}
	}
	if eventKey == "" || signingKey == "" {
		log.Fatal("必须提供 -event-key 与 -signing-key（或使用 triggerlink dev 自动生成）")
	}

	st, err := store.Open(dbPath)
	if err != nil {
		log.Fatalf("open store: %v", err)
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
			rfns = append(rfns, store.RegisteredFunction{ID: f.ID, AppURL: appURL, Event: f.Event, Retries: f.Retries, CancelOn: cancelOn})
		}
		return st.ReplaceAppFunctions(ctx, appURL, rfns)
	}
	if rfns, err := st.LoadRegisteredFunctions(ctx); err != nil {
		log.Printf("warn: load persisted registrations: %v", err)
	} else {
		byApp := map[string][]registry.Function{}
		for _, rf := range rfns {
			var cancelOn []registry.CancelRule
			if rf.CancelOn != "" {
				if err := json.Unmarshal([]byte(rf.CancelOn), &cancelOn); err != nil {
					log.Printf("warn: decode cancel_on of function %s: %v", rf.ID, err)
				}
			}
			byApp[rf.AppURL] = append(byApp[rf.AppURL],
				registry.Function{ID: rf.ID, Event: rf.Event, Retries: rf.Retries, CancelOn: cancelOn, AppURL: rf.AppURL})
		}
		for appURL, fns := range byApp {
			reg.Sync(appURL, fns)
		}
		if len(byApp) > 0 {
			log.Printf("restored %d app registration(s) from db", len(byApp))
		}
	}

	// 内省所有 -app 配置的应用，构建函数注册表
	introspectClient := &http.Client{Timeout: 10 * time.Second}
	for _, app := range apps {
		fns, err := registry.Fetch(ctx, introspectClient, app, signingKey)
		if err != nil {
			log.Printf("warn: introspect %s: %v", app, err)
			continue
		}
		reg.Sync(app, fns)
		if err := persist(ctx, app, fns); err != nil {
			log.Printf("warn: persist registration %s: %v", app, err)
		}
		for _, f := range fns {
			log.Printf("registered function %s (event=%s) from %s", f.ID, f.Event, app)
		}
	}

	stream := make(chan store.Event, 1024)
	rnr := &runner.Runner{Store: st, Reg: reg, Stream: stream}
	exec := &executor.Executor{Store: st, Reg: reg, SigningKey: signingKey, Stream: stream}
	go func() {
		if err := rnr.Run(ctx); err != nil {
			log.Printf("runner exited: %v", err)
		}
	}()
	go func() {
		if err := exec.Run(ctx); err != nil {
			log.Printf("executor exited: %v", err)
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
	mux.Handle("/dashboard/", secure(dashboard.Handler()))
	srv := &http.Server{Addr: addr, Handler: mux, ReadHeaderTimeout: 10 * time.Second}
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		srv.Shutdown(shutdownCtx)
	}()

	log.Printf("triggerlink %s [%s] listening on %s (db=%s, apps=%d)", version, cmd, addr, dbPath, len(apps))
	if err := srv.ListenAndServe(); err != http.ErrServerClosed {
		log.Fatalf("http: %v", err)
	}
}

// resolveDashAuth 解析 Dashboard 凭据：flag 优先，env 兜底，都为空则生成随机密码。
func resolveDashAuth(flagVal string) (string, string) {
	v := flagVal
	if v == "" {
		v = os.Getenv("TRIGGERLINK_DASHBOARD_AUTH")
	}
	if v == "" {
		pass := ids.NewID("dash")
		log.Printf("dashboard auth 未配置，生成随机凭据：user=admin password=%s（仅适合内网/本地使用）", pass)
		return "admin", pass
	}
	user, pass, err := auth.ParseCreds(v)
	if err != nil {
		log.Fatalf("-dashboard-auth: %v", err)
	}
	return user, pass
}
