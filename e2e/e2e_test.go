// e2e 在单进程内组装真实的服务端组件 + 真实 SDK 应用，覆盖 M0 验收路径。
package e2e

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bearalise/triggerlink/internal/appapi"
	"github.com/bearalise/triggerlink/internal/auth"
	"github.com/bearalise/triggerlink/internal/dashapi"
	"github.com/bearalise/triggerlink/internal/eventapi"
	"github.com/bearalise/triggerlink/internal/executor"
	"github.com/bearalise/triggerlink/internal/ids"
	"github.com/bearalise/triggerlink/internal/metrics"
	"github.com/bearalise/triggerlink/internal/registry"
	"github.com/bearalise/triggerlink/internal/runner"
	"github.com/bearalise/triggerlink/internal/scheduler"
	"github.com/bearalise/triggerlink/internal/sign"
	"github.com/bearalise/triggerlink/internal/store"
	"github.com/bearalise/triggerlink/internal/webhook"
	triggerlink "github.com/bearalise/triggerlink/sdk"
	"github.com/bearalise/triggerlink/sdk/step"
)

type stack struct {
	store    *store.SQLiteStore
	eventURL string
	stream   chan store.Event
}

// startStackRaw 组装 store + runner + executor + eventapi(httptest) + sdk app(httptest)，
// 返回空注册表与 app URL，由调用方决定注册方式（启动内省或管理 API）。
// beforeStart 非 nil 时在 runner/executor 启动前调用（注入"崩溃前"的库内状态、
// 或在做启动对账扫描前完成函数注册）。
func startStackRaw(t *testing.T, opts triggerlink.FunctionOpts,
	handler func(ctx context.Context, in triggerlink.Input) (any, error),
	beforeStart func(ctx context.Context, st *store.SQLiteStore, reg *registry.Registry, appURL string)) (*stack, *registry.Registry, string) {
	t.Helper()
	return startStackFns(t, []*triggerlink.Function{triggerlink.CreateFunction(opts, handler)}, beforeStart)
}

// startStackFns 与 startStackRaw 同，但 app 挂载多个函数（扇出/多订阅场景）。
func startStackFns(t *testing.T, fns []*triggerlink.Function,
	beforeStart func(ctx context.Context, st *store.SQLiteStore, reg *registry.Registry, appURL string)) (*stack, *registry.Registry, string) {
	t.Helper()
	return startStackFull(t, fns, beforeStart, nil)
}

// startStackFull 与 startStackFns 同，tweakExec 非 nil 时在 executor 启动前调整其配置
// （如调小 TimeoutScanInterval 加速 run 超时扫描）。
func startStackFull(t *testing.T, fns []*triggerlink.Function,
	beforeStart func(ctx context.Context, st *store.SQLiteStore, reg *registry.Registry, appURL string),
	tweakExec func(*executor.Executor)) (*stack, *registry.Registry, string) {
	t.Helper()

	st, err := store.Open(filepath.Join(t.TempDir(), "test.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() })

	client := triggerlink.NewClient(triggerlink.Options{AppID: "app", SigningKey: "k"})
	app := httptest.NewServer(triggerlink.Serve(client, fns...))
	t.Cleanup(app.Close)

	reg := registry.New()

	stream := make(chan store.Event, 64)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	rnr := &runner.Runner{Store: st, Reg: reg, Stream: stream}
	exec := &executor.Executor{
		Store: st, Reg: reg, Client: app.Client(), SigningKey: "k",
		MaxRetries: 4, Concurrency: 10, BatchSize: 4,
		PollInterval: 5 * time.Millisecond, LeaseDuration: 30 * time.Second,
		BaseBackoff: 10 * time.Millisecond,
		Stream:      stream,
	}
	if tweakExec != nil {
		tweakExec(exec)
	}
	if beforeStart != nil {
		beforeStart(ctx, st, reg, app.URL)
	}
	go rnr.Run(ctx)
	go exec.Run(ctx)

	eventSrv := httptest.NewServer(eventapi.NewHandler(st, "dev-key", stream))
	t.Cleanup(eventSrv.Close)
	return &stack{store: st, eventURL: eventSrv.URL, stream: stream}, reg, app.URL
}

// mountDash 挂一个 dashapi httptest server（写端点：replay/pause/resume），不带 basic auth。
func mountDash(t *testing.T, st *store.SQLiteStore, reg *registry.Registry) string {
	t.Helper()
	srv := httptest.NewServer(dashapi.NewHandler(st, reg))
	t.Cleanup(srv.Close)
	return srv.URL
}

// dashPost 调管理 API 写端点，返回状态码与 JSON 响应体。
func dashPost(t *testing.T, dashURL, path string) (int, map[string]any) {
	t.Helper()
	resp, err := http.Post(dashURL+path, "application/json", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var body map[string]any
	json.NewDecoder(resp.Body).Decode(&body)
	return resp.StatusCode, body
}

// startStack 组装：store + runner + executor + eventapi(httptest) + sdk app(httptest)。
// 函数注册在 runner 启动前完成（runner 启动即做孤儿事件对账，注册晚于对账会丢事件）。
// beforeStart 非 nil 时在 runner/executor 启动前调用（注入"崩溃前"的库内状态）。
func startStack(t *testing.T, opts triggerlink.FunctionOpts,
	handler func(ctx context.Context, in triggerlink.Input) (any, error),
	beforeStart func(ctx context.Context, st *store.SQLiteStore)) *stack {
	t.Helper()
	s, _, _ := startStackRaw(t, opts, handler,
		func(ctx context.Context, st *store.SQLiteStore, reg *registry.Registry, appURL string) {
			if beforeStart != nil {
				beforeStart(ctx, st)
			}
			fns, err := registry.Fetch(ctx, http.DefaultClient, appURL, "k")
			if err != nil {
				t.Fatalf("introspect: %v", err)
			}
			reg.Sync(appURL, fns)
		})
	return s
}

func (s *stack) sendEvent(t *testing.T, body string) {
	t.Helper()
	req, _ := http.NewRequest(http.MethodPost, s.eventURL+"/v1/events", bytes.NewBufferString(body))
	req.Header.Set("Authorization", "Bearer dev-key")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("send event: status %d", resp.StatusCode)
	}
}

// waitStatus 轮询直到存在目标状态的 run（M0 每个用例只发一个事件，run 唯一）。
func waitStatus(t *testing.T, st *store.SQLiteStore, want string) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if n, _ := st.CountRunsWithStatus(context.Background(), want); n > 0 {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("no run reached %s", want)
}

// waitCond 轮询直到 cond 成立（10s 上限），用于等待"已挂起"等复合状态。
func waitCond(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timeout waiting for %s", what)
}

func TestHappyPath(t *testing.T) {
	var parseCount, embedCount atomic.Int32
	st := startStack(t,
		triggerlink.FunctionOpts{ID: "process-doc", Event: "doc/uploaded"},
		func(ctx context.Context, in triggerlink.Input) (any, error) {
			p, err := step.Run(ctx, "parse", func(ctx context.Context) (string, error) {
				parseCount.Add(1)
				return "p", nil
			})
			if err != nil {
				return nil, err
			}
			e, err := step.Run(ctx, "embed", func(ctx context.Context) (string, error) {
				embedCount.Add(1)
				return p + "+e", nil
			})
			if err != nil {
				return nil, err
			}
			return map[string]any{"r": e}, nil
		}, nil)
	st.sendEvent(t, `{"name":"doc/uploaded","data":{"doc_id":"d1"}}`)
	waitStatus(t, st.store, store.RunCompleted)

	if parseCount.Load() != 1 || embedCount.Load() != 1 {
		t.Fatalf("parse=%d embed=%d", parseCount.Load(), embedCount.Load())
	}
}

func TestMemoAfterStepFailure(t *testing.T) {
	var parseCount, embedCount atomic.Int32
	st := startStack(t,
		triggerlink.FunctionOpts{ID: "process-doc", Event: "doc/uploaded"},
		func(ctx context.Context, in triggerlink.Input) (any, error) {
			_, err := step.Run(ctx, "parse", func(ctx context.Context) (string, error) {
				parseCount.Add(1)
				return "p", nil
			})
			if err != nil {
				return nil, err
			}
			_, err = step.Run(ctx, "embed", func(ctx context.Context) (string, error) {
				if embedCount.Add(1) == 1 {
					return "", fmt.Errorf("flaky failure") // 第一次失败 → 整个函数重入
				}
				return "e", nil
			})
			if err != nil {
				return nil, err
			}
			return "ok", nil
		}, nil)
	st.sendEvent(t, `{"name":"doc/uploaded","data":{"doc_id":"d1"}}`)
	waitStatus(t, st.store, store.RunCompleted)

	// 验收核心：embed 第一次失败触发 re-invocation 后，parse 不得重跑
	if parseCount.Load() != 1 {
		t.Fatalf("parse re-executed: %d", parseCount.Load())
	}
	if embedCount.Load() != 2 {
		t.Fatalf("embed=%d", embedCount.Load())
	}
}

func TestDedup(t *testing.T) {
	st := startStack(t,
		triggerlink.FunctionOpts{ID: "process-doc", Event: "doc/uploaded"},
		func(ctx context.Context, in triggerlink.Input) (any, error) { return "ok", nil }, nil)
	st.sendEvent(t, `{"id":"dup-1","name":"doc/uploaded","data":{}}`)
	st.sendEvent(t, `{"id":"dup-1","name":"doc/uploaded","data":{}}`) // 同 id 重发
	waitStatus(t, st.store, store.RunCompleted)
	time.Sleep(200 * time.Millisecond)
	if n, _ := st.store.CountRuns(context.Background()); n != 1 {
		t.Fatalf("runs=%d, want 1", n)
	}
}

// TestSleepResume：step A → Sleep(1s) → step B。验证挂起期间 B 不执行、
// 到点唤醒续跑完成，且 A 全程只执行一次（sleep 不重复、memo 注入）。
func TestSleepResume(t *testing.T) {
	var aCount, bCount atomic.Int32
	st := startStack(t,
		triggerlink.FunctionOpts{ID: "sleeper", Event: "doc/uploaded"},
		func(ctx context.Context, in triggerlink.Input) (any, error) {
			if _, err := step.Run(ctx, "a", func(ctx context.Context) (any, error) {
				aCount.Add(1)
				return "a", nil
			}); err != nil {
				return nil, err
			}
			step.Sleep(ctx, "nap", time.Second)
			if _, err := step.Run(ctx, "b", func(ctx context.Context) (any, error) {
				bCount.Add(1)
				return "b", nil
			}); err != nil {
				return nil, err
			}
			return "done", nil
		}, nil)

	start := time.Now()
	st.sendEvent(t, `{"name":"doc/uploaded","data":{}}`)

	// 挂起期间：run 存在但 B 未执行、未终态
	time.Sleep(400 * time.Millisecond)
	if bCount.Load() != 0 {
		t.Fatal("b executed during sleep")
	}
	if n, _ := st.store.CountRunsWithStatus(context.Background(), store.RunCompleted); n != 0 {
		t.Fatal("run completed during sleep")
	}

	waitStatus(t, st.store, store.RunCompleted)
	if elapsed := time.Since(start); elapsed < time.Second {
		t.Fatalf("resumed too early: %v", elapsed)
	}
	if aCount.Load() != 1 || bCount.Load() != 1 {
		t.Fatalf("a=%d b=%d", aCount.Load(), bCount.Load())
	}
}

// TestSendEventFanout：父函数内 step.SendEvent 扇出事件 → 触发子函数 run，
// 两个 run 均完成；父函数 memo 返回事件 ID 列表。
func TestSendEventFanout(t *testing.T) {
	var parentCalls, childCalls atomic.Int32
	parent := triggerlink.CreateFunction(
		triggerlink.FunctionOpts{ID: "parent", Event: "doc/uploaded"},
		func(ctx context.Context, in triggerlink.Input) (any, error) {
			parentCalls.Add(1)
			ids, err := step.SendEvent(ctx, "notify",
				step.Event{Name: "child/invoked", Data: map[string]any{"from": "parent"}})
			if err != nil {
				return nil, err
			}
			return map[string]any{"sent": ids}, nil
		})
	child := triggerlink.CreateFunction(
		triggerlink.FunctionOpts{ID: "child", Event: "child/invoked"},
		func(ctx context.Context, in triggerlink.Input) (any, error) {
			childCalls.Add(1)
			return "child-done", nil
		})

	s, _, _ := startStackFns(t, []*triggerlink.Function{parent, child},
		func(ctx context.Context, st *store.SQLiteStore, reg *registry.Registry, appURL string) {
			fns, err := registry.Fetch(ctx, http.DefaultClient, appURL, "k")
			if err != nil {
				t.Fatalf("introspect: %v", err)
			}
			reg.Sync(appURL, fns)
		})

	s.sendEvent(t, `{"name":"doc/uploaded","data":{}}`)

	// 两个 run 都到达 Completed
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if n, _ := s.store.CountRunsWithStatus(context.Background(), store.RunCompleted); n == 2 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if n, _ := s.store.CountRunsWithStatus(context.Background(), store.RunCompleted); n != 2 {
		t.Fatalf("completed runs=%d, want 2", n)
	}
	if childCalls.Load() != 1 {
		t.Fatalf("child executed %d times", childCalls.Load())
	}
}

// TestStartupReconciliation：事件已落库但未路由（模拟崩溃窗口），
// runner 启动时的对账扫描应补建 run 并执行完成。
func TestStartupReconciliation(t *testing.T) {
	st := startStack(t,
		triggerlink.FunctionOpts{ID: "process-doc", Event: "doc/uploaded"},
		func(ctx context.Context, in triggerlink.Input) (any, error) { return "ok", nil },
		func(ctx context.Context, st *store.SQLiteStore) {
			// 模拟崩溃窗口：事件落库了但还没进 stream（routed_at 为空）
			inserted, err := st.InsertEvent(ctx, store.Event{
				ID:   "orphan-1",
				Name: "doc/uploaded",
				Data: json.RawMessage(`{"doc_id":"orphan"}`),
				TS:   time.Now(),
			})
			if err != nil || !inserted {
				t.Fatalf("insert orphan event: inserted=%v err=%v", inserted, err)
			}
		})
	waitStatus(t, st.store, store.RunCompleted)
	if n, _ := st.store.CountRuns(context.Background()); n != 1 {
		t.Fatalf("runs=%d, want 1", n)
	}
}

// TestDynamicRegistration：平台启动时无注册函数，运行期间通过管理 API
// POST /api/v1/apps 注册应用，随后事件正常路由执行（PRD FR-5.3 运行时形态）。
func TestDynamicRegistration(t *testing.T) {
	s, reg, appURL := startStackRaw(t,
		triggerlink.FunctionOpts{ID: "process-doc", Event: "doc/uploaded"},
		func(ctx context.Context, in triggerlink.Input) (any, error) { return "ok", nil }, nil)

	admin := httptest.NewServer(appapi.NewHandler(reg, "dev-key", "k", nil))
	t.Cleanup(admin.Close)

	req, _ := http.NewRequest(http.MethodPost, admin.URL+"/api/v1/apps",
		bytes.NewBufferString(`{"url":"`+appURL+`"}`))
	req.Header.Set("Authorization", "Bearer dev-key")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("register app: status %d", resp.StatusCode)
	}

	s.sendEvent(t, `{"name":"doc/uploaded","data":{"doc_id":"dyn-1"}}`)
	waitStatus(t, s.store, store.RunCompleted)
}

// TestDashboardAPI 跑通真实 run 后，通过只读管理 API 断言四个端点的数据一致性。
func TestDashboardAPI(t *testing.T) {
	var calls atomic.Int32
	s, reg, _ := startStackRaw(t,
		triggerlink.FunctionOpts{ID: "fn-dash", Event: "doc/uploaded"},
		func(ctx context.Context, in triggerlink.Input) (any, error) {
			calls.Add(1)
			if _, err := step.Run(ctx, "parse", func(ctx context.Context) (any, error) {
				return map[string]any{"pages": 3}, nil
			}); err != nil {
				return nil, err
			}
			return map[string]any{"ok": true}, nil
		},
		func(ctx context.Context, st *store.SQLiteStore, reg *registry.Registry, appURL string) {
			fns, err := registry.Fetch(ctx, http.DefaultClient, appURL, "k")
			if err != nil {
				t.Fatalf("introspect: %v", err)
			}
			reg.Sync(appURL, fns)
		})

	s.sendEvent(t, `{"name":"doc/uploaded","data":{"doc_id":"d1"}}`)
	waitStatus(t, s.store, store.RunCompleted)

	dashSrv := httptest.NewServer(auth.Basic("admin", "pw", dashapi.NewHandler(s.store, reg)))
	t.Cleanup(dashSrv.Close)

	get := func(path, user, pass string) (*http.Response, map[string]any) {
		t.Helper()
		req, _ := http.NewRequest(http.MethodGet, dashSrv.URL+path, nil)
		if user != "" {
			req.SetBasicAuth(user, pass)
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		var body map[string]any
		json.NewDecoder(resp.Body).Decode(&body)
		return resp, body
	}

	// 未认证 → 401
	resp, _ := get("/api/v1/runs", "", "")
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("no auth: %d", resp.StatusCode)
	}

	// runs 列表
	_, body := get("/api/v1/runs", "admin", "pw")
	runs := body["runs"].([]any)
	if len(runs) != 1 {
		t.Fatalf("runs: %v", runs)
	}
	run := runs[0].(map[string]any)
	if run["function_id"] != "fn-dash" || run["status"] != "completed" {
		t.Fatalf("run: %v", run)
	}
	runID := run["id"].(string)

	// run 详情：step 时间线含 updated_at
	_, body = get("/api/v1/runs/"+runID, "admin", "pw")
	steps := body["steps"].([]any)
	if len(steps) != 1 || steps[0].(map[string]any)["step_id"] != "parse" {
		t.Fatalf("steps: %v", steps)
	}
	if steps[0].(map[string]any)["updated_at"] == nil {
		t.Fatalf("updated_at missing: %v", steps[0])
	}

	// 事件流：runs 反链
	_, body = get("/api/v1/events", "admin", "pw")
	events := body["events"].([]any)
	if len(events) != 1 {
		t.Fatalf("events: %v", events)
	}
	backlinks := events[0].(map[string]any)["runs"].([]any)
	if len(backlinks) != 1 || backlinks[0] != runID {
		t.Fatalf("backlinks: %v", backlinks)
	}

	// 函数列表：24h 统计
	_, body = get("/api/v1/functions", "admin", "pw")
	fns := body["functions"].([]any)
	if len(fns) != 1 || fns[0].(map[string]any)["id"] != "fn-dash" {
		t.Fatalf("functions: %v", fns)
	}
	stats := fns[0].(map[string]any)["stats_24h"].(map[string]any)
	if stats["total"] != float64(1) || stats["completed"] != float64(1) {
		t.Fatalf("stats: %v", stats)
	}
}

// TestRegistrationSurvivesRestart：经管理 API 动态注册的 app 落库后，
// 模拟平台重启（关库重开、新建内存注册表），仅从库恢复注册即可路由执行，
// 无需重新注册/内省（注册信息持久化验收，FR-5.3）。
func TestRegistrationSurvivesRestart(t *testing.T) {
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "test.db")

	client := triggerlink.NewClient(triggerlink.Options{AppID: "app", SigningKey: "k"})
	fn := triggerlink.CreateFunction(
		triggerlink.FunctionOpts{ID: "process-doc", Event: "doc/uploaded"},
		func(ctx context.Context, in triggerlink.Input) (any, error) { return "ok", nil })
	app := httptest.NewServer(triggerlink.Serve(client, fn))
	t.Cleanup(app.Close)

	// 与 main.go 一致的 persist 闭包：registry.Function → store.RegisteredFunction
	// （cancel_on 序列化为 JSON 字符串，空规则留空）
	persist := func(st *store.SQLiteStore) func(context.Context, string, []registry.Function) error {
		return func(ctx context.Context, appURL string, fns []registry.Function) error {
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
	}

	// 第一次"进程"：管理 API 动态注册（落库），随后关库模拟退出
	st1, err := store.Open(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	admin := appapi.NewHandler(registry.New(), "ek", "k", nil)
	admin.Persist = persist(st1)
	req := httptest.NewRequest(http.MethodPost, "/api/v1/apps", bytes.NewBufferString(`{"url":"`+app.URL+`"}`))
	req.Header.Set("Authorization", "Bearer ek")
	rec := httptest.NewRecorder()
	admin.ServeHTTP(rec, req)
	if rec.Code != http.StatusCreated {
		t.Fatalf("register: %d body=%s", rec.Code, rec.Body)
	}
	st1.Close()

	// 第二次"进程"：重开库，仅从持久化记录恢复注册表（不内省、不重新注册）
	st2, err := store.Open(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st2.Close() })
	rfns, err := st2.LoadRegisteredFunctions(ctx)
	if err != nil {
		t.Fatal(err)
	}
	reg := registry.New()
	byApp := map[string][]registry.Function{}
	for _, rf := range rfns {
		byApp[rf.AppURL] = append(byApp[rf.AppURL],
			registry.Function{ID: rf.ID, Event: rf.Event, Retries: rf.Retries, AppURL: rf.AppURL})
	}
	for appURL, fns := range byApp {
		reg.Sync(appURL, fns)
	}
	if _, ok := reg.Lookup("process-doc"); !ok {
		t.Fatalf("registration not restored: %+v", rfns)
	}

	// 接 runner/executor + eventapi，发事件 → run Completed（恢复的注册真实可路由）
	stream := make(chan store.Event, 64)
	runCtx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	rnr := &runner.Runner{Store: st2, Reg: reg, Stream: stream}
	exec := &executor.Executor{
		Store: st2, Reg: reg, Client: app.Client(), SigningKey: "k",
		MaxRetries: 4, Concurrency: 10, BatchSize: 4,
		PollInterval: 5 * time.Millisecond, LeaseDuration: 30 * time.Second,
		BaseBackoff: 10 * time.Millisecond,
		Stream:      stream,
	}
	go rnr.Run(runCtx)
	go exec.Run(runCtx)
	eventSrv := httptest.NewServer(eventapi.NewHandler(st2, "dev-key", stream))
	t.Cleanup(eventSrv.Close)

	s := &stack{store: st2, eventURL: eventSrv.URL}
	s.sendEvent(t, `{"name":"doc/uploaded","data":{}}`)
	waitStatus(t, st2, store.RunCompleted)
}

// TestWaitForEventResume：step A → WaitForEvent(带 match) 挂起。挂起期间 run 保持
// Running 且零占用；match 不命中的同名事件不恢复；命中事件到达后 run 恢复完成，
// WaitForEvent 返回该事件（平台写入 memo 的 {"id","name","data","ts"}）。
func TestWaitForEventResume(t *testing.T) {
	var appCalls atomic.Int32
	var got atomic.Pointer[triggerlink.Event]
	st := startStack(t,
		triggerlink.FunctionOpts{ID: "approver", Event: "doc/uploaded"},
		func(ctx context.Context, in triggerlink.Input) (any, error) {
			appCalls.Add(1)
			if _, err := step.Run(ctx, "parse", func(ctx context.Context) (any, error) {
				return "p", nil
			}); err != nil {
				return nil, err
			}
			ev, err := step.WaitForEvent(ctx, "wait-approval", step.WaitOpts{
				Event:   "doc/approved",
				Match:   "data.doc_id == event.data.doc_id",
				Timeout: 5 * time.Second,
			})
			if err != nil {
				return nil, err
			}
			if ev == nil {
				return "timeout", nil
			}
			got.Store(ev)
			return "approved", nil
		}, nil)

	st.sendEvent(t, `{"name":"doc/uploaded","data":{"doc_id":"d1"}}`)

	// 挂起：step A 与 waitForEvent 各一跳回调（appCalls=2），run 停在 Running 未完成
	waitCond(t, "run suspended", func() bool {
		n, _ := st.store.CountRunsWithStatus(context.Background(), store.RunRunning)
		return appCalls.Load() == 2 && n == 1
	})
	time.Sleep(200 * time.Millisecond)
	if n, _ := st.store.CountRunsWithStatus(context.Background(), store.RunCompleted); n != 0 {
		t.Fatal("run completed while suspended")
	}
	if appCalls.Load() != 2 {
		t.Fatalf("app called %d times while suspended", appCalls.Load())
	}

	// match 不命中的同名事件：不恢复
	st.sendEvent(t, `{"name":"doc/approved","data":{"doc_id":"other"}}`)
	time.Sleep(300 * time.Millisecond)
	if n, _ := st.store.CountRunsWithStatus(context.Background(), store.RunCompleted); n != 0 {
		t.Fatal("run resumed by non-matching event")
	}
	if appCalls.Load() != 2 {
		t.Fatalf("app called %d times after non-matching event", appCalls.Load())
	}

	// match 命中：恢复并完成，WaitForEvent 返回到达的事件
	st.sendEvent(t, `{"id":"approve-1","name":"doc/approved","data":{"doc_id":"d1","by":"alice"}}`)
	waitStatus(t, st.store, store.RunCompleted)

	ev := got.Load()
	if ev == nil {
		t.Fatal("WaitForEvent did not return the matched event")
	}
	if ev.ID != "approve-1" || ev.Name != "doc/approved" {
		t.Fatalf("event: %+v", ev)
	}
	var data map[string]any
	if err := json.Unmarshal(ev.Data, &data); err != nil {
		t.Fatalf("unmarshal event data: %v", err)
	}
	if data["doc_id"] != "d1" || data["by"] != "alice" {
		t.Fatalf("event data: %v", data)
	}
	if appCalls.Load() != 3 {
		t.Fatalf("appCalls=%d, want 3 (step A + wait + resume)", appCalls.Load())
	}
}

// TestWaitForEventTimeout：WaitForEvent 设几百毫秒超时，无事件到达 →
// 平台以 output=null 完成 memo 并恢复 run，SDK 返回 nil 走超时分支。
func TestWaitForEventTimeout(t *testing.T) {
	var appCalls atomic.Int32
	var timedOut atomic.Bool
	st := startStack(t,
		triggerlink.FunctionOpts{ID: "waiter", Event: "doc/uploaded"},
		func(ctx context.Context, in triggerlink.Input) (any, error) {
			appCalls.Add(1)
			ev, err := step.WaitForEvent(ctx, "wait", step.WaitOpts{
				Event:   "doc/approved",
				Timeout: 300 * time.Millisecond,
			})
			if err != nil {
				return nil, err
			}
			if ev == nil {
				timedOut.Store(true)
				return "timeout", nil
			}
			return "resumed", nil
		}, nil)

	st.sendEvent(t, `{"name":"doc/uploaded","data":{"doc_id":"d1"}}`)
	waitStatus(t, st.store, store.RunCompleted)

	if !timedOut.Load() {
		t.Fatal("WaitForEvent did not take the timeout branch")
	}
	if appCalls.Load() != 2 {
		t.Fatalf("appCalls=%d, want 2 (initial + timeout resume)", appCalls.Load())
	}
}

// TestCancelOn：函数配置 CancelOn（带 match），挂起等待期间匹配事件到达 →
// 在途 run 被取消（Cancelled）；此后等待的事件到达也不再恢复（应用侧不再被回调）。
func TestCancelOn(t *testing.T) {
	var appCalls atomic.Int32
	st := startStack(t,
		triggerlink.FunctionOpts{
			ID:    "cancellable",
			Event: "doc/uploaded",
			CancelOn: []triggerlink.CancelOn{
				{Event: "doc/cancelled", Match: "data.doc_id == event.data.doc_id"},
			},
		},
		func(ctx context.Context, in triggerlink.Input) (any, error) {
			appCalls.Add(1)
			if _, err := step.Run(ctx, "a", func(ctx context.Context) (any, error) {
				return "a", nil
			}); err != nil {
				return nil, err
			}
			if _, err := step.WaitForEvent(ctx, "wait", step.WaitOpts{
				Event:   "doc/approved",
				Timeout: time.Hour,
			}); err != nil {
				return nil, err
			}
			return "done", nil
		}, nil)

	st.sendEvent(t, `{"name":"doc/uploaded","data":{"doc_id":"d1"}}`)
	// 挂起：step A 与 waitForEvent 各一跳回调（appCalls=2），run 停在 Running
	waitCond(t, "run suspended", func() bool {
		n, _ := st.store.CountRunsWithStatus(context.Background(), store.RunRunning)
		return appCalls.Load() == 2 && n == 1
	})

	// cancelOn match 命中 → run 终态 Cancelled
	st.sendEvent(t, `{"name":"doc/cancelled","data":{"doc_id":"d1"}}`)
	waitStatus(t, st.store, store.RunCancelled)

	// 之后等待的事件到达：终态 run 不恢复，应用侧无新回调
	st.sendEvent(t, `{"name":"doc/approved","data":{"doc_id":"d1"}}`)
	time.Sleep(300 * time.Millisecond)
	if appCalls.Load() != 2 {
		t.Fatalf("app called %d times after cancel", appCalls.Load())
	}
	if n, _ := st.store.CountRunsWithStatus(context.Background(), store.RunCompleted); n != 0 {
		t.Fatal("cancelled run resumed")
	}
}

// TestOnFailureCompensation：函数 step 必失败且 Retries=1（首次尝试即耗尽进入 Failed
// 终态）→ 平台合成内部事件 triggerlink/run.failed → Serve 注册的隐式函数
// <id>/on-failure 作为普通函数被路由执行，内部 step 原语可用，且收到正确的
// run_id / function_id / error / 触发事件快照（FR-2.11）。
func TestOnFailureCompensation(t *testing.T) {
	var compStepCalls atomic.Int32
	var failData atomic.Pointer[map[string]any]
	st := startStack(t,
		triggerlink.FunctionOpts{
			ID: "flaky", Event: "flaky/invoked", Retries: 1,
			OnFailure: func(ctx context.Context, in triggerlink.Input) (any, error) {
				// 补偿函数内 step 原语可用（memo/恢复语义同普通函数）
				if _, err := step.Run(ctx, "record", func(ctx context.Context) (any, error) {
					compStepCalls.Add(1)
					return "recorded", nil
				}); err != nil {
					return nil, err
				}
				var data map[string]any
				if err := json.Unmarshal(in.Event.Data, &data); err != nil {
					return nil, err
				}
				failData.Store(&data)
				return "compensated", nil
			},
		},
		func(ctx context.Context, in triggerlink.Input) (any, error) {
			if _, err := step.Run(ctx, "boom", func(ctx context.Context) (any, error) {
				return nil, fmt.Errorf("boom")
			}); err != nil {
				return nil, err
			}
			return "never", nil
		}, nil)

	st.sendEvent(t, `{"name":"flaky/invoked","data":{"doc_id":"d1"}}`)
	waitStatus(t, st.store, store.RunFailed)

	// 原 run 唯一且已 Failed；补偿事件的 run_id 应指向它
	runs, err := st.store.ListRuns(context.Background(), store.ListRunsOptions{FunctionID: "flaky"})
	if err != nil || len(runs) != 1 {
		t.Fatalf("flaky runs: %v err=%v", runs, err)
	}
	failedRun := runs[0]

	// run.failed 事件走 InsertEvent + stream 异步路由 → 隐式函数 run Completed
	waitCond(t, "compensation run completed", func() bool {
		runs, _ := st.store.ListRuns(context.Background(),
			store.ListRunsOptions{FunctionID: "flaky/on-failure", Status: store.RunCompleted})
		return len(runs) == 1
	})

	got := failData.Load()
	if got == nil {
		t.Fatal("compensation handler did not receive the run.failed event")
	}
	data := *got
	if data["run_id"] != failedRun.ID {
		t.Fatalf("run_id=%v, want %s", data["run_id"], failedRun.ID)
	}
	if data["function_id"] != "flaky" {
		t.Fatalf("function_id=%v", data["function_id"])
	}
	if errStr, _ := data["error"].(string); !strings.Contains(errStr, "boom") {
		t.Fatalf("error=%v, want contains %q", data["error"], "boom")
	}
	evt, _ := data["event"].(map[string]any)
	if evt["name"] != "flaky/invoked" {
		t.Fatalf("trigger event snapshot: %v", evt)
	}
	if compStepCalls.Load() != 1 {
		t.Fatalf("compStepCalls=%d, want 1", compStepCalls.Load())
	}
}

// TestCronTrigger：函数只配 Cron（不经事件路由）。scheduler 的 check 未导出且为分钟
// 粒度（真等到点最长一分钟，不适合 e2e），这里按 scheduler.fire 的触发路径直接建
// run + 入队（EventName=triggerlink/cron，EventData={"ts":<到点>}），端到端验证 cron
// 触发的 run 被 executor 执行到 Completed，且 handler 收到正确的事件名与 ts（FR-3.2）。
// scheduler 的到点判定/幂等由 internal/scheduler 单测覆盖。
func TestCronTrigger(t *testing.T) {
	var gotName, gotTS atomic.Value
	s, reg, _ := startStackRaw(t,
		triggerlink.FunctionOpts{ID: "nightly", Cron: "* * * * *"},
		func(ctx context.Context, in triggerlink.Input) (any, error) {
			gotName.Store(in.Event.Name)
			var data map[string]any
			if err := json.Unmarshal(in.Event.Data, &data); err != nil {
				return nil, err
			}
			gotTS.Store(data["ts"])
			return "ok", nil
		},
		func(ctx context.Context, st *store.SQLiteStore, reg *registry.Registry, appURL string) {
			fns, err := registry.Fetch(ctx, http.DefaultClient, appURL, "k")
			if err != nil {
				t.Fatalf("introspect: %v", err)
			}
			reg.Sync(appURL, fns)
		})

	// 内省清单携带 cron 表达式，注册表可枚举出 cron 函数（scheduler 的输入）
	cronFns := reg.CronFunctions()
	if len(cronFns) != 1 || cronFns[0].ID != "nightly" || cronFns[0].Cron != "* * * * *" {
		t.Fatalf("cron functions: %+v", cronFns)
	}

	// 与 scheduler.fire 同构的触发路径：CreateRun + Enqueue
	at := time.Now().UTC().Truncate(time.Minute)
	data, _ := json.Marshal(map[string]any{"ts": at.Format(time.RFC3339)})
	ctx := context.Background()
	runID := ids.NewID("run")
	created, err := s.store.CreateRun(ctx, store.Run{
		ID:         runID,
		FunctionID: "nightly",
		Status:     store.RunQueued,
		EventID:    fmt.Sprintf("cron_nightly_%d", at.Unix()),
		EventName:  scheduler.EventName,
		EventData:  data,
		EventTS:    at,
	})
	if err != nil || !created {
		t.Fatalf("create cron run: created=%v err=%v", created, err)
	}
	if err := s.store.Enqueue(ctx, store.QueueItem{
		ID:         ids.NewID("qi"),
		FunctionID: "nightly",
		RunID:      runID,
		Score:      time.Now().UnixNano(),
		At:         time.Now(),
		Status:     store.QueuePending,
	}); err != nil {
		t.Fatalf("enqueue cron run: %v", err)
	}

	waitStatus(t, s.store, store.RunCompleted)

	// run 快照与 handler 收到的事件名/ts 正确
	runs, err := s.store.ListRuns(ctx, store.ListRunsOptions{FunctionID: "nightly"})
	if err != nil || len(runs) != 1 {
		t.Fatalf("nightly runs: %v err=%v", runs, err)
	}
	if runs[0].EventName != scheduler.EventName {
		t.Fatalf("run EventName=%q, want %q", runs[0].EventName, scheduler.EventName)
	}
	if gotName.Load() != scheduler.EventName {
		t.Fatalf("handler event name=%v", gotName.Load())
	}
	if gotTS.Load() != at.Format(time.RFC3339) {
		t.Fatalf("handler ts=%v, want %s", gotTS.Load(), at.Format(time.RFC3339))
	}
}

// registerApp 是 startStackRaw/startStackFull 的 beforeStart：内省 app 并同步注册表
// （runner 启动前完成，与 startStack 的注册路径一致）。
func registerApp(t *testing.T) func(context.Context, *store.SQLiteStore, *registry.Registry, string) {
	t.Helper()
	return func(ctx context.Context, st *store.SQLiteStore, reg *registry.Registry, appURL string) {
		fns, err := registry.Fetch(ctx, http.DefaultClient, appURL, "k")
		if err != nil {
			t.Fatalf("introspect: %v", err)
		}
		reg.Sync(appURL, fns)
	}
}

// TestReplayFailedRun：失败 run 经管理 API replay 出全新 run（新 id，不继承 step
// memo）。用 atomic 开关翻转行为：首次必失败 → run Failed；replay 前翻转为成功 →
// 新 run Completed 且 step 计数器再次 +1（证明 memo 未继承、step 真实重跑，FR-6.2）。
func TestReplayFailedRun(t *testing.T) {
	var failMode atomic.Bool
	failMode.Store(true)
	var workCalls atomic.Int32
	s, reg, _ := startStackRaw(t,
		triggerlink.FunctionOpts{ID: "flaky-replay", Event: "flaky/invoked", Retries: 1},
		func(ctx context.Context, in triggerlink.Input) (any, error) {
			if _, err := step.Run(ctx, "work", func(ctx context.Context) (any, error) {
				workCalls.Add(1)
				if failMode.Load() {
					return nil, fmt.Errorf("boom")
				}
				return "ok", nil
			}); err != nil {
				return nil, err
			}
			return "done", nil
		}, registerApp(t))
	dashURL := mountDash(t, s.store, reg)
	ctx := context.Background()

	s.sendEvent(t, `{"name":"flaky/invoked","data":{}}`)
	waitStatus(t, s.store, store.RunFailed)
	runs, err := s.store.ListRuns(ctx, store.ListRunsOptions{FunctionID: "flaky-replay"})
	if err != nil || len(runs) != 1 {
		t.Fatalf("runs: %v err=%v", runs, err)
	}
	orig := runs[0]
	if workCalls.Load() != 1 {
		t.Fatalf("workCalls=%d, want 1", workCalls.Load())
	}

	// 不存在的 run → 404
	if code, _ := dashPost(t, dashURL, "/api/v1/runs/run_nonexistent/replay"); code != http.StatusNotFound {
		t.Fatalf("replay nonexistent: %d", code)
	}

	// 翻转行为后 replay：返回新 run_id ≠ 原 id
	failMode.Store(false)
	code, body := dashPost(t, dashURL, "/api/v1/runs/"+orig.ID+"/replay")
	if code != http.StatusOK {
		t.Fatalf("replay: %d body=%v", code, body)
	}
	newID, _ := body["run_id"].(string)
	if newID == "" || newID == orig.ID {
		t.Fatalf("replay run_id=%q, want new id (orig %s)", newID, orig.ID)
	}

	// 新 run 从头执行到 Completed；step 真实重跑（计数器再 +1）
	waitCond(t, "replay run completed", func() bool {
		run, err := s.store.GetRun(ctx, newID)
		return err == nil && run.Status == store.RunCompleted
	})
	if workCalls.Load() != 2 {
		t.Fatalf("workCalls=%d, want 2 (replay 不继承 memo，step 应重跑)", workCalls.Load())
	}
}

// TestPauseResumeFunction：pause 后事件照收但不建 run；resume 后新事件正常触发（FR-7.3）。
func TestPauseResumeFunction(t *testing.T) {
	var calls atomic.Int32
	s, reg, _ := startStackRaw(t,
		triggerlink.FunctionOpts{ID: "pausable", Event: "doc/uploaded"},
		func(ctx context.Context, in triggerlink.Input) (any, error) {
			calls.Add(1)
			return "ok", nil
		}, registerApp(t))
	dashURL := mountDash(t, s.store, reg)
	ctx := context.Background()

	// pause → 发事件 → 不建 run
	if code, _ := dashPost(t, dashURL, "/api/v1/functions/pausable/pause"); code != http.StatusOK {
		t.Fatalf("pause: %d", code)
	}
	s.sendEvent(t, `{"name":"doc/uploaded","data":{"doc_id":"paused"}}`)
	time.Sleep(300 * time.Millisecond)
	if n, _ := s.store.CountRuns(ctx); n != 0 {
		t.Fatalf("runs=%d while paused, want 0", n)
	}
	if calls.Load() != 0 {
		t.Fatalf("handler called %d times while paused", calls.Load())
	}

	// resume → 再发事件 → 正常触发到 Completed
	if code, _ := dashPost(t, dashURL, "/api/v1/functions/pausable/resume"); code != http.StatusOK {
		t.Fatalf("resume: %d", code)
	}
	s.sendEvent(t, `{"name":"doc/uploaded","data":{"doc_id":"resumed"}}`)
	waitStatus(t, s.store, store.RunCompleted)
	if calls.Load() != 1 {
		t.Fatalf("calls=%d, want 1", calls.Load())
	}
	if n, _ := s.store.CountRuns(ctx); n != 1 {
		t.Fatalf("runs=%d, want 1", n)
	}
}

// TestRunTimeout：函数配 Timeout=2s，handler 在 step A 后 Sleep(30s) 挂起 →
// run 从 created_at 起超时仍在 Running → 平台置 Failed，error 含 "run timeout"
// （FR-4.3）。TimeoutScanInterval 调小到 10ms 加速扫描；Timeout 取 2s 以容忍
// 全量测试负载下首个回调的调度延迟（300ms 在高负载时会在回调发生前误杀 run）。
func TestRunTimeout(t *testing.T) {
	var aCount atomic.Int32
	fn := triggerlink.CreateFunction(
		triggerlink.FunctionOpts{ID: "slow", Event: "doc/uploaded", Timeout: 2 * time.Second},
		func(ctx context.Context, in triggerlink.Input) (any, error) {
			if _, err := step.Run(ctx, "a", func(ctx context.Context) (any, error) {
				aCount.Add(1)
				return "a", nil
			}); err != nil {
				return nil, err
			}
			step.Sleep(ctx, "nap", 30*time.Second) // 挂起 30s，远超 run 超时
			return "done", nil
		})
	s, _, _ := startStackFull(t, []*triggerlink.Function{fn}, registerApp(t),
		func(exec *executor.Executor) { exec.TimeoutScanInterval = 10 * time.Millisecond })

	s.sendEvent(t, `{"name":"doc/uploaded","data":{}}`)
	waitStatus(t, s.store, store.RunFailed)

	runs, err := s.store.ListRuns(context.Background(), store.ListRunsOptions{FunctionID: "slow"})
	if err != nil || len(runs) != 1 {
		t.Fatalf("runs: %v err=%v", runs, err)
	}
	run, err := s.store.GetRun(context.Background(), runs[0].ID)
	if err != nil {
		t.Fatalf("get run: %v", err)
	}
	if !strings.Contains(run.Error, "run timeout") {
		t.Fatalf("run error=%q, want contains %q", run.Error, "run timeout")
	}
	if aCount.Load() != 1 {
		t.Fatalf("a=%d, want 1", aCount.Load())
	}
}

// TestWebhookTrigger：走管理 API 创建 webhook → 带正确签名 POST /hooks/{id} →
// 订阅该 event_name 的函数 run Completed；错签名 → 401 且不触发（FR-3.4）。
func TestWebhookTrigger(t *testing.T) {
	var calls atomic.Int32
	s, reg, _ := startStackRaw(t,
		triggerlink.FunctionOpts{ID: "on-payment", Event: "stripe/payment"},
		func(ctx context.Context, in triggerlink.Input) (any, error) {
			calls.Add(1)
			return "ok", nil
		}, registerApp(t))
	dashURL := mountDash(t, s.store, reg)
	hookSrv := httptest.NewServer(webhook.NewHandler(s.store, s.stream))
	t.Cleanup(hookSrv.Close)
	ctx := context.Background()

	// 创建 webhook（管理 API，secret 自动生成）
	req, _ := http.NewRequest(http.MethodPost, dashURL+"/api/v1/webhooks",
		bytes.NewBufferString(`{"event_name":"stripe/payment"}`))
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	var created struct {
		ID     string `json:"id"`
		URL    string `json:"url"`
		Secret string `json:"secret"`
	}
	json.NewDecoder(resp.Body).Decode(&created)
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK || created.ID == "" || created.Secret == "" {
		t.Fatalf("create webhook: status=%d body=%+v", resp.StatusCode, created)
	}

	// 错签名 → 401，不触发
	body := `{"amount":100}`
	badReq, _ := http.NewRequest(http.MethodPost, hookSrv.URL+created.URL, bytes.NewBufferString(body))
	badReq.Header.Set("X-TriggerLink-Signature", sign.Sign("wrong-secret", []byte(body), time.Now()))
	badResp, err := http.DefaultClient.Do(badReq)
	if err != nil {
		t.Fatal(err)
	}
	badResp.Body.Close()
	if badResp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("bad signature: status %d", badResp.StatusCode)
	}
	time.Sleep(300 * time.Millisecond)
	if n, _ := s.store.CountRuns(ctx); n != 0 {
		t.Fatalf("runs=%d after bad signature, want 0", n)
	}

	// 正确签名 → 200 + 事件路由 → run Completed
	goodReq, _ := http.NewRequest(http.MethodPost, hookSrv.URL+created.URL, bytes.NewBufferString(body))
	goodReq.Header.Set("X-TriggerLink-Signature", sign.Sign(created.Secret, []byte(body), time.Now()))
	goodResp, err := http.DefaultClient.Do(goodReq)
	if err != nil {
		t.Fatal(err)
	}
	var out struct {
		ID string `json:"id"`
	}
	json.NewDecoder(goodResp.Body).Decode(&out)
	goodResp.Body.Close()
	if goodResp.StatusCode != http.StatusOK || !strings.HasPrefix(out.ID, "evt_") {
		t.Fatalf("signed post: status=%d body=%+v", goodResp.StatusCode, out)
	}
	waitStatus(t, s.store, store.RunCompleted)
	if calls.Load() != 1 {
		t.Fatalf("calls=%d, want 1", calls.Load())
	}
	runs, _ := s.store.ListRuns(ctx, store.ListRunsOptions{FunctionID: "on-payment"})
	if len(runs) != 1 || runs[0].EventID != out.ID {
		t.Fatalf("runs: %+v", runs)
	}
	run, err := s.store.GetRun(ctx, runs[0].ID)
	if err != nil {
		t.Fatal(err)
	}
	if string(run.EventData) != body || run.EventName != "stripe/payment" {
		t.Fatalf("run event snapshot: %+v", run)
	}
}

// scrapeMetric 抓一次 /metrics 并取出某条序列的值（样本行完全匹配 name+labels）。
// 序列不存在时返回 0：counter 未被触碰过与值为 0 在断言增量时等价。
func scrapeMetric(t *testing.T, series string) float64 {
	t.Helper()
	rec := httptest.NewRecorder()
	metrics.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("/metrics status=%d", rec.Code)
	}
	for _, line := range strings.Split(rec.Body.String(), "\n") {
		if !strings.HasPrefix(line, series+" ") {
			continue
		}
		v, err := strconv.ParseFloat(strings.TrimSpace(strings.TrimPrefix(line, series+" ")), 64)
		if err != nil {
			t.Fatalf("parse %q: %v", line, err)
		}
		return v
	}
	return 0
}

// 一条完整流水线跑完后，/metrics 上的接收/路由/终态计数与队列 gauge 都应随之变化（FR-6.4）。
func TestMetricsEndToEnd(t *testing.T) {
	before := map[string]float64{}
	series := []string{
		"triggerlink_events_received_total",
		`triggerlink_events_routed_total{result="routed"}`,
		`triggerlink_events_routed_total{result="filtered"}`,
		`triggerlink_runs_total{status="completed"}`,
		"triggerlink_run_duration_seconds_count",
		`triggerlink_step_duration_seconds_count{op="Run"}`,
		"triggerlink_callback_duration_seconds_count",
	}
	for _, s := range series {
		before[s] = scrapeMetric(t, s)
	}

	st := startStack(t,
		// match 表达式：只处理 doc_id=d1 的事件，另一条走 filtered 分支
		triggerlink.FunctionOpts{ID: "metrics-fn", Event: "doc/uploaded", Match: `data.doc_id == "d1"`},
		func(ctx context.Context, in triggerlink.Input) (any, error) {
			return step.Run(ctx, "work", func(ctx context.Context) (string, error) { return "ok", nil })
		}, nil)
	st.sendEvent(t, `{"name":"doc/uploaded","data":{"doc_id":"d1"}}`)
	st.sendEvent(t, `{"name":"doc/uploaded","data":{"doc_id":"other"}}`)
	waitStatus(t, st.store, store.RunCompleted)

	delta := func(s string) float64 { return scrapeMetric(t, s) - before[s] }
	if d := delta("triggerlink_events_received_total"); d != 2 {
		t.Fatalf("events_received delta=%v, want 2", d)
	}
	if d := delta(`triggerlink_events_routed_total{result="routed"}`); d != 1 {
		t.Fatalf("events_routed{routed} delta=%v, want 1", d)
	}
	if d := delta(`triggerlink_events_routed_total{result="filtered"}`); d != 1 {
		t.Fatalf("events_routed{filtered} delta=%v, want 1", d)
	}
	if d := delta(`triggerlink_runs_total{status="completed"}`); d != 1 {
		t.Fatalf("runs_total{completed} delta=%v, want 1", d)
	}
	if d := delta("triggerlink_run_duration_seconds_count"); d != 1 {
		t.Fatalf("run_duration samples delta=%v, want 1", d)
	}
	// 该函数一个 step，run 走两跳回调（step 完成一跳 + 函数返回一跳）
	if d := delta(`triggerlink_step_duration_seconds_count{op="Run"}`); d != 1 {
		t.Fatalf("step_duration{Run} delta=%v, want 1", d)
	}
	if d := delta("triggerlink_callback_duration_seconds_count"); d < 2 {
		t.Fatalf("callback_duration delta=%v, want >= 2", d)
	}

	// gauge 由采样器驱动：队列已排空，采样后应为 0
	metrics.Sample(context.Background(), st.store)
	if v := scrapeMetric(t, "triggerlink_queue_depth"); v != 0 {
		t.Fatalf("queue_depth=%v, want 0 after drain", v)
	}
	if v := scrapeMetric(t, "triggerlink_waits_active"); v != 0 {
		t.Fatalf("waits_active=%v, want 0", v)
	}
}

// 防抖端到端（FR-4.5）：窗口内连发的事件合并成一个 run，handler 只跑一次，
// 拿到的是最后一个事件；窗口关闭后的事件重新开窗。
func TestDebounceMergesEvents(t *testing.T) {
	var calls atomic.Int32
	seen := make(chan string, 8)
	st := startStack(t,
		triggerlink.FunctionOpts{
			ID: "index-doc", Event: "doc/changed",
			// period 短到测试能等完，但足以覆盖三条事件的连发间隔
			Debounce: &triggerlink.Debounce{Period: 300 * time.Millisecond, Key: "data.doc_id"},
		},
		func(ctx context.Context, in triggerlink.Input) (any, error) {
			calls.Add(1)
			var d struct {
				DocID string `json:"doc_id"`
				Rev   int    `json:"rev"`
			}
			json.Unmarshal(in.Event.Data, &d)
			seen <- fmt.Sprintf("%s@%d", d.DocID, d.Rev)
			return "ok", nil
		}, nil)

	for rev := 1; rev <= 3; rev++ {
		st.sendEvent(t, fmt.Sprintf(`{"name":"doc/changed","data":{"doc_id":"d1","rev":%d}}`, rev))
		time.Sleep(30 * time.Millisecond)
	}
	waitStatus(t, st.store, store.RunCompleted)

	select {
	case got := <-seen:
		if got != "d1@3" {
			t.Fatalf("handler saw %s, want d1@3 (latest event wins)", got)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("handler never ran")
	}
	if n, err := st.store.CountRuns(context.Background()); err != nil || n != 1 {
		t.Fatalf("runs=%d err=%v, want 1 (three events merged)", n, err)
	}

	// 窗口已随派发关闭：再发一条事件应开新窗口、产生第二个 run
	st.sendEvent(t, `{"name":"doc/changed","data":{"doc_id":"d1","rev":4}}`)
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if n, _ := st.store.CountRunsWithStatus(context.Background(), store.RunCompleted); n == 2 {
			if calls.Load() != 2 {
				t.Fatalf("handler calls=%d, want 2", calls.Load())
			}
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	n, _ := st.store.CountRuns(context.Background())
	t.Fatalf("second window did not produce a run (runs=%d)", n)
}

// 不同 debounce key 的事件不互相合并，各自独立成 run。
func TestDebounceKeyIsolation(t *testing.T) {
	st := startStack(t,
		triggerlink.FunctionOpts{
			ID: "index-doc-keys", Event: "doc/changed",
			Debounce: &triggerlink.Debounce{Period: 200 * time.Millisecond, Key: "data.doc_id"},
		},
		func(ctx context.Context, in triggerlink.Input) (any, error) { return "ok", nil }, nil)

	st.sendEvent(t, `{"name":"doc/changed","data":{"doc_id":"d1"}}`)
	st.sendEvent(t, `{"name":"doc/changed","data":{"doc_id":"d2"}}`)
	st.sendEvent(t, `{"name":"doc/changed","data":{"doc_id":"d1"}}`)

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if n, _ := st.store.CountRunsWithStatus(context.Background(), store.RunCompleted); n == 2 {
			total, _ := st.store.CountRuns(context.Background())
			if total != 2 {
				t.Fatalf("runs=%d, want exactly 2 (one per key)", total)
			}
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	n, _ := st.store.CountRuns(context.Background())
	t.Fatalf("expected 2 completed runs, got %d runs", n)
}

// 限流端到端（FR-4.4）：窗口内只放行 limit 个 run 的首次执行，超限的不丢弃，
// 下一窗口继续跑完。
func TestThrottleDefersToNextWindow(t *testing.T) {
	var running atomic.Int32
	st := startStack(t,
		triggerlink.FunctionOpts{
			ID: "rate-limited", Event: "api/call",
			Throttle: &triggerlink.Throttle{Limit: 1, Period: 500 * time.Millisecond},
		},
		func(ctx context.Context, in triggerlink.Input) (any, error) {
			running.Add(1)
			return "ok", nil
		}, nil)

	st.sendEvent(t, `{"name":"api/call","data":{"n":1}}`)
	st.sendEvent(t, `{"name":"api/call","data":{"n":2}}`)
	st.sendEvent(t, `{"name":"api/call","data":{"n":3}}`)

	// 第一个窗口内只应有一个 run 完成
	time.Sleep(250 * time.Millisecond)
	if n, _ := st.store.CountRunsWithStatus(context.Background(), store.RunCompleted); n != 1 {
		t.Fatalf("completed in first window=%d, want 1 (limit=1)", n)
	}
	if total, _ := st.store.CountRuns(context.Background()); total != 3 {
		t.Fatalf("runs=%d, want 3 (over-limit runs are created, just not executed yet)", total)
	}

	// 后续窗口把剩下的跑完，一个都不丢
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if n, _ := st.store.CountRunsWithStatus(context.Background(), store.RunCompleted); n == 3 {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	n, _ := st.store.CountRunsWithStatus(context.Background(), store.RunCompleted)
	t.Fatalf("completed=%d after waiting, want 3 (deferred runs must eventually run)", n)
}
