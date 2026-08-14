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
	"sync/atomic"
	"testing"
	"time"

	"github.com/triggerlink/triggerlink/internal/appapi"
	"github.com/triggerlink/triggerlink/internal/auth"
	"github.com/triggerlink/triggerlink/internal/dashapi"
	"github.com/triggerlink/triggerlink/internal/eventapi"
	"github.com/triggerlink/triggerlink/internal/executor"
	"github.com/triggerlink/triggerlink/internal/registry"
	"github.com/triggerlink/triggerlink/internal/runner"
	"github.com/triggerlink/triggerlink/internal/store"
	triggerlink "github.com/triggerlink/triggerlink/sdk"
	"github.com/triggerlink/triggerlink/sdk/step"
)

type stack struct {
	store    *store.SQLiteStore
	eventURL string
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
		Stream: stream,
	}
	if beforeStart != nil {
		beforeStart(ctx, st, reg, app.URL)
	}
	go rnr.Run(ctx)
	go exec.Run(ctx)

	eventSrv := httptest.NewServer(eventapi.NewHandler(st, "dev-key", stream))
	t.Cleanup(eventSrv.Close)
	return &stack{store: st, eventURL: eventSrv.URL}, reg, app.URL
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
	persist := func(st *store.SQLiteStore) func(context.Context, string, []registry.Function) error {
		return func(ctx context.Context, appURL string, fns []registry.Function) error {
			rfns := make([]store.RegisteredFunction, 0, len(fns))
			for _, f := range fns {
				rfns = append(rfns, store.RegisteredFunction{ID: f.ID, AppURL: appURL, Event: f.Event, Retries: f.Retries})
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
		Stream: stream,
	}
	go rnr.Run(runCtx)
	go exec.Run(runCtx)
	eventSrv := httptest.NewServer(eventapi.NewHandler(st2, "dev-key", stream))
	t.Cleanup(eventSrv.Close)

	s := &stack{store: st2, eventURL: eventSrv.URL}
	s.sendEvent(t, `{"name":"doc/uploaded","data":{}}`)
	waitStatus(t, st2, store.RunCompleted)
}
