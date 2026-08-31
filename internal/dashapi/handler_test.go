package dashapi

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"

	"github.com/bearalise/triggerlink/internal/registry"
	"github.com/bearalise/triggerlink/internal/store"
)

func newTestHandler(t *testing.T) (*Handler, *store.SQLiteStore, *registry.Registry) {
	t.Helper()
	st, err := store.Open(filepath.Join(t.TempDir(), "test.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() })
	reg := registry.New()
	return NewHandler(st, reg), st, reg
}

// seed 一条 completed run（含两个 step）与一个事件。
func seed(t *testing.T, st *store.SQLiteStore, reg *registry.Registry) string {
	t.Helper()
	ctx := context.Background()
	if _, err := st.InsertEvent(ctx, store.Event{
		ID: "evt_0000000000001aaa", Name: "doc/uploaded",
		Data: json.RawMessage(`{"doc_id":"d1"}`), TS: time.Now(),
	}); err != nil {
		t.Fatal(err)
	}
	runID := "run_0000000000001bbb"
	if _, err := st.CreateRun(ctx, store.Run{
		ID: runID, FunctionID: "fn-a", EventID: "evt_0000000000001aaa",
		EventName: "doc/uploaded", EventData: json.RawMessage(`{"doc_id":"d1"}`), EventTS: time.Now(),
	}); err != nil {
		t.Fatal(err)
	}
	for i, sid := range []string{"parse", "chunk"} {
		if err := st.SaveStepResult(ctx, store.StepResult{
			RunID: runID, StepHash: sid + "-hash", StepID: sid, Op: "run",
			Status: store.StepCompleted, Output: json.RawMessage(`{"n":1}`), Attempt: 1,
		}); err != nil {
			t.Fatalf("step %d: %v", i, err)
		}
	}
	if err := st.CompleteRun(ctx, runID, json.RawMessage(`{"ok":true}`)); err != nil {
		t.Fatal(err)
	}
	reg.Register(registry.Function{ID: "fn-a", Event: "doc/uploaded", AppURL: "http://app"})
	return runID
}

func doGet(t *testing.T, h http.Handler, path string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(http.MethodGet, path, nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec
}

func decode(t *testing.T, rec *httptest.ResponseRecorder, v any) {
	t.Helper()
	if err := json.Unmarshal(rec.Body.Bytes(), v); err != nil {
		t.Fatalf("decode %s: %v", rec.Body.String(), err)
	}
}

func TestRunsListAndDetail(t *testing.T) {
	h, st, reg := newTestHandler(t)
	runID := seed(t, st, reg)

	rec := doGet(t, h, "/api/v1/runs")
	if rec.Code != http.StatusOK {
		t.Fatalf("list: %d %s", rec.Code, rec.Body)
	}
	var list struct {
		Runs []struct {
			ID         string `json:"id"`
			FunctionID string `json:"function_id"`
			Status     string `json:"status"`
			Attempt    int    `json:"attempt"`
		} `json:"runs"`
	}
	decode(t, rec, &list)
	if len(list.Runs) != 1 || list.Runs[0].ID != runID || list.Runs[0].Status != "completed" {
		t.Fatalf("%+v", list)
	}

	rec = doGet(t, h, "/api/v1/runs?status=running")
	var empty struct {
		Runs []any `json:"runs"`
	}
	decode(t, rec, &empty)
	if len(empty.Runs) != 0 {
		t.Fatalf("filter: %+v", empty)
	}

	rec = doGet(t, h, "/api/v1/runs?status=bogus")
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("bad status: %d", rec.Code)
	}

	rec = doGet(t, h, "/api/v1/runs?event_id=evt_0000000000001aaa")
	var byEv struct {
		Runs []struct {
			ID string `json:"id"`
		} `json:"runs"`
	}
	decode(t, rec, &byEv)
	if len(byEv.Runs) != 1 || byEv.Runs[0].ID != runID {
		t.Fatalf("event_id filter: %+v", byEv)
	}
	rec = doGet(t, h, "/api/v1/runs?event_id=evt_none")
	decode(t, rec, &byEv)
	if len(byEv.Runs) != 0 {
		t.Fatalf("event_id miss: %+v", byEv)
	}

	rec = doGet(t, h, "/api/v1/runs/"+runID)
	if rec.Code != http.StatusOK {
		t.Fatalf("detail: %d", rec.Code)
	}
	var detail struct {
		Run struct {
			ID        string          `json:"id"`
			EventData json.RawMessage `json:"event_data"`
			Output    json.RawMessage `json:"output"`
		} `json:"run"`
		Steps []struct {
			StepID    string     `json:"step_id"`
			Status    string     `json:"status"`
			UpdatedAt *time.Time `json:"updated_at"`
		} `json:"steps"`
	}
	decode(t, rec, &detail)
	if detail.Run.ID != runID || string(detail.Run.Output) != `{"ok":true}` {
		t.Fatalf("%+v", detail.Run)
	}
	if len(detail.Steps) != 2 || detail.Steps[0].StepID != "parse" || detail.Steps[0].UpdatedAt == nil {
		t.Fatalf("%+v", detail.Steps)
	}

	rec = doGet(t, h, "/api/v1/runs/run_missing")
	if rec.Code != http.StatusNotFound {
		t.Fatalf("missing: %d", rec.Code)
	}
}

func TestEventsAndFunctions(t *testing.T) {
	h, st, reg := newTestHandler(t)
	runID := seed(t, st, reg)

	rec := doGet(t, h, "/api/v1/events")
	var ev struct {
		Events []struct {
			ID   string   `json:"id"`
			Name string   `json:"name"`
			Runs []string `json:"runs"`
		} `json:"events"`
	}
	decode(t, rec, &ev)
	if len(ev.Events) != 1 || ev.Events[0].Name != "doc/uploaded" {
		t.Fatalf("%+v", ev)
	}
	if len(ev.Events[0].Runs) != 1 || ev.Events[0].Runs[0] != runID {
		t.Fatalf("runs backlink: %+v", ev.Events[0])
	}

	rec = doGet(t, h, "/api/v1/functions")
	var fn struct {
		Functions []struct {
			ID       string `json:"id"`
			Event    string `json:"event"`
			AppURL   string `json:"app_url"`
			Stats24h struct {
				Total     int `json:"total"`
				Completed int `json:"completed"`
				Failed    int `json:"failed"`
			} `json:"stats_24h"`
		} `json:"functions"`
	}
	decode(t, rec, &fn)
	if len(fn.Functions) != 1 || fn.Functions[0].ID != "fn-a" || fn.Functions[0].AppURL != "http://app" {
		t.Fatalf("%+v", fn)
	}
	if s := fn.Functions[0].Stats24h; s.Total != 1 || s.Completed != 1 || s.Failed != 0 {
		t.Fatalf("stats: %+v", s)
	}
}

func TestMethodAndNotFound(t *testing.T) {
	h, _, _ := newTestHandler(t)
	req := httptest.NewRequest(http.MethodPost, "/api/v1/runs", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("post: %d", rec.Code)
	}
	if rec := doGet(t, h, "/api/v1/unknown"); rec.Code != http.StatusNotFound {
		t.Fatalf("unknown: %d", rec.Code)
	}
}

func TestCancelRunEndpoint(t *testing.T) {
	h, st, _ := newTestHandler(t)
	ctx := context.Background()
	if _, err := st.CreateRun(ctx, store.Run{
		ID: "run_c1", FunctionID: "fn-a", EventID: "evt_c1",
		EventName: "e/x", EventData: json.RawMessage(`{}`), EventTS: time.Now(),
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := st.MarkRunRunning(ctx, "run_c1"); err != nil {
		t.Fatal(err)
	}

	post := func(path string) *httptest.ResponseRecorder {
		t.Helper()
		req := httptest.NewRequest(http.MethodPost, path, nil)
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		return rec
	}

	// running → cancelled
	rec := post("/api/v1/runs/run_c1/cancel")
	if rec.Code != http.StatusOK {
		t.Fatalf("cancel: %d %s", rec.Code, rec.Body)
	}
	var out struct {
		Run runDTO `json:"run"`
	}
	decode(t, rec, &out)
	if out.Run.Status != "cancelled" || out.Run.EndedAt == nil {
		t.Fatalf("cancelled dto: %+v", out.Run)
	}

	// 已终态 → 409;不存在 → 404
	if rec := post("/api/v1/runs/run_c1/cancel"); rec.Code != http.StatusConflict {
		t.Fatalf("re-cancel: %d", rec.Code)
	}
	if rec := post("/api/v1/runs/run_none/cancel"); rec.Code != http.StatusNotFound {
		t.Fatalf("missing: %d", rec.Code)
	}
	// GET 打到 cancel 后缀路径 → 视为 getRun(id="run_c1/cancel") → 404
	if rec := doGet(t, h, "/api/v1/runs/run_c1/cancel"); rec.Code != http.StatusNotFound {
		t.Fatalf("get cancel path: %d", rec.Code)
	}
}

// TestReplayRunEndpoint：POST /api/v1/runs/{id}/replay（FR-6.2）——
// 以原 run 的触发事件快照创建全新 run（新 ID、Queued、at=now 入队），
// 不继承 step memo；不存在的 run → 404。
func TestReplayRunEndpoint(t *testing.T) {
	h, st, reg := newTestHandler(t)
	ctx := context.Background()
	runID := seed(t, st, reg) // seed 一个 completed run（含两个 step memo）

	post := func(path string) *httptest.ResponseRecorder {
		t.Helper()
		req := httptest.NewRequest(http.MethodPost, path, nil)
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		return rec
	}

	// 不存在 → 404
	if rec := post("/api/v1/runs/run_none/replay"); rec.Code != http.StatusNotFound {
		t.Fatalf("missing: %d", rec.Code)
	}

	rec := post("/api/v1/runs/" + runID + "/replay")
	if rec.Code != http.StatusOK {
		t.Fatalf("replay: %d %s", rec.Code, rec.Body)
	}
	var out struct {
		RunID string `json:"run_id"`
	}
	decode(t, rec, &out)
	if out.RunID == "" || out.RunID == runID {
		t.Fatalf("new run id: %q", out.RunID)
	}

	// 新 run 继承触发事件快照，状态 Queued
	nr, err := st.GetRun(ctx, out.RunID)
	if err != nil {
		t.Fatal(err)
	}
	if nr.Status != store.RunQueued || nr.FunctionID != "fn-a" ||
		nr.EventID != "evt_0000000000001aaa" || nr.EventName != "doc/uploaded" ||
		string(nr.EventData) != `{"doc_id":"d1"}` {
		t.Fatalf("replay run: %+v", nr)
	}
	// step memo 按 run_id 隔离：不继承原 run 的 memo
	steps, err := st.StepResults(ctx, out.RunID)
	if err != nil || len(steps) != 0 {
		t.Fatalf("replay run must not inherit memo: %+v err=%v", steps, err)
	}
	// 已以 at=now 入队：立即可租赁
	items, err := st.LeaseBatch(ctx, time.Now(), time.Minute, 4)
	if err != nil || len(items) != 1 || items[0].RunID != out.RunID {
		t.Fatalf("queue items: %+v err=%v", items, err)
	}
}

// TestPauseResumeEndpoints：POST /api/v1/functions/{id}/pause|resume（FR-7.3）——
// 200 {} 且状态翻转；functions 列表带 paused 标志；未注册函数也可暂停（幂等置位）。
func TestPauseResumeEndpoints(t *testing.T) {
	h, st, reg := newTestHandler(t)
	seed(t, st, reg)

	post := func(path string) *httptest.ResponseRecorder {
		t.Helper()
		req := httptest.NewRequest(http.MethodPost, path, nil)
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		return rec
	}
	pausedFlag := func() bool {
		t.Helper()
		rec := doGet(t, h, "/api/v1/functions")
		var out struct {
			Functions []struct {
				ID     string `json:"id"`
				Paused bool   `json:"paused"`
			} `json:"functions"`
		}
		decode(t, rec, &out)
		for _, f := range out.Functions {
			if f.ID == "fn-a" {
				return f.Paused
			}
		}
		t.Fatal("fn-a missing in functions list")
		return false
	}

	if pausedFlag() {
		t.Fatal("fn-a must not be paused initially")
	}
	if rec := post("/api/v1/functions/fn-a/pause"); rec.Code != http.StatusOK {
		t.Fatalf("pause: %d %s", rec.Code, rec.Body)
	}
	if paused, _ := st.IsFunctionPaused(context.Background(), "fn-a"); !paused {
		t.Fatal("fn-a not paused in store")
	}
	if !pausedFlag() {
		t.Fatal("functions list must show paused=true")
	}
	if rec := post("/api/v1/functions/fn-a/resume"); rec.Code != http.StatusOK {
		t.Fatalf("resume: %d %s", rec.Code, rec.Body)
	}
	if pausedFlag() {
		t.Fatal("functions list must show paused=false after resume")
	}
	// 未注册函数也可暂停（幂等置位，200 {}）
	if rec := post("/api/v1/functions/fn-ghost/pause"); rec.Code != http.StatusOK {
		t.Fatalf("pause ghost: %d", rec.Code)
	}
}
