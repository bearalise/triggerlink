// Package dashapi 实现 Dashboard 的管理 API（PRD 9.3）：以只读端点为主，
// 另提供写端点：POST /api/v1/runs/{id}/cancel（取消在途 run）、
// POST /api/v1/runs/{id}/replay（失败 run 一键重跑，FR-6.2）、
// POST /api/v1/functions/{id}/pause|resume（函数暂停/恢复，FR-7.3）、
// POST|GET /api/v1/webhooks 与 DELETE /api/v1/webhooks/{id}（webhook 触发端点管理，FR-3.4）。
// 鉴权由外层中间件负责（internal/auth），本包不含鉴权逻辑。
package dashapi

import (
	"database/sql"
	"encoding/json"
	"errors"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/bearalise/triggerlink/internal/ids"
	"github.com/bearalise/triggerlink/internal/metrics"
	"github.com/bearalise/triggerlink/internal/registry"
	"github.com/bearalise/triggerlink/internal/store"
)

// Handler 处理 /api/v1/{events,functions,runs} 只读端点及写端点
//（run 取消/重跑、函数暂停/恢复、webhook 触发端点管理 FR-3.4）。
type Handler struct {
	st  store.Store
	reg *registry.Registry
}

// NewHandler 构造 Handler。
func NewHandler(st store.Store, reg *registry.Registry) *Handler {
	return &Handler{st: st, reg: reg}
}

// statusToStore 把 API 契约的小写状态映射到 store 的大写常量。
var statusToStore = map[string]string{
	"queued": store.RunQueued, "running": store.RunRunning,
	"completed": store.RunCompleted, "failed": store.RunFailed,
	"cancelled": store.RunCancelled,
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(v)
}

func writeErr(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]any{"error": msg})
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	path := r.URL.Path
	// 写端点：取消/重跑 run、暂停/恢复函数（其余全部为只读 GET）
	if r.Method == http.MethodPost && strings.HasPrefix(path, "/api/v1/runs/") {
		id := strings.TrimPrefix(path, "/api/v1/runs/")
		switch {
		case strings.HasSuffix(id, "/cancel"):
			h.cancelRun(w, r, strings.TrimSuffix(id, "/cancel"))
		case strings.HasSuffix(id, "/replay"):
			h.replayRun(w, r, strings.TrimSuffix(id, "/replay"))
		default:
			writeErr(w, http.StatusNotFound, "not found")
		}
		return
	}
	if r.Method == http.MethodPost && strings.HasPrefix(path, "/api/v1/functions/") {
		id := strings.TrimPrefix(path, "/api/v1/functions/")
		switch {
		case strings.HasSuffix(id, "/pause"):
			h.setFunctionPaused(w, r, strings.TrimSuffix(id, "/pause"), true)
		case strings.HasSuffix(id, "/resume"):
			h.setFunctionPaused(w, r, strings.TrimSuffix(id, "/resume"), false)
		default:
			writeErr(w, http.StatusNotFound, "not found")
		}
		return
	}
	// webhook 触发端点管理（FR-3.4）
	if path == "/api/v1/webhooks" || strings.HasPrefix(path, "/api/v1/webhooks/") {
		switch {
		case r.Method == http.MethodPost && path == "/api/v1/webhooks":
			h.createWebhook(w, r)
		case r.Method == http.MethodGet && path == "/api/v1/webhooks":
			h.listWebhooks(w, r)
		case r.Method == http.MethodDelete && strings.HasPrefix(path, "/api/v1/webhooks/"):
			h.deleteWebhook(w, r, strings.TrimPrefix(path, "/api/v1/webhooks/"))
		default:
			writeErr(w, http.StatusMethodNotAllowed, "method not allowed")
		}
		return
	}
	if r.Method != http.MethodGet {
		writeErr(w, http.StatusMethodNotAllowed, "method not allowed")
		return
	}
	switch {
	case path == "/api/v1/events":
		h.listEvents(w, r)
	case path == "/api/v1/functions":
		h.listFunctions(w, r)
	case path == "/api/v1/runs":
		h.listRuns(w, r)
	case strings.HasPrefix(path, "/api/v1/runs/"):
		h.getRun(w, r, strings.TrimPrefix(path, "/api/v1/runs/"))
	default:
		writeErr(w, http.StatusNotFound, "not found")
	}
}

func queryLimit(r *http.Request) (int, error) {
	s := r.URL.Query().Get("limit")
	if s == "" {
		return 0, nil
	}
	n, err := strconv.Atoi(s)
	if err != nil || n <= 0 {
		return 0, err
	}
	return n, nil
}

// --- DTO ---

type eventDTO struct {
	ID         string          `json:"id"`
	Name       string          `json:"name"`
	Data       json.RawMessage `json:"data"`
	TS         time.Time       `json:"ts"`
	ReceivedAt time.Time       `json:"received_at"`
	Runs       []string        `json:"runs"`
}

type runDTO struct {
	ID         string     `json:"id"`
	FunctionID string     `json:"function_id"`
	Status     string     `json:"status"`
	EventID    string     `json:"event_id"`
	EventName  string     `json:"event_name"`
	Attempt    int        `json:"attempt"`
	CreatedAt  time.Time  `json:"created_at"`
	EndedAt    *time.Time `json:"ended_at,omitempty"`
}

func toRunDTO(r store.Run) runDTO {
	return runDTO{
		ID: r.ID, FunctionID: r.FunctionID, Status: strings.ToLower(r.Status),
		EventID: r.EventID, EventName: r.EventName, Attempt: r.Attempt,
		CreatedAt: r.CreatedAt, EndedAt: r.EndedAt,
	}
}

type runDetailDTO struct {
	runDTO
	EventData json.RawMessage `json:"event_data"`
	EventTS   time.Time       `json:"event_ts"`
	// Batch 为真时 EventData 是事件对象数组（FR-4.7 批触发），前端据此改用批视图。
	Batch  bool            `json:"batch,omitempty"`
	Output json.RawMessage `json:"output,omitempty"`
	Error  string          `json:"error,omitempty"`
}

type stepDTO struct {
	StepID    string          `json:"step_id"`
	Op        string          `json:"op"`
	Status    string          `json:"status"`
	Output    json.RawMessage `json:"output,omitempty"`
	Error     string          `json:"error,omitempty"`
	Attempt   int             `json:"attempt"`
	UpdatedAt *time.Time      `json:"updated_at,omitempty"`
}

type statsDTO struct {
	Total     int `json:"total"`
	Completed int `json:"completed"`
	Failed    int `json:"failed"`
}

type functionDTO struct {
	ID     string `json:"id"`
	Event  string `json:"event"`
	AppURL string `json:"app_url"`
	Paused bool   `json:"paused"`
	// 流控徽标（FR-4.4/4.5/4.7）：只报"配了什么"，具体参数在应用代码里，
	// Dashboard 不复述——避免注册表与展示各存一份配置定义。
	FlowControl []string `json:"flow_control,omitempty"`
	Stats24h    statsDTO `json:"stats_24h"`
}

// flowControlLabels 返回该函数启用的流控种类，顺序固定便于前端稳定渲染。
func flowControlLabels(f registry.Function) []string {
	var out []string
	if f.Debounce != nil {
		out = append(out, "debounce")
	}
	if f.Throttle != nil {
		out = append(out, "throttle")
	}
	if f.Batch != nil {
		out = append(out, "batch")
	}
	return out
}

// --- handlers ---

func (h *Handler) listEvents(w http.ResponseWriter, r *http.Request) {
	limit, err := queryLimit(r)
	if err != nil {
		writeErr(w, http.StatusBadRequest, "invalid limit")
		return
	}
	q := r.URL.Query()
	events, err := h.st.ListEvents(r.Context(), store.ListEventsOptions{
		Name: q.Get("name"), Before: q.Get("before"), Limit: limit,
	})
	if err != nil {
		writeErr(w, http.StatusInternalServerError, "list events")
		return
	}
	ids := make([]string, len(events))
	for i, e := range events {
		ids[i] = e.ID
	}
	backlinks, err := h.st.RunIDsByEventIDs(r.Context(), ids)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, "list event runs")
		return
	}
	out := make([]eventDTO, 0, len(events))
	for _, e := range events {
		runs := backlinks[e.ID]
		if runs == nil {
			runs = []string{}
		}
		out = append(out, eventDTO{
			ID: e.ID, Name: e.Name, Data: e.Data,
			TS: e.TS, ReceivedAt: e.ReceivedAt, Runs: runs,
		})
	}
	writeJSON(w, http.StatusOK, map[string]any{"events": out})
}

func (h *Handler) listRuns(w http.ResponseWriter, r *http.Request) {
	limit, err := queryLimit(r)
	if err != nil {
		writeErr(w, http.StatusBadRequest, "invalid limit")
		return
	}
	q := r.URL.Query()
	var statusStore string
	if s := q.Get("status"); s != "" {
		mapped, ok := statusToStore[s]
		if !ok {
			writeErr(w, http.StatusBadRequest, "invalid status")
			return
		}
		statusStore = mapped
	}
	runs, err := h.st.ListRuns(r.Context(), store.ListRunsOptions{
		FunctionID: q.Get("function_id"), Status: statusStore, EventID: q.Get("event_id"),
		Before: q.Get("before"), Limit: limit,
	})
	if err != nil {
		writeErr(w, http.StatusInternalServerError, "list runs")
		return
	}
	out := make([]runDTO, 0, len(runs))
	for _, run := range runs {
		out = append(out, toRunDTO(run))
	}
	writeJSON(w, http.StatusOK, map[string]any{"runs": out})
}

func (h *Handler) getRun(w http.ResponseWriter, r *http.Request, id string) {
	run, err := h.st.GetRun(r.Context(), id)
	if errors.Is(err, sql.ErrNoRows) {
		writeErr(w, http.StatusNotFound, "run not found")
		return
	}
	if err != nil {
		writeErr(w, http.StatusInternalServerError, "get run")
		return
	}
	recs, err := h.st.ListStepRecords(r.Context(), id)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, "list steps")
		return
	}
	steps := make([]stepDTO, 0, len(recs))
	for _, s := range recs {
		steps = append(steps, stepDTO{
			StepID: s.StepID, Op: s.Op, Status: s.Status,
			Output: s.Output, Error: s.Error, Attempt: s.Attempt, UpdatedAt: s.UpdatedAt,
		})
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"run": runDetailDTO{
			runDTO:    toRunDTO(run),
			EventData: run.EventData, EventTS: run.EventTS, Batch: run.Batch,
			Output: run.Output, Error: run.Error,
		},
		"steps": steps,
	})
}

// cancelRun 处理 POST /api/v1/runs/{id}/cancel：把 Queued/Running 的 run 置为终态
// Cancelled 并清掉其 pending 队列项；已终态返回 409（幂等语义交给客户端判断）。
func (h *Handler) cancelRun(w http.ResponseWriter, r *http.Request, id string) {
	if id == "" {
		writeErr(w, http.StatusBadRequest, "run id required")
		return
	}
	if _, err := h.st.GetRun(r.Context(), id); errors.Is(err, sql.ErrNoRows) {
		writeErr(w, http.StatusNotFound, "run not found")
		return
	} else if err != nil {
		writeErr(w, http.StatusInternalServerError, "get run")
		return
	}
	transitioned, err := h.st.CancelRun(r.Context(), id)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, "cancel run")
		return
	}
	if !transitioned {
		writeErr(w, http.StatusConflict, "run already in terminal state")
		return
	}
	run, err := h.st.GetRun(r.Context(), id)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, "get run")
		return
	}
	metrics.RunFinished(metrics.StatusCancelled, time.Since(run.CreatedAt))
	writeJSON(w, http.StatusOK, map[string]any{"run": toRunDTO(run)})
}

// replayRun 处理 POST /api/v1/runs/{id}/replay（FR-6.2）：以原 run 的触发事件快照
// （event_id/name/data/ts）创建全新 run（新随机 ID，不经事件路由，不继承 step memo）
// 并以 at=now 入队。任何状态的 run（含 Failed/Completed/Cancelled）都可重跑。
func (h *Handler) replayRun(w http.ResponseWriter, r *http.Request, id string) {
	if id == "" {
		writeErr(w, http.StatusBadRequest, "run id required")
		return
	}
	run, err := h.st.GetRun(r.Context(), id)
	if errors.Is(err, sql.ErrNoRows) {
		writeErr(w, http.StatusNotFound, "run not found")
		return
	}
	if err != nil {
		writeErr(w, http.StatusInternalServerError, "get run")
		return
	}
	newID := ids.NewID("run")
	created, err := h.st.CreateRun(r.Context(), store.Run{
		ID:         newID,
		FunctionID: run.FunctionID,
		Status:     store.RunQueued,
		EventID:    run.EventID,
		EventName:  run.EventName,
		EventData:  run.EventData,
		EventTS:    run.EventTS,
	})
	if err != nil || !created {
		writeErr(w, http.StatusInternalServerError, "create replay run")
		return
	}
	if err := h.st.Enqueue(r.Context(), store.QueueItem{
		ID:         ids.NewID("qi"),
		FunctionID: run.FunctionID,
		RunID:      newID,
		Score:      time.Now().UnixNano(),
		At:         time.Now(),
		Status:     store.QueuePending,
	}); err != nil {
		writeErr(w, http.StatusInternalServerError, "enqueue replay run")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"run_id": newID})
}

// setFunctionPaused 处理 POST /api/v1/functions/{id}/pause 与 /resume（FR-7.3）：
// 写入独立的 paused_functions 表（跨重启保留，且不随 functions 注册表覆盖丢失）。
// 函数未注册也可暂停/恢复（幂等置位），统一返回 200 {}。
func (h *Handler) setFunctionPaused(w http.ResponseWriter, r *http.Request, id string, paused bool) {
	if id == "" {
		writeErr(w, http.StatusBadRequest, "function id required")
		return
	}
	if err := h.st.SetFunctionPaused(r.Context(), id, paused); err != nil {
		writeErr(w, http.StatusInternalServerError, "set function paused")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{})
}

// --- Webhook 触发端点管理（PRD FR-3.4） ---

// webhookDTO 是管理 API 中 webhook 的对外形态。Secret 仅在创建响应中返回
//（创建后客户端无法再回读完整 secret），列表不回显。
type webhookDTO struct {
	ID        string    `json:"id"`
	URL       string    `json:"url"`
	EventName string    `json:"event_name"`
	Secret    string    `json:"secret,omitempty"`
	CreatedAt time.Time `json:"created_at"`
}

// createWebhook 处理 POST /api/v1/webhooks：body {"event_name", "secret"?}，
// secret 缺省自动生成；返回的 secret 仅此一次完整可见。
func (h *Handler) createWebhook(w http.ResponseWriter, r *http.Request) {
	var in struct {
		EventName string `json:"event_name"`
		Secret    string `json:"secret"`
	}
	if err := json.NewDecoder(r.Body).Decode(&in); err != nil {
		writeErr(w, http.StatusBadRequest, "invalid json")
		return
	}
	if in.EventName == "" {
		writeErr(w, http.StatusBadRequest, "event_name required")
		return
	}
	if in.Secret == "" {
		in.Secret = ids.NewID("whsec")
	}
	wh := store.Webhook{ID: ids.NewID("whk"), EventName: in.EventName, Secret: in.Secret, CreatedAt: time.Now()}
	if err := h.st.CreateWebhook(r.Context(), wh); err != nil {
		writeErr(w, http.StatusInternalServerError, "create webhook")
		return
	}
	writeJSON(w, http.StatusOK, webhookDTO{
		ID: wh.ID, URL: "/hooks/" + wh.ID, EventName: wh.EventName, Secret: wh.Secret, CreatedAt: wh.CreatedAt,
	})
}

// listWebhooks 处理 GET /api/v1/webhooks：列表不回显 secret。
func (h *Handler) listWebhooks(w http.ResponseWriter, r *http.Request) {
	whs, err := h.st.ListWebhooks(r.Context())
	if err != nil {
		writeErr(w, http.StatusInternalServerError, "list webhooks")
		return
	}
	out := make([]webhookDTO, 0, len(whs))
	for _, wh := range whs {
		out = append(out, webhookDTO{ID: wh.ID, URL: "/hooks/" + wh.ID, EventName: wh.EventName, CreatedAt: wh.CreatedAt})
	}
	writeJSON(w, http.StatusOK, map[string]any{"webhooks": out})
}

// deleteWebhook 处理 DELETE /api/v1/webhooks/{id}；不存在 → 404。
func (h *Handler) deleteWebhook(w http.ResponseWriter, r *http.Request, id string) {
	if id == "" {
		writeErr(w, http.StatusBadRequest, "webhook id required")
		return
	}
	deleted, err := h.st.DeleteWebhook(r.Context(), id)
	if err != nil {
		writeErr(w, http.StatusInternalServerError, "delete webhook")
		return
	}
	if !deleted {
		writeErr(w, http.StatusNotFound, "webhook not found")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{})
}

func (h *Handler) listFunctions(w http.ResponseWriter, r *http.Request) {
	stats, err := h.st.FunctionStats24h(r.Context(), time.Now().Add(-24*time.Hour))
	if err != nil {
		writeErr(w, http.StatusInternalServerError, "function stats")
		return
	}
	paused, err := h.st.PausedFunctionIDs(r.Context())
	if err != nil {
		writeErr(w, http.StatusInternalServerError, "paused functions")
		return
	}
	out := make([]functionDTO, 0)
	for _, appURL := range h.reg.Apps() {
		for _, f := range h.reg.FunctionsOf(appURL) {
			s := stats[f.ID]
			out = append(out, functionDTO{
				ID: f.ID, Event: f.Event, AppURL: appURL, Paused: paused[f.ID],
				FlowControl: flowControlLabels(f),
				Stats24h:    statsDTO{Total: s.Total, Completed: s.Completed, Failed: s.Failed},
			})
		}
	}
	writeJSON(w, http.StatusOK, map[string]any{"functions": out})
}
