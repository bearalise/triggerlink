// Package dashapi 实现 Dashboard 的管理 API（PRD 9.3）：以只读端点为主，
// 另提供 POST /api/v1/runs/{id}/cancel 用于主动取消在途 run。
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

	"github.com/bearalise/triggerlink/internal/registry"
	"github.com/bearalise/triggerlink/internal/store"
)

// Handler 处理 /api/v1/{events,functions,runs} 只读端点。
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
	// 唯一的写端点：取消 run（其余全部为只读 GET）
	if r.Method == http.MethodPost && strings.HasSuffix(path, "/cancel") &&
		strings.HasPrefix(path, "/api/v1/runs/") {
		h.cancelRun(w, r, strings.TrimSuffix(strings.TrimPrefix(path, "/api/v1/runs/"), "/cancel"))
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
	Output    json.RawMessage `json:"output,omitempty"`
	Error     string          `json:"error,omitempty"`
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
	ID       string   `json:"id"`
	Event    string   `json:"event"`
	AppURL   string   `json:"app_url"`
	Stats24h statsDTO `json:"stats_24h"`
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
			EventData: run.EventData, EventTS: run.EventTS,
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
	writeJSON(w, http.StatusOK, map[string]any{"run": toRunDTO(run)})
}

func (h *Handler) listFunctions(w http.ResponseWriter, r *http.Request) {
	stats, err := h.st.FunctionStats24h(r.Context(), time.Now().Add(-24*time.Hour))
	if err != nil {
		writeErr(w, http.StatusInternalServerError, "function stats")
		return
	}
	out := make([]functionDTO, 0)
	for _, appURL := range h.reg.Apps() {
		for _, f := range h.reg.FunctionsOf(appURL) {
			s := stats[f.ID]
			out = append(out, functionDTO{
				ID: f.ID, Event: f.Event, AppURL: appURL,
				Stats24h: statsDTO{Total: s.Total, Completed: s.Completed, Failed: s.Failed},
			})
		}
	}
	writeJSON(w, http.StatusOK, map[string]any{"functions": out})
}
