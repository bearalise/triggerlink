// Package appapi 实现管理 API：运行时注册/同步/查看应用（PRD FR-5.3、9.3）。
// M0 鉴权复用 event-key；后续里程碑拆独立 admin key。
package appapi

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"time"

	"github.com/bearalise/triggerlink/internal/registry"
)

// Handler 处理 /api/v1/apps 与 /api/v1/apps/sync。
type Handler struct {
	reg        *registry.Registry
	key        string
	signingKey string
	client     *http.Client
	// Persist 非 nil 时，注册/同步成功获取清单后先落库再更新内存注册表；
	// 落库失败返回 500，内存注册表不变（注册信息持久化，FR-5.3）。
	Persist func(ctx context.Context, appURL string, fns []registry.Function) error
}

// NewHandler 构造 Handler。client 为 nil 时用 10s 超时的默认客户端。
func NewHandler(reg *registry.Registry, key, signingKey string, client *http.Client) *Handler {
	if client == nil {
		client = &http.Client{Timeout: 10 * time.Second}
	}
	return &Handler{reg: reg, key: key, signingKey: signingKey, client: client}
}

type fnInfo struct {
	ID      string `json:"id"`
	Event   string `json:"event"`
	Retries int    `json:"retries"`
}

func toFnInfo(fns []registry.Function) []fnInfo {
	out := make([]fnInfo, 0, len(fns))
	for _, f := range fns {
		out = append(out, fnInfo{ID: f.ID, Event: f.Event, Retries: f.Retries})
	}
	return out
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(v)
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ") != h.key {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}
	isSync := r.URL.Path == "/api/v1/apps/sync"
	if r.URL.Path != "/api/v1/apps" && !isSync {
		http.Error(w, "not found", http.StatusNotFound)
		return
	}

	switch r.Method {
	case http.MethodGet:
		if isSync {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		type appInfo struct {
			URL       string   `json:"url"`
			Functions []fnInfo `json:"functions"`
		}
		apps := make([]appInfo, 0)
		for _, u := range h.reg.Apps() {
			apps = append(apps, appInfo{URL: u, Functions: toFnInfo(h.reg.FunctionsOf(u))})
		}
		writeJSON(w, http.StatusOK, map[string]any{"apps": apps})

	case http.MethodPost:
		var in struct {
			URL string `json:"url"`
		}
		if err := json.NewDecoder(r.Body).Decode(&in); err != nil || in.URL == "" {
			http.Error(w, "url required", http.StatusBadRequest)
			return
		}
		exists := len(h.reg.FunctionsOf(in.URL)) > 0
		if isSync && !exists {
			http.Error(w, "app not registered", http.StatusNotFound)
			return
		}
		ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
		defer cancel()
		fns, err := registry.Fetch(ctx, h.client, in.URL, h.signingKey)
		if err != nil {
			writeJSON(w, http.StatusBadGateway, map[string]any{"error": err.Error()})
			return
		}
		if h.Persist != nil {
			if err := h.Persist(ctx, in.URL, fns); err != nil {
				writeJSON(w, http.StatusInternalServerError, map[string]any{"error": "persist registration: " + err.Error()})
				return
			}
		}
		h.reg.Sync(in.URL, fns)
		status := http.StatusOK
		if !exists {
			status = http.StatusCreated
		}
		writeJSON(w, status, map[string]any{"url": in.URL, "functions": toFnInfo(fns)})

	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}
