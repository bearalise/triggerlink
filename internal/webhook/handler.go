// Package webhook 实现 Webhook 触发端点（PRD FR-3.4）：外部系统向
// POST /hooks/{id} 推送，HMAC 验签（internal/sign，与平台↔应用同一格式）通过后
// 转换为内部事件并路由。公开路径不套 basic auth，安全完全依赖签名。
// 顺序承诺与 eventapi 一致：先 InsertEvent 落库成功再入 stream 再返回 200（先 WAL 后处理）。
package webhook

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/bearalise/triggerlink/internal/ids"
	"github.com/bearalise/triggerlink/internal/sign"
	"github.com/bearalise/triggerlink/internal/store"
)

// 签名时间戳容差（防重放窗口），与平台↔应用验签一致。
const signatureTolerance = 5 * time.Minute

// Handler 处理 POST /hooks/{id}。
type Handler struct {
	store  store.Store
	stream chan<- store.Event
}

// NewHandler 构造 Handler。stream 与 eventapi 共用，由 main 创建并交给 Runner 消费。
func NewHandler(st store.Store, stream chan<- store.Event) *Handler {
	return &Handler{store: st, stream: stream}
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(v)
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		writeJSON(w, http.StatusMethodNotAllowed, map[string]any{"error": "method not allowed"})
		return
	}
	id := strings.TrimPrefix(r.URL.Path, "/hooks/")
	if id == "" || strings.Contains(id, "/") {
		writeJSON(w, http.StatusNotFound, map[string]any{"error": "webhook not found"})
		return
	}
	wh, err := h.store.GetWebhook(r.Context(), id)
	if errors.Is(err, sql.ErrNoRows) {
		writeJSON(w, http.StatusNotFound, map[string]any{"error": "webhook not found"})
		return
	}
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]any{"error": "get webhook"})
		return
	}
	// body 先读全（验签需要原始字节），限 4MB 防滥用
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, 4<<20))
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": "read body"})
		return
	}
	if err := sign.Verify(wh.Secret, r.Header.Get("X-TriggerLink-Signature"), body, time.Now(), signatureTolerance); err != nil {
		writeJSON(w, http.StatusUnauthorized, map[string]any{"error": "invalid signature"})
		return
	}

	// body 必须是 JSON：对象直接作为事件 data；其他 JSON 值包成 {"payload": ...}
	raw := json.RawMessage(bytes.TrimSpace(body))
	if !json.Valid(raw) {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": "invalid json"})
		return
	}
	data := raw
	if len(raw) == 0 || raw[0] != '{' {
		wrapped, err := json.Marshal(map[string]json.RawMessage{"payload": raw})
		if err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]any{"error": "invalid json"})
			return
		}
		data = wrapped
	}

	e := store.Event{ID: ids.NewID("evt"), Name: wh.EventName, Data: data, TS: time.Now()}
	inserted, err := h.store.InsertEvent(r.Context(), e)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]any{"error": "store error"})
		return
	}
	if inserted {
		h.stream <- e
	}
	writeJSON(w, http.StatusOK, map[string]any{"id": e.ID})
}
