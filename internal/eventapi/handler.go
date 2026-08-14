// Package eventapi 实现 Event API（PRD FR-1）：鉴权、校验、落库、入 stream。
// 顺序承诺：先持久化成功再入 stream 再返回 200（先 WAL 后处理）。
package eventapi

import (
	"encoding/json"
	"net/http"
	"strings"
	"time"

	"github.com/bearalise/triggerlink/internal/ids"
	"github.com/bearalise/triggerlink/internal/store"
)

// Handler 处理 POST /v1/events。
type Handler struct {
	store  store.Store
	key    string
	stream chan<- store.Event
}

// NewHandler 构造 Handler。stream 为有缓冲 channel，由 main 创建并交给 Runner 消费。
func NewHandler(st store.Store, key string, stream chan<- store.Event) *Handler {
	return &Handler{store: st, key: key, stream: stream}
}

type eventIn struct {
	ID   string          `json:"id"`
	Name string          `json:"name"`
	Data json.RawMessage `json:"data"`
	TS   time.Time       `json:"ts"`
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ") != h.key {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}
	var raw json.RawMessage
	if err := json.NewDecoder(r.Body).Decode(&raw); err != nil {
		http.Error(w, "invalid json", http.StatusBadRequest)
		return
	}
	var ins []eventIn
	if len(raw) > 0 && raw[0] == '[' {
		if err := json.Unmarshal(raw, &ins); err != nil {
			http.Error(w, "invalid event array", http.StatusBadRequest)
			return
		}
	} else {
		var in eventIn
		if err := json.Unmarshal(raw, &in); err != nil {
			http.Error(w, "invalid event", http.StatusBadRequest)
			return
		}
		ins = []eventIn{in}
	}
	if len(ins) == 0 {
		http.Error(w, "empty batch", http.StatusBadRequest)
		return
	}

	idsOut := make([]string, 0, len(ins))
	for _, in := range ins {
		if in.Name == "" {
			http.Error(w, "event name required", http.StatusBadRequest)
			return
		}
		if in.Data == nil {
			in.Data = json.RawMessage(`{}`)
		}
		if in.ID == "" {
			in.ID = ids.NewID("evt")
		}
		if in.TS.IsZero() {
			in.TS = time.Now()
		}
		e := store.Event{ID: in.ID, Name: in.Name, Data: in.Data, TS: in.TS}
		inserted, err := h.store.InsertEvent(r.Context(), e)
		if err != nil {
			http.Error(w, "store error", http.StatusInternalServerError)
			return
		}
		if inserted {
			h.stream <- e
		}
		// 重复 id：照常返回 200 + id（幂等生产者可安全重试），但不重复路由
		idsOut = append(idsOut, in.ID)
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]any{"ids": idsOut, "status": 200})
}
