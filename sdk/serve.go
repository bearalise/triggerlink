package triggerlink

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"runtime/debug"

	"github.com/triggerlink/triggerlink/sdk/internal/execx"
)

// sdkVersion 上报给平台内省清单。
const sdkVersion = "triggerlink-go/0.0.0-m0"

// Serve 返回承载函数注册与执行回调的 http.Handler（PRD FR-5.1）。
// 集成任意框架：标准库直接挂；Gin 用 gin.WrapH。
func Serve(client *Client, fns ...*Function) http.Handler {
	h := &serveHandler{client: client, byID: map[string]*Function{}}
	for _, f := range fns {
		h.byID[f.opts.ID] = f
	}
	return h
}

type serveHandler struct {
	client *Client
	byID   map[string]*Function
}

type callbackRequest struct {
	Ctx struct {
		RunID      string                     `json:"run_id"`
		FunctionID string                     `json:"function_id"`
		Attempt    int                        `json:"attempt"`
		Event      Event                      `json:"event"`
		Steps      map[string]execx.StepState `json:"steps"`
	} `json:"ctx"`
}

func (h *serveHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		h.handleManifest(w, r)
	case http.MethodPost:
		h.handleExec(w, r)
	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

func (h *serveHandler) handleManifest(w http.ResponseWriter, r *http.Request) {
	if err := verifySignature(h.client.signingKey, r.Header.Get("X-TriggerLink-Signature"), nil); err != nil {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}
	type fnInfo struct {
		ID      string `json:"id"`
		Event   string `json:"event"`
		Retries int    `json:"retries"`
	}
	fns := make([]fnInfo, 0, len(h.byID))
	for _, f := range h.byID {
		fns = append(fns, fnInfo{ID: f.opts.ID, Event: f.opts.Event, Retries: f.opts.Retries})
	}
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]any{
		"sdk": sdkVersion, "app_id": h.client.appID, "functions": fns,
	})
}

func (h *serveHandler) handleExec(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(io.LimitReader(r.Body, 10<<20))
	if err != nil {
		http.Error(w, "read body", http.StatusBadRequest)
		return
	}
	if err := verifySignature(h.client.signingKey, r.Header.Get("X-TriggerLink-Signature"), body); err != nil {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}
	var req callbackRequest
	if err := json.Unmarshal(body, &req); err != nil {
		http.Error(w, "invalid request", http.StatusBadRequest)
		return
	}
	fn, ok := h.byID[req.Ctx.FunctionID]
	if !ok {
		http.Error(w, "function not found", http.StatusNotFound)
		return
	}

	ec := execx.New(req.Ctx.FunctionID, req.Ctx.Steps)
	ctx := execx.With(r.Context(), ec)

	var op *execx.Opcode
	var output any
	var runErr error
	func() {
		defer func() {
			if rv := recover(); rv != nil {
				if o, ok := rv.(*execx.Opcode); ok {
					op = o // step 中断：正常控制流
					return
				}
				runErr = fmt.Errorf("panic: %v\n%s", rv, debug.Stack())
			}
		}()
		output, runErr = fn.handler(ctx, Input{Event: req.Ctx.Event})
	}()

	w.Header().Set("Content-Type", "application/json")
	switch {
	case op != nil:
		json.NewEncoder(w).Encode(op)
	case runErr != nil:
		json.NewEncoder(w).Encode(map[string]any{
			"op":    "RunError",
			"error": map[string]any{"message": runErr.Error(), "retryable": true},
		})
	default:
		json.NewEncoder(w).Encode(map[string]any{"op": "RunComplete", "output": output})
	}
}
