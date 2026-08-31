// Package registry 维护"事件名 → 函数"路由表与"函数 ID → serve URL"索引。
// M0：进程内内存表，启动时通过 GET 清单内省构建（PRD FR-5.3 的最小形态）。
package registry

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"sort"
	"sync"
	"time"

	"github.com/bearalise/triggerlink/internal/sign"
)

// CancelRule 是一条 cancelOn 规则（FR-4.9）：到达事件名为 Event 且 match 表达式命中时，
// 取消该函数的在途 run。Match 为空表示该事件到达即取消。
type CancelRule struct {
	Event string `json:"event"`
	Match string `json:"match,omitempty"`
}

// Function 是一个已注册的 durable 函数的路由条目。
// Match 是事件触发的过滤表达式（FR-3.1，expr-lang，环境只含 data）：非空且不命中则不为该函数建 run。
// Cron 是标准 5 字段 cron 表达式（FR-3.2，UTC，分钟粒度）：非空时由 scheduler 定时触发，不经事件路由。
// Timeout 是 run 级超时（FR-4.3）：run 从 created_at 起超过该时长仍在 Queued/Running 则置 Failed；0 表示不限制。
// 清单 JSON 中 timeout 为 Go duration 字符串（如 "5m"），故不参与默认 JSON 解码（见 UnmarshalJSON）。
type Function struct {
	ID       string        `json:"id"`
	Event    string        `json:"event"`
	Match    string        `json:"match,omitempty"`
	Cron     string        `json:"cron,omitempty"`
	Retries  int           `json:"retries"`
	CancelOn []CancelRule  `json:"cancel_on,omitempty"`
	Timeout  time.Duration `json:"-"`
	// 流控配置（FR-4.4/4.5/4.7），nil 表示未配置。时长字段在清单 JSON 中是 Go duration
	// 字符串，故与 Timeout 同样不参与默认解码（见 UnmarshalJSON）。
	Debounce *Debounce `json:"debounce,omitempty"`
	Throttle *Throttle `json:"throttle,omitempty"`
	Batch    *Batch    `json:"batch,omitempty"`
	AppURL   string    `json:"-"`
}

// UnmarshalJSON 解析清单 JSON 中需要额外处理的字段：timeout（Go duration 字符串，
// 如 "5m"）与三个流控配置（debounce/throttle/batch）。
// 单个字段非法只记日志并视为未配置，不使整个内省失败——一个函数的配置笔误不该让
// 整个应用注册不上。
func (f *Function) UnmarshalJSON(b []byte) error {
	type alias Function // 避免递归调用 UnmarshalJSON
	var raw struct {
		alias
		Timeout  string          `json:"timeout"`
		Debounce json.RawMessage `json:"debounce"`
		Throttle json.RawMessage `json:"throttle"`
		Batch    json.RawMessage `json:"batch"`
	}
	if err := json.Unmarshal(b, &raw); err != nil {
		return err
	}
	*f = Function(raw.alias)
	if raw.Timeout != "" {
		d, err := time.ParseDuration(raw.Timeout)
		if err != nil {
			slog.Warn("invalid function timeout, treated as unlimited", "component", "registry", "function_id", raw.ID, "timeout", raw.Timeout, "err", err)
		} else {
			f.Timeout = d
		}
	}
	f.Debounce = decodeFlowControl[Debounce](raw.Debounce, raw.ID, "debounce")
	f.Throttle = decodeFlowControl[Throttle](raw.Throttle, raw.ID, "throttle")
	f.Batch = decodeFlowControl[Batch](raw.Batch, raw.ID, "batch")
	return nil
}

// decodeFlowControl 解码一个流控配置；缺省/null 返回 nil，非法值记日志后返回 nil。
func decodeFlowControl[T any](raw json.RawMessage, functionID, field string) *T {
	if len(raw) == 0 || string(raw) == "null" {
		return nil
	}
	var v T
	if err := json.Unmarshal(raw, &v); err != nil {
		slog.Warn("invalid function flow-control config, ignored",
			"component", "registry", "function_id", functionID, "field", field, "err", err)
		return nil
	}
	return &v
}

type manifest struct {
	SDK       string     `json:"sdk"`
	AppID     string     `json:"app_id"`
	Functions []Function `json:"functions"`
}

// Registry 是线程安全的内存注册表。
type Registry struct {
	mu      sync.RWMutex
	byEvent map[string][]Function
	byID    map[string]Function
}

// New 构造空注册表。
func New() *Registry {
	return &Registry{byEvent: map[string][]Function{}, byID: map[string]Function{}}
}

// Register 注册一个函数（重复注册同 ID 覆盖）。
func (r *Registry) Register(f Function) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if old, ok := r.byID[f.ID]; ok {
		ev := r.byEvent[old.Event]
		for i, x := range ev {
			if x.ID == f.ID {
				r.byEvent[old.Event] = append(ev[:i], ev[i+1:]...)
				break
			}
		}
	}
	r.byID[f.ID] = f
	r.byEvent[f.Event] = append(r.byEvent[f.Event], f)
}

// Match 返回订阅某事件名的全部函数（fan-out）。
func (r *Registry) Match(eventName string) []Function {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return append([]Function(nil), r.byEvent[eventName]...)
}

// Lookup 按函数 ID 查路由条目。
func (r *Registry) Lookup(functionID string) (Function, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	f, ok := r.byID[functionID]
	return f, ok
}

// CancelMatchers 返回注册了针对某事件名的 cancelOn 规则的全部函数（FR-4.9）。
func (r *Registry) CancelMatchers(eventName string) []Function {
	r.mu.RLock()
	defer r.mu.RUnlock()
	var out []Function
	for _, f := range r.byID {
		for _, rule := range f.CancelOn {
			if rule.Event == eventName {
				out = append(out, f)
				break
			}
		}
	}
	return out
}

// CronFunctions 返回注册了 cron 表达式的全部函数（FR-3.2，scheduler 轮询用）。
func (r *Registry) CronFunctions() []Function {
	r.mu.RLock()
	defer r.mu.RUnlock()
	var out []Function
	for _, f := range r.byID {
		if f.Cron != "" {
			out = append(out, f)
		}
	}
	return out
}

// All 返回注册表中的全部函数（无序），executor 的 run 超时扫描（FR-4.3）枚举用。
func (r *Registry) All() []Function {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]Function, 0, len(r.byID))
	for _, f := range r.byID {
		out = append(out, f)
	}
	return out
}

// Sync 用一次内省结果整体替换 appURL 名下的函数集合（PRD FR-5.3 的运行时形态）。
// 改名/删除的函数不会残留；fns 的 AppURL 强制设为 appURL。
// 同一函数 ID 从其他 appURL 迁来时（如应用换端口/换地址重新注册），
// 其在旧 URL 下的订阅条目一并清除，避免 byEvent 残留导致重复路由。
func (r *Registry) Sync(appURL string, fns []Function) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for id, f := range r.byID {
		if f.AppURL != appURL {
			continue
		}
		ev := r.byEvent[f.Event]
		for i, x := range ev {
			if x.ID == id {
				r.byEvent[f.Event] = append(ev[:i], ev[i+1:]...)
				break
			}
		}
		delete(r.byID, id)
	}
	for _, f := range fns {
		f.AppURL = appURL
		if old, ok := r.byID[f.ID]; ok { // 同 ID 挂在其他 URL 下：先摘出旧订阅
			ev := r.byEvent[old.Event]
			for i, x := range ev {
				if x.ID == f.ID {
					r.byEvent[old.Event] = append(ev[:i], ev[i+1:]...)
					break
				}
			}
		}
		r.byID[f.ID] = f
		r.byEvent[f.Event] = append(r.byEvent[f.Event], f)
	}
}

// Apps 返回排序后的已注册 appURL 列表。只统计仍有函数注册的 app（M0 语义）。
func (r *Registry) Apps() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	seen := map[string]struct{}{}
	for _, f := range r.byID {
		seen[f.AppURL] = struct{}{}
	}
	out := make([]string, 0, len(seen))
	for u := range seen {
		out = append(out, u)
	}
	sort.Strings(out)
	return out
}

// FunctionsOf 返回某 appURL 名下的函数列表（无序）。
func (r *Registry) FunctionsOf(appURL string) []Function {
	r.mu.RLock()
	defer r.mu.RUnlock()
	var out []Function
	for _, f := range r.byID {
		if f.AppURL == appURL {
			out = append(out, f)
		}
	}
	return out
}

// Fetch 内省一个应用的 serve 端点（GET 清单，带签名），返回其函数列表（AppURL 已填充）。
func Fetch(ctx context.Context, client *http.Client, appURL, signingKey string) ([]Function, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, appURL, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("X-TriggerLink-Signature", sign.Sign(signingKey, nil, time.Now()))
	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("introspect %s: %w", appURL, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("introspect %s: status %d", appURL, resp.StatusCode)
	}
	var m manifest
	if err := json.NewDecoder(resp.Body).Decode(&m); err != nil {
		return nil, fmt.Errorf("introspect %s: decode: %w", appURL, err)
	}
	for i := range m.Functions {
		m.Functions[i].AppURL = appURL
	}
	return m.Functions, nil
}
