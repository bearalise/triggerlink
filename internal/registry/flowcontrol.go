// 流控配置（FR-4.4 throttle / FR-4.5 debounce / FR-4.7 batching）的清单类型。
//
// 三者的时长字段在 JSON 里一律是 Go duration 字符串（"5m"、"30s"），与 manifest 的
// timeout 字段同一约定；Marshal/Unmarshal 都走这里的自定义实现，因此清单形态与落库
// 形态完全一致——持久化只是把同一段 JSON 存进 functions 表的一列。
package registry

import (
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

// Debounce 是防抖配置（FR-4.5）：同 (function_id, Key) 的事件在 Period 窗口内合并为
// 一个 run，取尾事件。Key 为可选 expr 表达式（环境只含 data），留空表示整个函数共用
// 一个窗口。Timeout 是距首个事件的最长延迟：窗口被持续刷新时到点强制触发，0 = 不封顶。
type Debounce struct {
	Period  time.Duration
	Key     string
	Timeout time.Duration
}

// Throttle 是限流配置（FR-4.4）：同 (function_id, Key) 在 Period 窗口内最多启动
// Limit 个 run 的首次执行，超限的 run 延迟到下一窗口而非丢弃。
type Throttle struct {
	Limit  int
	Period time.Duration
	Key    string
}

// Batch 是批处理配置（FR-4.7）：同 (function_id, Key) 的事件攒够 MaxSize 条，
// 或距首条超过 Timeout，即以事件数组触发一个 run。
type Batch struct {
	MaxSize int
	Timeout time.Duration
	Key     string
}

// --- JSON 形态（时长为 Go duration 字符串）---

type debounceJSON struct {
	Period  string `json:"period"`
	Key     string `json:"key,omitempty"`
	Timeout string `json:"timeout,omitempty"`
}

type throttleJSON struct {
	Limit  int    `json:"limit"`
	Period string `json:"period"`
	Key    string `json:"key,omitempty"`
}

type batchJSON struct {
	MaxSize int    `json:"max_size"`
	Timeout string `json:"timeout"`
	Key     string `json:"key,omitempty"`
}

func (d Debounce) MarshalJSON() ([]byte, error) {
	out := debounceJSON{Period: d.Period.String(), Key: d.Key}
	if d.Timeout > 0 {
		out.Timeout = d.Timeout.String()
	}
	return json.Marshal(out)
}

func (d *Debounce) UnmarshalJSON(b []byte) error {
	var raw debounceJSON
	if err := json.Unmarshal(b, &raw); err != nil {
		return err
	}
	period, err := parsePositiveDuration(raw.Period, "period")
	if err != nil {
		return fmt.Errorf("debounce: %w", err)
	}
	timeout, err := parseOptionalDuration(raw.Timeout, "timeout")
	if err != nil {
		return fmt.Errorf("debounce: %w", err)
	}
	*d = Debounce{Period: period, Key: raw.Key, Timeout: timeout}
	return nil
}

func (t Throttle) MarshalJSON() ([]byte, error) {
	return json.Marshal(throttleJSON{Limit: t.Limit, Period: t.Period.String(), Key: t.Key})
}

func (t *Throttle) UnmarshalJSON(b []byte) error {
	var raw throttleJSON
	if err := json.Unmarshal(b, &raw); err != nil {
		return err
	}
	if raw.Limit <= 0 {
		return errors.New("throttle: limit must be > 0")
	}
	period, err := parsePositiveDuration(raw.Period, "period")
	if err != nil {
		return fmt.Errorf("throttle: %w", err)
	}
	*t = Throttle{Limit: raw.Limit, Period: period, Key: raw.Key}
	return nil
}

func (b Batch) MarshalJSON() ([]byte, error) {
	return json.Marshal(batchJSON{MaxSize: b.MaxSize, Timeout: b.Timeout.String(), Key: b.Key})
}

func (b *Batch) UnmarshalJSON(data []byte) error {
	var raw batchJSON
	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}
	if raw.MaxSize <= 0 {
		return errors.New("batch: max_size must be > 0")
	}
	// timeout 必填：没有它，攒不满 max_size 的窗口永远不会 flush。
	timeout, err := parsePositiveDuration(raw.Timeout, "timeout")
	if err != nil {
		return fmt.Errorf("batch: %w", err)
	}
	*b = Batch{MaxSize: raw.MaxSize, Timeout: timeout, Key: raw.Key}
	return nil
}

func parsePositiveDuration(s, field string) (time.Duration, error) {
	if s == "" {
		return 0, fmt.Errorf("%s is required", field)
	}
	d, err := time.ParseDuration(s)
	if err != nil {
		return 0, fmt.Errorf("invalid %s %q: %w", field, s, err)
	}
	if d <= 0 {
		return 0, fmt.Errorf("%s must be > 0, got %q", field, s)
	}
	return d, nil
}

func parseOptionalDuration(s, field string) (time.Duration, error) {
	if s == "" {
		return 0, nil
	}
	return parsePositiveDuration(s, field)
}
