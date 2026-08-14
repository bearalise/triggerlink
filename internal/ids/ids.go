// Package ids 生成全局唯一 ID：时间有序前缀 + 随机后缀，M0 不引第三方 ID 库。
package ids

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"time"
)

// NewID 返回 "<prefix>_<毫秒时间戳><16 hex 随机>" 形式的 ID。
func NewID(prefix string) string {
	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic(fmt.Sprintf("ids: crypto/rand unavailable: %v", err))
	}
	return fmt.Sprintf("%s_%013d%s", prefix, time.Now().UnixMilli(), hex.EncodeToString(b[:]))
}
