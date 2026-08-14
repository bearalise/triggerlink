// Package sign 实现平台与应用间的 HMAC-SHA256 请求签名（PRD FR-5.4）。
// 头部格式：t=<unix秒>,v1=<hex(hmac_sha256(key, "<t>.<body>"))>
package sign

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strconv"
	"strings"
	"time"
)

// Sign 生成签名头部值。
func Sign(key string, body []byte, now time.Time) string {
	t := now.Unix()
	mac := hmac.New(sha256.New, []byte(key))
	fmt.Fprintf(mac, "%d.", t)
	mac.Write(body)
	return fmt.Sprintf("t=%d,v1=%s", t, hex.EncodeToString(mac.Sum(nil)))
}

// Verify 校验签名头部值；时间戳超出 tolerance 视为重放。
func Verify(key, header string, body []byte, now time.Time, tolerance time.Duration) error {
	var ts, sig string
	for _, part := range strings.Split(header, ",") {
		if strings.HasPrefix(part, "t=") {
			ts = strings.TrimPrefix(part, "t=")
		}
		if strings.HasPrefix(part, "v1=") {
			sig = strings.TrimPrefix(part, "v1=")
		}
	}
	if ts == "" || sig == "" {
		return fmt.Errorf("malformed signature header")
	}
	sec, err := strconv.ParseInt(ts, 10, 64)
	if err != nil {
		return fmt.Errorf("bad timestamp: %w", err)
	}
	sentAt := time.Unix(sec, 0)
	if d := now.Sub(sentAt); d > tolerance || d < -tolerance {
		return fmt.Errorf("timestamp outside tolerance")
	}
	mac := hmac.New(sha256.New, []byte(key))
	fmt.Fprintf(mac, "%d.", sec)
	mac.Write(body)
	expected := hex.EncodeToString(mac.Sum(nil))
	if !hmac.Equal([]byte(expected), []byte(sig)) {
		return fmt.Errorf("signature mismatch")
	}
	return nil
}
