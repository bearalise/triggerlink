package triggerlink

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strconv"
	"strings"
	"time"
)

// verifySignature 校验平台回调签名（算法与 internal/sign 一致；
// SDK 必须独立可分发，故有意重复实现，不 import internal/）。
func verifySignature(key, header string, body []byte) error {
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
	if d := time.Since(time.Unix(sec, 0)); d > 5*time.Minute || d < -5*time.Minute {
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
