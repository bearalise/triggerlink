// Package auth 提供 Dashboard 与只读管理 API 的 basic auth 中间件（PRD FR-7.6）。
package auth

import (
	"crypto/subtle"
	"fmt"
	"net/http"
	"strings"
)

// ParseCreds 解析 "user:password" 形式的凭据配置。
func ParseCreds(s string) (string, string, error) {
	user, pass, ok := strings.Cut(s, ":")
	if !ok || user == "" || pass == "" {
		return "", "", fmt.Errorf("凭据格式应为 user:password")
	}
	return user, pass, nil
}

// Basic 返回要求 HTTP Basic 认证的中间件（常量时间比对）。
func Basic(user, pass string, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		u, p, ok := r.BasicAuth()
		if !ok ||
			subtle.ConstantTimeCompare([]byte(u), []byte(user)) != 1 ||
			subtle.ConstantTimeCompare([]byte(p), []byte(pass)) != 1 {
			w.Header().Set("WWW-Authenticate", `Basic realm="triggerlink"`)
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
		next.ServeHTTP(w, r)
	})
}
