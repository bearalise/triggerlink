// Package dashboard 以 go:embed 嵌入前端构建产物，在 /dashboard 提供 SPA（PRD 第 11 章）。
package dashboard

import (
	"embed"
	"io/fs"
	"net/http"
	"strings"
)

//go:embed all:dist
var assets embed.FS

// Handler 返回挂在 /dashboard/ 下的 SPA handler：存在的静态文件直接返回，
// 其余路径（含 /dashboard 本身与前端路由）回退 index.html。
func Handler() http.Handler {
	sub, err := fs.Sub(assets, "dist")
	if err != nil {
		panic(err)
	}
	fileSrv := http.FileServer(http.FS(sub))
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		p := strings.Trim(strings.TrimPrefix(r.URL.Path, "/dashboard"), "/")
		if p == "" {
			p = "index.html"
		}
		if _, err := fs.Stat(sub, p); err != nil || p == "index.html" {
			// 不经过 FileServer 直接写 index.html，避免其对 /index.html 的 301 重定向
			data, err := fs.ReadFile(sub, "index.html")
			if err != nil {
				http.Error(w, "dashboard assets missing", http.StatusInternalServerError)
				return
			}
			w.Header().Set("Content-Type", "text/html; charset=utf-8")
			w.Write(data)
			return
		}
		r2 := r.Clone(r.Context())
		r2.URL.Path = "/" + p
		fileSrv.ServeHTTP(w, r2)
	})
}
