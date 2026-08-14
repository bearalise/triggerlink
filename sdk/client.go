// Package triggerlink 是 TriggerLink 的 Go SDK（PRD 第 10 章）。
// M0 交付：Client、CreateFunction、Serve、step.Run 一个原语。
package triggerlink

// Options 是 Client 的配置。
type Options struct {
	AppID      string // 应用标识，如 "billing"
	SigningKey string // 与平台共享的签名密钥（验签平台回调）
}

// Client 承载应用级配置，供 CreateFunction / Serve 使用。
type Client struct {
	appID      string
	signingKey string
}

// NewClient 构造 Client。
func NewClient(opts Options) *Client {
	return &Client{appID: opts.AppID, signingKey: opts.SigningKey}
}
