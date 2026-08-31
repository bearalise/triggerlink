package store

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"
)

// TestWebhookCRUD：webhooks 表 CRUD 持久化往返（PRD FR-3.4）。
func TestWebhookCRUD(t *testing.T) {
	st := openTemp(t)
	ctx := context.Background()

	w := Webhook{ID: "whk_1", EventName: "stripe/payment", Secret: "s3cret", CreatedAt: time.Now()}
	if err := st.CreateWebhook(ctx, w); err != nil {
		t.Fatalf("create: %v", err)
	}

	// 读取往返：字段逐一回读（created_at 经 RFC3339Nano 序列化，比到秒）
	got, err := st.GetWebhook(ctx, "whk_1")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if got.ID != w.ID || got.EventName != w.EventName || got.Secret != w.Secret {
		t.Fatalf("roundtrip: %+v", got)
	}
	if got.CreatedAt.Unix() != w.CreatedAt.Unix() {
		t.Fatalf("created_at: %v vs %v", got.CreatedAt, w.CreatedAt)
	}

	// 不存在 → sql.ErrNoRows
	if _, err := st.GetWebhook(ctx, "whk_none"); !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("get missing: %v", err)
	}

	// 列表按创建时间升序
	if err := st.CreateWebhook(ctx, Webhook{
		ID: "whk_2", EventName: "github/push", Secret: "s2", CreatedAt: time.Now().Add(time.Second),
	}); err != nil {
		t.Fatalf("create 2: %v", err)
	}
	list, err := st.ListWebhooks(ctx)
	if err != nil {
		t.Fatalf("list: %v", err)
	}
	if len(list) != 2 || list[0].ID != "whk_1" || list[1].ID != "whk_2" {
		t.Fatalf("list: %+v", list)
	}

	// 删除：存在 → true；再删 → false
	deleted, err := st.DeleteWebhook(ctx, "whk_1")
	if err != nil || !deleted {
		t.Fatalf("delete: deleted=%v err=%v", deleted, err)
	}
	if deleted, err := st.DeleteWebhook(ctx, "whk_1"); err != nil || deleted {
		t.Fatalf("re-delete: deleted=%v err=%v", deleted, err)
	}
	if _, err := st.GetWebhook(ctx, "whk_1"); !errors.Is(err, sql.ErrNoRows) {
		t.Fatalf("get after delete: %v", err)
	}
}
