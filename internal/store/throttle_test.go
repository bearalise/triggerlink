package store

import (
	"context"
	"testing"
	"time"
)

// 窗口内计数到 limit 为止；超限返回下一窗口开始时刻；窗口过期后重新计数。
func TestReserveThrottleSlot(t *testing.T) {
	st := openTemp(t)
	ctx := context.Background()
	now := time.Date(2026, 3, 1, 12, 0, 0, 0, time.UTC)
	const period = time.Minute

	for i := 1; i <= 2; i++ {
		allowed, _, err := st.ReserveThrottleSlot(ctx, "f1:", 2, period, now)
		if err != nil || !allowed {
			t.Fatalf("reservation %d: allowed=%v err=%v, want allowed", i, allowed, err)
		}
	}

	// 第 3 个超限：拒绝，并给出下一窗口开始时刻
	allowed, retryAt, err := st.ReserveThrottleSlot(ctx, "f1:", 2, period, now.Add(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if allowed {
		t.Fatal("third reservation must be denied (limit=2)")
	}
	if want := now.Add(period); !retryAt.Equal(want) {
		t.Fatalf("retryAt=%v, want %v (window end)", retryAt, want)
	}

	// 窗口过期：重新开窗放行
	allowed, _, err = st.ReserveThrottleSlot(ctx, "f1:", 2, period, now.Add(period))
	if err != nil || !allowed {
		t.Fatalf("after window expiry: allowed=%v err=%v, want allowed", allowed, err)
	}
	// 新窗口从 0 计起：还能再放一个
	if allowed, _, _ := st.ReserveThrottleSlot(ctx, "f1:", 2, period, now.Add(period)); !allowed {
		t.Fatal("new window must start counting from zero")
	}
}

// 不同 key 的配额互不影响。
func TestReserveThrottleSlotKeyIsolation(t *testing.T) {
	st := openTemp(t)
	ctx := context.Background()
	now := time.Now()

	if allowed, _, _ := st.ReserveThrottleSlot(ctx, "f1:tenant-a", 1, time.Minute, now); !allowed {
		t.Fatal("first reservation for tenant-a must be allowed")
	}
	if allowed, _, _ := st.ReserveThrottleSlot(ctx, "f1:tenant-a", 1, time.Minute, now); allowed {
		t.Fatal("second reservation for tenant-a must be denied")
	}
	if allowed, _, _ := st.ReserveThrottleSlot(ctx, "f1:tenant-b", 1, time.Minute, now); !allowed {
		t.Fatal("tenant-b has its own quota")
	}
}

// 清理只删窗口起点早于 cutoff 的记录。
func TestDeleteThrottleStateBefore(t *testing.T) {
	st := openTemp(t)
	ctx := context.Background()
	now := time.Now()

	if _, _, err := st.ReserveThrottleSlot(ctx, "old", 5, time.Minute, now.Add(-48*time.Hour)); err != nil {
		t.Fatal(err)
	}
	if _, _, err := st.ReserveThrottleSlot(ctx, "fresh", 5, time.Minute, now); err != nil {
		t.Fatal(err)
	}

	n, err := st.DeleteThrottleStateBefore(ctx, now.Add(-24*time.Hour))
	if err != nil || n != 1 {
		t.Fatalf("deleted=%d err=%v, want 1", n, err)
	}
	if got := countRows(t, st, "throttle_state"); got != 1 {
		t.Fatalf("throttle_state=%d, want 1", got)
	}
}
