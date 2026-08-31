package janitor

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

type fakeStore struct {
	mu             sync.Mutex
	cutoffs        []time.Time
	throttleCutoff time.Time
	runs           int
	waits          int
	events         int
	throttles      int
	runsErr        error
	eventsErr      error
	calls          int
}

func (f *fakeStore) DeleteRunsBefore(_ context.Context, cutoff time.Time) (int, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	f.cutoffs = append(f.cutoffs, cutoff)
	return f.runs, f.runsErr
}

func (f *fakeStore) DeleteEventsBefore(context.Context, time.Time) (int, error) {
	return f.events, f.eventsErr
}

func (f *fakeStore) DeleteFinishedWaitsBefore(context.Context, time.Time) (int, error) {
	return f.waits, nil
}

func (f *fakeStore) DeleteThrottleStateBefore(_ context.Context, cutoff time.Time) (int, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.throttleCutoff = cutoff
	return f.throttles, nil
}

func (f *fakeStore) callCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls
}

// Clean 用 now - Retention 作为 cutoff，三张表各清一次。
func TestCleanUsesRetentionCutoff(t *testing.T) {
	fs := &fakeStore{runs: 2, waits: 1, events: 5}
	j := &Janitor{Store: fs, Retention: 30 * 24 * time.Hour}
	now := time.Date(2026, 3, 1, 12, 0, 0, 0, time.UTC)

	j.Clean(context.Background(), now)

	if len(fs.cutoffs) != 1 {
		t.Fatalf("cutoffs=%v, want 1 call", fs.cutoffs)
	}
	want := now.Add(-30 * 24 * time.Hour)
	if !fs.cutoffs[0].Equal(want) {
		t.Fatalf("cutoff=%v, want %v", fs.cutoffs[0], want)
	}
	// 限流配额用独立的 24h 阈值：它不是历史数据，不跟保留期一起留 30 天
	if wantThrottle := now.Add(-24 * time.Hour); !fs.throttleCutoff.Equal(wantThrottle) {
		t.Fatalf("throttle cutoff=%v, want %v", fs.throttleCutoff, wantThrottle)
	}
}

// 单表失败不阻断其余两表：清理是尽力而为的后台工作。
func TestCleanContinuesAfterError(t *testing.T) {
	fs := &fakeStore{runsErr: errors.New("db locked"), events: 3}
	j := &Janitor{Store: fs, Retention: time.Hour}

	j.Clean(context.Background(), time.Now()) // 不 panic，且事件删除照常发生

	if fs.callCount() != 1 {
		t.Fatalf("calls=%d, want 1", fs.callCount())
	}
}

// Retention <= 0 表示关闭清理：Run 立即返回，一次都不查库。
func TestRunDisabledWhenRetentionZero(t *testing.T) {
	fs := &fakeStore{}
	j := &Janitor{Store: fs, Retention: 0}
	if err := (j).Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	if fs.callCount() != 0 {
		t.Fatalf("calls=%d, want 0", fs.callCount())
	}
}

// Run 启动即清理一次（重启后不必等满一个周期），ctx 取消后退出。
func TestRunCleansImmediatelyThenStops(t *testing.T) {
	fs := &fakeStore{}
	j := &Janitor{Store: fs, Retention: time.Hour, Interval: time.Hour}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- j.Run(ctx) }()

	deadline := time.After(2 * time.Second)
	for fs.callCount() == 0 {
		select {
		case <-deadline:
			t.Fatal("janitor did not clean before the first tick")
		default:
			time.Sleep(5 * time.Millisecond)
		}
	}
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("janitor did not stop on ctx cancel")
	}
}
