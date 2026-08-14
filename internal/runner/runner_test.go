package runner

import (
	"context"
	"encoding/json"
	"path/filepath"
	"testing"
	"time"

	"github.com/triggerlink/triggerlink/internal/registry"
	"github.com/triggerlink/triggerlink/internal/store"
)

func setup(t *testing.T) (*Runner, store.Store, chan store.Event) {
	t.Helper()
	st, err := store.Open(filepath.Join(t.TempDir(), "test.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() })
	reg := registry.New()
	reg.Register(registry.Function{ID: "process-doc", Event: "doc/uploaded", AppURL: "http://app/serve"})
	stream := make(chan store.Event, 16)
	return &Runner{Store: st, Reg: reg, Stream: stream}, st, stream
}

func waitRuns(t *testing.T, st store.Store, want int) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if n, _ := st.CountRuns(context.Background()); n == want {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	n, _ := st.CountRuns(context.Background())
	t.Fatalf("runs=%d, want %d", n, want)
}

// waitRouted 等待所有已落库事件完成路由标记（routed_at 已提交）。
func waitRouted(t *testing.T, st store.Store) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if evts, _ := st.UnroutedEvents(context.Background()); len(evts) == 0 {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("events still unrouted")
}

func TestRoutesStreamEvent(t *testing.T) {
	r, st, stream := setup(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go r.Run(ctx)

	stream <- store.Event{ID: "evt_1", Name: "doc/uploaded",
		Data: json.RawMessage(`{"doc_id":"d1"}`), TS: time.Now()}
	waitRuns(t, st, 1)

	// 事件无订阅者：也标记 routed，不产生 run
	stream <- store.Event{ID: "evt_2", Name: "no/subscribers", Data: json.RawMessage(`{}`), TS: time.Now()}
	waitRuns(t, st, 1)
}

func TestStartupReconciliation(t *testing.T) {
	r, st, _ := setup(t)
	// 直接落库、不走 stream —— 模拟"已接收但崩溃前未路由"
	if _, err := st.InsertEvent(context.Background(), store.Event{
		ID: "evt_orphan", Name: "doc/uploaded", Data: json.RawMessage(`{}`), TS: time.Now()}); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go r.Run(ctx)
	waitRuns(t, st, 1) // 对账扫描捡起孤儿事件并创建 run

	// run 落库先于 MarkEventRouted（route 内顺序提交），须等路由标记提交后
	// 再启动第二个 Runner，否则其启动对账会把"标记在途"的事件重复路由
	waitRouted(t, st)

	// 再启动一个 Runner：事件已 routed，不得重复建 run
	r2 := &Runner{Store: st, Reg: r.Reg, Stream: make(chan store.Event, 1)}
	go r2.Run(ctx)
	time.Sleep(200 * time.Millisecond)
	if n, _ := st.CountRuns(context.Background()); n != 1 {
		t.Fatalf("duplicate routing, runs=%d", n)
	}
}
