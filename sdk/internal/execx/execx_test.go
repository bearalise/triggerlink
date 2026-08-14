package execx

import (
	"context"
	"testing"
)

func TestNextHashDeterministic(t *testing.T) {
	a := New("fn", nil)
	b := New("fn", nil)
	for i := 0; i < 3; i++ {
		ha, hb := a.NextHash("parse"), b.NextHash("parse")
		if ha != hb { // 同 functionID + stepID + 序号 → 同 hash（memo 定位的前提）
			t.Fatalf("not deterministic: %s != %s", ha, hb)
		}
	}
	if a.NextHash("parse") == a.NextHash("parse") {
		t.Fatal("counter not incrementing")
	}
	if New("fn", nil).NextHash("a") == New("fn", nil).NextHash("b") {
		t.Fatal("different step IDs must differ")
	}
	if New("fn1", nil).NextHash("a") == New("fn2", nil).NextHash("a") {
		t.Fatal("different functions must differ")
	}
}

func TestContextRoundTrip(t *testing.T) {
	c := New("fn", nil)
	if From(context.Background()) != nil {
		t.Fatal("expected nil")
	}
	if From(With(context.Background(), c)) != c {
		t.Fatal("round trip failed")
	}
}
