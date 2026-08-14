package ids

import (
	"strings"
	"testing"
)

func TestNewIDHasPrefixAndIsUnique(t *testing.T) {
	seen := make(map[string]bool, 1000)
	for i := 0; i < 1000; i++ {
		id := NewID("run")
		if !strings.HasPrefix(id, "run_") {
			t.Fatalf("id %q missing prefix", id)
		}
		if seen[id] {
			t.Fatalf("duplicate id %q", id)
		}
		seen[id] = true
	}
}
