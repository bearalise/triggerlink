package sign

import (
	"strings"
	"testing"
	"time"
)

var (
	testKey  = "test-signing-key"
	testBody = []byte(`{"hello":"world"}`)
	testNow  = time.Unix(1754000000, 0)
)

func TestSignVerifyRoundTrip(t *testing.T) {
	h := Sign(testKey, testBody, testNow)
	if !strings.HasPrefix(h, "t=1754000000,v1=") {
		t.Fatalf("unexpected header format: %q", h)
	}
	if err := Verify(testKey, h, testBody, testNow, 5*time.Minute); err != nil {
		t.Fatalf("verify failed: %v", err)
	}
}

func TestVerifyRejectsWrongKey(t *testing.T) {
	h := Sign("other-key", testBody, testNow)
	if err := Verify(testKey, h, testBody, testNow, 5*time.Minute); err == nil {
		t.Fatal("expected error for wrong key")
	}
}

func TestVerifyRejectsTamperedBody(t *testing.T) {
	h := Sign(testKey, testBody, testNow)
	if err := Verify(testKey, h, []byte(`{"hello":"evil"}`), testNow, 5*time.Minute); err == nil {
		t.Fatal("expected error for tampered body")
	}
}

func TestVerifyRejectsOldTimestamp(t *testing.T) {
	h := Sign(testKey, testBody, testNow)
	if err := Verify(testKey, h, testBody, testNow.Add(10*time.Minute), 5*time.Minute); err == nil {
		t.Fatal("expected error for expired timestamp")
	}
}

func TestVerifyRejectsMalformed(t *testing.T) {
	if err := Verify(testKey, "garbage", testBody, testNow, 5*time.Minute); err == nil {
		t.Fatal("expected error for malformed header")
	}
}
