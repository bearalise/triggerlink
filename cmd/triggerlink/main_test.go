package main

import (
	"testing"
	"time"
)

func TestResolveRetention(t *testing.T) {
	cases := []struct {
		name    string
		flag    int
		env     string
		want    time.Duration
		wantErr bool
	}{
		{name: "默认 30 天", flag: 0, want: 30 * 24 * time.Hour},
		{name: "flag 优先于 env", flag: 7, env: "90", want: 7 * 24 * time.Hour},
		{name: "env 兜底", flag: 0, env: "90", want: 90 * 24 * time.Hour},
		{name: "负数显式关闭", flag: -1, want: 0},
		{name: "env 负数关闭", flag: 0, env: "-1", want: 0},
		{name: "短于去重窗口报错", flag: 0, env: "0", wantErr: true},
		{name: "env 非整数报错", flag: 0, env: "forever", wantErr: true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			t.Setenv("TRIGGERLINK_RETENTION_DAYS", c.env)
			got, err := resolveRetention(c.flag)
			if c.wantErr {
				if err == nil {
					t.Fatalf("want error, got %v", got)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if got != c.want {
				t.Fatalf("got %v, want %v", got, c.want)
			}
		})
	}
}

func TestParseLogLevel(t *testing.T) {
	for _, c := range []struct {
		in      string
		want    string
		wantBad string
	}{
		{in: "", want: "INFO"},
		{in: "debug", want: "DEBUG"},
		{in: " WARN ", want: "WARN"},
		{in: "warning", want: "WARN"},
		{in: "error", want: "ERROR"},
		{in: "loud", want: "INFO", wantBad: "loud"},
	} {
		t.Setenv("TRIGGERLINK_LOG_LEVEL", "")
		lvl, bad := parseLogLevel(c.in)
		if lvl.String() != c.want || bad != c.wantBad {
			t.Fatalf("parseLogLevel(%q) = %v,%q; want %s,%q", c.in, lvl, bad, c.want, c.wantBad)
		}
	}
	// env 兜底
	t.Setenv("TRIGGERLINK_LOG_LEVEL", "debug")
	if lvl, _ := parseLogLevel(""); lvl.String() != "DEBUG" {
		t.Fatalf("env fallback: got %v", lvl)
	}
}
