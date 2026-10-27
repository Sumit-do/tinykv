package main

import (
	"bytes"
	"io"
	"log/slog"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/sumit-do/tinykv"
	"github.com/sumit-do/tinykv/internal/server"
)

func TestParseSync(t *testing.T) {
	cases := []struct {
		in       string
		mode     tinykv.SyncMode
		interval time.Duration
		wantErr  bool
	}{
		{"always", tinykv.SyncAlways, 0, false},
		{"NEVER", tinykv.SyncNever, 0, false},
		{"250ms", tinykv.SyncInterval, 250 * time.Millisecond, false},
		{"2s", tinykv.SyncInterval, 2 * time.Second, false},
		{"0s", 0, 0, true},
		{"sometimes", 0, 0, true},
	}
	for _, c := range cases {
		mode, iv, err := parseSync(c.in)
		if (err != nil) != c.wantErr {
			t.Errorf("parseSync(%q) err = %v, wantErr %v", c.in, err, c.wantErr)
			continue
		}
		if !c.wantErr && (mode != c.mode || iv != c.interval) {
			t.Errorf("parseSync(%q) = %v, %v; want %v, %v", c.in, mode, iv, c.mode, c.interval)
		}
	}
}

func TestNormalizeAddr(t *testing.T) {
	cases := map[string]string{
		":7700":                   "http://localhost:7700",
		"127.0.0.1:7700":          "http://127.0.0.1:7700",
		"http://example.com:1/":   "http://example.com:1",
		"https://kv.example.com":  "https://kv.example.com",
		"  kv.internal:7700  ":    "http://kv.internal:7700",
		"http://localhost:7700//": "http://localhost:7700",
	}
	for in, want := range cases {
		if got := normalizeAddr(in); got != want {
			t.Errorf("normalizeAddr(%q) = %q, want %q", in, got, want)
		}
	}
}

// TestClientCommands drives the CLI against an in-process server.
func TestClientCommands(t *testing.T) {
	quiet := slog.New(slog.NewTextHandler(io.Discard, nil))
	db, err := tinykv.Open(t.TempDir(), tinykv.Options{Sync: tinykv.SyncNever, SweepInterval: -1, Logger: quiet})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	ts := httptest.NewServer(server.New(db, quiet))
	defer ts.Close()

	exec := func(args ...string) (int, string, string) {
		var out, errb bytes.Buffer
		code := run(append([]string{args[0], "--addr", ts.URL}, args[1:]...), &out, &errb)
		return code, out.String(), errb.String()
	}

	if code, _, errs := exec("get", "missing"); code != 3 || !strings.Contains(errs, "not found") {
		t.Fatalf("get missing: code=%d stderr=%q", code, errs)
	}
	if code, _, errs := exec("set", "color", "blue"); code != 0 {
		t.Fatalf("set: code=%d stderr=%q", code, errs)
	}
	if code, _, errs := exec("set", "--ttl", "1h", "color:ttl", "red"); code != 0 {
		t.Fatalf("set ttl: code=%d stderr=%q", code, errs)
	}
	if code, out, _ := exec("get", "color"); code != 0 || out != "blue\n" {
		t.Fatalf("get: code=%d out=%q", code, out)
	}
	if code, out, _ := exec("keys", "color"); code != 0 || out != "color\ncolor:ttl\n" {
		t.Fatalf("keys: code=%d out=%q", code, out)
	}
	if code, out, _ := exec("stats"); code != 0 || !strings.Contains(out, "keys:       2") {
		t.Fatalf("stats: code=%d out=%q", code, out)
	}
	if code, out, _ := exec("compact"); code != 0 || !strings.HasPrefix(out, "compacted:") {
		t.Fatalf("compact: code=%d out=%q", code, out)
	}
	if code, _, _ := exec("del", "color"); code != 0 {
		t.Fatal("del failed")
	}
	if code, _, _ := exec("del", "color"); code != 3 {
		t.Fatalf("second del code=%d, want 3", code)
	}

	// Usage errors.
	if code, _, _ := exec("set", "only-key"); code != 2 {
		t.Fatalf("set with one arg code=%d, want 2", code)
	}
	var out, errb bytes.Buffer
	if code := run([]string{"bogus"}, &out, &errb); code != 2 || !strings.Contains(errb.String(), "unknown command") {
		t.Fatalf("bogus command: code=%d stderr=%q", code, errb.String())
	}
	if code := run(nil, &out, &errb); code != 2 {
		t.Fatalf("no args code=%d", code)
	}
}
