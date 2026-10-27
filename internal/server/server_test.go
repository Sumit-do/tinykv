package server

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/sumit-do/tinykv"
)

type fakeClock struct {
	mu sync.Mutex
	t  time.Time
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *fakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	c.t = c.t.Add(d)
	c.mu.Unlock()
}

func newTestServer(t *testing.T) (*httptest.Server, *fakeClock) {
	t.Helper()
	clk := &fakeClock{t: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)}
	db, err := tinykv.Open(t.TempDir(), tinykv.Options{
		Sync:          tinykv.SyncNever,
		SweepInterval: -1,
		Clock:         clk.Now,
		Logger:        slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	if err != nil {
		t.Fatal(err)
	}
	ts := httptest.NewServer(New(db, slog.New(slog.NewTextHandler(io.Discard, nil))))
	t.Cleanup(func() {
		ts.Close()
		db.Close()
	})
	return ts, clk
}

func do(t *testing.T, method, url string, body string) (*http.Response, []byte) {
	t.Helper()
	req, err := http.NewRequest(method, url, strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return resp, b
}

func wantStatus(t *testing.T, resp *http.Response, body []byte, want int) {
	t.Helper()
	if resp.StatusCode != want {
		t.Fatalf("%s %s: status %d, want %d (body %q)", resp.Request.Method, resp.Request.URL.Path, resp.StatusCode, want, body)
	}
}

func wantJSONError(t *testing.T, resp *http.Response, body []byte, status int) {
	t.Helper()
	wantStatus(t, resp, body, status)
	if ct := resp.Header.Get("Content-Type"); ct != "application/json" {
		t.Fatalf("Content-Type = %q, want application/json", ct)
	}
	var m map[string]string
	if err := json.Unmarshal(body, &m); err != nil || m["error"] == "" {
		t.Fatalf("expected JSON error body, got %q", body)
	}
}

func TestKVRoundTrip(t *testing.T) {
	ts, _ := newTestServer(t)

	resp, body := do(t, http.MethodGet, ts.URL+"/kv/missing", "")
	wantJSONError(t, resp, body, http.StatusNotFound)

	resp, body = do(t, http.MethodPut, ts.URL+"/kv/greeting", "hello world")
	wantStatus(t, resp, body, http.StatusNoContent)

	resp, body = do(t, http.MethodGet, ts.URL+"/kv/greeting", "")
	wantStatus(t, resp, body, http.StatusOK)
	if string(body) != "hello world" {
		t.Fatalf("body = %q", body)
	}
	if ct := resp.Header.Get("Content-Type"); ct != "application/octet-stream" {
		t.Fatalf("Content-Type = %q", ct)
	}

	// Keys may contain slashes and URL-encoded characters.
	resp, body = do(t, http.MethodPut, ts.URL+"/kv/users/42/name%20first", "Ada")
	wantStatus(t, resp, body, http.StatusNoContent)
	resp, body = do(t, http.MethodGet, ts.URL+"/kv/users/42/name%20first", "")
	wantStatus(t, resp, body, http.StatusOK)
	if string(body) != "Ada" {
		t.Fatalf("body = %q", body)
	}

	resp, body = do(t, http.MethodDelete, ts.URL+"/kv/greeting", "")
	wantStatus(t, resp, body, http.StatusNoContent)
	resp, body = do(t, http.MethodDelete, ts.URL+"/kv/greeting", "")
	wantJSONError(t, resp, body, http.StatusNotFound)
	resp, body = do(t, http.MethodGet, ts.URL+"/kv/greeting", "")
	wantJSONError(t, resp, body, http.StatusNotFound)

	resp, body = do(t, http.MethodPut, ts.URL+"/kv/", "x")
	wantJSONError(t, resp, body, http.StatusBadRequest)
}

func TestTTL(t *testing.T) {
	ts, clk := newTestServer(t)
	resp, body := do(t, http.MethodPut, ts.URL+"/kv/session?ttl=30s", "tok")
	wantStatus(t, resp, body, http.StatusNoContent)
	resp, body = do(t, http.MethodGet, ts.URL+"/kv/session", "")
	wantStatus(t, resp, body, http.StatusOK)

	clk.Advance(31 * time.Second)
	resp, body = do(t, http.MethodGet, ts.URL+"/kv/session", "")
	wantJSONError(t, resp, body, http.StatusNotFound)

	resp, body = do(t, http.MethodPut, ts.URL+"/kv/x?ttl=soon", "v")
	wantJSONError(t, resp, body, http.StatusBadRequest)
}

func TestListKeys(t *testing.T) {
	ts, _ := newTestServer(t)
	for _, k := range []string{"user:2", "user:1", "order:1"} {
		resp, body := do(t, http.MethodPut, ts.URL+"/kv/"+k, "v")
		wantStatus(t, resp, body, http.StatusNoContent)
	}
	resp, body := do(t, http.MethodGet, ts.URL+"/kv?prefix=user:", "")
	wantStatus(t, resp, body, http.StatusOK)
	var out struct {
		Keys  []string `json:"keys"`
		Count int      `json:"count"`
	}
	if err := json.Unmarshal(body, &out); err != nil {
		t.Fatal(err)
	}
	if out.Count != 2 || strings.Join(out.Keys, ",") != "user:1,user:2" {
		t.Fatalf("list = %+v", out)
	}

	resp, body = do(t, http.MethodGet, ts.URL+"/kv?prefix=zzz", "")
	wantStatus(t, resp, body, http.StatusOK)
	if !bytes.Contains(body, []byte(`"keys":[]`)) {
		t.Fatalf("empty list should be [] not null: %s", body)
	}
}

func TestBatch(t *testing.T) {
	ts, clk := newTestServer(t)
	do(t, http.MethodPut, ts.URL+"/kv/old", "x")

	payload := `[
		{"op":"set","key":"a","value":"1"},
		{"op":"set","key":"b","value":"2","ttl":"10s"},
		{"op":"delete","key":"old"}
	]`
	resp, body := do(t, http.MethodPost, ts.URL+"/batch", payload)
	wantStatus(t, resp, body, http.StatusOK)
	if !bytes.Contains(body, []byte(`"applied":3`)) {
		t.Fatalf("body = %s", body)
	}
	for k, want := range map[string]string{"a": "1", "b": "2"} {
		_, body := do(t, http.MethodGet, ts.URL+"/kv/"+k, "")
		if string(body) != want {
			t.Fatalf("%s = %q", k, body)
		}
	}
	resp, body = do(t, http.MethodGet, ts.URL+"/kv/old", "")
	wantStatus(t, resp, body, http.StatusNotFound)

	clk.Advance(11 * time.Second)
	resp, body = do(t, http.MethodGet, ts.URL+"/kv/b", "")
	wantStatus(t, resp, body, http.StatusNotFound)

	for _, bad := range []string{
		`not json`,
		`[]`,
		`[{"op":"frobnicate","key":"a"}]`,
		`[{"op":"set","key":"","value":"x"}]`,
		`[{"op":"set","key":"a","value":"x","ttl":"later"}]`,
		`[{"op":"set","key":"a","bogus":1}]`,
	} {
		resp, body := do(t, http.MethodPost, ts.URL+"/batch", bad)
		wantJSONError(t, resp, body, http.StatusBadRequest)
	}
	// A rejected batch applies nothing.
	resp, body = do(t, http.MethodPost, ts.URL+"/batch", `[{"op":"set","key":"c","value":"3"},{"op":"set","key":"","value":"x"}]`)
	wantJSONError(t, resp, body, http.StatusBadRequest)
	resp, body = do(t, http.MethodGet, ts.URL+"/kv/c", "")
	wantStatus(t, resp, body, http.StatusNotFound)
}

func TestStatsAndCompact(t *testing.T) {
	ts, _ := newTestServer(t)
	for i := 0; i < 3; i++ {
		do(t, http.MethodPut, ts.URL+"/kv/k", strings.Repeat("v", 100))
	}
	resp, body := do(t, http.MethodGet, ts.URL+"/stats", "")
	wantStatus(t, resp, body, http.StatusOK)
	var st tinykv.Stats
	if err := json.Unmarshal(body, &st); err != nil {
		t.Fatal(err)
	}
	if st.Keys != 1 || st.DeadBytes == 0 {
		t.Fatalf("stats = %+v", st)
	}

	resp, body = do(t, http.MethodPost, ts.URL+"/compact", "")
	wantStatus(t, resp, body, http.StatusOK)
	var out struct {
		Before, After tinykv.Stats
	}
	if err := json.Unmarshal(body, &out); err != nil {
		t.Fatal(err)
	}
	if out.After.DeadBytes != 0 || out.After.FileSize >= out.Before.FileSize || out.After.Keys != 1 {
		t.Fatalf("compact = %s", body)
	}
	_, body = do(t, http.MethodGet, ts.URL+"/kv/k", "")
	if len(body) != 100 {
		t.Fatalf("value lost after compaction: %q", body)
	}
}

func TestHealthAndErrors(t *testing.T) {
	ts, _ := newTestServer(t)
	resp, body := do(t, http.MethodGet, ts.URL+"/healthz", "")
	wantStatus(t, resp, body, http.StatusOK)
	if !bytes.Contains(body, []byte(`"status":"ok"`)) {
		t.Fatalf("body = %s", body)
	}

	resp, body = do(t, http.MethodPost, ts.URL+"/kv/x", "v")
	wantJSONError(t, resp, body, http.StatusMethodNotAllowed)
	if resp.Header.Get("Allow") == "" {
		t.Fatal("missing Allow header on 405")
	}
	resp, body = do(t, http.MethodGet, ts.URL+"/compact", "")
	wantJSONError(t, resp, body, http.StatusMethodNotAllowed)
	resp, body = do(t, http.MethodGet, ts.URL+"/nope", "")
	wantJSONError(t, resp, body, http.StatusNotFound)
}

func TestGracefulShutdown(t *testing.T) {
	db, err := tinykv.Open(t.TempDir(), tinykv.Options{Sync: tinykv.SyncNever, SweepInterval: -1})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	s := New(db, slog.New(slog.NewTextHandler(io.Discard, nil)))
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- s.Serve(ctx, ln) }()

	url := "http://" + ln.Addr().String()
	resp, body := do(t, http.MethodPut, url+"/kv/a", "1")
	wantStatus(t, resp, body, http.StatusNoContent)

	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Serve returned %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("server did not shut down")
	}
	if _, err := http.Get(url + "/healthz"); err == nil {
		t.Fatal("server still accepting connections after shutdown")
	}
}
