package tinykv

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math/rand/v2"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

// fakeClock is an injectable, manually advanced clock.
type fakeClock struct {
	mu sync.Mutex
	t  time.Time
}

func newFakeClock() *fakeClock {
	return &fakeClock{t: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)}
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

func quietLogger() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

func testOpts() Options {
	return Options{Sync: SyncNever, SweepInterval: -1, Logger: quietLogger()}
}

func openTest(t testing.TB, dir string, opts Options) *DB {
	t.Helper()
	db, err := Open(dir, opts)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { db.Close() })
	return db
}

func mustGet(t *testing.T, db *DB, key, want string) {
	t.Helper()
	got, err := db.Get(key)
	if err != nil {
		t.Fatalf("Get(%q): %v", key, err)
	}
	if string(got) != want {
		t.Fatalf("Get(%q) = %q, want %q", key, got, want)
	}
}

func mustMissing(t *testing.T, db *DB, key string) {
	t.Helper()
	if _, err := db.Get(key); !errors.Is(err, ErrNotFound) {
		t.Fatalf("Get(%q) err = %v, want ErrNotFound", key, err)
	}
}

func TestSetGetDelete(t *testing.T) {
	db := openTest(t, t.TempDir(), testOpts())

	mustMissing(t, db, "a")
	if err := db.Set("a", []byte("1")); err != nil {
		t.Fatal(err)
	}
	mustGet(t, db, "a", "1")

	if err := db.Set("a", []byte("two")); err != nil {
		t.Fatal(err)
	}
	mustGet(t, db, "a", "two")

	if err := db.Set("empty", nil); err != nil {
		t.Fatal(err)
	}
	mustGet(t, db, "empty", "")

	if err := db.Delete("a"); err != nil {
		t.Fatal(err)
	}
	mustMissing(t, db, "a")
	if err := db.Delete("a"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("second Delete err = %v, want ErrNotFound", err)
	}

	if err := db.Set("", []byte("x")); !errors.Is(err, ErrEmptyKey) {
		t.Fatalf("empty key err = %v", err)
	}
	ok, err := db.Has("empty")
	if err != nil || !ok {
		t.Fatalf("Has(empty) = %v, %v", ok, err)
	}
}

func TestBatch(t *testing.T) {
	db := openTest(t, t.TempDir(), testOpts())
	if err := db.Set("gone", []byte("x")); err != nil {
		t.Fatal(err)
	}
	ops := []Op{
		{Type: OpSet, Key: "a", Value: []byte("1")},
		{Type: OpSet, Key: "b", Value: []byte("2")},
		{Type: OpDelete, Key: "gone"},
		{Type: OpDelete, Key: "never-existed"},
		{Type: OpSet, Key: "a", Value: []byte("1b")},
	}
	if err := db.Batch(ops); err != nil {
		t.Fatal(err)
	}
	mustGet(t, db, "a", "1b")
	mustGet(t, db, "b", "2")
	mustMissing(t, db, "gone")

	if err := db.Batch([]Op{{Type: 9, Key: "x"}}); !errors.Is(err, ErrInvalidOp) {
		t.Fatalf("invalid op err = %v", err)
	}
	// A failed batch publishes nothing.
	if err := db.Batch([]Op{{Type: OpSet, Key: "c", Value: []byte("3")}, {Type: OpSet, Key: ""}}); err == nil {
		t.Fatal("expected error")
	}
	mustMissing(t, db, "c")
}

func TestTTLExpiry(t *testing.T) {
	clk := newFakeClock()
	opts := testOpts()
	opts.Clock = clk.Now
	dir := t.TempDir()
	db := openTest(t, dir, opts)

	if err := db.SetTTL("session", []byte("tok"), 10*time.Second); err != nil {
		t.Fatal(err)
	}
	if err := db.Set("forever", []byte("v")); err != nil {
		t.Fatal(err)
	}
	mustGet(t, db, "session", "tok")

	clk.Advance(9 * time.Second)
	mustGet(t, db, "session", "tok")

	clk.Advance(2 * time.Second)
	mustMissing(t, db, "session") // lazy expiry on read
	if err := db.Delete("session"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("Delete expired err = %v", err)
	}
	keys, _ := db.Keys("")
	if len(keys) != 1 || keys[0] != "forever" {
		t.Fatalf("Keys = %v", keys)
	}
	st, _ := db.Stats()
	if st.Keys != 1 {
		t.Fatalf("Stats.Keys = %d, want 1", st.Keys)
	}

	n, err := db.Sweep()
	if err != nil || n != 1 {
		t.Fatalf("Sweep = %d, %v; want 1", n, err)
	}
	n, _ = db.Sweep()
	if n != 0 {
		t.Fatalf("second Sweep = %d, want 0", n)
	}

	// Expired records are dropped on replay too.
	db.Close()
	db2 := openTest(t, dir, opts)
	mustMissing(t, db2, "session")
	mustGet(t, db2, "forever", "v")

	// A record that is still live at reopen keeps its original deadline.
	if err := db2.SetTTL("later", []byte("x"), time.Minute); err != nil {
		t.Fatal(err)
	}
	db2.Close()
	db3 := openTest(t, dir, opts)
	mustGet(t, db3, "later", "x")
	clk.Advance(61 * time.Second)
	mustMissing(t, db3, "later")
}

func TestBackgroundSweeper(t *testing.T) {
	clk := newFakeClock()
	opts := testOpts()
	opts.Clock = clk.Now
	opts.SweepInterval = 5 * time.Millisecond
	db := openTest(t, t.TempDir(), opts)
	if err := db.SetTTL("k", []byte("v"), time.Second); err != nil {
		t.Fatal(err)
	}
	clk.Advance(2 * time.Second)
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		db.mu.RLock()
		_, present := db.index["k"]
		db.mu.RUnlock()
		if !present {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("sweeper did not remove expired key")
}

func TestReopenPersistence(t *testing.T) {
	dir := t.TempDir()
	opts := Options{Sync: SyncAlways, SweepInterval: -1, Logger: quietLogger()}
	db := openTest(t, dir, opts)
	for i := 0; i < 100; i++ {
		if err := db.Set(fmt.Sprintf("k%03d", i), []byte(fmt.Sprintf("v%d", i))); err != nil {
			t.Fatal(err)
		}
	}
	if err := db.Set("k005", []byte("overwritten")); err != nil {
		t.Fatal(err)
	}
	if err := db.Delete("k010"); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Get("k001"); !errors.Is(err, ErrClosed) {
		t.Fatalf("Get after Close err = %v", err)
	}
	if err := db.Set("x", nil); !errors.Is(err, ErrClosed) {
		t.Fatalf("Set after Close err = %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}

	db2 := openTest(t, dir, opts)
	mustGet(t, db2, "k001", "v1")
	mustGet(t, db2, "k005", "overwritten")
	mustMissing(t, db2, "k010")
	keys, _ := db2.Keys("")
	if len(keys) != 99 {
		t.Fatalf("got %d keys, want 99", len(keys))
	}
}

func TestSyncIntervalMode(t *testing.T) {
	dir := t.TempDir()
	opts := Options{Sync: SyncInterval, SyncInterval: 5 * time.Millisecond, SweepInterval: -1, Logger: quietLogger()}
	db := openTest(t, dir, opts)
	if err := db.Set("a", []byte("1")); err != nil {
		t.Fatal(err)
	}
	time.Sleep(30 * time.Millisecond)
	if err := db.Sync(); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	db2 := openTest(t, dir, opts)
	mustGet(t, db2, "a", "1")
}

func recSize(key, value string) int64 {
	return int64(headerSize + payloadMeta + len(key) + len(value))
}

func TestCorruptTailRecovery(t *testing.T) {
	type step struct {
		name    string
		corrupt func(t *testing.T, path string, recordOffsets []int64, fileSize int64)
		wantN   int   // keys surviving
		wantLen int64 // file size after recovery
	}
	// Three records, sizes known.
	keys := []string{"alpha", "beta", "gamma"}
	vals := []string{"1111", "22", "333333"}
	var offs []int64
	var total int64
	for i := range keys {
		offs = append(offs, total)
		total += recSize(keys[i], vals[i])
	}

	steps := []step{
		{
			name: "partial header appended",
			corrupt: func(t *testing.T, path string, _ []int64, _ int64) {
				appendBytes(t, path, []byte{0x10, 0x00, 0x00})
			},
			wantN: 3, wantLen: total,
		},
		{
			name: "partial record appended",
			corrupt: func(t *testing.T, path string, _ []int64, _ int64) {
				var r record = record{op: OpSet, key: "delta", value: []byte("4444")}
				b := r.appendTo(nil)
				appendBytes(t, path, b[:len(b)-2])
			},
			wantN: 3, wantLen: total,
		},
		{
			name: "bit flip in last record",
			corrupt: func(t *testing.T, path string, offs []int64, size int64) {
				flipByte(t, path, size-1)
			},
			wantN: 2, wantLen: offs[2],
		},
		{
			name: "zeroed tail",
			corrupt: func(t *testing.T, path string, _ []int64, _ int64) {
				appendBytes(t, path, make([]byte, 64))
			},
			wantN: 3, wantLen: total,
		},
	}

	for _, s := range steps {
		t.Run(s.name, func(t *testing.T) {
			dir := t.TempDir()
			db := openTest(t, dir, testOpts())
			for i := range keys {
				if err := db.Set(keys[i], []byte(vals[i])); err != nil {
					t.Fatal(err)
				}
			}
			db.Close()
			path := filepath.Join(dir, LogFileName)
			s.corrupt(t, path, offs, total)

			var logBuf bytes.Buffer
			opts := testOpts()
			opts.Logger = slog.New(slog.NewTextHandler(&logBuf, nil))
			db2 := openTest(t, dir, opts)
			got, _ := db2.Keys("")
			if len(got) != s.wantN {
				t.Fatalf("recovered %d keys (%v), want %d", len(got), got, s.wantN)
			}
			for i := 0; i < s.wantN; i++ {
				mustGet(t, db2, keys[i], vals[i])
			}
			fi, err := os.Stat(path)
			if err != nil {
				t.Fatal(err)
			}
			if fi.Size() != s.wantLen {
				t.Fatalf("file size after recovery = %d, want %d", fi.Size(), s.wantLen)
			}
			if !bytes.Contains(logBuf.Bytes(), []byte("corrupt log tail")) {
				t.Fatalf("expected warning in log, got %q", logBuf.String())
			}
			// The store must be writable after recovery and the new record
			// must land right at the truncation point.
			if err := db2.Set("after", []byte("ok")); err != nil {
				t.Fatal(err)
			}
			db2.Close()
			db3 := openTest(t, dir, testOpts())
			mustGet(t, db3, "after", "ok")
		})
	}
}

func appendBytes(t *testing.T, path string, b []byte) {
	t.Helper()
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	if _, err := f.Write(b); err != nil {
		t.Fatal(err)
	}
}

func flipByte(t *testing.T, path string, off int64) {
	t.Helper()
	f, err := os.OpenFile(path, os.O_RDWR, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	var b [1]byte
	if _, err := f.ReadAt(b[:], off); err != nil {
		t.Fatal(err)
	}
	b[0] ^= 0xff
	if _, err := f.WriteAt(b[:], off); err != nil {
		t.Fatal(err)
	}
}

func TestCompaction(t *testing.T) {
	dir := t.TempDir()
	db := openTest(t, dir, testOpts())
	const n = 500
	for round := 0; round < 5; round++ {
		for i := 0; i < n; i++ {
			v := fmt.Sprintf("round%d-value%d", round, i)
			if err := db.Set(fmt.Sprintf("key%04d", i), []byte(v)); err != nil {
				t.Fatal(err)
			}
		}
	}
	for i := 0; i < n; i += 2 {
		if err := db.Delete(fmt.Sprintf("key%04d", i)); err != nil {
			t.Fatal(err)
		}
	}
	before, err := db.Stats()
	if err != nil {
		t.Fatal(err)
	}
	if before.Keys != n/2 || before.DeadBytes == 0 || before.LiveBytes+before.DeadBytes != before.FileSize {
		t.Fatalf("unexpected stats before compaction: %+v", before)
	}

	if err := db.Compact(); err != nil {
		t.Fatalf("Compact: %v", err)
	}
	after, err := db.Stats()
	if err != nil {
		t.Fatal(err)
	}
	if after.Keys != n/2 || after.DeadBytes != 0 || after.FileSize != before.LiveBytes {
		t.Fatalf("unexpected stats after compaction: %+v (before %+v)", after, before)
	}
	fi, err := os.Stat(filepath.Join(dir, LogFileName))
	if err != nil {
		t.Fatal(err)
	}
	if fi.Size() != after.FileSize || fi.Size() >= before.FileSize {
		t.Fatalf("file size %d, stats %d, before %d", fi.Size(), after.FileSize, before.FileSize)
	}
	if _, err := os.Stat(filepath.Join(dir, LogFileName+compactSuffix)); !os.IsNotExist(err) {
		t.Fatalf("compaction temp file left behind: %v", err)
	}

	check := func(db *DB) {
		for i := 0; i < n; i++ {
			k := fmt.Sprintf("key%04d", i)
			if i%2 == 0 {
				mustMissing(t, db, k)
			} else {
				mustGet(t, db, k, fmt.Sprintf("round4-value%d", i))
			}
		}
	}
	check(db)

	// Writes after compaction append to the new file at the right offset.
	if err := db.Set("post", []byte("compaction")); err != nil {
		t.Fatal(err)
	}
	mustGet(t, db, "post", "compaction")

	db.Close()
	db2 := openTest(t, dir, testOpts())
	check(db2)
	mustGet(t, db2, "post", "compaction")
}

func TestCompactionDropsExpired(t *testing.T) {
	clk := newFakeClock()
	opts := testOpts()
	opts.Clock = clk.Now
	db := openTest(t, t.TempDir(), opts)
	if err := db.SetTTL("ttl", []byte("x"), time.Second); err != nil {
		t.Fatal(err)
	}
	if err := db.Set("keep", []byte("y")); err != nil {
		t.Fatal(err)
	}
	clk.Advance(2 * time.Second)
	if err := db.Compact(); err != nil {
		t.Fatal(err)
	}
	st, _ := db.Stats()
	if st.Keys != 1 || st.FileSize != recSize("keep", "y") {
		t.Fatalf("stats after compaction: %+v", st)
	}
	mustGet(t, db, "keep", "y")
}

func TestStaleCompactionFileIsRemovedOnOpen(t *testing.T) {
	dir := t.TempDir()
	db := openTest(t, dir, testOpts())
	if err := db.Set("a", []byte("1")); err != nil {
		t.Fatal(err)
	}
	db.Close()
	stale := filepath.Join(dir, LogFileName+compactSuffix)
	if err := os.WriteFile(stale, []byte("garbage"), 0o644); err != nil {
		t.Fatal(err)
	}
	db2 := openTest(t, dir, testOpts())
	mustGet(t, db2, "a", "1")
	if _, err := os.Stat(stale); !os.IsNotExist(err) {
		t.Fatalf("stale compaction file still present: %v", err)
	}
}

func TestKeysAndScan(t *testing.T) {
	db := openTest(t, t.TempDir(), testOpts())
	for _, k := range []string{"user:3", "user:1", "order:9", "user:2", "zz"} {
		if err := db.Set(k, []byte("v-"+k)); err != nil {
			t.Fatal(err)
		}
	}
	keys, err := db.Keys("user:")
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"user:1", "user:2", "user:3"}
	if fmt.Sprint(keys) != fmt.Sprint(want) {
		t.Fatalf("Keys(user:) = %v, want %v", keys, want)
	}
	all, _ := db.Keys("")
	if len(all) != 5 || all[0] != "order:9" || all[4] != "zz" {
		t.Fatalf("Keys(\"\") = %v", all)
	}
	none, _ := db.Keys("nope")
	if len(none) != 0 {
		t.Fatalf("Keys(nope) = %v", none)
	}

	var seen []string
	err = db.Scan("user:", func(k string, v []byte) error {
		if string(v) != "v-"+k {
			t.Errorf("Scan value for %q = %q", k, v)
		}
		seen = append(seen, k)
		return nil
	})
	if err != nil || fmt.Sprint(seen) != fmt.Sprint(want) {
		t.Fatalf("Scan = %v, %v", seen, err)
	}

	seen = nil
	err = db.Scan("", func(k string, _ []byte) error {
		seen = append(seen, k)
		if len(seen) == 2 {
			return ErrStopScan
		}
		return nil
	})
	if err != nil || len(seen) != 2 {
		t.Fatalf("early stop: seen=%v err=%v", seen, err)
	}

	boom := errors.New("boom")
	if err := db.Scan("", func(string, []byte) error { return boom }); !errors.Is(err, boom) {
		t.Fatalf("Scan error propagation: %v", err)
	}
}

func TestConcurrentReadersAndWriter(t *testing.T) {
	db := openTest(t, t.TempDir(), testOpts())
	const n = 3000
	key := func(i int) string { return fmt.Sprintf("key-%05d", i) }

	stop := make(chan struct{})
	var wg sync.WaitGroup
	for r := 0; r < 4; r++ {
		wg.Add(1)
		go func(seed uint64) {
			defer wg.Done()
			rng := rand.New(rand.NewPCG(seed, seed))
			for {
				select {
				case <-stop:
					return
				default:
				}
				i := rng.IntN(n)
				v, err := db.Get(key(i))
				if err != nil && !errors.Is(err, ErrNotFound) {
					t.Errorf("Get: %v", err)
					return
				}
				if err == nil && !bytes.HasPrefix(v, []byte("val-")) {
					t.Errorf("Get(%q) returned garbage %q", key(i), v)
					return
				}
				if rng.IntN(50) == 0 {
					if _, err := db.Keys("key-00"); err != nil {
						t.Errorf("Keys: %v", err)
						return
					}
					if _, err := db.Stats(); err != nil {
						t.Errorf("Stats: %v", err)
						return
					}
				}
			}
		}(uint64(r + 1))
	}

	for i := 0; i < n; i++ {
		if err := db.Set(key(i), []byte(fmt.Sprintf("val-%d", i))); err != nil {
			t.Fatal(err)
		}
		if i%7 == 0 {
			if err := db.Set(key(i), []byte(fmt.Sprintf("val-%d-again", i))); err != nil {
				t.Fatal(err)
			}
		}
		if i%1000 == 999 {
			if err := db.Compact(); err != nil {
				t.Fatalf("Compact: %v", err)
			}
		}
	}
	close(stop)
	wg.Wait()

	for i := 0; i < n; i++ {
		want := fmt.Sprintf("val-%d", i)
		if i%7 == 0 {
			want += "-again"
		}
		mustGet(t, db, key(i), want)
	}
}

func TestConcurrentWriters(t *testing.T) {
	db := openTest(t, t.TempDir(), testOpts())
	var wg sync.WaitGroup
	const writers, per = 8, 200
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < per; i++ {
				k := fmt.Sprintf("w%d-%d", w, i)
				if err := db.Set(k, []byte(k)); err != nil {
					t.Errorf("Set: %v", err)
					return
				}
			}
		}(w)
	}
	wg.Wait()
	st, _ := db.Stats()
	if st.Keys != writers*per || st.DeadBytes != 0 {
		t.Fatalf("stats = %+v", st)
	}
	for w := 0; w < writers; w++ {
		for i := 0; i < per; i++ {
			k := fmt.Sprintf("w%d-%d", w, i)
			mustGet(t, db, k, k)
		}
	}
}

func TestRecordRoundTrip(t *testing.T) {
	r := record{op: OpSet, expires: 12345, key: "k", value: []byte("hello")}
	b := r.appendTo(nil)
	if len(b) != r.size() {
		t.Fatalf("encoded %d bytes, size() = %d", len(b), r.size())
	}
	if err := verifyRecord(b); err != nil {
		t.Fatal(err)
	}
	scratch := make([]byte, 0, 8)
	got, n, err := readRecord(bytes.NewReader(b), &scratch)
	if err != nil || n != len(b) {
		t.Fatalf("readRecord: n=%d err=%v", n, err)
	}
	if got.op != r.op || got.expires != r.expires || got.key != r.key || string(got.value) != "hello" {
		t.Fatalf("decoded %+v", got)
	}
	b[len(b)-1] ^= 1
	if err := verifyRecord(b); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("verifyRecord on corrupt = %v", err)
	}
}
