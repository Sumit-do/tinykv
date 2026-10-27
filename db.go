// Package tinykv is a small, persistent, embedded key-value store.
//
// Design in one paragraph: every write is appended to a single write-ahead
// log file as a length-prefixed, CRC32-checksummed record. An in-memory hash
// index maps each live key to the file offset of its latest record
// (Bitcask-style), so Get is one hash lookup plus one positional read.
// Open replays the log to rebuild the index and truncates any corrupt or
// partially written tail. Compact rewrites only the live records into a fresh
// log and atomically swaps it in.
//
// # Locking model
//
// DB uses two locks:
//
//   - writeMu (sync.Mutex) serializes all mutations of the log file: Set,
//     Delete, Batch, Compact and Close. Only one of these runs at a time.
//   - mu (sync.RWMutex) guards the in-memory state (index, file handle, size).
//     Readers (Get, Keys, Scan, Stats) take the read lock. Writers append to
//     the file while holding only writeMu, then take the write lock for the
//     brief moment needed to publish the new index entries.
//
// Because the log is append-only and readers only access offsets that are
// already published in the index, an in-flight append never races with a
// read. Compaction holds writeMu for its whole duration (blocking other
// writers but not readers), builds the new file from a snapshot of the index,
// and only takes the write lock for the final file swap.
package tinykv

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"
)

// Sentinel errors returned by DB methods.
var (
	ErrNotFound      = errors.New("tinykv: key not found")
	ErrClosed        = errors.New("tinykv: database is closed")
	ErrEmptyKey      = errors.New("tinykv: key must not be empty")
	ErrKeyTooLarge   = errors.New("tinykv: key exceeds MaxKeySize")
	ErrValueTooLarge = errors.New("tinykv: value exceeds MaxValueSize")
	ErrInvalidOp     = errors.New("tinykv: invalid batch operation")
	// ErrStopScan can be returned from a Scan callback to end the scan early
	// without Scan itself reporting an error.
	ErrStopScan = errors.New("tinykv: stop scan")
)

// LogFileName is the name of the log file inside the data directory.
const LogFileName = "tinykv.log"

const compactSuffix = ".compact"

// SyncMode controls when the log is flushed to stable storage.
type SyncMode int

const (
	// SyncAlways fsyncs after every write. Slowest, most durable (default).
	SyncAlways SyncMode = iota
	// SyncInterval fsyncs in the background every Options.SyncInterval.
	// A crash can lose up to one interval of acknowledged writes.
	SyncInterval
	// SyncNever leaves fsync to the operating system. Fastest; a power
	// loss may lose any writes still in the OS page cache.
	SyncNever
)

func (m SyncMode) String() string {
	switch m {
	case SyncAlways:
		return "always"
	case SyncInterval:
		return "interval"
	case SyncNever:
		return "never"
	}
	return fmt.Sprintf("SyncMode(%d)", int(m))
}

// Options configures a DB. The zero value is a safe default: fsync on every
// write, sweep expired keys every minute, wall clock, default logger.
type Options struct {
	// Sync selects the fsync policy.
	Sync SyncMode
	// SyncInterval is the background fsync period for SyncInterval
	// (default 100ms).
	SyncInterval time.Duration
	// SweepInterval is how often expired keys are purged from the index
	// (default 1 minute). A negative value disables the background sweeper;
	// expired keys are then only dropped lazily on read and during Compact.
	SweepInterval time.Duration
	// Clock returns the current time. Injectable for tests (default time.Now).
	Clock func() time.Time
	// Logger receives warnings such as a truncated log tail (default slog.Default()).
	Logger *slog.Logger
}

func (o Options) withDefaults() Options {
	if o.SyncInterval <= 0 {
		o.SyncInterval = 100 * time.Millisecond
	}
	if o.SweepInterval == 0 {
		o.SweepInterval = time.Minute
	}
	if o.Clock == nil {
		o.Clock = time.Now
	}
	if o.Logger == nil {
		o.Logger = slog.Default()
	}
	return o
}

// Op is a single operation inside a Batch.
type Op struct {
	Type  OpType
	Key   string
	Value []byte        // used by OpSet
	TTL   time.Duration // used by OpSet; 0 means no expiry
}

// Stats is a point-in-time snapshot of store size metrics.
type Stats struct {
	// Keys is the number of live (non-expired) keys.
	Keys int `json:"keys"`
	// LiveBytes is the on-disk size of the records that are still current.
	LiveBytes int64 `json:"live_bytes"`
	// DeadBytes is the on-disk size of overwritten, deleted or expired
	// records. Compaction reclaims this space.
	DeadBytes int64 `json:"dead_bytes"`
	// FileSize is the current size of the log file.
	FileSize int64 `json:"file_size"`
}

// entry is the index value: where the latest record for a key lives.
type entry struct {
	offset  int64
	size    uint32
	expires int64
}

func (e entry) expired(now int64) bool { return e.expires != 0 && e.expires <= now }

// DB is a persistent key-value store. It is safe for concurrent use.
type DB struct {
	path string
	opts Options

	// writeMu serializes Set/Delete/Batch/Compact/Close. See the package
	// documentation for the full locking model.
	writeMu sync.Mutex
	// mu guards everything below it.
	mu     sync.RWMutex
	f      *os.File
	size   int64
	index  map[string]entry
	closed bool

	dirty atomic.Bool // unsynced writes pending (SyncInterval mode)

	stop chan struct{}
	wg   sync.WaitGroup
}

// Open opens (or creates) the store in dir and replays its log.
func Open(dir string, opts Options) (*DB, error) {
	opts = opts.withDefaults()
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("tinykv: create dir: %w", err)
	}
	path := filepath.Join(dir, LogFileName)
	// A leftover compaction file means a previous compaction died before
	// the rename; the original log is still authoritative.
	if err := os.Remove(path + compactSuffix); err != nil && !os.IsNotExist(err) {
		return nil, fmt.Errorf("tinykv: remove stale compaction file: %w", err)
	}
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return nil, fmt.Errorf("tinykv: open log: %w", err)
	}
	db := &DB{
		path:  path,
		opts:  opts,
		f:     f,
		index: make(map[string]entry),
		stop:  make(chan struct{}),
	}
	if err := db.replay(); err != nil {
		f.Close()
		return nil, err
	}
	if opts.Sync == SyncInterval {
		db.wg.Add(1)
		go db.syncLoop()
	}
	if opts.SweepInterval > 0 {
		db.wg.Add(1)
		go db.sweepLoop()
	}
	return db, nil
}

// replay scans the log from the start, rebuilding the index. It stops at the
// first truncated or corrupt record and truncates the file there.
func (db *DB) replay() error {
	br := bufio.NewReaderSize(db.f, 1<<20)
	now := db.nowNanos()
	var off int64
	scratch := make([]byte, 0, 4096)
	for {
		rec, n, err := readRecord(br, &scratch)
		if err == io.EOF {
			break
		}
		if errors.Is(err, errTruncated) || errors.Is(err, ErrCorrupt) {
			db.opts.Logger.Warn("tinykv: discarding corrupt log tail",
				"path", db.path, "offset", off, "reason", err)
			if terr := db.f.Truncate(off); terr != nil {
				return fmt.Errorf("tinykv: truncate corrupt tail: %w", terr)
			}
			break
		}
		if err != nil {
			return fmt.Errorf("tinykv: replay: %w", err)
		}
		switch rec.op {
		case OpSet:
			if rec.expires != 0 && rec.expires <= now {
				delete(db.index, rec.key)
			} else {
				db.index[rec.key] = entry{offset: off, size: uint32(n), expires: rec.expires}
			}
		case OpDelete:
			delete(db.index, rec.key)
		}
		off += int64(n)
	}
	db.size = off
	return nil
}

func (db *DB) now() time.Time  { return db.opts.Clock() }
func (db *DB) nowNanos() int64 { return db.opts.Clock().UnixNano() }

// Get returns the value for key, or ErrNotFound if the key is absent or
// has expired.
func (db *DB) Get(key string) ([]byte, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()
	if db.closed {
		return nil, ErrClosed
	}
	e, ok := db.index[key]
	if !ok || e.expired(db.nowNanos()) {
		return nil, ErrNotFound
	}
	vlen := int(e.size) - headerSize - payloadMeta - len(key)
	buf := make([]byte, vlen)
	if _, err := db.f.ReadAt(buf, e.offset+int64(headerSize+payloadMeta+len(key))); err != nil {
		return nil, fmt.Errorf("tinykv: read value: %w", err)
	}
	return buf, nil
}

// Has reports whether key is present and not expired.
func (db *DB) Has(key string) (bool, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()
	if db.closed {
		return false, ErrClosed
	}
	e, ok := db.index[key]
	return ok && !e.expired(db.nowNanos()), nil
}

// Set stores value under key with no expiry.
func (db *DB) Set(key string, value []byte) error { return db.SetTTL(key, value, 0) }

// SetTTL stores value under key. If ttl > 0 the key expires after ttl.
func (db *DB) SetTTL(key string, value []byte, ttl time.Duration) error {
	return db.Batch([]Op{{Type: OpSet, Key: key, Value: value, TTL: ttl}})
}

// Delete removes key. It returns ErrNotFound if the key is absent.
func (db *DB) Delete(key string) error {
	db.writeMu.Lock()
	defer db.writeMu.Unlock()
	db.mu.RLock()
	closed := db.closed
	e, ok := db.index[key]
	db.mu.RUnlock()
	if closed {
		return ErrClosed
	}
	if !ok || e.expired(db.nowNanos()) {
		return ErrNotFound
	}
	return db.applyLocked([]Op{{Type: OpDelete, Key: key}})
}

// Batch applies ops atomically with respect to other writers: they are
// encoded into one contiguous append and published to the index together.
// Deleting a missing key inside a batch is not an error.
func (db *DB) Batch(ops []Op) error {
	if len(ops) == 0 {
		return nil
	}
	db.writeMu.Lock()
	defer db.writeMu.Unlock()
	db.mu.RLock()
	closed := db.closed
	db.mu.RUnlock()
	if closed {
		return ErrClosed
	}
	return db.applyLocked(ops)
}

// applyLocked encodes, appends, syncs and publishes ops. Caller holds writeMu.
func (db *DB) applyLocked(ops []Op) error {
	now := db.now()
	recs := make([]record, len(ops))
	total := 0
	for i, op := range ops {
		if op.Key == "" {
			return ErrEmptyKey
		}
		if len(op.Key) > MaxKeySize {
			return ErrKeyTooLarge
		}
		switch op.Type {
		case OpSet:
			if len(op.Value) > MaxValueSize {
				return ErrValueTooLarge
			}
			var exp int64
			if op.TTL > 0 {
				exp = now.Add(op.TTL).UnixNano()
			}
			recs[i] = record{op: OpSet, expires: exp, key: op.Key, value: op.Value}
		case OpDelete:
			recs[i] = record{op: OpDelete, key: op.Key}
		default:
			return fmt.Errorf("%w: type %d", ErrInvalidOp, op.Type)
		}
		total += recs[i].size()
	}
	buf := make([]byte, 0, total)
	for i := range recs {
		buf = recs[i].appendTo(buf)
	}

	// db.f and db.size are only ever changed by code holding writeMu
	// (which we hold), so reading them here without mu is race-free.
	f, off := db.f, db.size
	if _, err := f.WriteAt(buf, off); err != nil {
		// Nothing is published; the next append overwrites the partial
		// write and replay would discard it as a corrupt tail anyway.
		return fmt.Errorf("tinykv: append: %w", err)
	}
	if db.opts.Sync == SyncAlways {
		if err := f.Sync(); err != nil {
			return fmt.Errorf("tinykv: fsync: %w", err)
		}
	} else {
		db.dirty.Store(true)
	}

	db.mu.Lock()
	pos := off
	for i := range recs {
		sz := recs[i].size()
		switch recs[i].op {
		case OpSet:
			db.index[recs[i].key] = entry{offset: pos, size: uint32(sz), expires: recs[i].expires}
		case OpDelete:
			delete(db.index, recs[i].key)
		}
		pos += int64(sz)
	}
	db.size = pos
	db.mu.Unlock()
	return nil
}

// Sync forces any buffered writes to stable storage.
func (db *DB) Sync() error {
	db.mu.RLock()
	defer db.mu.RUnlock()
	if db.closed {
		return ErrClosed
	}
	db.dirty.Store(false)
	return db.f.Sync()
}

// Sweep drops every expired key from the in-memory index and returns how
// many were removed. No disk I/O is needed: replay and compaction already
// skip expired records, so the on-disk log needs no tombstone.
func (db *DB) Sweep() (int, error) {
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.closed {
		return 0, ErrClosed
	}
	now := db.nowNanos()
	n := 0
	for k, e := range db.index {
		if e.expired(now) {
			delete(db.index, k)
			n++
		}
	}
	return n, nil
}

// Stats returns size metrics. It walks the index, so it is O(keys).
func (db *DB) Stats() (Stats, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()
	if db.closed {
		return Stats{}, ErrClosed
	}
	now := db.nowNanos()
	var s Stats
	for _, e := range db.index {
		if e.expired(now) {
			continue
		}
		s.Keys++
		s.LiveBytes += int64(e.size)
	}
	s.FileSize = db.size
	s.DeadBytes = s.FileSize - s.LiveBytes
	return s, nil
}

// Close stops background goroutines, flushes and closes the log. It is
// safe to call more than once; later calls return nil.
func (db *DB) Close() error {
	db.writeMu.Lock()
	defer db.writeMu.Unlock()
	db.mu.RLock()
	closed := db.closed
	db.mu.RUnlock()
	if closed {
		return nil
	}
	close(db.stop)
	db.wg.Wait()

	db.mu.Lock()
	defer db.mu.Unlock()
	db.closed = true
	var err error
	if db.dirty.Load() {
		err = db.f.Sync()
	}
	if cerr := db.f.Close(); err == nil {
		err = cerr
	}
	return err
}

func (db *DB) syncLoop() {
	defer db.wg.Done()
	t := time.NewTicker(db.opts.SyncInterval)
	defer t.Stop()
	for {
		select {
		case <-db.stop:
			return
		case <-t.C:
			if db.dirty.Swap(false) {
				db.mu.RLock()
				if err := db.f.Sync(); err != nil {
					db.opts.Logger.Warn("tinykv: background fsync failed", "err", err)
				}
				db.mu.RUnlock()
			}
		}
	}
}

func (db *DB) sweepLoop() {
	defer db.wg.Done()
	t := time.NewTicker(db.opts.SweepInterval)
	defer t.Stop()
	for {
		select {
		case <-db.stop:
			return
		case <-t.C:
			if _, err := db.Sweep(); err != nil && !errors.Is(err, ErrClosed) {
				db.opts.Logger.Warn("tinykv: sweep failed", "err", err)
			}
		}
	}
}
