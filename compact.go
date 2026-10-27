package tinykv

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"sort"
)

// Compact rewrites the live, non-expired records into a new log file and
// atomically replaces the old one. Dead bytes (overwrites, tombstones,
// expired entries) are reclaimed.
//
// Compact holds writeMu for its entire duration, so other writers wait, but
// readers are served throughout from the old file and index. The write lock
// is only taken for the final swap: close old file, rename, reopen, publish
// the new index. Because the old file is closed under the write lock, no
// reader can be mid-ReadAt on it, which also keeps the rename valid on
// Windows where open files cannot be replaced.
//
// If the process dies before the rename, Open removes the leftover
// ".compact" file and the original log remains authoritative.
func (db *DB) Compact() error {
	db.writeMu.Lock()
	defer db.writeMu.Unlock()

	// Snapshot the index. Nothing can mutate it while we hold writeMu
	// except Sweep, which only removes expired entries that we skip anyway.
	db.mu.RLock()
	if db.closed {
		db.mu.RUnlock()
		return ErrClosed
	}
	oldF := db.f
	type kv struct {
		key string
		e   entry
	}
	snap := make([]kv, 0, len(db.index))
	for k, e := range db.index {
		snap = append(snap, kv{k, e})
	}
	db.mu.RUnlock()

	// Writing in key order keeps the output deterministic and gives
	// sequential scans good locality. The cost is an O(n log n) sort.
	sort.Slice(snap, func(i, j int) bool { return snap[i].key < snap[j].key })

	tmpPath := db.path + compactSuffix
	nf, err := os.OpenFile(tmpPath, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return fmt.Errorf("tinykv: create compaction file: %w", err)
	}
	fail := func(err error) error {
		nf.Close()
		os.Remove(tmpPath)
		return err
	}

	w := bufio.NewWriterSize(nf, 1<<20)
	newIndex := make(map[string]entry, len(snap))
	now := db.nowNanos()
	var off int64
	buf := make([]byte, 0, 4096)
	for _, it := range snap {
		if it.e.expired(now) {
			continue
		}
		if cap(buf) < int(it.e.size) {
			buf = make([]byte, 0, int(it.e.size)*2)
		}
		rec := buf[:it.e.size]
		if _, err := oldF.ReadAt(rec, it.e.offset); err != nil {
			return fail(fmt.Errorf("tinykv: compact read %q: %w", it.key, err))
		}
		if err := verifyRecord(rec); err != nil {
			return fail(fmt.Errorf("tinykv: compact %q at offset %d: %w", it.key, it.e.offset, err))
		}
		if _, err := w.Write(rec); err != nil {
			return fail(fmt.Errorf("tinykv: compact write: %w", err))
		}
		newIndex[it.key] = entry{offset: off, size: it.e.size, expires: it.e.expires}
		off += int64(it.e.size)
	}
	if err := w.Flush(); err != nil {
		return fail(fmt.Errorf("tinykv: compact flush: %w", err))
	}
	if err := nf.Sync(); err != nil {
		return fail(fmt.Errorf("tinykv: compact fsync: %w", err))
	}
	if err := nf.Close(); err != nil {
		os.Remove(tmpPath)
		return fmt.Errorf("tinykv: compact close: %w", err)
	}

	// Swap. Readers are excluded for the duration of this block only.
	db.mu.Lock()
	defer db.mu.Unlock()
	if err := oldF.Close(); err != nil {
		os.Remove(tmpPath)
		return fmt.Errorf("tinykv: close old log: %w", err)
	}
	if err := os.Rename(tmpPath, db.path); err != nil {
		// Try to recover the old handle so the DB remains usable.
		os.Remove(tmpPath)
		if f, oerr := os.OpenFile(db.path, os.O_RDWR, 0o644); oerr == nil {
			db.f = f
		}
		return fmt.Errorf("tinykv: rename compacted log: %w", err)
	}
	syncDir(filepath.Dir(db.path))
	f, err := os.OpenFile(db.path, os.O_RDWR, 0o644)
	if err != nil {
		return fmt.Errorf("tinykv: reopen compacted log: %w", err)
	}
	db.f = f
	db.index = newIndex
	db.size = off
	db.dirty.Store(false)
	return nil
}

// syncDir fsyncs a directory so a rename is durable. Directories cannot be
// fsynced on every platform (e.g. Windows), so failures are ignored.
func syncDir(dir string) {
	d, err := os.Open(dir)
	if err != nil {
		return
	}
	defer d.Close()
	_ = d.Sync()
}
