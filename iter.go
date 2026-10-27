package tinykv

import (
	"sort"
	"strings"
)

// Keys returns all live keys with the given prefix in ascending byte order.
// Pass "" to list every key.
//
// Tradeoff: the index is a hash map, which makes Get O(1) and keeps the
// write path trivial, but it has no ordering. Keys therefore walks the whole
// index (O(n)), filters by prefix, and sorts the matches (O(m log m)). This
// is the right choice when point lookups dominate and prefix listings are
// occasional; a store built for range queries would use a sorted structure
// (B-tree, skip list, or a radix tree) and pay for it on every write.
func (db *DB) Keys(prefix string) ([]string, error) {
	db.mu.RLock()
	if db.closed {
		db.mu.RUnlock()
		return nil, ErrClosed
	}
	now := db.nowNanos()
	keys := make([]string, 0, 16)
	for k, e := range db.index {
		if strings.HasPrefix(k, prefix) && !e.expired(now) {
			keys = append(keys, k)
		}
	}
	db.mu.RUnlock()
	sort.Strings(keys)
	return keys, nil
}

// Scan calls fn for every live key with the given prefix, in ascending key
// order. Return ErrStopScan from fn to stop early without error; any other
// error aborts the scan and is returned.
//
// Scan snapshots the matching keys under the read lock, then reads each
// value individually, so writers are not blocked for the whole scan. The
// consequence is that Scan is not a point-in-time snapshot: a key deleted
// concurrently is skipped, and a value overwritten concurrently is returned
// in its newer form.
func (db *DB) Scan(prefix string, fn func(key string, value []byte) error) error {
	keys, err := db.Keys(prefix)
	if err != nil {
		return err
	}
	for _, k := range keys {
		v, err := db.Get(k)
		if err == ErrNotFound {
			continue
		}
		if err != nil {
			return err
		}
		if err := fn(k, v); err != nil {
			if err == ErrStopScan {
				return nil
			}
			return err
		}
	}
	return nil
}
