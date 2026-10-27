// Command library shows tinykv used as an embedded store.
package main

import (
	"fmt"
	"log"
	"os"
	"time"

	"github.com/sumit-do/tinykv"
)

func main() {
	dir, err := os.MkdirTemp("", "tinykv-example-*")
	if err != nil {
		log.Fatal(err)
	}
	defer os.RemoveAll(dir)

	// SyncAlways is the default; SyncInterval trades a bounded window of
	// durability for much higher write throughput.
	db, err := tinykv.Open(dir, tinykv.Options{
		Sync:         tinykv.SyncInterval,
		SyncInterval: 50 * time.Millisecond,
	})
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	must(db.Set("user:1:name", []byte("Ada")))
	must(db.Set("user:2:name", []byte("Grace")))
	must(db.SetTTL("session:abc", []byte("token"), 30*time.Minute))

	// Several writes, one append, one index update.
	must(db.Batch([]tinykv.Op{
		{Type: tinykv.OpSet, Key: "user:3:name", Value: []byte("Linus")},
		{Type: tinykv.OpDelete, Key: "user:2:name"},
	}))

	v, err := db.Get("user:1:name")
	must(err)
	fmt.Printf("user:1:name = %s\n", v)

	if _, err := db.Get("user:2:name"); err == tinykv.ErrNotFound {
		fmt.Println("user:2:name was deleted")
	}

	fmt.Println("users:")
	must(db.Scan("user:", func(key string, value []byte) error {
		fmt.Printf("  %s = %s\n", key, value)
		return nil
	}))

	st, err := db.Stats()
	must(err)
	fmt.Printf("stats: %d keys, %d live bytes, %d dead bytes, %d file bytes\n",
		st.Keys, st.LiveBytes, st.DeadBytes, st.FileSize)

	must(db.Compact())
	st, _ = db.Stats()
	fmt.Printf("after compaction: %d file bytes\n", st.FileSize)
}

func must(err error) {
	if err != nil {
		log.Fatal(err)
	}
}
