package tinykv

import (
	"fmt"
	"math/rand/v2"
	"testing"
)

const (
	benchKeys    = 10_000
	benchValSize = 100
)

func benchDB(b *testing.B) (*DB, []string, []byte) {
	b.Helper()
	db := openTest(b, b.TempDir(), testOpts())
	keys := make([]string, benchKeys)
	for i := range keys {
		keys[i] = fmt.Sprintf("bench-key-%06d", i)
	}
	val := make([]byte, benchValSize)
	for i := range val {
		val[i] = byte('a' + i%26)
	}
	return db, keys, val
}

func BenchmarkSet(b *testing.B) {
	db, keys, val := benchDB(b)
	b.SetBytes(int64(benchValSize))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := db.Set(keys[i%benchKeys], val); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkGet(b *testing.B) {
	db, keys, val := benchDB(b)
	for _, k := range keys {
		if err := db.Set(k, val); err != nil {
			b.Fatal(err)
		}
	}
	b.SetBytes(int64(benchValSize))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := db.Get(keys[i%benchKeys]); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkMixed is 80% reads / 20% writes over a 10k-key working set.
func BenchmarkMixed(b *testing.B) {
	db, keys, val := benchDB(b)
	for _, k := range keys {
		if err := db.Set(k, val); err != nil {
			b.Fatal(err)
		}
	}
	rng := rand.New(rand.NewPCG(1, 2))
	b.SetBytes(int64(benchValSize))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		k := keys[rng.IntN(benchKeys)]
		if i%5 == 0 {
			if err := db.Set(k, val); err != nil {
				b.Fatal(err)
			}
		} else if _, err := db.Get(k); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkGetParallel measures read throughput with GOMAXPROCS readers.
func BenchmarkGetParallel(b *testing.B) {
	db, keys, val := benchDB(b)
	for _, k := range keys {
		if err := db.Set(k, val); err != nil {
			b.Fatal(err)
		}
	}
	b.SetBytes(int64(benchValSize))
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			if _, err := db.Get(keys[i%benchKeys]); err != nil {
				b.Fatal(err)
			}
			i++
		}
	})
}
