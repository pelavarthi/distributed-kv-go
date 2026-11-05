package wal

import (
	"fmt"
	"path/filepath"
	"testing"
)

// BenchmarkAppendSet measures one durable write: marshal, write,
// and sync to disk. The sync is the point of the WAL, so this
// number is the write-throughput ceiling for a single node.
// Run with: go test ./internal/wal/ -bench . -benchtime 100x
func BenchmarkAppendSet(b *testing.B) {
	l, err := Open(filepath.Join(b.TempDir(), "wal.log"))
	if err != nil {
		b.Fatal(err)
	}
	defer l.Close()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := l.AppendSet(fmt.Sprintf("key-%d", i), "value", 0); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkAppendSetParallel shows whether concurrent writers
// help or hurt: every append still syncs, so the disk, not the
// lock, sets the pace.
func BenchmarkAppendSetParallel(b *testing.B) {
	l, err := Open(filepath.Join(b.TempDir(), "wal.log"))
	if err != nil {
		b.Fatal(err)
	}
	defer l.Close()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			if err := l.AppendSet(fmt.Sprintf("key-%d", i), "value", 0); err != nil {
				b.Fatal(err)
			}
			i++
		}
	})
}
