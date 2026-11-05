package store

import (
	"fmt"
	"testing"
)

// Benchmarks for the in-memory store. Run with:
// go test ./internal/store/ -bench . -benchtime 1000000x

func BenchmarkSet(b *testing.B) {
	s := New()
	defer s.Close()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		s.Set(fmt.Sprintf("key-%d", i), "value", 0)
	}
}

func BenchmarkGet(b *testing.B) {
	s := New()
	defer s.Close()
	for i := 0; i < 1000; i++ {
		s.Set(fmt.Sprintf("key-%d", i), "value", 0)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		s.Get(fmt.Sprintf("key-%d", i%1000))
	}
}

func BenchmarkGetParallel(b *testing.B) {
	s := New()
	defer s.Close()
	for i := 0; i < 1000; i++ {
		s.Set(fmt.Sprintf("key-%d", i), "value", 0)
	}
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			s.Get(fmt.Sprintf("key-%d", i%1000))
			i++
		}
	})
}

// BenchmarkMixed is the closest to real traffic: mostly reads,
// some writes, spread across keys, from several goroutines.
func BenchmarkMixed(b *testing.B) {
	s := New()
	defer s.Close()
	for i := 0; i < 1000; i++ {
		s.Set(fmt.Sprintf("key-%d", i), "value", 0)
	}
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			key := fmt.Sprintf("key-%d", i%1000)
			if i%10 == 0 {
				s.Set(key, "value", 0)
			} else {
				s.Get(key)
			}
			i++
		}
	})
}

func BenchmarkDelete(b *testing.B) {
	s := New()
	defer s.Close()
	for i := 0; i < b.N; i++ {
		s.Set(fmt.Sprintf("key-%d", i), "value", 0)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		s.Delete(fmt.Sprintf("key-%d", i))
	}
}
