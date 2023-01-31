package raft

import "fmt"
import "time"
import "sync"
import "sync/atomic"

import "testing"

type Benchmark struct {
	value		int32
}

func TestBenchmarkLock(t *testing.T) {
	start := time.Now()
	benchmark := Benchmark{}
	var mu sync.Mutex

	mu.Lock()
	benchmark.value = 3
	mu.Unlock()
	end := time.Now()

	fmt.Println("TestBenchmarkLock ", end.Sub(start))
}

func TestBenchmarkAtomic(t *testing.T) {
	start := time.Now()
	benchmark := Benchmark{}
	// atomic.StoreInt32(&benchmark.value, 1)
	atomic.SwapInt32(&benchmark.value, int32(3))
	end := time.Now()

	fmt.Println("TestBenchmarkAtomic ", end.Sub(start))
}