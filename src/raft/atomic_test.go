package raft

// import "fmt"
import "sync/atomic"

import "testing"

func TestAtomicAddSub(t *testing.T) {
	var b int32
	b = 0

	ch := make(chan bool)
	for i := 0; i < 10; i++ {
		go func() {
			atomic.AddInt32(&b, 1)
			atomic.AddInt32(&b, -1)
			ch<-true
		}()
	}

	for i := 0; i < 10; i++ {
		<-ch
	}
	if atomic.LoadInt32(&b) != 0 {
		t.Errorf("TestAtomicAddSub expected 0 but got %v", atomic.LoadInt32(&b))
	}
}