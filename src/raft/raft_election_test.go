package raft

import "fmt"
import "time"
import "testing"

func TestElectionStartElectionCountdown(t *testing.T) {
	servers := 1
	electionTimeoutInMs := 50
	currentTerm := 1

	cfg := make_config(t, servers, false, false, true)
	raft := cfg.rafts[0]

	electionTimeoutCalled := false
	go raft.startElectionCountdown(electionTimeoutInMs, currentTerm)
	go func() {
		time.Sleep(time.Duration(electionTimeoutInMs * 2) * time.Millisecond)
		if !electionTimeoutCalled {
			t.Errorf("TestElectionStartElectionCountdown expected 1 election timeout within %d", electionTimeoutInMs)
		}
	}()
	<-raft.electionChan
	electionTimeoutCalled = true
}