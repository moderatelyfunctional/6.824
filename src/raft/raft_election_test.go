package raft

import "time"
import "testing"
import "reflect"

func TestElectionStartElectionCountdown(t *testing.T) {
	servers := 1
	electionTimeoutInMs := 50
	currentTerm := 1

	cfg := make_config(t, servers, false, false, true)
	rf := cfg.rafts[0]

	electionTimeoutCalled := false
	go rf.startElectionCountdown(electionTimeoutInMs, currentTerm)
	go func() {
		<-rf.electionChan
		electionTimeoutCalled = true	
	}()
	time.Sleep(time.Duration(electionTimeoutInMs * 2) * time.Millisecond)
	if !electionTimeoutCalled {
		t.Errorf("TestElectionStartElectionCountdown expected 1 election timeout within %d", electionTimeoutInMs)
	}
	
}

func TestElectionDuplicateRequestVoteTo(t *testing.T) {
	expectedVotesReceived := []int{1, 1, 0}

	servers := 3
	cfg := make_config(t, servers, false, false, true)
	rf := cfg.rafts[0]

	rf.setStateToCandidate()
	currentTerm := rf.currentTerm
	lastLogIndex := -1
	lastLogTerm := -1

	rf.requestVoteTo(1, currentTerm, lastLogIndex, lastLogTerm)
	rf.requestVoteTo(1, currentTerm, lastLogIndex, lastLogTerm)

	if !reflect.DeepEqual(rf.votesReceived, expectedVotesReceived) {
		t.Errorf("TestElectionDuplicateRequestVoteTo expected votesReceived %v, got %v", rf.votesReceived, expectedVotesReceived)
	}
}

func TestElectionDuplicateRequestVoteToOutdatedLog(t *testing.T) {
	firstExpectedVotesReceived := []int{1, 1, 0}
	secondExpectedVotesReceived := []int{1, 0, 0}

	servers := 3
	cfg := make_config(t, servers, false, false, true)
	rf := cfg.rafts[0]

	rf.setStateToCandidate()
	currentTerm := rf.currentTerm
	lastLogIndex := -1
	lastLogTerm := -1

	rf.requestVoteTo(1, currentTerm, lastLogIndex, lastLogTerm)
	if !reflect.DeepEqual(rf.votesReceived, firstExpectedVotesReceived) {
		t.Errorf("TestElectionDuplicateRequestVoteTo first expected votesReceived %v, got %v", rf.votesReceived, firstExpectedVotesReceived)
	}
	otherRf := cfg.rafts[1]
	otherRf.log = []Entry{
		Entry{
			Term: 1,
		},
	}
	rf.requestVoteTo(1, currentTerm, lastLogIndex, lastLogTerm)
	if !reflect.DeepEqual(rf.votesReceived, secondExpectedVotesReceived) {
		t.Errorf("TestElectionDuplicateRequestVoteTo second expected votesReceived %v, got %v", rf.votesReceived, secondExpectedVotesReceived)
	}
}