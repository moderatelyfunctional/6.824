package raft

import "time"
import "testing"

func TestStateIsLowerTerm(t *testing.T) {
	inputs := [][]any{
		{t, 1, 0, false},
		{t, 1, 1, false},
		{t, 1, 2, true},
	}

	test := func(t *testing.T, currentTerm int, otherTerm int, expected bool) {
		rf := &Raft{}
		rf.currentTerm = currentTerm
		if (rf.isLowerTerm(otherTerm) != expected) {
			t.Errorf("TestIsLowerTerm:\nexpected %v\ngot %v", expected, rf.isLowerTerm(otherTerm))
			t.Errorf("Inputs are: currentTerm %v otherTerm %v", currentTerm, otherTerm)
		}
	}
	parametrize(test, inputs)
}

func checkElectionTimeout(t *testing.T, rf *Raft) {
	timeoutCalled := false
	go func() {
		timeoutInMs := ELECTION_TIMEOUT_MIN_MS + 2 * ELECTION_TIMEOUT_SPREAD_MS
		time.Sleep(time.Duration(timeoutInMs) * time.Millisecond)
		if !timeoutCalled {
			t.Errorf("TestSetStateToFollower: Election timeout never occurred within %v ms", timeoutInMs)			
		}
	}()
	timeoutTerm := <-rf.electionChan
	timeoutCalled = true

	if timeoutTerm != rf.currentTerm {
		t.Errorf("TestSetStateToFollower: Timeout term expected %v\ngot%v", rf.currentTerm, timeoutTerm)
	}
}

func TestStateSetStateToFollower(t *testing.T) {
	expected := &Raft{
		currentTerm: 3,
		me: 2,
		votedFor: -1,
		votesReceived: 0,
		state: FOLLOWER,
		heartbeat: false,
	}

	rf := &Raft{
		currentTerm: 1,
		me: 2,
		votedFor: -1,
		votesReceived: 2,
		state: CANDIDATE,
		electionChan: make(chan int),
	}
	rf.setStateToFollower(expected.currentTerm)
	if rf.currentTerm != expected.currentTerm ||
		rf.votedFor != expected.votedFor ||
		rf.votesReceived != expected.votesReceived ||
		rf.state != expected.state ||
		rf.heartbeat != expected.heartbeat ||
		rf.electionTimeout == 0  {
		t.Errorf("TestSetStateToFollower:\nexpected %v\ngot %v", expected, rf)
	}

	checkElectionTimeout(t, rf)
}

func TestStateSetStateToCandidate(t *testing.T) {
	expected := &Raft{
		currentTerm: 2,
		me: 2,
		votedFor: 2,
		votesReceived: 1,
		state: CANDIDATE,
		heartbeat: false,
	}

	rf := &Raft{
		currentTerm: 1,
		me: 2,
		votedFor: -1,
		votesReceived: 2,
		state: CANDIDATE,
		electionChan: make(chan int),
	}
	rf.setStateToCandidate()
	if rf.currentTerm != expected.currentTerm ||
		rf.votedFor != expected.votedFor ||
		rf.votesReceived != expected.votesReceived ||
		rf.state != expected.state ||
		rf.heartbeat != expected.heartbeat ||
		rf.electionTimeout == 0  {
		t.Errorf("TestSetStateToCandidate:\nexpected %v\ngot %v", expected, rf)
	}

	checkElectionTimeout(t, rf)
}

func TestStateSetStateToLeader(t *testing.T) {
	expected := &Raft{
		state: LEADER,
	}

	rf := &Raft{
		state: CANDIDATE,
	}
	rf.setStateToLeader()
	if rf.state != expected.state {
		t.Errorf("TestSetStateToLeader:\nexpected %v\ngot %v", expected.state, rf.state)
	}
}




