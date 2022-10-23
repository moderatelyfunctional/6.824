package raft

import "time"
// import "reflect"
import "testing"

func TestIsLowerTerm(t *testing.T) {
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

func TestSetStateToFollower(t *testing.T) {
	expected := &Raft{
		currentTerm: 2,
		votedFor: nil,
		votesReceived: 0,
		state: FOLLOWER,
		heartbeat: false,
	}

	t.Run("TestSetStateToFollower", func(t *testing.T) {
		rf := &Raft{
			currentTerm: 1,
			votedFor: nil,
			votesReceived: 2,
			state: CANDIDATE,
			electionChan: make(chan bool),
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
		go func() {
			timeoutInMs := ELECTION_TIMEOUT_MIN_MS + 2 * ELECTION_TIMEOUT_SPREAD_MS
			time.Sleep(time.Duration(timeoutInMs) * time.Millisecond)
			t.Errorf("TestSetStateToFollower: Election timeout never occurred within %v ms", timeoutInMs)
		}()
		<-rf.electionChan
	})
}








