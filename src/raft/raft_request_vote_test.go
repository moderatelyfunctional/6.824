package raft

import "time"
import "reflect"
import "testing"

// if VoteGranted is false, then the Term in RequestVoteReply is the term that 
// the caller (raft instance) should update its term to.
func TestRequestVoteLowerTermCandidate(t *testing.T) {
	expected := &RequestVoteReply{
		Term: 2,
		VoteGranted: false,
	}
	args := &RequestVoteArgs{
		Term: 1,
		CandidateId: 0,
	}
	reply := &RequestVoteReply{}

	rf := &Raft{}
	rf.setStateToFollower(args.Term + 1)
	rf.RequestVote(args, reply)
	if !reflect.DeepEqual(*expected, *reply) {
		t.Errorf("TestRequestVoteFromCandidateWithLowerTerm expected %#v\ngot %#v", expected, reply)
	}
}

// raft instance with me = 3 and votedFor = 4 won't grant vote to a candidate = 2 on the same term
// if it's already voted for another candidate even if the candidate's log is more up-to-date.
func TestRequestVoteSameTermCandidateDidVote(t *testing.T) {
	rf := &Raft{
		me: 3,
		votedFor: 4,
		currentTerm: 7,
		log: []Entry{
			Entry{
				term: 2,
			},
			Entry{
				term: 5,
			},
		},
	}
	args := &RequestVoteArgs{
		CandidateId: 2,
		Term: 7,
		LastLogIndex: 4,
		LastLogTerm: 6,
	}
	expected := &RequestVoteReply{
		Term: 7,
		VoteGranted: false,
	}
	reply := &RequestVoteReply{}
	rf.RequestVote(args, reply)
	if !reflect.DeepEqual(*expected, *reply) {
		t.Errorf("TestRequestVoteFromCandidateInSameTermVotedForOtherCandidate expected %#v\ngot %#v", expected, reply)
	}
}

// raft instance with me = 3 and votedFor = -1 does grant vote to another candidate = 2 on the same term
// if it's never voted for a candidate and the candidate's log is more up-to-date.
func TestRequestVoteSameTermCandidateDidntVote(t *testing.T) {
	rf := &Raft{
		me: 3,
		votedFor: -1,
		currentTerm: 7,
		log: []Entry{
			Entry{
				term: 2,
			},
			Entry{
				term: 5,
			},
		},
	}
	args := &RequestVoteArgs{
		CandidateId: 2,
		Term: 7,
		LastLogIndex: 4,
		LastLogTerm: 6,
	}
	expected := &RequestVoteReply{
		Term: 7,
		VoteGranted: true,
	}
	reply := &RequestVoteReply{}
	rf.RequestVote(args, reply)
	if !reflect.DeepEqual(*expected, *reply) {
		t.Errorf("TestRequestVoteFromCandidateInSameTermVotedForOtherCandidate expected %#v\ngot %#v", expected, reply)
	}
}

func TestRequestVoteHigherTermCandidateSameUpToDateLog(t *testing.T) {
	args := &RequestVoteArgs{
		Term: 3,
		CandidateId: 0,
		LastLogIndex: -1,
		LastLogTerm: -1,
	}
	expected := &RequestVoteReply{
		Term: 3,
		VoteGranted: true,
	}
	reply := &RequestVoteReply{}
	
	rf := &Raft{
		electionChan: make(chan int),
	}
	rf.setStateToFollower(args.Term - 1)
	rf.RequestVote(args, reply)
	if !reflect.DeepEqual(*expected, *reply) {
		t.Errorf("TestRequestVoteHigherTermCandidateSameUpToDateLog expected %#v\ngot %#v", expected, reply)
	}

	timeoutCalledTwice := false
	go func() {
		timeoutInMs := ELECTION_TIMEOUT_MIN_MS + 2 * ELECTION_TIMEOUT_SPREAD_MS
		time.Sleep(time.Duration(timeoutInMs * 2) * time.Millisecond)
		if !timeoutCalledTwice {
			t.Errorf("TestRequestVoteHigherTermCandidateSameUpToDateLog expected 2 election timeouts within %d", timeoutInMs)
		}
	}()
	<-rf.electionChan
	<-rf.electionChan

	timeoutCalledTwice = true
}

func TestRequestVoteHigherTermCandidateOutdatedLog(t *testing.T) {
	args := &RequestVoteArgs{
		Term: 3,
		CandidateId: 0,
		LastLogIndex: -1,
		LastLogTerm: -1,
	}
	expected := &RequestVoteReply{
		Term: 3,
		VoteGranted: false,
	}
	reply := &RequestVoteReply{}
	
	rf := &Raft{
		log: []Entry{
			Entry{
				term: 1,
			},
			Entry{
				term: 2,
			},
		},
		electionChan: make(chan int),
	}
	rf.setStateToFollower(args.Term - 1)
	rf.RequestVote(args, reply)
	if !reflect.DeepEqual(*expected, *reply) {
		t.Errorf("TestRequestVoteHigherTermCandidateOutdatedLog expected %#v\ngot %#v", expected, reply)
	}
}






