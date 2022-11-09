package raft

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

	rf := &Raft{
		currentTerm: args.Term + 1,
	}
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
				Term: 2,
			},
			Entry{
				Term: 5,
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
		t.Errorf("TestRequestVoteSameTermCandidateDidVote expected %#v\ngot %#v", expected, reply)
	}
}

// raft instance with me = 3 and votedFor = -1 grants vote to another candidate = 2 on the same term
// if it's never voted for a candidate and the candidate's log is more up-to-date.
func TestRequestVoteSameTermCandidateDidntVote(t *testing.T) {
	rf := &Raft{
		me: 3,
		votedFor: -1,
		currentTerm: 7,
		log: []Entry{
			Entry{
				Term: 2,
			},
			Entry{
				Term: 5,
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
		t.Errorf("TestRequestVoteSameTermCandidateDidntVote expected %#v\ngot %#v", expected, reply)
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
		currentTerm: args.Term - 1,
		votedFor: -1,
		state: CANDIDATE,
	}
	rf.RequestVote(args, reply)
	if !reflect.DeepEqual(*expected, *reply) {
		t.Errorf("TestRequestVoteHigherTermCandidateSameUpToDateLog expected %#v\ngot %#v", expected, reply)
	}
	if rf.state != FOLLOWER {
		t.Errorf("TestRequestVoteHigherTermCandidateSameUpToDateLog expected state %#v\ngot %#v", FOLLOWER, rf.state)	
	}
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
		currentTerm: args.Term - 1,
		log: []Entry{
			Entry{
				Term: 1,
			},
			Entry{
				Term: 2,
			},
		},
	}
	rf.RequestVote(args, reply)
	if !reflect.DeepEqual(*expected, *reply) {
		t.Errorf("TestRequestVoteHigherTermCandidateOutdatedLog expected %#v\ngot %#v", expected, reply)
	}
	if rf.state != FOLLOWER {
		t.Errorf("TestRequestVoteHigherTermCandidateOutdatedLog expected state %#v\ngot %#v", FOLLOWER, rf.state)	
	}
}






