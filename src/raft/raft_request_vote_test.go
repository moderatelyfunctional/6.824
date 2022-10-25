package raft

import "time"
import "reflect"
import "testing"

// if VoteGranted is false, then the Term in RequestVoteReply is the term that 
// the caller (raft instance) should update its term to.
func TestRequestVoteFromCandidateWithLowerTerm(t *testing.T) {
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

func TestRequestVoteFromCandidateWithHigherTerm(t *testing.T) {
	expected := &RequestVoteReply{
		Term: 3,
		VoteGranted: true,
	}
	args := &RequestVoteArgs{
		Term: 3,
		CandidateId: 0,
	}
	reply := &RequestVoteReply{}
	
	rf := &Raft{
		electionChan: make(chan int),
	}
	rf.setStateToFollower(args.Term - 1)
	rf.RequestVote(args, reply)
	if !reflect.DeepEqual(*expected, *reply) {
		t.Errorf("TestRequestVoteFromCandidateWithHigherTerm expected %#v\ngot %#v", expected, reply)
	}

	timeoutCalledTwice := false
	go func() {
		timeoutInMs := ELECTION_TIMEOUT_MIN_MS + 2 * ELECTION_TIMEOUT_SPREAD_MS
		time.Sleep(time.Duration(timeoutInMs * 2) * time.Millisecond)
		if !timeoutCalledTwice {
			t.Errorf("TestRequestVoteFromCandidateWithHigherTerm expected 2 election timeouts within %d", timeoutInMs)
		}
	}()
	<-rf.electionChan
	<-rf.electionChan

	timeoutCalledTwice = true
}

// raft instance with me = 3 and votedFor = [-1, 2, 4] won't grant vote to another candidate on the same term
// if it's already voted for a candidate.
func TestRequestVoteFromCandidateInSameTerm(t *testing.T) {
	inputs := [][]any{
		{t, 3, -1, &RequestVoteArgs{CandidateId: 2, Term: 7}, &RequestVoteReply{Term: 7, VoteGranted: true,}},
		{t, 3, 2, &RequestVoteArgs{CandidateId: 2, Term: 7}, &RequestVoteReply{Term: 7, VoteGranted: true,}},
		{t, 3, 4, &RequestVoteArgs{CandidateId: 2, Term: 7}, &RequestVoteReply{Term: 7, VoteGranted: false,}},
	}

	test := func(t *testing.T, me int, votedFor int, args *RequestVoteArgs, expected *RequestVoteReply) {
		rf := &Raft{
			me: me,
			votedFor: votedFor,
			currentTerm: expected.Term,
		}
		reply := &RequestVoteReply{}
		rf.RequestVote(args, reply)
		if !reflect.DeepEqual(*expected, *reply) {
			t.Errorf("TestRequestVoteFromCandidateInSameTerm expected %#v\ngot %#v", expected, reply)
			t.Errorf("Input args rf.me %v rf.votedFor %v", me, votedFor)
		}
	}

	parametrize(test, inputs)
}











