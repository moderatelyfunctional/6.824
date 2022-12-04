package raft

import "reflect"
import "testing"

func TestAppendEntriesFromLowerTermLeader(t *testing.T) {
	expected := &AppendEntriesReply{
		Term: 3,
		Success: false,
	}

	args := &AppendEntriesArgs{
		Term: expected.Term - 1,
		LeaderId: 3,
		PrevLogIndex: 2,
		PrevLogTerm: expected.Term - 1,
		Entries: []Entry{},
		LeaderCommit: 2,
	}
	reply := &AppendEntriesReply{}
	rf := Raft{
		currentTerm: expected.Term,
	}
	rf.AppendEntries(args, reply)
	if !reflect.DeepEqual(*expected, *reply) {
		t.Errorf("TestAppendEntriesFromLowerTermLeader expected %#v\ngot %#v", expected, reply)
	}
}

// TODO Case 1
// TODO Case 2

// Case 3
func TestAppendEntriesToFollowerWithMissingEntries(t *testing.T) {
	expected := &AppendEntriesReply{
		Term: 7,
		Success: false,
		XTerm: -1,
		XIndex: -1,
		XLen: 0,
	}

	args := &AppendEntriesArgs{
		Term: expected.Term,
		LeaderId: 3,
		PrevLogIndex: 2,
		PrevLogTerm: expected.Term - 1,
		Entries: []Entry{},
		LeaderCommit: 2,
	}
	reply := &AppendEntriesReply{}
	rf := Raft{
		currentTerm: expected.Term,
		log: []Entry{},
		persister: &Persister{},
	}
	rf.AppendEntries(args, reply)
	if !reflect.DeepEqual(*expected, *reply) {
		t.Errorf("TestAppendEntriesToFollowerWithMissingEntries expected %#v\ngot %#v", expected, reply)
	}
}

func TestAppendEntriesToFollowerWithUncommittedEntries(t *testing.T) {
	expected := &AppendEntriesReply{
		Term: 3,
		Success: true,
		XTerm: 0,
		XIndex: 0,
		XLen: 0,
	}

	args := &AppendEntriesArgs{
		Term: expected.Term,
		LeaderId: 3,
		PrevLogIndex: -1,
		PrevLogTerm: -1,
		Entries: []Entry{
			Entry{Term: 1,},
		},
		LeaderCommit: 1,
	}
	reply := &AppendEntriesReply{}
	rf := Raft{
		currentTerm: expected.Term,
		log: []Entry{
			Entry{Term: 1,},
			Entry{Term: 1,},
		},
		persister: &Persister{},
	}
	rf.AppendEntries(args, reply)
	if !reflect.DeepEqual(*expected, *reply) {
		t.Errorf("TestAppendEntriesToFollowerWithUncommittedEntries expected %#v\ngot %#v", expected, reply)
	}
	if len(rf.log) != 1 {
		t.Errorf("TestAppendEntriesToFollowerWithUncommittedEntries expected log size 1, but got size %d", len(rf.log))	
	}
}

func TestAppendEntriesToUpToDateCandidate(t *testing.T) {
	expected := &AppendEntriesReply{
		Term: 3,
		Success: true,
	}

	args := &AppendEntriesArgs{
		Term: expected.Term,
		LeaderId: 3,
		PrevLogIndex: 1,
		PrevLogTerm: 2,
	}
	reply := &AppendEntriesReply{}
	rf := Raft{
		currentTerm: expected.Term,
		state: CANDIDATE,
		heartbeat: false,
		log: []Entry{
			Entry{Term: 1,},
			Entry{Term: 2,},
		},
		persister: &Persister{},
	}
	rf.AppendEntries(args, reply)
	if !reflect.DeepEqual(*expected, *reply) {
		t.Errorf("TestAppendEntriesFromLegitimateLeader expected %#v\ngot %#v", expected, reply)
	}
	if !rf.heartbeat || rf.state != FOLLOWER || rf.currentTerm != expected.Term {
		t.Errorf(
			"TestAppendEntriesFromLegitimateLeader expected heartbeat true state FOLLOWER currentTerm %d but got %#v",
			expected.Term, rf)
	}
}

func TestAppendEntriesIncrementCommitIndex(t *testing.T) {
	expected := &AppendEntriesReply{
		Term: 3,
		Success: true,
	}

	args := &AppendEntriesArgs{
		Term: expected.Term,
		LeaderId: 3,
		PrevLogIndex: 2,
		PrevLogTerm: 2,
		LeaderCommit: 2,
	}
	reply := &AppendEntriesReply{}
	rf := Raft{
		currentTerm: expected.Term,
		state: FOLLOWER,
		log: []Entry{
			Entry{Term: 1,},
			Entry{Term: 2,},
			Entry{Term: 2,},
		},
		commitIndex: 1,
		persister: &Persister{},
	}
	rf.AppendEntries(args, reply)
	if !reflect.DeepEqual(*expected, *reply) {
		t.Errorf("TestAppendEntriesIncrementCommitIndex expected %#v\ngot %#v", expected, reply)
	}
	if (rf.commitIndex != 2) {
		t.Errorf("TestAppendEntriesIncrementCommitIndex commitIndex expected %d got %d", 2, rf.commitIndex)
	}
}



