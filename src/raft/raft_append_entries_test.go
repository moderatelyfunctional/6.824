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

// Case 1/Case 2
// F: 1 2 2 2
// L: 1 3 3 3
func TestAppendEntriesToFollowerForConflictingEntry(t *testing.T) {
	expected := &AppendEntriesReply{
		Term: 3,
		Success: false,
		XTerm: 2,
		XIndex: 1,
		XLen: 4,
	}

	args := &AppendEntriesArgs{
		Term: expected.Term,
		LeaderId: 0,
		PrevLogIndex: 3,
		PrevLogTerm: 3,
		Entries: []Entry{},
	}
	reply := &AppendEntriesReply{}
	rf := Raft{
		currentTerm: expected.Term,
		log: []Entry{
			Entry{Term: 1,},
			Entry{Term: 2,},
			Entry{Term: 2,},
			Entry{Term: 2,},
		},
		persister: &Persister{},
	}
	rf.AppendEntries(args, reply)
	if !reflect.DeepEqual(*expected, *reply) {
		t.Errorf("TestAppendEntriesToFollowerFromLeaderWithoutTerm expected %#v\ngot %#v", expected, reply)
	}
}

// Case 3
// F:
// L: 1 1 6
func TestAppendEntriesToFollowerForMissingEntry(t *testing.T) {
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
		t.Errorf("TestAppendEntriesToFollowerWithMissingEntry expected %#v\ngot %#v", expected, reply)
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



