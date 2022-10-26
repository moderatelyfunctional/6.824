package raft

import "reflect"
import "testing"

func TestAppendEntriesFromOutdatedLeader(t *testing.T) {
	expected := &AppendEntriesReply{
		Term: 3,
		Success: false,
	}

	args := &AppendEntriesArgs{
		Term: expected.Term - 1,
		LeaderId: 3,
	}
	reply := &AppendEntriesReply{}
	rf := Raft{
		currentTerm: expected.Term,
	}
	rf.AppendEntries(args, reply)
	if !reflect.DeepEqual(*expected, *reply) {
		t.Errorf("TestAppendEntriesFromOutdatedLeader expected %#v\ngot %#v", expected, reply)
	}
}

func TestAppendEntriesFromLegitimateLeader(t *testing.T) {
	expected := &AppendEntriesReply{
		Term: 3,
		Success: true,
	}

	args := &AppendEntriesArgs{
		Term: expected.Term,
		LeaderId: 3,
	}
	reply := &AppendEntriesReply{}
	rf := Raft{
		currentTerm: expected.Term,
		state: FOLLOWER,
		heartbeat: false,
	}
	rf.AppendEntries(args, reply)
	if !reflect.DeepEqual(*expected, *reply) {
		t.Errorf("TestAppendEntriesFromLegitimateLeader expected %#v\ngot %#v", expected, reply)
	}
	if !rf.heartbeat || rf.state != FOLLOWER || rf.currentTerm != expected.Term {
		t.Errorf("Expected heartbeat true state FOLLOWER currentTerm %d but got %#v", expected.Term, rf)
	}
}