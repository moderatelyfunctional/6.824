package raft

import "reflect"
import "testing"

func TestNetworkOutOfOrderRequests(t *testing.T) {
	firstArgs := &AppendEntriesArgs{
		Term: 1,
		LeaderId: 2,
		PrevLogIndex: -1,
		PrevLogTerm: -1,
		Entries: []Entry{
			Entry{Term: 1, Command: 5282025426492834869},
			Entry{Term: 1, Command: 1871948457912531367},
			Entry{Term: 1, Command: 507673380456088345},
		},
		LeaderCommit: -1,
	}
	secondArgs := &AppendEntriesArgs{
		Term: 1,
		LeaderId: 2,
		PrevLogIndex: -1,
		PrevLogTerm: -1,
		Entries: []Entry{
			Entry{Term: 1, Command: 5282025426492834869},
		},
		LeaderCommit: -1,
	}
	rf := Raft{
		currentTerm: 1,
		state: FOLLOWER,
		persister: MakePersister(),
	}
	reply := &AppendEntriesReply{}
	rf.AppendEntries(firstArgs, reply)

	if !reflect.DeepEqual(rf.log, firstArgs.Entries) {
		t.Errorf("TestNetworkOutOfOrderRequests expected first logs to be %v\ngot %v", firstArgs.Entries, rf.log)
	}

	rf.AppendEntries(secondArgs, reply)
	if !reflect.DeepEqual(rf.log, firstArgs.Entries) {
		t.Errorf("TestNetworkOutOfOrderRequests expected second logs to be %v\ngot %v", firstArgs.Entries, rf.log)
	}
}

func TestNetworkDuplicateEntry(t *testing.T) {
	args := &AppendEntriesArgs{
		Term: 1,
		LeaderId: 2,
		PrevLogIndex: 11,
		PrevLogTerm: 1,
		Entries: []Entry{
			Entry{Term: 1, Command: 6393}, Entry{Term: 1, Command: 8548},
		},
		LeaderCommit: 14,
	}
	originalLog := []Entry{
		Entry{Term: 1, Command: 6377}, Entry{Term: 1, Command: 5575}, Entry{Term: 1, Command: 1719}, 
		Entry{Term: 1, Command: 6670}, Entry{Term: 1, Command: 7179}, Entry{Term: 1, Command: 1142}, 
		Entry{Term: 1, Command: 5929}, Entry{Term: 1, Command: 6942}, Entry{Term: 1, Command: 5426},
		Entry{Term: 1, Command: 5330}, Entry{Term: 1, Command: 4592}, Entry{Term: 1, Command: 1283},
		Entry{Term: 1, Command: 6393}, Entry{Term: 1, Command: 8548}, Entry{Term: 1, Command: 8552}, 
	}
	rf := Raft{
		currentTerm: 1,
		state: FOLLOWER,
		log: originalLog,
		commitIndex: 8,
		lastApplied: 8,
		persister: MakePersister(),
	}
	reply := &AppendEntriesReply{}
	rf.AppendEntries(args, reply)

	if !reflect.DeepEqual(rf.log, originalLog) {
		t.Errorf("TestNetworkOutOfOrderRequests expected first logs to be %v\ngot %v", originalLog, rf.log)
	}
}


