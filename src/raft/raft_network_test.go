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
	}
	reply := &AppendEntriesReply{}
	rf.AppendEntries(firstArgs, reply)

	if !reflect.DeepEqual(rf.log, firstArgs.Entries) {
		t.Errorf("TestNetworkOutOfOrderRequests expected logs to be equal %v %v", rf.log, firstArgs.Entries)
	}
}
