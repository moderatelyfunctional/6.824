package raft

import "6.824/labgob"

import "fmt"
import "time"
import "bytes"
import "reflect"
import "testing"

// Test case for:
// 1) snapshotInterval * 2 > commitIndex > snapshotInterval doesnt call Snapshot again
// 2) Leader tells follower to install snapshot, which does as it's expected

func TestRaftSnapshotSavesStateAndSnapshot(t *testing.T) {
	servers := 3
	nCommitedEntriesPerSnapshot := 9 // config.go L250
	cfg := make_config(t, servers, false, true, true)
	
	rf := cfg.rafts[0]
	snapCommands := []interface{}{nil}
	for i := 0; i < nCommitedEntriesPerSnapshot; i++ {
		command := fmt.Sprintf("Command-%d", i)
		snapCommands = append(snapCommands, command)
		rf.log = append(rf.log, Entry{Term: 1, Command: command,})
	}
	rf.currentTerm = 1
	rf.commitIndex = nCommitedEntriesPerSnapshot - 1 // 0-indexed	
	rf.sendApplyMsg()

	time.Sleep(1 * time.Second)
	
	state := rf.persister.ReadRaftState()
	snapshot := rf.persister.ReadSnapshot()

	rState := bytes.NewBuffer(state)
	dState := labgob.NewDecoder(rState)
	var currentTerm int
	var votesReceived []int
	var votedFor int
	var logState []Entry
	if dState.Decode(&currentTerm) != nil ||
		dState.Decode(&votesReceived) != nil ||
		dState.Decode(&votedFor) != nil ||
		dState.Decode(&logState) != nil {
		t.Errorf("TestRaftSnapshotSavesStateAndSnapshot: encountered problem decoding raft state")
	} else {
		if currentTerm != rf.currentTerm {
			t.Errorf(
				"TestRaftSnapshotSavesStateAndSnapshot: currentTerm expected %v, got %v",
				rf.currentTerm, currentTerm)
		}
		if len(logState) > 0 {
			t.Errorf("TestRaftSnapshotSavesStateAndSnapshot: expected empty logState, got %v", logState)
		}
	}

	rSnap := bytes.NewBuffer(snapshot)
	dSnap := labgob.NewDecoder(rSnap)
	var commandIndex int
	var logSnapshot []interface{}
	if dSnap.Decode(&commandIndex) != nil ||
		dSnap.Decode(&logSnapshot) != nil {
		t.Errorf("TestRaftSnapshotSavesStateAndSnapshot: Encountered problem decoding raft snapshot")
	} else {
		if commandIndex != nCommitedEntriesPerSnapshot {
			t.Errorf(
				"TestRaftSnapshotSavesStateAndSnapshot: CommandIndex expected %v, got %v",
				commandIndex, nCommitedEntriesPerSnapshot)
		}
		if !reflect.DeepEqual(snapCommands, logSnapshot) {
			t.Errorf(
				"TestRaftSnapshotSavesStateAndSnapshot: Log snapshot expected %v, got %v",
				snapCommands, logSnapshot)
		}
	}

}















