package raft

import "6.824/labgob"

import "fmt"
import "time"
import "bytes"
import "reflect"
import "testing"

func TestRaftSnapshotSavesStateAndSnapshot(t *testing.T) {
	servers := 3
	nCommitedEntriesPerSnapshot := 9 // config.go L250
	cfg := make_config(t, servers, false, true, true)
	
	rf := cfg.rafts[0]
	for i := 0; i < nCommitedEntriesPerSnapshot; i++ {
		rf.log = append(rf.log, Entry{Term: 1,})
	}
	rf.currentTerm = 1
	rf.commitIndex = nCommitedEntriesPerSnapshot - 1 // 0-indexed

	originalLog := make([]Entry, nCommitedEntriesPerSnapshot)
	copy(originalLog, rf.log)
	
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
		t.Errorf("TestRaftSnapshotSavesStateAndSnapshot: Encountered problem decoding raft state")
	} else {
		fmt.Println("LOG STATE IS ", logState)
		if currentTerm != rf.currentTerm {
			t.Errorf(
				"TestRaftSnapshotSavesStateAndSnapshot: currentTerm expected %v, got %v",
				rf.currentTerm, currentTerm)
		}
		if !reflect.DeepEqual([]Entry{}, logState) {
			t.Errorf(
				"TestRaftSnapshotSavesStateAndSnapshot: Log state expected %v, got %v",
				[]Entry{}, logState)
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
		if !reflect.DeepEqual(originalLog, logSnapshot) {
			t.Errorf(
				"TestRaftSnapshotSavesStateAndSnapshot: Log snapshot expected %v, got %v",
				originalLog, logSnapshot)
		}
	}

}