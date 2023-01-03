package raft

import "6.824/labgob"

import "fmt"
import "time"
import "bytes"
import "reflect"
import "testing"

var nCommitedEntriesPerSnapshot int = 9 // config.go L250

func checkRaftStateAndSnapshot(
	state []byte,
	snapshot []byte,
	currentTerm int,
	logSize int,
	snapshotIndex int,
	snapCommands []interface{},
	t *testing.T) {
	rState := bytes.NewBuffer(state)
	dState := labgob.NewDecoder(rState)
	var stateTerm int
	var votesReceived []int
	var votedFor int
	var logState []Entry
	if dState.Decode(&stateTerm) != nil ||
		dState.Decode(&votesReceived) != nil ||
		dState.Decode(&votedFor) != nil ||
		dState.Decode(&logState) != nil {
		t.Errorf("checkRaftStateAndSnapshot: encountered problem decoding raft state")
	} else {
		if stateTerm != currentTerm {
			t.Errorf(
				"checkRaftStateAndSnapshot: currentTerm expected %v, got %v",
				currentTerm, stateTerm)
		}
		if len(logState) != logSize {
			t.Errorf("checkRaftStateAndSnapshot: logState expected size %d, got %v", logSize, logState)
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
		if commandIndex != snapshotIndex {
			t.Errorf(
				"TestRaftSnapshotSavesStateAndSnapshot: CommandIndex expected %v, got %v",
				commandIndex, snapshotIndex)
		}
		if !reflect.DeepEqual(snapCommands, logSnapshot) {
			t.Errorf(
				"TestRaftSnapshotSavesStateAndSnapshot: Log snapshot expected %v, got %v",
				snapCommands, logSnapshot)
		}
	}
}

// Test case for:
// 1) Leader tells follower to install snapshot, which does as it's expected

func TestRaftSnapshotCommitIndexLessThanSnapshotInterval(t *testing.T) {
	servers := 3
	cfg := make_config(t, servers, false, true, true)
	
	rf := cfg.rafts[0]
	rf.currentTerm = 1
	rf.commitIndex = nCommitedEntriesPerSnapshot / 2 // 0-indexed	
	snapCommands := []interface{}{}
	for i := 0; i <= rf.commitIndex; i++ {
		command := fmt.Sprintf("Command-%d", i)
		snapCommands = append(snapCommands, command)
		rf.log.appendEntry(Entry{Term: 1, Command: command,})
	}
	rf.sendApplyMsg()

	time.Sleep(1 * time.Second)
	
	state := rf.persister.ReadRaftState()
	snapshot := rf.persister.ReadSnapshot()

	if len(state) > 0 {
		t.Errorf("TestRaftSnapshotCommitIndexLessThanSnapshotInterval expected state size 0 but got %d", len(state))
	}
	if len(snapshot) > 0 {
		t.Errorf("TestRaftSnapshotCommitIndexLessThanSnapshotInterval expected snapshot size 0 but got %d", len(snapshot))
	}
}

func TestRaftSnapshotCommitIndexEqualsSnapshotInterval(t *testing.T) {
	servers := 3
	cfg := make_config(t, servers, false, true, true)
	
	rf := cfg.rafts[0]
	rf.currentTerm = 1
	rf.commitIndex = nCommitedEntriesPerSnapshot - 1 // 0-indexed	
	snapCommands := []interface{}{nil}
	for i := 0; i <= rf.commitIndex; i++ {
		command := fmt.Sprintf("Command-%d", i)
		snapCommands = append(snapCommands, command)
		rf.log.appendEntry(Entry{Term: 1, Command: command,})
	}
	rf.sendApplyMsg()

	time.Sleep(1 * time.Second)
	
	checkRaftStateAndSnapshot(
		rf.persister.ReadRaftState(),
		rf.persister.ReadSnapshot(),
		rf.currentTerm,
		/* logSize= */ 0,
		/* snapshotIndex= */ nCommitedEntriesPerSnapshot,
		snapCommands,
		t)
}

func TestRaftSnapshotCommitIndexGreaterThanSnapshotInterval(t *testing.T) {
	servers := 3
	cfg := make_config(t, servers, false, true, true)
	
	rf := cfg.rafts[0]
	rf.currentTerm = 1
	rf.commitIndex = int(float64(nCommitedEntriesPerSnapshot) * 1.5) // 0-indexed	
	snapCommands := []interface{}{nil}
	for i := 0; i <= rf.commitIndex; i++ {
		command := fmt.Sprintf("Command-%d", i)
		if i < nCommitedEntriesPerSnapshot {
			snapCommands = append(snapCommands, command)
		}
		rf.log.appendEntry(Entry{Term: 1, Command: command,})
	}
	rf.sendApplyMsg()

	time.Sleep(1 * time.Second)
	
	checkRaftStateAndSnapshot(
		rf.persister.ReadRaftState(),
		rf.persister.ReadSnapshot(),
		rf.currentTerm,
		/* logSize= */ rf.commitIndex - nCommitedEntriesPerSnapshot + 1,
		/* snapshotIndex= */ nCommitedEntriesPerSnapshot,
		snapCommands,
		t)
}














