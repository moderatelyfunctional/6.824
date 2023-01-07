package raft

import "6.824/labgob"

import "fmt"
import "time"
import "bytes"
import "reflect"
import "testing"

var configSnapshotInterval int = 9 // config.go L250

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
	var logStartIndex int
	var logSnapshotTerm int
	var logSnapshotIndex int
	var logEntries []Entry
	if dState.Decode(&stateTerm) != nil ||
		dState.Decode(&votesReceived) != nil ||
		dState.Decode(&votedFor) != nil ||
		dState.Decode(&logStartIndex) != nil ||
		dState.Decode(&logSnapshotTerm) != nil ||
		dState.Decode(&logSnapshotIndex) != nil ||
		dState.Decode(&logEntries) != nil {
		t.Errorf("checkRaftStateAndSnapshot: encountered problem decoding raft state")
	} else {
		if stateTerm != currentTerm {
			t.Errorf(
				"checkRaftStateAndSnapshot: currentTerm expected %v, got %v",
				currentTerm, stateTerm)
		}
		if len(logEntries) != logSize {
			fmt.Println("logStartIndex, logEntries, logSize", logStartIndex, logEntries, logSize)
			t.Errorf("checkRaftStateAndSnapshot: logEntries expected size %d, got %v", logSize, logEntries)
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

// There is a back-and-forth between Raft and the service to configure the snapshot operation. 
// On a high level, every time an ApplyMsg is sent, config.go (service layer) will check the commandIndex
// and if every time commandIndex % snapshotInterval = 0, the service initiates a snapshot operation to raft.
// This occurs during normal operation as raft instances commit entries, and realize they can snapshot their
// logs because they've committed entries.
//
// It's also possible that the leader _directly_ sends a InstallSnapshotRPC to the follower. This occurs when
// the follower has fallen too far behind the leader's log, and needs to install the leader's snapshot.

// No snapshotting is expected here since the commitIndex < snapshotInterval
func TestSnapshotCommitIndexLessThanSnapshotInterval(t *testing.T) {
	servers := 3
	cfg := make_config(t, servers, false, true, true)
	
	rf := cfg.rafts[0]
	rf.currentTerm = 1
	rf.commitIndex = configSnapshotInterval / 2 // 0-indexed	
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

// Snapshotting is expected here since commitIndex = snapshotInterval.
func TestSnapshotCommitIndexEqualsSnapshotInterval(t *testing.T) {
	servers := 3
	cfg := make_config(t, servers, false, true, true)
	
	rf := cfg.rafts[0]
	rf.currentTerm = 1
	rf.commitIndex = configSnapshotInterval - 1 // 0-indexed
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
		/* snapshotIndex= */ configSnapshotInterval,
		snapCommands,
		t)
}

func TestSnapshotCommitIndexGreaterThanSnapshotInterval(t *testing.T) {
	servers := 3
	cfg := make_config(t, servers, false, true, true)
	
	rf := cfg.rafts[0]
	rf.currentTerm = 1
	rf.commitIndex = int(float64(configSnapshotInterval) * 1.5) // 0-indexed	
	snapCommands := []interface{}{nil}
	for i := 0; i <= rf.commitIndex; i++ {
		command := fmt.Sprintf("Command-%d", i)
		if i < configSnapshotInterval {
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
		/* logSize= */ rf.commitIndex - configSnapshotInterval + 1,
		/* snapshotIndex= */ configSnapshotInterval,
		snapCommands,
		t)
}














