package raft

import "6.824/labgob"

import "fmt"
import "time"
import "bytes"
import "reflect"
import "testing"

import "math/rand"

var configSnapshotInterval int = 9 // config.go L250

// A valid snapshot is defined in config.go in the ingestSnap method. It must have the index of the last 
// included entry and an array of interface objects representing the entries. Here we generate a randomized
// one since the actual data doesn't matter.
func createSnapshot() []byte {
	commandIndex := 1 + rand.Intn(10)
	command := 1 + rand.Intn(50)

	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(commandIndex)
	var xlog []interface{}
	for j := 0; j <= commandIndex; j++ {
		xlog = append(xlog, command)
	}
	e.Encode(xlog)
	return w.Bytes()
}

// The count is often set to commitIndex + 1 since commitIndex is 0-indexed and a commitIndex of 1 indicates
// that there are two entries in the log, not one.
func createEntries(term int, count int) []Entry {
	entries := make([]Entry, 0)
	for i := 0; i < count; i++ {
		command := fmt.Sprintf("Command-%d", i)
		entries = append(entries, Entry{Term: term, Command: command,})
	}
	return entries
}

func checkRaftState(
	state []byte,
	currentTerm int,
	logSize int,
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
}

func checkRaftSnapshot(snapshot []byte, snapshotIndex int, snapCommands []interface{}, t *testing.T) {
	bSnap := bytes.NewBuffer(snapshot)
	dSnap := labgob.NewDecoder(bSnap)
	var commandIndex int
	var commands []interface{}
	if dSnap.Decode(&commandIndex) != nil ||
		dSnap.Decode(&commands) != nil {
		t.Errorf("TestRaftSnapshotSavesStateAndSnapshot: Encountered problem decoding raft snapshot")
	} else {
		if commandIndex != snapshotIndex {
			t.Errorf(
				"TestRaftSnapshotSavesStateAndSnapshot: commandIndex expected %v, got %v",
				commandIndex, snapshotIndex)
		}
		if !reflect.DeepEqual(snapCommands, commands) {
			t.Errorf(
				"TestRaftSnapshotSavesStateAndSnapshot: commands expected %v, got %v",
				snapCommands, commands)
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
func TestSnapshotMsgCommitIndexLessThanSnapshotInterval(t *testing.T) {
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
		t.Errorf("TestSnapshotMsgCommitIndexLessThanSnapshotInterval expected state size 0 but got %d", len(state))
	}
	if len(snapshot) > 0 {
		t.Errorf("TestSnapshotMsgCommitIndexLessThanSnapshotInterval expected snapshot size 0 but got %d", len(snapshot))
	}
}

// Snapshotting is expected here since commitIndex = snapshotInterval.
func TestSnapshotMsgCommitIndexEqualsSnapshotInterval(t *testing.T) {
	servers := 3
	cfg := make_config(t, servers, false, true, true)
	
	rf := cfg.rafts[0]
	rf.currentTerm = 1
	rf.commitIndex = configSnapshotInterval - 1 // 0-indexed
	snapCommands := []interface{}{nil}
	rf.log = makeLog(createEntries(/* term= */ 1, /* count= */ rf.commitIndex + 1))
	for i := 0; i < rf.log.size(); i++ {
		snapCommands = append(snapCommands, rf.log.entry(i).Command)
	}

	rf.sendApplyMsg()

	time.Sleep(1 * time.Second)
	
	checkRaftState(rf.persister.ReadRaftState(), rf.currentTerm, /* logSize= */ 0, t)
	checkRaftSnapshot(rf.persister.ReadSnapshot(), configSnapshotInterval, snapCommands, t)
}

func TestSnapshotMsgCommitIndexGreaterThanSnapshotInterval(t *testing.T) {
	servers := 3
	cfg := make_config(t, servers, false, true, true)
	
	rf := cfg.rafts[0]
	rf.currentTerm = 1
	rf.commitIndex = int(float64(configSnapshotInterval) * 1.5) // 0-indexed	
	snapCommands := []interface{}{nil}
	rf.log = makeLog(createEntries(/* term= */ 1, /* count= */ rf.commitIndex + 1))
	for i := 0; i <= rf.log.size(); i++ {
		if i < configSnapshotInterval {
			snapCommands = append(snapCommands, rf.log.entry(i).Command)
		}
	}
	rf.sendApplyMsg()

	time.Sleep(1 * time.Second)
	
	checkRaftState(
		rf.persister.ReadRaftState(),
		rf.currentTerm,
		/* logSize= */ rf.commitIndex - configSnapshotInterval + 1,
		t)
	checkRaftSnapshot(rf.persister.ReadSnapshot(), configSnapshotInterval, snapCommands, t)
}

// Case 2 - The leader's InstallSnapshotRPC contains a log (startIndex = 12, snapshotIndex = 11, entries = 3)
// that exceeds the entirety of the follower's log (startIndex = 0, snapshotIndex = -1, entries = 5). Here the
// follower should install the snapshot, set its startIndex and snapshotIndex to the same as the leader. It
// should also clear its entries.
func TestSnapshotRpcSlowFollower(t *testing.T) {
	servers := 3
	cfg := make_config(t, servers, false, true, true)

	leader := cfg.rafts[0]
	follower := cfg.rafts[1]

	leader.me = 0
	leader.currentTerm = 3
	leader.commitIndex = 15
	leader.state = LEADER
	leader.log = makeLogFromSnapshot(
		/* startIndex= */ 12,
		/* snapshotTerm= */ 2,
		/* snapshotIndex= */ 11,
		/* entries= */ createEntries(/* term= */ 3, /* count= */ 3))
	snapshot := createSnapshot()
	state := leader.encodeState()
	leader.persister.SaveStateAndSnapshot(state, snapshot)

	follower.me = 1
	follower.currentTerm = 3
	follower.commitIndex = 5
	follower.state = FOLLOWER
	follower.log = makeLog(createEntries(/* term= */ 1, /* count= */ 5))
	
	leader.sendInstallSnapshotTo(/* index= */ follower.me, /* currentTerm= */ leader.currentTerm)

	// Provide enough time for the service layer to call CondInstallSnapshot
	time.Sleep(1)

	if !leader.log.isEqual(follower.log, /* checkEntries= */ false) {
		t.Errorf("TestSnapshotRpcSlowFollower expected log %#v, got log %#v", leader.log, follower.log)
	}
	if leader.log.startIndex != follower.log.size() {
		t.Errorf(
			"TestSnapshotRpcSlowFollower expected follower log size %v, got %v", 
			leader.log.startIndex, follower.log.size())
	}
	if !reflect.DeepEqual(snapshot, follower.persister.ReadSnapshot()) {
		t.Errorf("TestSnapshotRpcSlowFollower expected leader, follower snapshots to be equal")
	}
}












