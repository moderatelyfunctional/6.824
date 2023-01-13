package raft

import "6.824/labgob"

import "fmt"
import "time"
import "bytes"
import "reflect"
import "testing"

var configSnapshotInterval int = 9 // config.go L250

// A valid snapshot is defined in config.go in the ingestSnap method. It must have the index of the last 
// included entry and an array of interface objects representing the entries.
func createSnapshot(snapshotIndex int) []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(snapshotIndex)
	var xlog []interface{}
	for i := 0; i <= snapshotIndex; i++ {
		xlog = append(xlog, fmt.Sprintf("Command-%d", i))
	}
	e.Encode(xlog)
	return w.Bytes()
}

// The count is often set to commitIndex + 1 since commitIndex is 0-indexed and a commitIndex of 1 indicates
// that there are two entries in the log, not one.
func createEntries(term int, count int) []Entry {
	entries := make([]Entry, 0)
	for i := 0; i < count; i++ {
		entries = append(entries, Entry{Term: term, Command: fmt.Sprintf("Command-%d", i),})
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

// Case 1 (FALSE) - The leader's InstallSnapshotRPC is a stale entry. Its startIndex/log is outdated with respect to 
// the follower's. This can ocurr for two reasons: 1) the network is unreliable and the the RPC was sent from
// the leader quite some time ago. 2) the leader is an outdated leader and should step down. For the purpose
// of this test, the leader's state simulates one that sends an outdated RPC.
//
// The leader's log is (startIndex = 4, snapshotTerm = 2, snapshotIndex = 3) while the follower's log is
// (startIndex = 6, ...) so since 6 > 3, the follower rejects the leader's InstallSnapshotRPC.
// 
// In reality, the leader's state should never have a lower startIndex than the follower since the follower would
// only know to snapshot its log if the leader informs it of that. And there can be only one leader per term,
// so the leader's startIndex must be at least the value of the follower's.
func TestSnapshotRpcStaleRequest(t *testing.T) {
	servers := 3
	cfg := make_config(t, servers, false, true, true)

	leader := cfg.rafts[0]
	follower := cfg.rafts[1]

	leader.me = 0
	leader.currentTerm = 3
	leader.commitIndex = 3
	leader.state = LEADER
	leader.log = makeLogFromSnapshot(
		/* startIndex= */ 4,
		/* snapshotTerm= */ 2,
		/* snapshotIndex= */ 3,
		/* entries= */ []Entry{})
	leaderSnapshot := createSnapshot(/* snapshotIndex= */ 3)
	leaderState := leader.encodeState()
	leader.persister.SaveStateAndSnapshot(leaderState, leaderSnapshot)

	follower.me = 1
	follower.currentTerm = 3
	follower.commitIndex = 5
	follower.state = FOLLOWER
	follower.log = makeLogFromSnapshot(
		/* startIndex= */ 6,
		/* snapshotTerm= */ 2,
		/* snapshotIndex= */ 5,
		/* entries= */ []Entry{})
	expectedFollowerLog := follower.log.copyOf()
	followerSnapshot := createSnapshot(/* snapshotIndex= */ 5)
	followerState := follower.encodeState()
	follower.persister.SaveStateAndSnapshot(followerState, followerSnapshot)

	snapshotTerm, snapshotIndex := leader.log.snapshotEntry()
	leader.sendInstallSnapshotTo(follower.me, leader.currentTerm, snapshotTerm, snapshotIndex, leaderSnapshot)

	// Provide enough time for the service layer to call CondInstallSnapshot
	time.Sleep(1 * time.Second)

	if !expectedFollowerLog.isEqual(follower.log, /* checkEntries= */ true) {
		t.Errorf("TestSnapshotRpcSlowFollower expected log %#v, got log %#v", expectedFollowerLog, follower.log)
	}
	if !reflect.DeepEqual(followerSnapshot, follower.persister.ReadSnapshot()) {
		t.Errorf("TestSnapshotRpcSlowFollower expected follower snapshot to be unchanged")
	}
}

// Case 2 (TRUE) - The leader's InstallSnapshotRPC contains a log (startIndex = 12, snapshotIndex = 11, entries = 3)
// that exceeds the entirety of the slower follower's log (startIndex = 0, snapshotIndex = -1, entries = 5). Here the
// follower should install the snapshot, set its startIndex and snapshotIndex to the same as the leader. It
// should also clear its entries.
func TestSnapshotRpcCompleteRequest(t *testing.T) {
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
	snapshot := createSnapshot(/* snapshotIndex= */ 11)
	state := leader.encodeState()
	leader.persister.SaveStateAndSnapshot(state, snapshot)

	follower.me = 1
	follower.currentTerm = 3
	follower.commitIndex = 5
	follower.state = FOLLOWER
	follower.log = makeLog(createEntries(/* term= */ 1, /* count= */ 5))
	
	snapshotTerm, snapshotIndex := leader.log.snapshotEntry()
	leader.sendInstallSnapshotTo(follower.me, leader.currentTerm, snapshotTerm, snapshotIndex, snapshot)

	// Provide enough time for the service layer to call CondInstallSnapshot
	time.Sleep(1 * time.Second)

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
	if leader.nextIndex[follower.me] != leader.log.snapshotIndex + 1 {
		t.Errorf(
			"TestSnapshotRpcSlowFollower expected follower nextIndex %d, got %d", 
			leader.log.snapshotIndex + 1, leader.nextIndex[follower.me])
	}
	if leader.matchIndex[follower.me] != leader.log.snapshotIndex {
		t.Errorf(
			"TestSnapshotRpcSlowFollower expected follower matchIndex %d, got %d", 
			leader.log.snapshotIndex, leader.matchIndex[follower.me])
	}
}

// Case 3 (FALSE) - Due to an unreliable network creating a redundant request or a stale request, it's possible that
// the follower already contains the entry that the leader is providing a snapshot for. In that scenario, the follower
// should reject the leader's InstallSnapshotRPC.
//
// The leader's log is (startIndex = 4, snapshotTerm = 2, snapshotIndex = 3). The follower's log is (startIndex = 2,
// snapshotTerm = - 1, snapshotIndex = -1, 3 entries). So technically, the follower contains the leader's 
// InstallSnapshotRPC request since the snapshotIndex = 3, and the follower entries are from index 2 to 5.
//
// Technically the follower could honor the request since excessive snapshotting only affects performance, not 
// correctness. However because the leader knows the follower's log contains the entry, it will increment the 
// follower's commitIndex later. At that point in time, the follower will create its snapshot.
func TestSnapshotRpcRedundantRequest(t *testing.T) {
	servers := 3
	cfg := make_config(t, servers, false, true, true)

	leader := cfg.rafts[0]
	follower := cfg.rafts[1]

	leader.me = 0
	leader.currentTerm = 3
	leader.commitIndex = 4
	leader.state = LEADER
	leader.log = makeLogFromSnapshot(
		/* startIndex= */ 4,
		/* snapshotTerm= */ 2,
		/* snapshotIndex= */ 3,
		/* entries= */ []Entry{})
	leaderSnapshot := createSnapshot(/* snapshotIndex= */ 3)
	leaderState := leader.encodeState()
	leader.persister.SaveStateAndSnapshot(leaderState, leaderSnapshot)

	follower.me = 1
	follower.currentTerm = 3
	follower.commitIndex = 5
	follower.state = FOLLOWER
	follower.log = makeLogFromSnapshot(
		/* startIndex= */ 2,
		/* snapshotTerm= */ -1,
		/* snapshotIndex= */ -1,
		/* entries= */ createEntries(/* term= */ 2, /* count= */ 3))
	expectedFollowerLog := follower.log.copyOf()
	followerSnapshot := createSnapshot(/* snapshotIndex= */ 5)
	followerState := follower.encodeState()
	follower.persister.SaveStateAndSnapshot(followerState, followerSnapshot)

	snapshotTerm, snapshotIndex := leader.log.snapshotEntry()
	leader.sendInstallSnapshotTo(follower.me, leader.currentTerm, snapshotTerm, snapshotIndex, leaderSnapshot)

	// Provide enough time for the service layer to call CondInstallSnapshot
	time.Sleep(1 * time.Second)

	if !expectedFollowerLog.isEqual(follower.log, /* checkEntries= */ true) {
		t.Errorf("TestSnapshotRpcSlowFollower expected log %#v, got log %#v", expectedFollowerLog, follower.log)
	}
	if !reflect.DeepEqual(followerSnapshot, follower.persister.ReadSnapshot()) {
		t.Errorf("TestSnapshotRpcSlowFollower expected follower snapshot to be unchanged")
	}
}

// Case 4 (TRUE) - The leader successfully sends a InstallSnapshotRPC to the follower. However, the follower may have
// additional entries after the snapshotIndex. They should only be kept if they're from the same term as the leader.
// Otherwise, they will only conflict with new entries that this leader creates.
//
// The leader's log is (startIndex = 4, snapshotTerm = 3, snapshotIndex = 3, 0 entries) with followerOne's log 
// (startIndex = 2, snapshotIndex = -1, snapshotTerm = -1, 4 entries with term 2). Since the leader and follower
// disagree on snapshotTerm = 3, snapshotIndex = 3, the follower must remove its entries inclusive of index 3.
// Successive entries are of term 2, and the leader's term is 3, so they are all removed.
//
// For followerTwo, (startIndex = 2, snapshotIndex = -1, snapshotTerm = -1, 1 entry with term 2, 2 entries with term 3).
// Same as for followerOne, but it can keep its additional entries since they're on term 3.
func TestSnapshotRpcPartialRequest(t *testing.T) {
	servers := 3
	cfg := make_config(t, servers, false, true, true)

	leader := cfg.rafts[0]
	followerOne := cfg.rafts[1]
	followerTwo := cfg.rafts[2]

	leader.me = 0
	leader.currentTerm = 3
	leader.commitIndex = 4
	leader.state = LEADER
	leader.log = makeLogFromSnapshot(
		/* startIndex= */ 4,
		/* snapshotTerm= */ 3,
		/* snapshotIndex= */ 3,
		/* entries= */ []Entry{})
	leaderSnapshot := createSnapshot(/* snapshotIndex= */ 3)
	leaderState := leader.encodeState()
	leader.persister.SaveStateAndSnapshot(leaderState, leaderSnapshot)

	followerOne.me = 1
	followerOne.currentTerm = 3
	followerOne.commitIndex = 5
	followerOne.state = FOLLOWER
	followerOne.log = makeLogFromSnapshot(
		/* startIndex= */ 2,
		/* snapshotTerm= */ -1,
		/* snapshotIndex= */ -1,
		/* entries= */ createEntries(/* term= */ 2, /* count= */ 4))
	followerTwo.me = 2
	followerTwo.currentTerm = 3
	followerTwo.commitIndex = 5
	followerTwo.state = FOLLOWER
	followerTwo.log = makeLogFromSnapshot(
		/* startIndex= */ 2,
		/* snapshotTerm= */ -1,
		/* snapshotIndex= */ -1,
		/* entries= */ createEntries(/* term= */ 2, /* count= */ 2))
	followerTwo.log.appendEntry(Entry{Term: leader.currentTerm, Command: "Test1"})
	followerTwo.log.appendEntry(Entry{Term: leader.currentTerm, Command: "Test2"})
	expectedLogSize := followerTwo.log.size()

	snapshotTerm, snapshotIndex := leader.log.snapshotEntry()
	leader.sendInstallSnapshotTo(followerOne.me, leader.currentTerm, snapshotTerm, snapshotIndex, leaderSnapshot)
	leader.sendInstallSnapshotTo(followerTwo.me, leader.currentTerm, snapshotTerm, snapshotIndex, leaderSnapshot)

	// Provide enough time for the service layer to call CondInstallSnapshot
	time.Sleep(1 * time.Second)

	// Follower one assertions
	if !leader.log.isEqual(followerOne.log, /* checkEntries= */ false) {
		t.Errorf(
			"TestSnapshotRpcSlowFollower expected follower one log %#v, got log %#v", 
			leader.log, followerOne.log)
	}
	if leader.log.startIndex != followerOne.log.size() {
		t.Errorf(
			"TestSnapshotRpcSlowFollower expected follower log size %v, got %v", 
			leader.log.startIndex, followerOne.log.size())
	}
	if !reflect.DeepEqual(leaderSnapshot, followerOne.persister.ReadSnapshot()) {
		t.Errorf("TestSnapshotRpcSlowFollower expected leader, follower one snapshots to be equal")
	}
	if leader.nextIndex[followerOne.me] != leader.log.snapshotIndex + 1 {
		t.Errorf(
			"TestSnapshotRpcSlowFollower expected follower nextIndex %d, got %d", 
			leader.log.snapshotIndex + 1, leader.nextIndex[followerOne.me])
	}
	if leader.matchIndex[followerOne.me] != leader.log.snapshotIndex {
		t.Errorf(
			"TestSnapshotRpcSlowFollower expected follower matchIndex %d, got %d", 
			leader.log.snapshotIndex, leader.matchIndex[followerOne.me])
	}

	// Follower two assetions
	if !leader.log.isEqual(followerTwo.log, /* checkEntries= */ false) {
		t.Errorf(
			"TestSnapshotRpcSlowFollower expected follower two log %#v, got log %#v", 
			leader.log, followerTwo.log)
	}
	if expectedLogSize != followerTwo.log.size() {
		t.Errorf(
			"TestSnapshotRpcSlowFollower expected follower two log size %v, got %v", 
			expectedLogSize, followerTwo.log.size())
	}
	if !reflect.DeepEqual(leaderSnapshot, followerTwo.persister.ReadSnapshot()) {
		t.Errorf("TestSnapshotRpcSlowFollower expected leader, follower two snapshots to be equal")
	}
	if leader.nextIndex[followerTwo.me] != leader.log.snapshotIndex + 1 {
		t.Errorf(
			"TestSnapshotRpcSlowFollower expected follower nextIndex %d, got %d", 
			leader.log.snapshotIndex + 1, leader.nextIndex[followerTwo.me])
	}
	if leader.matchIndex[followerTwo.me] != leader.log.snapshotIndex {
		t.Errorf(
			"TestSnapshotRpcSlowFollower expected follower matchIndex %d, got %d", 
			leader.log.snapshotIndex, leader.matchIndex[followerTwo.me])
	}
}








