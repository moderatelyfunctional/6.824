package raft

// import "fmt"
import "time"
import "reflect"
import "testing"

// S0 LEADER 	T3 with log [1, 1, 2, 2]
// S1 FOLLOWER 	T3 with log [1, 1, 2, 2, 2, 2]
// S2 FOLLOWER 	T3 with log [1, 1, 2]
// 
// S0 LEADER at election on T3 (log [1, 1, 2, 2]) could only have received a vote from S2
// because S1 won't vote for it since its log would be more up to date (extra uncommitted entries).
// 
// If S0 doesn't receive any additional log entries, S1's extra uncommitted entries will never
// be destroyed. They also can never be committed by S0 since they are from a previous term.
// When S0 receives an entry on T3, it can now remove the extra uncommitted entries in S1. 
//
// However heartbeats to S2 (with missing entries) from S0 will eventually append the committed entries
// from T2 to S2.
func TestHeartbeat(t *testing.T) {
	servers := 3

	cfg := make_config(t, servers, false, false, true)
	leader := cfg.rafts[0]
	followerOne := cfg.rafts[1]
	followerTwo := cfg.rafts[2]

	leader.log = makeLog(
		[]Entry{
			Entry{Term: 1,},
			Entry{Term: 1,},
			Entry{Term: 2,},
			Entry{Term: 2,},
		},
	)
	leader.me = 0
	leader.currentTerm = 3
	leader.commitIndex = -1
	leader.state = LEADER

	for i, _ := range leader.nextIndex {
		leader.nextIndex[i] = leader.log.size()
		leader.matchIndex[i] = -1
	}
	leader.matchIndex[leader.me] = leader.log.size() - 1

	followerOne.log = makeLog(
		[]Entry{
			Entry{Term: 1,},
			Entry{Term: 1,},
			Entry{Term: 2,},
			Entry{Term: 2,},
			Entry{Term: 2,},
			Entry{Term: 2,},
		},
	)
	followerOne.me = 1
	followerOne.currentTerm = 2
	followerOneLog := makeLog([]Entry{})
	followerOneLog.entries = make([]Entry, followerOne.log.size())
	copy(followerOneLog.entries, followerOne.log.entries)

	leader.sendHeartbeatTo(
		followerOne.me, 
		leader.currentTerm)

	if (leader.nextIndex[followerOne.me] != leader.log.size()) {
		t.Errorf(
			"TestHeartbeat Leader S%d nextIndex for S%d expected %d, got %d",
			leader.me, followerOne.me, leader.log.size(), leader.nextIndex[followerOne.me])
	}
	if (leader.matchIndex[followerOne.me] != leader.log.size() - 1) {
		t.Errorf(
			"TestHeartbeat Leader S%d matchIndex for S%d expected %d, got %d",
			leader.me, followerOne.me, leader.log.size() - 1, leader.matchIndex[followerOne.me])
	}
	if (followerOne.currentTerm != leader.currentTerm) {
		t.Errorf(
			"TestHeartbeat Leader S%d currentTerm for S%d expected %d, got %d",
			leader.me, followerOne.me, leader.currentTerm, followerOne.currentTerm)
	}
	if (!reflect.DeepEqual(followerOneLog, followerOne.log)) {
		t.Errorf(
			"TestHeartbeat Leader S%d log for S%d expected %v, got %v",
			leader.me, followerOne.me, followerOneLog, followerOne.log)
	}

	followerTwo.log = makeLog(
		[]Entry{
			Entry{Term: 1,},
			Entry{Term: 1,},
			Entry{Term: 2,},
		},
	)
	followerTwo.me = 2
	followerTwo.currentTerm = 2

	leader.sendHeartbeatTo(
		followerTwo.me,
		leader.currentTerm)

	if (leader.nextIndex[followerTwo.me] != leader.log.size() - 1) {
		t.Errorf(
			"TestHeartbeat Leader S%d nextIndex for S%d expected %d, got %d",
			leader.me, followerTwo.me, leader.log.size() - 1, leader.nextIndex[followerTwo.me])
	}
	if (leader.matchIndex[followerTwo.me] != -1) {
		t.Errorf(
			"TestHeartbeat Leader S%d matchIndex for S%d expected %d, got %d",
			leader.me, followerTwo.me,  -1, leader.matchIndex[followerTwo.me])
	}

	leader.sendHeartbeatTo(
		followerTwo.me,
		leader.currentTerm)

	if (leader.nextIndex[followerTwo.me] != leader.log.size()) {
		t.Errorf(
			"TestHeartbeat Leader S%d nextIndex for S%d expected %d, got %d",
			leader.me, followerTwo.me, leader.log.size() - 1, leader.nextIndex[followerTwo.me])
	}
	if (leader.matchIndex[followerTwo.me] != leader.log.size() - 1) {
		t.Errorf(
			"TestHeartbeat Leader S%d matchIndex for S%d expected %d, got %d",
			leader.me, followerTwo.me, 0, leader.matchIndex[followerTwo.me])
	}
	if (!reflect.DeepEqual(followerTwo.log, leader.log)) {
		t.Errorf(
			"TestHeartbeat Leader S%d log for S%d expected %v, got %v",
			leader.me, followerTwo.me, leader.log, followerTwo.log)
	}

	// commitIndex must be 0 because there are no entries from the current term T3.
	if (leader.commitIndex != -1) {
		t.Errorf(
			"TestHeartbeat Leader S%d commitIndex expected %d, got %d",
			leader.me, -1, leader.commitIndex)
	}

}

// S0 LEADER 	T3 with log [1, 1, 2, 2, 3]
// S1 FOLLOWER 	T3 with log [1, 1, 2, 2, 2]
// S2 FOLLOWER 	T3 with log [1, 1, 2]
// 
// S0 LEADER at election on T3 (log [1, 1, 2, 2]) could only have received a vote from S2
// because S1 won't vote for it since its log would be more up to date (extra uncommitted entries).
// 
// S0 receives another log entry on T3 and on its heartbeat message to 
//   - S1 --> overwrite the extra uncommitted entry from T2 with the T3 entry
//   - S2 --> appends the missing entry from T2 and the additional T3 entry
//
// S0 also increments the commitIndex to 4 after replicating its log completely on S1 and S2.
func TestHeartbeatEntryOnCurrentTerm(t *testing.T) {
	servers := 3

	cfg := make_config(t, servers, false, false, true)
	leader := cfg.rafts[0]
	followerOne := cfg.rafts[1]
	followerTwo := cfg.rafts[2]

	leader.log = makeLog(
		[]Entry{
			Entry{Term: 1,},
			Entry{Term: 1,},
			Entry{Term: 2,},
			Entry{Term: 2,},
			Entry{Term: 3,},
		},
	)
	leader.me = 0
	leader.currentTerm = 3
	leader.commitIndex = -1
	leader.state = LEADER
	for i, _ := range leader.nextIndex {
		leader.nextIndex[i] = leader.log.size()
		leader.matchIndex[i] = -1
	}
	leader.matchIndex[leader.me] = leader.log.size() - 1

	followerOne.log = makeLog(
		[]Entry{
			Entry{Term: 1,},
			Entry{Term: 1,},
			Entry{Term: 2,},
			Entry{Term: 2,},
			Entry{Term: 2,},
		},
	)
	followerOne.me = 1
	followerOne.currentTerm = 3

	leader.sendHeartbeatTo(followerOne.me, leader.currentTerm)
	// Leader sets nextIndex to its first occurrence of term 2
	if (leader.nextIndex[followerOne.me] != 2) {
		t.Errorf(
			"TestHeartbeatEntryOnCurrentTerm Leader S%d nextIndex for S%d expected %d, got %d",
			leader.me, followerOne.me, leader.log.size() - 1, leader.nextIndex[followerOne.me])
	}
	if (leader.matchIndex[followerOne.me] != -1) {
		t.Errorf(
			"TestHeartbeatEntryOnCurrentTerm Leader S%d matchIndex for S%d expected %d, got %d",
			leader.me, followerOne.me, -1, leader.matchIndex[followerOne.me])
	}

	// commitIndex must be 0 because the matchIndex is [4, 0, 0].
	if (leader.commitIndex != -1) {
		t.Errorf(
			"TestHeartbeatEntryOnCurrentTerm Leader S%d commitIndex expected %d, got %d",
			leader.me, -1, leader.commitIndex)
	}

	leader.sendHeartbeatTo(followerOne.me, leader.currentTerm)
	if (leader.nextIndex[followerOne.me] != leader.log.size()) {
		t.Errorf(
			"TestHeartbeatEntryOnCurrentTerm Leader S%d nextIndex for S%d expected %d, got %d",
			leader.me, followerOne.me, leader.log.size(), leader.nextIndex[followerOne.me])
	}
	if (leader.matchIndex[followerOne.me] != leader.log.size() - 1) {
		t.Errorf(
			"TestHeartbeatEntryOnCurrentTerm Leader S%d matchIndex for S%d expected %d, got %d",
			leader.me, followerOne.me, leader.log.size() - 1, leader.matchIndex[followerOne.me])
	}
	// commitIndex must be 4 because the matchIndex is [4, 4, 0] and term of the entry at index 4
	// is equal to the current term T3.
	if (leader.commitIndex != 4) {
		t.Errorf(
			"TestHeartbeatEntryOnCurrentTerm Leader S%d commitIndex expected %d, got %d",
			leader.me,
			4,
			leader.commitIndex)
	}
	if (!reflect.DeepEqual(followerOne.log, leader.log)) {
		t.Errorf(
			"TestHeartbeat Leader S%d log for S%d expected %v, got %v",
			leader.me, followerOne.me, leader.log, followerOne.log)
	}

	followerTwo.log = makeLog(
		[]Entry{
			Entry{Term: 1,},
			Entry{Term: 1,},
			Entry{Term: 3,},
		},
	)
	followerTwo.me = 2
	followerTwo.currentTerm = 2

	leader.sendHeartbeatTo(followerTwo.me, leader.currentTerm)
	// Leader sets nextIndex to the XLen of the followerTwo log
	if (leader.nextIndex[followerTwo.me] != 3) {
		t.Errorf(
			"TestHeartbeatEntryOnCurrentTerm Leader S%d nextIndex for S%d expected %d, got %d",
			leader.me, followerTwo.me, leader.log.size(), leader.nextIndex[followerTwo.me])
	}
	if (leader.matchIndex[followerOne.me] != leader.log.size() - 1) {
		t.Errorf(
			"TestHeartbeatEntryOnCurrentTerm Leader S%d matchIndex for S%d expected %d, got %d",
			leader.me, followerOne.me, leader.log.size() - 1, leader.matchIndex[followerOne.me])
	}

	leader.sendHeartbeatTo(followerTwo.me, leader.currentTerm)
	// Leader sets nextIndex to the XIndex of followerTwo
	if (leader.nextIndex[followerTwo.me] != 2) {
		t.Errorf(
			"TestHeartbeatEntryOnCurrentTerm Leader S%d nextIndex for S%d expected %d, got %d",
			leader.me, followerTwo.me, leader.log.size(), leader.nextIndex[followerTwo.me])
	}
	if (leader.matchIndex[followerOne.me] != leader.log.size() - 1) {
		t.Errorf(
			"TestHeartbeatEntryOnCurrentTerm Leader S%d matchIndex for S%d expected %d, got %d",
			leader.me, followerOne.me, leader.log.size() - 1, leader.matchIndex[followerOne.me])
	}

	leader.sendHeartbeatTo(followerTwo.me, leader.currentTerm)
	if (leader.nextIndex[followerTwo.me] != leader.log.size()) {
		t.Errorf(
			"TestHeartbeatEntryOnCurrentTerm Leader S%d nextIndex for S%d expected %d, got %d",
			leader.me, followerTwo.me, leader.log.size(), leader.nextIndex[followerTwo.me])
	}
	if (leader.matchIndex[followerOne.me] != leader.log.size() - 1) {
		t.Errorf(
			"TestHeartbeatEntryOnCurrentTerm Leader S%d matchIndex for S%d expected %d, got %d",
			leader.me, followerOne.me, leader.log.size() - 1, leader.matchIndex[followerOne.me])
	}

	if (!reflect.DeepEqual(followerTwo.log, leader.log)) {
		t.Errorf(
			"TestHeartbeat Leader S%d log for S%d expected %v, got %v",
			leader.me,
			followerTwo.me,
			leader.log,
			followerTwo.log)
	}
}

// S0 LEADER 	T2 with log [2]
// S1 FOLLOWER 	T2 with log [1]
// S2 FOLLOWER 	T2 with log []
// 
// S0 LEADER sends the initial entry to S1, S2. They should both replicate the log entry in their 
// states.
// 
// On success, S0 then sets its commitIndex to 1.
func TestHeartbeatInitialLogEntry(t *testing.T) {
	servers := 3

	cfg := make_config(t, servers, false, false, true)
	leader := cfg.rafts[0]
	followerOne := cfg.rafts[1]
	followerTwo := cfg.rafts[2]

	leader.log = makeLog([]Entry{},)
	leader.me = 0
	leader.currentTerm = 2
	leader.commitIndex = -1
	leader.state = LEADER
	leader.applyCh = make(chan ApplyMsg)
	for i, _ := range leader.nextIndex {
		leader.nextIndex[i] = leader.log.size()
		leader.matchIndex[i] = -1
	}

	followerOne.me = 1
	followerOne.currentTerm = leader.currentTerm
	followerOne.log = makeLog(
		[]Entry{
			Entry{Term: leader.currentTerm - 1, Command: "x -> 4",},
		},
	)
	followerOne.applyCh = make(chan ApplyMsg)

	followerTwo.me = 2
	followerTwo.currentTerm = leader.currentTerm
	followerTwo.log = makeLog([]Entry{},)
	followerTwo.applyCh = make(chan ApplyMsg)

	leader.Start("x -> 1")

	expectedLog := makeLog(
		[]Entry{
			Entry{Term: leader.currentTerm, Command: "x -> 1",},
		},
	)
	if (!reflect.DeepEqual(leader.log, expectedLog)) {
		t.Errorf(
			"TestHeartbeatInitialLogEntry Leader S%d log expected %v, got %v",
			leader.me, expectedLog, leader.log)
	}

	leader.sendHeartbeatTo(followerOne.me, leader.currentTerm)
	leader.sendHeartbeatTo(followerTwo.me, leader.currentTerm)
	if (!reflect.DeepEqual(followerOne.log, expectedLog)) {
		t.Errorf(
			"TestHeartbeatInitialLogEntry Follower S%d log expected %v, got %v",
			followerOne.me, expectedLog, followerOne.log)	
	}
	if (!reflect.DeepEqual(followerTwo.log, expectedLog)) {
		t.Errorf(
			"TestHeartbeatInitialLogEntry Follower S%d log expected %v, got %v",
			followerTwo.me, expectedLog, followerTwo.log)	
	}
	if (leader.commitIndex != 0) {
		t.Errorf(
			"TestHeartbeatInitialLogEntry Leader S%d commitIndex expected %d got %d",
			leader.me, 0, leader.commitIndex)
	}

	leader.sendApplyMsg()
	leaderApplyMsg := false
	go func() {
		<-leader.applyCh
		leaderApplyMsg = true
	}()
	time.Sleep(time.Duration(APPLY_MSG_INTERVAL_MS * 2) * time.Millisecond)
	if (!leaderApplyMsg) {
		t.Errorf("TestHeartbeatInitialLogEntry expected leaderApplyMsg to be true, but was false")
	}

	leader.sendHeartbeatTo(followerOne.me, leader.currentTerm)
	leader.sendHeartbeatTo(followerTwo.me, leader.currentTerm)
	followerOne.sendApplyMsg()
	followerTwo.sendApplyMsg()
	followerApplyMsg := false
	go func() {
		<-followerOne.applyCh
		<-followerTwo.applyCh
		followerApplyMsg = true
	}()
	time.Sleep(time.Duration(APPLY_MSG_INTERVAL_MS * 2) * time.Millisecond)
	if (!followerApplyMsg) {
		t.Errorf("TestHeartbeatInitialLogEntry expected followerApplyMsg to be true, but was false")
	}
}

func TestHeartbeatSnapshotAppend(t *testing.T) {
	servers := 3

	cfg := make_config(t, servers, false, true, true)
	follower := cfg.rafts[0]
	leader := cfg.rafts[1]

	leader.me = 0
	leader.currentTerm = 1
	leader.commitIndex = -1
	leader.lastApplied = -1
	leader.state = LEADER
	leader.nextIndex = []int{0, 0, 10}
	leader.matchIndex = []int{-1, -1, 9}
	for i := 0; i < configSnapshotInterval; i++ {
		leader.Start(i)
	}
	leader.checkCommitIndex()

	// Provide time for the service to tell the leader to snapshot its log.
	time.Sleep(time.Duration(2) * time.Second)

	follower.me = 1
	follower.currentTerm = 1
	follower.state = FOLLOWER
	follower.log = makeLog(
		[]Entry{
			Entry{Term: 1, Command: 239048204821,},
			Entry{Term: 1, Command: 242342424241,},
			Entry{Term: 1, Command: 980978973421},
		},
	)
	leader.sendInstallSnapshotTo(
		follower.me,
		leader.currentTerm,
		leader.log.snapshotTerm,
		leader.log.snapshotIndex,
		leader.persister.snapshot)

	// Provide time for the service to tell the follower to install the leader's snapshot. The entries should be
	// set to nil since compaction sets the follower log entries to []raft.Entry(nil) while makeLogFromSnapshot
	// sets it to []raft.Entry{}
	time.Sleep(time.Duration(2) * time.Second)

	expectedFollowerLog := makeLogFromSnapshot(
		/* startIndex= */ 9,
		/* snapshotTerm= */ 1,
		/* snapshotIndex= */ 8,
		/* entries= */ []Entry{},
	)
	if (!reflect.DeepEqual(expectedFollowerLog, follower.log)) {
		t.Errorf(
			"TestHeartbeatSnapshotAppend follower log after snapshot expected %v got %v",
			expectedFollowerLog, follower.log)
	}
	if (follower.lastApplied != leader.log.snapshotIndex) {
		t.Errorf(
			"TestHeartbeatSnapshotAppend follower last applied after snapshot expected %v got %v",
			leader.log.snapshotIndex, follower.lastApplied)
	}
	if (follower.commitIndex != leader.log.snapshotIndex) {
		t.Errorf(
			"TestHeartbeatSnapshotAppend follower commit index after snapshot expected %v got %v",
			leader.log.snapshotIndex, follower.commitIndex)
	}

	// Pretend the leader received another entry from a client. The leader now sends the entry to the follower.
	for i := configSnapshotInterval; i <= 2 * configSnapshotInterval; i++ {
		leader.Start(i)
	}
	// The first sendHeartbeatTo sends the entries to the follower, while the second one sends the incremented
	// commitIndex from the successful completion of the first sendHeartbeatTo.
	leader.sendHeartbeatTo(follower.me, leader.currentTerm)
	leader.sendHeartbeatTo(follower.me, leader.currentTerm)

	// Wait for the leader and follower to snapshot their logs.
	time.Sleep(time.Duration(2) * time.Second)

	expectedLog := makeLogFromSnapshot(
		/* startIndex= */ 19,
		/* snapshotTerm= */ 1,
		/* snapshotIndex= */ 18,
		/* entries= */ []Entry{},
	)
	if (!reflect.DeepEqual(expectedLog, leader.log)) {
		t.Errorf(
			"TestHeartbeatSnapshotAppend leader log after heartbeat expected %v got %v",
			expectedLog, leader.log)
	}
	if (!reflect.DeepEqual(expectedLog, follower.log)) {
		t.Errorf(
			"TestHeartbeatSnapshotAppend follower log after heartbeat expected %v got %v",
			expectedLog, follower.log)
	}
}

func TestHeartbeatSnapshotDroppedResponse(t *testing.T) {
	servers := 3

	cfg := make_config(t, servers, false, true, true)
	leader := cfg.rafts[0]
	follower := cfg.rafts[1]

	leader.me = 0
	leader.currentTerm = 1
	leader.commitIndex = -1
	leader.lastApplied = -1
	leader.state = LEADER
	leader.nextIndex = []int{0, 0, 10}
	leader.matchIndex = []int{-1, -1, 9}

	additionalEntries := 5
	for i := 0; i < configSnapshotInterval + additionalEntries; i++ {
		leader.Start(i)
	}
	leader.checkCommitIndex()
	// Provide time for the service to tell the leader to snapshot its log.
	time.Sleep(time.Duration(2) * time.Second)

	snapshotTerm, snapshotIndex := leader.log.snapshotEntryInfo()
	args := InstallSnapshotArgs{
		Term: leader.currentTerm,
		SnapshotTerm: snapshotTerm,
		SnapshotIndex: snapshotIndex,
		Snapshot: leader.persister.ReadSnapshot(),
	}
	reply := InstallSnapshotReply{}

	follower.me = 1
	follower.currentTerm = 1
	follower.commitIndex = -1
	follower.lastApplied = -1
	follower.state = FOLLOWER
	follower.log = makeLog(
		[]Entry{
			Entry{Term: 1, Command: 239048204821,},
			Entry{Term: 1, Command: 242342424241,},
			Entry{Term: 1, Command: 980978973421,},
		},
	)

	leader.sendInstallSnapshot(follower.me, &args, &reply)
	time.Sleep(time.Duration(2) * time.Second)
	leader.sendHeartbeat()
	time.Sleep(time.Duration(2) * time.Second)

	if leader.nextIndex[follower.me] == 0 {
		t.Errorf("TestHeartbeatSnapshotDroppedResponse leader nextIndex for follower expected non-zero but was zero")
	}
}

func TestHeartbeatSnapshotAppendTooSoon(t *testing.T) {
	servers := 3

	cfg := make_config(t, servers, false, true, true)
	leader := cfg.rafts[0]
	follower := cfg.rafts[1]

	// Follower apply channel is _guaranteed_ to not complete the InstallSnapshotRPC the leader sent by the time
	//the leader sends an AppendEntriesRPC (sendHeartbeatTo).
	followerApplyCh := make(chan ApplyMsg)
	go func() {
		for {
			time.Sleep(time.Duration(200) * time.Millisecond)
			<-followerApplyCh
		}
	}()
	follower.applyCh = followerApplyCh

	leader.me = 0
	leader.currentTerm = 1
	leader.commitIndex = -1
	leader.lastApplied = -1
	leader.state = LEADER
	leader.nextIndex = []int{0, 0, 10}
	leader.matchIndex = []int{-1, -1, 9}

	additionalEntries := 5
	for i := 0; i < configSnapshotInterval + additionalEntries; i++ {
		leader.Start(i)
	}
	leader.checkCommitIndex()

	// Provide time for the service to tell the leader to snapshot its log.
	time.Sleep(time.Duration(2) * time.Second)

	follower.me = 1
	follower.currentTerm = 1
	follower.commitIndex = -1
	follower.lastApplied = -1
	follower.state = FOLLOWER
	follower.log = makeLog(
		[]Entry{
			Entry{Term: 1, Command: 239048204821,},
			Entry{Term: 1, Command: 242342424241,},
			Entry{Term: 1, Command: 980978973421,},
		},
	)

	followerNextIndex := leader.nextIndex[follower.me]
	leader.sendInstallSnapshotTo(
		follower.me,
		leader.currentTerm,
		leader.log.snapshotTerm,
		leader.log.snapshotIndex,
		leader.persister.snapshot)
	leader.sendHeartbeatTo(
		follower.me,
		leader.currentTerm)
	if (leader.nextIndex[follower.me] != followerNextIndex - 1) {
		t.Errorf(
			"TestHeartbeatSnapshotAppendTooSoon expected leader to decrement nextIndex to %v but got %v",
			followerNextIndex - 1, leader.nextIndex[follower.me])
	}
}

func TestHeartbeatSnapshotDeadlock(t *testing.T) {
	servers := 3

	cfg := make_config(t, servers, false, true, true)
	leader := cfg.rafts[1]
	followerOne := cfg.rafts[0]
	followerTwo := cfg.rafts[2]

	leader.me = 1
	leader.currentTerm = 2

	followerOne.me = 0
	followerOne.currentTerm = 2

	followerTwo.me = 2
	followerTwo.currentTerm = 2
}

























