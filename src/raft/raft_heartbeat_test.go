package raft

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
func TestRaftHeartbeat(t *testing.T) {
	servers := 3

	cfg := make_config(t, servers, false, false, true)
	leader := cfg.rafts[0]
	followerOne := cfg.rafts[1]
	followerTwo := cfg.rafts[2]

	leader.log = []Entry{
		Entry{Term: 1,},
		Entry{Term: 1,},
		Entry{Term: 2,},
		Entry{Term: 2,},
	}
	leader.me = 0
	leader.currentTerm = 3
	leader.commitIndex = -1
	leader.state = LEADER

	for i, _ := range leader.nextIndex {
		leader.nextIndex[i] = len(leader.log)
		leader.matchIndex[i] = -1
	}
	leader.matchIndex[leader.me] = len(leader.log) - 1

	followerOne.log = []Entry{
		Entry{Term: 1,},
		Entry{Term: 1,},
		Entry{Term: 2,},
		Entry{Term: 2,},
		Entry{Term: 2,},
		Entry{Term: 2,},
	}
	followerOne.me = 1
	followerOne.currentTerm = 2
	followerOneLog := make([]Entry, len(followerOne.log))
	copy(followerOneLog, followerOne.log)

	leader.sendHeartbeatTo(followerOne.me, leader.currentTerm, leader.me)

	if (leader.nextIndex[followerOne.me] != len(leader.log)) {
		t.Errorf(
			"TestRaftHeartbeat Leader S%d nextIndex for S%d expected %d, got %d",
			leader.me, followerOne.me, len(leader.log), leader.nextIndex[followerOne.me])
	}
	if (leader.matchIndex[followerOne.me] != len(leader.log) - 1) {
		t.Errorf(
			"TestRaftHeartbeat Leader S%d matchIndex for S%d expected %d, got %d",
			leader.me, followerOne.me, len(leader.log) - 1, leader.matchIndex[followerOne.me])
	}
	if (followerOne.currentTerm != leader.currentTerm) {
		t.Errorf(
			"TestRaftHeartbeat Leader S%d currentTerm for S%d expected %d, got %d",
			leader.me, followerOne.me, leader.currentTerm, followerOne.currentTerm)
	}
	if (!reflect.DeepEqual(followerOneLog, followerOne.log)) {
		t.Errorf(
			"TestRaftHeartbeat Leader S%d log for S%d expected %v, got %v",
			leader.me, followerOne.me, followerOneLog, followerOne.log)
	}

	followerTwo.log = []Entry{
		Entry{Term: 1,},
		Entry{Term: 1,},
		Entry{Term: 2,},
	}
	followerTwo.me = 2
	followerTwo.currentTerm = 2

	leader.sendHeartbeatTo(followerTwo.me, leader.currentTerm, leader.me)

	if (leader.nextIndex[followerTwo.me] != len(leader.log) - 1) {
		t.Errorf(
			"TestRaftHeartbeat Leader S%d nextIndex for S%d expected %d, got %d",
			leader.me, followerTwo.me, len(leader.log) - 1, leader.nextIndex[followerTwo.me])
	}
	if (leader.matchIndex[followerTwo.me] != -1) {
		t.Errorf(
			"TestRaftHeartbeat Leader S%d matchIndex for S%d expected %d, got %d",
			leader.me, followerTwo.me,  -1, leader.matchIndex[followerTwo.me])
	}

	leader.sendHeartbeatTo(followerTwo.me, leader.currentTerm, leader.me)

	if (leader.nextIndex[followerTwo.me] != len(leader.log)) {
		t.Errorf(
			"TestRaftHeartbeat Leader S%d nextIndex for S%d expected %d, got %d",
			leader.me, followerTwo.me, len(leader.log) - 1, leader.nextIndex[followerTwo.me])
	}
	if (leader.matchIndex[followerTwo.me] != len(leader.log) - 1) {
		t.Errorf(
			"TestRaftHeartbeat Leader S%d matchIndex for S%d expected %d, got %d",
			leader.me, followerTwo.me, 0, leader.matchIndex[followerTwo.me])
	}
	if (!reflect.DeepEqual(followerTwo.log, leader.log)) {
		t.Errorf(
			"TestRaftHeartbeat Leader S%d log for S%d expected %v, got %v",
			leader.me, followerTwo.me, leader.log, followerTwo.log)
	}

	// commitIndex must be 0 because there are no entries from the current term T3.
	if (leader.commitIndex != -1) {
		t.Errorf(
			"TestRaftHeartbeat Leader S%d commitIndex expected %d, got %d",
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
func TestRaftHeartbeatEntryOnCurrentTerm(t *testing.T) {
	servers := 3

	cfg := make_config(t, servers, false, false, true)
	leader := cfg.rafts[0]
	followerOne := cfg.rafts[1]
	followerTwo := cfg.rafts[2]

	leader.log = []Entry{
		Entry{Term: 1,},
		Entry{Term: 1,},
		Entry{Term: 2,},
		Entry{Term: 2,},
		Entry{Term: 3,},
	}
	leader.me = 0
	leader.currentTerm = 3
	leader.commitIndex = -1
	leader.state = LEADER
	for i, _ := range leader.nextIndex {
		leader.nextIndex[i] = len(leader.log)
		leader.matchIndex[i] = -1
	}
	leader.matchIndex[leader.me] = len(leader.log) - 1

	followerOne.log = []Entry{
		Entry{Term: 1,},
		Entry{Term: 1,},
		Entry{Term: 2,},
		Entry{Term: 2,},
		Entry{Term: 2,},
	}
	followerOne.me = 1
	followerOne.currentTerm = 3

	leader.sendHeartbeatTo(followerOne.me, leader.currentTerm, leader.me)
	// Leader sets nextIndex to its first occurrence of term 2
	if (leader.nextIndex[followerOne.me] != 2) {
		t.Errorf(
			"TestRaftHeartbeatEntryOnCurrentTerm Leader S%d nextIndex for S%d expected %d, got %d",
			leader.me, followerOne.me, len(leader.log) - 1, leader.nextIndex[followerOne.me])
	}
	if (leader.matchIndex[followerOne.me] != -1) {
		t.Errorf(
			"TestRaftHeartbeatEntryOnCurrentTerm Leader S%d matchIndex for S%d expected %d, got %d",
			leader.me, followerOne.me, -1, leader.matchIndex[followerOne.me])
	}

	// commitIndex must be 0 because the matchIndex is [4, 0, 0].
	if (leader.commitIndex != -1) {
		t.Errorf(
			"TestRaftHeartbeatEntryOnCurrentTerm Leader S%d commitIndex expected %d, got %d",
			leader.me, -1, leader.commitIndex)
	}

	leader.sendHeartbeatTo(followerOne.me, leader.currentTerm, leader.me)
	if (leader.nextIndex[followerOne.me] != len(leader.log)) {
		t.Errorf(
			"TestRaftHeartbeatEntryOnCurrentTerm Leader S%d nextIndex for S%d expected %d, got %d",
			leader.me, followerOne.me, len(leader.log), leader.nextIndex[followerOne.me])
	}
	if (leader.matchIndex[followerOne.me] != len(leader.log) - 1) {
		t.Errorf(
			"TestRaftHeartbeatEntryOnCurrentTerm Leader S%d matchIndex for S%d expected %d, got %d",
			leader.me, followerOne.me, len(leader.log) - 1, leader.matchIndex[followerOne.me])
	}
	// commitIndex must be 4 because the matchIndex is [4, 4, 0] and term of the entry at index 4
	// is equal to the current term T3.
	if (leader.commitIndex != 4) {
		t.Errorf(
			"TestRaftHeartbeatEntryOnCurrentTerm Leader S%d commitIndex expected %d, got %d",
			leader.me,
			4,
			leader.commitIndex)
	}
	if (!reflect.DeepEqual(followerOne.log, leader.log)) {
		t.Errorf(
			"TestRaftHeartbeat Leader S%d log for S%d expected %v, got %v",
			leader.me, followerOne.me, leader.log, followerOne.log)
	}

	followerTwo.log = []Entry{
		Entry{Term: 1,},
		Entry{Term: 1,},
		Entry{Term: 3,},
	}
	followerTwo.me = 2
	followerTwo.currentTerm = 2

	leader.sendHeartbeatTo(followerTwo.me, leader.currentTerm, leader.me)
	// Leader sets nextIndex to the XLen of the followerTwo log
	if (leader.nextIndex[followerTwo.me] != 3) {
		t.Errorf(
			"TestRaftHeartbeatEntryOnCurrentTerm Leader S%d nextIndex for S%d expected %d, got %d",
			leader.me, followerTwo.me, len(leader.log), leader.nextIndex[followerTwo.me])
	}
	if (leader.matchIndex[followerOne.me] != len(leader.log) - 1) {
		t.Errorf(
			"TestRaftHeartbeatEntryOnCurrentTerm Leader S%d matchIndex for S%d expected %d, got %d",
			leader.me, followerOne.me, len(leader.log) - 1, leader.matchIndex[followerOne.me])
	}

	leader.sendHeartbeatTo(followerTwo.me, leader.currentTerm, leader.me)
	// Leader sets nextIndex to the XIndex of followerTwo
	if (leader.nextIndex[followerTwo.me] != 2) {
		t.Errorf(
			"TestRaftHeartbeatEntryOnCurrentTerm Leader S%d nextIndex for S%d expected %d, got %d",
			leader.me, followerTwo.me, len(leader.log), leader.nextIndex[followerTwo.me])
	}
	if (leader.matchIndex[followerOne.me] != len(leader.log) - 1) {
		t.Errorf(
			"TestRaftHeartbeatEntryOnCurrentTerm Leader S%d matchIndex for S%d expected %d, got %d",
			leader.me, followerOne.me, len(leader.log) - 1, leader.matchIndex[followerOne.me])
	}

	leader.sendHeartbeatTo(followerTwo.me, leader.currentTerm, leader.me)
	if (leader.nextIndex[followerTwo.me] != len(leader.log)) {
		t.Errorf(
			"TestRaftHeartbeatEntryOnCurrentTerm Leader S%d nextIndex for S%d expected %d, got %d",
			leader.me, followerTwo.me, len(leader.log), leader.nextIndex[followerTwo.me])
	}
	if (leader.matchIndex[followerOne.me] != len(leader.log) - 1) {
		t.Errorf(
			"TestRaftHeartbeatEntryOnCurrentTerm Leader S%d matchIndex for S%d expected %d, got %d",
			leader.me, followerOne.me, len(leader.log) - 1, leader.matchIndex[followerOne.me])
	}

	if (!reflect.DeepEqual(followerTwo.log, leader.log)) {
		t.Errorf(
			"TestRaftHeartbeat Leader S%d log for S%d expected %v, got %v",
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
func TestRaftHeartbeatInitialLogEntry(t *testing.T) {
	servers := 3

	cfg := make_config(t, servers, false, false, true)
	leader := cfg.rafts[0]
	followerOne := cfg.rafts[1]
	followerTwo := cfg.rafts[2]

	leader.log = []Entry{}
	leader.me = 0
	leader.currentTerm = 2
	leader.commitIndex = -1
	leader.state = LEADER
	leader.applyCh = make(chan ApplyMsg)
	for i, _ := range leader.nextIndex {
		leader.nextIndex[i] = len(leader.log)
		leader.matchIndex[i] = -1
	}

	followerOne.me = 1
	followerOne.currentTerm = leader.currentTerm
	followerOne.log = []Entry{
		Entry{Term: leader.currentTerm - 1, Command: "x -> 4",},
	}
	followerOne.applyCh = make(chan ApplyMsg)

	followerTwo.me = 2
	followerTwo.currentTerm = leader.currentTerm
	followerTwo.applyCh = make(chan ApplyMsg)

	leader.Start("x -> 1")

	expectedLog := []Entry{
		Entry{Term: leader.currentTerm, Command: "x -> 1",},
	}
	if (!reflect.DeepEqual(leader.log, expectedLog)) {
		t.Errorf(
			"TestRaftHeartbeatInitialLogEntry Leader S%d log expected %v, got %v",
			leader.me, expectedLog, leader.log)
	}

	leader.sendHeartbeatTo(followerOne.me, leader.currentTerm, leader.me)
	leader.sendHeartbeatTo(followerTwo.me, leader.currentTerm, leader.me)
	if (!reflect.DeepEqual(followerOne.log, expectedLog)) {
		t.Errorf(
			"TestRaftHeartbeatInitialLogEntry Follower S%d log expected %v, got %v",
			followerOne.me, expectedLog, followerOne.log)	
	}
	if (!reflect.DeepEqual(followerTwo.log, expectedLog)) {
		t.Errorf(
			"TestRaftHeartbeatInitialLogEntry Follower S%d log expected %v, got %v",
			followerTwo.me, expectedLog, followerTwo.log)	
	}
	if (leader.commitIndex != 0) {
		t.Errorf(
			"TestRaftHeartbeatInitialLogEntry Leader S%d commitIndex expected %d got %d",
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
		t.Errorf("TestRaftHeartbeatInitialLogEntry expected leaderApplyMsg to be true, but was false")
	}

	leader.sendHeartbeatTo(followerOne.me, leader.currentTerm, leader.me)
	leader.sendHeartbeatTo(followerTwo.me, leader.currentTerm, leader.me)
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
		t.Errorf("TestRaftHeartbeatInitialLogEntry expected followerApplyMsg to be true, but was false")
	}
}
