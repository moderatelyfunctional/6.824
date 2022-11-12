package raft

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
	for i, _ := range leader.nextIndex {
		leader.nextIndex[i] = len(leader.log)
	}

	followerOne.log = []Entry{
		Entry{Term: 1,},
		Entry{Term: 1,},
		Entry{Term: 2,},
		Entry{Term: 2,},
		Entry{Term: 2,},
		Entry{Term: 2,},
	}
	followerOne.me = 1
	followerOne.currentTerm = 3

	leader.sendHeartbeatTo(followerOne.me, leader.currentTerm, leader.me)

	if (leader.nextIndex[followerOne.me] != len(leader.log)) {
		t.Errorf(
			"TestRaftHeartbeat Leader S%d nextIndex for S%d expected %d, got %d",
			leader.me,
			followerOne.me,
			len(leader.log),
			leader.nextIndex[followerOne.me])
	}
	if (leader.matchIndex[followerOne.me] != len(leader.log) - 1) {
		t.Errorf(
			"TestRaftHeartbeat Leader S%d matchIndex for S%d expected %d, got %d",
			leader.me,
			followerOne.me,
			len(leader.log) - 1,
			leader.matchIndex[followerOne.me])
	}

	followerTwo.log = []Entry{
		Entry{Term: 1,},
		Entry{Term: 1,},
		Entry{Term: 2,},
	}
	followerTwo.me = 2
	followerTwo.currentTerm = 3

	leader.sendHeartbeatTo(followerTwo.me, leader.currentTerm, leader.me)

	if (leader.nextIndex[followerTwo.me] != len(leader.log) - 1) {
		t.Errorf(
			"TestRaftHeartbeat Leader S%d nextIndex for S%d expected %d, got %d",
			leader.me,
			followerTwo.me,
			len(leader.log) - 1,
			leader.nextIndex[followerTwo.me])
	}
	if (leader.matchIndex[followerTwo.me] != 0) {
		t.Errorf(
			"TestRaftHeartbeat Leader S%d matchIndex for S%d expected %d, got %d",
			leader.me,
			followerTwo.me,
			0,
			leader.matchIndex[followerTwo.me])
	}

	leader.sendHeartbeatTo(followerTwo.me, leader.currentTerm, leader.me)

	if (leader.nextIndex[followerTwo.me] != len(leader.log)) {
		t.Errorf(
			"TestRaftHeartbeat Leader S%d nextIndex for S%d expected %d, got %d",
			leader.me,
			followerTwo.me,
			len(leader.log) - 1,
			leader.nextIndex[followerTwo.me])
	}
	if (leader.matchIndex[followerTwo.me] != len(leader.log) - 1) {
		t.Errorf(
			"TestRaftHeartbeat Leader S%d matchIndex for S%d expected %d, got %d",
			leader.me,
			followerTwo.me,
			0,
			leader.matchIndex[followerTwo.me])
	}
}








