package raft

import "time"
import "testing"

func TestApplyOverlappingInvocations(t *testing.T) {
	servers := 3

	cfg := make_config(t, servers, false, false, true)
	leader := cfg.rafts[0]
	leader.log = makeLog(
		[]Entry{
			Entry{Term: 1,},
			Entry{Term: 1,},
			Entry{Term: 2,},
			Entry{Term: 2,},
			Entry{Term: 2,},
			Entry{Term: 2,},
		},
	)
	leader.me = 0
	leader.currentTerm = 2
	leader.commitIndex = 2
	leader.state = LEADER

	leader.sendApplyMsg()

	leader.commitIndex = 5
	leader.sendApplyMsg()

	time.Sleep(1 * time.Second)

	if leader.lastApplied != leader.commitIndex {
		t.Errorf("TestApplyOverlappingInvocations lastApplied expected %d, got %d", leader.commitIndex, leader.lastApplied)
	}
}