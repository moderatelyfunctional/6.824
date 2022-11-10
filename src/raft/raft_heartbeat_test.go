package raft

import "testing"

func TestRaftHeartbeatSuccess(t *testing.T) {
	servers := 3

	cfg := make_config(t, servers, false, false, true)
	leader_rf := cfg.rafts[0]
	follower_rf := cfg.rafts[1]

	// leader_rf.sendHeartbeatTo()
}