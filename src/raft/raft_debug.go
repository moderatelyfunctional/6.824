package raft

// The methods in this file are used by external libraries to debug raft instances, and therefore
// must all be capitalized.

func (rf *Raft) IsLeader() bool {
	return rf.state == LEADER
}

func (rf *Raft) IsFollower() bool {
	return rf.state == FOLLOWER
}