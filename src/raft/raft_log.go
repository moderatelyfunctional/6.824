package raft

func (rf *Raft) isLogMoreUpToDate(otherLastLogIndex int, otherLastLogTerm int) bool {
	currentLastLogIndex := -1
	currentLastLogTerm := -1
	if len(rf.log) > 0 {
		currentLastLogIndex = len(rf.log) - 1
		currentLastLogTerm = rf.log[currentLastLogIndex].term
	}

	// if the instance's last log term is higher than the other instance's, it's more up-to-date. 
	if currentLastLogTerm > otherLastLogTerm {
		return true
	}
	// if both instances have the same last log term, the instance must have a longer log to be more 
	// up-to-date.
	if currentLastLogTerm == otherLastLogTerm && currentLastLogIndex > otherLastLogIndex {
		return true
	}
	// the rest of the scenarios where the instance's log isn't as updated: 1) its last log term is lower
	// than the other instance's or 2) both instances have the same last log term, but the other last log
	// index >= current last log index
	return false
}