package raft

// Method is a no-op for raft instances in a follower or candidate state. For instances in a leader state, 
// empty AppendEntries RPCs are sent to the other instances. If any RPC reply return a term > that of the
// leader, the leader acks that it is not a legitimate leader, and converts to a follower.
func (rf *Raft) sendHeartbeat() {
	rf.mu.Lock()
	state := rf.state
	currentTerm := rf.currentTerm
	me := rf.me
	rf.mu.Unlock()
	if state != LEADER {
		return
	}
	
	DPrintf(dHeart, "S%d T%d Leader, sending heartbeats", rf.me, currentTerm)
	for i := 0; i < len(rf.peers); i++ {
		if me == i {
			continue
		}
		go rf.sendHeartbeatTo(i, currentTerm, me)
	}
}

func (rf *Raft) sendHeartbeatTo(index int, currentTerm int, leaderIndex int) {
	rf.mu.Lock()
	prevLogIndex := rf.nextIndex[index] - 1
	prevLogTerm := rf.log[prevLogIndex]
	rf.mu.Unlock()

	args := AppendEntriesArgs{
		Term: currentTerm,
		LeaderId: leaderIndex,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm: prevLogTerm,
	}
	reply := AppendEntriesReply{}
	rf.sendAppendEntries(index, &args, &reply)

	rf.mu.Lock()
	defer rf.mu.Unlock()
	if currentTerm < reply.Term {
		DPrintf(dHeart, "S%d T%d Leader resetting to follower %#v. ", rf.me, currentTerm, reply)
		rf.setStateToFollower(reply.Term)
	} else if !reply.Success {
		DPrintf(dHeart, "S%d T%d Leader decrementing nextIndex for S%d to %d. ", rf.me, currentTerm, index, rf.nextIndex[index] - 1)
		rf.nextIndex[index] -= 1
	} else {
		Dprintf(dHeart, "S%d T%d Leader setting matchIndex for S%d to %d", rf.me, currentTerm, index, rf.nextIndex[index] - 1)
		rf.matchIndex = rf.nextIndex[index] - 1
	}
}
