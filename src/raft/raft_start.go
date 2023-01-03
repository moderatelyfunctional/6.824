package raft

// import "time"
//
// The service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// The first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	isLeader := rf.state == LEADER

	if !isLeader {
		return -1, -1, false
	}

	rf.log.entries = append(rf.log.entries, Entry{
		Term: rf.currentTerm,
		Command: command,
	})
	rf.nextIndex[rf.me] = len(rf.log.entries)
	rf.matchIndex[rf.me] = len(rf.log.entries) - 1

	rf.persist()		
	return rf.matchIndex[rf.me] + 1, rf.currentTerm, true
}