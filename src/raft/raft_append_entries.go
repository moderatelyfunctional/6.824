package raft

import "time"

type AppendEntriesArgs struct {
	Term 			int 
	LeaderId 	 	int
}

type AppendEntriesReply struct {
	Term 			int
	Success 		bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {

}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) startElectionCountdown() {
	var electionTimeout int
	var heatbeat bool
	rf.mu.Lock()
	electionTimeout = rf.electionTimeout
	rf.mu.Unlock()
	time.Sleep(time.Duration(electionTimeout) * time.Millisecond)

	rf.electionChan<-true
}

func (rf *Raft) checkElectionTimeout() {
	var state State
	var currentTerm, electionTimeout, electionTicker int
	rf.mu.Lock()
	state = rf.state
	currentTerm = rf.currentTerm
	rf.mu.Unlock()

	if electionTicker < electionTimeout {
		rf.mu.Lock()
		defer rf.mu.Unlock()
		rf.electionTicker += BASE_INTERVAL_MS
		return
	}

	if state == LEADER {
		return
	}

}

// Method is a no-op for raft instances in a follower or candidate state. For instances in a leader state, 
// empty AppendEntries RPCs are sent to the other instances. If any RPC reply return a term > that of the
// leader, the leader acks that it is not a legitimate leader, and converts to a follower.
func (rf *Raft) sendHeartbeat() {
	var state State 
	var currentTerm int
	rf.mu.Lock()
	state = rf.state
	currentTerm = rf.currentTerm
	rf.mu.Unlock()

	if state != LEADER {
		return
	}
	for i := 0; i < len(rf.peers); i++ {
		if rf.me == i {
			continue
		}
		args := AppendEntriesArgs{
			Term: rf.currentTerm,
			LeaderId: rf.me,
		}
		reply := AppendEntriesReply{}
		rf.sendAppendEntries(i, &args, &reply)

		if currentTerm < reply.Term {
			rf.mu.Lock()
			defer rf.mu.Unlock()
			rf.setStateToFollowerForLowerTerm(reply.Term)
			return
		}
	}
}







