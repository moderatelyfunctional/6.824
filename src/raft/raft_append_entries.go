package raft

type AppendEntriesArgs struct {
	Term 			int 
	LeaderId 	 	int
	PrevLogIndex 	int
	PrevLogTerm 	int
	Entries 		[]Entry
	LeaderCommit 	int
}

type AppendEntriesReply struct {
	Term 			int
	Success 		bool
}

func min(a, b int) int {
    if a < b {
        return a
    }
    return b
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// An outdated leader sending the AppendEntries (from a previous term), so inform that leader
	// to reset itself to a follower on the current term. The heartbeat should not be acked because
	// it should only be acked for an AppendEntries RPC from the current leader.
	DPrintf(dAppend, "%v", rf.prettyPrint())
	if rf.currentTerm > args.Term {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	// heartbeat must be set AFTER rf.setStateToFollower since the method will reset it to false.
	if rf.currentTerm < args.Term || rf.state != FOLLOWER {
		DPrintf(dAppend, "S%d, on T%d setting %v state to follower %#v.", rf.me, rf.currentTerm, rf.state, args)
		rf.setStateToFollower(args.Term)
	}
	rf.heartbeat = true
	
	// Check if the AppendEntriesArgs PrevLogIndex/PrevLogTerm matches the raft instance's values. There are
	// two scenarios where that won't be true:
	// 	   - Missing entry args.PrevLogIndex > len(rf.log) - 1 
	//         --> Success = false, return now.
	//     - Conflicting entry rf.log[args.PrevLogIndex].Term != PrevLogTerm
	//         --> Remove conflicting entry, Success = false, return now.
	if args.PrevLogIndex > len(rf.log) - 1 {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	} 
	if args.PrevLogIndex >= 0 && rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		// After removing conflicting entries the commitIndex should decrease to the length fo the log.
		// The lastApplied value stays the same since it's an immutable operation.
		rf.log = rf.log[:args.PrevLogIndex]
		rf.commitIndex = min(rf.commitIndex, args.PrevLogIndex - 1)
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	// The leader matches the follower's log at args.PrevLogIndex (same term). By induction, it matches the follower's log
	// up to that point. If the leader contains any additional entries, they must override the follower's log at those indices.
	// If not, then the follower can hold onto its additional uncommitted entries. This allows the entries to not be lost
	// should the follower be elected leader at a later time, and can replicate its uncommitted entries on other servers.
	if (len(args.Entries) > 0) {
		rf.log = rf.log[:args.PrevLogIndex + 1]
		rf.log = append(rf.log, args.Entries...)
	}

	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(args.LeaderCommit, len(rf.log) - 1)
	}

	reply.Term = args.Term
	reply.Success = true
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}