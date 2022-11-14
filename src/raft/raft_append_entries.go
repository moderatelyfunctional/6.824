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
	DPrintf(dHeart, "S%d with state %#v", rf.me, rf)
	if rf.currentTerm > args.Term {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	// heartbeat must be set AFTER rf.setStateToFollower since the method will reset it to false.
	if rf.currentTerm < args.Term || rf.state != FOLLOWER {
		DPrintf(dHeart, "S%d, on T%d setting %v state to follower %#v.", rf.me, rf.currentTerm, rf.state, args)
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
		DPrintf(dHeart, "S%d with log FAILED on FIRST %#v", rf.me, rf.log)
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	} else if args.PrevLogIndex >= 0 && rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		DPrintf(dHeart, "S%d with log FAILED on SECOND", rf.me, rf.log)
		rf.log = rf.log[:args.PrevLogIndex]
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	// The leader with an empty log was elected (a majority of the servers have empty logs). Therefore,
	// in the scenario where this raft instance has additional uncommitted entries, they should be discarded.
	// Otherwise append any new entries from the leader to the raft instance. Also update the commitIndex
	// if possible.
	if args.PrevLogIndex == -1 {
		rf.log = args.Entries
	} else {
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