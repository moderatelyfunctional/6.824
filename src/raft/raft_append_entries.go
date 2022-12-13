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
	XTerm 			int 	// Term of conflicting entry
	XIndex 			int 	// Index of first entry with cTerm
	XLen 			int 	// Length of log
}

// XTerm, XIndex, and XLen are only valid if Success = False. They describe the three types of leader/follower log conflicts.
//
// Case 1 (The leader doesn't contain the follower's conflicting term)
// F: 1 2 2 2 2
// L: 1 3 3 3 3
// Implementation: Leader checks if its log doesn't contain XTerm
// Resolution: Leader to go to XIndex
//
// Case 2 (The leader contains the follower's conflicting term by checking if its log contains XTerm)
// F: 1 1 1 1 1
// L: 1 1 1 2 2
// Implementation: Leader checks if its log contains XTerm
// Resolution: Leader to go to its most recent entry with XTerm.
//
// Case 3 (The follower's conflicting entry doesn't exist at the prevLogIndex)
// F: 1 1
// L: 1 1 2 2 2
// Implementation: Leader checks if XTerm/XIndex = -1
// Resolution: Leader to set prevLogIndex to XLen

func min(a, b int) int {
    if a < b {
        return a
    }
    return b
}

func max(a, b int) int {
	return min(-1 * a, -1 * b) * -1
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// An outdated leader sending the AppendEntries (from a previous term), so inform that leader
	// to reset itself to a follower on the current term. The heartbeat should not be acked because
	// it should only be acked for an AppendEntries RPC from the current leader.
	DPrintf(dAppend, "%v with args %v", rf.prettyPrint(), args)
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
		reply.XTerm = -1
		reply.XIndex = -1
		reply.XLen = len(rf.log)
		return
	} 
	if args.PrevLogIndex >= 0 && rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		reply.Term = rf.currentTerm
		reply.Success = false

		xIndex := args.PrevLogIndex
		xEntry := rf.log[xIndex]
		for xIndex >= 1 {
			xPrevIndex := xIndex - 1
			if rf.log[xPrevIndex].Term != xEntry.Term {
				break
			}
			xIndex = xIndex - 1
		}
		reply.XTerm = rf.log[args.PrevLogIndex].Term
		reply.XIndex = xIndex
		reply.XLen = len(rf.log)
		return
	}

	// The leader matches the follower's log at args.PrevLogIndex (same term). By induction, it matches the follower's log
	// up to that point. If the leader contains any additional entries, they must override the follower's log at those indices.
	// If not, then the follower can hold onto its additional entries (committed or uncommitted). 
	// 
	// The entries can be committed if the network is unreliable and there are two AppendEntries RPCs RPC1 and RPC2 in that order
	// but are sent to the instance as RPC2, then RPC1. In that case, this results in a no-op.
	// 
	// The entries can be uncommitted: 
	// 1) if on a prior term, the instance was a leader and received entries from the clients that it didn't
	// replicate before it became a follower. These entries should be deleted because they break the monotonically increasing term
	// count of the entries. They also allow other older instances to be elected leader based on the log comparison scheme. In the event that it wins a later election, it can replicate the entries. Uncommitted
	// entries are ONLY kept if they're from the current term. 
	//
	// 2) If on the same term, RPCs are out of order from the current leader. These entries can be safely kept.
	//
	if (len(args.Entries) > 0) {
		// additionalIndex must be bound between [0, len(rf.log)]. It is always >= 0 since PrevLogIndex >= -1, args.Entries >= 0.
		// However, it can be larger than len(rf.log) so it must be bounded between min(additionalIndex, len(rf.log)).
		additionalIndex := args.PrevLogIndex + len(args.Entries) + 1
		additionalIndex = min(additionalIndex, len(rf.log))
		additionalEntries := rf.log[additionalIndex:]

		if len(additionalEntries) > 0 && additionalEntries[0].Term != args.Term {
			additionalEntries = []Entry{}
		}

		rf.log = rf.log[:args.PrevLogIndex + 1]
		rf.log = append(rf.log, args.Entries...)
		rf.log = append(rf.log, additionalEntries...)
		rf.persist()
	}

	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(args.LeaderCommit, len(rf.log) - 1)
		go rf.sendApplyMsg()
	}

	reply.Term = args.Term
	reply.Success = true
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}