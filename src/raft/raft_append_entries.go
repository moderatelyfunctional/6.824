package raft

type AppendEntriesArgs struct {
	Term 			int 
	LeaderId 	 	int
	prevLogIndex 	int
	prevLogTerm 	int
	entries 		[]Entry
	leaderCommit 	int
}

type AppendEntriesReply struct {
	Term 			int
	Success 		bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.currentTerm > args.Term {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	DPrintf(dHeart, "S%d, on T%d setting %v state  to follower %#v.", rf.me, rf.currentTerm, rf.state, args)
	rf.state = FOLLOWER
	rf.heartbeat = true
	reply.Term = args.Term
	reply.Success = true
	return
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}