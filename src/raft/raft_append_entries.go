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
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.currentTerm > args.Term {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

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