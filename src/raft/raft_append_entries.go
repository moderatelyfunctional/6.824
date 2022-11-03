package raft

import "math"

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

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.currentTerm > args.Term || args.PrevLogTerm > len(rf.log) - 1 {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}
	if rf.log[PrevLogIndex].term != args.PrevLogTerm {
		rf.log = rf.log[:PrevLogIndex]
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	DPrintf(dHeart, "S%d, on T%d setting %v state  to follower %#v.", rf.me, rf.currentTerm, rf.state, args)
	rf.state = FOLLOWER
	rf.heartbeat = true
	rf.log = append(rf.log, args.Entries)
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex := min(args.LeaderCommit, len(rf.log) - 1)
	}

	reply.Term = args.Term
	reply.Success = true
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}