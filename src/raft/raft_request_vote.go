package raft

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term 			int 	// candidate's term
	CandidateId 	int 	// candidate requesting vote
	LastLogIndex	int 	// index of candidate's last log entry
	LastLogTerm 	int 	// term of candidate's last log entry
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term 		int 	// currentTerm for candidate to update itself
	VoteGranted bool 	// whether the candidate received vote
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// if the term of the candidate requesting the vote is lower than the requestee's term
	// exit early and follow the second rule for All Servers.
	if rf.currentTerm > args.Term {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}

	if rf.currentTerm < args.Term {
		rf.setStateToFollower(args.Term)
	}
	// there are two scenarios for a raft instance to grant its vote to a candidate:
	// 1) it has not voted for any candidates in the current term && candidate log restriction*
	// 2) it already voted for the candidate in question && candidate log restriction*
	// *candidate log restriction - candidate log is at least as up-to-date as the instance's log.
	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && !rf.isLogMoreUpToDate(args.LastLogIndex, args.LastLogTerm) {
		DPrintf(dVote, "S%d T%d voted for S%d T%d on same term", rf.me, rf.currentTerm, args.CandidateId, args.Term)
		rf.votedFor = args.CandidateId
		reply.Term = args.Term
		reply.VoteGranted = true
		rf.persist()
	} else {
		// the raft instance voted for another server in this term or its log is more up-to-date than the candidate.
		DPrintf(dVote, "S%d T%d didn't vote for S%d T%d on same term", rf.me, rf.currentTerm, args.CandidateId, args.Term)
		reply.Term = args.Term
		reply.VoteGranted = false
	}
}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}