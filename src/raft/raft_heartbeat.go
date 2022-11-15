package raft

import "sort"

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
	prevLogIndex := -1
	prevLogTerm := -1

	rf.mu.Lock()
	entries := rf.log
	if rf.nextIndex[index] > 0 {
		prevLogIndex = rf.nextIndex[index] - 1
		prevLogTerm = rf.log[prevLogIndex].Term
		entries = rf.log[rf.nextIndex[index]:]
	}
	commitIndex := rf.commitIndex
	rf.mu.Unlock()

	args := AppendEntriesArgs{
		Term: currentTerm,
		LeaderId: leaderIndex,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm: prevLogTerm,
		Entries: entries,
		LeaderCommit: commitIndex,
	}
	reply := AppendEntriesReply{}
	DPrintf(dHeart, "S%d T%d Leader %v.", rf.me, currentTerm, rf.prettyPrint())
	DPrintf(dHeart, "S%d T%d Leader sending args %#v to S%d.", rf.me, currentTerm, args, index)
	ok := rf.sendAppendEntries(index, &args, &reply)
	if !ok {
		DPrintf(dHeart, "S%d T%d Leader RPC failed for S%d.", rf.me, currentTerm, index)
		return
	}
	DPrintf(dHeart, "S%d T%d Leader receiving reply %#v from S%d.", rf.me, currentTerm, reply, index)

	rf.mu.Lock()
	defer rf.mu.Unlock()
	if currentTerm < reply.Term {
		DPrintf(dHeart, "S%d T%d Leader resetting to follower %#v. ", rf.me, currentTerm, reply)
		rf.setStateToFollower(reply.Term)
	} else if !reply.Success {
		DPrintf(dHeart, "S%d T%d Leader decrementing nextIndex for S%d to %d. ", rf.me, currentTerm, index, rf.nextIndex[index] - 1)
		// newIndex := -1
		// for i := prevLogIndex; i >= 0; i-- {
		// 	if rf.log[i].Term != prevLogTerm {
		// 		newIndex = i
		// 		break
		// 	}
		// }
		// rf.nextIndex[index] = newIndex

		rf.nextIndex[index] -= 1
	} else {
		DPrintf(dHeart, "S%d T%d Leader setting matchIndex for S%d to %d", rf.me, currentTerm, index, len(rf.log) - 1)
		rf.nextIndex[index] = len(rf.log)
		rf.matchIndex[index] = rf.nextIndex[index] - 1
		rf.checkCommitIndex()
	}
}

func (rf *Raft) checkCommitIndex() {
	matchIndex := make([]int, len(rf.peers))
	copy(matchIndex, rf.matchIndex)

	sort.Ints(matchIndex)
	midpoint := len(matchIndex) / 2
	possibleCommitIndex := matchIndex[midpoint]

	// if the new commit index <= the existing one there is no need to update it. If it corresponds to a entry 
	// from a previous term, it cannot be safely committed. In both cases return early.
	if possibleCommitIndex <= rf.commitIndex || rf.log[possibleCommitIndex].Term != rf.currentTerm {
		return
	}

	rf.commitIndex = possibleCommitIndex
}

func (rf *Raft) sendApplyMsg() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	DPrintf(dApply, rf.prettyPrint())
	if rf.commitIndex == -1 || rf.log[rf.commitIndex].Term != rf.currentTerm {
		return
	}

	// The log entry at lastApplied is already sent via the applyCh, so start at lastApplied + 1.
	lastApplied := rf.lastApplied
	applyNextIndex := lastApplied + 1
	commitIndex := rf.commitIndex

	// commitIndex needs to be included because the log entry at that index isn't applied yet.
	logSubset := make([]Entry, commitIndex - lastApplied)
	copy(logSubset, rf.log[applyNextIndex:commitIndex + 1])

	if lastApplied == commitIndex {
		return
	}

	go func(startIndex int, logSubset []Entry) {
		for i, v := range logSubset {
			applyMsg := ApplyMsg{
				CommandValid: true,
				Command: v.Command,
				CommandIndex: startIndex + i + 1, // raft expects the log to be 1-indexed rather than 0-indexed
			}
			rf.applyCh<-applyMsg
		}
	}(applyNextIndex, logSubset)
	rf.lastApplied = commitIndex

	// The log entry at lastApplied is already sent via the applyCh, so start at lastApplied + 1.
	// commitIndex needs to be included because the log entry at that index isn't sent yet.
	// for i := lastApplied + 1; i <= commitIndex; i++ {
	// 	go func(commandValid bool, command interface{}, commandIndex int) {
	// 		applyMsg := ApplyMsg{
	// 			CommandValid: commandValid,
	// 			Command: command,
	// 			CommandIndex: commandIndex + 1, // raft expects the log to be 1-indexed rather than 0-indexed
	// 		}
	// 		fmt.Printf("ApplyMsg PEEK %#v\n", applyMsg)
	// 		rf.applyCh<-applyMsg
	// 	}(true, rf.log[i].Command, i)
	// 	fmt.Printf("ApplyMsg SENDING S%d T%d %v on index %d\n", rf.me, rf.currentTerm, rf.log[i], i + 1)
	// }
	// rf.lastApplied = commitIndex
}
