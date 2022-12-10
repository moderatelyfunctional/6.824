package raft

import "sort"

// Method is a no-op for raft instances in a follower or candidate state. For instances in a leader state, 
// empty AppendEntries RPCs are sent to the other instances. If any RPC reply return a term > that of the
// leader, the leader acks that it is not a legitimate leader, and converts to a follower.
func (rf *Raft) sendHeartbeat() {
	rf.mu.Lock()
	state := rf.state
	currentTerm := rf.currentTerm
	// rf.setNextHeartbeat()
	rf.mu.Unlock()
	if state != LEADER {
		return
	}

	DPrintf(dHeart, "S%d T%d Leader, sending heartbeats", rf.me, currentTerm)
	for i := 0; i < len(rf.peers); i++ {
		if rf.me == i {
			continue
		}
		go rf.sendHeartbeatTo(i, currentTerm, rf.me, false)
	}
}

func (rf *Raft) sendCatchupHeartbeatTo(index int) {
	rf.mu.Lock()
	currentTerm := rf.currentTerm
	defer rf.mu.Unlock()

	go rf.sendHeartbeatTo(index, currentTerm, rf.me, true)
}

func (rf *Raft) sendHeartbeatTo(index int, currentTerm int, leaderIndex int, catchup bool) {
	rf.mu.Lock()
	if rf.state == FOLLOWER {
		DPrintf(dHeart, "S%d T%d Leader now a follower %#v. ", rf.me, rf.prettyPrint())
		rf.mu.Unlock()
		return
	}
	var prevLogIndex, prevLogTerm int
	var entries []Entry
	if rf.nextIndex[index] > 0 {
		prevLogIndex = rf.nextIndex[index] - 1
		prevLogTerm = rf.log[prevLogIndex].Term

		entries = make([]Entry, len(rf.log) - rf.nextIndex[index])
		copy(entries, rf.log[rf.nextIndex[index]:])
	} else {
		prevLogIndex = -1
		prevLogTerm = -1

		entries = make([]Entry, len(rf.log))
		copy(entries, rf.log)
	}
	commitIndex := rf.commitIndex

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
	rf.mu.Unlock()

	ok := rf.sendAppendEntries(index, &args, &reply)
	if !ok {
		DPrintf(dHeart, "S%d T%d Leader RPC failed for S%d.", rf.me, currentTerm, index)
		return
	}
	DPrintf(dHeart, "S%d T%d Leader receiving reply %#v from S%d.", rf.me, currentTerm, reply, index)

	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.state == FOLLOWER {
		DPrintf(dHeart, "S%d T%d Leader already set to follower %#v. ", rf.me, currentTerm, reply)
		return
	} else if currentTerm < reply.Term {
		DPrintf(dHeart, "S%d T%d Leader resetting to follower %#v. ", rf.me, currentTerm, reply)
		rf.setStateToFollower(reply.Term)
	} else if !reply.Success {
		DPrintf(dHeart, "S%d T%d Leader decrementing nextIndex for S%d", rf.me, currentTerm, index)
		var newIndex int
		if reply.XIndex == -1 {
			// Case 3, Follower is missing an entry at prevLogIndex
			newIndex = reply.XLen
		} else {
			// Case 1, 2
			firstIndexForTerm := -1
			for i := prevLogIndex; i >= 0; i-- {
				if rf.log[i].Term == reply.XTerm {
					firstIndexForTerm = i
				}
			}

			if firstIndexForTerm == -1 {
				newIndex = reply.XIndex
			} else {
				newIndex = firstIndexForTerm
			}
		}
		rf.nextIndex[index] = newIndex
		go func() {
			rf.heartbeatChan<-index
		}()
	} else {
		DPrintf(dHeart, "S%d T%d Leader setting matchIndex for S%d with prevLogIndex %v entries %v", rf.me, currentTerm, index, prevLogIndex, len(entries))
		rf.nextIndex[index] = prevLogIndex + len(entries) + 1
		rf.matchIndex[index] = prevLogIndex + len(entries)
		rf.checkCommitIndex(index, catchup)
	}
}

func (rf *Raft) checkCommitIndex(index int, catchup bool) {
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
	go rf.sendApplyMsg()
	
	// go func() {
	// 	for i := 0; i <= index; i++ {
	// 		rf.heartbeatChan<-i
	// 	}
	// }()
}

func (rf *Raft) sendApplyMsg() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	DPrintf(dApply, rf.prettyPrint())
	if rf.applyInProg || 
	   rf.commitIndex == -1 ||
	   rf.log[rf.commitIndex].Term != rf.currentTerm ||
	   rf.lastApplied == rf.commitIndex {
		return
	}

	rf.applyInProg = true
	nextApplyIndex := rf.lastApplied + 1
	commitToIndex := rf.commitIndex + 1

	// The log entry at lastApplied is already sent via the applyCh, so start at lastApplied + 1.
	// commitIndex needs to be included because the log entry at that index isn't applied yet.
	logSubset := make([]Entry, commitToIndex - nextApplyIndex)
	copy(logSubset, rf.log[nextApplyIndex:commitToIndex])

	rf.lastApplied = rf.commitIndex

	go func(startIndex int, logSubset []Entry) {
		for i, v := range logSubset {
			applyMsg := ApplyMsg{
				CommandValid: true,
				Command: v.Command,
				CommandIndex: startIndex + i + 1, // raft expects the log to be 1-indexed rather than 0-indexed
			}
			rf.applyCh<-applyMsg
		}
		rf.mu.Lock()
		rf.applyInProg = false
		rf.mu.Unlock()
	}(nextApplyIndex, logSubset)
}
