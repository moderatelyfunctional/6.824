package raft

import "sort"
import "math/rand"

// This is a no-op for raft instances in a follower or candidate state. For instances in a leader state, 
// empty AppendEntries RPCs are sent to the other instances. If any RPC reply return a term > that of the
// leader, the leader acks that it is not a legitimate leader, and converts to a follower.
//
// TODO: Optimization to only do locking ONCE before sending RPCs to all the servers.
func (rf *Raft) sendHeartbeat() {
	rf.mu.Lock()
	state := rf.state
	currentTerm := rf.currentTerm
	rf.mu.Unlock()
	if state != LEADER {
		return
	}

	DPrintf(dHeart, "S%d T%d Leader, sending heartbeats", rf.me, currentTerm)
	for i := 0; i < len(rf.peers); i++ {
		if rf.me == i {
			continue
		}
		go rf.sendHeartbeatTo(i, currentTerm)
	}
}

func (rf *Raft) sendOnHeartbeatChan(index int) {
	rf.heartbeatChan<-index
}

func (rf *Raft) sendCatchupHeartbeatTo(index int) {
	rf.mu.Lock()
	currentTerm := rf.currentTerm
	defer rf.mu.Unlock()

	go rf.sendHeartbeatTo(index, currentTerm)
}

// Only the leader should send the heartbeat to the other instances. There are three possible outcomes from a 
// leader sending a heartbeat message to another server.
// 1) The leader discovers that the other server has a higher term than it does. It is actually a stale leader
// and should revert its state back to a follower.
// 2) The leader log doesn't match the follower's log. The leader should use the smart backtracking method outlined in 
// raft_append_entries and backup quickly. A message is sent via the heartbeatChan so that the discrepancy doesn't have to wait
// until the next heartbeat interval to continue resolution.
// 3) The leader and the follower match at the index. Both the matchIndex and nextIndex should be updated. The leader also
// checks if it can increment its commitIndex. If so, it should do so and apply messages to its state machine.
//
// RPC requests/responses can be sent out of order. Imagine the following scenario:
//
// Time 1: Leader S1 on T1 with follower S2. 
// Time 2: S1 sends two RPCs to S2, RPC1 and RPC2 in that order. S1 receives an entry between RPC1/2 
// so the matchIndex is x for RPC1 and x + 1 for RPC2.
// Time 3: RPC2 returns first, and S1 sets the matchIndex to x + 1.
// Time 4: RPC1 returns and S1 sets the matchIndex to x (effectively decrementing it)
// Time 5: S1 commits its entries up to x + 1, but informs other servers to commit their entries up to x on this heartbeat interval.
// Time 6: S2 is disconnected from the network, and S1 is effectively stuck at x if enough of the remaining servers fail.
//
// _Unlike_ sendRequestVoteTo which doesn't use a lock before sending the RPC, sendHeartbeatTo does to configure the entries logic.
// TODO: Optimize it so locking is not required.
func (rf *Raft) sendHeartbeatTo(index int, currentTerm int) {
	key := rand.Intn(1000)
	rf.mu.Lock()
	if rf.state == FOLLOWER {
		DPrintf(dHeart, "S%d T%d Leader now a follower %#v. ", rf.me, currentTerm, rf.prettyPrint())
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
	rf.mu.Unlock()

	args := AppendEntriesArgs{
		Term: currentTerm,
		LeaderId: rf.me,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm: prevLogTerm,
		Entries: entries,
		LeaderCommit: commitIndex,
	}
	reply := AppendEntriesReply{}
	DPrintf(dHeart, "S%d T%d Leader key %v sending args %#v to S%d.", rf.me, currentTerm, key, args, index)

	ok := rf.sendAppendEntries(index, &args, &reply)
	if !ok {
		DPrintf(dHeart, "S%d T%d Leader RPC failed for S%d.", rf.me, currentTerm, index)
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf(dHeart, "S%d T%d Leader key %v receiving reply %#v from S%d with %v.", rf.me, currentTerm, key, reply, index, rf.prettyPrint())
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
		go rf.sendOnHeartbeatChan(index)
	} else {
		DPrintf(dHeart, "S%d T%d Leader setting matchIndex for S%d with prevLogIndex %v entries %v", rf.me, currentTerm, index, prevLogIndex, len(entries))
		rf.nextIndex[index] = prevLogIndex + len(entries) + 1
		rf.matchIndex[index] = max(rf.matchIndex[index], prevLogIndex + len(entries)) // for out of order network requests, matchIndex can decrease
		rf.checkCommitIndex(index)
	}
}

func (rf *Raft) checkCommitIndex(index int) {
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
}

// It's possible for two sendApplyMsg invoications to overlap. That occurs if the leader receives two AppendEntries RPC responses
// and can increment its commitIndex both times. To solve that and a number of edge cases, this method:
// 1) Sets a boolean applyInProg that is only set to false after all the entries have been applied to the state machine
// 2) Checks that the term of the commitIndex is equal to the currentTerm to avoid the Figure 8 problem
// 3) Early exit if lastApplied == commitIndex (all the entries that should be applied have already been applied)
// 4) Early exit if commitIndex == -1 (there are no entries to apply)
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
