package raft

// import "fmt"
import "time"
import "sort"
import "math/rand"

// This is a no-op for raft instances in a follower or candidate state. For instances in a leader state, 
// empty AppendEntries RPCs are sent to the other instances. If any RPC reply return a term > that of the
// leader, the leader acks that it is not a legitimate leader, and converts to a follower.
//
// The leader may send either a AppendEntriesRPC or an InstallSnapshotRPC to the follower depending on whether
// the follower's log contains the specified prevLogIndex (nextIndex - 1). This can be indirectly checked via 
// the leader's snapshot entry (specifically snapshotIndex).
// The snapshotIndex is the _maximum index_ which has been snapshotted via the service code invoication 
// (raft.Snapshot(...)). So any followers whose nextIndex is equal to the snapshotIndex must install the leader's
// most recent snapshot.
//
// TODO: Followers don't need to sendHeartbeat. If they're turned off, that could improve performance since locking
// is not required. Figure out how to start ticker on switch to LEADER, and stop ticker on switch to FOLLOWER.
//
// TODO: Once the TODO above is complete, there is no need to lock _twice_ per RPC call. Switch now to locking once 
// before sending the heartbeat RPCs.
func (rf *Raft) sendHeartbeat() {
	rf.mu.Lock()
	state := rf.state
	if state != LEADER {
		rf.mu.Unlock()
		return
	}
	currentTerm := rf.currentTerm

	shouldSnapshot := make([]bool, len(rf.peers))
	snapshot := rf.persister.ReadSnapshot()
	snapshotTerm, snapshotIndex := rf.log.snapshotEntryInfo()
	for i := 0; i < len(rf.nextIndex); i++ {
		shouldSnapshot[i] = rf.nextIndex[i] <= snapshotIndex
	}
	rf.mu.Unlock()

	DPrintf(dHeart, "S%d T%d Leader, sending heartbeats", rf.me, currentTerm)
	for i := 0; i < len(rf.peers); i++ {
		if rf.me == i {
			continue
		}
		if shouldSnapshot[i] {
			go rf.sendInstallSnapshotTo(i, currentTerm, snapshotTerm, snapshotIndex, snapshot)
		} else {
			go rf.sendHeartbeatTo(i, currentTerm)
		}
	}
}

func (rf *Raft) sendOnHeartbeatChan(index int) {
	if rf.killed() {
		return
	}
	rf.heartbeatChan<-index
}

// TODO: Optimize by removing the lock here and read the currentTerm in the sendHeartbeatTo body
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
// T1: Leader S1 on T1 with follower S2. 
// T2: S1 sends two RPCs to S2, RPC1 and RPC2 in that order. S1 receives an entry between RPC1/2 
// so the matchIndex is x for RPC1 and x + 1 for RPC2.
// T3: RPC2 returns first, and S1 sets the matchIndex to x + 1.
// T4: RPC1 returns and S1 sets the matchIndex to x (effectively decrementing it)
// T5: S1 commits its entries up to x + 1, but informs other servers to commit their entries up to x on this heartbeat interval.
// T6: S2 is disconnected from the network, and S1 is effectively stuck at x if enough of the remaining servers fail.
//
// While nextIndex can decrease or increase based on leader follower RPC interactions, the matchIndex is a _monotonically_
// increasing value since once an entry is committed it can never be uncommitted. That's why the max() operation is used
// to ensure that property.
//
// _Unlike_ sendRequestVoteTo which doesn't use a lock before sending the RPC, sendHeartbeatTo does to configure the entries logic.
// TODO: Optimize it so locking is not required.
func (rf *Raft) sendHeartbeatTo(index int, currentTerm int) {
	rpcKey := rand.Intn(1000)
	rf.mu.Lock()
	if rf.state == FOLLOWER {
		DPrintf(dHeart, "S%d T%d Leader now a follower %#v. ", rf.me, currentTerm, rf.prettyPrint())
		rf.mu.Unlock()
		return
	}
	// The application state can change drastically between lock statements. So between sendHeartbeat and sendHeartbeatTo,
	// if there is a call to rf.Snapshot(...), then the snapshot index might have been incremented. In that case, a previously
	// valid nextIndex is now too far behind the leader's log and the follower should receive a InstallSnapshotRPC at the next
	// heartbeat interval.
	_, snapshotIndex := rf.log.snapshotEntryInfo()
	if rf.nextIndex[index] <= snapshotIndex {
		rf.mu.Unlock()
		return
	}

	DPrintf(dHeart, "S%d T%d Leader R%d, %#v. ", rf.me, currentTerm, index, rf.prettyPrint())
	commitIndex := rf.commitIndex
	var prevLogIndex, prevLogTerm int
	entries := rf.log.copyEntries(rf.nextIndex[index])
	if rf.nextIndex[index] > 0 {
		prevLogIndex = rf.nextIndex[index] - 1
		prevLogTerm = rf.log.entry(prevLogIndex).Term
	} else {
		prevLogIndex = -1
		prevLogTerm = -1
	}
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
	DPrintf(dHeart, "S%d T%d Leader key %v sending args %#v to S%d.", rf.me, currentTerm, rpcKey, args, index)

	ok := rf.sendAppendEntries(index, &args, &reply)
	if !ok {
		DPrintf(dHeart, "S%d T%d Leader RPC failed for S%d.", rf.me, currentTerm, index)
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf(dHeart, "S%d T%d Leader key %v receiving reply %#v from S%d with %v.", rf.me, currentTerm, rpcKey, reply, index, rf.prettyPrint())
	if rf.state == FOLLOWER {
		DPrintf(dHeart, "S%d T%d Leader already set to follower %#v. ", rf.me, currentTerm, reply)
		return
	} 
	if currentTerm < reply.Term {
		DPrintf(dHeart, "S%d T%d Leader resetting to follower %#v. ", rf.me, currentTerm, reply)
		rf.setStateToFollower(reply.Term)
		return
	}
	if !reply.Success {
		DPrintf(dHeart, "S%d T%d Leader decrementing nextIndex for S%d", rf.me, currentTerm, index)
		var newIndex int
		if reply.XIndex == -1 {
			// Case 3, Follower is missing an entry at prevLogIndex
			newIndex = reply.XLen
		} else {
			// Case 1, 2
			firstIndexForTerm := -1
			for i := prevLogIndex; i >= 0; i-- {
				if rf.log.entries[i].Term == reply.XTerm {
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
		rf.checkCommitIndex()
	}
}

func (rf *Raft) sendInstallSnapshotTo(index int, currentTerm int, snapshotTerm int, snapshotIndex int, snapshot []byte) {
	rpcKey := rand.Intn(1000)
	args := InstallSnapshotArgs{
		Term: currentTerm,
		SnapshotTerm: snapshotTerm,
		SnapshotIndex: snapshotIndex,
		Snapshot: snapshot,
	}
	reply := InstallSnapshotReply{}
	DPrintf(dSnap, "S%d T%d Leader key %v sending args %#v to S%d.", rf.me, currentTerm, rpcKey, args, index)

	ok := rf.sendInstallSnapshot(index, &args, &reply)
	if !ok {
		DPrintf(dSnap, "S%d T%d Leader RPC failed for S%d.", rf.me, currentTerm, index)
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf(dSnap, "S%d T%d Leader key %v receiving reply %#v from S%d with %v.", rf.me, currentTerm, rpcKey, reply, index, rf.prettyPrint())
	if rf.state == FOLLOWER {
		DPrintf(dSnap, "S%d T%d Leader already set to follower %#v. ", rf.me, currentTerm, reply)
		return
	}
	if currentTerm < reply.Term {
		DPrintf(dSnap, "S%d T%d Leader resetting to follower %#v. ", rf.me, currentTerm, reply)
		rf.setStateToFollower(reply.Term)
		return
	}
	if reply.Success {
		rf.nextIndex[index] = snapshotIndex + 1
		rf.matchIndex[index] = max(rf.matchIndex[index], snapshotIndex)
		rf.checkCommitIndex()
		// go rf.sendOnHeartbeatChan(index) 
	} else {
		// On an InstallSnapshotRPC failure, check if the nextIndex/matchIndex can be updated. It's possible the leader is STUCK 
		// sending InstallSnapshotRPCs because the response for the very first request was dropped, causing the leader to not
		// update the nextIndex/matchIndex. Responses for subsequent requests will fail since they are technically stale, and the
		// the leader does nothing because it assumes in parallel it is sending AppendEntriesRPC to the follower that would update 
		// the nextIndex/matchIndex. Refer to raft_snapshot.go for more details.
		rf.nextIndex[index] = max(rf.nextIndex[index], reply.SnapshotIndex + 1)
		rf.matchIndex[index] = max(rf.matchIndex[index], reply.SnapshotIndex)
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
	if possibleCommitIndex <= rf.commitIndex || rf.log.entry(possibleCommitIndex).Term != rf.currentTerm {
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
	DPrintf(dApply, "HUHHH", rf.killed())
	if rf.killed() {
		return
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()

	DPrintf(dApply, rf.prettyPrint())
	if rf.commitIndex == -1 ||
	   rf.log.entry(rf.commitIndex).Term != rf.currentTerm ||
	   rf.lastApplied == rf.commitIndex {
		return
	}
	// It's possible for there to be two overlapping sendApplyMsg where the second one isn't redundant. If so, delay one
	// and try it at some later point in time. Redundancy is defined as an invocation where lastApplied == commitIndex 
	// (where no work should be done) or if the term of the index to commit isn't equal to the current term.
	if rf.isApplyInProg() {
		go func() {
			time.Sleep(time.Duration(APPLY_MSG_INTERVAL_MS) * time.Millisecond)
			rf.sendApplyMsg()
		}()
		return
	}

	rf.setApplyInProg(true)
	nextApplyIndex := rf.lastApplied + 1
	commitToIndex := rf.commitIndex + 1

	// The log entry at lastApplied is already sent via the applyCh, so start at lastApplied + 1.
	// commitToIndex = commitIndex + 1 because commitIndex needs to be included because the log 
	// entry at that index isn't applied yet.
	logSubset := rf.log.copyEntriesInRange(nextApplyIndex, commitToIndex)
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
		rf.setApplyInProg(false)
	}(nextApplyIndex, logSubset)
}
