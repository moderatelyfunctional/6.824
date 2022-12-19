package raft

type Log struct {
	startIndex			int
	entries				[]Entry
}

// Adjusts the startIndex. Can only increase the startIndex, otherwise results in a no-op.
// Should be called when the raft instance is alerted for log compaction.
func (log *Log) setStartIndex(index int) {
	if index <= log.startIndex {
		return
	}

	log.entries = log.entries[index - log.startIndex:]
	log.startIndex = index
}

// Checks whether the log entry at the prevLogIndex/Term is equal to the specified input.
func (log *Log) checkEntry(prevLogIndex int, prevLogTerm int) bool {
	// indicates the prevLogIndex is invalid given the log which occurs when:
	// 1) prevLogIndex < log.startIndex which means the all the entries in the log are conflicting 
	// and the leader moved its prevLogIndex to a prior point than the starting log index. The leader
	// should now send an InstallSnapshotRPC to this instance to update its log.
	// 2) prevLogIndex >= log.startIndex + len(log.entries) which indicates the leader has a lot more
	// entries than exist in the log. The leader should then decrement its prevLogIndex to check
	// whether that equals the corresponding entry in this raft instance.
	if prevLogIndex < log.startIndex || prevLogIndex >= log.startIndex + len(log.entries) {
		return false
	}

	return log.entries[prevLogIndex - log.startIndex].Term == prevLogTerm
} 

// Appends the series of values into the log. The operation is a no-op if values is empty (for heartbeat messages).
func (log *Log) append(startIndex int, values []Entry) {
	if len(values) == 0 {
		return
	}

	log.entries = append(log.entries, values...)
}

// Removes the conflicting entries starting at the specified index from the log.
func (log *Log) remove(index int) {
	if index < log.startIndex {
		return
	}
	log.entries = log.entries[index - log.startIndex - 1:]
}

// Returns a subarray of the elements from indices i such that startIndex <= i <= endIndex.
func (log *Log) subarray(startIndex int, endIndex int, shouldCopy bool) []Entry {
	logSubslice := log.entries[startIndex - log.startIndex : endIndex + 1 - log.startIndex]
	// Returning a slice is acceptable, such as for the sendApplyMsg method
	if !shouldCopy {
		return logSubslice
	}

	logSubarray := make([]Entry, endIndex - startIndex + 1)
	copy(logSubarray, logSubslice)
	return logSubarray
}

// func (log *Log) isMoreUpToDate(otherLastLogIndex int, otherLastLogTerm int) bool {
// 	if len(log.entries) == 0 {
// 		return 
// 	}
// }

func (rf *Raft) isLogMoreUpToDate(otherLastLogIndex int, otherLastLogTerm int) bool {
	currentLastLogIndex := -1
	currentLastLogTerm := -1
	if len(rf.log) > 0 {
		currentLastLogIndex = len(rf.log) - 1
		currentLastLogTerm = rf.log[currentLastLogIndex].Term
	}

	// if the instance's last log term is higher than the other instance's, it's more up-to-date. 
	if currentLastLogTerm > otherLastLogTerm {
		return true
	}
	// if both instances have the same last log term, the instance must have a longer log to be more 
	// up-to-date.
	if currentLastLogTerm == otherLastLogTerm &&
	   currentLastLogIndex > otherLastLogIndex {
		return true
	}
	// the rest of the scenarios where the instance's log isn't as updated: 1) its last log term is lower
	// than the other instance's or 2) both instances have the same last log term, but the other last log
	// index >= current last log
	return false
}