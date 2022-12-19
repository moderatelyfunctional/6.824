package raft

type Log struct {
	startIndex			int
	entries				[]Entry
}

type AppendCase string
const (
	APPEND_MISSING_ENTRY		AppendCase = "APPEND_MISSING_ENTRY"
	APPEND_CONFLICTING_ENTRY	AppendCase = "APPEND_CONFLICTING_ENTRY"

	APPEND_STALE_REQUEST		AppendCase = "APPEND_STALE_REQUEST"
	APPEND_REQUIRE_SNAPSHOT		AppendCase = "APPEND_REQUIRE_SNAPSHOT"

	APPEND_EMPTY				AppendCase = "APPEND_EMPTY"
	APPEND_ADD_ENTRIES			AppendCase = "APPEND_ADD_ENTRIES"
)

// Adjusts the startIndex. Can only increase the startIndex, otherwise results in a no-op.
// Should be called when the raft instance is alerted for log compaction.
func (log *Log) compactLog(index int) {
	if index <= log.startIndex || index > log.startIndex + len(log.entries) {
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

// Only within raft_start when the corresponding instance believes it's a leader. 
func (log *Log) appendEntry(entry Entry) {
	log.entries = append(log.entries, entry)
}

// Only called within the AppendEntries RPC handler (heartbeat messages) for instances receiving heartbeats.
func (log *Log) appendEntries(startIndex int, entries []Entry, currentTerm int) {
	// Do nothing since it's a stale request. Proof: startIndex is only set if the entries up from [0, startIndex)
	// can be snapshotted. Therefore, at that point the leader must have set its startIndex to >= log.startIndex
	// and any contradicting RPC must be from a outdated leader or the same leader, but delayed by a few terms. 
	if startIndex < log.startIndex {
		return
	}
	// Send message back to install a later snapshot. This scenario occurs when the leader asked the followers to 
	// snapshot up to an index, but some partitioned or slow rafts don't receive the message in time. This instance, 
	// on receiving the message should update its startIndex before the leader tries to establish consensus again.
	if startIndex > log.startIndex + len(log.entries) {
		return
	}

	if len(entries) == 0 {
		return
	}

	startIndex = startIndex - log.startIndex
	additionalIndex := startIndex + len(entries)
	additionalIndex = min(additionalIndex, len(log.entries))
	additionalEntries := log.entries[additionalIndex:]

	if len(additionalEntries) > 0 && additionalEntries[0].Term != currentTerm {
		additionalEntries = []Entry{}
	}

	log.entries = log.entries[:startIndex]
	log.entries = append(log.entries, entries...)
	log.entries = append(log.entries, additionalEntries...)
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