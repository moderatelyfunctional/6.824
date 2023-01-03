package raft

// import "fmt"

type Log struct {
	startIndex				int
	snapshotLogTerm			int			// snapshotLogTerm is set in compactLog and used in isMoreUpToDate when the log is empty.
	entries					[]Entry
}

type AppendCase string
const (
	APPEND_MISSING_ENTRY		AppendCase = "APPEND_MISSING_ENTRY"
	APPEND_CONFLICTING_ENTRY	AppendCase = "APPEND_CONFLICTING_ENTRY"

	APPEND_STALE_REQUEST		AppendCase = "APPEND_STALE_REQUEST"
	APPEND_REQUIRE_SNAPSHOT		AppendCase = "APPEND_REQUIRE_SNAPSHOT"

	APPEND_ADD_ENTRIES			AppendCase = "APPEND_ADD_ENTRIES"
)

// Compacts the log from [startIndex, compactIndex], compactIndex must be in the bounds
// [startIndex, startIndex + len(log.entries) - 1]. Otherwise this operation is a no-op.
//
// The compact index is always set equal to the commitIndex, so each instance should include the compactIndex
// into its snapshot. That's why after each compactLog operation, the startIndex should be compactIndex + 1.
// Otherwise, each instance will always snapshot *one fewer entry* than is possible.
func (log *Log) compactLog(compactIndex int) {
	if compactIndex < log.startIndex || compactIndex >= log.startIndex + len(log.entries) {
		return
	}

	// Nothing to compact.
	if len(log.entries) == 0 {
		return
	}

	snapshotLogTerm := log.entries[compactIndex - log.startIndex].Term
	log.entries = log.entries[compactIndex - log.startIndex + 1:]
	log.snapshotLogTerm = snapshotLogTerm
	log.startIndex = compactIndex + 1
}

// Only within raft_start when the corresponding instance believes it's a leader. 
func (log *Log) appendEntry(entry Entry) {
	log.entries = append(log.entries, entry)
}

// Should be called prior to appendEntries to ensure that the latter can be called safely.
func (log *Log) checkAppendCase(prevLogIndex int, prevLogTerm int, entries []Entry, isFirstIndex bool) AppendCase {
	// Do nothing since it's a stale request. Proof: the raft instance set its log to startIndex which means 
	// the entries are snapshotted from [0, startIndex - 1]. Therefore at that point the leader's prevLogIndex 
	// must be >= log.startIndex and any contradicting RPC must be from a outdated leader or the same leader, 
	// but delayed by a few terms. 
	if prevLogIndex < log.startIndex {
		return APPEND_STALE_REQUEST
	}

	// If the leader's prevLogIndex > log.startIndex + len(log.entries), there are two scenarios: 
	// 1) If it's the firstIndex the leader can't backup anymore and the follower should install a snapshot up to 
	// this index at which point the leader's heartbeat will succeed. This happens when some partitioned or slow rafts 
	// don't receive the message in time. This instance, on receiving the message should update its startIndex before 
	// the leader tries to establish consensus again.
	// 2) The leader can continue to backup, for which case the leader should figure out using the smart backup logic,
	// how far back it can set the prevLogIndex on the next heartbeat.
	if prevLogIndex >= log.startIndex + len(log.entries) {
		if isFirstIndex {
			return APPEND_REQUIRE_SNAPSHOT
		} else {
			return APPEND_MISSING_ENTRY	
		}
		
	}
	// The follower and leader could agree at this index/term. If so, the follower should append the leader's entries.
	// Otherwise, it should continue backing up.
	if log.entries[prevLogIndex - log.startIndex].Term != prevLogTerm {
		return APPEND_CONFLICTING_ENTRY
	} else {
		return APPEND_ADD_ENTRIES
	}
}

// Only called within the AppendEntries RPC handler (heartbeat messages) for instances receiving heartbeats matching
// the APPEND_ADD_ENTRIES case. It's important to note that entries could be empty.
func (log *Log) appendEntries(startIndex int, entries []Entry, currentTerm int) {
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

func (log *Log) isMoreUpToDate(otherLastLogIndex int, otherLastLogTerm int) bool {
	currentLastLogIndex := -1
	currentLastLogTerm := -1
	// The snapshotLogTerm is important when the raft log is empty but startIndex is non-zero.
	// That scenario occurs when the compactLog method is called as part of a snapshot operation.
	if len(log.entries) > 0 {
		currentLastLogIndex = log.startIndex + len(log.entries)
		currentLastLogTerm = log.entries[len(log.entries) - 1].Term
	} else {
		currentLastLogIndex = log.startIndex - 1
		currentLastLogTerm = log.snapshotLogTerm
	}

	// If the instance's last log term is higher than the other instance's, it's more up-to-date. 
	if currentLastLogTerm > otherLastLogTerm {
		return true
	}
	// If both instances have the same last log term, the instance must have a longer log to be more 
	// up-to-date.
	if currentLastLogTerm == otherLastLogTerm &&
	   currentLastLogIndex > otherLastLogIndex {
		return true
	}
	// The rest of the scenarios where the instance's log isn't as updated: 1) its last log term is lower
	// than the other instance's or 2) both instances have the same last log term, but the other last log
	// index >= current last log
	return false
}

func (log *Log) size() int {
	return log.startIndex + len(log.entries)
}

func (rf *Raft) isLogMoreUpToDate(otherLastLogIndex int, otherLastLogTerm int) bool {
	currentLastLogIndex := -1
	currentLastLogTerm := -1
	if len(rf.log.entries) > 0 {
		currentLastLogIndex = len(rf.log.entries) - 1
		currentLastLogTerm = rf.log.entries[currentLastLogIndex].Term
	}

	// If the instance's last log term is higher than the other instance's, it's more up-to-date. 
	if currentLastLogTerm > otherLastLogTerm {
		return true
	}
	// If both instances have the same last log term, the instance must have a longer log to be more 
	// up-to-date.
	if currentLastLogTerm == otherLastLogTerm &&
	   currentLastLogIndex > otherLastLogIndex {
		return true
	}
	// The rest of the scenarios where the instance's log isn't as updated: 1) its last log term is lower
	// than the other instance's or 2) both instances have the same last log term, but the other last log
	// index >= current last log
	return false
}


