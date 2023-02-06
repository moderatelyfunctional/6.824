package raft

// import "fmt"
import "reflect"

type Log struct {
	startIndex				int
	snapshotTerm			int			// snapshotTerm is set in compactLog and used in isMoreUpToDate when the log is empty.
	snapshotIndex			int			// snapshotIndex = startIndex - 1
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

func makeLog(entries []Entry) *Log {
	return &Log{
		startIndex: 0,
		snapshotTerm: -1,
		snapshotIndex: -1,
		entries: entries,
	}
}

// startIndex must always be equal to snapshotIndex + 1
func makeLogFromSnapshot(startIndex int, snapshotTerm int, snapshotIndex int, entries []Entry) *Log {
	return &Log{
		startIndex: startIndex,
		snapshotTerm: snapshotTerm,
		snapshotIndex: snapshotIndex,
		entries: entries,
	}
}

// Compacts the log from [startIndex, compactIndex], compactIndex must be in the bounds
// [startIndex, startIndex + len(log.entries) - 1]. Otherwise this operation is a no-op.
//
// compactIndex <= commitIndex, so the compactIndex should also be included in the snapshot.
// That's why after each compactLog operation, the startIndex should be compactIndex + 1.
// Otherwise, each instance will always snapshot *one fewer entry* than is possible.
//
// This method should only be used within the context of a raft instance incrementing its
// applied index (through sendApplyMsg) because every applied entry by definition is committed and
// is compactible.
func (log *Log) compact(compactIndex int) {
	if compactIndex < log.startIndex || compactIndex > log.size() {
		return
	}
	if len(log.entries) == 0 {
		return
	}

	snapshotTerm := log.entries[compactIndex - log.startIndex].Term
	log.entries = log.entries[compactIndex - log.startIndex + 1:]
	log.startIndex = compactIndex + 1
	log.snapshotTerm = snapshotTerm
	log.snapshotIndex = log.startIndex - 1
}

// Compacts the log as a consequence of receiving an InstallSnapshotRPC from a presumed leader.
// There are a few possible scenarios:
// Case 1: Stale InstallSnapshotRPC --> compactIndex < log.startIndex 
// 		- Occurs when an unreliable network sent out two distinct InstallSnapshotRPCs with different snapshot indices
//        and the _later_ one arrived first to the follower. Then the earlier one arrived and nothing should be done.
// 		- Return FALSE
// Case 2: Complete InstallSnapshotRPC --> compactIndex > log.size()
// 		- Occurs in typical operation when the leader discovers it needs to backup its nextIndex for the follower 
//        before its log startIndex. The entire follower log struct is obsolete and should be reset: delete entries, 
// 		  update startIndex/snapshotLog(Term|Index).
// 		- Return TRUE
// Case 3: Redundant InstallSnapshotRPC --> entry.Term == snapshotLastTerm
// 		- Occurs when an unreliable network sent out two identical InstallSnapshotRPCs. No work should be done since
//		  the one that arrived first will have done all the work by the time the second arrives.
// 		- Return FALSE
// Case 4: Partial InstallSnapshotRPC --> (entry.Term != snapshotLastTerm) AND entries exist after snapshotLastIndex
// 		- Occurs in typical operation (same as above). However, there are entries _after_ the snapshotLastIndex.
// 		  The same rules for additional committed/uncommitted entries for AppendEntriesRPC should be applied here.
//
// 		  If the term of the first entry after the snapshotLastIndex >= snapshotLastTerm, keep the entries because 
// 		  there is a chance they can be committed later. Otherwise if the term < snapshotLastTerm, the entries are
// 		  from an earlier term that can never be committed since all snapshots are committed, and the server would
// 		  never be able to receive a majority of votes from the cluster. So delete the entries.
//		- Return TRUE
func (log *Log) snapshot(snapshotLastTerm int, snapshotLastIndex int) bool {
	// Case 1
	if snapshotLastIndex < log.startIndex {
		return false
	}
	// Case 2
	if snapshotLastIndex >= log.size() {
		log.startIndex = snapshotLastIndex + 1
		log.snapshotTerm = snapshotLastTerm
		log.snapshotIndex = snapshotLastIndex
		log.entries = []Entry{}
		return true
	}
	// Case 3, 4
	entry := log.entry(snapshotLastIndex)
	if entry.Term == snapshotLastTerm {
		return false
	}
	// The log.startIndex can't be modified until the end of the method because log.size() and log.entry()
	// require it to work.
	nextStartIndex := snapshotLastIndex + 1
	log.snapshotTerm = snapshotLastTerm
	log.snapshotIndex = snapshotLastIndex

	// If startIndex exceeds the final entry index, remove all the entries.
	if nextStartIndex >= log.size() {
		log.entries = []Entry{}
	} else if log.entry(nextStartIndex).Term < snapshotLastTerm {
		log.entries = []Entry{}
	} else {
		// without explicit copying, Go wont do garbage collection on the original log.entries. The math here is
		// a bit involved. The number of entries is log.startIndex + len(log.entries) - nextStartIndex
		// From log.entries, we want to copy [nextStartIndex - log.startIndex, len(log.entries) - 1], which includes
		// len(log.entries) - 1 - (nextStartIndex - log.startIndex) + 1 values which reduces down to the same value
		entries := make([]Entry, log.size() - nextStartIndex)
		copy(entries, log.entries[nextStartIndex - log.startIndex:])
		log.entries = entries
	}
	log.startIndex = nextStartIndex
	return true
}

func (log *Log) canSnapshot(snapshotLastTerm int, snapshotLastIndex int) (bool, int) {
	// Case 1
	if log.startIndex > snapshotLastIndex {
		return false, log.snapshotIndex
	}
	if snapshotLastIndex >= log.size() {
		return true, log.snapshotIndex
	}
	// Case 3, 4
	entry := log.entry(snapshotLastIndex)
	return entry.Term != snapshotLastTerm, log.snapshotIndex
}

// Only within raft_start when the corresponding instance believes it's a leader. 
func (log *Log) appendEntry(entry Entry) {
	log.entries = append(log.entries, entry)
}

// Should be called prior to appendEntries to ensure that the latter can be called safely.
func (log *Log) checkAppendCase(prevIndex int, prevTerm int, entries []Entry, isFirstIndex bool) AppendCase {
	// Do nothing since it's a stale request. Proof: the raft instance set its log to startIndex which means 
	// the entries are snapshotted from [0, startIndex - 1]. Therefore at that point the leader's prevIndex 
	// must be >= log.startIndex and any contradicting RPC must be from a outdated leader or the same leader, 
	// but delayed by a few terms. 
	if prevIndex < log.startIndex {
		return APPEND_STALE_REQUEST
	}

	// If the leader's prevIndex > log.startIndex + len(log.entries), there are two scenarios: 
	// 1) If it's the firstIndex the leader can't backup anymore and the follower should install a snapshot up to 
	// this index at which point the leader's heartbeat will succeed. This happens when some partitioned or slow rafts 
	// don't receive the message in time. This instance, on receiving the message should update its startIndex before 
	// the leader tries to establish consensus again.
	// 2) The leader can continue to backup, for which case the leader should figure out using the smart backup logic,
	// how far back it can set the prevIndex on the next heartbeat.
	if prevIndex >= log.startIndex + len(log.entries) {
		if isFirstIndex {
			return APPEND_REQUIRE_SNAPSHOT
		} else {
			return APPEND_MISSING_ENTRY	
		}
		
	}
	// The follower and leader could agree at this index/term. If so, the follower should append the leader's entries.
	// Otherwise, it should continue backing up.
	if log.entries[prevIndex - log.startIndex].Term != prevTerm {
		return APPEND_CONFLICTING_ENTRY
	} else {
		return APPEND_ADD_ENTRIES
	}
}

// Only called within the AppendEntries RPC handler (heartbeat messages) for instances receiving heartbeats matching
// the APPEND_ADD_ENTRIES case. It's important to note that entries could be empty.
func (log *Log) appendEntries(startIndex int, entries []Entry, currentTerm int) {
	if startIndex < log.startIndex {
		return
	}
	// additionalIndex must be bound between [0, len(log.entries)]. It is always >= 0 since startIndex >= 0, len(entries) >= 0.
	// However, it can be larger than len(log.entries) so it must be bounded between min(additionalIndex, len(log.entries)).
	//
	// If it's larger than len(log.entries), then there are no additional entries to consider since the new entries will
	// overwrite everything in the raft instance's entries.
	startIndex = startIndex - log.startIndex
	additionalIndex := startIndex + len(entries)
	additionalIndex = min(additionalIndex, len(log.entries))
	additionalEntries := log.entries[additionalIndex:]

	// If the raft's additional entries are from a previous term, these stale entries will never be committed by the 
	// log comparison rules (since a majority of servers will have a log whose last entry is of a higher term). It's
	// safe to delete them.
	if len(additionalEntries) > 0 && additionalEntries[0].Term != currentTerm {
		additionalEntries = []Entry{}
	}

	log.entries = log.entries[:startIndex]
	log.entries = append(log.entries, entries...)
	log.entries = append(log.entries, additionalEntries...)
}

func (log *Log) isMoreUpToDate(otherLastIndex int, otherLastTerm int) bool {
	currentLastIndex, currentLastTerm := log.lastEntryInfo()

	// If the instance's last log term is higher than the other instance's, it's more up-to-date. 
	if currentLastTerm > otherLastTerm {
		return true
	}
	// If both instances have the same last log term, the instance must have a longer log to be more 
	// up-to-date.
	if currentLastTerm == otherLastTerm &&
	   currentLastIndex > otherLastIndex {
		return true
	}
	// The rest of the scenarios where the instance's log isn't as updated: 1) its last log term is lower
	// than the other instance's or 2) both instances have the same last log term, but the other last log
	// index >= current last log
	return false
}

// Size is not affected by snapshotting because it includes the startIndex. The philosophy here is for Raft
// instances, the burden of snapshotting/compaction rests entirely within this struct.
func (log *Log) size() int {
	return log.startIndex + len(log.entries)
}

// Entries should only be accessed via the helper method and not directly via dot notation.
func (log *Log) entry(entryIndex int) Entry {
	// It's possible for the client to index for an entryIndex which doesn't exist in log.entries but 
	// is actually snapshotted. 
	if entryIndex == log.snapshotIndex {
		return Entry{Term: log.snapshotTerm, Command: "",}
	}
	return log.entries[entryIndex - log.startIndex]
}

// Returns the index and term of the last entry, not the term and command.
func (log *Log) lastEntryInfo() (int, int) {
	currentLastIndex := -1
	currentLastTerm := -1
	// The snapshotTerm is important when the raft log is empty but startIndex is non-zero.
	// That scenario occurs when the compactLog method is called as part of a snapshot operation
	// and no new entries have been been added to the log (so the entries is now empty).
	if len(log.entries) > 0 {
		currentLastIndex = log.startIndex + len(log.entries) - 1
		currentLastTerm = log.entries[len(log.entries) - 1].Term
	} else {
		currentLastIndex = log.startIndex - 1
		currentLastTerm = log.snapshotTerm
	}

	return currentLastIndex, currentLastTerm
}

// Returns the index and term of the last snapshot entry, not the term and command.
func (log *Log) snapshotEntryInfo() (int, int) {
	return log.snapshotTerm, log.snapshotIndex
}

func (log *Log) copyOf() *Log {
	copyEntries := make([]Entry, len(log.entries))
	copy(copyEntries, log.entries)
	return &Log{
		startIndex: log.startIndex,
		snapshotTerm: log.snapshotTerm,
		snapshotIndex: log.snapshotIndex,
		entries: copyEntries,
	}
}

func (log *Log) copyEntries(beginIndex int) []Entry {
	copyStartIndex := beginIndex - log.startIndex

	copyEntries := make([]Entry, len(log.entries) - copyStartIndex)
	copy(copyEntries, log.entries[copyStartIndex:])
	return copyEntries
}

func (log *Log) copyEntriesInRange(beginIndex int, endIndex int) []Entry {
	copyStartIndex := beginIndex - log.startIndex
	copyEndIndex := endIndex - log.startIndex

	copyEntries := make([]Entry, endIndex - beginIndex)
	copy(copyEntries, log.entries[copyStartIndex:copyEndIndex])
	return copyEntries
}

func (log *Log) isEqual(otherLog *Log, checkEntries bool) bool {
	return log.startIndex == otherLog.startIndex &&
		   log.snapshotTerm == otherLog.snapshotTerm &&
		   log.snapshotIndex == otherLog.snapshotIndex &&
		   (!checkEntries || reflect.DeepEqual(log.entries, otherLog.entries))
}

