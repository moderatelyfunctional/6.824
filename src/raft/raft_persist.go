package raft

import "bytes"
import "6.824/labgob"

func (rf *Raft) encodeState() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votesReceived)
	e.Encode(rf.votedFor)
	e.Encode(rf.log.startIndex)
	e.Encode(rf.log.snapshotTerm)
	e.Encode(rf.log.snapshotIndex)
	e.Encode(rf.log.entries)
	return w.Bytes()
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	data := rf.encodeState()
	rf.persister.SaveRaftState(data)
}


//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var votesReceived []int
	var votedFor int
	var logStartIndex int
	var logSnapshotTerm int
	var logSnapshotIndex int
	var logEntries []Entry
	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votesReceived) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&logStartIndex) != nil ||
		d.Decode(&logSnapshotTerm) != nil ||
		d.Decode(&logSnapshotIndex) != nil ||
		d.Decode(&logEntries) != nil {
		DPrintf(dError, "Reading data for S%d on T%d", rf.me, currentTerm)
	} else {
		rf.currentTerm = currentTerm
		rf.votesReceived = votesReceived
		rf.votedFor = votedFor
		rf.lastApplied = logSnapshotIndex
		rf.commitIndex = logSnapshotIndex
		rf.log = makeLogFromSnapshot(logStartIndex, logSnapshotTerm, logSnapshotIndex, logEntries)
	}
}




