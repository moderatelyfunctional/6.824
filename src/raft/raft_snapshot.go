package raft

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
//

// import "fmt"

type ApplyMsg struct {
	CommandValid	bool
	Command			interface{}
	CommandIndex	int

	// For 2D:
	SnapshotValid	bool
	Snapshot		[]byte
	SnapshotTerm	int
	SnapshotIndex	int
}

type InstallSnapshotArgs struct {
	Term			int
	SnapshotTerm	int
	SnapshotIndex	int
	Snapshot		[]byte
}

// The SnapshotIndex is important if Success = False because it's possible for the leader to be *STUCK* sending
// InstallSnapshotRPCs to the follower. Imagine the following scenario:
// T1: The leader sends InstallSnapshotRPC1 to the follower, but the response is DROPPED. 
// T2: The follower sends an ApplyMsg to the service layer and installs the new snapshot.
// T3: The leader sends InstallSnapshotRPC2, but it's a stale request since the log startIndex = SnapshotIndex + 1
// and the snapshot is now redundant.
// T4: The leader cannot handle this because currently a Success = False scenario doesn't require updating the 
// nextIndex/matchIndex for the follower, but it does require that.
// T5 [ADDED]: Check SnapshotIndex on Success = False, and increment the nextIndex/matchIndex for the follower
// if the updated values would be higher than the existing values.
type InstallSnapshotReply struct {
	Term			int
	SnapshotIndex	int
	Success			bool
}

//
// A service wants to switch to snapshot.  Only do so if Raft doesn't
// have more recent info since it communicated the snapshot on applyCh.
//
// If the snapshot request is valid, update lastApplied and commitIndex since the
// service layer will already have the log entries from a leader snapshot some time ago. 
//
// Since lastIncludedIndex is 1-indexed from the service layer, subtract 1
// to make it 0-indexed which raft expects.
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {
	// Your code here (2D).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf(dSnap, "S%d T%d conditionally installing snapshot %d %d with log %#v.", 
		rf.me, rf.currentTerm, lastIncludedIndex, lastIncludedTerm, rf.log)

	lastIncludedIndex = lastIncludedIndex - 1
	rf.lastApplied = lastIncludedIndex
	rf.commitIndex = lastIncludedIndex

	shouldSnapshot := rf.log.snapshot(lastIncludedTerm, lastIncludedIndex)
	if shouldSnapshot {
		state := rf.encodeState()
		rf.persister.SaveStateAndSnapshot(state, snapshot)
		rf.setSnapshotInProg(false)
	}

	return shouldSnapshot
}

// The service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
//
// The index here is 1-indexed, while our raft implementation is 0-indexed.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf(dSnap, "S%d T%d installing snapshot with index %v with log %#v.", 
		rf.me, rf.currentTerm, index, rf.log)

	index = index - 1
	rf.log.compact(index)
	state := rf.encodeState()
	rf.persister.SaveStateAndSnapshot(state, snapshot)
}

// There is a discrepancy between the raft log (0-indexed) and the service log (1-indexed). 
// The log snapshot method is within the raft domain so nothing should be done to snapshotIndex,
// but when sending the value to the service layer, ApplyMsg must set SnapshotIndex to args.SnapshotIndex + 1.
func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	DPrintf(dSnap, "%v with args %#v", rf.prettyPrint(), args)
	if rf.currentTerm > args.Term {
		reply.Term = rf.currentTerm
		reply.SnapshotIndex = -1 // The snapshotIndex doesn't matter if the follower is a higher term than the leader.
		reply.Success = false
		return
	}
	rf.heartbeat = true
	if rf.isLowerTerm(args.Term) {
		rf.setStateToFollower(args.Term)
	}
	// Early exit if the snapshot term/index already exists in the follower or if a snapshot operation is already underway.
	// This covers Case 1 and 3 of compactSnapshot. For more info refer to log.compactSnapshot.
	canSnapshot, snapshotIndex := rf.log.canSnapshot(args.SnapshotTerm, args.SnapshotIndex)
	if !canSnapshot || rf.isSnapshotInProg() {
		reply.Term = rf.currentTerm
		reply.SnapshotIndex = snapshotIndex
		reply.Success = false
		return
	}
	rf.setSnapshotInProg(true)
	go func() {
		applyMsg := ApplyMsg{
			SnapshotValid: true,
			Snapshot: args.Snapshot,
			SnapshotTerm: args.SnapshotTerm,
			SnapshotIndex: args.SnapshotIndex + 1, // convert 0-index to 1-index
		}
		rf.applyCh<-applyMsg
	}()
	reply.Term = rf.currentTerm
	reply.SnapshotIndex = args.SnapshotIndex
	reply.Success = true
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}


