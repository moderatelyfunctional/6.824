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
	SnapshotTerm	int
	SnapshotIndex	int
	Snapshot		[]byte
}

type InstallSnapshotReply struct {
	Success			bool
}

//
// A service wants to switch to snapshot.  Only do so if Raft doesn't
// have more recent info since it communicated the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {
	// Your code here (2D).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	return rf.log.snapshot(lastIncludedTerm, lastIncludedIndex)
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.log.compact(index)
	state := rf.encodeState()
	rf.persister.SaveStateAndSnapshot(state, snapshot)
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// Early exit if the snapshot term/index already exists in the follower. This covers Case 1 and 3 of compactSnapshot.
	// For more info refer to log.compactSnapshot.
	canSnapshot := rf.log.canSnapshot(args.snapshotLogTerm, args.snapshotLogIndex)
	if !canSnapshot {
		reply.Success = false
		return
	}
	go func() {
		applyMsg := ApplyMsg{
			SnapshotValid: true,
			Snapshot: args.Snapshot,
			SnapshotTerm: args.SnapshotTerm,
			SnapshotIndex: args.SnapshotIndex,
		}
		rf.applyCh<-applyMsg
	}()
	reply.Success = true
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}


