package raft

import "time"
import "math/rand"

func (rf *Raft) startElectionCountdown(electionTimeout int, currentTerm int) {
	time.Sleep(time.Duration(electionTimeout) * time.Millisecond)
	rf.electionChan<-currentTerm
}

func (rf *Raft) checkElectionTimeout(timeoutTerm int) {
	rf.mu.Lock()
	state := rf.state
	heartbeat := rf.heartbeat
	currentTerm := rf.currentTerm
	rf.mu.Unlock()
	DPrintf(dTimer, "S%d T%d checking election timeout", rf.me, currentTerm)

	// Two cases to consider here:
	// 1) If the instance is a leader, there is no need to start another leader election.
	// 2) If timeout term < currentTerm, that indicates the instance recently incremented its term 
	// and so this electionTimeout is outdated. There will be _another_ timeout set by in the raft state
	// methods. That one should have its timeoutTerm = currentTerm.
	if state == LEADER || timeoutTerm < currentTerm {
		return
	}
	// if the raft instance (follower) received a heartbeat, then reset the election countdown to check
	// that the leader is available at a later time.
	if heartbeat {
		DPrintf(dElection, "S%d T%d received heartbeat, resetting election timeout. ", rf.me, currentTerm)
		electionTimeout := ELECTION_TIMEOUT_MIN_MS + rand.Intn(ELECTION_TIMEOUT_SPREAD_MS)
		rf.mu.Lock()
		rf.electionTimeout = electionTimeout
		rf.heartbeat = false
		rf.mu.Unlock()

		go rf.startElectionCountdown(electionTimeout, currentTerm)
		return
	}

	DPrintf(dElection, "S%d T%d didn't received heartbeat, starting new election. ", rf.me, currentTerm)
	rf.startElection()
}

func (rf *Raft) startElection() {
	rf.mu.Lock()
	me := rf.me
	rf.setStateToCandidate()
	currentTerm := rf.currentTerm
	lastLogIndex := -1
	lastLogTerm := -1
	if len(rf.log) != 0 {
		lastLogIndex = len(rf.log) - 1
		lastLogTerm = rf.log[lastLogIndex].term
	}
	rf.mu.Unlock()

	for i := 0; i < len(rf.peers); i++ {
		if rf.me == i {
			continue
		}
		go rf.requestVoteTo(i, currentTerm, lastLogIndex, lastLogTerm, me)
	}
}

func (rf *Raft) requestVoteTo(index int, currentTerm int, lastLogIndex int, lastLogTerm int, me int) {
	args := RequestVoteArgs{
		Term: currentTerm,
		CandidateId: me,
		LastLogIndex: lastLogIndex,
		LastLogTerm: lastLogTerm,
	}
	reply := RequestVoteReply{}
	rf.sendRequestVote(index, &args, &reply)

	if currentTerm < reply.Term {
		DPrintf(dVote, "S%d T%d found S%d with higher term. Setting state from candidate to follower", rf.me, index, currentTerm)
		rf.mu.Lock()
		defer rf.mu.Unlock()
		rf.setStateToFollower(reply.Term)
		return
	}

	if reply.VoteGranted {
		rf.mu.Lock()
		defer rf.mu.Unlock()
		rf.votesReceived += 1

		DPrintf(dVote, "S%d T%d (%d/%d votes) received vote from S%d", rf.me, currentTerm, rf.votesReceived, len(rf.peers), index)
		if rf.votesReceived * 2 > len(rf.peers) {
			DPrintf(dVote, "S%d T%d set state to leader.", rf.me, currentTerm)
			rf.setStateToLeader()
		}
	}

}

// Method is a no-op for raft instances in a follower or candidate state. For instances in a leader state, 
// empty AppendEntries RPCs are sent to the other instances. If any RPC reply return a term > that of the
// leader, the leader acks that it is not a legitimate leader, and converts to a follower.
func (rf *Raft) sendHeartbeat() {
	rf.mu.Lock()
	state := rf.state
	currentTerm := rf.currentTerm
	me := rf.me
	rf.mu.Unlock()

	if state != LEADER {
		return
	}
	DPrintf(dHeart, "S%d T%d Leader, sending heartbeats", rf.me, currentTerm)
	for i := 0; i < len(rf.peers); i++ {
		if me == i {
			continue
		}
		go rf.sendHeartbeatTo(i, currentTerm, me)
	}
}

func (rf *Raft) sendHeartbeatTo(index int, currentTerm int, leaderIndex int) {
	args := AppendEntriesArgs{
		Term: currentTerm,
		LeaderId: leaderIndex,

	}
	reply := AppendEntriesReply{}
	rf.sendAppendEntries(index, &args, &reply)

	rf.mu.Lock()
	defer rf.mu.Unlock()
	if currentTerm < reply.Term {
		DPrintf(dHeart, "S%d T%d Leader resetting to follower %#v. ", rf.me, currentTerm, reply)
		rf.setStateToFollower(reply.Term)
	} else if !reply.Success {
		DPrintf(dHeart, "S%d T%d Leader decrementing nextIndex for %#v. ", rf.me, currentTerm, index)
		rf.nextIndex[index] -= 1
	} else {
		
	}
}


