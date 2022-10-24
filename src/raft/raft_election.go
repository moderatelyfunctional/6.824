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
		electionTimeout := ELECTION_TIMEOUT_MIN_MS + rand.Intn(ELECTION_TIMEOUT_SPREAD_MS)
		rf.mu.Lock()
		rf.electionTimeout = electionTimeout
		rf.heartbeat = false
		rf.mu.Unlock()

		go rf.startElectionCountdown(electionTimeout, currentTerm)
		return
	}

	rf.startElection()
}

func (rf *Raft) startElection() {
	DPrintf("%d instance - ELECTION", rf.me)
	rf.mu.Lock()
	me := rf.me
	rf.setStateToCandidate()
	currentTerm := rf.currentTerm
	rf.mu.Unlock()

	for i := 0; i < len(rf.peers); i++ {
		if rf.me == i {
			continue
		}
		DPrintf("%d instance - ELECTION - REQUEST VOTE TO %d on TERM %d", rf.me, i, currentTerm)
		go rf.requestVoteTo(i, currentTerm, me)
	}
}

func (rf *Raft) requestVoteTo(index int, currentTerm int, me int) {
	args := RequestVoteArgs{
		Term: currentTerm,
		CandidateId: me,
	}
	reply := RequestVoteReply{}
	rf.sendRequestVote(index, &args, &reply)

	if currentTerm < reply.Term {
		rf.mu.Lock()
		defer rf.mu.Unlock()
		rf.setStateToFollower(reply.Term)
	}

	if reply.VoteGranted {
		rf.mu.Lock()
		defer rf.mu.Unlock()
		rf.votesReceived += 1

		DPrintf("%d instance - VOTE FROM - %d", rf.me, index)
		if rf.votesReceived * 2 > len(rf.peers) {
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
	for i := 0; i < len(rf.peers); i++ {
		if me == i {
			continue
		}
		go rf.sendHeartbeatTo(i, currentTerm, me)
	}
}

func (rf *Raft) sendHeartbeatTo(index int, currentTerm int, me int) {
	args := AppendEntriesArgs{
		Term: currentTerm,
		LeaderId: me,
	}
	reply := AppendEntriesReply{}
	rf.sendAppendEntries(index, &args, &reply)

	if currentTerm < reply.Term {
		rf.mu.Lock()
		defer rf.mu.Unlock()
		rf.setStateToFollower(reply.Term)
	}
}


