package raft

import "time"
import "math/rand"

func (rf *Raft) startElectionCountdown(electionTimeout int) {
	time.Sleep(time.Duration(electionTimeout) * time.Millisecond)
	rf.electionChan<-true
}

func (rf *Raft) checkElectionTimeout() {
	rf.mu.Lock()
	state := rf.state
	heartbeat := rf.heartbeat
	rf.mu.Unlock()

	// if the raft instance is the leader there is no need to check the election timeout.
	if state == LEADER {
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

		go rf.startElectionCountdown(electionTimeout)
		return
	}

	rf.startElection()
}

func (rf *Raft) startElection() {
	rf.mu.Lock()
	rf.setStateToCandidate()
	currentTerm := rf.currentTerm
	me := rf.me
	rf.mu.Unlock()

	for i := 0; i < len(rf.peers); i++ {
		if rf.me == i {
			continue
		}
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


