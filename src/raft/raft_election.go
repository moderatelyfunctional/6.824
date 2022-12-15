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
	DPrintf(dTimer, "S%d T%d checking election timeout with timeoutTerm %d", rf.me, currentTerm, timeoutTerm)

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
	rf.setStateToCandidate()
	currentTerm := rf.currentTerm
	lastLogIndex := -1
	lastLogTerm := -1
	if len(rf.log) != 0 {
		lastLogIndex = len(rf.log) - 1
		lastLogTerm = rf.log[lastLogIndex].Term
	}
	rf.mu.Unlock()

	for i := 0; i < len(rf.peers); i++ {
		if rf.me == i {
			continue
		}
		go rf.requestVoteTo(i, currentTerm, lastLogIndex, lastLogTerm, rf.me)
	}
}

func (rf *Raft) requestVoteTo(index int, currentTerm int, lastLogIndex int, lastLogTerm int, me int) {
	// rf.mu.Lock()
	// if rf.state != CANDIDATE {
	// 	DPrintf(dVote, "S%d T%d state already set to %v", rf.me, currentTerm, rf.state)
	// 	return
	// }
	// rf.mu.Unlock()

	args := RequestVoteArgs{
		Term: currentTerm,
		CandidateId: me,
		LastLogIndex: lastLogIndex,
		LastLogTerm: lastLogTerm,
	}
	reply := RequestVoteReply{}
	ok := rf.sendRequestVote(index, &args, &reply)
	if !ok {
		DPrintf(dVote, "S%d T%d RPC failed for S%d", rf.me, currentTerm, index)
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.state == FOLLOWER {
		DPrintf(dVote, "S%d T%d state already set to %v", rf.me, currentTerm, rf.state)
		return
	}
	if currentTerm < reply.Term {
		DPrintf(dVote, "S%d T%d found S%d with higher term. Setting state from candidate to follower", rf.me, currentTerm, index)
		rf.setStateToFollower(reply.Term)
		return
	}

	if !reply.VoteGranted {
		rf.votesReceived[index] = 0
	} else {
		rf.votesReceived[index] = 1
		votesTotal := 0
		for i, _ := range rf.votesReceived {
			votesTotal += rf.votesReceived[i]
		}

		DPrintf(dVote, "S%d T%d (%d/%d votes) received vote from S%d", rf.me, currentTerm, votesTotal, len(rf.peers), index)
		if votesTotal * 2 > len(rf.peers) && rf.state != LEADER {
			DPrintf(dVote, "S%d T%d set state to leader.", rf.me, currentTerm)
			rf.setStateToLeader()
		}
	}
	rf.persist()
}

