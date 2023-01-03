package raft

import "time"
import "math/rand"

// This method is called during raft state transitions to a follower or a candidate. It's also called in 
// checkElectionTimeout which is the downstream client of rf.electionChan to set the _next_ election countdown.
// 
// The intuition is a follower should receive a heartbeat (AppendEntries RPC) within each timeout period which
// causes it to reset the election countdown for the next term. If the follower doesn't receive the heartbeat,
// it then starts another election.
//
// Similarly, a candidate that doesn't receive a majority of votes in its current term will start another election.
//
// It's possible for an instance to be in a higher term than the term number sent on rf.electionChan. In that case,
// this message should be thrown away since it's not relevant. It would be best to delete prior term messages if possible,
// but there is no way to delete in-flight messages in a channel.
func (rf *Raft) startElectionCountdown(electionTimeout int, currentTerm int) {
	time.Sleep(time.Duration(electionTimeout) * time.Millisecond)
	rf.electionChan<-currentTerm
}

// Called by rf.electionChan in raft.go to process messages sent by startElectionCountdown. Messages to: 1) instances that are
// now leaders or 2) instances whose terms have changed are dropped.
//
// If no heartbeat is received by the instance at the time this method is called, the instance starts a new election.
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

// There could be multiple overlapping elections. That may split the vote but doesn't affect correctness. Each
// instance votes for itself (indirectly by setting its state to candidate), and then requests votes to other rafts.
func (rf *Raft) startElection() {
	rf.mu.Lock()
	rf.setStateToCandidate()
	currentTerm := rf.currentTerm
	lastLogIndex, lastLogTerm := rf.log.lastEntry()
	rf.mu.Unlock()

	for i := 0; i < len(rf.peers); i++ {
		if rf.me == i {
			continue
		}
		go rf.requestVoteTo(i, currentTerm, lastLogIndex, lastLogTerm)
	}
}

// Send requestVote requests to other rafts and check the response. If vote granted, then the instance checks if it
// can become a leader (after receiving a majority of votes). There are a few subtle points to consider:
// 1) The new leader should still communicate with the rest of the instances so they don't try to start new elections.
// 2) _The interplay between locking and RPCs is very interesting_. At least for dstest, because all the tests run on the same
// physical hardware, returning early if the instance is in a FOLLOWER state drastically increases the number of Lock calls,
// which grinds the performance of the system down enough to have a few timeout errors per hundreds of invocations.
//
// On a production grade system, the inverse may be true: sending additional RPCs may be more expensive for the network than
// locking is on the raft application code.
//
// The state should be checked AFTER the RPC is sent. If the instance is now a follower, there is no point becoming a leader even
// if it receives sufficient votes since there is a more recent leader.
func (rf *Raft) requestVoteTo(index int, currentTerm int, lastLogIndex int, lastLogTerm int) {
	// rf.mu.Lock()
	// if rf.state == FOLLOWER {
	// 	DPrintf(dVote, "S%d T%d state already set to %v", rf.me, currentTerm, rf.state)
	// 	return
	// }
	// rf.mu.Unlock()

	args := RequestVoteArgs{
		Term: currentTerm,
		CandidateId: rf.me,
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

