package raft

import "math/rand"

// Checks whether the raft instance's term is >= the other term.
func (rf *Raft) isLowerTerm(otherTerm int) bool {
	return rf.currentTerm < otherTerm
}

// Rules for servers: on discovering a higher term, all servers set their term to that term
// and set their state to a follower.
//
// rf.heartbeat = False must always be set in conjunction with rf.startElectionCountdown.
func (rf *Raft) setStateToFollower(currentTerm int) {
	rf.state = FOLLOWER

	rf.currentTerm = currentTerm
	rf.votedFor = -1
	for i, _ := range rf.votesReceived {
		rf.votesReceived[i] = 0
	}

	electionTimeout := ELECTION_TIMEOUT_MIN_MS + rand.Intn(ELECTION_TIMEOUT_SPREAD_MS)
	rf.electionTimeout = electionTimeout
	rf.heartbeat = false

	rf.persist()

	go rf.startElectionCountdown(electionTimeout, currentTerm)
}

// When a server becomes a candidate, it MUST reset the votesReceived array so that votes from a prior election
// are not counted for the current election. Not doing so can result in two servers being leaders in the same
// term.
//
// rf.heartbeat = False must always be set in conjunction with rf.startElectionCountdown.
func (rf *Raft) setStateToCandidate() {
	rf.state = CANDIDATE

	rf.currentTerm += 1
	rf.votedFor = rf.me
	for i, _ := range rf.votesReceived {
		rf.votesReceived[i] = 0
	}
	rf.votesReceived[rf.me] = 1

	electionTimeout := ELECTION_TIMEOUT_MIN_MS + rand.Intn(ELECTION_TIMEOUT_SPREAD_MS)
	rf.electionTimeout = electionTimeout
	rf.heartbeat = false

	rf.persist()

	currentTerm := rf.currentTerm
	go rf.startElectionCountdown(electionTimeout, currentTerm)
}

// When a server becomes a leader, it must immediately send out heartbeats to other instances so that 1) outdated leaders revert
// back to followers 2) followers and candidates become followers, and don't start a new election on election timeout.
func (rf *Raft) setStateToLeader() {
	rf.state = LEADER

	for i := 0; i < len(rf.peers); i++ {
		rf.nextIndex[i] = rf.log.size()
		rf.matchIndex[i] = -1
	}
	rf.matchIndex[rf.me] = rf.log.size() - 1
	go rf.sendHeartbeat()
}

