package raft

import "math/rand"

const ELECTION_TIMEOUT_MIN_MS int = 500
const ELECTION_TIMEOUT_SPREAD_MS int = 1000

// checks whether the raft instance's term is >= the other term.
func (rf *Raft) isLowerTerm(otherTerm int) bool {
	return rf.currentTerm < otherTerm
}

// rules for servers: on discovering a higher term, all servers set their term to that term
// and set their state to a follower.
func (rf *Raft) setStateToFollower(currentTerm int) {
	rf.currentTerm = currentTerm
	rf.votedFor = -1
	for i, _ := range rf.votesReceived {
		rf.votesReceived[i] = 0
	}
	rf.state = FOLLOWER

	electionTimeout := ELECTION_TIMEOUT_MIN_MS + rand.Intn(ELECTION_TIMEOUT_SPREAD_MS)
	rf.electionTimeout = electionTimeout
	rf.heartbeat = false

	go rf.startElectionCountdown(electionTimeout, currentTerm)
}

func (rf *Raft) setStateToCandidate() {
	rf.currentTerm += 1
	rf.votedFor = rf.me
	rf.votesReceived[rf.me] = 1
	rf.state = CANDIDATE

	electionTimeout := ELECTION_TIMEOUT_MIN_MS + rand.Intn(ELECTION_TIMEOUT_SPREAD_MS)
	rf.electionTimeout = electionTimeout
	rf.heartbeat = false

	currentTerm := rf.currentTerm
	go rf.startElectionCountdown(electionTimeout, currentTerm)
}

func (rf *Raft) setStateToLeader() {
	rf.state = LEADER

	for i := 0; i < len(rf.peers); i++ {
		rf.nextIndex[i] = len(rf.log)
		rf.matchIndex[i] = 0
	}
	rf.nextIndex[rf.me] = len(rf.log)
	rf.matchIndex[rf.me] = len(rf.log) - 1
	go rf.sendHeartbeat()
}




