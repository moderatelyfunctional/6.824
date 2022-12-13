package raft

import "time"
import "math/rand"

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

	rf.persist()

	go rf.startElectionCountdown(electionTimeout, currentTerm)
}

func (rf *Raft) setStateToCandidate() {
	rf.currentTerm += 1
	rf.votedFor = rf.me
	for i, _ := range rf.votesReceived {
		rf.votesReceived[i] = 0
	}
	rf.votesReceived[rf.me] = 1
	rf.state = CANDIDATE

	electionTimeout := ELECTION_TIMEOUT_MIN_MS + rand.Intn(ELECTION_TIMEOUT_SPREAD_MS)
	rf.electionTimeout = electionTimeout
	rf.heartbeat = false

	rf.persist()

	currentTerm := rf.currentTerm
	go rf.startElectionCountdown(electionTimeout, currentTerm)
}

func (rf *Raft) setStateToLeader() {
	rf.state = LEADER

	// rf.commitIndex = rf.lastApplied
	for i := 0; i < len(rf.peers); i++ {
		rf.nextIndex[i] = len(rf.log)
		rf.matchIndex[i] = -1
	}
	rf.matchIndex[rf.me] = len(rf.log) - 1
	// rf.setNextHeartbeat()
	go rf.sendHeartbeat()
}

func (rf *Raft) setNextHeartbeat() {
	rf.heartbeatIndex = rf.commitIndex
	rf.nextHeartbeat = time.Now().UnixMilli() + int64(HEARTBEAT_INTERVAL_MS)
}



