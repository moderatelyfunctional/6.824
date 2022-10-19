package raft

import "math/rand"

const ELECTION_TIMEOUT_MIN_MS int = 750
const ELECTION_TIMEOUT_SPREAD_MS int = 500

// checks whether the raft instance's term is >= the other term.
func (rf *Raft) isLowerTerm(otherTerm int) bool {
	return rf.currentTerm < otherTerm
}

func (rf *Raft) setStateToFollower(currentTerm int) {
	rf.currentTerm = currentTerm
	rf.votedFor = nil
	rf.state = FOLLOWER
	rf.electionTimeout = ELECTION_TIMEOUT_MIN_MS + rand.Intn(ELECTION_TIMEOUT_SPREAD_MS)
}

// rules for servers: on discovering a higher term, all servers set their term to that term
// and set their state to a follower.
func (rf *Raft) setStateToFollowerForLowerTerm(otherTerm int) bool {
	if !rf.isLowerTerm(otherTerm) {
		return false
	}

	rf.setStateToFollower(otherTerm)
	return true
}


func (rf *Raft) setStateToCandidate() {

}

func (rf *Raft) setStateToLeader() {
	
}
