package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	//	"bytes"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labrpc"
)

type State string

const (
	FOLLOWER	State = "FOLLOWER"
	CANDIDATE	State = "CANDIDATE"
	LEADER 		State = "LEADER"
)

const (
	KILL_INTERVAL_MS 			int = 50 	
	BASE_INTERVAL_MS 	 		int = 50
	HEARTBEAT_INTERVAL_MS 		int = 150
)

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        			sync.Mutex          	// Lock to protect shared access to this peer's state
	peers     			[]*labrpc.ClientEnd 	// RPC end points of all peers
	persister 			*Persister          	// Object to hold this peer's persisted state
	me 		        	int                 	// this peer's index into peers[]
	dead    		    int32               	// set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	currentTerm			int 					// latest term the server has seen (init to 0, increases monotonically)
	votedFor 			*int 					// index of the candidate that received a vote in the current term
	state 				State 					// the instance's state (follower, candidate or leader)

	heartbeat 			bool 					// received a heartbeat from the leader
	electionTimeout 	int 					// randomized timeout duration of the raft instance prior to starting another election

	quitChan 	 		chan bool 				// channel to signal that the instance should shut down (killswitch)
	electionChan		chan bool 				// channel to signal that the instance reached the election timeout duration
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	return rf.currentTerm, rf.state == LEADER
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// Check if the raft instance is killed, and if so, send a message on the quit channel
// to exit the ticker method.
func (rf *Raft) checkKilledAndQuit() {
	for {
		if rf.killed() {
			go func() {
				rf.quit <- true
			}()
		}
		time.Sleep(time.Duration(KILL_INTERVAL_MS) * time.Millisecond)
	}
}

// The ticker go routine starts a new election if this peer hasn't received
// heartbeats recently.
func (rf *Raft) ticker() {
	go rf.checkKilledAndQuit()
	go rf.startElectionCountdown()
	heartbeatTicker := time.NewTicker(time.Duration(HEARTBEAT_INTERVAL_MS) * time.Millisecond)

	for {
		select {
		case <-heartbeatTicker.C:
			rf.sendHeartbeat()
		case <-rf.electionChan:
			rf.checkElectionTimeout()
		case <-rf.quitChan:
			return 
		}
	}
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()


	return rf
}
