package raft

//
// This is an outline of the API that raft must expose to
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
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
	// "runtime"

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
	KILL_INTERVAL_MS				int = 150 	
	HEARTBEAT_INTERVAL_MS			int = 150
	APPLY_MSG_INTERVAL_MS			int = 300
	ELECTION_TIMEOUT_MIN_MS			int = 500
	ELECTION_TIMEOUT_SPREAD_MS		int = 1000
)

type Entry struct {
	Term 				int
	Command				interface{}
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu					sync.Mutex				// Lock to protect shared access to this peer's state
	peers				[]*labrpc.ClientEnd		// RPC end points of all peers
	persister			*Persister				// Object to hold this peer's persisted state
	me					int						// This peer's index into peers[]
	dead				int32					// Set by Kill()
	applyInProg 		int32					// An apply operation is underway so stop any new apply operations or the kill switch.
	timeoutCount		int32 					// An election timeout countdown counter that should be checked before executing the kill switch.

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	currentTerm			int 					// Latest term the server has seen (init to 0, increases monotonically)
	votedFor			int 					// Index of the candidate that received a vote in the current term
	votesReceived		[]int 					// Votes the instance received in its latest election from each of the other servers
	state				State 					// The instance's state (follower, candidate or leader)

	log 				*Log					// Log object which supports appending, compaction for snapshotting and log comparisons 
	commitIndex 		int 					// Index of highest log entry known to be committed (replicated durably on a majority of servers)
	lastApplied 		int 					// Index of highest log entry applied to state machine 		 
	nextIndex 			[]int					// For each server, index of the next log entry to send to that server. (init to leader last log entry + 1)
	matchIndex 			[]int					// For each server, index of the highest log entry known to be replicated on the server.

	heartbeat 			bool 					// Received a heartbeat from the leader
	electionTimeout 	int 					// Randomized timeout duration of the raft instance prior to starting another election

	electionChan		chan int 				// Signals the instance reached the election timeout duration
	heartbeatChan 		chan int 				// Signals the leader should send another heartbeat message immediately to the specified instance.
	applyCh 			chan ApplyMsg 			// Signals the instance applied the given log entry to its state machine
	quitChan 	 		chan bool 				// Signals the instance should shut down (killswitch)
}

func (rf *Raft) prettyPrint() string {
	return fmt.Sprintf(
		"S%d T%d state %v log %v commitIndex %d lastApplied %d nextIndex %v matchIndex %v",
		rf.me, rf.currentTerm, rf.state, rf.log, rf.commitIndex, rf.lastApplied, rf.nextIndex, rf.matchIndex)
}

// Return (currentTerm, isLeader)
func (rf *Raft) GetState() (int, bool) {
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	return rf.currentTerm, rf.state == LEADER
}

// Because applyInProg is checked every KILL_INTERVAL_MS in checkKilledAndQuit, using the Raft lock is not performant.
// To solve that problem, applyInProg uses the atomic package like dead. All READ/WRITE operations to applyToProg *must*
// use the following two methods.
func (rf *Raft) setApplyInProg(value bool) {
	stored := int32(0)
	if value {
		stored = 1
	} 
	atomic.StoreInt32(&rf.applyInProg, stored)
}

func (rf *Raft) isApplyInProg() bool {
	a := atomic.LoadInt32(&rf.applyInProg)
	return a == 1
}

func (rf *Raft) modifyTimeoutCount(delta int32) {
	atomic.AddInt32(&rf.timeoutCount, delta)
}

func (rf *Raft) isTimeoutInProg() bool {
	a := atomic.LoadInt32(&rf.timeoutCount)
	return a > 0
}

//
// The tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// The issue is that long-running goroutines use memory and may chew
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
		// Check that the instance is killed and no apply messages are being sent on the instance.
		// This prevents a race condition because a killed instance will close its channels (applyCh)
		// and so any messages sent on applyCh from sendApplyMsg() will cause a panic.
		if !rf.isApplyInProg() && 
		   !rf.isTimeoutInProg() &&
		   rf.killed() {
		   	fmt.Println("CLOSED???")
			go func() {
				rf.quitChan<-true
			}()
			return
		}
		time.Sleep(time.Duration(KILL_INTERVAL_MS) * time.Millisecond)
	}
}

// The ticker go routine starts a new election if this peer hasn't received
// heartbeats recently.
func (rf *Raft) ticker() {
	rf.mu.Lock()
	electionTimeout := rf.electionTimeout
	currentTerm := rf.currentTerm
	rf.mu.Unlock()

	go rf.checkKilledAndQuit()
	go rf.startElectionCountdown(electionTimeout, currentTerm)
	heartbeatTicker := time.NewTicker(time.Duration(HEARTBEAT_INTERVAL_MS) * time.Millisecond)

	for {
		select {
		case <-heartbeatTicker.C:
			rf.sendHeartbeat()
		case timeoutTerm := <-rf.electionChan:
			rf.checkElectionTimeout(timeoutTerm)
		case other := <-rf.heartbeatChan:
			rf.sendCatchupHeartbeatTo(other)
		case <-rf.quitChan:
			heartbeatTicker.Stop()
			close(rf.applyCh)
			close(rf.electionChan)
			close(rf.heartbeatChan)
			close(rf.quitChan)
			return
		}
	}
}

//
// The service or tester wants to create a Raft server. the ports
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
	rf := FuncMake(peers, me, persister, applyCh)

	// Start ticker goroutine to start elections
	go rf.ticker()

	return rf
}

func FuncMake(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	setupDebug()
	rf := &Raft{
		peers: peers,
		persister: persister,
		me: me,
		votesReceived: make([]int, len(peers)),
		state: FOLLOWER,
		log: makeLog([]Entry{}),
		commitIndex: -1,
		lastApplied: -1,
		nextIndex: make([]int, len(peers)),
		matchIndex: make([]int, len(peers)),
		electionTimeout: ELECTION_TIMEOUT_MIN_MS + rand.Intn(ELECTION_TIMEOUT_SPREAD_MS),
		electionChan: make(chan int),
		heartbeatChan: make(chan int),
		applyCh: applyCh,
		quitChan: make(chan bool),
	}
	// Your initialization code here (2A, 2B, 2C).

	// Initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
}

