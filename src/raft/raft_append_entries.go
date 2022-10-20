package raft

type AppendEntriesArgs struct {
	Term 			int 
	LeaderId 	 	int
}

type AppendEntriesReply struct {
	Term 			int
	Success 		bool
}

func (rf *Raft) checkElectionTimeout() {

}

func (rf *Raft) sendHeartbeat() {

}