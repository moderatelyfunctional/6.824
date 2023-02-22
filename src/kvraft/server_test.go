package kvraft

import "6.824/labrpc"
import "6.824/raft"

import "fmt"
import "time"
import "testing"

func createMockRaft() *raft.Raft {
	return raft.FuncMake(
		/* servers= */ []*labrpc.ClientEnd{},
		/* me= */ 0,
		/* persister= */ raft.MakePersister(),
		/* applyCh= */ make(chan raft.ApplyMsg))
}

func TestServerGetIsFollower(t *testing.T) {
	applyCh := make(chan raft.ApplyMsg)
	kvServer := &KVServer{
		rf: createMockRaft(),
		applyCh: applyCh,
	}
	
	getArgs := &GetArgs{
		OpId: fmt.Sprintf("%v", nrand()),
		Key: "X",
	}
	getReply := &GetReply{}
	kvServer.Get(getArgs, getReply)

	if getReply.Err != ErrWrongLeader {
		t.Errorf(
			"TestServerGetIsFollower expected ErrWrongLeader but got %#v",
			getReply)
	}
}

func TestServerGetDuplicateOp(t *testing.T) {
	originalOpId := nrand()
	applyCh := make(chan raft.ApplyMsg)
	kvServer := &KVServer{
		rf: createMockRaft(),
		executedOpIds: map[string]bool{
			fmt.Sprint(originalOpId): true,
		},
		applyCh: applyCh,
	}
	
	getArgs := &GetArgs{
		OpId: fmt.Sprintf("%v", originalOpId),
		Key: "X",
	}
	getReply := &GetReply{}
	kvServer.Get(getArgs, getReply)

	if getReply.Err != ErrOpExecuted {
		t.Errorf(
			"TestServerGetDuplicateOp expected ErrOpExecuted but got %#v",
			getReply)
	}
}

func TestServerExecuteOpNoKey(t *testing.T) {
	cfg := make_config(
		/* testing.T= */ t,
		/* nservers= */ 3,
		/* unreliable= */ false,
		/* maxraftsize= */ -1,
		/* frozen= */ false)
	
	// Give raft sufficient time to elect a leader.
	time.Sleep(2 * time.Second)

	leaderIndex := -1
	for i := 0; i < cfg.n; i++ {
		if cfg.kvservers[i].rf.IsLeader() {
			leaderIndex = i
			break
		}
	}

	if leaderIndex == -1 {
		t.Errorf("TestServerExecuteOp expected leaderIndex to be set")
	}
	getArgs := &GetArgs{
		OpId: fmt.Sprintf("%v", nrand()),
		Key: "X",
	}
	getReply := &GetReply{}
	cfg.kvservers[leaderIndex].Get(getArgs, getReply)

	time.Sleep(1 * time.Second)
	if getReply.Err != ErrNoKey {
		t.Errorf(
			"TestServerExecuteOpNoKey expected ErrNoKey but got %#v",
			getReply)		
	}
}



