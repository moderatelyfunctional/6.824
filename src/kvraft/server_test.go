package kvraft

import "6.824/labrpc"
import "6.824/raft"

import "fmt"
// import "time"
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
			"TestServerGetIsFollower expected error due to raft being a follower but got %#v",
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
			"TestServerGetDuplicateOp expected error due to op executed but got %#v",
			getReply)
	}
}

