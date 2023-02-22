package kvraft

import (
	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"
)

const Debug = false

const (
	RPC_CHECK_INTERVAL_MS		int = 50
	RPC_TIMEOUT_INTERVAL_MS		int = 450
)

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Id				string
	Key				string
	Value			string
	Action			Action

	CommitIndex		int
}

type KVServer struct {
	mu				sync.Mutex
	me				int
	rf				*raft.Raft
	applyCh			chan raft.ApplyMsg
	dead			int32 // set by Kill()
	commitIndex 	int32 // checked by Get and PutAppend to determine if an operation is committed

	maxraftstate	int // snapshot if log grows this big

	state			map[string]string // the set of key value pair mappings
	committedOpIds	map[string]int // the set of operations that are now committed
	executedOpIds	map[string]bool // the set of operations that are now executed (don't execute again)
}


func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	kv.mu.Lock()
	_, ok := kv.executedOpIds[args.OpId]
	if ok {
		reply.Err = ErrOpExecuted
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()

	op := Op{
		Id: args.OpId,
		Key: args.Key,
		Action: GET,
		CommitIndex: kv.getNextCommitIndex(),
	}
	fmt.Println("What is op", op)
	// To prevent a stale value from being returned, the KVServer should ensure it's the leader first
	// by committing the entry before returning a value.
	expectedCommitIndex, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	// Periodically check if raft committed the entry in its log that may correspond to the operation specified
	// in the RPC. There are two potential outcomes here:
	//   1) The committed entry matches the expected one --> this KVServer was the leader and the command
	//   can now be executed.
	// 	 2) Another entry was committed at this index, indicating another KVServer was the leader. This RPC
	//   failed, and the client should retry it at the other KVServer.
	fmt.Println(RPC_TIMEOUT_INTERVAL_MS / RPC_CHECK_INTERVAL_MS)
	for i := 0; i < RPC_TIMEOUT_INTERVAL_MS / RPC_CHECK_INTERVAL_MS; i++ {
		fmt.Println("First sleep...", kv.getCommitIndex(), expectedCommitIndex)
		time.Sleep(time.Duration(RPC_CHECK_INTERVAL_MS) * time.Millisecond)
		if kv.getCommitIndex() != expectedCommitIndex {
			continue
		}
		kv.mu.Lock()
		defer kv.mu.Unlock()
		commitIndex, isCommitted := kv.committedOpIds[args.OpId]
		fmt.Println("committed ops is empty????????????????", kv.committedOpIds)
		fmt.Println("commitIndex versus expected", commitIndex, expectedCommitIndex)
		if !isCommitted || commitIndex != expectedCommitIndex {
			reply.Err = ErrWrongLeader
			return
		}
		_, isExecuted := kv.executedOpIds[args.OpId]
		if isExecuted {
			reply.Err = ErrOpExecuted
			return
		}
		fmt.Println("KV STATE", kv.state)
		kv.executedOpIds[args.OpId] = true
		value, isKeyPresent := kv.state[args.Key]
		if isKeyPresent {
			reply.Value = value
			reply.Err = OK
			return
		}
		reply.Err = ErrNoKey
		return
	}

	// This could have occurred if the leader is a partitioned leader (and doesn't know it should be a follower)
	// or if raft is failing and there is no way for the command to be committed (cannot establish majority).
	reply.Err = ErrNoCommit
	return
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
}

func (kv *KVServer) applyCommittedOps(applyCh chan raft.ApplyMsg) {
	for m := range applyCh {
		fmt.Println("EVER CALLED applyCommittedOps")
		op := m.Command.(Op)
		kv.applyCommittedOp(op)
	}
}

func (kv *KVServer) applyCommittedOp(op Op) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	// Check if the command was already executed, and if so, it should not be executed again.
	fmt.Println("Here", op)
	_, ok := kv.committedOpIds[op.Id]
	if ok {
		return
	}
	kv.committedOpIds[op.Id] = op.CommitIndex
	fmt.Println(kv.committedOpIds)

	// For GET operations, there's no ned to modify the state. For PUT and APPEND operations, the
	// state should be modified. If a key doesn't exist for an APPEND operation, it should behave
	// like a PUT operation.
	if op.Action == PUT {
		kv.state[op.Key] = op.Value
	} else if op.Action == APPEND {
		value, ok := kv.state[op.Key]
		if ok {
			kv.state[op.Key] = value + op.Value
		} else {
			kv.state[op.Key] = op.Value
		}
	}
	fmt.Println("Down to increment...")
	kv.incrementCommitIndex()
}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

func (kv *KVServer) getCommitIndex() int {
	z := atomic.LoadInt32(&kv.commitIndex)
	return int(z)
}

func (kv *KVServer) incrementCommitIndex() {
	atomic.AddInt32(&kv.commitIndex, 1)
}

func (kv *KVServer) getNextCommitIndex() int {
	z := atomic.LoadInt32(&kv.commitIndex)
	return int(z) + 1
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(
	servers []*labrpc.ClientEnd, 
	me int, 
	persister *raft.Persister, 
	maxraftstate int, 
	frozen bool) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	if (frozen) {
		kv.rf = raft.FuncMake(servers, me, persister, kv.applyCh)
	} else {
		kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	}

	kv.state = make(map[string]string)
	kv.committedOpIds = make(map[string]int)
	kv.executedOpIds = make(map[string]bool)

	// You may need initialization code here.
	go kv.applyCommittedOps(kv.applyCh)

	return kv
}
