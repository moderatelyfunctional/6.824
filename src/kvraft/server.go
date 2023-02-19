package kvraft

import (
	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
	"log"
	"sync"
	"sync/atomic"
)

const Debug = false

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
	Action			string

	CommitIndex		int
}

type KVServer struct {
	mu				sync.Mutex
	me				int
	rf				*raft.Raft
	applyCh			chan raft.ApplyMsg
	dead			int32 // set by Kill()
	commitIndex 	int32 // checked by Get and PutAppend to determine if an operation is committed.

	maxraftstate	int // snapshot if log grows this big

	state			map[string]string // the set of key value pair mappings
	operationIds	map[string]int // the set of previous operationIds that should be executed exactly once.
}


func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	op := Op{
		Id: args.OpId,
		Key: args.Key,
		Action: GET,
	}
	commitIndex, term, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
}

func (kv *KVServer) applyCommittedOps(applyCh chan ApplyMsg) {
	for m := range applyCh {
		op := m.Command.(Op)
		kv.applyCommittedOp(op)
	}
}

func (kv *KVServer) applyCommittedOp(Op op) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	// Check if the command was already executed, and if so, it should not be executed again.
	_, ok := kv.operationIds[op.Id]
	if ok {
		return
	}
	kv.operationIds[op.Id] = op.CommitIndex

	// For GET operations, there's no ned to modify the state. For PUT and APPEND operations, the
	// state should be modified. If a key doesn't exist for an APPEND operation, it should behave
	// like a PUT operation.
	if op.Action == PUT {
		kv.state[op.Key] = op.Value
	} else {
		value, ok := kv.state[op.Key]
		if ok {
			kv.state[op.Key] = value + op.Value
		} else {
			kv.state[op.Key] = op.Value
		}
	}
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

func (kv *KVServer) getCommitIndex() {
	z := atomic.LoadInt32(&kv.commitIndex)
	return z
}

func (kv *KVServer) incrementCommitIndex() {
	atomic.AddInt32(&kv.timeoutCount, 1)
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

	// You may need initialization code here.
	go kv.applyCommittedOps()

	return kv
}
