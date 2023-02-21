package kvraft

import "fmt"
import "testing"

func TestServerGet(t *testing.T) {
	applyCh := make(chan ApplyMsg)
	kvServer := &KVServer{
		applyCh: applyCh,
	}
	
	getArgs := &GetArgs{
		OpId: fmt.Sprintf("%s", nrand()),
		Key: "X",
	}
	getReply := &GetReply{}
	
}