package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.


type AssignTaskArgs struct {}

type AssignTaskReply struct {
	TaskType			AssignTaskType
	MapTask 			MapTask
	ReduceTask 			ReduceTask	
}

type AssignTaskType string

const (
	ASSIGN_TASK_IDLE 		AssignTaskType = "ASSIGN_TASK_IDLE"
	ASSIGN_TASK_MAP 		AssignTaskType = "ASSIGN_TASK_MAP"
	ASSIGN_TASK_REDUCE 		AssignTaskType = "ASSIGN_TASK_REDUCE"
	ASSIGN_TASK_DONE 		AssignTaskType = "ASSIGN_TASK_DONE"
)

func (reply *AssignTaskReply) removeRandomness() {
	reply.MapTask.assignedTimeInMs = 0
	reply.ReduceTask.assignedTimeInMs = 0
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
