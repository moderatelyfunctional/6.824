package mr

import "fmt"
import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"


type Coordinator struct {
	// Your definitions here.
	mapTasks 		[]MapTask
	nReduce			int
	reduceTasks		[]ReduceTask
	state 			CoordinatorState
}

type CoordinatorState string

const (
	COORDINATOR_MAP 	CoordinatorState = "COORDINATOR_MAP"
	COORDINATOR_REDUCE 	CoordinatorState = "COORDINATOR_REDUCE"
	COORDINATOR_DONE 	CoordinatorState = "COORDINATOR_DONE"
)

const (
	INTERMEDIATE_FILE_PREFIX 	string = "mr"
	OUTPUT_FILE_PREFIX			string = "mr-out"
)

// Each worker will take a filename as input into a MapTask and write the output
// into mr-[mapIndex]-[0, 1, ...nReduce] intermediate files.
type MapTask struct {
	filename 		string
	outputPrefix 	string
	mapIndex 		int
	nReduce 		int
	state 			TaskState
}

// Each worker will take as input mr-[0, 1, ...nMap]-[reduceIndex] intermediate
// filenames and write the output into mr-out[reduceIndex]
type ReduceTask struct {
	filenames 		[]string
	outputPrefix 	string
	reduceIndex 	int
	state 			TaskState
}

type TaskState string

const (
	TASK_NOT_STARTED 	TaskState = "TASK_NOT_STARTED"
	TASK_ASSIGNED		TaskState = "TASK_ASSIGNED"
	TASK_DONE			TaskState = "TASK_DONE"
)

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}


//
// start a thread that listens for RPCs from worker.go
//
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	// Your code here.
	return c.state == COORDINATOR_DONE
}

func setupCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		mapTasks: []MapTask{},
		nReduce: nReduce,
		reduceTasks: []ReduceTask{},
		state: COORDINATOR_MAP,
	}

	// Your code here.
	for i, file := range files {
		mapTask := MapTask{
			filename: file,
			outputPrefix: INTERMEDIATE_FILE_PREFIX,
			mapIndex: i,
			nReduce: nReduce,
			state: TASK_NOT_STARTED,			
		}
		c.mapTasks = append(c.mapTasks, mapTask)
	}
	for i := 0; i < nReduce; i++ {
		intermediateFiles := []string{}
		for j := 0; j < len(files); j++ {
			intermediateFile := fmt.Sprintf(
				"%s-%v-%v",
				INTERMEDIATE_FILE_PREFIX,
				j,
				i,
			)
			intermediateFiles = append(intermediateFiles, intermediateFile)
		}
		reduceTask := ReduceTask{
			filenames: intermediateFiles,
			outputPrefix: OUTPUT_FILE_PREFIX,
			reduceIndex: i,
			state: TASK_NOT_STARTED,
		}
		c.reduceTasks = append(c.reduceTasks, reduceTask)
	}

	return &c
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := setupCoordinator(files, nReduce)
	c.server()
	return c
}




