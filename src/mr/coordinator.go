package mr

import "os"
import "errors"

import "fmt"
import "log"

import "time"
import "sync"

import "net"
import "net/rpc"
import "net/http"


type Coordinator struct {
	// Your definitions here.
	mu 						sync.Mutex
	
	mapTasks 				[]MapTask
	nReduce					int
	reduceTasks				[]ReduceTask
	state 					CoordinatorState
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
	Filename 		string
	OutputPrefix 	string
	MapIndex 		int
	NumReduce 		int
	State 			TaskState
}

// Each worker will take as input mr-[0, 1, ...nMap]-[reduceIndex] intermediate
// filenames and write the output into mr-out[reduceIndex]
type ReduceTask struct {
	Filenames 		[]string
	OutputPrefix 	string
	ReduceIndex 	int
	State 			TaskState
}

type TaskState string

const (
	TASK_NOT_STARTED 	TaskState = "TASK_NOT_STARTED"
	TASK_ASSIGNED		TaskState = "TASK_ASSIGNED"
	TASK_DONE			TaskState = "TASK_DONE"
)

// Your code here -- RPC handlers for the worker to call.

func (c *Coordinator) AssignTask(args *AssignTaskArgs, reply *AssignTaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.state == COORDINATOR_DONE {
		reply.TaskType = ASSIGN_TASK_DONE
		return nil
	}

	taskStates := c.getTaskStates()
	for i, taskState := range taskStates {
		if taskState == TASK_NOT_STARTED {
			if c.state == COORDINATOR_MAP {
				c.mapTasks[i].State = TASK_ASSIGNED
				reply.MapTask = c.mapTasks[i]
				reply.TaskType = ASSIGN_TASK_MAP
				break
			} else {
				c.reduceTasks[i].State = TASK_ASSIGNED
				reply.ReduceTask = c.reduceTasks[i]
				reply.TaskType = ASSIGN_TASK_REDUCE
				break
			}
		}
	}
	if reply.TaskType == "" {
		reply.TaskType = ASSIGN_TASK_IDLE
	}
	return nil
}

func (c *Coordinator) getTaskStates() []TaskState {
	var taskStates []TaskState
	if c.state == COORDINATOR_MAP {
		taskStates = make([]TaskState, len(c.mapTasks))
		for i, mapTask := range c.mapTasks {
			taskStates[i] = mapTask.State
		}
	} else if c.state == COORDINATOR_REDUCE {
		taskStates = make([]TaskState, len(c.reduceTasks))
		for i, reduceTask := range c.reduceTasks {
			taskStates[i] = reduceTask.State
		}
	}
	return taskStates
}

func (c *Coordinator) hasUnassignedTasks() bool {
	taskStates := c.getTaskStates()
	for _, taskState := range taskStates {
		if taskState == TASK_NOT_STARTED  {
			return true
		}
	}
	return false
}

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

func (c *Coordinator) StartReduce() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.state = COORDINATOR_REDUCE
}

func (c *Coordinator) Stop() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.state = COORDINATOR_DONE
}

func (c *Coordinator) CheckDone() {
	for {
		time.Sleep(1 * time.Second)
		if c.state == COORDINATOR_MAP {
			c.CheckMapDone()
		} else if c.state == COORDINATOR_REDUCE {
			c.CheckReduceDone()
		}
	}
}

func (c *Coordinator) CheckMapDone() {
	doneIndices := []int{}
	c.mu.Lock()
	for i, mapTask := range c.mapTasks {
		isMapTaskDone := true
		for j := 0; j < c.nReduce; j++ {
			mapTaskOutputFilename := fmt.Sprintf(
				"%s-%d-%d", mapTask.OutputPrefix, mapTask.MapIndex, j)
			if !exists(mapTaskOutputFilename) {
				isMapTaskDone = false
				break
			}
		}
		if isMapTaskDone {
			doneIndices = append(doneIndices, i)
		}
	}
	c.mu.Unlock()
	c.SetTaskDone(doneIndices)
	if len(doneIndices) == len(c.mapTasks) {
		c.StartReduce()
	}
}

func (c *Coordinator) CheckReduceDone() {
	doneIndices := []int{}
	c.mu.Lock()
	for i, reduceTask := range c.reduceTasks {
		reduceTaskOutputFilename := fmt.Sprintf(
			"%s-%d", reduceTask.OutputPrefix, i)
		if exists(reduceTaskOutputFilename) {
			doneIndices = append(doneIndices, i)
		}
	}
	c.mu.Unlock()
	c.SetTaskDone(doneIndices)
	if len(doneIndices) == len(c.reduceTasks) {
		c.Stop()
	}
}

func (c *Coordinator) SetTaskDone(indices []int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, index := range indices {
		if c.state == COORDINATOR_MAP {
			c.mapTasks[index].State = TASK_DONE
		} else if c.state == COORDINATOR_REDUCE {
			c.reduceTasks[index].State = TASK_DONE
		}
	}
}

func exists(filename string) bool {
	if _, err := os.Stat(filename); errors.Is(err, os.ErrNotExist) {
		return false
	}
	return true
}

func checkFilesExist(filenames []string) bool {
	for _, filename := range filenames {
		if !exists(filename) {
			return false
		}
	}
	return true
}

func removeFiles(filenames []string) {
	for _, filename := range filenames {
		os.Remove(filename)
	}
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
			Filename: file,
			OutputPrefix: INTERMEDIATE_FILE_PREFIX,
			MapIndex: i,
			NumReduce: nReduce,
			State: TASK_NOT_STARTED,			
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
			Filenames: intermediateFiles,
			OutputPrefix: OUTPUT_FILE_PREFIX,
			ReduceIndex: i,
			State: TASK_NOT_STARTED,
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
	go c.CheckDone()
	return c
}




