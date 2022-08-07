package mr

import "fmt"
import "log"
import "reflect"

import "os"
import "time"
import "encoding/json"

import "net/rpc"
import "hash/fnv"

type WorkerDetails struct {
	mapf 			func(string, string) []KeyValue
	reducef 		func(string, []string) string

	mapTask 		MapTask
	reduceTask 		ReduceTask
	taskType 		AssignTaskType

	state			WorkerState
	quit 			chan bool

	debug			bool
	counter 		WorkerDetailsCounter
}

const DEBUG bool = true

type WorkerDetailsCounter struct {
	isBusy					int
	isStuck 				int
	setAssignTask    		int
	processAssignTask		int
	processMapTask 			int
}

type WorkerState string

const (
	WORKER_IDLE_STATE		WorkerState = "WORKER_IDLE_STATE"
	WORKER_BUSY_STATE		WorkerState = "WORKER_BUSY_STATE"
	WORKER_STUCK_STATE 		WorkerState = "WORKER_STUCK_STATE"
	WORKER_DONE_STATE 		WorkerState = "WORKER_DONE_STATE"
)

const FILE_PREFIX = "../main"

func (workerDetails *WorkerDetails) isBusy() bool {
	if workerDetails.debug {
		workerDetails.counter.isBusy += 1
	}
	return workerDetails.state == WORKER_BUSY_STATE
}

func (workerDetails *WorkerDetails) isStuck() bool {
	if workerDetails.debug {
		workerDetails.counter.isStuck += 1
	}
	return workerDetails.state == WORKER_STUCK_STATE
}

func (workerDetails *WorkerDetails) name() string {
	return fmt.Sprintf("MapTask: %v ReduceTask %v", workerDetails.mapTask, workerDetails.reduceTask)
}

func (workerDetails *WorkerDetails) setAssignTask(reply AssignTaskReply) {
	if workerDetails.debug {
		workerDetails.counter.setAssignTask += 1
	}
	// There are no unassigned tasks for the coordinator to assign, the worker
	// should wait for the next tick and ask the coordinator for another task.
	if reflect.DeepEqual(reply, AssignTaskReply{TaskType: ASSIGN_TASK_IDLE}) {
		return
	}

	// The coordinator is completed with both the Map/Reduce tasks and the worker
	// should now exit. The worker will send a message on the quit channel. This 
	// send op must be wrapped in a goroutine so the select statement can receive 
	// the message.
	if reply.TaskType == ASSIGN_TASK_DONE {
		go func() {
			workerDetails.quit<-true
		}()
		workerDetails.state = WORKER_DONE_STATE
		return
	}

	// Only ASSIGN_TASK_MAP and ASSIGN_TASK_REDUCE should arrive in this codepath.
	workerDetails.taskType = reply.TaskType
	if reply.TaskType == ASSIGN_TASK_MAP {
		workerDetails.mapTask = reply.MapTask
	} else {
		workerDetails.reduceTask = reply.ReduceTask
	}
}

func (workerDetails *WorkerDetails) processAssignTask() {
	if workerDetails.debug {
		workerDetails.counter.processAssignTask += 1
	}
	if workerDetails.state == WORKER_BUSY_STATE || workerDetails.state == WORKER_DONE_STATE {
		return
	}
	if workerDetails.taskType == ASSIGN_TASK_MAP {
		workerDetails.processMapTask()
	}
}

func (workerDetails *WorkerDetails) processMapTask() {
	fmt.Println("START")
	if workerDetails.debug {
		workerDetails.counter.processMapTask += 1
	}
	data, err := os.ReadFile(fmt.Sprintf("%s/%s", FILE_PREFIX, workerDetails.mapTask.Filename))
	fmt.Println("COUDLNT OPEN", err)
	if err != nil {
		fmt.Println(err)
		workerDetails.state = WORKER_STUCK_STATE
		return
	}
	keyValues := workerDetails.mapf(workerDetails.mapTask.Filename, string(data))
	tempFiles := []*os.File{}
	intermediateFilenames := []string{}
	encoders := []*json.Encoder{}
	for i := 0; i < workerDetails.mapTask.NumReduce; i++ {
		intermediateFilename := fmt.Sprintf(
			"%s-%d-%d",
			workerDetails.mapTask.OutputPrefix,
			workerDetails.mapTask.MapIndex,
			i)
		tempFile, err := os.CreateTemp(
			FILE_PREFIX, 
			intermediateFilename)
		if err != nil {
			workerDetails.state = WORKER_STUCK_STATE
			return
		}
		tempFiles = append(tempFiles, tempFile)
		intermediateFilenames = append(intermediateFilenames, intermediateFilename)
		encoders = append(encoders, json.NewEncoder(tempFile))
	}
	for _, keyValue := range keyValues {
		reduceIndex := ihash(keyValue.Key) % workerDetails.mapTask.NumReduce
		err := encoders[reduceIndex].Encode(&keyValue)
		if err != nil {
			fmt.Println(err)
		}
	}
	fmt.Println("RENAMING")
	for i, tempFile := range(tempFiles) {
		err := os.Rename(tempFile.Name(), fmt.Sprintf("%s/%s", FILE_PREFIX, intermediateFilenames[i]))
		if err != nil {
			fmt.Println("ERROR ON RENAME", err)
			workerDetails.state = WORKER_STUCK_STATE
			return
		}
	}
	workerDetails.state = WORKER_IDLE_STATE
}

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	workerDetails := WorkerDetails{
		mapf: mapf,
		reducef: reducef,
		state: WORKER_IDLE_STATE,
		debug: DEBUG,
	}
	fmt.Println(workerDetails)
	// Your worker implementation here.
	ticker := time.NewTicker(1 * time.Second)
	for {
		select {
		case <-ticker.C:
			fmt.Println("TICK")
			if workerDetails.isBusy() {
				continue
			} else if workerDetails.isStuck() {
				workerDetails.processAssignTask()
				continue
			}
			_, reply := CallAssignTask()
			workerDetails.setAssignTask(reply)
			workerDetails.processAssignTask()
		case <-workerDetails.quit:
			return
		}
	}

	// fmt.Println("worker output", reply)
	// if reply.taskType == COORDINATOR_DONE {
	// 	return
	// }

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()
}
 
func CallAssignTask() (AssignTaskArgs, AssignTaskReply) {
	args := AssignTaskArgs{}
	reply := AssignTaskReply{} 
	ok := call("Coordinator.AssignTask", &args, &reply)
	if ok {
		return args, reply
	} else {
		// returns the empty reply so the Worker can try again at the next 
		return AssignTaskArgs{}, AssignTaskReply{}
	}
}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	return false
}
