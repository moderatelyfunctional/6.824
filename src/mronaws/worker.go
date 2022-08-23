package mronaws

import "fmt"
import "log"
import "time"

import "os"
import "errors"

import "net/rpc"
import "hash/fnv"

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

//
// use ihash(key) % NumReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

func createDir(path string) {
	if _, err := os.Stat(path); errors.Is(err, os.ErrNotExist) {
		os.Mkdir(path, os.ModePerm)
	}
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	WorkerInternal(mapf, reducef, WorkerCrash{}, /* changeSeed= */ false)
}

func WorkerInternal(
	mapf func(string, string) []KeyValue,
	reducef func(string, []string) string, 
	workerCrash WorkerCrash,
	changeSeed bool) {
	workerDetails := WorkerDetails{
		detailKey: createDetailKey(/* changeSeed= */ changeSeed),
		mapf: mapf,
		reducef: reducef,
		state: WORKER_IDLE_STATE,
		quit: make(chan bool),
		crash: workerCrash,
		debug: DEBUG,
	}
	createDir(workerDetails.detailKey)
	time.Sleep(1 * time.Second)
	workerDetails.moveOutputToTmp()
	// ticker := time.NewTicker(1 * time.Second)
	// for {
	// 	select {
	// 	case <-ticker.C:
	// 		// The isDone check occurs first to prevent the worker from processing
	// 		// its assigned task if it is stuck.
	// 		if workerDetails.isDone() {
	// 			return
	// 		} else if workerDetails.isBusy() {
	// 			continue
	// 		} else if workerDetails.isStuck() {
	// 			workerDetails.processAssignTask()
	// 			continue
	// 		}

	// 		// The worker is either in the idle or none state. In both cases, call the 
	// 		// coordinator to ask for another task.
	// 		_, reply := CallAssignTask()
	// 		workerDetails.setAssignTask(reply)
	// 		workerDetails.processAssignTask()
	// 	case <-workerDetails.quit:
	// 		// TODO(USE_IN_EC2)
	// 		// os.RemoveAll(workerDetails.detailKey)
	// 		return
	// 	}
	// }
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
	c, err := rpc.DialHTTP("tcp", "localhost" + ":1234")
	// sockname := coordinatorSock()
	// c, err := rpc.DialHTTP("unix", sockname)
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
