package mr

import "fmt"
import "log"

import "time"

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

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	workerDetails := WorkerDetails{
		mapf: mapf,
		reducef: reducef,
		state: WORKER_IDLE_STATE,
		quit: make(chan bool),
		debug: DEBUG,
	}
	// Your worker implementation here.
	ticker := time.NewTicker(1 * time.Second)
	go func() {
		time.Sleep(2 * time.Second)
		workerDetails.quit<-true
	}()
	for {
		select {
		case <-ticker.C:
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
