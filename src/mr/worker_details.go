package mr

import "fmt"
import "reflect"

import "time"

import "os"
import "encoding/json"

type WorkerDetails struct {
	mapf 			func(string, string) []KeyValue
	reducef 		func(string, []string) string

	mapTask 		MapTask
	reduceTask 		ReduceTask
	taskType 		AssignTaskType

	state			WorkerState
	quit 			chan bool

	crash			WorkerCrash
	debug			bool
	counter 		WorkerDetailsCounter
}

/*
	WorkerCrash should only be used for debugging purposes in conjunction with MakeCoordinatorInternal
	and WorkerInternal. The first value shouldCrash determines if the worker should crash, crashAfterDurationInMs
	should be used in consideration of the Coordinator's reassignTaskDurationInMs.

	sleepForDurationInMs simulates a slow worker, and should be used with shouldCrash set to false.
*/
type WorkerCrash struct {
	shouldCrash 				bool
	sleepForDurationInMs		int
	crashAfterDurationInMs		int
}

const DEBUG bool = true

const CURR_DIR string = ""

type WorkerDetailsCounter struct {
	isBusy					int
	isStuck 				int
	isDone 					int
	setAssignTask    		int
	processAssignTask		int
	processMapTask 			int
	processReduceTask		int
}

type WorkerState string

/* 
	The various states are as follows:
	IDLE - 	The worker is instantiated in the idle state and also transitions to it after completing a task.
		   	In this state, the worker should call the coordinator to receive another task, if possible.
	NONE - 	The worker called the coordinator to receive a task and didn't receive one. This state indicates the worker
		   	should not process the map or reduce task set on it, as the task corresponds to one the worker has already
		   	completed.
	STUCK - The worker is stuck processing a map or reduce task (failure on opening temporary files, encoding/decoding
			key value pairs, hardware failures). If stuck, the worker should retry its existing task at the next tick 
			interval. It should not call the coordinator to ask for another task. Workers may be stuck indefinitely 
			processing the task. In that case, the coordinator may reassign the task to another worker and mark it as complete
			if the primary or secondary worker succeeds.
	BUSY -  The worker is currently processing a task. If the worker is busy, it won't call the coordinator to ask for 
		   	another task or retry its existing task. The state will be checked again at the next tick interval.
	DONE - 	The coordinator is complete with all tasks. The worker should quit at the next available moment. A worker may
			quit on the next tick interval or quit channel depending on what happens first.

	Check worker_state_transitions.png for more details.
*/
const (
	WORKER_IDLE_STATE		WorkerState = "WORKER_IDLE_STATE"
	WORKER_NONE_STATE 		WorkerState = "WORKER_NONE_STATE"
	WORKER_STUCK_STATE 		WorkerState = "WORKER_STUCK_STATE"
	WORKER_BUSY_STATE		WorkerState = "WORKER_BUSY_STATE"
	WORKER_DONE_STATE 		WorkerState = "WORKER_DONE_STATE"
)

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

func (workerDetails *WorkerDetails) isDone() bool {
	if workerDetails.debug {
		workerDetails.counter.isDone += 1
	}
	return workerDetails.state == WORKER_DONE_STATE
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
		workerDetails.state = WORKER_NONE_STATE
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

	// Set state to IDLE in case the state is NONE from a previous invocation. This is so 
	// processAssignTask knows the worker is now assigned a task, and can start processing it.
	workerDetails.state = WORKER_IDLE_STATE
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
	fmt.Println("Processing...", workerDetails)
	if workerDetails.crash != (WorkerCrash{}) {
		fmt.Println("Crashed the first one")
		if workerDetails.crash.shouldCrash {
			time.Sleep(time.Duration(workerDetails.crash.crashAfterDurationInMs) * time.Millisecond)
			go func() {
				workerDetails.quit<-true
			}()
			return
		} else {
			time.Sleep(time.Duration(workerDetails.crash.sleepForDurationInMs) * time.Millisecond)
		}
	}

	// Solves two edge cases where 1) the coordinator assigns no work to the worker and 
	// it is in the NONE state and 2) setAssignTask sends a message on the quit channel but
	// the code path in Worker(mapf, reducef) continues to the processAssignTask method.
	//
	// In both cases, early exit.
	if workerDetails.state == WORKER_NONE_STATE || workerDetails.state == WORKER_DONE_STATE {
		return
	}
	// The worker state can be either STUCK or BUSY, both should be set to BUSY now.
	workerDetails.state = WORKER_BUSY_STATE
	if workerDetails.taskType == ASSIGN_TASK_MAP {
		workerDetails.processMapTask()
	} else {
		workerDetails.processReduceTask()
	}
}

func (workerDetails *WorkerDetails) processMapTask() {
	if workerDetails.debug {
		workerDetails.counter.processMapTask += 1
	}
	data, err := os.ReadFile(workerDetails.mapTask.Filename)
	if err != nil {
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
		tempFile, _ := os.CreateTemp(CURR_DIR, intermediateFilename)
		tempFiles = append(tempFiles, tempFile)
		intermediateFilenames = append(intermediateFilenames, intermediateFilename)
		encoders = append(encoders, json.NewEncoder(tempFile))
	}
	for _, keyValue := range keyValues {
		reduceIndex := ihash(keyValue.Key) % workerDetails.mapTask.NumReduce
		encoders[reduceIndex].Encode(&keyValue)
	}
	for i, tempFile := range(tempFiles) {
		os.Rename(tempFile.Name(), intermediateFilenames[i])
	}
	workerDetails.state = WORKER_IDLE_STATE

}

func (workerDetails *WorkerDetails) processReduceTask() {
	if workerDetails.debug {
		workerDetails.counter.processReduceTask += 1
	}
	valuesByKey := map[string][]string{}
	for _, filename := range workerDetails.reduceTask.Filenames {
		file, err := os.Open(filename)
		if err != nil {
			workerDetails.state = WORKER_STUCK_STATE
			return
		}
		dec := json.NewDecoder(file)
		for {
			var keyValue KeyValue
			if err := dec.Decode(&keyValue); err != nil {
				break
			}
			valuesByKey[keyValue.Key] = append(valuesByKey[keyValue.Key], keyValue.Value)
		}
	}
	outputFilename := fmt.Sprintf(
		"%s-%d",
		workerDetails.reduceTask.OutputPrefix,
		workerDetails.reduceTask.ReduceIndex)
	tempFile, _ := os.CreateTemp(CURR_DIR, outputFilename)
	for key, values := range valuesByKey {
		combined := workerDetails.reducef(key, values)
		fmt.Fprintf(tempFile, "%v %v\n", key, combined)
	}
	os.Rename(tempFile.Name(), outputFilename)
	workerDetails.state = WORKER_IDLE_STATE
}



