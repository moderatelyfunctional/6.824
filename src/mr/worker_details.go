package mr

import "fmt"
import "reflect"

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
	processReduceTask		int
}

type WorkerState string

const (
	WORKER_IDLE_STATE		WorkerState = "WORKER_IDLE_STATE"
	WORKER_BUSY_STATE		WorkerState = "WORKER_BUSY_STATE"
	WORKER_STUCK_STATE 		WorkerState = "WORKER_STUCK_STATE"
	WORKER_DONE_STATE 		WorkerState = "WORKER_DONE_STATE"
)

const OUTPUT_FILE_DIR = "../main"

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
	} else {
		workerDetails.processReduceTask()
	}
}

func (workerDetails *WorkerDetails) processMapTask() {
	if workerDetails.debug {
		workerDetails.counter.processMapTask += 1
	}
	data, err := os.ReadFile(fmt.Sprintf("%s/%s", OUTPUT_FILE_DIR, workerDetails.mapTask.Filename))
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
			OUTPUT_FILE_DIR, 
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
	for i, tempFile := range(tempFiles) {
		err := os.Rename(tempFile.Name(), fmt.Sprintf("%s/%s", OUTPUT_FILE_DIR, intermediateFilenames[i]))
		if err != nil {
			workerDetails.state = WORKER_STUCK_STATE
			return
		}
	}
	workerDetails.state = WORKER_IDLE_STATE
}

func (workerDetails *WorkerDetails) processReduceTask() {
	if workerDetails.debug {
		workerDetails.counter.processReduceTask += 1
	}
}









