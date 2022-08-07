package mr

import "fmt"
import "testing"

var MapWorkerDetails WorkerDetails = WorkerDetails{
	mapf: Map,
	reducef: Reduce,
	mapTask: MapTask{
		"pg-being_ernest.txt",
		INTERMEDIATE_FILE_PREFIX,
		/* mapIndex= */ 0,
		/* nReduce= */ 2,
		TASK_NOT_STARTED,
	},
	state: WORKER_IDLE_STATE,
}

func TestWorkerDetailsProcessMapTask(t *testing.T) {
	t.Run(MapWorkerDetails.name(), func(t *testing.T) {
		MapWorkerDetails.processMapTask()
		fmt.Println(MapWorkerDetails)	
	})

}











