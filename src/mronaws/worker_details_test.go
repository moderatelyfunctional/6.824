package mronaws

import "fmt"
import "testing"

import "time"

// The MapWorkerDetails struct should be copied into each test through a xyz := MapWorkerDetails
// to enforce state invariance across each tests.  
var MapWorkerDetails WorkerDetails = WorkerDetails{
	mapf: CountMap,
	reducef: CountReduce,
	mapTask: MapTask{
		"input/pg-being_ernest.txt",
		INTERMEDIATE_FILE_PREFIX,
		/* mapIndex= */ 0,
		/* nReduce= */ 2,
		TASK_ASSIGNED,
		/* assignedTimeInMs= */ 0,
	},
	state: WORKER_BUSY_STATE,
}

func TestOneWorkerDetailsProcessMapTask(t *testing.T) {
	expectedFilenames := []string{
		"mr-0-0",
		"mr-0-1",
	}
	mapWorkerDetails := MapWorkerDetails
	t.Run(mapWorkerDetails.name(), func(t *testing.T) {
		mapWorkerDetails.processMapTask()
		for _, expectedFilename := range expectedFilenames {
			if !exists(expectedFilename) {
				t.Errorf("Expected %v file to exist", expectedFilename)
			}
		}
		removeFiles(expectedFilenames)
	})

}

func TestTwoWorkerDetailsProcessMapTask(t *testing.T) {
	expectedFilenames := []string{
		"mr-0-0",
		"mr-0-1",
	}
	mapWorkerDetailsOne := MapWorkerDetails
	mapWorkerDetailsTwo := MapWorkerDetails
	t.Run(fmt.Sprintf("Two map worker details of %s", mapWorkerDetailsOne.name()), func(t *testing.T) {
		go func() {
			mapWorkerDetailsOne.processMapTask()
		}()
		go func() {
			mapWorkerDetailsTwo.processMapTask()
		}()
		for {
			if mapWorkerDetailsOne.state == WORKER_IDLE_STATE && mapWorkerDetailsTwo.state == WORKER_IDLE_STATE {
				break
			}
			time.Sleep(1 * time.Second)
		}
		if !checkFilesExist(expectedFilenames) {
			t.Errorf("Expected files to exist: %v\n", expectedFilenames)
		}
		removeFiles(expectedFilenames)
	})

}











