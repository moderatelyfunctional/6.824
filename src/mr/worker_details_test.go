package mr

import "os"
import "fmt"
import "time"
import "errors"
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

func TestOneWorkerDetailsProcessMapTask(t *testing.T) {
	expectedFilenames := []string{
		"../main/mr-0-0",
		"../main/mr-0-1",
	}
	t.Run(MapWorkerDetails.name(), func(t *testing.T) {
		MapWorkerDetails.processMapTask()
		for _, expectedFilename := range expectedFilenames {
			if !exists(expectedFilename) {
				fmt.Printf("Expected %v file to exist", expectedFilename)
			}
			os.Remove(expectedFilename)
		}
	})

}

func TestTwoWorkerDetailsProcessMapTask(t *testing.T) {
	expectedFilenames := []string{
		"../main/mr-0-0",
		"../main/mr-0-1",
	}
	t.Run(MapWorkerDetails.name(), func(t *testing.T) {
		OtherMapWorkerDetails := MapWorkerDetails
		go func() {
			MapWorkerDetails.processMapTask()
		}()
		go func() {
			OtherMapWorkerDetails.processMapTask()
		}()
		for {
			if MapWorkerDetails.state == WORKER_IDLE_STATE && OtherMapWorkerDetails.state == WORKER_IDLE_STATE {
				break
			}
			time.Sleep(1 * time.Second)
		}
		checkFilesExist(expectedFilenames)
	})

}











