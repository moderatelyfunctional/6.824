package mr

import "testing"

import "time"

var MapWorkerDetails WorkerDetails = WorkerDetails{
	mapf: Map,
	reducef: Reduce,
	mapTask: MapTask{
		"input/pg-being_ernest.txt",
		INTERMEDIATE_FILE_PREFIX,
		/* mapIndex= */ 0,
		/* nReduce= */ 2,
		TASK_ASSIGNED,
	},
	state: WORKER_BUSY_STATE,
}

func TestOneWorkerDetailsProcessMapTask(t *testing.T) {
	expectedFilenames := []string{
		"mr-0-0",
		"mr-0-1",
	}
	t.Run(MapWorkerDetails.name(), func(t *testing.T) {
		MapWorkerDetails.processMapTask()
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
		if !checkFilesExist(expectedFilenames) {
			t.Errorf("Expected files to exist: %v\n", expectedFilenames)
		}
		removeFiles(expectedFilenames)
	})

}











