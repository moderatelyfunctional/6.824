package mronaws

import "fmt"
import "testing"

import "os"
import "time"


// The MapWorkerDetails struct should be copied into each test through a xyz := MapWorkerDetails
// to enforce state invariance across each tests.
var MapWorkerDetails WorkerDetails = WorkerDetails{
	detailKey: createDetailKey(/* changeSeed= */ false),
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
		"intermediate/mr-0-0",
		"intermediate/mr-0-1",
	}
	mapWorkerDetails := MapWorkerDetails
	t.Run(mapWorkerDetails.name(), func(t *testing.T) {
		os.Mkdir(mapWorkerDetails.detailKey, os.ModePerm)
		mapWorkerDetails.processMapTask()
		objects, err := ListFilesInS3(AWS_INTERMEDIATE_PREFIX)
		contentSet := map[string]bool{}
		if err != nil {
			t.Errorf("Error listing contents in S3 %v", err)
		}
		for _, content := range objects.Contents {
			contentSet[*content.Key] = true
		}
		for _, expectedFilename := range expectedFilenames {
			if _, ok := contentSet[expectedFilename]; !ok {
				t.Errorf("Expected %v file to exist on AWS", expectedFilename)
			}
			DeleteFileInS3(expectedFilename)
		}
		os.RemoveAll(mapWorkerDetails.detailKey)
	})

}

func TestTwoWorkerDetailsProcessMapTask(t *testing.T) {
	expectedFilenames := []string{
		"intermediate/mr-0-0",
		"intermediate/mr-0-1",
	}
	mapWorkerDetailsOne := MapWorkerDetails
	mapWorkerDetailsTwo := MapWorkerDetails
	mapWorkerDetailsTwo.detailKey = createDetailKey(/* changeSeed= */ true)
	t.Run(fmt.Sprintf("Two map worker details of %s", mapWorkerDetailsOne.name()), func(t *testing.T) {
		os.Mkdir(mapWorkerDetailsOne.detailKey, os.ModePerm)
		os.Mkdir(mapWorkerDetailsTwo.detailKey, os.ModePerm)
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
		objects, err := ListFilesInS3(AWS_INTERMEDIATE_PREFIX)
		contentSet := map[string]bool{}
		if err != nil {
			t.Errorf("Error listing contents in S3 %v", err)
		}
		for _, content := range objects.Contents {
			contentSet[*content.Key] = true
		}
		for _, expectedFilename := range expectedFilenames {
			if _, ok := contentSet[expectedFilename]; !ok {
				t.Errorf("Expected %v file to exist on AWS", expectedFilename)
			}
			DeleteFileInS3(expectedFilename)
		}
		os.RemoveAll(mapWorkerDetailsOne.detailKey)
		os.RemoveAll(mapWorkerDetailsTwo.detailKey)
	})

}











