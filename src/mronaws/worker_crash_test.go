package mronaws

import "time"

import "testing"

var crashImmediately WorkerCrash = WorkerCrash{
	shouldCrash: true,
	sleepForDurationInMs: 0,
	crashAfterDurationInMs: 0,
}

var crashNever WorkerCrash = WorkerCrash{
	shouldCrash: false,
	sleepForDurationInMs: 0,
	crashAfterDurationInMs: 0,
}

var crashNeverWithSleep WorkerCrash = WorkerCrash{
	shouldCrash: false,
	sleepForDurationInMs: 4000,
	crashAfterDurationInMs: 0,
}

func TestWorkerCrashCoordinatorReassignsTaskToOtherWorker(t *testing.T) {
	setup()
	expectedIntermediateFilenames := buildIntermediateFiles(simpleTestInput)
	expectedOutputFilenames := buildOutputFiles(simpleTestInput)
	t.Run(simpleTestInput.name(), func(t *testing.T) {
		c := MakeCoordinatorInternal(
			simpleTestInput.files,
			simpleTestInput.nReduce,
			/* reassignTaskDurationInMs= */ 3000)
		go func() {
			WorkerInternal(CountMap, CountReduce, crashImmediately, /* changeSeed= */ false)
		}()
		time.Sleep(500 * time.Millisecond)
		go func() {
			WorkerInternal(CountMap, CountReduce, crashNever, /* changeSeed= */ false)
		}()

		closeChan := make(chan bool)
		time.AfterFunc(10 * time.Second, func() {
			closeChan<-true
		})
		ticker := time.NewTicker(1 * time.Second) 
		out:
		for {
			select {
			case <-ticker.C:
				if c.Done() {
					break out
				}
			case <-closeChan:
				break out
			}
		}
		checkAndDeleteFilesInS3(AWS_INTERMEDIATE_PREFIX, expectedIntermediateFilenames, t)
		checkAndDeleteFilesInS3(AWS_OUTPUT_PREFIX, expectedOutputFilenames, t)
	})
}

func TestWorkerCrashCoordinatorNoOtherWorkerToReassignTo(t *testing.T) {
	setup()
	t.Run(simpleTestInput.name(), func(t *testing.T) {
		c := MakeCoordinatorInternal(simpleTestInput.files, simpleTestInput.nReduce, /* reassignTaskDurationInMs= */ 3000)
		go func() {
			WorkerInternal(CountMap, CountReduce, crashImmediately, /* changeSeed= */ false)
		}()

		closeChan := make(chan bool)
		time.AfterFunc(10 * time.Second, func() {
			closeChan<-true
		})
		ticker := time.NewTicker(1 * time.Second) 
		out:
		for {
			select {
			case <-ticker.C:
				if c.Done() {
					break out
				}
			case <-closeChan:
				break out
			}
		}
		checkAndDeleteFilesInS3(AWS_INTERMEDIATE_PREFIX, []string{}, t)
		checkAndDeleteFilesInS3(AWS_OUTPUT_PREFIX, []string{}, t)
	})
}





