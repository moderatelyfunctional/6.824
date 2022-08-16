package mr

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
	expectedIntermediateFilenames := []string{
		"mr-0-0",
		"mr-0-1",
	}
	expectedOutputFilenames := []string{
		"mr-out-0",
		"mr-out-1",
	}
	t.Run(simpleTestInput.name(), func(t *testing.T) {
		c := MakeCoordinatorInternal(simpleTestInput.files, simpleTestInput.nReduce, /* reassignTaskDurationInMs= */ 3000)
		go func() {
			WorkerInternal(CountMap, CountReduce, crashImmediately)
		}()
		time.Sleep(500 * time.Millisecond)
		go func() {
			WorkerInternal(CountMap, CountReduce, crashNever)
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
		if !checkFilesExist(expectedIntermediateFilenames) {
			t.Errorf("Expected intermediate files %v to exist", expectedIntermediateFilenames)
		}
		removeFiles(expectedIntermediateFilenames)
		if !checkFilesExist(expectedOutputFilenames) {
			t.Errorf("Expected output files %v to exist", expectedIntermediateFilenames)	
		}
		removeFiles(expectedOutputFilenames)
	})
}

func TestWorkerCrashCoordinatorNoOtherWorkerToReassignTo(t *testing.T) {
	setup()
	expectedIntermediateFilenames := []string{
		"mr-0-0",
		"mr-0-1",
	}
	expectedOutputFilenames := []string{
		"mr-out-0",
		"mr-out-1",
	}
	t.Run(simpleTestInput.name(), func(t *testing.T) {
		c := MakeCoordinatorInternal(simpleTestInput.files, simpleTestInput.nReduce, /* reassignTaskDurationInMs= */ 3000)
		go func() {
			WorkerInternal(CountMap, CountReduce, crashImmediately)
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
		if checkFilesExist(expectedIntermediateFilenames) {
			t.Errorf("Didn't' expected intermediate files %v to exist", expectedIntermediateFilenames)
		}
		if checkFilesExist(expectedOutputFilenames) {
			t.Errorf("Didn't expected output files %v to exist", expectedIntermediateFilenames)	
		}
	})
}





