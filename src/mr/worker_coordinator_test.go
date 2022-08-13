package mr

import "testing"

import "time"

import "strings"
import "strconv"
import "unicode"

//
// The map function is called once for each file of input. The first
// argument is the name of the input file, and the second is the
// file's complete contents. You should ignore the input file name,
// and look only at the contents argument. The return value is a slice
// of key/value pairs.
//
func Map(filename string, contents string) []KeyValue {
	// function to detect word separators.
	ff := func(r rune) bool { return !unicode.IsLetter(r) }

	// split contents into an array of words.
	words := strings.FieldsFunc(contents, ff)

	kva := []KeyValue{}
	for _, w := range words {
		kv := KeyValue{w, "1"}
		kva = append(kva, kv)
	}
	return kva
}

//
// The reduce function is called once for each key generated by the
// map tasks, with a list of all the values created for that key by
// any map task.
//
func Reduce(key string, values []string) string {
	// return the number of occurrences of this word.
	return strconv.Itoa(len(values))
}

func TestWorkerCoordinatorCompletesMapTask(t *testing.T) {
	setup()
	expectedIntermediateFilenames := []string{
		"mr-0-0",
		"mr-0-1",
	}
	t.Run(simpleTestInput.name(), func(t *testing.T) {
		c := MakeCoordinator(simpleTestInput.files, simpleTestInput.nReduce)
		done := make(chan bool)
		go func() {
			Worker(Map, Reduce)
			done<-true
		}()
		for {
			if c.state == COORDINATOR_REDUCE {
				break
			}
			time.Sleep(500 * time.Millisecond)
		}
		c.Stop()
		<-done
		checkFilesExist(expectedIntermediateFilenames)
		removeFiles(expectedIntermediateFilenames)
	})
}

func TestWorkerCoordinatorOneWorkerCompletesMapAndReduceTask(t *testing.T) {
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
		MakeCoordinator(simpleTestInput.files, simpleTestInput.nReduce)
		done := make(chan bool)
		go func() {
			Worker(Map, Reduce)
			done<-true
		}()
		<-done
		checkFilesExist(expectedIntermediateFilenames)
		removeFiles(expectedIntermediateFilenames)
		checkFilesExist(expectedOutputFilenames)
		removeFiles(expectedOutputFilenames)
	})
}

func TestWorkerCoordinatorTwoWorkersCompletesMapAndReduceTask(t *testing.T) {
	setup()
	expectedIntermediateFilenames := []string{
		"mr-0-0",
		"mr-0-1",
		"mr-0-2",
		"mr-1-0",
		"mr-1-1",
		"mr-1-2",
	}
	expectedOutputFilenames := []string{
		"mr-out-0",
		"mr-out-1",
		"mr-out-2",
	}
	t.Run(complexTestInput.name(), func(t *testing.T) {
		MakeCoordinator(complexTestInput.files, complexTestInput.nReduce)
		doneOne := make(chan bool)
		doneTwo := make(chan bool)
		go func() {
			Worker(Map, Reduce)
			doneOne<-true
		}()
		go func() {
			Worker(Map, Reduce)
			doneTwo<-true
		}()
		<-doneOne
		<-doneTwo
		checkFilesExist(expectedIntermediateFilenames)
		removeFiles(expectedIntermediateFilenames)
		checkFilesExist(expectedOutputFilenames)
		removeFiles(expectedOutputFilenames)
	})
}

func TestWorkerCoordinatorLabConditions(t *testing.T) {
	setup()
	expectedIntermediateFilenames := []string{
		"mr-0-0",
		"mr-0-1",
		"mr-0-2",
		"mr-1-0",
		"mr-1-1",
		"mr-1-2",
		"mr-2-0",
		"mr-2-1",
		"mr-2-2",
		"mr-3-0",
		"mr-3-1",
		"mr-3-2",
		"mr-4-0",
		"mr-4-1",
		"mr-4-2",
		"mr-5-0",
		"mr-5-1",
		"mr-5-2",
		"mr-6-0",
		"mr-6-1",
		"mr-6-2",
		"mr-7-0",
		"mr-7-1",
		"mr-7-2",
	}
	expectedOutputFilenames := []string{
		"mr-out-0",
		"mr-out-1",
		"mr-out-2",
		"mr-out-3",
		"mr-out-4",
		"mr-out-5",
		"mr-out-6",
		"mr-out-7",
	}
	t.Run(labTestInput.name(), func(t *testing.T) {
		MakeCoordinator(labTestInput.files, labTestInput.nReduce)
		doneOne := make(chan bool)
		doneTwo := make(chan bool)
		doneThree := make(chan bool)
		go func() {
			Worker(Map, Reduce)
			doneOne<-true
		}()
		go func() {
			Worker(Map, Reduce)
			doneTwo<-true
		}()
		go func() {
			Worker(Map, Reduce)
			doneThree<-true
		}()
		<-doneOne
		<-doneTwo
		<-doneThree
		checkFilesExist(expectedIntermediateFilenames)
		removeFiles(expectedIntermediateFilenames)
		checkFilesExist(expectedOutputFilenames)
		removeFiles(expectedOutputFilenames)
	})

}




