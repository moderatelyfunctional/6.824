package mr

import "fmt"
import "testing"

import "strings"
import "regexp"

const NEWLINE string = "\n"
const PATTERN string = "([a-zA-Z]+)?mus([a-zA-Z]+)?"

var r, _ = regexp.Compile(PATTERN)

func GrepMap(filename string, contents string) []KeyValue {
	kva := []KeyValue{}
	for i, line := range strings.Split(contents, NEWLINE) {
		for _, match := range r.FindAllString(line, -1) {
			kv := KeyValue{match, fmt.Sprintf("%s-%d", filename, i)}
			kva = append(kva, kv)
		}
	}
	return kva
}

func GrepReduce(key string, values []string) string {
	return fmt.Sprintf("\n%s", strings.Join(values, "\n"))
}

func TestWorkerCoordinatorCompletesGrepTask(t *testing.T) {
	setup()
	expectedIntermediateFilenames := buildIntermediateFiles(simpleTestInput)
	expectedOutputFilenames := buildOutputFiles(simpleTestInput)
	t.Run(simpleTestInput.name(), func(t *testing.T) {
		c := MakeCoordinator(simpleTestInput.files, simpleTestInput.nReduce)
		done := make(chan bool)
		go func() {
			Worker(GrepMap, GrepReduce)
			done<-true
		}()
		<-done
		checkFilesExist(expectedIntermediateFilenames)
		removeFiles(expectedIntermediateFilenames)
		checkFilesExist(expectedOutputFilenames)
		removeFiles(expectedOutputFilenames)
		c.ShutDown()
	})
}
