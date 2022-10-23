package raft_test

import "fmt"
import "raft"
import "testing"

func TestIsLowerTerm(t *testing.T) {
	rf := &Raft{}
	rf.currentTerm = 0

	t.Run(simpleTestInput.name(), func(t *testing.T) {
		actual := MakeCoordinator(simpleTestInput.files, simpleTestInput.nReduce)
		if !reflect.DeepEqual(expected, actual.mapTasks) {
			t.Errorf("Coordinator map tasks:\nexpected %v\ngot %v", expected, actual.mapTasks)
		}
	})
}