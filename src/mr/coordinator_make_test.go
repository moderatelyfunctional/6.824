package mr

import "fmt"
import "reflect"
import "testing"

import "net/http"

func setup() {
	http.DefaultServeMux = new(http.ServeMux)
}

type TestCoordinatorInput struct {
	files 		[]string
	nReduce  	int
}

func (testInput *TestCoordinatorInput) name() string {
	return fmt.Sprintf("%v,%v", testInput.files, testInput.nReduce)
}

var simpleTestInput TestCoordinatorInput = TestCoordinatorInput{
	[]string{"input/pg-being_ernest.txt"},
	/* nReduce= */ 2,
}

var complexTestInput TestCoordinatorInput = TestCoordinatorInput{
	[]string{"input/pg-being_ernest.txt", "input/pg-dorian_gray.txt"},
	/* nReduce= */ 3,
}

var labTestInput TestCoordinatorInput = TestCoordinatorInput{
	[]string{
		"input/pg-being_ernest.txt",
		"input/pg-frankenstein.txt",
		"input/pg-huckleberry_finn.txt",
		"input/pg-sherlock_holmes.txt",
		"input/pg-dorian_gray.txt",
		"input/pg-grimm.txt",
		"input/pg-metamorphosis.txt",
		"input/pg-tom_sawyer.txt",
	},
	/* nReduce= */ 3,
}

func TestMakeCoordinatorOneFileMapTasks(t *testing.T) {
	setup()
	expected := []MapTask{
		{
			"input/pg-being_ernest.txt",
			INTERMEDIATE_FILE_PREFIX,
			/* mapIndex= */ 0,
			/* nReduce= */ 2,
			TASK_NOT_STARTED,
			/* assignedTimeInMs= */ 0,
		},
	}
	t.Run(simpleTestInput.name(), func(t *testing.T) {
		actual := MakeCoordinator(simpleTestInput.files, simpleTestInput.nReduce)
		if !reflect.DeepEqual(expected, actual.mapTasks) {
			t.Errorf("Coordinator map tasks:\nexpected %v\ngot %v", expected, actual.mapTasks)
		}
		actual.ShutDown()
	})
}

func TestMakeCoordinatorOneFileNReduce(t *testing.T) {
	setup()
	expected := 2
	t.Run(simpleTestInput.name(), func(t *testing.T) {
		actual := MakeCoordinator(simpleTestInput.files, simpleTestInput.nReduce)
		if !reflect.DeepEqual(expected, actual.nReduce) {
			t.Errorf("Coordinator nReduce:\nexpected %v\ngot %v", expected, actual.nReduce)
		}
	})
}

func TestMakeCoordinatorOneFileReduceTasks(t *testing.T) {
	setup()
	expected := []ReduceTask{
		{
			[]string{"mr-0-0"},
			OUTPUT_FILE_PREFIX,
			/* reduceIndex= */ 0,
			TASK_NOT_STARTED,
			/* assignedTimeInMs= */ 0,
		},
		{
			[]string{"mr-0-1"},
			OUTPUT_FILE_PREFIX,
			/* reduceIndex= */ 1,
			TASK_NOT_STARTED,
			/* assignedTimeInMs= */ 0,
		},
	}
	t.Run(simpleTestInput.name(), func(t *testing.T) {
		actual := MakeCoordinator(simpleTestInput.files, simpleTestInput.nReduce)
		if !reflect.DeepEqual(expected, actual.reduceTasks) {
			t.Errorf("Coordinator reduce tasks:\nexpected %v\ngot %v", expected, actual.reduceTasks)
		}
		actual.ShutDown()
	})
}

func TestMakeCoordinatorOneFileState(t *testing.T) {
	setup()
	expected := COORDINATOR_MAP
	t.Run(simpleTestInput.name(), func(t *testing.T) {
		actual := MakeCoordinator(simpleTestInput.files, simpleTestInput.nReduce)
		if !reflect.DeepEqual(expected, actual.state) {
			t.Errorf("Coordinator state:\nexpected %v\ngot %v", expected, actual.state)
		}
		actual.ShutDown()
	})
}

func TestMakeCoordinatorManyFilesMapTasks(t *testing.T) {
	setup()
	expected := []MapTask{
		{
			"input/pg-being_ernest.txt",
			INTERMEDIATE_FILE_PREFIX,
			/* mapIndex= */ 0,
			/* nReduce= */ 3,
			TASK_NOT_STARTED,
			/* assignedTimeInMs= */ 0,
		},
		{
			"input/pg-dorian_gray.txt",
			INTERMEDIATE_FILE_PREFIX,
			/* mapIndex= */ 1,
			/* nReduce= */ 3,
			TASK_NOT_STARTED,
			/* assignedTimeInMs= */ 0,
		},
	}
	t.Run(complexTestInput.name(), func(t *testing.T) {
		actual := MakeCoordinator(complexTestInput.files, complexTestInput.nReduce)
		if !reflect.DeepEqual(expected, actual.mapTasks) {
			t.Errorf("Coordinator map tasks:\nexpected %v\ngot %v", expected, actual.mapTasks)
		}
		actual.ShutDown()
	})
}

func TestMakeCoordinatorManyFilesNReduce(t *testing.T) {
	setup()
	expected := 3
	t.Run(complexTestInput.name(), func(t *testing.T) {
		actual := MakeCoordinator(complexTestInput.files, complexTestInput.nReduce)
		if !reflect.DeepEqual(expected, actual.nReduce) {
			t.Errorf("Coordinator nReduce:\nexpected %v\ngot %v", expected, actual.nReduce)
		}
		actual.ShutDown()
	})
}

func TestMakeCoordinatorManyFilesReduceTasks(t *testing.T) {
	setup()
	expected := []ReduceTask{
		{
			[]string{"mr-0-0", "mr-1-0"},
			OUTPUT_FILE_PREFIX,
			/* reduceIndex= */ 0,
			TASK_NOT_STARTED,
			/* assignedTimeInMs= */ 0,
		},
		{
			[]string{"mr-0-1", "mr-1-1"},
			OUTPUT_FILE_PREFIX,
			/* reduceIndex= */ 1,
			TASK_NOT_STARTED,
			/* assignedTimeInMs= */ 0,
		},
		{
			[]string{"mr-0-2", "mr-1-2"},
			OUTPUT_FILE_PREFIX,
			/* reduceIndex= */ 2,
			TASK_NOT_STARTED,
			/* assignedTimeInMs= */ 0,
		},
	}
	t.Run(complexTestInput.name(), func(t *testing.T) {
		actual := MakeCoordinator(complexTestInput.files, complexTestInput.nReduce)
		if !reflect.DeepEqual(expected, actual.reduceTasks) {
			t.Errorf("Coordinator reduce tasks:\nexpected %v\ngot %v", expected, actual.reduceTasks)
		}
		actual.ShutDown()
	})
}

func TestMakeCoordinatorManyFilesState(t *testing.T) {
	setup()
	expected := COORDINATOR_MAP
	t.Run(complexTestInput.name(), func(t *testing.T) {
		actual := MakeCoordinator(complexTestInput.files, complexTestInput.nReduce)
		if !reflect.DeepEqual(expected, actual.state) {
			t.Errorf("Coordinator state:\nexpected %v\ngot %v", expected, actual.state)
		}
		actual.ShutDown()
	})
}



