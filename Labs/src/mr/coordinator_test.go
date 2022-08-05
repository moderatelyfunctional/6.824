package mr

import "fmt"
import "reflect"
import "testing"

import "net/http"

type TestCoordinatorInput struct {
	files 		[]string
	nReduce  	int
}

func (testInput *TestCoordinatorInput) name() string {
	return fmt.Sprintf("%v,%v", testInput.files, testInput.nReduce)
}

func setup() {
	http.DefaultServeMux = new(http.ServeMux)
}

func TestMakeCoordinatorOneFileMapTasks(t *testing.T) {
	setup()
	testInput := TestCoordinatorInput{
		[]string{"pg-being_ernest.txt"},
		/* nReduce= */ 2,
	}
	expected := []MapTask{
		{
			"pg-being_ernest.txt",
			INTERMEDIATE_FILE_PREFIX,
			/* mapIndex= */ 0,
			/* nReduce= */ 2,
			TASK_NOT_STARTED,
		},
	}
	t.Run(testInput.name(), func(t *testing.T) {
		actual := MakeCoordinator(testInput.files, testInput.nReduce)
		if !reflect.DeepEqual(expected, actual.mapTasks) {
			t.Errorf("Coordinator map tasks:\nexpected %v\ngot %v", expected, actual.mapTasks)
		}
	})
}

func TestMakeCoordinatorOneFileNReduce(t *testing.T) {
	setup()
	testInput := TestCoordinatorInput{
		[]string{"pg-being_ernest.txt"},
		/* nReduce= */ 2,
	}
	expected := 2
	t.Run(testInput.name(), func(t *testing.T) {
		actual := MakeCoordinator(testInput.files, testInput.nReduce)
		if !reflect.DeepEqual(expected, actual.nReduce) {
			t.Errorf("Coordinator nReduce:\nexpected %v\ngot %v", expected, actual.nReduce)
		}
	})
}

func TestMakeCoordinatorOneFileReduceTasks(t *testing.T) {
	setup()
	testInput := TestCoordinatorInput{
		[]string{"pg-being_ernest.txt"},
		/* nReduce= */ 2,
	}
	expected := []ReduceTask{
		{
			[]string{"mr-0-0"},
			OUTPUT_FILE_PREFIX,
			/* reduceIndex= */ 0,
			TASK_NOT_STARTED,
		},
		{
			[]string{"mr-0-1"},
			OUTPUT_FILE_PREFIX,
			/* reduceIndex= */ 1,
			TASK_NOT_STARTED,	
		},
	}
	t.Run(testInput.name(), func(t *testing.T) {
		actual := MakeCoordinator(testInput.files, testInput.nReduce)
		if !reflect.DeepEqual(expected, actual.reduceTasks) {
			t.Errorf("Coordinator reduce tasks:\nexpected %v\ngot %v", expected, actual.reduceTasks)
		}
	})
}

func TestMakeCoordinatorOneFileState(t *testing.T) {
	setup()
	testInput := TestCoordinatorInput{
		[]string{"pg-being_ernest.txt"},
		/* nReduce= */ 2,
	}
	expected := COORDINATOR_MAP
	t.Run(testInput.name(), func(t *testing.T) {
		actual := MakeCoordinator(testInput.files, testInput.nReduce)
		if !reflect.DeepEqual(expected, actual.state) {
			t.Errorf("Coordinator state:\nexpected %v\ngot %v", expected, actual.state)
		}
	})
}

func TestMakeCoordinatorManyFilesMapTasks(t *testing.T) {
	setup()
	testInput := TestCoordinatorInput{
		[]string{"pg-being_ernest.txt", "pg-dorian_gray.txt"},
		/* nReduce= */ 3,
	}
	expected := []MapTask{
		{
			"pg-being_ernest.txt",
			INTERMEDIATE_FILE_PREFIX,
			/* mapIndex= */ 0,
			/* nReduce= */ 3,
			TASK_NOT_STARTED,
		},
		{
			"pg-dorian_gray.txt",
			INTERMEDIATE_FILE_PREFIX,
			/* mapIndex= */ 1,
			/* nReduce= */ 3,
			TASK_NOT_STARTED,
		},
	}
	t.Run(testInput.name(), func(t *testing.T) {
		actual := MakeCoordinator(testInput.files, testInput.nReduce)
		if !reflect.DeepEqual(expected, actual.mapTasks) {
			t.Errorf("Coordinator map tasks:\nexpected %v\ngot %v", expected, actual.mapTasks)
		}
	})
}

func TestMakeCoordinatorManyFilesNReduce(t *testing.T) {
	setup()
	testInput := TestCoordinatorInput{
		[]string{"pg-being_ernest.txt", "pg-dorian_gray.txt"},
		/* nReduce= */ 3,
	}
	expected := 3
	t.Run(testInput.name(), func(t *testing.T) {
		actual := MakeCoordinator(testInput.files, testInput.nReduce)
		if !reflect.DeepEqual(expected, actual.nReduce) {
			t.Errorf("Coordinator nReduce:\nexpected %v\ngot %v", expected, actual.nReduce)
		}
	})
}

func TestMakeCoordinatorManyFilesReduceTasks(t *testing.T) {
	setup()
	testInput := TestCoordinatorInput{
		[]string{"pg-being_ernest.txt", "pg-dorian_gray.txt"},
		/* nReduce= */ 3,
	}
	expected := []ReduceTask{
		{
			[]string{"mr-0-0", "mr-1-0"},
			OUTPUT_FILE_PREFIX,
			/* reduceIndex= */ 0,
			TASK_NOT_STARTED,
		},
		{
			[]string{"mr-0-1", "mr-1-1"},
			OUTPUT_FILE_PREFIX,
			/* reduceIndex= */ 1,
			TASK_NOT_STARTED,	
		},
		{
			[]string{"mr-0-2", "mr-1-2"},
			OUTPUT_FILE_PREFIX,
			/* reduceIndex= */ 2,
			TASK_NOT_STARTED,	
		},
	}
	t.Run(testInput.name(), func(t *testing.T) {
		actual := MakeCoordinator(testInput.files, testInput.nReduce)
		if !reflect.DeepEqual(expected, actual.reduceTasks) {
			t.Errorf("Coordinator reduce tasks:\nexpected %v\ngot %v", expected, actual.reduceTasks)
		}
	})
}

func TestMakeCoordinatorManyFilesState(t *testing.T) {
	setup()
	testInput := TestCoordinatorInput{
		[]string{"pg-being_ernest.txt", "pg-dorian_gray.txt"},
		/* nReduce= */ 3,
	}
	expected := COORDINATOR_MAP
	t.Run(testInput.name(), func(t *testing.T) {
		actual := MakeCoordinator(testInput.files, testInput.nReduce)
		if !reflect.DeepEqual(expected, actual.state) {
			t.Errorf("Coordinator state:\nexpected %v\ngot %v", expected, actual.state)
		}
	})
}


