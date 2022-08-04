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
		if !reflect.DeepEqual(expected, actual.mapTasks) {
			t.Errorf("Coordinator map tasks:\nexpected %v\ngot %v", expected, actual.mapTasks)
		}
	})
}

// func TestMakeCoordinatorOneFileMapOutput(t *testing.T) {
// 	setup()
// 	test := struct{
// 		files 			[]string
// 		nReduce 		int
// 	}{
// 		[]string{"pg-being_ernest.txt"},
// 		/* nReduce= */ 2,
// 	}
// 	expectedMap =  := &Coordinator{
// 		[]MapTask{
// 			{
// 				"pg-being_ernest.txt",
// 				INTERMEDIATE_FILE_PREFIX,
// 				/* mapIndex= */ 0,
// 				/* nReduce= */ 2,
// 				TASK_NOT_STARTED,
// 			},
// 		},
// 		/* wantnReduce= */ 2,
// 		[]ReduceTask{
// 			{
// 				[]string{"mr-0-0"},
// 				OUTPUT_FILE_PREFIX,
// 				/* reduceIndex= */ 0,
// 				TASK_NOT_STARTED,
// 			},
// 			{
// 				[]string{"mr-0-1"},
// 				OUTPUT_FILE_PREFIX,
// 				/* reduceIndex= */ 1,
// 				TASK_NOT_STARTED,	
// 			},
// 		},
// 		COORDINATOR_MAP,
// 	}

// 	testname := fmt.Sprintf("%v,%v", test.files, test.nReduce)
// 	t.Run(testname, func(t *testing.T) {
// 		actual := MakeCoordinator(test.files, test.nReduce)
// 		if !reflect.DeepEqual(expected, actual) {
// 			t.Errorf("Coordinator:\nexpected %v\ngot %v", expected, actual)
// 		}
// 	})
// }

// func TestMakeCoordinatorManyFiles(t *testing.T) {
// 	setup()
// 	test := struct{
// 		files 			[]string
// 		nReduce 		int
// 	}{
// 		[]string{"pg-being_ernest.txt", "pg-dorian_gray.txt"},
// 		/* nReduce= */ 3,
// 	}
// 	expected := &Coordinator{
// 		[]MapTask{
// 			{
// 				"pg-being_ernest.txt",
// 				INTERMEDIATE_FILE_PREFIX,
// 				/* mapIndex= */ 0,
// 				/* nReduce= */ 3,
// 				TASK_NOT_STARTED,
// 			},
// 			{
// 				"pg-dorian_gray.txt",
// 				INTERMEDIATE_FILE_PREFIX,
// 				/* mapIndex= */ 1,
// 				/* nReduce= */ 3,
// 				TASK_NOT_STARTED,
// 			},
// 		},
// 		/* wantnReduce= */ 3,
// 		[]ReduceTask{
// 			{
// 				[]string{"mr-0-0", "mr-1-0"},
// 				OUTPUT_FILE_PREFIX,
// 				/* reduceIndex= */ 0,
// 				TASK_NOT_STARTED,
// 			},
// 			{
// 				[]string{"mr-0-1", "mr-1-1"},
// 				OUTPUT_FILE_PREFIX,
// 				/* reduceIndex= */ 1,
// 				TASK_NOT_STARTED,	
// 			},
// 			{
// 				[]string{"mr-0-2", "mr-1-2"},
// 				OUTPUT_FILE_PREFIX,
// 				/* reduceIndex= */ 2,
// 				TASK_NOT_STARTED,	
// 			},
// 		},
// 		COORDINATOR_MAP,
// 	}

// 	testname := fmt.Sprintf("%v,%v", test.files, test.nReduce)
// 	t.Run(testname, func(t *testing.T) {
// 		actual := MakeCoordinator(test.files, test.nReduce)
// 		if !reflect.DeepEqual(expected, actual) {
// 			t.Errorf("Coordinator:\nexpected %v\ngot %v", expected, actual)
// 		}
// 	})
// }

func setup() {
	http.DefaultServeMux = new(http.ServeMux)
}



