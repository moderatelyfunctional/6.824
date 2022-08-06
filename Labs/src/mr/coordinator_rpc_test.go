package mr

import "reflect"
import "testing"

func TestCoordinatorAssignTaskRpcOneMapTask(t *testing.T) {
	setup()
	expected_reply_one := AssignTaskReply{
		taskType: COORDINATOR_MAP,
		mapTask: MapTask{
			filename: "pg-being_ernest.txt",
			outputPrefix: INTERMEDIATE_FILE_PREFIX,
			mapIndex: 0,
			nReduce: 2,
			state: TASK_ASSIGNED,
		},
		reduceTask: ReduceTask{},
	}
	expected_reply_two := AssignTaskReply{taskType: COORDINATOR_MAP}
	t.Run(simpleTestInput.name(), func(t *testing.T) {
		c := MakeCoordinator(simpleTestInput.files, simpleTestInput.nReduce)
		args_one := AssignTaskArgs{}
		reply_one := AssignTaskReply{}
		c.AssignTask(&args_one, &reply_one)
		if !reflect.DeepEqual(expected_reply_one, reply_one) {
			t.Errorf("AssignTask reply_one:\nexpected %v\ngot %v", expected_reply_one, reply_one)
		}
		// go func() {
			args_two := AssignTaskArgs{}
			reply_two := AssignTaskReply{}
			c.AssignTask(&args_two, &reply_two)
			if !reflect.DeepEqual(expected_reply_two, reply_two) {
				t.Errorf("AssignTask reply_two:\nexpected %v\ngot %v", expected_reply_two, reply_two)
			}
		// }()
	})
}

// func TestCoordinatorAssignTaskTwoMapTask(t *testing.T) {
// 	setup()
// 	expected_reply := AssignTaskReply{
// 		taskType: COORDINATOR_MAP,
// 		mapTask: MapTask{
// 			filename: "pg-being_ernest.txt",
// 			outputPrefix: INTERMEDIATE_FILE_PREFIX,
// 			mapIndex: 0,
// 			nReduce: 2,
// 			state: TASK_ASSIGNED,
// 		},
// 		reduceTask: ReduceTask{},
// 	}
// 	t.Run(simpleTestInput.name(), func(t *testing.T) {
// 		c := MakeCoordinator(simpleTestInput.files, simpleTestInput.nReduce)
// 		args := AssignTaskArgs{}
// 		reply := AssignTaskReply{}
// 		c.AssignTask(&args, &reply)
// 		if !reflect.DeepEqual(expected_reply, reply) {
// 			t.Errorf("AssignTask reply:\nexpected %v\ngot %v", expected_reply, reply)
// 		}
// 	})
// }