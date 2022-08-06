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
	expected_empty_reply := AssignTaskReply{taskType: COORDINATOR_MAP}
	t.Run(simpleTestInput.name(), func(t *testing.T) {
		c := MakeCoordinator(simpleTestInput.files, simpleTestInput.nReduce)
		args_one := AssignTaskArgs{}
		reply_one := AssignTaskReply{}
		c.AssignTask(&args_one, &reply_one)
		if !reflect.DeepEqual(expected_reply_one, reply_one) {
			t.Errorf("AssignTask reply_one:\nexpected %v\ngot %v", expected_reply_one, reply_one)
		}
		go func() {
			args_two := AssignTaskArgs{}
			reply_two := AssignTaskReply{}
			c.AssignTask(&args_two, &reply_two)
			if !reflect.DeepEqual(expected_empty_reply, reply_two) {
				t.Errorf("AssignTask reply_two:\nexpected %v\ngot %v", expected_empty_reply, reply_two)
			}
		}()
		args_three := AssignTaskArgs{}
		reply_three := AssignTaskReply{}
		c.AssignTask(&args_three, &reply_three)
		if !reflect.DeepEqual(expected_empty_reply, reply_three) {
			t.Errorf("AssignTask reply_three:\nexpected %v\ngot %v", expected_empty_reply, reply_three)
		}
	})
}

func TestCoordinatorAssignTaskTwoMapTasks(t *testing.T) {
	setup()
	expected_reply_one := AssignTaskReply{
		taskType: COORDINATOR_MAP,
		mapTask: MapTask{
			filename: "pg-being_ernest.txt",
			outputPrefix: INTERMEDIATE_FILE_PREFIX,
			mapIndex: 0,
			nReduce: 3,
			state: TASK_ASSIGNED,
		},
		reduceTask: ReduceTask{},
	}
	expected_reply_two := AssignTaskReply{
		taskType: COORDINATOR_MAP,
		mapTask: MapTask{
			filename: "pg-dorian_gray.txt",
			outputPrefix: INTERMEDIATE_FILE_PREFIX,
			mapIndex: 1,
			nReduce: 3,
			state: TASK_ASSIGNED,
		},
		reduceTask: ReduceTask{},
	}
	t.Run(complexTestInput.name(), func(t *testing.T) {
		c := MakeCoordinator(complexTestInput.files, complexTestInput.nReduce)
		replyChan := make(chan AssignTaskReply)
		go func() {
			args_one := AssignTaskArgs{}
			reply_one := AssignTaskReply{}
			c.AssignTask(&args_one, &reply_one)
			replyChan<-reply_one
		}()
		go func() {
			args_two := AssignTaskArgs{}
			reply_two := AssignTaskReply{}
			c.AssignTask(&args_two, &reply_two)
			replyChan<-reply_two
		}()
		chan_reply_one := <-replyChan
		chan_reply_two := <-replyChan
		if !((reflect.DeepEqual(expected_reply_one, chan_reply_one) && 
			reflect.DeepEqual(expected_reply_two, chan_reply_two)) ||
		   (reflect.DeepEqual(expected_reply_one, chan_reply_two) &&
			reflect.DeepEqual(expected_reply_two, chan_reply_one))) {
			t.Errorf("AssignTask replys expected:\n%v\n%v", expected_reply_one, expected_reply_two)
			t.Errorf("AssignTask replys got:\n%v\n%v", chan_reply_one, chan_reply_two)
		}
	})
}



