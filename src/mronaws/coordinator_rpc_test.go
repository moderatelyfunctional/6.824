package mronaws

import "reflect"
import "testing"

func TestCoordinatorAssignTaskRpcOneMapTask(t *testing.T) {
	setup()
	expected_reply_one := AssignTaskReply{
		TaskType: ASSIGN_TASK_MAP,
		MapTask: MapTask{
			Filename: "pg-being_ernest.txt",
			OutputPrefix: INTERMEDIATE_FILE_PREFIX,
			MapIndex: 0,
			NumReduce: 2,
			State: TASK_ASSIGNED,
		},
		ReduceTask: ReduceTask{},
	}
	expected_empty_reply := AssignTaskReply{TaskType: ASSIGN_TASK_IDLE}
	t.Run(simpleTestInput.name(), func(t *testing.T) {
		c := MakeCoordinator(simpleTestInput.files, simpleTestInput.nReduce)
		args_one := AssignTaskArgs{}
		reply_one := AssignTaskReply{}
		c.AssignTask(&args_one, &reply_one)
		reply_one.removeRandomness()
		if !reflect.DeepEqual(expected_reply_one, reply_one) {
			t.Errorf("AssignTask reply_one:\nexpected %v\ngot %v", expected_reply_one, reply_one)
		}
		go func() {
			args_two := AssignTaskArgs{}
			reply_two := AssignTaskReply{}
			reply_two.removeRandomness()
			c.AssignTask(&args_two, &reply_two)
			if !reflect.DeepEqual(expected_empty_reply, reply_two) {
				t.Errorf("AssignTask reply_two:\nexpected %v\ngot %v", expected_empty_reply, reply_two)
			}
		}()
		args_three := AssignTaskArgs{}
		reply_three := AssignTaskReply{}
		reply_three.removeRandomness()
		c.AssignTask(&args_three, &reply_three)
		if !reflect.DeepEqual(expected_empty_reply, reply_three) {
			t.Errorf("AssignTask reply_three:\nexpected %v\ngot %v", expected_empty_reply, reply_three)
		}
	})
}

func TestCoordinatorAssignTaskTwoMapTasks(t *testing.T) {
	setup()
	expected_reply_one := AssignTaskReply{
		TaskType: ASSIGN_TASK_MAP,
		MapTask: MapTask{
			Filename: "pg-being_ernest.txt",
			OutputPrefix: INTERMEDIATE_FILE_PREFIX,
			MapIndex: 0,
			NumReduce: 3,
			State: TASK_ASSIGNED,
		},
		ReduceTask: ReduceTask{},
	}
	expected_reply_two := AssignTaskReply{
		TaskType: ASSIGN_TASK_MAP,
		MapTask: MapTask{
			Filename: "pg-dorian_gray.txt",
			OutputPrefix: INTERMEDIATE_FILE_PREFIX,
			MapIndex: 1,
			NumReduce: 3,
			State: TASK_ASSIGNED,
		},
		ReduceTask: ReduceTask{},
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
		chan_reply_one.removeRandomness()
		chan_reply_two.removeRandomness()
		if !((reflect.DeepEqual(expected_reply_one, chan_reply_one) && 
			reflect.DeepEqual(expected_reply_two, chan_reply_two)) ||
		   (reflect.DeepEqual(expected_reply_one, chan_reply_two) &&
			reflect.DeepEqual(expected_reply_two, chan_reply_one))) {
			t.Errorf("AssignTask replys expected one:\n%v\none:%v", expected_reply_one, expected_reply_two)
			t.Errorf("AssignTask replys got one:\n%v\none:%v", chan_reply_one, chan_reply_two)
		}
	})
}



