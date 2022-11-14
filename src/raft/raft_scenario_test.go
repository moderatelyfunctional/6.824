package raft

import "fmt"
import "testing"

// The following code snippet will not print out the numbers in order.
func TestRaftScenarioUnorderedChannel(t *testing.T) {
	output := make(chan int)
	for i := 0; i < 10; i++ {
		go func(number int) {
			output<-number
		}(i)
	}

	for i := 0; i < 10; i++ {
		fmt.Println("The output is ", <-output)
	}
}

// The following code snippet will print out the numbers in order.
func TestRaftScenarioOrderedChannel(t *testing.T) {
	output := make(chan int)
	go func() {
		for i := 0; i < 10; i++ {
			output<-i
		}
	}()

	for i := 0; i < 10; i++ {
		fmt.Println("The output is ", <-output)
	}
}