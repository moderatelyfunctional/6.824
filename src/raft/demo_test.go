package raft

import "fmt"
import "time"
import "testing"
import "runtime"

// One approach to communicate a heartbeat event that can be configured based on raft state:
// start for leader and stop for follower. Some open questions remain such as:
// 1) How expensive is creating goroutines/tickers/channels versus using locks?
// 2) If the same ticker is reused, how much overhead is it to have two for select statements?
// 3) How are unused tickers garbage collected?
//
// Potential solution without locks
// 1) Initialize heartbeatTicker.C in the main thread and save it to the raft state.
// 2) Create goroutine when an instance becomes a leader which listens to heartbeatTicker.C,
// and sends a heartbeat message to heartbeatChan for processing.
// 3) Kill goroutine via tickerQuitChan messages when the instance becomes a follower.
// 4) sendHeartbeat can be presumed to always be sent from leaders now, so no need to lock to
// check the instance state.
func TestDemo(t *testing.T) {
	heartbeatChan := make(chan bool)
	quitChan := make(chan bool)
	tickerQuitChan := make(chan bool)
	fmt.Println("STARTING NUMBER OF GOROUTINES", runtime.NumGoroutine())
	go func() {
		heartbeatTicker := time.NewTicker(time.Duration(200) * time.Millisecond)
		for {
			select {
			case <-heartbeatTicker.C:
				heartbeatChan<-true
			case <-tickerQuitChan:
				fmt.Println("RETURNING FROM INNER")
				fmt.Println("NUMBER OF GOROUTINES", runtime.NumGoroutine())
				return
			}
		}
	}()
	go func() {
		time.Sleep(time.Duration(5) * time.Second)
		tickerQuitChan<-true
	}()
	go func() {
		time.Sleep(time.Duration(10) * time.Second)
		quitChan<-true
	}()
	for {
		select {
		case <-heartbeatChan:
			fmt.Println("NUMBER OF GOROUTINES", runtime.NumGoroutine())
			fmt.Println("CALL ORIGINAL THREAD")
		case <-quitChan:
			fmt.Println("NUMBER OF GOROUTINES", runtime.NumGoroutine())
			fmt.Println("QUITTING")
			return
		}
	}
}


