package raft

import "os"
import "fmt"
import "time"
import "testing"
import "runtime"
import "runtime/pprof"

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
func TestTickerChannel(t *testing.T) {
	heartbeatChan := make(chan bool)
	quitChan := make(chan bool)
	tickerQuitChan := make(chan bool)
	fmt.Println("TestTickerChannel starting numGoroutines", runtime.NumGoroutine())
	go func() {
		heartbeatTicker := time.NewTicker(time.Duration(200) * time.Millisecond)
		for {
			select {
			case <-heartbeatTicker.C:
				heartbeatChan<-true
			case <-tickerQuitChan:
				fmt.Println("TestTickerChannel ticker quit chan", runtime.NumGoroutine())
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
			fmt.Println("TestTickerChannel heartbeat", runtime.NumGoroutine())
		case <-quitChan:
			fmt.Println("TestTickerChannel main quit chan", runtime.NumGoroutine())
			return
		}
	}
}

func TestCloseChannel(t *testing.T) {
	fmt.Println("TestCloseChannel starting numGoroutines", runtime.NumGoroutine())
	applierChan := make(chan bool)
	go func() {
		fmt.Println("TestCloseChannel starting applierChan")
		for m := range applierChan {
			fmt.Println("Received message", m)
		}
		fmt.Println("TestCloseChannel closing applierChan")
	}()

	fmt.Println("TestCloseChannel number of goroutines", runtime.NumGoroutine())
	time.Sleep(time.Duration(2) * time.Second)
	
	close(applierChan)
	time.Sleep(time.Duration(3) * time.Second)
	fmt.Println("TestCloseChannel goroutines after closing channel", runtime.NumGoroutine())
}

func TestGoroutine(t *testing.T) {
	pprof.Lookup("goroutine").WriteTo(os.Stdout, 1)
	fmt.Println("TestGoroutine starting numGoroutines", runtime.NumGoroutine())
	
}






