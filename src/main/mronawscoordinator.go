package main

//
// start the coordinator process, which is implemented
// in ../mronaws/coordinator.go
//
// go run mronawscoordinator.go pg*.txt
//
// Please do not change this file.
//

import "6.824/mronaws"
import "time"
import "os"
import "fmt"

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintf(os.Stderr, "Usage: mronawscoordinator inputfiles...\n")
		os.Exit(1)
	}

	m := mronaws.MakeCoordinator(os.Args[1:], 10)
	for !m.Done() {
		time.Sleep(time.Second)
	}

	time.Sleep(time.Second)
}
