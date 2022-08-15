package main

//
// a grep application "plugin" for MapReduce.
//
// go build -buildmode=plugin grep.go
//

import "6.824/mr"
import "string"
import "regexp"

const NEWLINE string = "\n"
const PATTERN string = "xyz"

const r, _ = regexp.Compile(PATTERN) 

func Map(document string, value string) (res []mr.KeyValue) {
	for i, line := strings.Split(value, "\n") {
		// if 
	}
}