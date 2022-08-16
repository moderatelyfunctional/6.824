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

func GrepMap(filename string, contents string) []KeyValue {
	kva := []KeyValue{}
	for i, line := range strings.Split(contents, "\n") {
		for _, match := range r.FindAllString(line, -1) {
			kv := KeyValue{match, fmt.Sprintf("%s-%d", filename, i)}
			kva = append(kva, kv)
		}
	}
	return kva
}

func GrepReduce(key string, values []string) string {
	return fmt.Sprintf("\n%s", strings.Join(values, "\n"))
}


func Map(filename string, contents string) (res []mr.KeyValue) {
	// for i, line := strings.Split(value, "\n") {
	// 	if r.Match(line) {
	// 		kv := mr.KeyValue{}
	// 		res = 
	// 	}
	// }
}