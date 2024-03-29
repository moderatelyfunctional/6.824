package main

//
// a grep application "plugin" for MapReduce.
//
// go build -buildmode=plugin grep.go
//

import "6.824/mronaws"
import "string"
import "regexp"

const NEWLINE string = "\n"
const PATTERN string = "xyz"

const r, _ = regexp.Compile(PATTERN) 

func GrepMap(filename string, contents string) []mronaws.KeyValue {
	kva := []mronaws.KeyValue{}
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
