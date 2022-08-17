#!/bin/bash

TESTS=$(go test -list=.)

SAVEIFS=$IFS   # Save current IFS (Internal Field Separator)
IFS=$'\n'      # Change IFS to newline char
TESTS=($TESTS) # split the `TESTS` string into an array by the same TESTS
IFS=$SAVEIFS   # Restore original IFS

unset 'TESTS[${#TESTS[@]}-1]'
for test in "${TESTS[@]}"
do
	echo "go test -run $test"
	go test -run "$test"
done

