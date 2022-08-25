#!/usr/bin/env bash

#
# map-reduce tests
#

# comment this out to run the tests without the Go race detector.
RACE=-race

if [[ "$OSTYPE" = "darwin"* ]]
then
  if go version | grep 'go1.17.[012345]'
  then
    # -race with plug-ins on x86 MacOS 12 with
    # go1.17 before 1.17.6 sometimes crash.
    RACE=
    echo '*** Turning off -race since it may not work on a Mac'
    echo '    with ' `go version`
  fi
fi

TIMEOUT=timeout
if timeout 2s sleep 1 > /dev/null 2>&1
then
  :
else
  if gtimeout 2s sleep 1 > /dev/null 2>&1
  then
    TIMEOUT=gtimeout
  else
    # no timeout command
    TIMEOUT=
    echo '*** Cannot find timeout command; proceeding without timeouts.'
  fi
fi
if [ "$TIMEOUT" != "" ]
then
  TIMEOUT+=" -k 2s 180s "
fi

# run the test in a fresh sub-directory.
rm -rf mr-tmp
mkdir mr-tmp || exit 1
cd mr-tmp || exit 1
rm -f mr-*

# make sure software is freshly built.
(cd ../../mronawsapps && go clean)
(cd .. && go clean)
(cd ../../mronawsapps && go build $RACE -buildmode=plugin indexer.go) || exit 1
(cd .. && go build $RACE mronawscoordinator.go) || exit 1
(cd .. && go build $RACE mronawsworker.go) || exit 1
(cd .. && go build $RACE mronawssequential.go) || exit 1

failed_any=0

#########################################################
# first word-count

# generate the correct output
../mronawssequential ../../mronawsapps/indexer.so ../pg*txt || exit 1
sort mr-out-0 > mr-correct-indexer.txt
rm -f mr-out*

echo '***' Starting indexer test.

$TIMEOUT ../mronawscoordinator ../pg*txt &
pid=$!

# give the coordinator time to create the sockets.
sleep 1

# start multiple workers.
$TIMEOUT ../mronawsworker ../../mronawsapps/indexer.so &
$TIMEOUT ../mronawsworker ../../mronawsapps/indexer.so

sort mr-out* | grep . > mr-indexer-all
if cmp mr-indexer-all mr-correct-indexer.txt
then
  echo '---' indexer test: PASS
else
  echo '---' indexer output is not the same as mr-correct-indexer.txt
  echo '---' indexer test: FAIL
  failed_any=1
fi

wait

aws s3 rm s3://mapreducedata/intermediate --recursive --exclude "."
aws s3 rm s3://mapreducedata/output --recursive --exclude "."