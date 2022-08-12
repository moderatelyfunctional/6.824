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

rm -rf mr-tmp
mkdir mr-tmp || exit 1
cd mr-tmp || exit 1
rm -f mr-*

(cd ../../mrapps && go clean)
(cd .. && go clean)
(cd ../../mrapps && go build $RACE -buildmode=plugin jobcount.go) || exit 1
(cd .. && go build $RACE mrcoordinator.go) || exit 1
(cd .. && go build $RACE mrworker.go) || exit 1

rm -f mr-*

$TIMEOUT ../mrcoordinator ../pg*txt &
sleep 1

$TIMEOUT ../mrworker ../../mrapps/jobcount.so &
$TIMEOUT ../mrworker ../../mrapps/jobcount.so
$TIMEOUT ../mrworker ../../mrapps/jobcount.so &
$TIMEOUT ../mrworker ../../mrapps/jobcount.so

NT=`cat mr-out* | awk '{print $2}'`
if [ "$NT" -eq "8" ]
then
  echo '---' job count test: PASS
else
  echo '---' map jobs ran incorrect number of times "($NT != 8)"
  echo '---' job count test: FAIL
  failed_any=1
fi
