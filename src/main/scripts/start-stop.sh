#!/bin/bash

pushd `dirname $0` > /dev/null
HOME_DIR=`pwd`
PID_FILE="${HOME_DIR}/the.pid"
MAX_WAIT_FOR_TERMINATE=60
CLASSPATH=conf:lib/*
EXEC_CLASS=com.btoddb.fastpersitentqueue.speedtest.SpeedTest

echo

pid_running() {
  PID=$1
  ps -p ${PID} > /dev/null
}

case "$1" in
  start)
    shift 
    java -cp ${CLASSPATH} ${EXEC_CLASS}
    echo $? > ${PID_FILE}
    ;;

  stop)
    shift 
    if ! [ -f $PID_FILE ] ; then
      echo "PID file, ${PID_FILE}, doesn't exist"
      echo
      exit 1
    fi

    PID=`cat ${PID_FILE}`
    if ! pid_running ${PID} ; then
      echo "PID, ${PID}, is not running"
      echo
      exit 1
    fi

    kill ${PID} > /dev/null 2&>1

    MAX_END_TIME=$((`date +%s` + ${MAX_WAIT_FOR_TERMINATE}))
    echo -n "waiting ${MAX_WAIT_FOR_TERMINATE} seconds "
    while [ $((${MAX_END_TIME} - `date +%s`)) -ge 0 ] && pid_running ${PID} ; do
      echo -n "."
      sleep 1
    done

    if ! pid_running ${PID} > /dev/null 2&>1 ; then
      echo " success!"
    else
      echo " did not stop ... killing with -9"
      kill -9 ${PID} > /dev/null 2&>1
      sleep 1
    fi

    if ! pid_running ${PID} > /dev/null 2&>1 ; then
      rm -f ${PID_FILE}
    else
      echo "could not kill PID, ${PID}"
    fi

    echo
    ;;
  
  *)
    echo
    echo "usage:  ${HOME_DIR}/`basename $0` start|stop"
    echo
    ;;
esac

popd > /dev/null
