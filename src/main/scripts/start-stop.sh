#!/bin/bash

###
# #%L
# fast-persistent-queue
# %%
# Copyright (C) 2014 btoddb.com
# %%
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
# 
# The above copyright notice and this permission notice shall be included in
# all copies or substantial portions of the Software.
# 
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
# THE SOFTWARE.
# #L%
###

pushd `dirname $0` > /dev/null
HOME_DIR=`pwd`
PID_FILE="${HOME_DIR}/the.pid"
MAX_WAIT_FOR_TERMINATE=60
CLASSPATH=conf:lib/*
EXEC_CLASS=com.btoddb.fastpersitentqueue.speedtest.SpeedTest

JMX_OPTS="-ea -Dcom.sun.management.jmxremote.port=6786 -Dcom.sun.management.jmxremote.authenticate=false -Dcom.sun.management.jmxremote.ssl=false"
DEBUG_OPTS="-Xdebug -Xrunjdwp:transport=dt_socket,server=y,suspend=n,address=8000"

JAVA_OPTS="${JMX_OPTS} -Xms1G -Xmx1G -XX:+UseParNewGC -XX:+UseConcMarkSweepGC -XX:+CMSParallelRemarkEnabled -XX:CMSInitiatingOccupancyFraction=75"
echo

pid_running() {
  PID=$1
  ps -p ${PID} > /dev/null
}

case "$1" in
  start)
    shift 
    java ${JAVA_OPTS} -cp ${CLASSPATH} ${EXEC_CLASS} $*
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
