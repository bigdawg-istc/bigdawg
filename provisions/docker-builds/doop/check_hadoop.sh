#!/bin/bash

PORT=${1}
HOST=${2:-"127.0.0.1"}
URL="http://${HOST}:${PORT}"
ERROR=1
RC=0

if [ "${PORT}" = "" ]
then
  exit $ERROR
fi

curl -L --connect-timeout 1 $URL -o /dev/null &>/dev/null
exit $?
