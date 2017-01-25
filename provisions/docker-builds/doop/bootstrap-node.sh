#!/bin/bash

# a generic bootstrapper to start services via supervisor
# The supervisor config has to be part of the image, the list of service
# is declared as comma-separated list 
#   docker run ... -e SVCLIST=aservice,bservice ...

if [ "$SVCLIST" != "" ]
then
  services=(${SVCLIST//,/ })
  for svc in "${services[@]}"
  do
    echo "[$(date)] starting $svc"
    supervisorctl start $svc
    sleep 1
    supervisorctl status $svc
  done
fi

