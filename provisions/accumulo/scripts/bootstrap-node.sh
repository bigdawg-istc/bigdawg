#!/bin/bash

# a generic bootstrapper to put service checks in place for consul
# and start services via supervisor
# The supervisor config has to be part of the image, the list of service
# is declared as comma-separated list 
#   docker run ... -e SVCLIST=aservice,bservice ...

if [ "$SVCLIST" != "" ]
then
  services=(${SVCLIST//,/ })
  for svc in "${services[@]}"
  do
    echo "[$(date)] starting $svc"
    svccheck=/etc/consul/services/${svc}_service.json
    if [ -f $svccheck ]
    then
      echo "[$(date)] Add consul service check for $svc"
      cp $svccheck /etc/consul
    else
      echo "[$(date)] No consul service check for $svc"
    fi
    supervisorctl start $svc
    sleep 1
    supervisorctl status $svc
  done
fi

supervisorctl restart consul-agent

